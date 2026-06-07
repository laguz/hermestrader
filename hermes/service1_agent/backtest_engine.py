"""In-memory backtest broker, database, and controller to simulate strategy execution over historical bars."""
from __future__ import annotations

import datetime
import logging
import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from hermes.common import OCC_RE
from hermes.greeks import black_scholes_greeks, black_scholes_price
from hermes.service1_agent.core import CascadingEngine, IronCondorBuilder, MoneyManager

logger = logging.getLogger("hermes.backtest")


class BacktestDatabase:
    """Mock repository matching IDatabase Protocol, persisting state in memory."""

    def __init__(self):
        self.logs: List[Dict[str, Any]] = []
        self.trades: List[Dict[str, Any]] = []
        self.pending_orders: List[Any] = []
        self.pending_approvals: List[Dict[str, Any]] = []
        self.settings: Dict[str, str] = {
            "hermes_mode": "paper",
            "agent_autonomy": "autonomous",
            "agent_paused": "false",
            "approval_mode": "false",
            "llm_out_of_loop": "true",
        }
        self.predictions: Dict[str, Dict[str, Any]] = {}
        self.trade_counter = 1

    async def write_log(self, strategy_id: str, msg: str, level: str = "INFO") -> None:
        self.logs.append({
            "strategy_id": strategy_id,
            "msg": msg,
            "level": level,
            "timestamp": datetime.datetime.utcnow(),
        })

    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.settings.get(key, default)

    async def set_setting(self, key: str, value: str) -> None:
        self.settings[key] = str(value)

    async def open_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        return [t for t in self.trades if t["strategy_id"] == strategy_id and t["status"] == "OPEN"]

    async def all_open_trades(self) -> List[Dict[str, Any]]:
        return [t for t in self.trades if t["status"] == "OPEN"]

    async def open_legs(self, strategy_id: str, symbol: str) -> List[Dict[str, Any]]:
        out = []
        for t in self.trades:
            if t["strategy_id"] == strategy_id and t["symbol"] == symbol and t["status"] == "OPEN":
                expiry_iso = t["expiry"].isoformat() if hasattr(t["expiry"], "isoformat") else str(t["expiry"])
                if t.get("short_leg"):
                    out.append({"option_symbol": t["short_leg"], "side": t["side_type"], "expiry": expiry_iso})
                if t.get("long_leg"):
                    out.append({"option_symbol": t["long_leg"], "side": t["side_type"], "expiry": expiry_iso})
        return out

    async def count_open_contracts(self, strategy_id: str, symbol: str, side: str, expiry: str) -> int:
        total = 0
        for t in self.trades:
            if t["strategy_id"] == strategy_id and t["symbol"] == symbol and t["status"] == "OPEN":
                if t.get("side_type") == side:
                    t_expiry = t["expiry"].isoformat() if hasattr(t["expiry"], "isoformat") else str(t["expiry"])
                    if t_expiry == expiry:
                        total += int(t["lots"])
        return total

    async def count_pending_orders(self, strategy_id: str, symbol: str, side: str, expiry: str) -> int:
        return 0

    async def has_pending_approval(self, strategy_id: str, symbol: str, side: str, expiry: str) -> bool:
        return False

    async def latest_prediction(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.predictions.get(symbol)

    async def write_ai_decision(self, strategy_id: str, symbol: str, autonomy: str, decision: Dict[str, Any]) -> None:
        pass

    async def record_pending_order(self, action: Any) -> None:
        pass

    async def record_order_response(self, action: Any, response: Dict[str, Any]) -> None:
        order = (response or {}).get("order") or {}
        order_status = order.get("status", "filled")
        if order_status == "rejected":
            return

        sp = action.strategy_params or {}
        short_leg = sp.get("short_leg")
        long_leg = sp.get("long_leg")
        if not short_leg or not long_leg:
            for leg in (action.legs or []):
                ls = (leg.get("side") or "").lower()
                osym = leg.get("option_symbol")
                if not osym:
                    continue
                if not short_leg and ("sell" in ls or "open" in ls and "sell" in ls):
                    short_leg = osym
                elif not long_leg and ("buy" in ls or "open" in ls and "buy" in ls):
                    long_leg = osym

        def extract_strike(occ):
            if not occ:
                return None
            m = OCC_RE.match(str(occ))
            return int(m.group(4)) / 1000.0 if m else None

        short_strike = extract_strike(short_leg)
        long_strike = extract_strike(long_leg)
        width = action.width
        if width is None and short_strike is not None and long_strike is not None:
            width = abs(float(short_strike) - float(long_strike))

        expiry_date = None
        if action.expiry:
            try:
                expiry_date = datetime.datetime.strptime(str(action.expiry), "%Y-%m-%d").date()
            except ValueError:
                expiry_date = action.expiry

        lots = action.quantity or 1
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side:
                lots = int(leg.get("quantity", lots))
                break

        side_value = sp.get("side_type")
        if not side_value:
            for leg in (action.legs or []):
                m = OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break

        trade = {
            "id": self.trade_counter,
            "strategy_id": action.strategy_id,
            "symbol": action.symbol,
            "side_type": (side_value or "unknown").lower(),
            "short_leg": short_leg,
            "long_leg": long_leg,
            "short_strike": short_strike,
            "long_strike": long_strike,
            "width": width,
            "lots": lots,
            "entry_credit": float(action.price or 0.0) if (action.order_type == "credit" or action.side == "sell") else 0.0,
            "entry_debit": float(action.price or 0.0) if (action.order_type == "debit" or action.side == "buy") else 0.0,
            "expiry": expiry_date,
            "status": "OPEN",
            "opened_at": datetime.datetime.utcnow(),
            "closed_at": None,
            "exit_price": None,
            "pnl": None,
            "close_reason": None,
            "close_tag": None,
        }
        self.trades.append(trade)
        self.trade_counter += 1

    async def close_trade_from_action(self, action: Any, response: Dict[str, Any]) -> None:
        order = (response or {}).get("order") or {}
        order_status = order.get("status", "filled")
        if order_status == "rejected":
            return

        sp = action.strategy_params or {}
        trade_id = sp.get("trade_id")
        close_reason = sp.get("close_reason") or "MANAGED_CLOSE"
        exit_price = float(action.price) if action.price is not None else 0.0

        row = None
        if trade_id is not None:
            for t in self.trades:
                if t["id"] == int(trade_id) and t["status"] == "OPEN":
                    row = t
                    break
        if row is None:
            leg_syms = [leg.get("option_symbol") for leg in (action.legs or []) if leg.get("option_symbol")]
            for t in reversed(self.trades):
                if t["status"] == "OPEN" and t["symbol"] == action.symbol and t["strategy_id"] == action.strategy_id:
                    if t["short_leg"] in leg_syms:
                        row = t
                        break

        if row is not None:
            row["close_reason"] = close_reason
            row["exit_price"] = exit_price
            row["close_tag"] = getattr(action, "tag", None)
            row["status"] = "CLOSED"
            row["closed_at"] = datetime.datetime.utcnow()

            entry_credit = row.get("entry_credit") or 0.0
            entry_debit = row.get("entry_debit") or 0.0
            lots = int(row.get("lots") or 1)
            if entry_credit > 0:
                row["pnl"] = (entry_credit - exit_price) * 100.0 * lots
            else:
                row["pnl"] = (exit_price - entry_debit) * 100.0 * lots

    async def upsert_positions(self, positions: List[Dict[str, Any]], active_order_legs: Optional[Any] = None) -> None:
        broker_legs = {p["symbol"] for p in positions}
        for t in self.trades:
            if t["status"] == "OPEN":
                legs = {leg for leg in (t["short_leg"], t["long_leg"]) if leg}
                if not (legs & broker_legs):
                    t["status"] = "CLOSED"
                    t["close_reason"] = t.get("close_reason") or "RECONCILED_BROKER_FLAT"
                    t["closed_at"] = datetime.datetime.utcnow()

    async def equity_position(self, symbol: str) -> int:
        return 0

    async def tracked_option_symbols(self) -> set[str]:
        return {t["short_leg"] for t in self.trades if t["status"] == "OPEN"} | \
               {t["long_leg"] for t in self.trades if t["status"] == "OPEN" and t.get("long_leg")}

    async def recent_logs(self, limit: int = 200) -> str:
        return "\n".join(l["msg"] for l in self.logs[-limit:])


class BacktestBroker:
    """Mock broker matching IBroker Protocol, replaying historical stock bars."""

    def __init__(self, ts_engine: Any, start_balance: float = 100000.0):
        self.ts_engine = ts_engine
        self._current_date = datetime.datetime(2025, 1, 2)
        self.balance = start_balance
        self.dry_run = False
        self.placed_orders: List[Dict[str, Any]] = []
        self.virtual_positions: Dict[str, Dict[str, Any]] = {}

    @property
    def current_date(self) -> datetime.datetime:
        return self._current_date

    @current_date.setter
    def current_date(self, val: datetime.datetime) -> None:
        self._current_date = val

    async def get_account_balances(self) -> Dict[str, Any]:
        return {
            "option_buying_power": self.balance,
            "stock_buying_power": self.balance,
            "cash": self.balance,
            "total_equity": self.balance,
            "account_type": "margin",
        }

    async def get_positions(self) -> List[Dict[str, Any]]:
        return list(self.virtual_positions.values())

    async def get_orders(self) -> List[Dict[str, Any]]:
        return self.placed_orders

    async def _get_spot(self, symbol: str) -> float:
        try:
            spot = await self.ts_engine.get_price_on_date(symbol, self.current_date.date())
            if spot is not None and spot > 0:
                return float(spot)
        except Exception:
            pass
        return 100.0

    async def get_option_expirations(self, symbol: str) -> List[str]:
        expirations = []
        today = self.current_date.date()
        for d in range(1, 100):
            next_day = today + datetime.timedelta(days=d)
            if next_day.weekday() == 4:
                expirations.append(next_day.strftime("%Y-%m-%d"))
            if len(expirations) >= 8:
                break
        return expirations

    async def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        spot = await self._get_spot(symbol)
        expiry_date = datetime.datetime.strptime(expiry, "%Y-%m-%d").date()
        T = (expiry_date - self.current_date.date()).days / 365.0
        if T <= 0:
            return []

        sigma = 0.30
        try:
            bars = await self.ts_engine.daily_bars(symbol, lookback_days=45)
            if bars is not None and not bars.empty:
                log_ret = np.log(bars["close"] / bars["close"].shift(1)).dropna()
                if len(log_ret) >= 10:
                    sigma = float(log_ret.std() * np.sqrt(252))
        except Exception:
            pass

        r = 0.05
        chain = []
        yymmdd = expiry_date.strftime("%y%m%d")
        step = 1.0 if spot < 150 else 5.0
        start_strike = round(spot * 0.85 / step) * step
        end_strike = round(spot * 1.15 / step) * step

        for k in np.arange(start_strike, end_strike + step, step):
            strike = round(float(k), 2)
            for option_type in ("put", "call"):
                price = black_scholes_price(spot, strike, T, r, sigma, option_type)
                greeks = black_scholes_greeks(spot, strike, T, r, sigma, option_type)
                occ = f"{symbol.upper()}{yymmdd}{'P' if option_type == 'put' else 'C'}{int(round(strike * 1000)):08d}"
                chain.append({
                    "symbol": occ,
                    "option_type": option_type,
                    "strike": strike,
                    "bid": round(max(0.01, price - 0.05), 2),
                    "ask": round(price + 0.05, 2),
                    "greeks": greeks,
                    "underlying_price": spot,
                })
        return chain

    async def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        result = []
        for s in symbols.split(","):
            s = s.strip().upper()
            if not s:
                continue

            m = OCC_RE.match(s)
            if m:
                underlying = m.group(1)
                yymmdd = m.group(2)
                option_type = "put" if m.group(3) == "P" else "call"
                strike = int(m.group(4)) / 1000.0
                expiry_date = datetime.datetime.strptime(yymmdd, "%y%m%d").date()

                spot = await self._get_spot(underlying)
                T = (expiry_date - self.current_date.date()).days / 365.0
                sigma = 0.30
                r = 0.05
                price = black_scholes_price(spot, strike, T, r, sigma, option_type)
                greeks = black_scholes_greeks(spot, strike, T, r, sigma, option_type)
                result.append({
                    "symbol": s,
                    "option_type": option_type,
                    "strike": strike,
                    "bid": round(max(0.01, price - 0.05), 2),
                    "ask": round(price + 0.05, 2),
                    "greeks": greeks,
                    "last": price,
                })
            else:
                spot = await self._get_spot(s)
                result.append({
                    "symbol": s,
                    "last": spot,
                    "bid": round(spot - 0.02, 2),
                    "ask": round(spot + 0.02, 2),
                })
        return result

    async def place_order_from_action(self, action: Any) -> Dict[str, Any]:
        order_id = f"BT-ORD-{len(self.placed_orders) + 1}"
        order = {
            "id": order_id,
            "status": "filled",
            "symbol": action.symbol,
            "quantity": action.quantity or 1,
            "class": action.order_class,
            "tag": action.tag,
        }
        self.placed_orders.append(order)

        is_close = "close" in str(action.tag).lower() or "close" in str(action.legs[0].get("side", "")).lower()
        lots = action.quantity or 1
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side or "close" in leg_side:
                lots = int(leg.get("quantity", lots))
                break

        for leg in (action.legs or []):
            occ = leg["option_symbol"]
            side = leg["side"]
            if is_close or "close" in side:
                if occ in self.virtual_positions:
                    self.virtual_positions[occ]["quantity"] -= lots
                    if self.virtual_positions[occ]["quantity"] <= 0:
                        del self.virtual_positions[occ]
            else:
                if occ not in self.virtual_positions:
                    self.virtual_positions[occ] = {
                        "symbol": occ,
                        "quantity": -lots if "sell" in side else lots,
                    }
                else:
                    self.virtual_positions[occ]["quantity"] += -lots if "sell" in side else lots

        multiplier = 100.0
        if action.price is not None:
            tx_val = float(action.price) * lots * multiplier
            if action.order_type == "credit" or action.side == "sell":
                self.balance += tx_val
            else:
                self.balance -= tx_val

        return {"status": "ok", "order": order}

    async def roll_to_next_month(self, option_symbol: str) -> str:
        m = OCC_RE.match(option_symbol or "")
        if not m:
            raise ValueError(f"Not an OCC option symbol: {option_symbol!r}")
        underlying, yymmdd, pc, strike = m.groups()
        current_exp = datetime.datetime.strptime(yymmdd, "%y%m%d").date()
        expirations = await self.get_option_expirations(underlying)
        future = [datetime.datetime.strptime(e, "%Y-%m-%d").date() for e in expirations
                  if datetime.datetime.strptime(e, "%Y-%m-%d").date() > current_exp]
        if not future:
            raise RuntimeError(f"No later expirations available for {underlying}")
        next_exp = min(future)
        return f"{underlying}{next_exp.strftime('%y%m%d')}{pc}{strike}"

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        spot = await self._get_spot(symbol)
        return {
            "symbol": symbol,
            "current_price": spot,
            "current_vol": 0.20,
            "avg_vol": 0.20,
            "key_levels": [
                {"price": round(spot * 0.90, 2), "type": "support", "strength": 5, "pop": 0.80},
                {"price": round(spot * 1.10, 2), "type": "resistance", "strength": 5, "pop": 0.80},
            ],
            "put_entry_points": [{"price": round(spot * 0.90, 2), "pop": 0.80}],
            "call_entry_points": [{"price": round(spot * 1.10, 2), "pop": 0.80}],
            "samples": 100,
            "period": period,
        }


class BacktestController:
    """Orchestrates strategy ticks over a range of dates, replaying market price changes."""

    def __init__(
        self,
        strategies: List[Any],
        watchlist: List[str],
        ts_engine: Any,
        start_date: datetime.date,
        end_date: datetime.date,
        start_balance: float = 100000.0,
    ):
        self.db = BacktestDatabase()
        self.broker = BacktestBroker(ts_engine, start_balance)
        self.watchlist = watchlist
        self.start_date = start_date
        self.end_date = end_date

        self.mm = MoneyManager(self.broker, self.db, config={})
        self.ic = IronCondorBuilder(self.mm)

        self.strategy_instances = []
        for s_cls in strategies:
            self.strategy_instances.append(s_cls(
                broker=self.broker,
                db=self.db,
                money_manager=self.mm,
                ic_builder=self.ic,
                config={},
            ))

        self.engine = CascadingEngine(
            broker=self.broker,
            db=self.db,
            strategies=self.strategy_instances,
            overseer=None,
            approval_mode=False,
            money_manager=self.mm,
            config={},
            llm_out_of_loop=True,
        )

    async def step(self, current_date: datetime.date) -> Dict[str, int]:
        # Update current date at market close
        self.broker.current_date = datetime.datetime.combine(current_date, datetime.time(16, 0, 0))

        # Check for option expirations / assignment
        # If spot price is below put strike, or above call strike at expiration, assign
        spot_map = {}
        for w in self.watchlist:
            spot_map[w] = await self.broker._get_spot(w)

        closed_contracts = []
        for occ, pos in list(self.broker.virtual_positions.items()):
            m = OCC_RE.match(occ)
            if not m:
                continue
            underlying = m.group(1)
            yymmdd = m.group(2)
            option_type = "put" if m.group(3) == "P" else "call"
            strike = int(m.group(4)) / 1000.0
            expiry_date = datetime.datetime.strptime(yymmdd, "%y%m%d").date()

            if current_date >= expiry_date:
                # Expired! Check ITM.
                spot = spot_map.get(underlying, 100.0)
                is_itm = (option_type == "put" and spot < strike) or (option_type == "call" and spot > strike)
                lots = abs(pos["quantity"])

                if is_itm:
                    # Intrinsic loss
                    loss = abs(spot - strike) * 100.0 * lots
                    # If short (quantity was negative), subtract loss. If long, add cash.
                    if pos["quantity"] < 0:
                        self.broker.balance -= loss
                    else:
                        self.broker.balance += loss

                closed_contracts.append(occ)

        for occ in closed_contracts:
            del self.broker.virtual_positions[occ]

        # Tick engine
        return await self.engine.tick(self.watchlist)

    async def run(self) -> Dict[str, Any]:
        current = self.start_date
        ticks_run = 0
        total_trades_count = 0

        while current <= self.end_date:
            # Skip weekends for simplicity
            if current.weekday() < 5:
                await self.step(current)
                ticks_run += 1
            current += datetime.timedelta(days=1)

        # Calculate final stats
        closed_trades = [t for t in self.db.trades if t["status"] == "CLOSED"]
        total_pnl = sum(float(t["pnl"] or 0.0) for t in closed_trades)
        wins = sum(1 for t in closed_trades if float(t["pnl"] or 0.0) > 0)
        win_rate = wins / len(closed_trades) if closed_trades else 0.0

        return {
            "ticks_run": ticks_run,
            "total_trades": len(self.db.trades),
            "closed_trades": len(closed_trades),
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "final_balance": self.broker.balance,
        }
