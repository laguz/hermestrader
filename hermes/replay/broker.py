"""Data-driven simulated broker for historical replay.

``ReplayBroker`` extends :class:`~hermes.service1_agent.mock_broker.MockBroker`
— reusing its fill-acceptance machinery via the ``_leg_quote`` /
``_leg_slippage`` hooks — but prices everything from a
:class:`~hermes.replay.data.ReplayDataSource` at the current simulated instant:

* underlying quotes come from historical bars (lookahead-safe),
* option chains/quotes are Black-Scholes priced off trailing realized vol,
* fills mutate an in-broker position/cash book, so ``get_positions`` /
  ``get_orders`` / ``get_account_balances`` reflect the replayed account, and
* expired legs cash-settle at intrinsic value on the expiry-day close.

It is constructed from a data source only — it cannot hold Tradier
credentials and never opens a network connection.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from hermes.common import OCC_RE
from hermes.greeks import black_scholes_greeks, black_scholes_price
from hermes.market_hours import ET
from hermes.service1_agent.mock_broker import MockBroker
from hermes.broker.models import MarketQuote, OptionChainLeg, OrderPlacementResult

from .data import ReplayDataSource

logger = logging.getLogger("hermes.replay.broker")

_MAX_ORDER_HISTORY = 200


def _parse_occ(symbol: str) -> Optional[Dict[str, Any]]:
    m = OCC_RE.match(str(symbol or ""))
    if not m:
        return None
    underlying, yymmdd, pc, strike_str = m.groups()
    try:
        expiry = datetime.strptime(yymmdd, "%y%m%d").date()
    except ValueError:
        return None
    return {
        "underlying": underlying,
        "expiry": expiry,
        "option_type": "put" if pc == "P" else "call",
        "strike": int(strike_str) / 1000.0,
    }


class ReplayBroker(MockBroker):
    """MockBroker whose prices, fills and account state replay history."""

    def __init__(self, data: ReplayDataSource, config: Optional[Dict[str, Any]] = None,
                 *, starting_bp: float = 100_000.0, risk_free: float = 0.05,
                 spread_pct: float = 0.04, slippage_frac: float = 0.0,
                 vol_floor: float = 0.10):
        super().__init__(config or {})
        self.data = data
        self.current_date: Optional[datetime] = None   # naive UTC, set via set_time
        self.risk_free = risk_free
        self.spread_pct = spread_pct
        self.slippage_frac = slippage_frac
        self.vol_floor = vol_floor

        self.starting_bp = float(starting_bp)
        self.cash = float(starting_bp)
        # occ/equity symbol → signed contract/share quantity
        self._position_qty: Dict[str, int] = {}
        self._orders: List[Dict[str, Any]] = []
        self._order_seq = 0
        # order_id → margin dollars reserved while its short legs stay open
        self._margin: Dict[str, Dict[str, Any]] = {}
        self.fills: List[Dict[str, Any]] = []
        self.settlements: List[Dict[str, Any]] = []

    # ── simulated time ───────────────────────────────────────────────────────
    def set_time(self, sim_dt: datetime) -> None:
        if sim_dt.tzinfo is not None:
            sim_dt = sim_dt.astimezone(timezone.utc).replace(tzinfo=None)
        self.current_date = sim_dt

    def _now(self) -> datetime:
        if self.current_date is None:
            raise RuntimeError("ReplayBroker.set_time was never called")
        return self.current_date

    def _today_et(self) -> date:
        return self._now().replace(tzinfo=timezone.utc).astimezone(ET).date()

    # ── pricing ──────────────────────────────────────────────────────────────
    def _spot(self, symbol: str) -> Optional[float]:
        return self.data.spot(symbol, self._now())

    def _sigma(self, symbol: str) -> float:
        """Annualized trailing-21-bar realized vol (deterministic, floored)."""
        bars = self.data.completed_daily(symbol, self._now())
        closes = bars["close"].dropna() if not bars.empty else pd.Series(dtype=float)
        if len(closes) < 5:
            return 0.30
        log_ret = np.log(closes / closes.shift(1)).dropna().iloc[-21:]
        if log_ret.empty:
            return 0.30
        vol = float(log_ret.std() * math.sqrt(252))
        if not math.isfinite(vol):
            return 0.30
        return max(self.vol_floor, vol)

    def _tte_years(self, expiry: date) -> float:
        # Options expire at the 16:00 ET close on expiry day.
        exp_dt = datetime.combine(expiry, dt_time(16, 0), tzinfo=ET).astimezone(
            timezone.utc).replace(tzinfo=None)
        return max(0.0, (exp_dt - self._now()).total_seconds() / (365.0 * 86400.0))

    def _quote_option(self, occ_symbol: str) -> Optional[Dict[str, Any]]:
        """Deterministic {bid, ask, delta, mid_iv} for one OCC symbol.

        bid/ask are exact cent multiples with an even-cent spread, so every
        mid computed downstream ((bid+ask)/2, short_credit, close debits and
        the fill engine's ``_leg_quote``) lands on exact cents — no float
        boundary flakiness between "the credit the strategy asked for" and
        "the credit the fill engine grants".
        """
        info = _parse_occ(occ_symbol)
        if info is None:
            return None
        spot = self._spot(info["underlying"])
        if spot is None or spot <= 0:
            return None
        sigma = self._sigma(info["underlying"])
        t = self._tte_years(info["expiry"])
        mid = black_scholes_price(spot, info["strike"], t, self.risk_free,
                                  sigma, info["option_type"])
        greeks = black_scholes_greeks(spot, info["strike"], t, self.risk_free,
                                      sigma, info["option_type"])
        mid_cents = max(1, int(round(mid * 100)))
        half_cents = max(1, int(round(mid_cents * self.spread_pct / 2)))
        bid_cents = max(1, mid_cents - half_cents)
        ask_cents = bid_cents + 2 * half_cents
        return {
            "symbol": occ_symbol,
            "bid": bid_cents / 100.0,
            "ask": ask_cents / 100.0,
            "delta": float(greeks.get("delta", 0.0)),
            "mid_iv": sigma,
        }

    # ── MockBroker fill hooks (reused fill machinery, replay pricing) ────────
    def _leg_quote(self, opt_symbol: str) -> tuple:
        q = self._quote_option(opt_symbol)
        if q is None:
            # Unpriceable leg → zero value; credit orders against it reject.
            return 0.0, 0.0
        mid = (q["bid"] + q["ask"]) / 2.0
        return mid, q["ask"] - q["bid"]

    def _leg_slippage(self, opt_symbol: str, spread: float) -> float:
        return self.slippage_frac * spread

    # ── market data surface ──────────────────────────────────────────────────
    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        quotes: List[MarketQuote] = []
        ts = self._now().isoformat()
        for raw in symbols.split(","):
            sym = raw.strip()
            if not sym:
                continue
            occ = self._quote_option(sym)
            if occ is not None:
                price = round((occ["bid"] + occ["ask"]) / 2.0, 4)
                quotes.append(MarketQuote(symbol=sym, price=price, bid=occ["bid"],
                                          ask=occ["ask"], volume=0,
                                          timestamp=ts, last=price))
                continue
            spot = self._spot(sym)
            if spot is None:
                continue
            quotes.append(MarketQuote(symbol=sym, price=round(spot, 4),
                                      bid=round(spot - 0.02, 4),
                                      ask=round(spot + 0.02, 4),
                                      volume=0, timestamp=ts, last=round(spot, 4)))
        return quotes

    async def get_delta(self, option_symbol: str) -> float:
        q = self._quote_option(option_symbol)
        if q is None:
            return await super().get_delta(option_symbol)
        return q["delta"]

    async def get_option_expirations(self, symbol: str) -> List[str]:
        """Today (0DTE), each weekday out to 10 days, and Fridays out to ~75."""
        today = self._today_et()
        out: List[date] = []
        for i in range(0, 11):
            d = today + timedelta(days=i)
            if d.weekday() < 5:
                out.append(d)
        d = today
        while (d - today).days <= 75:
            if d.weekday() == 4 and d not in out:
                out.append(d)
            d += timedelta(days=1)
        return [x.strftime("%Y-%m-%d") for x in sorted(set(out))]

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        spot = self._spot(symbol)
        if spot is None or spot <= 0:
            return []
        if spot < 25:
            spacing = 1.0
        elif spot < 100:
            spacing = 2.5
        else:
            spacing = 5.0
        try:
            exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
        except ValueError:
            return []
        yymmdd = exp_date.strftime("%y%m%d")
        center = round(spot / spacing) * spacing
        legs: List[OptionChainLeg] = []
        for i in range(-40, 41):
            strike = center + i * spacing
            if strike <= 0:
                continue
            strike_str = f"{int(round(strike * 1000)):08d}"
            for pc, otype in (("P", "put"), ("C", "call")):
                occ = f"{symbol}{yymmdd}{pc}{strike_str}"
                q = self._quote_option(occ)
                if q is None:
                    continue
                legs.append(OptionChainLeg(
                    symbol=occ, option_type=otype, strike=float(strike),
                    bid=q["bid"], ask=q["ask"], delta=q["delta"],
                    greeks={"delta": q["delta"], "mid_iv": q["mid_iv"]},
                ))
        return legs

    async def get_history(self, symbol: str, *, interval: str = "daily",
                          start: Optional[str] = None,
                          end: Optional[str] = None) -> List[Dict[str, Any]]:
        bars = self.data.completed_daily(symbol, self._now())
        today_bar = self.data.today_bar(symbol, self._now())
        rows: List[Dict[str, Any]] = []
        for ts, r in bars.iterrows():
            rows.append({
                "date": ts.strftime("%Y-%m-%d"),
                "open": float(r["open"]) if pd.notna(r.get("open")) else None,
                "high": float(r["high"]) if pd.notna(r.get("high")) else None,
                "low": float(r["low"]) if pd.notna(r.get("low")) else None,
                "close": float(r["close"]) if pd.notna(r.get("close")) else None,
                "volume": int(r["volume"]) if pd.notna(r.get("volume")) else 0,
            })
        if today_bar is not None and pd.notna(today_bar.get("open")):
            # The forming bar: only its open is knowable intraday.
            rows.append({
                "date": self._today_et().strftime("%Y-%m-%d"),
                "open": float(today_bar["open"]),
                "high": float(today_bar["open"]),
                "low": float(today_bar["open"]),
                "close": float(self._spot(symbol) or today_bar["open"]),
                "volume": 0,
            })
        if start:
            rows = [r for r in rows if r["date"] >= start[:10]]
        if end:
            rows = [r for r in rows if r["date"] <= end[:10]]
        return rows

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        """Mirror TradierBroker.analyze_symbol over historical bars, cut at sim time."""
        from hermes.ml.pop_engine import find_key_levels, wilder_atr

        lookback = {"3m": 63, "6m": 126, "1y": 252}.get(period.lower(), 126)
        completed = self.data.completed_daily(symbol, self._now()).tail(lookback)
        if completed.empty or completed["close"].dropna().empty:
            return {"error": f"no history for {symbol}"}
        df = completed.reset_index().rename(columns={"ts": "date"})
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        df = df.dropna(subset=["close"])
        if df.empty:
            return {"error": f"invalid history data for {symbol}"}

        current = float(self._spot(symbol) or df["close"].iloc[-1])
        key_levels = find_key_levels(df["close"], df["volume"], window=5, n_clusters=6)

        log_ret = np.log(df["close"] / df["close"].shift(1)).dropna()
        realized_vol = float(log_ret.iloc[-21:].std() * np.sqrt(252)) if len(log_ret) >= 21 else 0.0
        avg_vol = float(log_ret.std() * np.sqrt(252)) if len(log_ret) >= 2 else 0.0
        if not np.isfinite(realized_vol):
            realized_vol = 0.0
        if not np.isfinite(avg_vol):
            avg_vol = 0.0

        put_entries = [lvl for lvl in key_levels if lvl.get("type") == "support"]
        call_entries = [lvl for lvl in key_levels if lvl.get("type") == "resistance"]
        put_entries.sort(key=lambda x: abs(x["price"] - current))
        call_entries.sort(key=lambda x: abs(x["price"] - current))

        today_bar = self.data.today_bar(symbol, self._now())
        today_open = None
        if today_bar is not None and pd.notna(today_bar.get("open")) and float(today_bar["open"]) > 0:
            today_open = float(today_bar["open"])
        atr = wilder_atr(df, period=14)

        return {
            "symbol": symbol,
            "current_price": current,
            "current_vol": realized_vol,
            "avg_vol": avg_vol,
            "key_levels": key_levels,
            "put_entry_points": put_entries,
            "call_entry_points": call_entries,
            "samples": len(df),
            "period": period,
            "atr": atr,
            "atr_period": 14,
            "today_open": today_open,
        }

    # ── account surface ───────────────────────────────────────────────────────
    async def get_positions(self) -> List[Dict[str, Any]]:
        return [{"symbol": sym, "quantity": qty, "cost_basis": 0.0}
                for sym, qty in self._position_qty.items() if qty != 0]

    async def get_orders(self) -> List[Dict[str, Any]]:
        return list(self._orders)

    def _margin_in_use(self) -> float:
        return sum(m["amount"] for m in self._margin.values())

    async def get_account_balances(self) -> Dict[str, Any]:
        obp = max(0.0, self.cash - self._margin_in_use())
        return {
            "option_buying_power": obp,
            "stock_buying_power": obp,
            "cash": self.cash,
            "total_equity": self.cash,
            "account_type": "margin",
            "margin_buying_power": obp,
        }

    # ── fills ─────────────────────────────────────────────────────────────────
    async def place_order_from_action(self, action) -> OrderPlacementResult:
        result = await super().place_order_from_action(action)
        self._order_seq += 1
        order_id = f"SIM-{self._order_seq}"
        status = result["status"]
        net = float(result["raw_response"].get("simulated_net_price", 0.0))
        if status != "ok":
            return OrderPlacementResult(order_id=order_id, status="rejected",
                                        raw_response=result["raw_response"])

        # Apply the fill: positions, cash, order history, margin, ledger.
        opens = 0
        for leg in (action.legs or []):
            occ = leg.get("option_symbol", "")
            qty = int(leg.get("quantity", 1) or 1)
            side = (leg.get("side") or "buy").lower()
            signed = qty if "buy" in side else -qty
            self._position_qty[occ] = self._position_qty.get(occ, 0) + signed
            if "open" in side:
                opens += 1
        self.cash += net * 100.0

        strikes = [s["strike"] for s in
                   (_parse_occ(leg.get("option_symbol", "")) for leg in (action.legs or []))
                   if s is not None]
        if opens and (action.order_type or "").lower() == "credit":
            lots = max((int(leg.get("quantity", 1) or 1) for leg in action.legs), default=1)
            if len(strikes) >= 2:
                width = abs(strikes[0] - strikes[1])
                margin = width * 100.0 * lots
            elif len(strikes) == 1:
                margin = strikes[0] * 100.0 * lots     # cash-secured single short
            else:
                margin = 0.0
            if margin > 0:
                self._margin[order_id] = {
                    "amount": margin,
                    "short_legs": [leg.get("option_symbol", "") for leg in action.legs
                                   if "sell" in (leg.get("side") or "").lower()],
                }

        order_row = {
            "id": order_id,
            "status": "filled",
            "tag": getattr(action, "tag", None),
            "symbol": action.symbol,
            "side": action.side,
            "quantity": action.quantity,
            "price": action.price,
            "avg_fill_price": abs(net),
            "leg": [dict(leg) for leg in (action.legs or [])],
        }
        self._orders.append(order_row)
        if len(self._orders) > _MAX_ORDER_HISTORY:
            self._orders = self._orders[-_MAX_ORDER_HISTORY:]

        self.fills.append({
            "ts": self._now(),
            "order_id": order_id,
            "strategy_id": action.strategy_id,
            "symbol": action.symbol,
            "tag": getattr(action, "tag", None),
            "net": net,
            "price": action.price,
            "order_type": action.order_type,
            "pop": (action.strategy_params or {}).get("pop"),
            "legs": [dict(leg) for leg in (action.legs or [])],
        })
        self._release_flat_margin()
        return OrderPlacementResult(order_id=order_id, status="filled",
                                    raw_response=result["raw_response"])

    def _release_flat_margin(self) -> None:
        for oid in list(self._margin):
            shorts = self._margin[oid]["short_legs"]
            if all(self._position_qty.get(s, 0) == 0 for s in shorts):
                del self._margin[oid]

    # ── expiry settlement ─────────────────────────────────────────────────────
    def settle_expired(self) -> List[Dict[str, Any]]:
        """Cash-settle every leg whose expiry is before the current ET date.

        Uses the expiry-day close (already historical at this point, so no
        lookahead). Returns the settlement records appended this call.
        """
        today = self._today_et()
        new_records: List[Dict[str, Any]] = []
        for occ, qty in list(self._position_qty.items()):
            if qty == 0:
                continue
            info = _parse_occ(occ)
            if info is None or info["expiry"] >= today:
                continue
            spot = self.data.close_on(info["underlying"], info["expiry"])
            if spot is None:
                spot = self.data.close_on(info["underlying"], today) or 0.0
            if info["option_type"] == "call":
                intrinsic = max(0.0, spot - info["strike"])
            else:
                intrinsic = max(0.0, info["strike"] - spot)
            cash_delta = qty * intrinsic * 100.0
            self.cash += cash_delta
            rec = {
                "ts": self._now(),
                "occ": occ,
                "qty": qty,
                "expiry": info["expiry"],
                "settle_spot": spot,
                "intrinsic": round(intrinsic, 4),
                "cash_delta": round(cash_delta, 2),
            }
            self.settlements.append(rec)
            new_records.append(rec)
            self._position_qty[occ] = 0
        self._release_flat_margin()
        return new_records
