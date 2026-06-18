from __future__ import annotations
import logging
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from hermes.common import OCC_RE
from hermes.broker.base import AbstractBroker
from hermes.broker.models import (
    AccountBalances,
    BrokerPosition,
    BrokerOrder,
    OptionChainLeg,
    MarketQuote,
    OrderPlacementResult,
)

logger = logging.getLogger("hermes.broker.mock_engine")

class MockAsyncTradierBroker(AbstractBroker):
    """
    An in-memory simulated matching engine that satisfies the TradierBroker interface.
    Exposes simulated ticks to execute limit/market orders with simulated slippage.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.dry_run = False
        self.current_date: Optional[datetime] = None
        self.balances = {
            "option_buying_power": 100000.0,
            "stock_buying_power": 100000.0,
            "cash": 100000.0,
            "total_equity": 100000.0,
            "account_type": "margin",
        }
        self.positions: List[Dict[str, Any]] = []
        self.orders: List[Dict[str, Any]] = []
        
        # Symbol quotes cache: map of symbol -> quote dict
        self.quotes: Dict[str, Dict[str, Any]] = {}
        # Symbol historical bar cache: map of symbol -> list of bar dicts
        self.history: Dict[str, List[Dict[str, Any]]] = {}

        # Tracking variables
        self.commission_per_contract = float(self.config.get("commission_per_contract", 0.35))
        self.slippage_pct = float(self.config.get("slippage_pct", 0.05))

    async def get_account_balances(self) -> AccountBalances:
        return AccountBalances(
            option_buying_power=float(self.balances.get("option_buying_power", 0.0)),
            stock_buying_power=float(self.balances.get("stock_buying_power", 0.0)),
            total_equity=float(self.balances.get("total_equity", 0.0)),
            cash=float(self.balances.get("cash", 0.0)),
            account_type=self.balances.get("account_type", "margin"),
            margin_buying_power=float(self.balances.get("margin_buying_power", 0.0)),
            raw=self.balances
        )

    async def get_positions(self) -> List[BrokerPosition]:
        return [
            BrokerPosition(
                symbol=p.get("symbol", ""),
                quantity=float(p.get("quantity", 0.0)),
                cost_basis=float(p.get("cost_basis", 0.0)),
                date_acquired=p.get("date_acquired", "")
            )
            for p in self.positions
        ]

    async def get_orders(self) -> List[BrokerOrder]:
        orders_list = []
        for o in self.orders:
            orders_list.append(
                BrokerOrder(
                    order_id=str(o.get("id") or o.get("order_id") or ""),
                    symbol=str(o.get("symbol", "")),
                    status=str(o.get("status", "")),
                    quantity=int(o.get("quantity", 1) or 1),
                    price=float(o.get("price") or o.get("avg_fill_price") or 0.0),
                    side=str(o.get("side", "")),
                    tag=str(o.get("tag", "")),
                    legs=o.get("leg") or [],
                    option_symbol=o.get("option_symbol"),
                    **o
                )
            )
        return orders_list

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        for o in self.orders:
            if str(o.get("id")) == str(order_id):
                if o["status"] in ("open", "pending"):
                    o["status"] = "canceled"
                    return {"status": "ok", "order_id": order_id}
                return {"status": "error", "message": f"Order already in status {o['status']}"}
        return {"status": "error", "message": "Order not found"}

    async def get_option_expirations(self, symbol: str) -> List[str]:
        ref = self.current_date or datetime.utcnow()
        ref_date = ref.date()
        expirations = []
        for i in range(1, 10):
            days_to_friday = (4 - ref_date.weekday()) % 7
            if days_to_friday == 0:
                days_to_friday = 7
            friday = ref_date + timedelta(days=days_to_friday + (i - 1) * 7)
            expirations.append(friday.strftime("%Y-%m-%d"))
        return expirations

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        quote = await self.get_quote(symbol)
        underlying_price = float(quote[0]["price"]) if quote else 100.0
        
        center_strike = round(underlying_price)
        strikes = range(center_strike - 20, center_strike + 20, 5)
        
        exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
        yymmdd = exp_date.strftime("%y%m%d")
        
        chain = []
        for strike in strikes:
            strike_str = f"{int(strike * 1000):08d}"
            
            # Put option
            put_symbol = f"{symbol}{yymmdd}P{strike_str}"
            put_val = self._sim_option_value(underlying_price, strike, "P")
            chain.append(
                OptionChainLeg(
                    symbol=put_symbol,
                    option_type="put",
                    strike=float(strike),
                    bid=max(0.01, round(put_val - 0.05, 2)),
                    ask=max(0.02, round(put_val + 0.05, 2)),
                    delta=-self._sim_delta(underlying_price, strike, "P"),
                    greeks={"delta": -self._sim_delta(underlying_price, strike, "P")},
                    underlying=symbol,
                    expiration=expiry
                )
            )
            
            # Call option
            call_symbol = f"{symbol}{yymmdd}C{strike_str}"
            call_val = self._sim_option_value(underlying_price, strike, "C")
            chain.append(
                OptionChainLeg(
                    symbol=call_symbol,
                    option_type="call",
                    strike=float(strike),
                    bid=max(0.01, round(call_val - 0.05, 2)),
                    ask=max(0.02, round(call_val + 0.05, 2)),
                    delta=self._sim_delta(underlying_price, strike, "C"),
                    greeks={"delta": self._sim_delta(underlying_price, strike, "C")},
                    underlying=symbol,
                    expiration=expiry
                )
            )
            
        return chain

    def _sim_option_value(self, spot: float, strike: float, option_type: str) -> float:
        intrinsic = max(0.0, strike - spot) if option_type == "P" else max(0.0, spot - strike)
        time_val = 2.0 / (1.0 + 0.1 * abs(spot - strike))
        return round(intrinsic + time_val, 2)

    def _sim_delta(self, spot: float, strike: float, option_type: str) -> float:
        diff = spot - strike
        try:
            val = 1.0 / (1.0 + math.exp(-diff * 0.1))
        except Exception:
            val = 0.5
        if option_type == "P":
            return round(1.0 - val, 2)
        return round(val, 2)

    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        res = []
        for sym in symbols.split(","):
            sym = sym.strip()
            m = OCC_RE.match(sym)
            if m:
                underlying = m.group(1)
                strike = int(m.group(4)) / 1000.0
                option_type = m.group(3)
                
                und_quote = self.quotes.get(underlying, {"last": 100.0})
                spot = float(und_quote.get("last") or und_quote.get("price") or 100.0)
                val = self._sim_option_value(spot, strike, option_type)
                res.append(
                    MarketQuote(
                        symbol=sym,
                        price=val,
                        bid=max(0.01, round(val - 0.05, 2)),
                        ask=max(0.02, round(val + 0.05, 2)),
                        volume=1000,
                        timestamp=datetime.utcnow().isoformat(),
                        greeks={"delta": -self._sim_delta(spot, strike, option_type) if option_type == "P" else self._sim_delta(spot, strike, option_type)},
                        last=val
                    )
                )
            elif sym in self.quotes:
                q = self.quotes[sym]
                price = float(q.get("last") or q.get("price") or 0.0)
                res.append(
                    MarketQuote(
                        symbol=sym,
                        price=price,
                        bid=float(q.get("bid", 0.0) or 0.0),
                        ask=float(q.get("ask", 0.0) or 0.0),
                        volume=int(q.get("volume", 0) or 0),
                        timestamp=str(q.get("timestamp") or ""),
                        **q
                    )
                )
            else:
                res.append(
                    MarketQuote(
                        symbol=sym,
                        price=100.0,
                        bid=99.95,
                        ask=100.05,
                        volume=1000000,
                        timestamp=datetime.utcnow().isoformat(),
                        last=100.0
                    )
                )
        return res

    async def get_delta(self, option_symbol: str) -> float:
        quotes = await self.get_quote(option_symbol)
        if not quotes:
            return 0.0
        greeks = quotes[0].get("greeks") or {}
        return float(greeks.get("delta") or 0.0)

    async def get_history(self, symbol: str, *, interval: str = "daily",
                          start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.history.get(symbol, [])

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        bars = self.history.get(symbol, [])
        if not bars:
            return {"error": f"no history for {symbol}"}
            
        import pandas as pd
        import numpy as np
        from hermes.ml.pop_engine import find_key_levels
        
        df = pd.DataFrame(bars)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df = df.dropna(subset=["close", "volume"])
        if df.empty:
            return {"error": f"invalid history data for {symbol}"}
            
        current = float(df["close"].iloc[-1])
        key_levels = find_key_levels(df["close"], df["volume"], window=5, n_clusters=6)
        
        log_ret = np.log(df["close"] / df["close"].shift(1)).dropna()
        realized_vol = float(log_ret.iloc[-21:].std() * np.sqrt(252)) if len(log_ret) >= 21 else 0.0
        avg_vol = float(log_ret.std() * np.sqrt(252)) if len(log_ret) >= 2 else 0.0
        
        put_entries = [lvl for lvl in key_levels if lvl.get("type") == "support"]
        call_entries = [lvl for lvl in key_levels if lvl.get("type") == "resistance"]
        put_entries.sort(key=lambda x: abs(x["price"] - current))
        call_entries.sort(key=lambda x: abs(x["price"] - current))
        
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
        }

    async def place_order_from_action(self, action) -> OrderPlacementResult:
        order_id = f"MOCK-{len(self.orders) + 1}"
        legs = action.legs or []
        order_data = {
            "id": order_id,
            "symbol": action.symbol,
            "class": action.order_class,
            "type": action.order_type,
            "price": action.price,
            "side": action.side,
            "quantity": action.quantity,
            "status": "open",
            "tag": action.tag,
            "leg": legs
        }
        self.orders.append(order_data)
        
        if action.order_type == "market" or action.price is None:
            await self._fill_order(order_data)
        else:
            await self._check_order_fill_immediate(order_data)
            
        return OrderPlacementResult(order_id=order_id, status="ok", raw_response={"status": "ok", "order_id": order_id})

    async def _check_order_fill_immediate(self, order):
        await self._fill_order(order)

    async def _fill_order(self, order):
        order["status"] = "filled"
        class_type = order.get("class")
        qty = order.get("quantity", 1)
        price = order.get("price") or 0.0
        side = order.get("side", "buy").lower()
        multiplier = 100 if class_type in ("option", "multileg") else 1
        
        num_legs = len(order.get("leg") or []) if order.get("leg") else 1
        commissions = self.commission_per_contract * num_legs * qty * 2
        
        slippage = abs(price) * self.slippage_pct * qty
        
        fill_price = price
        if side == "buy":
            self.balances["cash"] -= (fill_price * qty * multiplier) + commissions + (slippage * multiplier)
        else:
            self.balances["cash"] += (fill_price * qty * multiplier) - commissions - (slippage * multiplier)
            
        if class_type == "equity":
            symbol = order.get("symbol")
            pos = next((p for p in self.positions if p["symbol"] == symbol), None)
            if not pos:
                pos = {"symbol": symbol, "quantity": 0.0, "cost_basis": fill_price}
                self.positions.append(pos)
            change = qty if side == "buy" else -qty
            pos["quantity"] += change
            if pos["quantity"] == 0:
                self.positions.remove(pos)
        else:
            for leg in order.get("leg", []):
                leg_sym = leg["option_symbol"]
                leg_qty = leg.get("quantity", qty)
                leg_side = leg.get("side", "buy_to_open").lower()
                
                pos = next((p for p in self.positions if p["symbol"] == leg_sym), None)
                if not pos:
                    pos = {"symbol": leg_sym, "quantity": 0.0, "cost_basis": fill_price}
                    self.positions.append(pos)
                
                if "to_open" in leg_side:
                    change = leg_qty if "buy" in leg_side else -leg_qty
                else:
                    change = -leg_qty if "buy" in leg_side else leg_qty
                pos["quantity"] += change
                if pos["quantity"] == 0:
                    self.positions.remove(pos)

    async def roll_to_next_month(self, option_symbol: str) -> str:
        m = OCC_RE.match(option_symbol or "")
        if not m:
            return option_symbol + "_ROLLED"
        underlying, yymmdd, pc, strike = m.groups()
        current_exp = datetime.strptime(yymmdd, "%y%m%d").date()
        next_exp = current_exp + timedelta(days=30)
        return f"{underlying}{next_exp.strftime('%y%m%d')}{pc}{strike}"

    async def close(self) -> None:
        pass

    def tick_underlying(self, symbol: str, spot: float, high: float, low: float, dt: datetime):
        self.current_date = dt
        self.quotes[symbol] = {
            "symbol": symbol,
            "last": spot,
            "bid": spot - 0.05,
            "ask": spot + 0.05,
            "high": high,
            "low": low
        }
        
        # Check open positions to simulate touch / exercise
        for pos in list(self.positions):
            leg_sym = pos["symbol"]
            m = OCC_RE.match(leg_sym)
            if not m:
                continue
            underlying = m.group(1)
            if underlying != symbol:
                continue
            
            strike = int(m.group(4)) / 1000.0
            option_type = m.group(3)
            quantity = pos["quantity"]
            
            if quantity < 0:
                touched = False
                if option_type == "P" and low <= strike:
                    touched = True
                elif option_type == "C" and high >= strike:
                    touched = True
                    
                if touched:
                    # Simulate touch event closure
                    # spread width: 5.0
                    width = 5.0
                    multiplier = 100
                    # Deduct the width representing the spread max loss
                    self.balances["cash"] -= (width * abs(quantity) * multiplier)
                    self.positions = [p for p in self.positions if p["symbol"] != leg_sym]
                    
                    # Remove matching long leg
                    long_type = "P" if option_type == "P" else "C"
                    for other in list(self.positions):
                        om = OCC_RE.match(other["symbol"])
                        if om and om.group(1) == symbol and om.group(3) == long_type and other["quantity"] > 0:
                            self.positions.remove(other)
                            break
