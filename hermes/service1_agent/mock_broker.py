import logging
from typing import Any, Dict, List, Optional
from .core import TradeAction
from hermes.broker.base import AbstractBroker
from hermes.broker.models import (
    AccountBalances,
    BrokerPosition,
    BrokerOrder,
    OptionChainLeg,
    MarketQuote,
    OrderPlacementResult,
)

logger = logging.getLogger("hermes.broker.mock")

class MockBroker(AbstractBroker):
    """
    A mock broker for testing and Docker demonstration.
    Returns dummy data for chains, quotes, and analysis.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.current_date = None  # Use system time

    async def get_account_balances(self) -> AccountBalances:
        # account_type matches the field MoneyManager logs and Tradier returns
        # ("margin" / "pdt" / "cash") — without it, debug lines render as None.
        return AccountBalances(
            option_buying_power=100000.0,
            stock_buying_power=100000.0,
            cash=50000.0,
            total_equity=100000.0,
            account_type="margin",
            margin_buying_power=100000.0,
            raw={}
        )

    async def get_positions(self) -> List[BrokerPosition]:
        return []

    async def get_orders(self) -> List[BrokerOrder]:
        # CascadingEngine.tick() → MoneyManager.sync_broker_orders() calls
        # this. Without it the call raised AttributeError, was swallowed by
        # the except clause, and mock-mode broker-side capacity tracking
        # ran on an empty cache forever. Returning [] keeps mock capacity
        # calculations consistent (no resting broker orders) without crashing.
        return []

    async def get_option_expirations(self, symbol: str) -> List[str]:
        from datetime import datetime, timedelta
        today = datetime.utcnow().date()
        return [(today + timedelta(days=d)).strftime("%Y-%m-%d") for d in [7, 14, 21, 28, 45]]

    def _get_symbol_price(self, symbol: str) -> float:
        """Synchronously get a deterministic current price for a symbol."""
        base = 100.0 + (hash(symbol) % 200)
        return round(base, 2)

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        """Generate options chain dynamically centered around underlying spot price."""
        from datetime import datetime
        import math

        spot = self._get_symbol_price(symbol)
        
        # Determine strike spacing based on spot price
        if spot < 25:
            strike_spacing = 1.0
        elif spot < 100:
            strike_spacing = 2.5
        else:
            strike_spacing = 5.0

        # Center strike
        center_strike = round(spot / strike_spacing) * strike_spacing
        
        try:
            exp_date = datetime.strptime(expiry, "%Y-%m-%d")
            occ_expiry = exp_date.strftime("%y%m%d")
        except Exception:
            occ_expiry = "260620"

        ticker_part = symbol.ljust(6)
        legs = []
        strikes = [center_strike + i * strike_spacing for i in range(-6, 7)]
        
        for k in strikes:
            if k <= 0:
                continue
                
            dist = (k - spot) / (0.05 * spot) if spot > 0 else 0
            
            call_delta = round(1.0 / (1.0 + math.exp(dist)), 2)
            put_delta = round(-1.0 / (1.0 + math.exp(-dist)), 2)
            
            call_intrinsic = max(0.0, spot - k)
            put_intrinsic = max(0.0, k - spot)
            
            extrinsic = 0.04 * spot * math.exp(-dist**2 / 2.0)
            
            call_mid = call_intrinsic + extrinsic
            put_mid = put_intrinsic + extrinsic
            
            call_spread = max(0.05, round(call_mid * 0.1, 2))
            put_spread = max(0.05, round(put_mid * 0.1, 2))
            
            call_bid = max(0.01, round(call_mid - call_spread / 2.0, 2))
            call_ask = max(0.02, round(call_mid + call_spread / 2.0, 2))
            
            put_bid = max(0.01, round(put_mid - put_spread / 2.0, 2))
            put_ask = max(0.02, round(put_mid + put_spread / 2.0, 2))

            strike_cents = int(round(k * 1000))
            strike_str = f"{strike_cents:08d}"
            
            call_sym = f"{ticker_part}{occ_expiry}C{strike_str}"
            put_sym = f"{ticker_part}{occ_expiry}P{strike_str}"
            
            legs.append(OptionChainLeg(
                symbol=call_sym,
                option_type="call",
                strike=k,
                bid=call_bid,
                ask=call_ask,
                delta=call_delta
            ))
            legs.append(OptionChainLeg(
                symbol=put_sym,
                option_type="put",
                strike=k,
                bid=put_bid,
                ask=put_ask,
                delta=put_delta
            ))
            
        return legs

    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        from datetime import datetime
        quotes = []
        for s in symbols.split(","):
            sym = s.strip()
            price = self._get_symbol_price(sym)
            bid = round(price - 0.05, 2)
            ask = round(price + 0.05, 2)
            quotes.append(MarketQuote(
                symbol=sym,
                price=price,
                bid=bid,
                ask=ask,
                volume=1000000,
                timestamp=datetime.utcnow().isoformat(),
                last=price
            ))
        return quotes

    async def get_delta(self, option_symbol: str) -> float:
        # standard fallback delta
        return -0.15 if "P" in option_symbol else 0.15

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        """Mirror TradierBroker.analyze_symbol over mock bars so the K-Means
        S/R panel renders in dev/paper mode without real Tradier credentials."""
        import numpy as np
        import pandas as pd
        from hermes.ml.pop_engine import find_key_levels

        bars = await self.get_history(symbol, interval="daily")
        if not bars:
            return {"error": f"no history for {symbol}"}
        df = pd.DataFrame(bars)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df = df.dropna(subset=["close", "volume"])
        if df.empty:
            return {"error": f"invalid history data for {symbol}"}

        # Mock get_history is reverse-chronological (i=0 is today). Sort
        # ascending so recency-weighted clustering treats the latest bars
        # as the most recent.
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)

        current = float(df["close"].iloc[-1])
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

    async def get_history(
        self, symbol: str, *, interval: str = "daily",
        start: Optional[str] = None, end: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Return dummy history data with deterministic per-symbol noise so
        downstream rolling stats (volume z-score, realized vol, beta) are
        well-defined."""
        import random
        from datetime import datetime, timedelta
        out = []
        now = datetime.utcnow()
        is_intraday = interval in ("1min", "5min", "15min")
        if is_intraday:
            # ~6.5 trading hours/day × ~10 days, every 5 minutes — plenty for
            # the FeatureEngineer's last-30-min volume window.
            steps = 78 * 10
            step = timedelta(minutes=5)
        else:
            steps = 400
            step = timedelta(days=1)

        rng = random.Random(hash(symbol) & 0xFFFFFFFF)
        # Anchor base price per symbol so SPY ≠ AAPL ≠ TSLA in mock mode.
        base = 100.0 + (hash(symbol) % 200)
        price = base
        for i in range(steps):
            ts = now - i * step
            drift = rng.gauss(0, 0.012) * price
            price = max(1.0, price + drift)
            open_p = price + rng.gauss(0, 0.5)
            close_p = price + rng.gauss(0, 0.5)
            high_p = max(open_p, close_p) + abs(rng.gauss(0, 0.7))
            low_p = min(open_p, close_p) - abs(rng.gauss(0, 0.7))
            vol = int(800_000 + rng.random() * 600_000)
            out.append({
                "date": ts.strftime("%Y-%m-%d %H:%M:%S" if is_intraday else "%Y-%m-%d"),
                "open": round(open_p, 2),
                "high": round(high_p, 2),
                "low": round(low_p, 2),
                "close": round(close_p, 2),
                "volume": vol,
                "vwap": round((open_p + high_p + low_p + close_p) / 4, 2),
            })
        return out

    async def place_order_from_action(self, action: TradeAction) -> OrderPlacementResult:
        logger.info("[MOCK] Placing order for %s: %s", action.symbol, action.legs)
        
        simulated_net_price = 0.0
        for leg in action.legs:
            opt_symbol = leg.get("option_symbol", "")
            leg_side = leg.get("side", "buy")
            qty = leg.get("quantity", 1)
            
            # Deterministic mid price and spread based on option symbol
            h = hash(opt_symbol) & 0xFFFFFFFF
            leg_mid = 0.5 + (h % 450) / 100.0
            leg_spread = max(0.05, round(leg_mid * 0.1, 2))
            
            if leg_side == "buy":
                # Buy filled slightly above mid (slippage)
                slippage = (h % 3) * 0.1 * leg_spread
                fill_price = round(leg_mid + slippage, 2)
                simulated_net_price -= fill_price * qty
            else:
                # Sell filled slightly below mid (slippage)
                slippage = (h % 3) * 0.1 * leg_spread
                fill_price = round(leg_mid - slippage, 2)
                simulated_net_price += fill_price * qty
                
        simulated_net_price = round(simulated_net_price, 2)
        fill_status = "ok"
        
        # Determine if limit price is fillable
        if action.order_type == "credit" or (action.side == "sell" and action.price is not None):
            if action.price is not None and simulated_net_price < action.price:
                logger.info("[MOCK] Order rejected: credit limit price %s not met by simulated net price %s", action.price, simulated_net_price)
                fill_status = "rejected"
        elif action.order_type == "debit" or (action.side == "buy" and action.price is not None):
            if action.price is not None and -simulated_net_price > action.price:
                logger.info("[MOCK] Order rejected: debit limit price %s not met by simulated net price %s", action.price, -simulated_net_price)
                fill_status = "rejected"
                
        return OrderPlacementResult(
            order_id="MOCK-123",
            status=fill_status,
            raw_response={
                "status": fill_status,
                "order_id": "MOCK-123",
                "simulated_net_price": simulated_net_price
            }
        )

    async def roll_to_next_month(self, option_symbol: str) -> str:
        return option_symbol + "_ROLLED"

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return {"status": "ok", "id": order_id}

    async def close(self) -> None:
        pass

class MockLLM:
    def chat(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        return '{"verdict": "APPROVE", "rationale": "Mock approval"}'
