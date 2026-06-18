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

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        # Return some dummy legs
        return [
            OptionChainLeg(symbol=f"{symbol}230519P00150000", option_type="put", strike=150.0, bid=1.5, ask=1.6, delta=-0.3),
            OptionChainLeg(symbol=f"{symbol}230519P00145000", option_type="put", strike=145.0, bid=0.5, ask=0.6, delta=-0.1),
            OptionChainLeg(symbol=f"{symbol}230519C00160000", option_type="call", strike=160.0, bid=1.5, ask=1.6, delta=0.3),
            OptionChainLeg(symbol=f"{symbol}230519C00165000", option_type="call", strike=165.0, bid=0.5, ask=0.6, delta=0.1),
        ]

    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        from datetime import datetime
        return [
            MarketQuote(
                symbol=s.strip(),
                price=155.0,
                bid=154.9,
                ask=155.1,
                volume=1000000,
                timestamp=datetime.utcnow().isoformat(),
                last=155.0
            )
            for s in symbols.split(",")
        ]

    async def get_delta(self, option_symbol: str) -> float:
        return 0.15

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        """Mirror TradierBroker.analyze_symbol over mock bars so the K-Means
        S/R panel renders in dev/paper mode without real Tradier credentials."""
        import numpy as np
        import pandas as pd
        from hermes.ml.pop_engine import find_key_levels

        bars = self.get_history(symbol, interval="daily")
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

    def get_history(self, symbol: str, interval: str = "daily",
                    start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
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
        return OrderPlacementResult(
            order_id="MOCK-123",
            status="ok",
            raw_response={"status": "ok", "order_id": "MOCK-123"}
        )

    async def roll_to_next_month(self, option_symbol: str) -> str:
        return option_symbol + "_ROLLED"

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return {"status": "ok", "id": order_id}

    async def get_history(
        self, symbol: str, *, interval: str = "daily",
        start: Optional[str] = None, end: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return []

    async def close(self) -> None:
        pass

class MockLLM:
    def chat(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        return '{"verdict": "APPROVE", "rationale": "Mock approval"}'
