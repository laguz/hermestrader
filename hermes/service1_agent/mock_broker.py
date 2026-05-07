import logging
import time
from typing import Any, Dict, List, Optional
from .core import TradeAction

logger = logging.getLogger("hermes.broker.mock")

class MockBroker:
    """
    A mock broker for testing and Docker demonstration.
    Returns dummy data for chains, quotes, and analysis.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.current_date = None  # Use system time

    def get_account_balances(self) -> Dict[str, Any]:
        # account_type matches the field MoneyManager logs and Tradier returns
        # ("margin" / "pdt" / "cash") — without it, debug lines render as None.
        return {
            "option_buying_power": 100000.0,
            "stock_buying_power": 100000.0,
            "cash": 50000.0,
            "total_equity": 100000.0,
            "account_type": "margin",
        }

    def get_positions(self) -> List[Dict[str, Any]]:
        return []

    def get_orders(self) -> List[Dict[str, Any]]:
        # CascadingEngine.tick() → MoneyManager.sync_broker_orders() calls
        # this. Without it the call raised AttributeError, was swallowed by
        # the except clause, and mock-mode broker-side capacity tracking
        # ran on an empty cache forever. Returning [] keeps mock capacity
        # calculations consistent (no resting broker orders) without crashing.
        return []

    def get_option_expirations(self, symbol: str) -> List[str]:
        from datetime import datetime, timedelta
        today = datetime.utcnow().date()
        return [(today + timedelta(days=d)).strftime("%Y-%m-%d") for d in [7, 14, 21, 28, 45]]

    def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        # Return some dummy legs
        return [
            {"symbol": f"{symbol}230519P00150000", "option_type": "put", "strike": 150.0, "bid": 1.5, "ask": 1.6, "greeks": {"delta": -0.3}},
            {"symbol": f"{symbol}230519P00145000", "option_type": "put", "strike": 145.0, "bid": 0.5, "ask": 0.6, "greeks": {"delta": -0.1}},
            {"symbol": f"{symbol}230519C00160000", "option_type": "call", "strike": 160.0, "bid": 1.5, "ask": 1.6, "greeks": {"delta": 0.3}},
            {"symbol": f"{symbol}230519C00165000", "option_type": "call", "strike": 165.0, "bid": 0.5, "ask": 0.6, "greeks": {"delta": 0.1}},
        ]

    def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        return [{"symbol": s.strip(), "last": 155.0, "bid": 154.9, "ask": 155.1} for s in symbols.split(",")]

    def get_delta(self, option_symbol: str) -> float:
        return 0.15

    def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        return {
            "current_price": 155.0,
            "put_entry_points": [{"price": 145.0, "pop": 80}, {"price": 140.0, "pop": 90}],
            "call_entry_points": [{"price": 165.0, "pop": 80}, {"price": 170.0, "pop": 90}],
        }

    def get_history(self, symbol: str, interval: str = "daily",
                    start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return dummy history data."""
        from datetime import datetime, timedelta
        out = []
        now = datetime.utcnow()
        days = 400 if interval == "daily" else 10
        for i in range(days):
            ts = now - timedelta(days=i)
            out.append({
                "date": ts.strftime("%Y-%m-%d" if interval == "daily" else "%Y-%m-%d %H:%M:%S"),
                "open": 150.0 + i % 10,
                "high": 155.0 + i % 10,
                "low": 145.0 + i % 10,
                "close": 152.0 + i % 10,
                "volume": 1000000,
                "vwap": 152.0 + i % 10
            })
        return out

    def place_order_from_action(self, action: TradeAction) -> Dict[str, Any]:
        logger.info("[MOCK] Placing order for %s: %s", action.symbol, action.legs)
        return {"status": "ok", "order_id": "MOCK-123"}

    def roll_to_next_month(self, option_symbol: str) -> str:
        return option_symbol + "_ROLLED"

class MockLLM:
    def chat(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        return '{"verdict": "APPROVE", "rationale": "Mock approval"}'
