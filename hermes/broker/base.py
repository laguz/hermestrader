from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class AbstractBroker(ABC):
    """Abstract base class representing the broker interface."""

    @abstractmethod
    async def get_account_balances(self) -> Dict[str, Any]:
        """Fetch option and stock buying power, total equity, cash, and account type."""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch list of open positions from the broker."""
        pass

    @abstractmethod
    async def get_orders(self) -> List[Dict[str, Any]]:
        """Fetch list of working and completed orders."""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel a pending/working order."""
        pass

    @abstractmethod
    async def get_option_expirations(self, symbol: str) -> List[str]:
        """Fetch available options expiration dates for a symbol."""
        pass

    @abstractmethod
    async def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        """Fetch the options chain for a symbol at a specific expiration."""
        pass

    @abstractmethod
    async def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        """Fetch market quotes for one or more comma-separated symbols."""
        pass

    @abstractmethod
    async def get_delta(self, option_symbol: str) -> float:
        """Fetch the delta greek value for an option symbol."""
        pass

    @abstractmethod
    async def get_history(
        self, symbol: str, *, interval: str = "daily",
        start: Optional[str] = None, end: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch historical price bars for an equity/symbol."""
        pass

    @abstractmethod
    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        """Run technical analysis (volatility, key levels, support/resistance) on a symbol."""
        pass

    @abstractmethod
    async def place_order_from_action(self, action) -> Dict[str, Any]:
        """Submit an equity, single option, or multileg option order from a TradeAction."""
        pass

    @abstractmethod
    async def roll_to_next_month(self, option_symbol: str) -> str:
        """Helper to find the next monthly expiration and construct the rolled OCC option symbol."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close any open connections or clients."""
        pass
