from __future__ import annotations

from typing import Any, Dict, List, Optional

from hermes.broker.base import AbstractBroker
from hermes.broker.mock_engine import MockAsyncTradierBroker


class HistoricalBacktestBroker(MockAsyncTradierBroker, AbstractBroker):
    """A historical backtest broker that implements AbstractBroker by inheriting from MockAsyncTradierBroker."""

    async def close(self) -> None:
        """Close virtual connections."""
        pass
