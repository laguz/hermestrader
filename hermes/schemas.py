"""
hermes/schemas.py — Typed Pydantic v2 schemas for the trading agent.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class OptionLegSchema(BaseModel):
    """Represents a single leg of a multi-leg or single option trade."""
    option_symbol: str = Field(..., description="OCC option symbol (e.g. AAPL250620P00150000)")
    side: str = Field(..., description="Order side: buy_to_open, sell_to_open, buy_to_close, sell_to_close")
    quantity: int = Field(..., gt=0, description="Number of contracts")


class MarketQuoteSchema(BaseModel):
    """Represents a quote update from the broker."""
    symbol: str
    bid: float
    ask: float
    last: Optional[float] = None
    volume: Optional[int] = None
    implied_volatility: Optional[float] = None
    delta: Optional[float] = None

    @property
    def mid(self) -> float:
        """Calculate the mid price of the quote."""
        return (self.bid + self.ask) / 2.0


class PositionSchema(BaseModel):
    """Represents an active position held at the broker."""
    symbol: str
    quantity: int
    cost_basis: float
    date_acquired: Optional[datetime] = None
