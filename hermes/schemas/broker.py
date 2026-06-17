from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class OptionLeg(BaseModel):
    """Pydantic schema representing a single option leg."""
    option_symbol: str = Field(..., description="OCC Option Symbol")
    quantity: int = Field(..., gt=0, description="Quantity of contracts")
    side: str = Field(..., description="buy_to_open, sell_to_open, buy_to_close, sell_to_close, buy, or sell")
    action: Optional[str] = Field(None, description="Explicit action mapping")


class MultiLegOrder(BaseModel):
    """Pydantic schema representing a multi-leg option order or simple order."""
    strategy_id: str
    symbol: str
    order_class: str = Field("multileg", description="multileg, option, or equity")
    legs: List[OptionLeg] = Field(default_factory=list)
    price: Optional[float] = None
    side: str = Field("buy", description="buy or sell")
    quantity: int = Field(1, gt=0)
    duration: str = "day"
    order_type: str = "credit"
    tag: Optional[str] = None
    expiry: Optional[str] = None
    width: Optional[float] = None


class ExecutionReport(BaseModel):
    """Pydantic schema for broker execution reports."""
    order_id: str
    status: str
    filled_quantity: int
    avg_fill_price: Optional[float] = None
    transaction_time: datetime = Field(default_factory=datetime.utcnow)
    raw_response: Dict[str, Any] = Field(default_factory=dict)


class BrokerPosition(BaseModel):
    """Pydantic schema representing a position fetched from the broker."""
    symbol: str
    quantity: float
    cost_basis: float
    date_acquired: Optional[str] = None
