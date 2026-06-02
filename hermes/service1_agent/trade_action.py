"""
[Service-1: Hermes-Agent-Core]
TradeAction — the single canonical order envelope used by every strategy.

Kept in its own module so the order primitive sits *below* MoneyManager,
AbstractStrategy and CascadingEngine in the import graph. Everything builds
TradeActions; nothing TradeAction depends on imports back up.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# TradeAction — single canonical order envelope used by every strategy
# ---------------------------------------------------------------------------
@dataclass
class TradeAction:
    """Order routing envelope. Strategies build these; TradeManager submits them."""
    strategy_id: str
    symbol: str
    order_class: str                       # 'multileg' | 'equity' | 'option'
    legs: List[Dict[str, Any]]             # [{'option_symbol','side','quantity'}, ...]
    price: Optional[float]                 # net credit (sell) or debit (buy)
    side: str                              # 'sell' | 'buy'
    quantity: int = 1                      # overall order qty (legs carry per-leg qty)
    duration: str = "day"
    order_type: str = "credit"             # 'credit' | 'debit' | 'limit' | 'market'
    tag: Optional[str] = None
    strategy_params: Dict[str, Any] = field(default_factory=dict)
    dte: Optional[int] = None
    expiry: Optional[str] = None
    width: Optional[float] = None
    # AI override metadata — set when HermesOverseer authored or modified the action
    ai_authored: bool = False
    ai_rationale: Optional[str] = None
