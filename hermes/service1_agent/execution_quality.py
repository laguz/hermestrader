"""
[Service-1: Hermes-Agent-Core]
Execution-quality measurement — quote-mid capture at order submission.

The mid stamped here is persisted alongside the pending order and on the
Trade row, then compared to the broker's actual fill in
``TradesRepository.apply_entry_fill_price`` to yield per-order
``entry_slippage``. Measurement only: a failure in this module must never
block or delay an order, so every path degrades to ``None`` ("slippage
unknown") — a missing mid is never turned into a fabricated 0.0.
"""
from __future__ import annotations

import inspect
import logging
import statistics
from typing import Optional

logger = logging.getLogger("hermes.agent.execution_quality")


async def capture_submission_mid(broker, action) -> Optional[float]:
    """Stamp ``action.strategy_params['mid_at_submit']`` with the current net
    quote mid of the action's legs; return it.

    Degrades to ``None`` (and leaves ``strategy_params`` untouched) when the
    broker doesn't implement ``get_action_net_mid`` — e.g. MockBroker and the
    test stubs — or when the quote fetch fails for any reason.
    """
    fn = getattr(broker, "get_action_net_mid", None)
    if fn is None:
        return None
    try:
        raw = fn(action)
        if inspect.isawaitable(raw):
            raw = await raw
        mid = float(raw) if raw is not None else None
    except Exception as exc:
        logger.warning("[EXEC-Q] mid-at-submit capture failed for %s %s: %s",
                       action.strategy_id, action.symbol, exc)
        return None
    if mid is not None:
        if action.strategy_params is None:
            action.strategy_params = {}
        action.strategy_params["mid_at_submit"] = mid
    return mid


async def estimate_symbol_slippage(db, symbol: str, min_fills: int,
                                   lookback: int = 20) -> Optional[float]:
    """Trailing median fill-vs-mid slippage for ``symbol`` (same sign
    convention as ``Trade.entry_slippage``: positive = filled worse than mid).

    Degrades to ``None`` ("no adjustment") with fewer than ``min_fills``
    recorded fills — a thin history isn't a reliable cost estimate, and a
    fabricated adjustment would either falsely reject good entries or
    understate real execution cost.
    """
    fetch = getattr(db.trades, "recent_entry_slippage", None)
    if fetch is None:
        return None
    try:
        values = await fetch(symbol, lookback)
    except Exception as exc:
        logger.warning("[EXEC-Q] slippage history fetch failed for %s: %s", symbol, exc)
        return None
    if len(values) < min_fills:
        return None
    return statistics.median(values)
