"""Per-strategy watchlist management.

Routes
------
- ``GET    /api/watchlist``                 — list per-strategy watchlists + global default
- ``PUT    /api/watchlist/{strategy_id}``   — replace a strategy's watchlist
- ``DELETE /api/watchlist/{strategy_id}``   — clear a strategy's watchlist (falls back to global default)

Per-strategy lists are stored in the ``strategy_watchlists`` table (FK to
``strategies``). The agent reads them via ``HermesDB.list_watchlist`` on
every tick.
"""
from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.common import STRATEGY_PRIORITIES

from .._app_state import WATCHLIST, db

router = APIRouter()


@router.get("/api/watchlist")
def get_watchlist() -> Dict[str, Any]:
    """Return per-strategy watchlists + the global default from env."""
    per_strategy = db.list_all_watchlists()
    return {
        "global_default": WATCHLIST,
        "per_strategy": per_strategy,
        "strategies": list(STRATEGY_PRIORITIES.keys()),
    }


class WatchlistBody(BaseModel):
    symbols: List[str]


@router.put("/api/watchlist/{strategy_id}")
def set_watchlist(strategy_id: str, body: WatchlistBody) -> Dict[str, Any]:
    sid = strategy_id.upper()
    if sid not in STRATEGY_PRIORITIES:
        raise HTTPException(status_code=400, detail=f"Unknown strategy: {sid}")
    cleaned = [s.strip().upper() for s in body.symbols if s.strip()]
    saved = db.set_watchlist(sid, cleaned)
    db.write_log("ENGINE", f"[C2] Watchlist updated for {sid}: {saved}")
    return {"strategy_id": sid, "symbols": saved}


@router.delete("/api/watchlist/{strategy_id}")
def reset_watchlist(strategy_id: str) -> Dict[str, Any]:
    """Clear per-strategy watchlist so it falls back to the global default."""
    sid = strategy_id.upper()
    if sid not in STRATEGY_PRIORITIES:
        raise HTTPException(status_code=400, detail=f"Unknown strategy: {sid}")
    db.set_watchlist(sid, [])
    db.write_log("ENGINE", f"[C2] Watchlist reset for {sid} — using global default")
    return {"strategy_id": sid, "symbols": [], "using_default": True}
