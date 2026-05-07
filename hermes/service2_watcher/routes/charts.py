"""Chart endpoints — PNGs and per-symbol LLM analyses.

Routes
------
- ``GET /api/chart/{symbol}/image``     — candlestick PNG (rendered + cached)
- ``GET /api/chart/{symbol}/analysis``  — most recent LLM chart analysis
- ``GET /api/charts``                   — latest analysis for every watchlist symbol

The PNG is rendered from ``bars_daily`` via ``HermesChartProvider`` (same
implementation the agent uses) and cached for 5 minutes. Analyses are
written by the agent's overseer (``strategy_id='CHART'``) and read back
through ``HermesDB.recent_ai_decisions``.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from .._app_state import WATCHLIST, db

router = APIRouter()
logger = logging.getLogger("hermes.c2.api")


# Module-level lazy chart provider — one instance shared across requests.
_watcher_chart_provider = None


def _get_chart_provider():
    """Return (or lazily build) the watcher-side chart provider.

    Lazy because matplotlib is optional — installing it is a sizeable
    container-image cost, so the watcher should still boot without it.
    """
    global _watcher_chart_provider                               # noqa: PLW0603
    if _watcher_chart_provider is not None:
        return _watcher_chart_provider
    try:
        from hermes.charts.provider import HermesChartProvider
        _watcher_chart_provider = HermesChartProvider(
            db, lookback_days=210, cache_ttl_s=300,
        )
        return _watcher_chart_provider
    except ImportError:
        return None
    except Exception as exc:                                     # noqa: BLE001
        logger.warning("Could not build watcher chart provider: %s", exc)
        return None


@router.get("/api/chart/{symbol}/image")
def chart_image(symbol: str) -> Response:
    """PNG candlestick chart for ``symbol``, cached for 5 min.

    503 when matplotlib is unavailable; 404 when bars are missing.
    """
    sym = symbol.upper().strip()
    provider = _get_chart_provider()
    if provider is None:
        raise HTTPException(
            status_code=503,
            detail="Chart rendering unavailable — install matplotlib in the watcher container",
        )
    png = provider.snapshot(sym)
    if png is None:
        raise HTTPException(
            status_code=404,
            detail=f"No chart data available for {sym} — bars may not yet be populated",
        )
    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": "max-age=300, public"},
    )


@router.get("/api/chart/{symbol}/analysis")
def chart_analysis(symbol: str) -> Dict[str, Any]:
    """Most recent LLM chart analysis written by the agent for ``symbol``."""
    sym = symbol.upper().strip()
    try:
        rows = db.recent_ai_decisions(strategy_id="CHART", symbol=sym, limit=1)
        return {"symbol": sym, "analysis": rows[0] if rows else None}
    except Exception as exc:                                     # noqa: BLE001
        logger.warning("chart_analysis query failed for %s: %s", sym, exc)
        return {"symbol": sym, "analysis": None}


@router.get("/api/charts")
def all_chart_analyses() -> Dict[str, Any]:
    """Latest LLM analysis for every symbol in the watchlist."""
    results: Dict[str, Any] = {}
    for sym in WATCHLIST:
        try:
            rows = db.recent_ai_decisions(strategy_id="CHART", symbol=sym, limit=1)
            results[sym] = rows[0] if rows else None
        except Exception:                                        # noqa: BLE001
            results[sym] = None
    return {"analyses": results, "watchlist": WATCHLIST}
