"""Analytics + per-symbol structural analysis.

Routes
------
- ``GET /analytics``                — analytics dashboard HTML
- ``GET /api/analytics``            — predictions + closed-trade perf + open trades + P&L series
- ``GET /api/analysis/{symbol}``    — S/R clustering for one symbol (POP-augmented)
- ``GET /api/analysis``             — S/R clustering for every watchlist symbol

The /api/analytics endpoint is the heaviest read in the watcher; cache
client-side if the dashboard polls it more than once per minute.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

from fastapi import APIRouter
from fastapi.responses import FileResponse

from hermes.db.models import HermesDB
from hermes.ml.pop_engine import augment_levels_with_pop

from .._app_state import DSN, STATIC_DIR, WATCHLIST, db

router = APIRouter()
logger = logging.getLogger("hermes.c2.api")


@router.get("/analytics")
def analytics_page() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "analytics.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@router.get("/api/analytics")
def get_analytics() -> Dict[str, Any]:
    """ML predictions + closed-trade performance + open trades + P&L series."""
    from sqlalchemy import text as sa_text

    result: Dict[str, Any] = {
        "predictions": [],
        "performance": {},
        "open_trades": [],
        "closed_trades": [],
        "pnl_series": [],
    }

    try:
        with db.Session() as s:
            ml_err = s.execute(sa_text(
                "SELECT value FROM system_settings WHERE key = 'ml_last_error'"
            )).scalar()
            if ml_err:
                result["ml_last_error"] = ml_err

            # Latest prediction per symbol
            raw = s.execute(sa_text("""
                SELECT DISTINCT ON (symbol)
                    symbol, predicted_return, predicted_price, spot, ts, model_tag
                FROM predictions
                ORDER BY symbol, ts DESC
            """)).fetchall()
            result["predictions"] = [
                {
                    "symbol": r.symbol,
                    "predicted_return": float(r.predicted_return or 0),
                    "predicted_price": float(r.predicted_price or 0),
                    "spot": float(r.spot or 0),
                    "as_of": r.ts.isoformat() if r.ts else None,
                    "model_tag": r.model_tag,
                }
                for r in raw
            ]

            # Performance per strategy (closed trades)
            raw_perf = s.execute(sa_text("""
                SELECT
                    strategy_id,
                    COUNT(*) FILTER (WHERE status = 'CLOSED') AS total_closed,
                    COUNT(*) FILTER (WHERE status = 'CLOSED' AND pnl > 0) AS winners,
                    COUNT(*) FILTER (WHERE status = 'CLOSED' AND pnl <= 0) AS losers,
                    COALESCE(SUM(pnl) FILTER (WHERE status = 'CLOSED'), 0) AS total_pnl,
                    COALESCE(AVG(pnl) FILTER (WHERE status = 'CLOSED'), 0) AS avg_pnl,
                    COALESCE(MAX(pnl) FILTER (WHERE status = 'CLOSED'), 0) AS best_trade,
                    COALESCE(MIN(pnl) FILTER (WHERE status = 'CLOSED'), 0) AS worst_trade,
                    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_count
                FROM trades
                GROUP BY strategy_id
                ORDER BY strategy_id
            """)).fetchall()
            for r in raw_perf:
                total = int(r.total_closed or 0)
                winners = int(r.winners or 0)
                result["performance"][r.strategy_id] = {
                    "total_closed": total,
                    "winners": winners,
                    "losers": int(r.losers or 0),
                    "win_rate": round(winners / total * 100, 1) if total else 0,
                    "total_pnl": float(r.total_pnl or 0),
                    "avg_pnl": float(r.avg_pnl or 0),
                    "best_trade": float(r.best_trade or 0),
                    "worst_trade": float(r.worst_trade or 0),
                    "open_count": int(r.open_count or 0),
                }

            # Open trades (all strategies)
            raw_open = s.execute(sa_text("""
                SELECT id, strategy_id, symbol, side_type, short_leg, long_leg,
                       short_strike, long_strike, width, lots, entry_credit,
                       expiry, opened_at, ai_authored
                FROM trades
                WHERE status = 'OPEN'
                ORDER BY opened_at DESC
                LIMIT 100
            """)).fetchall()
            result["open_trades"] = [
                {
                    "id": r.id,
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "side_type": r.side_type,
                    "short_leg": r.short_leg,
                    "long_leg": r.long_leg,
                    "short_strike": float(r.short_strike) if r.short_strike else None,
                    "long_strike": float(r.long_strike) if r.long_strike else None,
                    "width": float(r.width) if r.width else None,
                    "lots": int(r.lots or 0),
                    "entry_credit": float(r.entry_credit or 0),
                    "expiry": r.expiry.isoformat() if r.expiry else None,
                    "opened_at": r.opened_at.isoformat() if r.opened_at else None,
                    "ai_authored": bool(r.ai_authored),
                }
                for r in raw_open
            ]

            # Recent closed trades
            raw_closed = s.execute(sa_text("""
                SELECT id, strategy_id, symbol, side_type, lots, entry_credit,
                       pnl, close_reason, expiry, opened_at, closed_at, ai_authored
                FROM trades
                WHERE status = 'CLOSED'
                ORDER BY closed_at DESC
                LIMIT 50
            """)).fetchall()
            result["closed_trades"] = [
                {
                    "id": r.id,
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "side_type": r.side_type,
                    "lots": int(r.lots or 0),
                    "entry_credit": float(r.entry_credit or 0),
                    "pnl": float(r.pnl or 0),
                    "close_reason": r.close_reason,
                    "expiry": r.expiry.isoformat() if r.expiry else None,
                    "opened_at": r.opened_at.isoformat() if r.opened_at else None,
                    "closed_at": r.closed_at.isoformat() if r.closed_at else None,
                    "ai_authored": bool(r.ai_authored),
                }
                for r in raw_closed
            ]

    except Exception as exc:                                       # noqa: BLE001
        logger.exception("analytics query failed: %s", exc)
        result["error"] = str(exc)

    # Daily P&L series (last 60 days) from pnl_daily view
    try:
        result["pnl_series"] = db.pnl_daily(days=60)
        for row in result["pnl_series"]:
            if hasattr(row.get("day"), "isoformat"):
                row["day"] = row["day"].isoformat()
            row["realized_pnl"] = float(row.get("realized_pnl") or 0)
    except Exception:                                              # noqa: BLE001
        result["pnl_series"] = []

    return result


def _build_broker_for_analysis():
    """Construct a TradierBroker matching the operator's current mode.

    Inlined here (rather than re-using a global) so the analysis endpoints
    pick up mode toggles immediately without a watcher restart. The agent
    has its own broker instance — these are independent.
    """
    from hermes.broker.tradier import TradierBroker
    mode = os.environ.get("HERMES_MODE", "paper").lower()
    if mode == "paper":
        token = os.environ.get("TRADIER_PAPER_TOKEN") or os.environ.get("TRADIER_ACCESS_TOKEN")
        account = os.environ.get("TRADIER_PAPER_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = (os.environ.get("TRADIER_PAPER_BASE_URL")
               or os.environ.get("TRADIER_ENDPOINT")
               or "https://sandbox.tradier.com/v1")
    else:
        token = os.environ.get("TRADIER_LIVE_TOKEN") or os.environ.get("TRADIER_ACCESS_TOKEN")
        account = os.environ.get("TRADIER_LIVE_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = os.environ.get("TRADIER_LIVE_BASE_URL") or "https://api.tradier.com/v1"
    return TradierBroker({
        "tradier_access_token": token,
        "tradier_account_id": account,
        "tradier_base_url": url,
    })


@router.get("/api/analysis/{symbol}")
def get_symbol_analysis(symbol: str) -> Dict[str, Any]:
    """S/R clustering for one symbol, augmented with the latest XGB POP."""
    try:
        broker = _build_broker_for_analysis()
        analysis = broker.analyze_symbol(symbol.upper())
        if "error" in analysis:
            return analysis
        local_db = HermesDB(DSN)
        xgb_pred = local_db.latest_prediction(symbol.upper()) or {}
        return augment_levels_with_pop(analysis, xgb_pred)
    except Exception as exc:                                       # noqa: BLE001
        return {"error": str(exc)}


@router.get("/api/analysis")
def get_watchlist_analysis(period: str = "6m") -> Dict[str, Any]:
    """S/R clustering for every watchlist symbol, augmented with POP."""
    try:
        local_db = HermesDB(DSN)
        all_wl = local_db.list_all_watchlists()
        symbols = set()
        for wl in all_wl.values():
            symbols.update(wl)
        if not symbols:
            symbols = set(WATCHLIST)
        if not symbols:
            return {}
        broker = _build_broker_for_analysis()
        results: Dict[str, Any] = {}
        for sym in sorted(symbols):
            ans = broker.analyze_symbol(sym, period=period)
            if "error" not in ans:
                xgb_pred = local_db.latest_prediction(sym) or {}
                ans = augment_levels_with_pop(ans, xgb_pred, period=period)
            results[sym] = ans
        return results
    except Exception as exc:                                       # noqa: BLE001
        return {"error": str(exc)}
