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

import json as _json
import math

import numpy as np
from fastapi import APIRouter
from fastapi.responses import Response

_NO_CACHE_HEADERS = {"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}


def _sanitize_floats(obj: Any) -> Any:
    """Recursively replace NaN/Inf floats with None and unwrap numpy scalars.

    JSON encoders that use allow_nan=False reject NaN/Inf, producing "Out of
    range float values are not JSON compliant". numpy float32/float16 don't
    inherit from Python float, so isinstance(x, float) misses them — handle
    np.floating/np.integer explicitly. ndarrays get converted to lists.
    """
    if isinstance(obj, np.ndarray):
        return _sanitize_floats(obj.tolist())
    if isinstance(obj, np.floating):
        f = float(obj)
        return f if math.isfinite(f) else None
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        cleaned = [_sanitize_floats(v) for v in obj]
        return tuple(cleaned) if isinstance(obj, tuple) else cleaned
    return obj


def _safe_json_response(content: Any, headers: Dict[str, str] = None) -> Response:
    """Sanitize then encode by hand so non-finite floats can never escape.

    Uses a default= fallback that nulls out anything still non-finite or
    unrecognized; this is the last line of defense regardless of producer.
    """
    cleaned = _sanitize_floats(content)

    def _default(o: Any) -> Any:
        if isinstance(o, np.ndarray):
            return _sanitize_floats(o.tolist())
        if isinstance(o, (np.floating, float)):
            f = float(o)
            return f if math.isfinite(f) else None
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.bool_):
            return bool(o)
        return None

    body = _json.dumps(cleaned, allow_nan=False, default=_default).encode("utf-8")
    return Response(content=body, media_type="application/json",
                    headers=headers or {})

from hermes.db.models import HermesDB
from hermes.ml.pop_engine import augment_levels_with_pop

from .._app_state import DSN, WATCHLIST, db

router = APIRouter()
logger = logging.getLogger("hermes.c2.api")




@router.get("/api/analytics")
async def get_analytics() -> Response:
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

            # Recent closed trades. ``pnl`` is left as NULL→None (instead of
            # coerced to 0) so the dashboard can render an "unknown" cell
            # and not lie when realized P&L couldn't be computed.
            raw_closed = s.execute(sa_text("""
                SELECT id, strategy_id, symbol, side_type, lots, entry_credit,
                       pnl, close_reason, expiry, opened_at, closed_at, ai_authored,
                       tag, close_tag, exit_price
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
                    "pnl": float(r.pnl) if r.pnl is not None else None,
                    "close_reason": r.close_reason,
                    "expiry": r.expiry.isoformat() if r.expiry else None,
                    "opened_at": r.opened_at.isoformat() if r.opened_at else None,
                    "closed_at": r.closed_at.isoformat() if r.closed_at else None,
                    "ai_authored": bool(r.ai_authored),
                    "tag": r.tag,
                    "close_tag": r.close_tag,
                    "exit_price": float(r.exit_price) if r.exit_price is not None else None,
                }
                for r in raw_closed
            ]

    except Exception as exc:                                       # noqa: BLE001
        logger.exception("analytics query failed: %s", exc)
        result["error"] = str(exc)

    # Daily P&L series (last 60 days) from pnl_daily view
    try:
        result["pnl_series"] = await db.pnl_daily(days=60)
        for row in result["pnl_series"]:
            if hasattr(row.get("day"), "isoformat"):
                row["day"] = row["day"].isoformat()
            row["realized_pnl"] = float(row.get("realized_pnl") or 0)
    except Exception:                                              # noqa: BLE001
        result["pnl_series"] = []

    return _safe_json_response(result)


@router.get("/api/analytics/attribution")
async def get_attribution(strategy_id: str = None, days: int = None,
                          min_bucket_n: int = 5) -> Response:
    """Per-knob / per-feature expectancy from closed-trade outcomes.

    The offline evaluator behind outcome-driven tuning (Phase 1): it pairs each
    closed trade's ``entry_features`` snapshot with its realized P&L and reports
    win-rate + expectancy bucketed by market context and by knob value.

    Query params: ``strategy_id`` (filter), ``days`` (look-back window),
    ``min_bucket_n`` (small-sample flag threshold).
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    from hermes.ml.attribution import attribute_outcomes

    since = (_dt.now(_tz.utc) - _td(days=days)) if days else None
    try:
        rows = await db.fetch_trade_outcomes(strategy_id=strategy_id, since=since)
    except Exception:                                              # noqa: BLE001
        logger.exception("[ATTRIBUTION] fetch_trade_outcomes failed")
        rows = []
    report = attribute_outcomes(rows, min_bucket_n=max(1, int(min_bucket_n)))
    return _safe_json_response(report)


@router.get("/api/analytics/bandit")
async def get_bandit(min_observations: int = 20) -> Response:
    """Thompson-bandit knob proposals + per-arm posteriors (read-only).

    Shows what the Phase-2 bandit would select for each learnable knob given
    the closed-trade outcomes so far, and whether each proposal is
    ``actionable`` (has enough data to act on). Never mutates a setting — the
    agent tick does that, only when ``bandit_tuner_mode=active``.
    """
    from hermes.ml.bandit import propose_knob_updates, LEARNABLE_KNOBS

    try:
        rows = await db.fetch_trade_outcomes()
        keys = [k for knobs in LEARNABLE_KNOBS.values() for k in knobs]
        current = await db.get_settings(keys) or {}
    except Exception:                                              # noqa: BLE001
        logger.exception("[BANDIT] proposal fetch failed")
        rows, current = [], {}

    mode = (await db.get_setting("bandit_tuner_mode") or "off")
    proposals = propose_knob_updates(
        rows, current, min_observations=max(1, int(min_observations)))
    return _safe_json_response({
        "mode": str(mode).strip().lower(),
        "n_trades": len(rows),
        "min_observations": min_observations,
        "proposals": proposals,
    })


def _build_broker_for_analysis():
    """Construct a broker matching the operator's current mode.

    Inlined here (rather than re-using a global) so the analysis endpoints
    pick up mode toggles immediately without a watcher restart. The agent
    has its own broker instance — these are independent.

    Returns an async TradierBroker when credentials are present, or a
    sync MockBroker as a fallback for dev/demo mode.
    """
    from hermes.broker.tradier import TradierBroker
    mode = os.environ.get("HERMES_MODE", "paper").lower()
    if mode == "paper":
        token = (os.environ.get("TRADIER_PAPER_TOKEN")
                 or os.environ.get("TRADIER_ACCESS_TOKEN")
                 or os.environ.get("TRADIER_API_KEY"))
        account = os.environ.get("TRADIER_PAPER_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = (os.environ.get("TRADIER_PAPER_BASE_URL")
               or os.environ.get("TRADIER_ENDPOINT")
               or "https://sandbox.tradier.com/v1")
    else:
        token = (os.environ.get("TRADIER_LIVE_TOKEN")
                 or os.environ.get("TRADIER_ACCESS_TOKEN")
                 or os.environ.get("TRADIER_API_KEY"))
        account = os.environ.get("TRADIER_LIVE_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = os.environ.get("TRADIER_LIVE_BASE_URL") or "https://api.tradier.com/v1"
    if not token or not account:
        from hermes.service1_agent.mock_broker import MockBroker
        return MockBroker({})
    return TradierBroker({
        "tradier_access_token": token,
        "tradier_account_id": account,
        "tradier_base_url": url,
    })


async def _analyze_one(broker, symbol: str, period: str) -> Dict[str, Any]:
    """Await an async broker or run a sync broker in a thread, uniformly."""
    import asyncio
    import functools
    import inspect
    result = broker.analyze_symbol(symbol, period=period)
    if inspect.isawaitable(result):
        return await result
    # MockBroker is synchronous — offload to a thread so we don't block the
    # event loop while it generates its deterministic mock bars.
    loop = asyncio.get_event_loop()
    fn = functools.partial(broker.analyze_symbol, symbol, period=period)
    return await loop.run_in_executor(None, fn)


@router.get("/api/analysis/{symbol}")
async def get_symbol_analysis(symbol: str) -> Response:
    """S/R clustering for one symbol, augmented with the latest XGB POP."""
    try:
        broker = _build_broker_for_analysis()
        analysis = await _analyze_one(broker, symbol.upper(), period="6m")
        if "error" in analysis:
            return _safe_json_response(analysis)
        local_db = HermesDB(DSN)
        xgb_pred = await local_db.latest_prediction(symbol.upper()) or {}
        return _safe_json_response(augment_levels_with_pop(analysis, xgb_pred))
    except Exception as exc:                                       # noqa: BLE001
        return _safe_json_response({"error": str(exc)})


@router.get("/api/analysis")
async def get_watchlist_analysis(period: str = "6m") -> Response:
    """S/R clustering for every watchlist symbol, augmented with POP."""
    import asyncio
    period = (period or "6m").lower()
    if period not in {"1m", "3m", "6m", "1y"}:
        period = "6m"
    try:
        local_db = HermesDB(DSN)
        all_wl = await local_db.list_all_watchlists()
        symbols = set()
        for wl in all_wl.values():
            symbols.update(wl)
        if not symbols:
            symbols = set(WATCHLIST)
        if not symbols:
            return _safe_json_response({}, headers=_NO_CACHE_HEADERS)

        sorted_symbols = sorted(symbols)
        broker = _build_broker_for_analysis()

        # 1. Fetch analysis concurrently — gather drives async broker calls;
        #    _analyze_one handles the sync MockBroker fallback via run_in_executor.
        raw = await asyncio.gather(
            *[_analyze_one(broker, sym, period) for sym in sorted_symbols],
            return_exceptions=True,
        )
        results: Dict[str, Any] = {}
        for sym, res in zip(sorted_symbols, raw):
            if isinstance(res, Exception):
                results[sym] = {"error": str(res)}
            else:
                results[sym] = res

        # 2. Fetch all predictions in one batch (DB optimization)
        preds_map = await local_db.latest_predictions_batch(sorted_symbols)

        # 3. Augment with POP
        for sym, ans in results.items():
            if "error" not in ans:
                xgb_pred = preds_map.get(sym) or {}
                results[sym] = augment_levels_with_pop(ans, xgb_pred, period=period)

        return _safe_json_response(results, headers=_NO_CACHE_HEADERS)
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("watchlist analysis failed: %s", exc)
        return _safe_json_response({"error": str(exc)}, headers=_NO_CACHE_HEADERS)
