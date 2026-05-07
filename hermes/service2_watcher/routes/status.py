"""Status endpoints — what the dashboard polls every few seconds.

Routes
------
- ``GET /``                 — serve the dashboard HTML
- ``GET /api/status``       — single roll-up of agent/Tradier/LLM/market state
- ``GET /api/health``       — liveness probe for the watcher itself
- ``GET /api/logs``         — recent bot_logs lines (activity feed)
- ``GET /api/balances``     — live Tradier balances + computed true-available BP
- ``GET /api/debug``        — diagnostic counters used when triaging issues

The status roll-up is the single read most clients hit, so keep it cheap.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

from fastapi import APIRouter
from fastapi.responses import FileResponse

from hermes.common import STRATEGIES, VALID_MODES
from hermes.market_hours import market_session, next_open

from .._app_state import (
    SETTING_AGENT_STARTED,
    SETTING_APPROVAL_MODE,
    SETTING_LLM_ERROR,
    SETTING_LLM_MODEL,
    SETTING_LLM_OK_TS,
    SETTING_LLM_PROVIDER,
    SETTING_MODE,
    SETTING_PAUSED,
    SETTING_TRADIER_ERROR,
    SETTING_TRADIER_OK_TS,
    STALE_AFTER_S,
    STATIC_DIR,
    TICK_INTERVAL_S,
    db,
    parse_iso,
    read_version,
    seconds_since,
    strategy_enabled_key,
)

router = APIRouter()


@router.get("/")
def index() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "dashboard.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@router.get("/api/status")
def get_status() -> Dict[str, Any]:
    last_log_ts = db.latest_log_ts()
    last_log_age = seconds_since(last_log_ts)
    hermes_running = last_log_age is not None and last_log_age <= STALE_AFTER_S

    started_iso = db.get_setting(SETTING_AGENT_STARTED)
    started_at = parse_iso(started_iso)
    uptime_s = seconds_since(started_at) if hermes_running else None

    last_ok = parse_iso(db.get_setting(SETTING_TRADIER_OK_TS))
    tradier_error = (db.get_setting(SETTING_TRADIER_ERROR) or "").strip()
    tradier_ok = (
        seconds_since(last_ok) is not None
        and seconds_since(last_ok) <= STALE_AFTER_S
        and not tradier_error
    )

    llm_error = (db.get_setting(SETTING_LLM_ERROR) or "").strip()
    llm_last_ok = parse_iso(db.get_setting(SETTING_LLM_OK_TS))
    # LLM may not be used every tick (advisory autonomy with no actions);
    # be 4x more lenient than the Tradier window before declaring unhealthy.
    llm_ok = (
        llm_last_ok is not None
        and seconds_since(llm_last_ok) is not None
        and seconds_since(llm_last_ok) <= STALE_AFTER_S * 4
        and not llm_error
    )

    mode = (db.get_setting(SETTING_MODE) or "paper").lower()
    if mode not in VALID_MODES:
        mode = "paper"

    paused = (db.get_setting(SETTING_PAUSED) or "false").lower() == "true"
    approval_mode = (db.get_setting(SETTING_APPROVAL_MODE) or "true").lower() == "true"
    pending_count = len(db.list_approvals(status="PENDING", limit=500))

    strategy_enabled = {
        sid: (db.get_setting(strategy_enabled_key(sid)) or "true").lower() != "false"
        for sid in STRATEGIES
    }

    llm_model = (db.get_setting(SETTING_LLM_MODEL) or "").strip()
    llm_provider = (db.get_setting(SETTING_LLM_PROVIDER) or "mock").strip()

    try:
        mkt = market_session()
        nxt = next_open() if not mkt["is_open"] else None
    except Exception:                                               # noqa: BLE001
        mkt = {"session": "unknown", "is_open": False,
               "et_time": "--:--", "et_date": "", "trading_day": False}
        nxt = None

    return {
        "hermes_running": hermes_running,
        "hermes_last_seen_age_s": last_log_age,
        "agent_started_at": started_iso,
        "uptime_s": uptime_s,
        "tradier_ok": tradier_ok,
        "tradier_error": tradier_error or None,
        "llm_ok": llm_ok,
        "llm_error": llm_error or None,
        "llm_model": llm_model or None,
        "llm_provider": llm_provider,
        "mode": mode,
        "paused": paused,
        "approval_mode": approval_mode,
        "pending_approvals": pending_count,
        "strategy_enabled": strategy_enabled,
        "stale_after_s": STALE_AFTER_S,
        "tick_interval_s": TICK_INTERVAL_S,
        "version": os.environ.get("HERMES_VERSION") or read_version(),
        "market_session": mkt["session"],
        "market_is_open": mkt["is_open"],
        "market_et_time": mkt["et_time"],
        "market_trading_day": mkt["trading_day"],
        "market_next_open": nxt.isoformat() if nxt else None,
    }


@router.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "hermes-c2"}


@router.get("/api/logs")
def get_logs(limit: int = 100) -> List[Dict[str, Any]]:
    raw = db.recent_logs(limit=min(limit, 500))
    # recent_logs returns a single concatenated string; the dashboard's
    # activity feed wants one entry per line.
    return [{"text": line} for line in (raw or "").splitlines()]


@router.get("/api/balances")
def get_balances() -> Dict[str, Any]:
    """Live Tradier balances + the computed true-available BP.

    Diagnostic endpoint — the operator uses this when capacity decisions
    look wrong (e.g. "why is CS75 saying insufficient BP?").
    """
    try:
        from hermes.broker.tradier import TradierBroker
        broker = TradierBroker(
            api_key=os.environ.get("TRADIER_API_KEY", ""),
            account_id=os.environ.get("TRADIER_ACCOUNT_ID", ""),
            paper=os.environ.get("HERMES_MODE", "paper").lower() != "live",
        )
        balances = broker.get_account_balances() or {}
        reserve = float(os.environ.get("HERMES_MIN_OBP_RESERVE", 5000.0))
        raw_obp = float(balances.get("option_buying_power", 0.0))
        true_bp = max(0.0, raw_obp - reserve)
        return {
            "ok": True,
            "account_type": balances.get("account_type"),
            "option_buying_power": raw_obp,
            "min_obp_reserve": reserve,
            "true_available_bp": true_bp,
            "stock_buying_power": balances.get("stock_buying_power"),
            "total_equity": balances.get("total_equity"),
            "cash": balances.get("cash"),
            "raw": balances.get("raw", {}),
        }
    except Exception as exc:                                      # noqa: BLE001
        return {"ok": False, "error": str(exc)}


@router.get("/api/debug")
def get_debug_info() -> Dict[str, Any]:
    """Counts of bars / predictions / recent logs — used when the dashboard
    looks empty and the operator needs to know whether the agent or the
    DB is at fault."""
    try:
        import xgboost
        xgb_ver = xgboost.__version__
    except ImportError:
        xgb_ver = "MISSING"

    result: Dict[str, Any] = {"xgboost": xgb_ver, "logs": [], "db": {}}

    with db.Session() as s:
        from sqlalchemy import text as sa_text
        try:
            result["db"]["bars_daily"] = s.execute(sa_text(
                "SELECT COUNT(*) FROM bars_daily")).scalar()
            result["db"]["bars_intraday"] = s.execute(sa_text(
                "SELECT COUNT(*) FROM bars_intraday")).scalar()
            result["db"]["predictions"] = s.execute(sa_text(
                "SELECT COUNT(*) FROM predictions")).scalar()
            result["db"]["ml_last_error"] = s.execute(sa_text(
                "SELECT value FROM system_settings WHERE key='ml_last_error'"
            )).scalar()
        except Exception as e:                                    # noqa: BLE001
            result["db"]["error"] = str(e)

        try:
            raw = s.execute(sa_text(
                "SELECT message FROM bot_logs ORDER BY ts DESC LIMIT 10"
            )).fetchall()
            result["logs"] = [r[0] for r in raw]
        except Exception:                                         # noqa: BLE001
            pass

    return result
