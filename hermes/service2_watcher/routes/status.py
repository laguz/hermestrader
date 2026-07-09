"""Status endpoints — what the dashboard polls every few seconds.

Routes
------
- ``GET /``                 — serve the dashboard HTML
- ``GET /api/status``       — single roll-up of agent/Tradier/LLM/market state
- ``GET /api/health``       — liveness probe for the watcher itself
- ``GET /api/logs``         — recent bot_logs lines (activity feed)
- ``GET /api/debug``        — diagnostic counters used when triaging issues

The status roll-up is the single read most clients hit, so keep it cheap.
"""
from __future__ import annotations

import os
import json
import asyncio
import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from hermes.common import STRATEGIES, VALID_MODES
from hermes.market_hours import market_session, next_open

from .._app_state import (
    SETTING_AGENT_STARTED,
    SETTING_ALPHA_AUTONOMOUS_LIVE,
    SETTING_APPROVAL_MODE,
    SETTING_AUTONOMY,
    SETTING_LLM_ERROR,
    SETTING_LLM_MODEL,
    SETTING_LLM_OK_TS,
    SETTING_LLM_PROVIDER,
    SETTING_MODE,
    SETTING_PAUSED,
    SETTING_TRADIER_ERROR,
    SETTING_TRADIER_OK_TS,
    STALE_AFTER_S,
    TICK_INTERVAL_S,
    db,
    parse_iso,
    read_version,
    seconds_since,
    strategy_enabled_key,
)

logger = logging.getLogger("hermes.c2.api")

router = APIRouter()



@router.get("/api/status")
async def get_status() -> Dict[str, Any]:
    last_log_ts = await db.logs.latest_log_ts_async()
    last_log_age = seconds_since(last_log_ts)
    hermes_running = last_log_age is not None and last_log_age <= STALE_AFTER_S

    started_iso = await db.settings.get_setting_async(SETTING_AGENT_STARTED)
    started_at = parse_iso(started_iso)
    uptime_s = seconds_since(started_at) if hermes_running else None

    last_ok = parse_iso(await db.settings.get_setting_async(SETTING_TRADIER_OK_TS))
    tradier_error = (await db.settings.get_setting_async(SETTING_TRADIER_ERROR) or "").strip()
    tradier_ok = (
        seconds_since(last_ok) is not None
        and seconds_since(last_ok) <= STALE_AFTER_S
        and not tradier_error
    )

    llm_error = (await db.settings.get_setting_async(SETTING_LLM_ERROR) or "").strip()
    llm_last_ok = parse_iso(await db.settings.get_setting_async(SETTING_LLM_OK_TS))
    # LLM may not be used every tick (advisory autonomy with no actions);
    # be 4x more lenient than the Tradier window before declaring unhealthy.
    llm_ok = (
        llm_last_ok is not None
        and seconds_since(llm_last_ok) is not None
        and seconds_since(llm_last_ok) <= STALE_AFTER_S * 4
        and not llm_error
    )

    mode = (await db.settings.get_setting_async(SETTING_MODE) or "paper").lower()
    if mode not in VALID_MODES:
        mode = "paper"

    paused = (await db.settings.get_setting_async(SETTING_PAUSED) or "false").lower() == "true"
    approval_mode = (await db.settings.get_setting_async(SETTING_APPROVAL_MODE) or "true").lower() == "true"
    autonomy = (await db.settings.get_setting_async(SETTING_AUTONOMY) or "advisory").lower()
    alpha_autonomous_live = (
        await db.settings.get_setting_async(SETTING_ALPHA_AUTONOMOUS_LIVE) or "false"
    ).lower() == "true"
    pending_count = len(await db.approvals.list_approvals_async(status="PENDING", limit=500))

    strategy_enabled = {}
    for sid in STRATEGIES:
        val = await db.settings.get_setting_async(strategy_enabled_key(sid))
        strategy_enabled[sid] = (val or "true").lower() != "false"

    llm_model = (await db.settings.get_setting_async(SETTING_LLM_MODEL) or "").strip()
    llm_provider = (await db.settings.get_setting_async(SETTING_LLM_PROVIDER) or "mock").strip()

    try:
        mkt = market_session()
        nxt = next_open() if not mkt["is_open"] else None
    except Exception:
        mkt = {"session": "unknown", "is_open": False,
               "et_time": "--:--", "et_date": "", "trading_day": False}
        nxt = None

    update_status_raw = await db.settings.get_setting_async("update_status")
    update_status = None
    if update_status_raw:
        try:
            update_status = json.loads(update_status_raw)
        except Exception as e:
            logger.warning("Failed to parse update_status setting: %s", e)

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
        "autonomy": autonomy,
        "alpha_autonomous_live": alpha_autonomous_live,
        "pending_approvals": pending_count,
        "strategy_enabled": strategy_enabled,
        "stale_after_s": STALE_AFTER_S,
        "tick_interval_s": TICK_INTERVAL_S,
        "version": os.environ.get("HERMES_VERSION") or read_version(),
        "update_status": update_status,
        "market_session": mkt["session"],
        "market_is_open": mkt["is_open"],
        "market_et_time": mkt["et_time"],
        "market_trading_day": mkt["trading_day"],
        "market_next_open": nxt.isoformat() if nxt else None,
    }


@router.get("/api/status/stream")
async def status_stream(request: Request):
    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            try:
                # 1. Fetch status
                status_data = await get_status()

                # 2. Fetch pending approvals (limit 50)
                pending_approvals = await db.approvals.list_approvals_async(status="PENDING", limit=50)

                # 3. Fetch decided/historical approvals (limit 30)
                decided_approvals = await db.approvals.list_approvals_async(limit=30)
                decided_approvals = [r for r in decided_approvals if r.get("status") != "PENDING"][:20]

                # 4. Fetch logs
                raw_logs = await db.logs.recent_logs_async(limit=100)
                logs_list = [{"text": line} for line in (raw_logs or "").splitlines()]

                payload = {
                    "status": status_data,
                    "approvals": pending_approvals + decided_approvals,
                    "logs": logs_list
                }
                
                yield {
                    "event": "message",
                    "data": json.dumps(payload)
                }
            except Exception as e:
                yield {
                    "event": "error",
                    "data": json.dumps({"error": str(e)})
                }

            await asyncio.sleep(2)

    return EventSourceResponse(event_generator())


@router.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "hermes-c2"}


@router.get("/api/logs")
async def get_logs(limit: int = 100) -> List[Dict[str, Any]]:
    raw = await db.logs.recent_logs_async(limit=min(limit, 500))
    # recent_logs returns a single concatenated string; the dashboard's
    # activity feed wants one entry per line.
    return [{"text": line} for line in (raw or "").splitlines()]


@router.get("/api/debug")
async def get_debug_info() -> Dict[str, Any]:
    """Counts of bars / predictions / recent logs — used when the dashboard
    looks empty and the operator needs to know whether the agent or the
    DB is at fault."""
    try:
        import xgboost
        xgb_ver = xgboost.__version__
    except ImportError:
        xgb_ver = "MISSING"

    result: Dict[str, Any] = {"xgboost": xgb_ver, "logs": [], "db": {}}

    try:
        daily_cnt, intra_cnt = await db.ts_engine.get_total_bars_count()
        result["db"]["bars_daily"] = daily_cnt
        result["db"]["bars_intraday"] = intra_cnt
    except Exception as e:
        result["db"]["bars_daily"] = 0
        result["db"]["bars_intraday"] = 0
        logger.warning("Failed to get timeseries count: %s", e)

    async with db.AsyncSession() as s:
        from sqlalchemy import text as sa_text
        try:
            res = await s.execute(sa_text("SELECT COUNT(*) FROM predictions"))
            result["db"]["predictions"] = res.scalar()
            res_err = await s.execute(sa_text(
                "SELECT value FROM system_settings WHERE key='ml_last_error'"
            ))
            result["db"]["ml_last_error"] = res_err.scalar()
        except Exception as e:
            result["db"]["error"] = str(e)

        try:
            res_raw = await s.execute(sa_text(
                "SELECT message FROM bot_logs ORDER BY ts DESC LIMIT 10"
            ))
            raw = res_raw.fetchall()
            result["logs"] = [r[0] for r in raw]
        except Exception as e:
            logger.exception("Failed to query recent bot logs for status endpoint")
            result["logs"] = []

        try:
            from hermes.ml.pop_calibration import POP_CAL_STATE_KEY
            res_cal = await s.execute(sa_text(
                "SELECT value FROM system_settings WHERE key=:k"
            ), {"k": POP_CAL_STATE_KEY})
            cal_raw = res_cal.scalar()
            result["db"]["pop_calibration"] = json.loads(cal_raw) if cal_raw else None
        except Exception:
            result["db"]["pop_calibration"] = None

    return result
