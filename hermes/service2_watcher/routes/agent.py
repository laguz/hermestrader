"""Agent lifecycle controls.

Routes
------
- ``POST /api/agent/pause``   — set agent_paused=true (skips engine ticks)
- ``POST /api/agent/resume``  — set agent_paused=false
- ``POST /api/ml/trigger``    — force the XGBoost background thread to
                                 retrain + predict on the next 10s wake
- ``PUT  /api/mode``          — switch paper ↔ live (agent reconciles next tick)

All four are simple system_settings writes; the agent's tick loop reads
them at the start of every iteration.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.common import VALID_MODES

from .._app_state import SETTING_MODE, SETTING_PAUSED, db

router = APIRouter()


@router.post("/api/agent/pause")
async def pause_agent() -> Dict[str, Any]:
    await db.set_setting(SETTING_PAUSED, "true")
    await db.write_log("ENGINE", "[C2] Agent PAUSED by operator")
    try:
        from hermes.ipc import ipc
        await ipc.publish("agent_commands", {"action": "sync_settings"})
    except Exception:
        pass
    return {"paused": True}


@router.post("/api/agent/resume")
async def resume_agent() -> Dict[str, Any]:
    await db.set_setting(SETTING_PAUSED, "false")
    await db.write_log("ENGINE", "[C2] Agent RESUMED by operator")
    try:
        from hermes.ipc import ipc
        await ipc.publish("agent_commands", {"action": "sync_settings"})
    except Exception:
        pass
    return {"paused": False}


@router.post("/api/ml/trigger")
async def trigger_ml_predictor() -> Dict[str, Any]:
    """Force the XGBoost background thread to retrain and predict immediately."""
    await db.set_setting("ml_force_run", "true")
    await db.write_log("ENGINE", "[C2] XGBoost ML Predictor manual trigger activated")
    try:
        from hermes.ipc import ipc
        await ipc.publish("agent_commands", {"action": "trigger_ml"})
    except Exception:
        pass
    return {"status": "triggered"}


class ModeBody(BaseModel):
    mode: str


@router.put("/api/mode")
async def set_mode(body: ModeBody) -> Dict[str, Any]:
    m = body.mode.lower().strip()
    if m not in VALID_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"mode must be one of {list(VALID_MODES)}",
        )
    await db.set_setting(SETTING_MODE, m)
    await db.write_log("ENGINE", f"[C2] Mode switched to {m}")
    try:
        from hermes.ipc import ipc
        await ipc.publish("agent_commands", {"action": "sync_settings"})
    except Exception:
        pass
    return {"mode": m}
