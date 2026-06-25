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

from hermes.common import (
    IPC_ACTION_SYNC_SETTINGS,
    IPC_ACTION_TRIGGER_ML,
    IPC_CHANNEL_AGENT_COMMANDS,
    VALID_MODES,
)

from .._app_state import (
    SETTING_ALPHA_AUTONOMOUS_LIVE,
    SETTING_AUTONOMY,
    SETTING_MODE,
    SETTING_PAUSED,
    db,
)

router = APIRouter()


@router.post("/api/agent/pause")
async def pause_agent() -> Dict[str, Any]:
    await db.commands.enqueue_setting(SETTING_PAUSED, "true")
    await db.logs.write_log("ENGINE", "[C2] Agent PAUSED by operator")
    try:
        from hermes.ipc import ipc
        await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, {"action": IPC_ACTION_SYNC_SETTINGS})
    except Exception:
        pass
    return {"paused": True}


@router.post("/api/agent/resume")
async def resume_agent() -> Dict[str, Any]:
    await db.commands.enqueue_setting(SETTING_PAUSED, "false")
    await db.logs.write_log("ENGINE", "[C2] Agent RESUMED by operator")
    try:
        from hermes.ipc import ipc
        await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, {"action": IPC_ACTION_SYNC_SETTINGS})
    except Exception:
        pass
    return {"paused": False}


@router.post("/api/ml/trigger")
async def trigger_ml_predictor() -> Dict[str, Any]:
    """Force the XGBoost background thread to retrain and predict immediately."""
    await db.commands.enqueue_setting("ml_force_run", "true")
    await db.logs.write_log("ENGINE", "[C2] XGBoost ML Predictor manual trigger activated")
    try:
        from hermes.ipc import ipc
        await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, {"action": IPC_ACTION_TRIGGER_ML})
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
    await db.commands.enqueue_setting(SETTING_MODE, m)
    await db.logs.write_log("ENGINE", f"[C2] Mode switched to {m}")
    try:
        from hermes.ipc import ipc
        await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, {"action": IPC_ACTION_SYNC_SETTINGS})
    except Exception:
        pass
    return {"mode": m}


class AlphaAutonomousBody(BaseModel):
    enabled: bool


@router.put("/api/alpha-autonomous")
async def set_alpha_autonomous(body: AlphaAutonomousBody) -> Dict[str, Any]:
    """Arm/disarm the no-human-in-the-loop HermesAlpha auto-execute path.

    This is the operator-facing switch for the scoped carve-out in
    ``_engine_pipeline._execute_or_queue`` (CLAUDE.md safety rule #2). The gate
    requires BOTH ``agent_autonomy == 'autonomous'`` AND ``alpha_autonomous_live``,
    so arming here also sets autonomy to ``autonomous`` — otherwise the switch
    would be a no-op. Disarming only clears the live switch and leaves the
    autonomy level untouched. Paper/live, dry_run, off-hours and the risk engine
    still apply; only the human approval queue is bypassed, and only for Alpha.
    """
    if body.enabled:
        await db.commands.enqueue_setting(SETTING_AUTONOMY, "autonomous")
    await db.commands.enqueue_setting(
        SETTING_ALPHA_AUTONOMOUS_LIVE, "true" if body.enabled else "false"
    )
    await db.logs.write_log(
        "ENGINE",
        f"[C2] Alpha autonomous-live {'ARMED' if body.enabled else 'DISARMED'} by operator"
        + (" (autonomy → autonomous)" if body.enabled else ""),
    )
    try:
        from hermes.ipc import ipc
        await ipc.publish(IPC_CHANNEL_AGENT_COMMANDS, {"action": IPC_ACTION_SYNC_SETTINGS})
    except Exception:
        pass
    return {"alpha_autonomous_live": body.enabled}
