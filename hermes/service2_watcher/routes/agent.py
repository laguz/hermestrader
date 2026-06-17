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

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.common import VALID_MODES

from .._app_state import SETTING_MODE, SETTING_PAUSED, db

router = APIRouter()

# Outcome-driven learning toggles (Phases 2–3). Both default to "off".
#   off    — dormant; no proposals, no capture, no behaviour change.
#   shadow — track data + log what it *would* do; never acts. ← data-collection.
#   active — additionally apply changes (bandit knobs / exit closes), still
#            gated by agent autonomy (enforcing/autonomous) inside the engine.
LEARNING_MODES = {"off", "shadow", "active"}
BANDIT_MODE_KEY = "bandit_tuner_mode"
EXIT_MODE_KEY = "exit_policy_mode"


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


@router.get("/api/agent/learning")
async def get_learning() -> Dict[str, Any]:
    """Current outcome-driven learning modes (bandit entries + exit policy)."""
    bandit = (await db.get_setting(BANDIT_MODE_KEY) or "off").strip().lower()
    exit_mode = (await db.get_setting(EXIT_MODE_KEY) or "off").strip().lower()
    return {
        "bandit_tuner_mode": bandit,
        "exit_policy_mode": exit_mode,
        "valid_modes": sorted(LEARNING_MODES),
    }


class LearningBody(BaseModel):
    # Either or both; omitted fields are left unchanged.
    bandit_tuner_mode: Optional[str] = None
    exit_policy_mode: Optional[str] = None


@router.put("/api/agent/learning")
async def set_learning(body: LearningBody) -> Dict[str, Any]:
    """Toggle the learning subsystems on/off (off | shadow | active).

    ``shadow`` is the data-collection setting: the bot records the trajectories
    and proposals the learners need without changing any trade. Flip to
    ``active`` only after a shadow period has validated the signal.
    """
    updated: Dict[str, str] = {}
    for key, value in ((BANDIT_MODE_KEY, body.bandit_tuner_mode),
                       (EXIT_MODE_KEY, body.exit_policy_mode)):
        if value is None:
            continue
        m = str(value).strip().lower()
        if m not in LEARNING_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"{key} must be one of {sorted(LEARNING_MODES)}",
            )
        await db.set_setting(key, m)
        updated[key] = m

    if not updated:
        raise HTTPException(
            status_code=400,
            detail="provide bandit_tuner_mode and/or exit_policy_mode",
        )

    await db.write_log("ENGINE", f"[C2] Learning modes updated by operator: {updated}")
    try:
        from hermes.ipc import ipc
        await ipc.publish("agent_commands", {"action": "sync_settings"})
    except Exception:
        pass
    return {"updated": updated}


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
