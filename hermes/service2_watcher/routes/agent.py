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
def pause_agent() -> Dict[str, Any]:
    db.set_setting(SETTING_PAUSED, "true")
    db.write_log("ENGINE", "[C2] Agent PAUSED by operator")
    return {"paused": True}


@router.post("/api/agent/resume")
def resume_agent() -> Dict[str, Any]:
    db.set_setting(SETTING_PAUSED, "false")
    db.write_log("ENGINE", "[C2] Agent RESUMED by operator")
    return {"paused": False}


@router.post("/api/ml/trigger")
def trigger_ml_predictor() -> Dict[str, Any]:
    """Force the XGBoost background thread to retrain and predict immediately."""
    db.set_setting("ml_force_run", "true")
    db.write_log("ENGINE", "[C2] XGBoost ML Predictor manual trigger activated")
    return {"status": "triggered"}


class ModeBody(BaseModel):
    mode: str


@router.put("/api/mode")
def set_mode(body: ModeBody) -> Dict[str, Any]:
    m = body.mode.lower().strip()
    if m not in VALID_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"mode must be one of {list(VALID_MODES)}",
        )
    db.set_setting(SETTING_MODE, m)
    db.write_log("ENGINE", f"[C2] Mode switched to {m}")
    return {"mode": m}
