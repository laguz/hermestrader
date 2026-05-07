"""Operator doctrine ("soul") + autonomy level.

Routes
------
- ``GET /api/soul``   — read current soul + autonomy
- ``PUT /api/soul``   — update soul and/or autonomy

The ``soul_md`` system_setting is appended to the LLM overseer's system
prompt verbatim. ``agent_autonomy`` controls whether the overseer's
verdict is logged only (``advisory``), enforced (``enforcing``), or also
allowed to originate new trades (``autonomous``).

Soul payloads are capped at 64 KB to keep prompt latency predictable.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.common import VALID_AUTONOMY

from .._app_state import (
    MAX_SOUL_BYTES,
    SETTING_AUTONOMY,
    SETTING_SOUL,
    db,
)

router = APIRouter()


@router.get("/api/soul")
def get_soul() -> Dict[str, Any]:
    soul = db.get_setting(SETTING_SOUL) or ""
    autonomy = (db.get_setting(SETTING_AUTONOMY) or "advisory").lower()
    if autonomy not in VALID_AUTONOMY:
        autonomy = "advisory"
    return {
        "soul": soul,
        "autonomy": autonomy,
        "valid_autonomy": list(VALID_AUTONOMY),
        "max_bytes": MAX_SOUL_BYTES,
        "current_bytes": len(soul.encode()),
    }


class SoulBody(BaseModel):
    soul: Optional[str] = None
    autonomy: Optional[str] = None


@router.put("/api/soul")
def set_soul(body: SoulBody) -> Dict[str, Any]:
    if body.soul is not None:
        if len(body.soul.encode()) > MAX_SOUL_BYTES:
            raise HTTPException(
                status_code=400,
                detail=f"Soul exceeds {MAX_SOUL_BYTES // 1024}KB limit",
            )
        db.set_setting(SETTING_SOUL, body.soul)
        db.write_log("ENGINE", f"[C2] Soul updated ({len(body.soul.encode())}B)")

    if body.autonomy is not None:
        a = body.autonomy.lower().strip()
        if a not in VALID_AUTONOMY:
            raise HTTPException(
                status_code=400,
                detail=f"autonomy must be one of {list(VALID_AUTONOMY)}",
            )
        db.set_setting(SETTING_AUTONOMY, a)
        db.write_log("ENGINE", f"[C2] Autonomy set to {a}")

    return get_soul()
