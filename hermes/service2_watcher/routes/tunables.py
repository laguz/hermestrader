"""Strategy tunables — read the catalog and override any value live.

Routes
------
- ``GET /api/tunables``  — every tunable with its metadata + current value
- ``PUT /api/tunables``  — set one tunable (validated against catalog bounds)

Values are stored in ``system_settings``; the agent's tick loop resolves them
each iteration via ``hermes.service1_agent.tunables.resolve``, so a change
takes effect within ``HERMES_TICK_INTERVAL`` (default 5 min) without a deploy.
This is the operator-facing surface for the parameters the four strategies
read at entry and management time.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.service1_agent.tunables import TUNABLES, catalog, groups

from .._app_state import db

router = APIRouter()


@router.get("/api/tunables")
async def get_tunables() -> Dict[str, Any]:
    """Return the catalog grouped, each entry carrying its current value.

    ``value`` is the override stored in ``system_settings`` coerced to the
    tunable's type, or the spec default when no override exists — i.e. exactly
    what the agent would resolve (absent an env-config override, which the
    panel does not surface).
    """
    stored = await db.settings.get_settings(list(TUNABLES.keys()))
    by_group: Dict[str, List[Dict[str, Any]]] = {g: [] for g in groups()}
    for entry in catalog():
        spec = TUNABLES[entry["key"]]
        raw = stored.get(entry["key"])
        entry["value"] = spec.coerce(raw) if raw is not None else spec.default
        entry["overridden"] = raw is not None
        by_group[entry["group"]].append(entry)
    return {"groups": groups(), "tunables": by_group}


class TunableBody(BaseModel):
    key: str
    value: float


@router.put("/api/tunables")
async def set_tunable(body: TunableBody) -> Dict[str, Any]:
    spec = TUNABLES.get(body.key)
    if spec is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown tunable {body.key!r}. See GET /api/tunables.",
        )
    # Coerce to the declared type so e.g. an int tunable rejects 0.5 cleanly
    # and is stored without a spurious '.0'.
    try:
        value = spec.cast(body.value)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=400,
            detail=f"{body.key} must be {'an integer' if spec.cast is int else 'a number'}.",
        )
    if spec.min is not None and value < spec.min:
        raise HTTPException(status_code=400, detail=f"{body.key} must be ≥ {spec.min}.")
    if spec.max is not None and value > spec.max:
        raise HTTPException(status_code=400, detail=f"{body.key} must be ≤ {spec.max}.")

    await db.settings.set_setting(body.key, str(value))
    await db.logs.write_log("ENGINE", f"[C2] Tunable {body.key} set to {value}")
    return {"key": body.key, "value": value, "group": spec.group}
