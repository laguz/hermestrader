"""Per-strategy enable + lot configuration.

Routes
------
- ``PUT /api/strategies/{strategy_id}``   — toggle one strategy on/off
- ``GET /api/lots``                       — read per-strategy target/max lot sizes
- ``PUT /api/lots``                       — update target/max lots for a strategy

The agent's tick loop reads both groups of settings each iteration, so
changes take effect within ``HERMES_TICK_INTERVAL`` (default 5 min).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.common import STRATEGIES

from .._app_state import db, strategy_enabled_key

router = APIRouter()


# ── Strategy on/off ──────────────────────────────────────────────────────────
class StrategyToggleBody(BaseModel):
    enabled: bool


@router.put("/api/strategies/{strategy_id}")
async def toggle_strategy(strategy_id: str, body: StrategyToggleBody) -> Dict[str, Any]:
    _STRATEGIES_BY_UPPER = {k.upper(): k for k in STRATEGIES}
    sid = _STRATEGIES_BY_UPPER.get(strategy_id.upper())
    if sid is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown strategy {strategy_id!r}; valid: {list(STRATEGIES)}",
        )
    await db.commands.enqueue_setting(strategy_enabled_key(sid), "true" if body.enabled else "false")
    await db.logs.write_log(
        "ENGINE",
        f"[C2] Strategy {sid} {'ENABLED' if body.enabled else 'DISABLED'}",
    )
    return {"id": sid, "enabled": body.enabled}


# ── Lot sizing ───────────────────────────────────────────────────────────────
# Setting keys mirror the conf-dict keys strategies read via self.config.get(...).
_LOT_SPECS = {
    "CS75":  {"target": ("cs75_target_lots", 1), "max": ("cs75_max_lots", 1)},
    "CS7":   {"target": ("cs7_target_lots",  1), "max": ("cs7_max_lots",  1)},
    "TT45":  {"target": ("tt45_target_lots", 5), "max": ("tt45_max_lots", 5)},
    "WHEEL": {"target": ("wheel_max_lots",   5), "max": ("wheel_max_lots", 5)},
    # HermesAlpha is max-only (no per-entry target) and self-sizes within
    # alpha_max_lots. It MUST appear here: the C2 dashboard renders a lots
    # control for every watchlisted strategy and indexes this payload, so an
    # omission leaves the UI reading an undefined entry and blanks the view.
    "HermesAlpha": {"target": ("alpha_max_lots", 1), "max": ("alpha_max_lots", 1)},
    "DS0":   {"target": ("ds0_target_lots",  1), "max": ("ds0_max_lots",  1)},
}

# Case-insensitive lookup: every other strategy id is upper-case, but
# HermesAlpha is mixed-case, so PUT can't blindly upper() the incoming id.
_LOT_SPECS_BY_UPPER = {k.upper(): k for k in _LOT_SPECS}


async def _read_lots() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for sid, spec in _LOT_SPECS.items():
        entry: Dict[str, Any] = {}
        for role, (key, default) in spec.items():
            raw = await db.settings.get_setting(key)
            try:
                entry[role] = int(raw) if raw is not None else default
            except (ValueError, TypeError):
                entry[role] = default
            entry[f"{role}_key"] = key
        out[sid] = entry
    return out


@router.get("/api/lots")
async def get_lots() -> Dict[str, Any]:
    return await _read_lots()


class LotBody(BaseModel):
    strategy_id: str
    target_lots: Optional[int] = None
    max_lots: Optional[int] = None


@router.put("/api/lots")
async def set_lots(body: LotBody) -> Dict[str, Any]:
    sid = _LOT_SPECS_BY_UPPER.get(body.strategy_id.upper())
    if sid is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy. Valid: {list(_LOT_SPECS)}",
        )
    spec = _LOT_SPECS[sid]
    changed: List[str] = []
    if body.target_lots is not None:
        if body.target_lots < 1 or body.target_lots > 100:
            raise HTTPException(status_code=400, detail="target_lots must be 1–100")
        await db.commands.enqueue_setting(spec["target"][0], str(body.target_lots))
        changed.append(f"target→{body.target_lots}")
    if body.max_lots is not None:
        if body.max_lots < 1 or body.max_lots > 100:
            raise HTTPException(status_code=400, detail="max_lots must be 1–100")
        await db.commands.enqueue_setting(spec["max"][0], str(body.max_lots))
        changed.append(f"max→{body.max_lots}")
    if changed:
        await db.logs.write_log("ENGINE", f"[C2] {sid} lots updated: {', '.join(changed)}")
    return await _read_lots()
