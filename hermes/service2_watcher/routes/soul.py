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
async def get_soul() -> Dict[str, Any]:
    soul = await db.get_setting(SETTING_SOUL) or ""
    autonomy = (await db.get_setting(SETTING_AUTONOMY) or "advisory").lower()
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
async def set_soul(body: SoulBody) -> Dict[str, Any]:
    if body.soul is not None:
        if len(body.soul.encode()) > MAX_SOUL_BYTES:
            raise HTTPException(
                status_code=400,
                detail=f"Soul exceeds {MAX_SOUL_BYTES // 1024}KB limit",
            )
        await db.set_setting(SETTING_SOUL, body.soul)
        await db.write_log("ENGINE", f"[C2] Soul updated ({len(body.soul.encode())}B)")
        import os
        soul_path = os.environ.get("HERMES_SOUL_PATH", "/app/soul.md")
        try:
            with open(soul_path, "w", encoding="utf-8") as f:
                f.write(body.soul)
            
            # Trigger Git Sync
            try:
                import git
                repo_path = os.path.dirname(os.path.abspath(soul_path))
                if os.path.exists(os.path.join(repo_path, ".git")):
                    repo = git.Repo(repo_path)
                    
                    # Defensively set Git user configuration if missing
                    with repo.config_writer() as cw:
                        if not cw.has_option("user", "name"):
                            cw.set_value("user", "name", "HermesTrader C2")
                        if not cw.has_option("user", "email"):
                            cw.set_value("user", "email", "hermes@trader.c2")
                    
                    repo.git.add("soul.md")
                    if repo.is_dirty(index=True, working_tree=False, path="soul.md"):
                        try:
                            branch_name = repo.active_branch.name
                        except TypeError:
                            branch_name = "detached-head"
                        
                        commit_msg = f"docs: update operator doctrine (soul.md) on branch {branch_name}"
                        repo.index.commit(commit_msg)
                        await db.write_log("ENGINE", f"[GIT] Committed soul.md: {commit_msg}")
                        
                        try:
                            origin = repo.remote(name="origin")
                            origin.push()
                            await db.write_log("ENGINE", f"[GIT] Pushed soul.md changes to origin/{branch_name}")
                        except Exception as push_err:
                            await db.write_log("ENGINE", f"[GIT WARNING] Failed to push to remote: {push_err}", level="WARNING")
                    else:
                        await db.write_log("ENGINE", "[GIT] No changes detected in soul.md for commit")
            except Exception as git_err:
                await db.write_log("ENGINE", f"[GIT WARNING] Git sync failed: {git_err}", level="WARNING")

        except Exception as exc:
            await db.write_log("ENGINE", f"Failed to write soul to file path {soul_path}: {exc}", level="WARNING")

    if body.autonomy is not None:
        a = body.autonomy.lower().strip()
        if a not in VALID_AUTONOMY:
            raise HTTPException(
                status_code=400,
                detail=f"autonomy must be one of {list(VALID_AUTONOMY)}",
            )
        await db.set_setting(SETTING_AUTONOMY, a)
        await db.write_log("ENGINE", f"[C2] Autonomy set to {a}")

    return await get_soul()
