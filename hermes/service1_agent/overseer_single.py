"""
[Service-1: Hermes-Agent-Core] — Single-LLM reviewer.

Split out of ``overseer.py`` to separate the single-LLM review path from the
multi-agent committee path (:mod:`overseer_committee`). :class:`SingleReviewer`
takes the shared :class:`~hermes.service1_agent.overseer_context.OverseerContext`
and reads the live state (``db``, ``vision_enabled``, ``chart_provider``) and the
LLM transport (``get_system_prompt`` / ``chat_with_retry`` / ``safe_json``)
through it — one source of truth, no per-collaborator forwarding.

Flow: one LLM call reviews the action against market context, the recent
execution log, and (when vision is enabled) the underlying's chart, returning the
final APPROVE / VETO / MODIFY verdict. On total LLM failure it fails *open* —
passing the action through flagged with ``llm_error_fallback`` — so a dead LLM
never silently blocks the rules engine. The committee path falls back to this
reviewer on any failure, so both modes fail closed identically.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import TradeAction
    from .overseer_context import OverseerContext

logger = logging.getLogger("hermes.agent.overseer")


class SingleReviewer:
    """Owns the overseer's single-LLM review path.

    Reads live state and the LLM transport off the shared
    :class:`~hermes.service1_agent.overseer_context.OverseerContext`
    (``self.ctx``), so there is a single source of truth and no forwarding.
    """

    def __init__(self, ctx: "OverseerContext") -> None:
        self.ctx = ctx

    async def consult(self, action: "TradeAction") -> Dict[str, Any]:
        ctx = self.ctx
        prompt = (
            "Review this trade action against general market context, the recent "
            "execution log, and (if attached) the underlying's chart. "
            "Reply with JSON {verdict: APPROVE|VETO|MODIFY, rationale, modifications?}.\n"
            f"ACTION:\n{json.dumps(asdict(action), default=str)}\n"
        )
        recent_logs = await ctx.db.logs.recent_logs(limit=200)
        # Enforce token budget: truncate from the front (oldest entries dropped).
        if len(recent_logs) > ctx.MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-ctx.MAX_LOG_CHARS:]
        prompt += f"RECENT_LOGS:\n{recent_logs}\n"
        images = []
        if ctx.vision_enabled and ctx.chart_provider is not None:
            try:
                img = await ctx.chart_provider.snapshot(action.symbol)
                if img is not None:
                    images.append(img)
            except Exception:                                          # noqa: BLE001
                pass

        system_prompt = await ctx.get_system_prompt()
        messages = [{"role": "system", "content": system_prompt},
                    {"role": "user",   "content": prompt}]
        try:
            msg = await ctx.chat_with_retry(messages, images=images)
            # Clear any stored LLM error on success.
            try:
                await ctx.db.settings.set_setting("llm_last_error", "")
                await ctx.db.settings.set_setting(
                    "llm_last_ok_ts",
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                )
            except Exception:                                      # noqa: BLE001
                pass
            return ctx.safe_json(msg)
        except Exception as last_exc:
            logger.warning("Single-LLM call failed after %d attempts — passing action through: %s",
                           ctx.LLM_MAX_RETRIES, last_exc)
            try:
                await ctx.db.settings.set_setting("llm_last_error", (str(last_exc) or repr(last_exc))[:500])
            except Exception:                                              # noqa: BLE001
                pass
            # Fail-safe: pass action through but flag so the operator can see it.
            return {
                "verdict": "APPROVE",
                "rationale": f"LLM unavailable after {ctx.LLM_MAX_RETRIES} attempts ({last_exc or repr(last_exc)}); defaulting to APPROVE.",
                "llm_error_fallback": True,
            }
