"""
[Service-1: Hermes-Agent-Core] — Single-LLM reviewer.

Split out of ``overseer.py`` to separate the single-LLM review path from the
multi-agent committee path (:mod:`overseer_committee`). :class:`SingleReviewer`
is an injected collaborator owned by
:class:`~hermes.service1_agent.overseer.HermesOverseer`: it reads the overseer's
state (db, vision/chart) and reuses its LLM transport (``get_system_prompt`` /
``_chat_with_retry`` / ``_safe_json``) through a back-reference, so the method
body moved out of the overseer unchanged.

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
    from .overseer import HermesOverseer

logger = logging.getLogger("hermes.agent.overseer")


class SingleReviewer:
    """Owns the overseer's single-LLM review path.

    Reads overseer state via ``self._ov``; the forwarding properties below let
    the method body keep reading ``self.db`` / ``self.vision_enabled`` /
    ``self._safe_json`` etc. unchanged, so the extraction from the inline
    ``_consult_single`` was a move, not a rewrite.
    """

    def __init__(self, overseer: "HermesOverseer") -> None:
        self._ov = overseer

    # ── forwarded overseer handles (single source of truth on the overseer) ──
    @property
    def db(self):
        return self._ov.db

    @property
    def vision_enabled(self):
        return self._ov.vision_enabled

    @property
    def chart_provider(self):
        return self._ov.chart_provider

    @property
    def _MAX_LOG_CHARS(self):
        return self._ov._MAX_LOG_CHARS

    @property
    def _LLM_MAX_RETRIES(self):
        return self._ov._LLM_MAX_RETRIES

    @property
    def _chat_with_retry(self):
        return self._ov._chat_with_retry

    @property
    def _safe_json(self):
        return self._ov._safe_json

    @property
    def get_system_prompt(self):
        return self._ov.get_system_prompt

    async def consult(self, action: "TradeAction") -> Dict[str, Any]:
        prompt = (
            "Review this trade action against general market context, the recent "
            "execution log, and (if attached) the underlying's chart. "
            "Reply with JSON {verdict: APPROVE|VETO|MODIFY, rationale, modifications?}.\n"
            f"ACTION:\n{json.dumps(asdict(action), default=str)}\n"
        )
        recent_logs = await self.db.logs.recent_logs(limit=200)
        # Enforce token budget: truncate from the front (oldest entries dropped).
        if len(recent_logs) > self._MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self._MAX_LOG_CHARS:]
        prompt += f"RECENT_LOGS:\n{recent_logs}\n"
        images = []
        if self.vision_enabled and self.chart_provider is not None:
            try:
                img = await self.chart_provider.snapshot(action.symbol)
                if img is not None:
                    images.append(img)
            except Exception:                                          # noqa: BLE001
                pass

        system_prompt = await self.get_system_prompt()
        messages = [{"role": "system", "content": system_prompt},
                    {"role": "user",   "content": prompt}]
        try:
            msg = await self._chat_with_retry(messages, images=images)
            # Clear any stored LLM error on success.
            try:
                await self.db.settings.set_setting("llm_last_error", "")
                await self.db.settings.set_setting(
                    "llm_last_ok_ts",
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                )
            except Exception:                                      # noqa: BLE001
                pass
            return self._safe_json(msg)
        except Exception as last_exc:
            logger.warning("Single-LLM call failed after %d attempts — passing action through: %s",
                           self._LLM_MAX_RETRIES, last_exc)
            try:
                await self.db.settings.set_setting("llm_last_error", (str(last_exc) or repr(last_exc))[:500])
            except Exception:                                              # noqa: BLE001
                pass
            # Fail-safe: pass action through but flag so the operator can see it.
            return {
                "verdict": "APPROVE",
                "rationale": f"LLM unavailable after {self._LLM_MAX_RETRIES} attempts ({last_exc or repr(last_exc)}); defaulting to APPROVE.",
                "llm_error_fallback": True,
            }
