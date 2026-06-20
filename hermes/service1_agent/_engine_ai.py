"""
[Service-1: Hermes-Agent-Core] — overseer AI controller.

Split out of ``core.py`` so the engine spine stays readable. ``AIController`` is
an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.ai``). In Phase 0 the overseer is **review-only**, so this controller
owns exactly one concern: executing the verdict after an AI review completes
(``AIApprovalEvent`` → submit/veto). Autonomous origination, overseer-authored
closes, and out-of-loop parameter/risk tuning were deferred (see ``REBUILD.md``),
so none of that wiring lives here.

It reads everything it needs — the shared dependency surface (``db`` /
``broker`` / ``event_bus`` / ``config`` / ``overseer`` / ``mm`` / ``clock`` /
``quote_cache``) — off the
:class:`~hermes.service1_agent.engine_context.EngineContext` (``self.ctx``) and
emits its results back onto the event bus, so it needs no back-reference to the
engine at all.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from hermes.events.bus import (
    AIApprovalEvent,
    ExecuteAIApprovalCommand,
    SubmitTradeActionsCommand,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class AIController:
    """Executes the engine-side verdict after the overseer reviews an action."""

    def __init__(self, engine: "CascadingEngine") -> None:
        # AIController depends only on the shared dependency surface, not on the
        # engine spine — it reads ``self.ctx`` and emits back onto the event bus.
        self.ctx = engine.ctx

        if self.ctx.event_bus is not None:
            self.ctx.event_bus.subscribe(ExecuteAIApprovalCommand, self.handle_execute_ai_approval)

    # ── overseer AI proposals / closes / gating ──────────────────────────────
    async def handle_execute_ai_approval(self, command: ExecuteAIApprovalCommand) -> None:
        try:
            res = await self._handle_ai_approval_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def _handle_ai_approval_internal(self, event: AIApprovalEvent) -> None:
        """Asynchronously executes or queues an action after AI approval."""
        a = event.original_action
        if a is None:
            logger.warning("AIApprovalEvent has no original_action; skipping.")
            return

        if event.verdict == "VETO":
            logger.info("[AI VETOED] Strategy=%s symbol=%s - %s", event.strategy_id, event.symbol, event.rationale)
            await self.ctx.db.logs.write_log(
                event.strategy_id,
                f"[AI VETOED] {event.symbol} — {event.rationale}"
            )
            if event.approval_id is not None:
                await self.ctx.db.approvals.update_approval_status(event.approval_id, "REJECTED", notes=event.rationale)

            ttl = int(self.ctx.config.get("veto_suppression_s", 1800))
            if ttl > 0:
                veto_side = (a.strategy_params or {}).get("side_type")
                if veto_side and str(veto_side).lower() in {"buy", "sell"}:
                    veto_side = None
                try:
                    hits = await self.ctx.db.approvals.record_veto(
                        event.strategy_id, event.symbol, veto_side,
                        a.expiry, event.rationale, ttl)
                    logger.info("[VETO] suppression recorded for %s (hits=%d, ttl=%ds)",
                                event.symbol, hits, ttl * hits)
                except Exception:
                    logger.exception("[VETO] record_veto failed for %s", event.symbol)
            return

        if event.verdict == "MODIFY":
            if event.modifications:
                for k, v in event.modifications.items():
                    if hasattr(a, k):
                        setattr(a, k, v)
                a.ai_authored = True
                a.ai_rationale = event.rationale

        cmd = SubmitTradeActionsCommand(
            actions=[a],
            action_type=getattr(event, "action_type", "entry"),
            approval_id=getattr(event, "approval_id", None),
            execute_directly=True
        )
        self.ctx.event_bus.emit(cmd)
        await cmd.future
