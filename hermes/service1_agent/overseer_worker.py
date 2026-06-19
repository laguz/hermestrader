"""
[Service-1: Hermes-Agent-Core] — Event-bus review worker.

Split out of ``overseer.py`` to separate the overseer's autonomous background
worker — the queue + task that consume ``ReviewRequestEvent``s off the
:class:`~hermes.events.bus.EventBus` and emit ``AIApprovalEvent``s — from the
review logic itself.

:class:`ReviewWorker` owns the queue and worker-task state. It reads live state
(``db``, ``autonomy``) off the shared
:class:`~hermes.service1_agent.overseer_context.OverseerContext` (``self.ctx``),
and is handed the :class:`~hermes.events.bus.EventBus` plus the overseer's
mode-aware review dispatch (``review_fn``) at construction — the two things that
are overseer-level rather than shared context. The overseer keeps thin
``start`` / ``stop`` delegators because that lifecycle is driven from
``main.py``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional

from hermes.events.bus import EventBus, ReviewRequestEvent, AIApprovalEvent

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import TradeAction
    from .overseer_context import OverseerContext

logger = logging.getLogger("hermes.agent.overseer")


class ReviewWorker:
    """Owns the overseer's autonomous event-bus review worker.

    Reads live state off the shared
    :class:`~hermes.service1_agent.overseer_context.OverseerContext`
    (``self.ctx``); the event bus and the mode-aware review dispatch are injected
    directly. The queue + worker-task state lives here.
    """

    def __init__(self, ctx: "OverseerContext", event_bus: Optional[EventBus],
                 review_fn: "Callable[[TradeAction], Awaitable[Dict[str, Any]]]") -> None:
        self.ctx = ctx
        self.event_bus = event_bus
        self._review = review_fn
        self._queue: Optional[asyncio.Queue[ReviewRequestEvent]] = None
        self._worker_task: Optional[asyncio.Task] = None

    @property
    def queue(self) -> asyncio.Queue[ReviewRequestEvent]:
        """Lazy-initialize queue so synchronous tests don't fail due to missing event loop."""
        if self._queue is None:
            self._queue = asyncio.Queue()
        return self._queue

    async def start(self) -> None:
        """Start the autonomous background worker."""
        if self.event_bus is None:
            return
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._run_loop())
            self.event_bus.subscribe(ReviewRequestEvent, self.handle_review_request)
            logger.info("HermesOverseer background worker started.")

            if self.ctx.db is not None:
                try:
                    pending = await self.ctx.db.approvals.fetch_pending_ai_review_actions()
                    if pending:
                        logger.info("Found %d pending AI review(s) in database at startup; enqueuing...", len(pending))
                        from .core import TradeAction
                        for item in pending:
                            try:
                                action = TradeAction(**item["action_json"])
                                event = ReviewRequestEvent(
                                    strategy_id=item["strategy_id"],
                                    symbol=item["symbol"],
                                    trade_action=action,
                                    action_type=item["action_type"],
                                    approval_id=item["id"]
                                )
                                await self.queue.put(event)
                            except Exception as parse_exc:
                                logger.error("Failed to parse pending AI review action id=%d: %s", item["id"], parse_exc)
                except Exception as db_exc:
                    logger.error("Failed to fetch pending AI reviews at startup: %s", db_exc)

    async def stop(self) -> None:
        """Stop the autonomous background worker."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
            logger.info("HermesOverseer background worker stopped.")

    async def handle_review_request(self, event: ReviewRequestEvent) -> None:
        """Puts review requests onto the queue for sequential processing."""
        await self.queue.put(event)

    async def _run_loop(self) -> None:
        """Sequentially processes review requests from the queue."""
        while True:
            try:
                event = await self.queue.get()
                action = event.trade_action

                # Execute LLM review
                decision = await self._review(action)

                # Write to database (advisory/enforcing decision)
                if self.ctx.db is not None:
                    await self.ctx.db.decisions.write_ai_decision(
                        action.strategy_id,
                        action.symbol,
                        self.ctx.autonomy,
                        decision
                    )

                verdict = decision.get("verdict", "APPROVE").upper()
                modifications = decision.get("modifications") or {}
                rationale = decision.get("rationale") or ""

                # Emit AIApprovalEvent onto the event bus
                approval_event = AIApprovalEvent(
                    strategy_id=action.strategy_id,
                    symbol=action.symbol,
                    verdict=verdict,
                    rationale=rationale,
                    modifications=modifications,
                    original_action=action,
                    action_type=getattr(event, "action_type", "entry"),
                    approval_id=getattr(event, "approval_id", None),
                )
                if self.event_bus:
                    self.event_bus.emit(approval_event)

                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in HermesOverseer worker loop: %s", exc, exc_info=True)
