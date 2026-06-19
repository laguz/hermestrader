"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer
A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions, may VETO
or MODIFY them, and may PROPOSE new ones from chart-image analysis. The class is
provider-agnostic — `LLMClient` is any object with `.chat(messages, images=...)`.

``overseer.py`` is the spine: it owns construction + wiring and the per-action
``review`` path. The shared, operator-tunable state and the LLM transport
(``get_system_prompt`` / ``chat_with_timeout`` / ``chat_with_retry`` /
``safe_json``) live on an :class:`~.overseer_context.OverseerContext`, held here
as ``self.ctx``. The heavier concerns are owned collaborators, each in its own
module:

- review verdict  → :class:`~.overseer_single.SingleReviewer` /
  :class:`~.overseer_committee.CommitteeReviewer`
- autonomous origination + chart reads
  → :class:`~.overseer_proposers.OverseerProposers`
- out-of-loop settings tuning
  → :class:`~.overseer_governance.OverseerGovernor`
- event-bus background worker → :class:`~.overseer_worker.ReviewWorker`

Each collaborator takes the **shared context** (not a back-reference to this
overseer) and reads the live state / transport through it, so there is one
source of truth even though ``main.py`` reconfigures ``autonomy`` / ``soul`` /
``vision_enabled`` / ``overseer_mode`` / ``llm`` live each tick. The public
method surface (``review``, ``propose``, ``propose_closes``,
``propose_alpha_setup``, ``analyze_charts``, ``propose_parameter_adjustments``,
``propose_risk_restrictions``, ``start``, ``stop``) is preserved here, and the
operator-tunable attributes are proxied onto ``self.ctx`` so existing call-sites
(``overseer.autonomy = ...``) keep working unchanged.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Iterable, List, Optional, Protocol

from hermes.common import VALID_OVERSEER_MODES, normalize_overseer_mode
from hermes.events.bus import EventBus, ReviewRequestEvent
from .core import TradeAction
from .overseer_context import OverseerContext
from .overseer_committee import CommitteeReviewer
from .overseer_governance import OverseerGovernor
from .overseer_proposers import OverseerProposers
from .overseer_single import SingleReviewer
from .overseer_worker import ReviewWorker

logger = logging.getLogger("hermes.agent.overseer")


class Reviewer(Protocol):
    """The contract both review paths honor.

    :class:`~.overseer_single.SingleReviewer` and
    :class:`~.overseer_committee.CommitteeReviewer` each expose exactly this:
    one async ``consult`` returning a verdict dict shaped
    ``{"verdict": "APPROVE"|"VETO"|"MODIFY", "rationale": str,
    "modifications"?: dict, ...}``. :meth:`HermesOverseer.review` consumes only
    that shape, so the two paths stay interchangeable.
    """

    async def consult(self, action: TradeAction) -> Dict[str, Any]: ...


class HermesOverseer:
    """Visual + statistical override layer above the rules engine."""

    # Kept as class attributes for backward-compatible external/test access;
    # the live values used by the transport live on :class:`OverseerContext`.
    BASE_SYSTEM_PROMPT = OverseerContext.BASE_SYSTEM_PROMPT
    _MAX_LOG_CHARS = OverseerContext.MAX_LOG_CHARS
    _LLM_MAX_RETRIES = OverseerContext.LLM_MAX_RETRIES

    def __init__(self, llm_client, db, *, vision_enabled: bool = True,
                 chart_provider=None, autonomy: str = "advisory",
                 soul: Optional[str] = None,
                 overseer_mode: str = "single",
                 event_bus: Optional[EventBus] = None):
        """
        autonomy: 'advisory'  → log decisions, never block (default for new deployments)
                  'enforcing' → veto/modify takes effect
                  'autonomous'→ may also originate new actions

        soul: free-text operator doctrine (typically the contents of a
              soul.md the user maintains in the watcher). When non-empty it
              is appended to the base system prompt on every LLM call, so
              the overseer is shaped by both the base instructions and the
              operator's current preferences without anyone touching code.
        """
        # Single source of truth for the live, operator-tunable state and the
        # LLM transport. The operator-tunable attributes below are properties
        # that read/write straight through to this object.
        self.ctx = OverseerContext(
            llm_client, db, vision_enabled=vision_enabled,
            chart_provider=chart_provider, autonomy=autonomy, soul=soul,
            overseer_mode=overseer_mode,
        )
        self.event_bus = event_bus
        # Owned collaborators. Each takes the shared context (not ``self``) and
        # reads live state + transport through it:
        #   committee/single — the two review paths (single is also the
        #                      committee's own failure fallback, injected below)
        #   proposers        — autonomous origination + chart reads
        #   governor         — out-of-loop settings tuning
        #   worker           — the event-bus background review worker (also
        #                      needs the bus + the mode-aware review dispatch)
        self.single = SingleReviewer(self.ctx)
        self.committee = CommitteeReviewer(self.ctx, self.single)
        # Single selection point: ``_consult`` normalizes the live overseer_mode
        # (via hermes.common) and dispatches through this registry. Keys are the
        # canonical modes; the guard fails loudly if the registry ever drifts
        # from the vocabulary in hermes.common (e.g. a new mode added there but
        # left unwired here).
        self._reviewers: Dict[str, Reviewer] = {
            "single": self.single,
            "committee": self.committee,
        }
        assert set(self._reviewers) == set(VALID_OVERSEER_MODES), (
            "overseer reviewer registry out of sync with VALID_OVERSEER_MODES: "
            f"{sorted(self._reviewers)} != {sorted(VALID_OVERSEER_MODES)}"
        )
        self.proposers = OverseerProposers(self.ctx)
        self.governor = OverseerGovernor(self.ctx)
        self.worker = ReviewWorker(self.ctx, self.event_bus, self._consult)

    # ── operator-tunable state proxied to the shared context ──────────────────
    # main.py reconfigures these live each tick; routing them through ctx keeps
    # the collaborators (which read ctx.*) seeing the same single source.
    @property
    def llm(self):
        return self.ctx.llm

    @llm.setter
    def llm(self, val):
        self.ctx.llm = val

    @property
    def db(self):
        return self.ctx.db

    @db.setter
    def db(self, val):
        self.ctx.db = val

    @property
    def vision_enabled(self):
        return self.ctx.vision_enabled

    @vision_enabled.setter
    def vision_enabled(self, val):
        self.ctx.vision_enabled = val

    @property
    def chart_provider(self):
        return self.ctx.chart_provider

    @chart_provider.setter
    def chart_provider(self, val):
        self.ctx.chart_provider = val

    @property
    def autonomy(self):
        return self.ctx.autonomy

    @autonomy.setter
    def autonomy(self, val):
        self.ctx.autonomy = val

    @property
    def soul(self):
        return self.ctx.soul

    @soul.setter
    def soul(self, val):
        self.ctx.soul = val

    @property
    def overseer_mode(self):
        return self.ctx.overseer_mode

    @overseer_mode.setter
    def overseer_mode(self, val):
        self.ctx.overseer_mode = val

    # ── transport delegators (bodies live on OverseerContext) ─────────────────
    # Preserved so existing call-sites / tests that reach the overseer's
    # transport surface keep working; the single implementation is on ctx.
    async def _chat_with_timeout(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        return await self.ctx.chat_with_timeout(messages, images=images)

    async def get_system_prompt(self) -> str:
        return await self.ctx.get_system_prompt()

    # -- review existing rule-driven actions ---------------------------------
    async def review(self, action: TradeAction) -> Optional[TradeAction]:
        if self.autonomy == "advisory":
            decision = await self._consult(action)
            await self.db.decisions.write_ai_decision(action.strategy_id, action.symbol,
                                      "advisory", decision)
            return action

        decision = await self._consult(action)
        await self.db.decisions.write_ai_decision(action.strategy_id, action.symbol,
                                  self.autonomy, decision)

        verdict = decision.get("verdict", "APPROVE").upper()
        if verdict == "VETO":
            return None
        if verdict == "MODIFY" and isinstance(decision.get("modifications"), dict):
            for k, v in decision["modifications"].items():
                if hasattr(action, k):
                    setattr(action, k, v)
            action.ai_authored = True
            action.ai_rationale = decision.get("rationale")
        return action

    # -- autonomous origination + chart reads (→ OverseerProposers) ----------
    async def propose(self, watchlist: Iterable[str]) -> List[TradeAction]:
        return await self.proposers.propose(watchlist)

    async def propose_closes(self) -> List[TradeAction]:
        return await self.proposers.propose_closes()

    async def propose_alpha_setup(
        self, universe: Iterable[str], open_positions: Iterable[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        return await self.proposers.propose_alpha_setup(universe, open_positions)

    async def analyze_charts(self, watchlist: Iterable[str]) -> None:
        return await self.proposers.analyze_charts(watchlist)

    # -- out-of-loop settings tuning (→ OverseerGovernor) --------------------
    async def propose_parameter_adjustments(self) -> Dict[str, Any]:
        return await self.governor.propose_parameter_adjustments()

    async def propose_risk_restrictions(self) -> Dict[str, Any]:
        return await self.governor.propose_risk_restrictions()

    # -- LLM I/O -------------------------------------------------------------
    async def _chat_with_retry(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        return await self.ctx.chat_with_retry(messages, images=images)

    async def _consult(self, action: TradeAction) -> Dict[str, Any]:
        """Routes review to the reviewer for the active overseer_mode.

        The live mode is normalized through ``hermes.common`` (lowercase,
        unknown → default) so the routing layer — not just the settings readers
        — is authoritative; a stray or typo'd mode resolves deterministically
        here rather than silently picking a path.
        """
        mode = normalize_overseer_mode(self.overseer_mode)
        reviewer = self._reviewers.get(mode, self.single)
        return await reviewer.consult(action)

    async def _consult_single(self, action: TradeAction) -> Dict[str, Any]:
        """Thin delegator preserving the internal surface (the committee's
        failure fallback calls this). The body now lives on
        :class:`SingleReviewer`."""
        return await self.single.consult(action)

    # JSON-reply parsing is stateless; the implementation lives on the context.
    # Exposed here (class-callable) for backward-compatible test/external access.
    _safe_json = staticmethod(OverseerContext.safe_json)

    # -- autonomous background worker (→ ReviewWorker) -----------------------
    @property
    def queue(self) -> asyncio.Queue[ReviewRequestEvent]:
        return self.worker.queue

    async def start(self) -> None:
        """Start the autonomous background worker."""
        return await self.worker.start()

    async def stop(self) -> None:
        """Stop the autonomous background worker."""
        return await self.worker.stop()

    async def handle_review_request(self, event: ReviewRequestEvent) -> None:
        """Puts review requests onto the queue for sequential processing."""
        return await self.worker.handle_review_request(event)
