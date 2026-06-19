"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer
A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions, may VETO
or MODIFY them, and may PROPOSE new ones from chart-image analysis. The class is
provider-agnostic — `LLMClient` is any object with `.chat(messages, images=...)`.

``overseer.py`` is the spine: it owns construction + wiring, the shared prompt /
LLM transport (``get_system_prompt`` / ``_chat_with_timeout`` /
``_chat_with_retry`` / ``_safe_json``), and the per-action ``review`` path. The
heavier concerns are owned collaborators, each in its own module:

- review verdict  → :class:`~.overseer_single.SingleReviewer` /
  :class:`~.overseer_committee.CommitteeReviewer`
- autonomous origination + chart reads
  → :class:`~.overseer_proposers.OverseerProposers`
- out-of-loop settings tuning
  → :class:`~.overseer_governance.OverseerGovernor`
- event-bus background worker → :class:`~.overseer_worker.ReviewWorker`

Each collaborator takes a back-reference to this overseer and reuses its state
and transport, so the public method surface (``propose``, ``propose_closes``,
``propose_alpha_setup``, ``analyze_charts``, ``propose_parameter_adjustments``,
``propose_risk_restrictions``, ``start``, ``stop``) is preserved here as thin
delegators.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from hermes.events.bus import EventBus, ReviewRequestEvent
from .core import TradeAction
from .overseer_committee import CommitteeReviewer
from .overseer_governance import OverseerGovernor
from .overseer_proposers import OverseerProposers
from .overseer_single import SingleReviewer
from .overseer_worker import ReviewWorker

logger = logging.getLogger("hermes.agent.overseer")


class HermesOverseer:
    """Visual + statistical override layer above the rules engine."""

    BASE_SYSTEM_PROMPT = (
        "You are HERMES, a quantitative options-trading overseer. "
        "You review trade actions produced by rule-based strategies and decide: "
        "APPROVE / VETO / MODIFY. You also propose new trades when chart context "
        "shows superior setups or imminent risks the rules missed. "
        "Output strict JSON."
    )

    # Token budget for log context: ~2 000 tokens ≈ 8 000 chars. Keeps cheap
    # local LLMs from overflowing their context window on vision prompts.
    _MAX_LOG_CHARS = 8_000
    # Retry policy for transient LLM failures (network blip, timeout).
    _LLM_MAX_RETRIES = 3

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
        self.llm = llm_client
        self.db = db
        self.vision_enabled = vision_enabled
        self.chart_provider = chart_provider
        self.autonomy = autonomy
        self.soul = (soul or "").strip()
        self.overseer_mode = overseer_mode
        self.event_bus = event_bus
        # Owned collaborators, routed to from the thin delegators below. Each
        # reads this overseer's state and reuses its LLM transport via a
        # back-reference (see the module docstring for the split):
        #   committee/single — the two review paths (single is also the
        #                      committee's own failure fallback)
        #   proposers        — autonomous origination + chart reads
        #   governor         — out-of-loop settings tuning
        #   worker           — the event-bus background review worker
        self.committee = CommitteeReviewer(self)
        self.single = SingleReviewer(self)
        self.proposers = OverseerProposers(self)
        self.governor = OverseerGovernor(self)
        self.worker = ReviewWorker(self)

    async def _chat_with_timeout(self, messages: List[Dict[str, str]], images: List[Any] = None) -> str:
        """Call the LLM with a strict timeout gate to prevent hanging."""
        timeout_val = getattr(self.llm, "timeout_s", 15.0)
        # Safeguard: if self.llm is a MagicMock, getattr returns a mock object, which is not a float/int
        if not isinstance(timeout_val, (int, float)):
            timeout_s = 15.0
        else:
            timeout_s = timeout_val or 15.0

        return await asyncio.wait_for(
            asyncio.to_thread(self.llm.chat, messages, images=images or []),
            timeout=timeout_s
        )

    async def get_system_prompt(self) -> str:
        """Base instructions + market session context + operator doctrine + strategy metrics."""
        try:
            from hermes.market_hours import session_label
            mkt_line = session_label()
        except Exception:                                        # noqa: BLE001
            mkt_line = ""

        parts = [self.BASE_SYSTEM_PROMPT]
        if mkt_line:
            parts.append(f"\nCURRENT MARKET STATUS: {mkt_line}")

        try:
            if self.db is not None:
                perf_metrics = await self.db.analytics.get_strategy_performance_metrics(days=30)
                perf_lines = []
                for strat, data in perf_metrics.items():
                    perf_lines.append(
                        f"- {strat}: status={data['status']}, closed={data['closed_trades']}, "
                        f"passed={data['passed']}, failed={data['failed']}, total_pnl=${data['total_pnl']:.2f}"
                    )
                perf_str = "\n".join(perf_lines)
                parts.append(f"\nRECENT STRATEGY PERFORMANCE (30-DAY WINDOW):\n{perf_str}")
        except Exception as exc:
            logger.warning("Failed to fetch performance metrics for SYSTEM_PROMPT: %s", exc)

        if self.soul:
            parts.append(
                "\n--- OPERATOR DOCTRINE (soul.md) ---\n"
                f"{self.soul}\n"
                "--- END DOCTRINE ---"
            )
        return "\n".join(parts)

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
        """Call the LLM with a strict timeout and automatic retry logic."""
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(self._LLM_MAX_RETRIES):
            try:
                return await self._chat_with_timeout(messages, images=images)
            except (asyncio.TimeoutError, Exception) as exc:
                last_exc = exc
                if attempt < self._LLM_MAX_RETRIES - 1:
                    wait_s = 2 ** attempt          # 1 s, 2 s
                    logger.warning(
                        "LLM attempt %d/%d failed/timed out; retrying in %ds: %s",
                        attempt + 1, self._LLM_MAX_RETRIES, wait_s, exc,
                    )
                    await asyncio.sleep(wait_s)
        raise last_exc

    async def _consult(self, action: TradeAction) -> Dict[str, Any]:
        """Routes review to the owned reviewer for the active overseer_mode."""
        if self.overseer_mode == "committee":
            return await self.committee.consult(action)
        return await self.single.consult(action)

    async def _consult_single(self, action: TradeAction) -> Dict[str, Any]:
        """Thin delegator preserving the internal surface (the committee's
        failure fallback calls this). The body now lives on
        :class:`SingleReviewer`."""
        return await self.single.consult(action)

    @staticmethod
    def _safe_json(text: str) -> Dict[str, Any]:
        if isinstance(text, dict):
            return text
        if not isinstance(text, str):
            return {"verdict": "APPROVE", "rationale": "Non-string LLM reply; defaulting."}

        clean_text = text.strip()
        if "```" in clean_text:
            parts = clean_text.split("```")
            for i in range(1, len(parts), 2):
                block = parts[i].strip()
                if block.startswith("json"):
                    block = block[4:].strip()
                start, end = block.find("{"), block.rfind("}")
                if start >= 0 and end > start:
                    try:
                        return json.loads(block[start:end + 1])
                    except Exception:                                  # noqa: BLE001
                        pass

        try:
            return json.loads(clean_text)
        except Exception:                                              # noqa: BLE001
            # Try to find a JSON block embedded in prose
            start, end = clean_text.find("{"), clean_text.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(clean_text[start:end + 1])
                except Exception:                                      # noqa: BLE001
                    pass
        return {"verdict": "APPROVE", "rationale": "Unparseable LLM reply; defaulting."}

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
