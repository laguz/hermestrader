"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer

A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions and may
VETO or MODIFY them. The class is provider-agnostic — ``LLMClient`` is any object
with ``.chat(messages, images=...)``.

Phase 0 is **review-only**: the overseer sits above the rules engine and trims or
vetoes what the strategies produce; it does not originate trades or tune live
settings. (Autonomous origination and out-of-loop governance were deferred per
``REBUILD.md`` — they earn their way back behind the promotion gate, not before.)

One cohesive class owns the whole overseer surface:

- **live, operator-tunable state** (``llm`` / ``db`` / ``vision_enabled`` /
  ``chart_provider`` / ``autonomy`` / ``soul`` / ``overseer_mode``) as plain
  attributes — ``main.py`` reassigns them live each tick as the operator changes
  settings in the watcher, and plain attributes pick that up with no proxying;
- the **LLM transport** (``get_system_prompt`` / ``_chat_with_timeout`` /
  ``_chat_with_retry`` / ``_safe_json``);
- the **review** path (``review`` → ``_consult`` → ``_consult_single``);
- the **vision chart reads** (``analyze_charts`` — informational only, surfaced
  by the C2 chart routes; never originates trades);
- the **event-bus worker** lifecycle (``start`` / ``stop`` / ``queue``).

Phase 0 ships a *single* review mode. ``_consult`` still routes through the
``overseer_mode`` registry so an unknown or retired mode resolves deterministically
to the single path rather than silently picking an unintended reviewer; a second
mode (committee) earns its way back in by adding a reviewer here, not by being
pre-scaffolded.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from hermes.common import VALID_OVERSEER_MODES, normalize_overseer_mode
from hermes.events.bus import EventBus, ReviewRequestEvent, AIApprovalEvent
from .core import TradeAction

logger = logging.getLogger("hermes.agent.overseer")


class HermesOverseer:
    """Visual + statistical override layer above the rules engine."""

    BASE_SYSTEM_PROMPT = (
        "You are HERMES, a quantitative options-trading overseer. "
        "You review trade actions produced by rule-based strategies and decide: "
        "APPROVE / VETO / MODIFY. "
        "Output strict JSON."
    )

    # Token budget for log context: ~2 000 tokens ≈ 8 000 chars. Keeps cheap
    # local LLMs from overflowing their context window on vision prompts.
    MAX_LOG_CHARS = 8_000
    # Retry policy for transient LLM failures (network blip, timeout).
    LLM_MAX_RETRIES = 3
    # Legacy underscore aliases, kept for backward-compatible external/test access.
    _MAX_LOG_CHARS = MAX_LOG_CHARS
    _LLM_MAX_RETRIES = LLM_MAX_RETRIES

    def __init__(self, llm_client, db, *, vision_enabled: bool = True,
                 chart_provider=None, autonomy: str = "advisory",
                 soul: Optional[str] = None,
                 overseer_mode: str = "single",
                 event_bus: Optional[EventBus] = None):
        """
        autonomy: 'advisory'  → log decisions, never block (default for new deployments)
                  'enforcing' → veto/modify takes effect
                  'autonomous'→ reserved; reviews exactly like 'enforcing' in
                                Phase 0 (autonomous origination is deferred)

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
        # Stripped once on init; later reassignments are taken verbatim, exactly
        # as main.py always has.
        self.soul = (soul or "").strip()
        self.overseer_mode = overseer_mode
        self.event_bus = event_bus

        # Single selection point: ``_consult`` normalizes the live overseer_mode
        # (via hermes.common) and dispatches through this registry. Keys are the
        # canonical modes; the guard fails loudly if the registry ever drifts
        # from the vocabulary in hermes.common.
        self._reviewers: Dict[str, Any] = {
            "single": self._consult_single,
        }
        assert set(self._reviewers) == set(VALID_OVERSEER_MODES), (
            "overseer reviewer registry out of sync with VALID_OVERSEER_MODES: "
            f"{sorted(self._reviewers)} != {sorted(VALID_OVERSEER_MODES)}"
        )

        # Event-bus worker state (lazy queue so synchronous tests don't need a
        # running loop).
        self._queue: Optional[asyncio.Queue[ReviewRequestEvent]] = None
        self._worker_task: Optional[asyncio.Task] = None

    # ── LLM transport ─────────────────────────────────────────────────────────
    async def _chat_with_timeout(self, messages: List[Dict[str, str]],
                                 images: List[Any] = None) -> str:
        """Call the LLM with a strict timeout gate to prevent hanging."""
        timeout_val = getattr(self.llm, "timeout_s", 15.0)
        # Safeguard: if self.llm is a MagicMock, getattr returns a mock object,
        # which is not a float/int.
        if not isinstance(timeout_val, (int, float)):
            timeout_s = 15.0
        else:
            timeout_s = timeout_val or 15.0

        return await asyncio.wait_for(
            asyncio.to_thread(self.llm.chat, messages, images=images or []),
            timeout=timeout_s,
        )

    async def _chat_with_retry(self, messages: List[Dict[str, str]],
                               images: List[Any] = None) -> str:
        """Call the LLM with a strict timeout and automatic retry logic."""
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(self.LLM_MAX_RETRIES):
            try:
                return await self._chat_with_timeout(messages, images=images)
            except (asyncio.TimeoutError, Exception) as exc:
                last_exc = exc
                if attempt < self.LLM_MAX_RETRIES - 1:
                    wait_s = 2 ** attempt          # 1 s, 2 s
                    logger.warning(
                        "LLM attempt %d/%d failed/timed out; retrying in %ds: %s",
                        attempt + 1, self.LLM_MAX_RETRIES, wait_s, exc,
                    )
                    await asyncio.sleep(wait_s)
        raise last_exc

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

    # ── review existing rule-driven actions ───────────────────────────────────
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

    async def _consult(self, action: TradeAction) -> Dict[str, Any]:
        """Routes review to the reviewer for the active overseer_mode.

        The live mode is normalized through ``hermes.common`` (lowercase,
        unknown → default) so the routing layer — not just the settings readers
        — is authoritative; a stray or typo'd mode resolves deterministically
        here rather than silently picking a path.
        """
        mode = normalize_overseer_mode(self.overseer_mode)
        reviewer = self._reviewers.get(mode, self._consult_single)
        return await reviewer(action)

    async def _consult_single(self, action: TradeAction) -> Dict[str, Any]:
        """The single-LLM review path.

        One LLM call reviews the action against market context, the recent
        execution log, and (when vision is enabled) the underlying's chart,
        returning the final APPROVE / VETO / MODIFY verdict. On total LLM failure
        it fails *open* — passing the action through flagged with
        ``llm_error_fallback`` — so a dead LLM never silently blocks the rules
        engine.
        """
        prompt = (
            "Review this trade action against general market context, the recent "
            "execution log, and (if attached) the underlying's chart. "
            "Reply with JSON {verdict: APPROVE|VETO|MODIFY, rationale, modifications?}.\n"
            f"ACTION:\n{json.dumps(asdict(action), default=str)}\n"
        )
        recent_logs = await self.db.logs.recent_logs(limit=200)
        # Enforce token budget: truncate from the front (oldest entries dropped).
        if len(recent_logs) > self.MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self.MAX_LOG_CHARS:]
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
                           self.LLM_MAX_RETRIES, last_exc)
            try:
                await self.db.settings.set_setting("llm_last_error", (str(last_exc) or repr(last_exc))[:500])
            except Exception:                                              # noqa: BLE001
                pass
            # Fail-safe: pass action through but flag so the operator can see it.
            return {
                "verdict": "APPROVE",
                "rationale": f"LLM unavailable after {self.LLM_MAX_RETRIES} attempts ({last_exc or repr(last_exc)}); defaulting to APPROVE.",
                "llm_error_fallback": True,
            }

    # ── vision chart reads ────────────────────────────────────────────────────
    # Informational only — the overseer renders each watchlist symbol's chart and
    # stores the read for the C2 chart routes to surface. It never originates a
    # trade; review is the overseer's only path to the order flow.
    async def analyze_charts(self, watchlist: Iterable[str]) -> None:
        """Run a vision-only read on each symbol's chart and store the result.

        Runs regardless of autonomy level — purely informational.  Results are
        stored back on the chart_provider so the C2 API can surface them.
        """
        if not self.vision_enabled or self.chart_provider is None:
            return
        for symbol in watchlist:
            chart = await self.chart_provider.snapshot(symbol)
            if chart is None:
                continue
            system_prompt = await self.get_system_prompt()
            prompt = (
                f"Analyse this price chart for {symbol}. "
                "Identify the current trend, key support/resistance levels, "
                "any chart patterns (e.g. head-and-shoulders, cup-and-handle, "
                "double top/bottom, flags, wedges), RSI regime (overbought / "
                "oversold / neutral), and whether the Bollinger Band squeeze "
                "suggests an imminent volatility expansion. "
                "Reply with JSON: "
                "{trend, support, resistance, pattern, rsi_regime, bb_squeeze, "
                "outlook, rationale} — all string values."
            )
            try:
                msg = await self._chat_with_timeout(
                    [{"role": "system", "content": system_prompt},
                     {"role": "user",   "content": prompt}],
                    images=[chart],
                )
                analysis = self._safe_json(msg)
                verdict  = analysis.get("outlook", "NEUTRAL").upper()
                rationale = analysis.get("rationale", "")
                self.chart_provider.record_analysis(symbol, verdict, rationale, analysis)
                await self.db.decisions.write_ai_decision(
                    "CHART", symbol, "vision",
                    {"type": "chart_analysis", **analysis},
                )
                logger.info("Chart analysis %s → %s", symbol, verdict)
            except Exception as exc:                                   # noqa: BLE001
                logger.warning("Chart analysis failed for %s: %s", symbol, exc)

    # ── autonomous event-bus review worker ────────────────────────────────────
    # The queue + task that consume ``ReviewRequestEvent``s off the EventBus and
    # emit ``AIApprovalEvent``s. The lifecycle (start/stop) is driven from main.py.
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

            if self.db is not None:
                try:
                    pending = await self.db.approvals.fetch_pending_ai_review_actions()
                    if pending:
                        logger.info("Found %d pending AI review(s) in database at startup; enqueuing...", len(pending))
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
                decision = await self._consult(action)

                # Write to database (advisory/enforcing decision)
                if self.db is not None:
                    await self.db.decisions.write_ai_decision(
                        action.strategy_id,
                        action.symbol,
                        self.autonomy,
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
