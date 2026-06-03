"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer
A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions, may VETO
or MODIFY them, and may PROPOSE new ones from chart-image analysis. The class is
provider-agnostic — `LLMClient` is any object with `.chat(messages, images=...)`.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from hermes.events.bus import EventBus, ReviewRequestEvent, AIApprovalEvent
from .core import TradeAction

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

    def __init__(self, llm_client, db, *, vision_enabled: bool = True,
                 chart_provider=None, autonomy: str = "advisory",
                 soul: Optional[str] = None,
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
        self.event_bus = event_bus
        self._queue: Optional[asyncio.Queue[ReviewRequestEvent]] = None
        self._worker_task: Optional[asyncio.Task] = None

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
                perf_metrics = await self.db.get_strategy_performance_metrics(days=30)
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
            await self.db.write_ai_decision(action.strategy_id, action.symbol,
                                      "advisory", decision)
            return action

        decision = await self._consult(action)
        await self.db.write_ai_decision(action.strategy_id, action.symbol,
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

    # -- propose new actions (vision-driven) ---------------------------------
    async def propose(self, watchlist: Iterable[str]) -> List[TradeAction]:
        if self.autonomy != "autonomous":
            return []
        proposed: List[TradeAction] = []
        for symbol in watchlist:
            chart = await self.chart_provider.snapshot(symbol) if self.chart_provider else None
            payload = await self._propose_for(symbol, chart)
            if not payload:
                continue
            try:
                a = TradeAction(**payload, ai_authored=True)
                proposed.append(a)
            except Exception as exc:                                   # noqa: BLE001
                logger.warning("Overseer proposal malformed for %s: %s", symbol, exc)
        return proposed

    # -- goal-aware parameter tuning -----------------------------------------
    # Allow-list of live-tunable settings the overseer may adjust toward the
    # operator's goal, each as (kind, lo, hi). This is a hard safety boundary:
    # the LLM can nudge these knobs, but can neither invent new settings nor
    # push a value outside its range. Gate-threshold bounds are sourced from
    # entry_gate so the gate and the tuner never drift apart.
    @staticmethod
    def _tunable_params() -> Dict[str, tuple]:
        from .entry_gate import BOUNDS as GATE_BOUNDS
        params: Dict[str, tuple] = {
            # DTE knobs the strategies already read live from system_settings.
            "cs7_dte": ("int", 5, 10),
            "cs75_min_dte": ("int", 30, 45),
            "cs75_max_dte": ("int", 35, 60),
        }
        # AI-entry-gate stringency — "adjust your approval stringency" (soul.md).
        for key, (lo, hi) in GATE_BOUNDS.items():
            kind = "int" if key in ("ai_gate_min_dte", "ai_gate_max_dte") else "float"
            params[key] = (kind, lo, hi)
        return params

    async def propose_parameter_adjustments(self) -> Dict[str, Any]:
        """Let the overseer tune sanctioned knobs toward the operator's goal.

        Rather than inventing arbitrary entry points, the overseer adjusts a
        bounded set of live parameters (DTE windows + AI-gate stringency) in
        response to recent strategy performance and the doctrine in soul.md.
        Only ``enforcing`` / ``autonomous`` modes take effect — advisory never
        mutates live settings.

        Every proposed change is clamped to its allow-listed range and coerced
        to its declared type before it is written; out-of-list keys are ignored.
        Returns ``{"applied": {...}, "rationale": str, "skipped": [...]}``.
        """
        if self.autonomy not in ("enforcing", "autonomous"):
            return {"applied": {}, "rationale": "advisory mode — no changes", "skipped": []}

        tunables = self._tunable_params()
        current: Dict[str, Any] = {}
        for key, (kind, lo, _hi) in tunables.items():
            raw = await self.db.get_setting(key)
            if raw is None:
                # Surface the in-effect default so the LLM sees a real baseline.
                from .entry_gate import DEFAULTS as GATE_DEFAULTS
                raw = GATE_DEFAULTS.get(key)
            current[key] = raw

        bounds_desc = {
            k: f"{kind}[{lo}..{hi}]" for k, (kind, lo, hi) in tunables.items()
        }
        system_prompt = await self.get_system_prompt()
        prompt = (
            "Review recent strategy performance against the operator doctrine "
            "and propose adjustments to the TUNABLE PARAMETERS that move the "
            "system toward the stated goal (capital preservation + consistent "
            "positive returns). Tighten stringency (higher POP, lower delta "
            "cap, higher min-credit) for strategies that recently FAILED; you "
            "may relax modestly only for consistent PASSers.\n"
            f"CURRENT VALUES: {json.dumps(current, default=str)}\n"
            f"ALLOWED RANGES: {json.dumps(bounds_desc)}\n"
            "Reply with strict JSON {adjustments: {key: number, ...}, rationale}. "
            "Only include keys you want to change. Omit anything you'd leave as-is."
        )
        try:
            msg = await asyncio.to_thread(
                self.llm.chat,
                [{"role": "system", "content": system_prompt},
                 {"role": "user", "content": prompt}],
                images=[],
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("Parameter-tuning LLM call failed: %s", exc)
            return {"applied": {}, "rationale": f"LLM error: {exc}", "skipped": []}

        decision = self._safe_json(msg)
        adjustments = decision.get("adjustments") or {}
        rationale = decision.get("rationale", "")
        applied: Dict[str, Any] = {}
        skipped: List[str] = []

        for key, value in adjustments.items():
            if key not in tunables:
                skipped.append(f"{key} (not tunable)")
                continue
            kind, lo, hi = tunables[key]
            try:
                num = float(value)
            except (TypeError, ValueError):
                skipped.append(f"{key} (non-numeric {value!r})")
                continue
            num = max(float(lo), min(float(hi), num))   # clamp to allow-listed range
            coerced: Any = int(round(num)) if kind == "int" else round(num, 4)
            old = current.get(key)
            if str(old) == str(coerced):
                continue                                # no-op; don't log churn
            await self.db.set_setting(key, str(coerced))
            applied[key] = coerced
            await self.db.write_log(
                "OVERSEER",
                f"[PARAM-TUNE] {key}: {old} → {coerced} (goal-aligned)",
            )

        if applied:
            logger.info("[PARAM-TUNE] applied %s — %s", applied, rationale)
        result = {"applied": applied, "rationale": rationale, "skipped": skipped}
        try:
            await self.db.write_ai_decision(
                "OVERSEER", "PARAMS", self.autonomy,
                {"type": "param_tuning", **result},
            )
        except Exception:                                          # noqa: BLE001
            pass
        return result

    # -- chart-only analysis (always runs when vision enabled) ---------------
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
                msg = await asyncio.to_thread(
                    self.llm.chat,
                    [{"role": "system", "content": system_prompt},
                     {"role": "user",   "content": prompt}],
                    images=[chart],
                )
                analysis = self._safe_json(msg)
                verdict  = analysis.get("outlook", "NEUTRAL").upper()
                rationale = analysis.get("rationale", "")
                self.chart_provider.record_analysis(symbol, verdict, rationale, analysis)
                await self.db.write_ai_decision(
                    "CHART", symbol, "vision",
                    {"type": "chart_analysis", **analysis},
                )
                logger.info("Chart analysis %s → %s", symbol, verdict)
            except Exception as exc:                                   # noqa: BLE001
                logger.warning("Chart analysis failed for %s: %s", symbol, exc)

    # Token budget for log context: ~2 000 tokens ≈ 8 000 chars. Keeps cheap
    # local LLMs from overflowing their context window on vision prompts.
    _MAX_LOG_CHARS = 8_000
    # Retry policy for transient LLM failures (network blip, timeout).
    _LLM_MAX_RETRIES = 3

    # -- LLM I/O -------------------------------------------------------------
    async def _consult(self, action: TradeAction) -> Dict[str, Any]:
        prompt = (
            "Review this trade action against general market context, the recent "
            "execution log, and (if attached) the underlying's chart. "
            "Reply with JSON {verdict: APPROVE|VETO|MODIFY, rationale, modifications?}.\n"
            f"ACTION:\n{json.dumps(asdict(action), default=str)}\n"
        )
        recent_logs = await self.db.recent_logs(limit=200)
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
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(self._LLM_MAX_RETRIES):
            try:
                msg = await asyncio.to_thread(self.llm.chat, messages, images=images)
                # Clear any stored LLM error on success.
                try:
                    await self.db.set_setting("llm_last_error", "")
                    await self.db.set_setting(
                        "llm_last_ok_ts",
                        datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    )
                except Exception:                                      # noqa: BLE001
                    pass
                return self._safe_json(msg)
            except Exception as exc:                                   # noqa: BLE001
                last_exc = exc
                if attempt < self._LLM_MAX_RETRIES - 1:
                    wait_s = 2 ** attempt          # 1 s, 2 s
                    logger.warning(
                        "LLM attempt %d/%d failed for %s; retrying in %ds: %s",
                        attempt + 1, self._LLM_MAX_RETRIES,
                        action.symbol, wait_s, exc,
                    )
                    await asyncio.sleep(wait_s)

        logger.warning("LLM call failed after %d attempts — passing action through: %s",
                       self._LLM_MAX_RETRIES, last_exc)
        try:
            await self.db.set_setting("llm_last_error", str(last_exc)[:500])
        except Exception:                                              # noqa: BLE001
            pass
        # Fail-safe: pass action through but flag so the operator can see it.
        return {
            "verdict": "APPROVE",
            "rationale": f"LLM unavailable after {self._LLM_MAX_RETRIES} attempts ({last_exc}); defaulting to APPROVE.",
            "llm_error_fallback": True,
        }

    async def _propose_for(self, symbol: str, chart) -> Optional[Dict[str, Any]]:
        system_prompt = await self.get_system_prompt()
        prompt = (
            f"Propose ONE high-conviction options TradeAction for {symbol} or null. "
            "Use only fields from the dataclass schema. JSON only."
        )
        try:
            msg = await asyncio.to_thread(
                self.llm.chat,
                [{"role": "system", "content": system_prompt},
                 {"role": "user",   "content": prompt}],
                images=[chart] if chart is not None else [],
            )
        except Exception as exc:                                       # noqa: BLE001
            logger.warning("LLM propose failed for %s: %s", symbol, exc)
            return None
        data = self._safe_json(msg)
        if not data or data.get("verdict") == "PASS":
            return None
        return data.get("action")

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
                    await self.db.write_ai_decision(
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
                )
                if self.event_bus:
                    self.event_bus.emit(approval_event)
                
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in HermesOverseer worker loop: %s", exc, exc_info=True)
