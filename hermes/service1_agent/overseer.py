"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer
A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions, may VETO
or MODIFY them, and may PROPOSE new ones from chart-image analysis. The class is
provider-agnostic — `LLMClient` is any object with `.chat(messages, images=...)`.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

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
                 soul: Optional[str] = None):
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

    @property
    def SYSTEM_PROMPT(self) -> str:                              # noqa: N802
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
                perf_metrics = self.db.get_strategy_performance_metrics(days=30)
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
    def review(self, action: TradeAction) -> Optional[TradeAction]:
        if self.autonomy == "advisory":
            decision = self._consult(action)
            self.db.write_ai_decision(action.strategy_id, action.symbol,
                                      "advisory", decision)
            return action

        decision = self._consult(action)
        self.db.write_ai_decision(action.strategy_id, action.symbol,
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
    def propose(self, watchlist: Iterable[str]) -> List[TradeAction]:
        if self.autonomy != "autonomous":
            return []
        proposed: List[TradeAction] = []
        for symbol in watchlist:
            chart = self.chart_provider.snapshot(symbol) if self.chart_provider else None
            payload = self._propose_for(symbol, chart)
            if not payload:
                continue
            try:
                a = TradeAction(**payload, ai_authored=True)
                proposed.append(a)
            except Exception as exc:                                   # noqa: BLE001
                logger.warning("Overseer proposal malformed for %s: %s", symbol, exc)
        return proposed

    # -- chart-only analysis (always runs when vision enabled) ---------------
    def analyze_charts(self, watchlist: Iterable[str]) -> None:
        """Run a vision-only read on each symbol's chart and store the result.

        Runs regardless of autonomy level — purely informational.  Results are
        stored back on the chart_provider so the C2 API can surface them.
        """
        if not self.vision_enabled or self.chart_provider is None:
            return
        for symbol in watchlist:
            chart = self.chart_provider.snapshot(symbol)
            if chart is None:
                continue
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
                msg = self.llm.chat(
                    [{"role": "system", "content": self.SYSTEM_PROMPT},
                     {"role": "user",   "content": prompt}],
                    images=[chart],
                )
                analysis = self._safe_json(msg)
                verdict  = analysis.get("outlook", "NEUTRAL").upper()
                rationale = analysis.get("rationale", "")
                self.chart_provider.record_analysis(symbol, verdict, rationale, analysis)
                self.db.write_ai_decision(
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
    def _consult(self, action: TradeAction) -> Dict[str, Any]:
        prompt = (
            "Review this trade action against general market context, the recent "
            "execution log, and (if attached) the underlying's chart. "
            "Reply with JSON {verdict: APPROVE|VETO|MODIFY, rationale, modifications?}.\n"
            f"ACTION:\n{json.dumps(asdict(action), default=str)}\n"
        )
        recent_logs = self.db.recent_logs(limit=200)
        # Enforce token budget: truncate from the front (oldest entries dropped).
        if len(recent_logs) > self._MAX_LOG_CHARS:
            recent_logs = "[...truncated...]\n" + recent_logs[-self._MAX_LOG_CHARS:]
        prompt += f"RECENT_LOGS:\n{recent_logs}\n"
        images = []
        if self.vision_enabled and self.chart_provider is not None:
            try:
                images.append(self.chart_provider.snapshot(action.symbol))
            except Exception:                                          # noqa: BLE001
                pass

        messages = [{"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt}]
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(self._LLM_MAX_RETRIES):
            try:
                msg = self.llm.chat(messages, images=images)
                # Clear any stored LLM error on success.
                try:
                    self.db.set_setting("llm_last_error", "")
                    self.db.set_setting(
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
                    time.sleep(wait_s)

        logger.warning("LLM call failed after %d attempts — passing action through: %s",
                       self._LLM_MAX_RETRIES, last_exc)
        try:
            self.db.set_setting("llm_last_error", str(last_exc)[:500])
        except Exception:                                              # noqa: BLE001
            pass
        # Fail-safe: pass action through but flag so the operator can see it.
        return {
            "verdict": "APPROVE",
            "rationale": f"LLM unavailable after {self._LLM_MAX_RETRIES} attempts ({last_exc}); defaulting to APPROVE.",
            "llm_error_fallback": True,
        }

    def _propose_for(self, symbol: str, chart) -> Optional[Dict[str, Any]]:
        prompt = (
            f"Propose ONE high-conviction options TradeAction for {symbol} or null. "
            "Use only fields from the dataclass schema. JSON only."
        )
        try:
            msg = self.llm.chat(
                [{"role": "system", "content": self.SYSTEM_PROMPT},
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
