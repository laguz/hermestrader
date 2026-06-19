"""
[Service-1: Hermes-Agent-Core] — shared overseer state + LLM transport.

:class:`OverseerContext` is the single source of truth for the overseer's
*live, operator-tunable* configuration (``autonomy`` / ``soul`` /
``vision_enabled`` / ``chart_provider`` / ``overseer_mode`` / ``llm``) and the
shared LLM transport (system-prompt assembly, the timeout/retry chat calls, and
JSON-reply parsing). ``main.py`` reconfigures these knobs *live* every tick as
the operator changes settings in the watcher, so they cannot be copied into each
collaborator at construction — they would go stale. Instead the overseer and
every owned collaborator hold a reference to **one** context and read the live
values back through it.

This is what lets the collaborators (:mod:`overseer_single` /
:mod:`overseer_committee` / :mod:`overseer_proposers` / :mod:`overseer_governance`
/ :mod:`overseer_worker`) depend on a small, named state object instead of a
back-reference to the whole :class:`~hermes.service1_agent.overseer.HermesOverseer`
(which also carries the public review/propose API and the worker lifecycle).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hermes.agent.overseer")


class OverseerContext:
    """Live overseer state + LLM transport, shared by reference.

    Construction-time defaults mirror the historical :class:`HermesOverseer`
    behaviour exactly: ``soul`` is stripped once on init (later reassignments
    are taken verbatim, as ``main.py`` always has), and the budgets/base prompt
    are class constants.
    """

    BASE_SYSTEM_PROMPT = (
        "You are HERMES, a quantitative options-trading overseer. "
        "You review trade actions produced by rule-based strategies and decide: "
        "APPROVE / VETO / MODIFY. You also propose new trades when chart context "
        "shows superior setups or imminent risks the rules missed. "
        "Output strict JSON."
    )

    # Token budget for log context: ~2 000 tokens ≈ 8 000 chars. Keeps cheap
    # local LLMs from overflowing their context window on vision prompts.
    MAX_LOG_CHARS = 8_000
    # Retry policy for transient LLM failures (network blip, timeout).
    LLM_MAX_RETRIES = 3

    def __init__(self, llm_client, db, *, vision_enabled: bool = True,
                 chart_provider=None, autonomy: str = "advisory",
                 soul: Optional[str] = None, overseer_mode: str = "single") -> None:
        self.llm = llm_client
        self.db = db
        self.vision_enabled = vision_enabled
        self.chart_provider = chart_provider
        self.autonomy = autonomy
        self.soul = (soul or "").strip()
        self.overseer_mode = overseer_mode

    async def chat_with_timeout(self, messages: List[Dict[str, str]],
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

    async def chat_with_retry(self, messages: List[Dict[str, str]],
                              images: List[Any] = None) -> str:
        """Call the LLM with a strict timeout and automatic retry logic."""
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(self.LLM_MAX_RETRIES):
            try:
                return await self.chat_with_timeout(messages, images=images)
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
    def safe_json(text: str) -> Dict[str, Any]:
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
