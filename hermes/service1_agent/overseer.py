"""
[Service-1: Hermes-Agent-Core] — Hermes AI Overseer
A local LLM (Gemma 3 Flash / Gemma 4 e4b) reviews proposed TradeActions, may VETO
or MODIFY them, and may PROPOSE new ones from chart-image analysis. The class is
provider-agnostic — `LLMClient` is any object with `.chat(messages, images=...)`.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
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
        """Base instructions plus operator doctrine, when present."""
        if not self.soul:
            return self.BASE_SYSTEM_PROMPT
        return (
            f"{self.BASE_SYSTEM_PROMPT}\n\n"
            "--- OPERATOR DOCTRINE (soul.md) ---\n"
            f"{self.soul}\n"
            "--- END DOCTRINE ---"
        )

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

    # -- LLM I/O -------------------------------------------------------------
    def _consult(self, action: TradeAction) -> Dict[str, Any]:
        prompt = (
            "Review this trade action against general market context, the recent "
            "execution log, and (if attached) the underlying's chart. "
            "Reply with JSON {verdict: APPROVE|VETO|MODIFY, rationale, modifications?}.\n"
            f"ACTION:\n{json.dumps(asdict(action), default=str)}\n"
        )
        recent_logs = self.db.recent_logs(limit=200)
        prompt += f"RECENT_LOGS:\n{recent_logs}\n"
        images = []
        if self.vision_enabled and self.chart_provider is not None:
            try:
                images.append(self.chart_provider.snapshot(action.symbol))
            except Exception:                                          # noqa: BLE001
                pass
        msg = self.llm.chat(
            [{"role": "system", "content": self.SYSTEM_PROMPT},
             {"role": "user",   "content": prompt}],
            images=images,
        )
        return self._safe_json(msg)

    def _propose_for(self, symbol: str, chart) -> Optional[Dict[str, Any]]:
        prompt = (
            f"Propose ONE high-conviction options TradeAction for {symbol} or null. "
            "Use only fields from the dataclass schema. JSON only."
        )
        msg = self.llm.chat(
            [{"role": "system", "content": self.SYSTEM_PROMPT},
             {"role": "user",   "content": prompt}],
            images=[chart] if chart is not None else [],
        )
        data = self._safe_json(msg)
        if not data or data.get("verdict") == "PASS":
            return None
        return data.get("action")

    @staticmethod
    def _safe_json(text: str) -> Dict[str, Any]:
        if isinstance(text, dict):
            return text
        try:
            return json.loads(text)
        except Exception:                                              # noqa: BLE001
            # Try to find a JSON block embedded in prose
            start, end = text.find("{"), text.rfind("}")
            if start >= 0 and end > start:
                try:
                    return json.loads(text[start:end + 1])
                except Exception:                                      # noqa: BLE001
                    pass
        return {"verdict": "APPROVE", "rationale": "Unparseable LLM reply; defaulting."}
