"""
[Service-1: Hermes-Agent-Core] — Multi-Agent Risk Committee reviewer.

Split out of ``overseer.py`` to separate the committee review path from the
single-LLM (monolithic) path. :class:`CommitteeReviewer` is an injected
collaborator owned by :class:`~hermes.service1_agent.overseer.HermesOverseer`:
it reads the overseer's state (db, soul, vision/chart) and reuses its LLM
transport (``_chat_with_retry`` / ``_safe_json``) through a back-reference, so
the four method bodies moved out of the overseer unchanged.

Flow: the Macro Specialist and the Strategy Specialist run in parallel, then the
Risk Officer synthesises their findings into the final APPROVE / VETO / MODIFY
verdict. On any failure the whole path falls back to the overseer's monolithic
review, so committee mode never fails closed differently from monolithic mode.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import TradeAction
    from .overseer import HermesOverseer

logger = logging.getLogger("hermes.agent.overseer")


class CommitteeReviewer:
    """Owns the overseer's multi-agent committee review path.

    Reads overseer state via ``self._ov``; the forwarding properties below let
    the method bodies keep reading ``self.db`` / ``self.soul`` / ``self._safe_json``
    etc. unchanged, so the extraction from the inline committee was a move, not a
    rewrite. The three specialist prompts live here because nothing else uses
    them.
    """

    MACRO_SPECIALIST_PROMPT = (
        "You are the Macro Specialist for HERMES, a quantitative options-trading system. "
        "Your role is to analyze market trends, technical levels, volatility regimes, and chart patterns "
        "to assess short-term direction and risk.\n"
        "You review a proposed options TradeAction and any attached chart image to identify:\n"
        "1. Price trend direction (bullish, bearish, rangebound).\n"
        "2. Key support/resistance levels relative to the trade strikes.\n"
        "3. RSI levels and Bollinger Band metrics.\n"
        "4. Macro/technical risks that could invalidate the trade.\n\n"
        "Format your reply in strict JSON with the following structure:\n"
        "{\n"
        '  "trend": "bullish|bearish|neutral",\n'
        '  "support_resistance_analysis": "string detailing support/resistance relative to strikes",\n'
        '  "technical_indicators": "string summarizing RSI, BB, etc.",\n'
        '  "macro_risk_rating": "low|medium|high",\n'
        '  "rationale": "detailed explanation of macro observations"\n'
        "}"
    )

    STRATEGY_SPECIALIST_PROMPT = (
        "You are the Strategy and Sizing Specialist for HERMES. Your role is to evaluate option-specific "
        "parameters (DTE, delta, strike width, lot count) and historical strategy performance.\n"
        "You review a proposed options TradeAction, recent strategy metrics, and execution logs to identify:\n"
        "1. Delta of short and long legs.\n"
        "2. Days to expiration (DTE) relative to strategy specifications.\n"
        "3. Sizing (lots) and premium credit compared to account limits and recent performance.\n"
        "4. Win rates and drawdowns of this specific strategy over the past 30 days.\n\n"
        "Format your reply in strict JSON with the following structure:\n"
        "{\n"
        '  "sizing_suitability": "appropriate|excessive|conservative",\n'
        '  "parameter_suitability": "appropriate|aggressive|conservative",\n'
        '  "performance_context": "string summarizing recent strategy performance",\n'
        '  "strategy_risk_rating": "low|medium|high",\n'
        '  "rationale": "detailed explanation of parameters and performance context"\n'
        "}"
    )

    RISK_OFFICER_PROMPT = (
        "You are the Risk Officer and Chairman of the HERMES Multi-Agent Risk Committee. "
        "Your role is to make the final trading decision by enforcing the operator's doctrine (soul.md) "
        "and synthesizing the findings from the Macro Specialist and the Strategy Specialist.\n"
        "You review the proposed TradeAction, the Macro Specialist's technical/market trend analysis, "
        "and the Strategy Specialist's parameter/sizing/performance analysis.\n\n"
        "Your possible verdicts are:\n"
        "- APPROVE: The trade is fully aligned.\n"
        "- VETO: The trade has excessive macro or strategy risk, or violates soul.md.\n"
        "- MODIFY: Adjust key fields like price, lots, or strikes (return changes in `modifications`).\n\n"
        "Format your reply in strict JSON with the following structure:\n"
        "{\n"
        '  "verdict": "APPROVE|VETO|MODIFY",\n'
        '  "rationale": "synthesis of specialists\' opinions and how they relate to the operator\'s doctrine",\n'
        '  "modifications": {\n'
        '    "price": optional_float,\n'
        '    "quantity": optional_int\n'
        '  }\n'
        "}"
    )

    def __init__(self, overseer: "HermesOverseer") -> None:
        self._ov = overseer

    # ── forwarded overseer handles (single source of truth on the overseer) ──
    @property
    def db(self):
        return self._ov.db

    @property
    def soul(self):
        return self._ov.soul

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
    def _chat_with_retry(self):
        return self._ov._chat_with_retry

    @property
    def _safe_json(self):
        return self._ov._safe_json

    @property
    def _consult_monolithic(self):
        return self._ov._consult_monolithic

    async def consult(self, action: "TradeAction") -> Dict[str, Any]:
        """Decomposes review into a Multi-Agent Committee: Macro + Strategy Specialists (parallel) -> Risk Officer."""
        try:
            mkt_line = ""
            try:
                from hermes.market_hours import session_label
                mkt_line = session_label()
            except Exception:                                          # noqa: BLE001
                pass

            recent_logs = ""
            if self.db is not None:
                recent_logs = await self.db.logs.recent_logs(limit=200)
                if len(recent_logs) > self._MAX_LOG_CHARS:
                    recent_logs = "[...truncated...]\n" + recent_logs[-self._MAX_LOG_CHARS:]

            images = []
            if self.vision_enabled and self.chart_provider is not None:
                try:
                    img = await self.chart_provider.snapshot(action.symbol)
                    if img is not None:
                        images.append(img)
                except Exception:                                      # noqa: BLE001
                    pass

            # Parallelized Execution of Macro & Strategy Specialists
            macro_task = self._run_macro_specialist(action, mkt_line, recent_logs, images)
            strategy_task = self._run_strategy_specialist(action, recent_logs)
            macro_res, strategy_res = await asyncio.gather(macro_task, strategy_task)

            # Risk Officer Synthesis & Verdict
            decision = await self._run_risk_officer(action, macro_res, strategy_res, mkt_line)

            # Embed specialist reviews in final payload for transparency/audit logs
            decision["committee"] = {
                "macro_analysis": macro_res,
                "strategy_analysis": strategy_res,
            }
            return decision

        except Exception as exc:
            logger.warning("Committee execution failed: %s; falling back to monolithic review.", exc)
            return await self._consult_monolithic(action)

    async def _run_macro_specialist(self, action: "TradeAction", mkt_line: str, recent_logs: str, images: List[Any]) -> Dict[str, Any]:
        sys_prompt = self.MACRO_SPECIALIST_PROMPT
        if mkt_line:
            sys_prompt += f"\nCURRENT MARKET STATUS: {mkt_line}"

        prompt = (
            f"Review this proposed TradeAction:\n{json.dumps(asdict(action), default=str)}\n\n"
            f"RECENT_LOGS:\n{recent_logs}\n"
        )
        try:
            msg = await self._chat_with_retry(
                [{"role": "system", "content": sys_prompt},
                 {"role": "user",   "content": prompt}],
                images=images
            )
            res = self._safe_json(msg)
            logger.info("Macro Specialist: trend=%s risk=%s rationale=%s",
                        res.get("trend"), res.get("macro_risk_rating"), res.get("rationale"))
            return res
        except Exception as exc:
            logger.warning("Macro Specialist evaluation failed: %s", exc)
            return {"error": f"Macro Specialist failed: {exc}"}

    async def _run_strategy_specialist(self, action: "TradeAction", recent_logs: str) -> Dict[str, Any]:
        sys_prompt = self.STRATEGY_SPECIALIST_PROMPT

        perf_metrics_str = ""
        try:
            if self.db is not None:
                perf_metrics = await self.db.analytics.get_strategy_performance_metrics(days=30)
                perf_lines = []
                for strat, data in perf_metrics.items():
                    perf_lines.append(
                        f"- {strat}: status={data['status']}, closed={data['closed_trades']}, "
                        f"passed={data['passed']}, failed={data['failed']}, total_pnl=${data['total_pnl']:.2f}"
                    )
                perf_metrics_str = "\n".join(perf_lines)
        except Exception as exc:                                       # noqa: BLE001
            logger.warning("Failed to fetch performance metrics for Strategy Specialist: %s", exc)

        prompt = (
            f"Review this proposed TradeAction:\n{json.dumps(asdict(action), default=str)}\n\n"
        )
        if perf_metrics_str:
            prompt += f"RECENT STRATEGY PERFORMANCE (30-DAY WINDOW):\n{perf_metrics_str}\n\n"
        prompt += f"RECENT_LOGS:\n{recent_logs}\n"

        try:
            msg = await self._chat_with_retry(
                [{"role": "system", "content": sys_prompt},
                 {"role": "user",   "content": prompt}],
                images=[]
            )
            res = self._safe_json(msg)
            logger.info("Strategy Specialist: parameter=%s risk=%s rationale=%s",
                        res.get("parameter_suitability"), res.get("strategy_risk_rating"), res.get("rationale"))
            return res
        except Exception as exc:
            logger.warning("Strategy Specialist evaluation failed: %s", exc)
            return {"error": f"Strategy Specialist failed: {exc}"}

    async def _run_risk_officer(self, action: "TradeAction", macro_res: Dict[str, Any], strategy_res: Dict[str, Any], mkt_line: str) -> Dict[str, Any]:
        sys_prompt = self.RISK_OFFICER_PROMPT
        if mkt_line:
            sys_prompt += f"\nCURRENT MARKET STATUS: {mkt_line}"
        if self.soul:
            sys_prompt += (
                "\n--- OPERATOR DOCTRINE (soul.md) ---\n"
                f"{self.soul}\n"
                "--- END DOCTRINE ---"
            )

        prompt = (
            f"Review this proposed TradeAction:\n{json.dumps(asdict(action), default=str)}\n\n"
            f"MACRO SPECIALIST REVIEW:\n{json.dumps(macro_res, indent=2)}\n\n"
            f"STRATEGY SPECIALIST REVIEW:\n{json.dumps(strategy_res, indent=2)}\n"
        )
        msg = await self._chat_with_retry(
            [{"role": "system", "content": sys_prompt},
             {"role": "user",   "content": prompt}],
            images=[]
        )
        res = self._safe_json(msg)
        # Clear any stored LLM error on success.
        try:
            await self.db.settings.set_setting("llm_last_error", "")
            await self.db.settings.set_setting(
                "llm_last_ok_ts",
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
        except Exception:                                              # noqa: BLE001
            pass
        return res
