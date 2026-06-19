"""
[Service-1: Hermes-Agent-Core] — Multi-Agent Risk Committee reviewer.

Split out of ``overseer.py`` to separate the committee review path from the
single-LLM path. :class:`CommitteeReviewer` takes the shared
:class:`~hermes.service1_agent.overseer_context.OverseerContext` (live state +
LLM transport) and the sibling :class:`~.overseer_single.SingleReviewer` it falls
back to, so there is one source of truth and no per-collaborator forwarding.

Flow: the Macro Specialist and the Strategy Specialist run in parallel, then the
Risk Officer synthesises their findings into the final APPROVE / VETO / MODIFY
verdict. On any failure the whole path falls back to the overseer's single-LLM
review, so committee mode never fails closed differently from single mode.
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
    from .overseer_context import OverseerContext
    from .overseer_single import SingleReviewer

logger = logging.getLogger("hermes.agent.overseer")


class CommitteeReviewer:
    """Owns the overseer's multi-agent committee review path.

    Reads live state and the LLM transport off the shared
    :class:`~hermes.service1_agent.overseer_context.OverseerContext`
    (``self.ctx``), and falls back to the sibling :class:`SingleReviewer` on any
    failure. The three specialist prompts live here because nothing else uses
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

    def __init__(self, ctx: "OverseerContext", single: "SingleReviewer") -> None:
        self.ctx = ctx
        self._single = single

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
            if self.ctx.db is not None:
                recent_logs = await self.ctx.db.logs.recent_logs(limit=200)
                if len(recent_logs) > self.ctx.MAX_LOG_CHARS:
                    recent_logs = "[...truncated...]\n" + recent_logs[-self.ctx.MAX_LOG_CHARS:]

            images = []
            if self.ctx.vision_enabled and self.ctx.chart_provider is not None:
                try:
                    img = await self.ctx.chart_provider.snapshot(action.symbol)
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
            logger.warning("Committee execution failed: %s; falling back to single-LLM review.", exc)
            return await self._single.consult(action)

    async def _run_macro_specialist(self, action: "TradeAction", mkt_line: str, recent_logs: str, images: List[Any]) -> Dict[str, Any]:
        sys_prompt = self.MACRO_SPECIALIST_PROMPT
        if mkt_line:
            sys_prompt += f"\nCURRENT MARKET STATUS: {mkt_line}"

        prompt = (
            f"Review this proposed TradeAction:\n{json.dumps(asdict(action), default=str)}\n\n"
            f"RECENT_LOGS:\n{recent_logs}\n"
        )
        try:
            msg = await self.ctx.chat_with_retry(
                [{"role": "system", "content": sys_prompt},
                 {"role": "user",   "content": prompt}],
                images=images
            )
            res = self.ctx.safe_json(msg)
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
            if self.ctx.db is not None:
                perf_metrics = await self.ctx.db.analytics.get_strategy_performance_metrics(days=30)
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
            msg = await self.ctx.chat_with_retry(
                [{"role": "system", "content": sys_prompt},
                 {"role": "user",   "content": prompt}],
                images=[]
            )
            res = self.ctx.safe_json(msg)
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
        if self.ctx.soul:
            sys_prompt += (
                "\n--- OPERATOR DOCTRINE (soul.md) ---\n"
                f"{self.ctx.soul}\n"
                "--- END DOCTRINE ---"
            )

        prompt = (
            f"Review this proposed TradeAction:\n{json.dumps(asdict(action), default=str)}\n\n"
            f"MACRO SPECIALIST REVIEW:\n{json.dumps(macro_res, indent=2)}\n\n"
            f"STRATEGY SPECIALIST REVIEW:\n{json.dumps(strategy_res, indent=2)}\n"
        )
        msg = await self.ctx.chat_with_retry(
            [{"role": "system", "content": sys_prompt},
             {"role": "user",   "content": prompt}],
            images=[]
        )
        res = self.ctx.safe_json(msg)
        # Clear any stored LLM error on success.
        try:
            await self.ctx.db.settings.set_setting("llm_last_error", "")
            await self.ctx.db.settings.set_setting(
                "llm_last_ok_ts",
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
        except Exception:                                              # noqa: BLE001
            pass
        return res
