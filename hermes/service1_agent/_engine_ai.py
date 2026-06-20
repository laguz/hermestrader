"""
[Service-1: Hermes-Agent-Core] — overseer AI controller.

Split out of ``core.py`` so the engine spine stays readable. ``AIController`` is
an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.ai``). It owns the overseer-driven concerns: proposing entries,
pricing/gating overseer-authored closes, executing the verdict after AI review
(``AIApprovalEvent``), and the goal-aware out-of-loop parameter/risk tuning.

It reads everything it needs — the shared dependency surface (``db`` /
``broker`` / ``event_bus`` / ``config`` / ``overseer`` / ``mm`` / ``clock`` /
``quote_cache``) — off the
:class:`~hermes.service1_agent.engine_context.EngineContext` (``self.ctx``) and
emits its results back onto the event bus, so it needs no back-reference to the
engine at all.
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Sequence

from hermes.events.bus import (
    AIApprovalEvent,
    ExecuteAIApprovalCommand,
    SubmitTradeActionsCommand,
)
from .strategy_base import AbstractStrategy
from .trade_action import TradeAction

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .context import TickContext
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class AIController:
    """Overseer AI proposals / closes / gating + out-of-loop parameter tuning."""

    def __init__(self, engine: "CascadingEngine") -> None:
        # AIController depends only on the shared dependency surface, not on the
        # engine spine — it reads ``self.ctx`` and emits back onto the event bus.
        self.ctx = engine.ctx

        if self.ctx.event_bus is not None:
            self.ctx.event_bus.subscribe(ExecuteAIApprovalCommand, self.handle_execute_ai_approval)

    # ── overseer AI proposals / closes / gating ──────────────────────────────
    async def handle_execute_ai_approval(self, command: ExecuteAIApprovalCommand) -> None:
        try:
            res = await self._handle_ai_approval_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def _handle_ai_approval_internal(self, event: AIApprovalEvent) -> None:
        """Asynchronously executes or queues an action after AI approval."""
        a = event.original_action
        if a is None:
            logger.warning("AIApprovalEvent has no original_action; skipping.")
            return

        if event.verdict == "VETO":
            logger.info("[AI VETOED] Strategy=%s symbol=%s - %s", event.strategy_id, event.symbol, event.rationale)
            await self.ctx.db.logs.write_log(
                event.strategy_id,
                f"[AI VETOED] {event.symbol} — {event.rationale}"
            )
            if event.approval_id is not None:
                await self.ctx.db.approvals.update_approval_status(event.approval_id, "REJECTED", notes=event.rationale)

            ttl = int(self.ctx.config.get("veto_suppression_s", 1800))
            if ttl > 0:
                veto_side = (a.strategy_params or {}).get("side_type")
                if veto_side and str(veto_side).lower() in {"buy", "sell"}:
                    veto_side = None
                try:
                    hits = await self.ctx.db.approvals.record_veto(
                        event.strategy_id, event.symbol, veto_side,
                        a.expiry, event.rationale, ttl)
                    logger.info("[VETO] suppression recorded for %s (hits=%d, ttl=%ds)",
                                event.symbol, hits, ttl * hits)
                except Exception:
                    logger.exception("[VETO] record_veto failed for %s", event.symbol)
            return

        if event.verdict == "MODIFY":
            if event.modifications:
                for k, v in event.modifications.items():
                    if hasattr(a, k):
                        setattr(a, k, v)
                a.ai_authored = True
                a.ai_rationale = event.rationale

        cmd = SubmitTradeActionsCommand(
            actions=[a],
            action_type=getattr(event, "action_type", "entry"),
            approval_id=getattr(event, "approval_id", None),
            execute_directly=True
        )
        self.ctx.event_bus.emit(cmd)
        await cmd.future

    async def _async_propose(self, ctx: "TickContext") -> None:
        """Asynchronously triggers the overseer to propose actions without blocking the tick loop."""
        try:
            ai_actions = await self.ctx.overseer.propose(ctx.watchlist)
            ai_actions = await self._gate_ai_actions(ai_actions)
            if ai_actions:
                cmd = SubmitTradeActionsCommand(actions=ai_actions, action_type="ai")
                self.ctx.event_bus.emit(cmd)
                await cmd.future
        except Exception as exc:
            logger.exception("Error in async propose: %s", exc)

    async def _async_propose_closes(self, ctx: "TickContext") -> None:
        """Asynchronously let the overseer close positions without blocking the tick."""
        try:
            closes = await self.ctx.overseer.propose_closes()
            closes = await self._price_ai_closes(ctx, closes)
            if closes:
                cmd = SubmitTradeActionsCommand(actions=closes, action_type="management")
                self.ctx.event_bus.emit(cmd)
                await cmd.future
        except Exception as exc:
            logger.exception("Error in async propose_closes: %s", exc)

    async def _broker_position_state(self, ctx: "TickContext") -> tuple[Dict[str, float], set]:
        """Live broker holdings + legs already worked by a resting order."""
        qty: Dict[str, float] = {}
        for p in ctx.positions:
            sym = p.get("symbol")
            if not sym:
                continue
            try:
                qty[sym] = qty.get(sym, 0.0) + float(p.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
        return qty, ctx.active_order_legs

    async def _price_ai_closes(
        self, ctx: "TickContext", actions: Sequence[TradeAction]
    ) -> List[TradeAction]:
        """Price overseer-proposed closes against live quotes, gated on holdings."""
        if not actions:
            return []
        qty_map, active_legs = await self._broker_position_state(ctx)
        priced: List[TradeAction] = []
        for a in actions:
            try:
                legs = a.legs or []
                syms = [leg.get("option_symbol") for leg in legs if leg.get("option_symbol")]
                if not syms:
                    continue
                short_leg = next((l for l in legs if "buy_to_close" in (l.get("side") or "")), None)
                long_leg = next((l for l in legs if "sell_to_close" in (l.get("side") or "")), None)
                if short_leg is None:
                    continue

                short_sym = short_leg.get("option_symbol")
                lots = int(short_leg.get("quantity") or a.quantity or 1)
                trade_id = (a.strategy_params or {}).get("trade_id")
                held = qty_map.get(short_sym, 0.0)
                if held > -lots:
                    await self.ctx.db.logs.write_log(
                        a.strategy_id,
                        f"[AI-CLOSE] {a.symbol} trade_id={trade_id}: broker holds "
                        f"{held:g} of {short_sym} (need short ≥ {lots}); skip — "
                        f"position not (yet) held",
                    )
                    continue
                long_sym = long_leg.get("option_symbol") if long_leg else None
                if short_sym in active_legs or (long_sym and long_sym in active_legs):
                    await self.ctx.db.logs.write_log(
                        a.strategy_id,
                        f"[AI-CLOSE] {a.symbol} trade_id={trade_id}: a resting order "
                        f"already works this position; skip — avoids duplicate cover",
                    )
                    continue

                quotes = await self.ctx.broker.get_quote(",".join(syms)) or []
                qmap = {q.get("symbol"): q for q in quotes}
                sq = qmap.get(short_leg.get("option_symbol"))
                if long_leg is not None:
                    lq = qmap.get(long_leg.get("option_symbol"))
                    debit, blocked, reason = AbstractStrategy.compute_close_debit(sq, lq, a.width)
                    if blocked:
                        await self.ctx.db.logs.write_log(
                            a.strategy_id,
                            f"[AI-CLOSE] {a.symbol} trade_id="
                            f"{(a.strategy_params or {}).get('trade_id')}: "
                            f"close-debit blocked ({reason}); skip this tick",
                        )
                        continue
                else:
                    ask = float((sq or {}).get("ask") or 0)
                    if ask <= 0:
                        await self.ctx.db.logs.write_log(
                            a.strategy_id,
                            f"[AI-CLOSE] {a.symbol}: stale ask on "
                            f"{short_leg.get('option_symbol')}; skip this tick",
                        )
                        continue
                    debit = ask
                a.price = round(debit * 1.05, 2)
                logger.info("[AI-CLOSE] %s trade_id=%s debit=$%.2f — %s",
                            a.symbol, (a.strategy_params or {}).get("trade_id"),
                            a.price, a.ai_rationale)
                await self.ctx.db.logs.write_log(
                    a.strategy_id,
                    f"[AI-CLOSE] {a.symbol} trade_id="
                    f"{(a.strategy_params or {}).get('trade_id')} debit=${a.price:.2f} "
                    f"— {a.ai_rationale}",
                )
                priced.append(a)
            except Exception as exc:
                logger.exception("[AI-CLOSE] pricing failed for %s: %s", a.symbol, exc)
        return priced

    async def _gate_ai_actions(
        self, actions: Sequence[TradeAction]
    ) -> List[TradeAction]:
        """Run AI-originated proposals through the mechanical entry gate."""
        if not actions:
            return []
        if self.ctx.mm is None:
            for a in actions:
                await self.ctx.db.logs.write_log(
                    a.strategy_id,
                    f"[AI-GATE] {a.symbol}: rejected — no MoneyManager wired; "
                    f"cannot validate capacity (fail-closed)",
                )
            return []

        from .entry_gate import gate_ai_action

        gated: List[TradeAction] = []
        for a in actions:
            try:
                validated, reason = await gate_ai_action(
                    a, broker=self.ctx.broker, db=self.ctx.db, mm=self.ctx.mm)
            except Exception as exc:
                logger.exception("[AI-GATE] error validating %s: %s", a.symbol, exc)
                await self.ctx.db.logs.write_log(
                    a.strategy_id,
                    f"[AI-GATE] {a.symbol}: rejected — validation error: {exc}",
                )
                continue
            if validated is None:
                logger.info("[AI-GATE] REJECTED %s", reason)
                await self.ctx.db.logs.write_log(a.strategy_id, f"[AI-GATE] REJECTED {reason}")
            else:
                logger.info("[AI-GATE] %s", reason)
                await self.ctx.db.logs.write_log(a.strategy_id, f"[AI-GATE] {reason}")
                gated.append(validated)
        return gated

    # ── out-of-loop parameter tuning (overseer governor) ─────────────────────
    async def _maybe_tune_parameters(self) -> None:
        """Run the overseer's goal-aware parameter tuning, throttled by interval."""
        interval = int(self.ctx.config.get("param_tuning_interval_s", 3600))
        if interval <= 0:
            return
        tuner = getattr(self.ctx.overseer, "propose_parameter_adjustments", None)
        if tuner is None:
            return
        try:
            now = time.time()
            last_raw = await self.ctx.db.settings.get_setting("ai_last_param_tuning_ts")
            last = float(last_raw) if last_raw else 0.0
            if now - last < interval:
                return
            await self.ctx.db.settings.set_setting("ai_last_param_tuning_ts", str(now))
            await tuner()

            risk_tuner = getattr(self.ctx.overseer, "propose_risk_restrictions", None)
            if risk_tuner is not None:
                await risk_tuner()
        except Exception as exc:
            logger.exception("[PARAM-TUNE] tuning tick failed: %s", exc)
