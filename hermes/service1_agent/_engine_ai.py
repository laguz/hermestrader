"""
[Service-1: Hermes-Agent-Core] — overseer AI proposal / close / gating controller.

Split out of ``core.py`` to keep the engine's spine readable. ``AIController``
is an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.ai``); it shares the engine's hot tick state.
Not meant to be used standalone.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Sequence

from hermes.events.bus import AIApprovalEvent
from .strategy_base import AbstractStrategy
from .trade_action import TradeAction
from ._engine_base import _EngineCollaborator

if TYPE_CHECKING:
    from .core import CascadingEngine
    from .context import TickContext

logger = logging.getLogger("hermes.agent.core")


class AIController(_EngineCollaborator):
    async def handle_ai_approval(self, event: AIApprovalEvent) -> None:
        await self.engine.publish_event("AI_APPROVAL", {"event": event})

    async def _handle_ai_approval_internal(self, event: AIApprovalEvent) -> None:
        """Asynchronously executes or queues an action after AI approval."""
        a = event.original_action
        if a is None:
            logger.warning("AIApprovalEvent has no original_action; skipping.")
            return

        if event.verdict == "VETO":
            logger.info("[AI VETOED] Strategy=%s symbol=%s - %s", event.strategy_id, event.symbol, event.rationale)
            await self.engine.db.logs.write_log(
                event.strategy_id,
                f"[AI VETOED] {event.symbol} — {event.rationale}"
            )
            if event.approval_id is not None:
                await self.engine.db.approvals.update_approval_status(event.approval_id, "REJECTED", notes=event.rationale)
            # Record a short-lived suppression so the rules engine stops
            # re-proposing this identical entry next tick (a veto consumes
            # no capacity, so without this it would brute-force the same
            # action and re-veto it every cycle). Best-effort: a failure
            # here must never block the tick.
            ttl = int(self.engine.config.get("veto_suppression_s", 1800))
            if ttl > 0:
                veto_side = (a.strategy_params or {}).get("side_type")
                if veto_side and str(veto_side).lower() in {"buy", "sell"}:
                    veto_side = None
                try:
                    hits = await self.engine.db.approvals.record_veto(
                        event.strategy_id, event.symbol, veto_side,
                        a.expiry, event.rationale, ttl)
                    logger.info("[VETO] suppression recorded for %s (hits=%d, ttl=%ds)",
                                event.symbol, hits, ttl * hits)
                except Exception:                                  # noqa: BLE001
                    logger.exception("[VETO] record_veto failed for %s", event.symbol)
            return

        if event.verdict == "MODIFY":
            # Apply modifications
            if event.modifications:
                for k, v in event.modifications.items():
                    if hasattr(a, k):
                        setattr(a, k, v)
                a.ai_authored = True
                a.ai_rationale = event.rationale

        # Proceed to the shared order sink — same dedup / pure-close routing /
        # dry-run guard as the synchronous submit() path. ``action_type`` is
        # carried through the event so a management close approved via the bus
        # is routed as a close, not re-queued as a fresh entry.
        await self.engine._execute_or_queue(a, getattr(event, "action_type", "entry"), approval_id=getattr(event, "approval_id", None))

    async def _async_propose(self, ctx: TickContext) -> None:
        """Asynchronously triggers the overseer to propose actions without blocking the tick loop."""
        try:
            ai_actions = await self.engine.overseer.propose(ctx.watchlist)
            ai_actions = await self._gate_ai_actions(ai_actions)
            if ai_actions:
                await self.engine.submit(ai_actions, action_type="ai")
        except Exception as exc:
            logger.exception("Error in async propose: %s", exc)

    async def _async_propose_closes(self, ctx: TickContext) -> None:
        """Asynchronously let the overseer close positions without blocking the tick."""
        try:
            closes = await self.engine.overseer.propose_closes()
            closes = await self._price_ai_closes(ctx, closes)
            if closes:
                await self.engine.submit(closes, action_type="management")
        except Exception as exc:                                   # noqa: BLE001
            logger.exception("Error in async propose_closes: %s", exc)

    async def _broker_position_state(self, ctx: TickContext) -> tuple[Dict[str, float], set]:
        """Live broker holdings + legs already worked by a resting order.

        Returns ``(qty_by_option_symbol, active_order_legs)`` where the qty is
        net and signed (shorts negative). Used to gate AI closes against the
        actual book. Reads directly from the cached tick context to avoid duplicate
        outbound REST API calls.
        """
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
        self, ctx: TickContext, actions: Sequence[TradeAction]
    ) -> List[TradeAction]:
        """Price overseer-proposed closes against live quotes, gated on holdings.

        The overseer builds closes with ``price=None`` — it has no broker. We
        fill the debit here the same way a strategy's ``manage_positions``
        would: ``short_ask − long_bid`` for a spread (guarded by
        ``compute_close_debit`` against stale/phantom quotes), or the ask for
        a single short option. A leg whose quote is missing or whose debit
        looks phantom is skipped this tick rather than priced blind.

        Before pricing, every close is gated on the live broker book
        (``_broker_position_state``): we only cover a short the broker is
        actually holding, and never one a resting order already works. This is
        the fix for AI closes being rejected with "Buy To Cover ... unless
        closing a short position" when the DB believed a not-yet-filled entry
        was open.
        """
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

                # --- broker-holdings gate ---------------------------------
                short_sym = short_leg.get("option_symbol")
                lots = int(short_leg.get("quantity") or a.quantity or 1)
                trade_id = (a.strategy_params or {}).get("trade_id")
                held = qty_map.get(short_sym, 0.0)
                if held > -lots:
                    # Not short, or not short enough, to cover this close.
                    await self.engine.db.logs.write_log(
                        a.strategy_id,
                        f"[AI-CLOSE] {a.symbol} trade_id={trade_id}: broker holds "
                        f"{held:g} of {short_sym} (need short ≥ {lots}); skip — "
                        f"position not (yet) held",
                    )
                    continue
                long_sym = long_leg.get("option_symbol") if long_leg else None
                if short_sym in active_legs or (long_sym and long_sym in active_legs):
                    await self.engine.db.logs.write_log(
                        a.strategy_id,
                        f"[AI-CLOSE] {a.symbol} trade_id={trade_id}: a resting order "
                        f"already works this position; skip — avoids duplicate cover",
                    )
                    continue
                # ----------------------------------------------------------

                quotes = await self.engine.broker.get_quote(",".join(syms)) or []
                qmap = {q.get("symbol"): q for q in quotes}
                sq = qmap.get(short_leg.get("option_symbol"))
                if long_leg is not None:
                    lq = qmap.get(long_leg.get("option_symbol"))
                    debit, blocked, reason = AbstractStrategy.compute_close_debit(sq, lq, a.width)
                    if blocked:
                        await self.engine.db.logs.write_log(
                            a.strategy_id,
                            f"[AI-CLOSE] {a.symbol} trade_id="
                            f"{(a.strategy_params or {}).get('trade_id')}: "
                            f"close-debit blocked ({reason}); skip this tick",
                        )
                        continue
                else:
                    ask = float((sq or {}).get("ask") or 0)
                    if ask <= 0:
                        await self.engine.db.logs.write_log(
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
                await self.engine.db.logs.write_log(
                    a.strategy_id,
                    f"[AI-CLOSE] {a.symbol} trade_id="
                    f"{(a.strategy_params or {}).get('trade_id')} debit=${a.price:.2f} "
                    f"— {a.ai_rationale}",
                )
                priced.append(a)
            except Exception as exc:                              # noqa: BLE001
                logger.exception("[AI-CLOSE] pricing failed for %s: %s", a.symbol, exc)
        return priced

    async def _gate_ai_actions(
        self, actions: Sequence[TradeAction]
    ) -> List[TradeAction]:
        """Run AI-originated proposals through the mechanical entry gate.

        Overseer proposals carry no POP / delta / credit / capacity guarantees
        of their own — the rule-based strategies enforce those on *their*
        entries, but a vision-proposed action skips them. We re-derive every
        gate against live market data here so an AI idea can only fill if it
        clears the same bar a rules entry would. Rejections are logged with a
        reason; passing actions come back normalised and capacity-scaled.

        Fails closed: if the MoneyManager isn't wired (legacy callers) we have
        no capacity check, so no AI entry may originate.
        """
        if not actions:
            return []
        if self.engine.mm is None:
            for a in actions:
                await self.engine.db.logs.write_log(
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
                    a, broker=self.engine.broker, db=self.engine.db, mm=self.engine.mm)
            except Exception as exc:                              # noqa: BLE001
                logger.exception("[AI-GATE] error validating %s: %s", a.symbol, exc)
                await self.engine.db.logs.write_log(
                    a.strategy_id,
                    f"[AI-GATE] {a.symbol}: rejected — validation error: {exc}",
                )
                continue
            if validated is None:
                logger.info("[AI-GATE] REJECTED %s", reason)
                await self.engine.db.logs.write_log(a.strategy_id, f"[AI-GATE] REJECTED {reason}")
            else:
                logger.info("[AI-GATE] %s", reason)
                await self.engine.db.logs.write_log(a.strategy_id, f"[AI-GATE] {reason}")
                gated.append(validated)
        return gated
