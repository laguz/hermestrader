"""
[Service-1: Hermes-Agent-Core] — overseer AI + ML knob/exit-policy controller.

Split out of ``core.py`` so the engine spine stays readable. ``AIController`` is
an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.ai``). It owns two related, overseer/ML-driven concerns:

* **overseer AI** — proposing entries, pricing/gating overseer-authored closes,
  and executing the verdict after AI review (``AIApprovalEvent``);
* **outcome-driven tuning** — the Thompson-bandit knob tuner, goal-aware
  parameter/risk adjustments, and the exit-policy capture/advise loop (both the
  per-tick and the reactive-quote variants).

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
    EvaluateReactiveExitEvent,
    SubmitTradeActionsCommand,
)
from .strategy_base import AbstractStrategy
from .trade_action import TradeAction

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .context import TickContext
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class AIController:
    """Overseer AI proposals/closes/gating + bandit/exit-policy tuning."""

    def __init__(self, engine: "CascadingEngine") -> None:
        # AIController depends only on the shared dependency surface, not on the
        # engine spine — it reads ``self.ctx`` and emits back onto the event bus.
        self.ctx = engine.ctx

        if self.ctx.event_bus is not None:
            self.ctx.event_bus.subscribe(ExecuteAIApprovalCommand, self.handle_execute_ai_approval)
            self.ctx.event_bus.subscribe(EvaluateReactiveExitEvent, self.handle_evaluate_reactive_exit)

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

    # ── outcome-driven knob / exit-policy tuning ─────────────────────────────
    async def handle_evaluate_reactive_exit(self, event: EvaluateReactiveExitEvent) -> None:
        try:
            res = await self._maybe_evaluate_reactive_exit(event.symbol, event.mgmt_actions)
            if event.future and not event.future.done():
                event.future.set_result(res)
        except Exception as exc:
            if event.future and not event.future.done():
                event.future.set_exception(exc)
            raise

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

    async def _maybe_run_bandit_tuner(self) -> None:
        """Run the Thompson-bandit knob tuner, throttled and mode-gated."""
        try:
            mode = (await self.ctx.db.settings.get_setting("bandit_tuner_mode") or "off")
            mode = str(mode).strip().lower()
            if mode not in ("shadow", "active"):
                return

            interval = int(self.ctx.config.get("bandit_tuning_interval_s", 3600))
            now = time.time()
            last_raw = await self.ctx.db.settings.get_setting("bandit_last_run_ts")
            last = float(last_raw) if last_raw else 0.0
            if interval > 0 and now - last < interval:
                return
            await self.ctx.db.settings.set_setting("bandit_last_run_ts", str(now))

            from hermes.ml.bandit import propose_knob_updates, LEARNABLE_KNOBS

            outcomes = await self.ctx.db.trades.fetch_trade_outcomes()
            keys = [k for knobs in LEARNABLE_KNOBS.values() for k in knobs]
            current: Dict[str, Any] = {}
            bulk = getattr(self.ctx.db.settings, "get_settings", None)
            if callable(bulk):
                current = await bulk(keys) or {}

            min_obs = int(self.ctx.config.get("bandit_min_observations", 20))
            proposals = propose_knob_updates(
                outcomes, current, min_observations=min_obs)

            autonomy = (getattr(self.ctx.overseer, "autonomy", "advisory")
                        if self.ctx.overseer is not None else "advisory")
            can_apply = mode == "active" and autonomy in ("enforcing", "autonomous")

            applied: Dict[str, Any] = {}
            for p in proposals:
                if can_apply and p["actionable"] and p["changed"]:
                    await self.ctx.db.settings.set_setting(p["key"], str(p["proposed"]))
                    applied[p["key"]] = p["proposed"]
                    await self.ctx.db.logs.write_log(
                        "BANDIT",
                        f"[BANDIT-TUNE] {p['key']}: {p['current']} → "
                        f"{p['proposed']} (n={p['n_obs']}, mode={mode})",
                    )

            if applied:
                logger.info("[BANDIT-TUNE] applied %s", applied)
            try:
                await self.ctx.db.decisions.write_ai_decision(
                    "BANDIT", "PARAMS", autonomy,
                    {"type": "bandit_tuning", "mode": mode,
                     "applied": applied, "proposals": proposals,
                     "min_observations": min_obs},
                )
            except Exception:
                pass
        except Exception as exc:
            logger.exception("[BANDIT-TUNE] tuning tick failed: %s", exc)

    async def _maybe_capture_and_advise_exits(self, mgmt_actions) -> None:
        """Capture exit-state trajectories and run the advisory exit policy."""
        try:
            mode = (await self.ctx.db.settings.get_setting("exit_policy_mode") or "off")
            mode = str(mode).strip().lower()
            if mode not in ("shadow", "active"):
                return

            from hermes.ml.exit_policy import train_exit_policy, recommend

            open_trades = await self.ctx.db.trades.all_open_trades()
            if not open_trades:
                return

            closing_ids = {
                (a.strategy_params or {}).get("trade_id")
                for a in (mgmt_actions or [])
                if (a.strategy_params or {}).get("trade_id") is not None
            }

            legs = set()
            for tr in open_trades:
                for k in ("short_leg", "long_leg"):
                    if tr.get(k):
                        legs.add(tr[k])
            quotes: Dict[str, Any] = {}
            if legs:
                raw = await self.ctx.broker.get_quote(",".join(sorted(legs))) or []
                quotes = {q["symbol"]: q for q in raw if "symbol" in q}

            def _mid(sym):
                q = quotes.get(sym) or {}
                try:
                    bid, ask = float(q.get("bid")), float(q.get("ask"))
                except (TypeError, ValueError):
                    return None
                return (bid + ask) / 2.0 if ask > 0 and bid >= 0 else None

            today = self.ctx.clock.utc_now().date()
            autonomy = (getattr(self.ctx.overseer, "autonomy", "advisory")
                        if self.ctx.overseer is not None else "advisory")
            can_act = mode == "active" and autonomy in ("enforcing", "autonomous")

            policy = train_exit_policy(await self.ctx.db.trades.fetch_exit_ticks())
            advice: List[Dict[str, Any]] = []
            acted: List[int] = []

            for tr in open_trades:
                entry_credit = tr.get("entry_credit")
                short_mid = _mid(tr.get("short_leg"))
                long_mid = _mid(tr.get("long_leg")) if tr.get("long_leg") else 0.0
                expiry = tr.get("expiry")
                if not entry_credit or short_mid is None or long_mid is None or not expiry:
                    continue
                debit = round(short_mid - long_mid, 4)
                pnl_pct = round((float(entry_credit) - debit) / float(entry_credit), 4)
                exp_date = expiry if hasattr(expiry, "year") else None
                if exp_date is None:
                    continue
                dte = (exp_date - today).days

                tid = tr.get("id")
                action = "close" if tid in closing_ids else "hold"
                await self.ctx.db.trades.record_exit_tick(
                    trade_id=tid, strategy_id=tr.get("strategy_id"),
                    symbol=tr.get("symbol"), dte=dte, unrealized_pnl_pct=pnl_pct,
                    debit=debit, entry_credit=float(entry_credit), action=action,
                    close_reason=("MANAGED" if action == "close" else None),
                )

                rec = recommend(policy, pnl_pct, dte)
                rec.update({"trade_id": tid, "symbol": tr.get("symbol"),
                            "pnl_pct": pnl_pct, "dte": dte})
                advice.append(rec)

                width = tr.get("width")
                close_price = round(debit * 1.05, 2)
                if width:
                    close_price = min(close_price, round(float(width), 2))

                if (can_act and rec["confident"] and tid not in closing_ids):
                    close = TradeAction(
                        strategy_id=tr.get("strategy_id"), symbol=tr.get("symbol"),
                        order_class="multileg",
                        legs=[
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                            {"option_symbol": tr["long_leg"], "side": "sell_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ] if tr.get("long_leg") else [
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ],
                        price=close_price, side="buy", quantity=1,
                        order_type="debit",
                        tag=f"HERMES_{tr.get('strategy_id')}_CLOSE_EXIT-POLICY",
                        strategy_params={"trade_id": tid, "close_reason": "EXIT-POLICY",
                                         "side_type": tr.get("side_type")},
                        ai_authored=True,
                    )
                    cmd = SubmitTradeActionsCommand(actions=[close], action_type="management")
                    self.ctx.event_bus.emit(cmd)
                    await cmd.future
                    acted.append(tid)
                    await self.ctx.db.logs.write_log(
                        "EXITPOLICY",
                        f"[EXIT-POLICY] closing trade {tid} {tr.get('symbol')} "
                        f"pnl%={pnl_pct} dte={dte} (q_close={rec['q_close']} "
                        f"> q_hold={rec['q_hold']})",
                    )

            if acted:
                logger.info("[EXIT-POLICY] closed %s", acted)
            try:
                await self.ctx.db.decisions.write_ai_decision(
                    "EXITPOLICY", "EXITS", autonomy,
                    {"type": "exit_policy", "mode": mode, "acted": acted,
                     "n_completed_trajectories": policy["n_completed_trajectories"],
                     "advice": advice},
                )
            except Exception:
                pass
        except Exception as exc:
            logger.exception("[EXIT-POLICY] capture/advise tick failed: %s", exc)

    async def _maybe_evaluate_reactive_exit(self, symbol: str, mgmt_actions) -> None:
        """Evaluate continuous exit model reactively on quote changes for a specific option symbol."""
        try:
            mode = (await self.ctx.db.settings.get_setting("exit_policy_mode") or "off")
            mode = str(mode).strip().lower()
            if mode not in ("shadow", "active"):
                return

            open_trades = await self.ctx.db.trades.all_open_trades()
            if not open_trades:
                return

            trades_for_symbol = [
                t for t in open_trades
                if t.get("short_leg") == symbol or t.get("long_leg") == symbol
            ]
            if not trades_for_symbol:
                return

            closing_ids = {
                (a.strategy_params or {}).get("trade_id")
                for a in (mgmt_actions or [])
                if (a.strategy_params or {}).get("trade_id") is not None
            }

            from hermes.ml.exit_policy import train_exit_policy, recommend

            today = self.ctx.clock.utc_now().date()
            autonomy = (getattr(self.ctx.overseer, "autonomy", "advisory")
                        if self.ctx.overseer is not None else "advisory")
            can_act = mode == "active" and autonomy in ("enforcing", "autonomous")

            policy = train_exit_policy(await self.ctx.db.trades.fetch_exit_ticks())
            advice: List[Dict[str, Any]] = []
            acted: List[int] = []

            for tr in trades_for_symbol:
                tid = tr.get("id")
                if tid in closing_ids:
                    continue

                entry_credit = tr.get("entry_credit")
                short_leg = tr.get("short_leg")
                long_leg = tr.get("long_leg")
                expiry = tr.get("expiry")

                if not entry_credit or not expiry:
                    continue

                short_mid = None
                if short_leg in self.ctx.quote_cache:
                    q = self.ctx.quote_cache[short_leg]
                    try:
                        short_mid = (float(q.get("bid")) + float(q.get("ask"))) / 2.0
                    except (TypeError, ValueError):
                        pass

                long_mid = 0.0
                if long_leg:
                    if long_leg in self.ctx.quote_cache:
                        q = self.ctx.quote_cache[long_leg]
                        try:
                            long_mid = (float(q.get("bid")) + float(q.get("ask"))) / 2.0
                        except (TypeError, ValueError):
                            pass
                    else:
                        continue

                if short_mid is None:
                    continue

                debit = round(short_mid - long_mid, 4)
                pnl_pct = round((float(entry_credit) - debit) / float(entry_credit), 4)
                exp_date = expiry if hasattr(expiry, "year") else None
                if exp_date is None:
                    continue
                dte = (exp_date - today).days

                rec = recommend(policy, pnl_pct, dte)
                rec.update({"trade_id": tid, "symbol": tr.get("symbol"),
                            "pnl_pct": pnl_pct, "dte": dte})
                advice.append(rec)

                width = tr.get("width")
                close_price = round(debit * 1.05, 2)
                if width:
                    close_price = min(close_price, round(float(width), 2))

                if (can_act and rec["confident"]):
                    close = TradeAction(
                        strategy_id=tr.get("strategy_id"), symbol=tr.get("symbol"),
                        order_class="multileg",
                        legs=[
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                            {"option_symbol": tr["long_leg"], "side": "sell_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ] if tr.get("long_leg") else [
                            {"option_symbol": tr["short_leg"], "side": "buy_to_close",
                             "quantity": int(tr.get("lots") or 1)},
                        ],
                        price=close_price, side="buy", quantity=1,
                        order_type="debit",
                        tag=f"HERMES_{tr.get('strategy_id')}_CLOSE_EXIT-POLICY-REACTIVE",
                        strategy_params={"trade_id": tid, "close_reason": "EXIT-POLICY-REACTIVE",
                                         "side_type": tr.get("side_type")},
                        ai_authored=True,
                    )
                    cmd = SubmitTradeActionsCommand(actions=[close], action_type="management")
                    self.ctx.event_bus.emit(cmd)
                    await cmd.future
                    acted.append(tid)
                    await self.ctx.db.logs.write_log(
                        "EXITPOLICY",
                        f"[REACTIVE-EXIT] closing trade {tid} {tr.get('symbol')} "
                        f"pnl%={pnl_pct} dte={dte} (q_close={rec['q_close']} "
                        f"> q_hold={rec['q_hold']})",
                    )

            if acted:
                logger.info("[REACTIVE-EXIT] closed %s", acted)
                try:
                    await self.ctx.db.decisions.write_ai_decision(
                        "EXITPOLICY", "REACTIVE-EXITS", autonomy,
                        {"type": "exit_policy_reactive", "mode": mode, "acted": acted,
                         "advice": advice},
                    )
                except Exception:
                    pass
        except Exception as exc:
            logger.exception("[REACTIVE-EXIT] evaluation failed: %s", exc)
