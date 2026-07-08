"""
[Service-1: Hermes-Agent-Core] — tick-pipeline + heartbeat controller.

Split out of ``core.py`` so the engine spine (``CascadingEngine._run_tick_internal``)
stays pure orchestration — it names the phases (sync → reconcile → manage →
entries → submit) and ``PipelineController`` owns each phase *body*. It also owns
the slow operator-guard heartbeat (``handle_clock_tick_internal``) that wraps the
trading pipeline: circuit breaker, pause / kill-switch, daily-loss limit, stale
cleanup, approved-action execution, the market-hours gate, weekly chart-vision
analysis, and live status writes.

``PipelineController`` is an owned collaborator of
:class:`~hermes.service1_agent.core.CascadingEngine` (``engine.pipeline``). It
reads the shared dependency surface
(``mm`` / ``overseer`` / ``control_state`` / ``risk_engine`` / ``approval_mode`` /
``db`` / ``broker`` / ``config`` …) off the
:class:`~hermes.service1_agent.engine_context.EngineContext` (``self.ctx``), and
keeps ``self.engine`` for the cross-phase orchestration calls it routes through
the engine spine (``submit`` / ``sync_positions`` / ``_execute_or_queue`` …,
which stay the single seam tests monkeypatch) and the engine's circuit-breaker
counters.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence

from hermes.events.bus import ReviewRequestEvent, AIApprovalEvent, ClockTickEvent
from .trade_action import TradeAction

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class PipelineController:
    """The agent tick pipeline — one method per phase body.

    Phases run in priority order from ``CascadingEngine._run_tick_internal``:
        1. :meth:`sync_positions`   (broker → DB)
        2. :meth:`reconcile_orphans`
        3. :meth:`process_management`
        4. :meth:`process_entries`  (CS75 → CS7 → TT45 → Wheel → HermesAlpha)
    with :meth:`submit` / :meth:`_execute_or_queue` as the shared order sink.
    """

    def __init__(self, engine: "CascadingEngine") -> None:
        self.engine = engine
        # Shared dependency surface read through the context; ``self.engine`` is
        # kept for cross-phase orchestration calls + circuit-breaker counters.
        self.ctx = engine.ctx

    # 1
    async def sync_positions(self) -> tuple[List[Dict[str, Any]], set[str]]:
        ctx = self.ctx
        positions = await ctx.broker.get_positions() or []
        if not isinstance(positions, list):
            logger.warning("[ENGINE] get_positions returned non-list: %r", positions)
            positions = []
        # Resting/accepted orders haven't created positions yet; the
        # reconciler must treat their legs as still-alive coverage so
        # just-submitted spreads aren't flipped to CLOSED before fill.
        active_legs: set = set()
        try:
            active_statuses = {"open", "partially_filled", "pending",
                                "accepted", "calculated"}
            orders = await ctx.broker.get_orders() or []
            if not isinstance(orders, list):
                logger.warning("[ENGINE] get_orders returned non-list: %r", orders)
                orders = []
            for o in orders:
                if str(o.get("status", "")).lower() not in active_statuses:
                    continue
                legs = o.get("leg") or []
                if isinstance(legs, dict):
                    legs = [legs]
                for leg in legs:
                    sym = leg.get("option_symbol")
                    if sym:
                        active_legs.add(sym)
                top = o.get("option_symbol")
                if top:
                    active_legs.add(top)
        except Exception:
            logger.exception("[ENGINE] active-order leg fetch failed")
        await ctx.db.trades.upsert_positions(positions, active_order_legs=active_legs)
        return positions, active_legs

    # 2
    async def reconcile_orphans(self) -> None:
        """Adopt Hermes-tagged orphans into a Trade row; flag the rest.

        A broker position lands here as an "orphan" whenever it has no
        matching OPEN/CLOSING ``Trade`` row — normally because it's a
        position the operator opened by hand outside Hermes. But it can also
        happen when Hermes' own fill was recorded at the broker while the
        local bookkeeping step never completed (e.g. an exception between
        the broker accepting the order and ``record_order_response``
        writing the Trade row) — that variant is silently invisible to
        every strategy's TP/SL/time-exit logic forever, since
        ``manage_positions`` only ever looks at tracked ``Trade`` rows.

        A position carrying a recognizable ``HERMES_<STRAT>``/``HERMES-<STRAT>``
        order tag (CLAUDE.md safety rule #5) is the latter case — re-run it
        through the normal fill-recording path so it becomes a real, managed
        Trade again. Anything without a Hermes tag is left untouched and
        just logged; that's a genuine manual/foreign position, not ours to
        adopt.
        """
        ctx = self.ctx
        tracked = await ctx.db.trades.tracked_option_symbols()
        live = {p["symbol"] for p in await ctx.broker.get_positions() or []}
        orphans = live - tracked
        if not orphans:
            return

        orders = await ctx.broker.get_orders() or []
        adopted = await self._adopt_orphans(orphans, orders)
        remaining = orphans - adopted
        if remaining:
            await ctx.db.logs.flag_orphans(remaining)

    async def _adopt_orphans(self, orphans: set, orders: List[Dict[str, Any]]) -> set:
        """Reopen orphaned legs whose originating order carries a Hermes tag.

        Groups orphan symbols by the (filled) broker order that produced
        them, so a 2-leg spread is adopted as one Trade rather than two.
        Returns the subset of ``orphans`` successfully adopted.
        """
        from hermes.common import strategy_id_from_tag
        from .strategies._helpers import parse_occ

        ctx = self.ctx
        adopted: set = set()
        for order in orders:
            if str(order.get("status", "")).lower() != "filled":
                continue
            tag = order.get("tag") or ""
            strategy_id = strategy_id_from_tag(tag)
            if not strategy_id:
                continue

            legs = order.get("leg") or order.get("legs") or []
            if isinstance(legs, dict):
                legs = [legs]
            leg_symbols = {leg.get("option_symbol") for leg in legs if leg.get("option_symbol")}
            if not leg_symbols:
                top_sym = order.get("option_symbol")
                if top_sym:
                    leg_symbols = {top_sym}

            matched = leg_symbols & orphans
            if not matched or matched <= adopted:
                continue

            expiry = None
            for sym in leg_symbols:
                parsed = parse_occ(sym)
                if parsed:
                    expiry = parsed["expiry"].isoformat()
                    break

            action = TradeAction(
                strategy_id=strategy_id,
                symbol=str(order.get("symbol") or "").upper(),
                order_class="multileg" if len(legs) > 1 else "option",
                legs=legs,
                price=(
                    float(order.get("price"))
                    if order.get("price") is not None and str(order.get("price")).strip() != ""
                    else (
                        float(order.get("avg_fill_price"))
                        if order.get("avg_fill_price") is not None and str(order.get("avg_fill_price")).strip() != ""
                        else None
                    )
                ),
                side=str(order.get("side") or "sell"),
                quantity=int(order.get("quantity") if order.get("quantity") is not None else 1),
                order_type="credit",
                expiry=expiry,
                tag=tag,
            )
            resp = {"order": {"id": order.get("id") or order.get("order_id"), "status": "filled"}}
            try:
                await ctx.db.trades.record_order_response(action, resp)
            except Exception:
                logger.exception("[ENGINE] orphan adoption failed for order %s", order.get("id"))
                continue

            adopted |= matched
            await ctx.db.logs.write_log(
                "ENGINE",
                f"[ORPHAN ADOPTED] {action.symbol} strategy={strategy_id} "
                f"legs={sorted(matched)} tag={tag} — reopened as a tracked Trade",
            )
        return adopted

    # 3
    async def process_management(self) -> List[TradeAction]:
        ctx = self.ctx

        async def _run_strategy_management(s):
            try:
                return await s.manage_positions()
            except Exception as exc:
                logger.exception("Management failure in %s: %s", s.NAME, exc)
                return []

        results = await asyncio.gather(*[_run_strategy_management(s) for s in ctx.strategies])
        actions: List[TradeAction] = []
        for res in results:
            actions.extend(res)
        return actions

    # 4
    async def _watchlist_for(self, strategy_id: str, default: Sequence[str]) -> List[str]:
        """Per-strategy watchlist with fallback to the engine-level default."""
        ctx = self.ctx
        getter = getattr(ctx.db.watchlist, "list_watchlist", None)
        if getter is None:
            return list(default)
        try:
            import inspect
            if inspect.iscoroutinefunction(getter):
                wl = await getter(strategy_id)
            else:
                wl = getter(strategy_id)
                if inspect.iscoroutine(wl):
                    wl = await wl
        except Exception as exc:
            logger.exception("watchlist read failed for %s: %s", strategy_id, exc)
            return list(default)
        return (wl or []) or list(default)

    async def process_entries(self, watchlist: Sequence[str]) -> int:
        """Execute entries in priority order. Delegates validation, safety checks,
        and lot scaling to the centralized PortfolioRiskEngine.
        Returns total number of entry actions planned.
        """
        engine = self.engine
        ctx = self.ctx
        unique_watchlist = list(dict.fromkeys(watchlist))
        max_per_tick = int(ctx.config.get("max_orders_per_tick", 5))

        # Gather proposed actions across all strategies concurrently
        async def _run_strategy_entries(s):
            try:
                wl = await self._watchlist_for(s.strategy_id, unique_watchlist)
                return s, await s.execute_entries(wl)
            except Exception as exc:
                logger.exception("Entry proposal failure in %s: %s", s.NAME, exc)
                return s, []

        results = await asyncio.gather(*[_run_strategy_entries(s) for s in ctx.strategies])

        all_proposed_actions = []
        for _s, actions in results:
            all_proposed_actions.extend(actions)

        # Keep the risk engine's per-strategy lot caps in sync with the
        # operator's lot settings. control_state owns them (event-updated, with
        # the clock-tick DB backstop); the risk engine reads them from the shared
        # config dict, so push them across here before evaluation. Without this,
        # risk_engine falls back to a hard-coded 1-lot cap and the bot silently
        # under-trades, ignoring cs*/tt*/wheel _max_lots entirely.
        if ctx.control_state is not None:
            ctx.config.update(ctx.control_state.lot_settings)

        # Delegate validation, scaling, and risk filtering to risk engine
        validated_actions = await ctx.risk_engine.evaluate_and_scale(all_proposed_actions)

        # Cap to max per tick
        if len(validated_actions) > max_per_tick:
            logger.warning(
                "[ENGINE] Risk-validated entries generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                len(validated_actions), max_per_tick, max_per_tick,
            )
            for a in validated_actions[max_per_tick:]:
                await ctx.db.logs.write_log(
                    a.strategy_id,
                    f"[GUARD] {a.symbol} entry trimmed due to max_orders_per_tick={max_per_tick}"
                )
            validated_actions = validated_actions[:max_per_tick]

        # Submit the validated actions
        await engine.submit(validated_actions, action_type="entry")

        # Sync broker orders
        if validated_actions and ctx.mm is not None:
            await ctx.mm.sync_broker_orders()

        return len(validated_actions)

    async def _attach_entry_features(self, a: TradeAction) -> None:
        """Snapshot the resolved knobs + entry context onto an entry action.

        Best-effort and fail-open: any error here must never block a trade, so
        we swallow exceptions and simply leave ``entry_features`` unset. The
        snapshot rides in ``strategy_params`` so it survives both the direct
        broker path and the approval-queue round-trip (``dataclasses.asdict``),
        and is persisted by ``record_order_response``.
        """
        ctx = self.ctx
        try:
            from .tunables import resolve as _resolve_tunables
            from .strategies._helpers import entry_feature_snapshot

            sp = dict(a.strategy_params or {})
            if "entry_features" in sp:        # already stamped (e.g. by a strategy)
                return
            try:
                knobs = (await _resolve_tunables(
                    ctx.db, ctx.config, group=a.strategy_id)).as_dict()
            except Exception:
                knobs = None

            credit = a.price if (a.order_type or "").lower() == "credit" else None
            sp["entry_features"] = entry_feature_snapshot(
                a.strategy_id,
                knobs,
                side_type=sp.get("side_type"),
                pop=sp.get("pop"),
                short_delta=sp.get("short_delta"),
                width=getattr(a, "width", None),
                entry_credit=credit,
                expiry=getattr(a, "expiry", None),
                ai_authored=bool(getattr(a, "ai_authored", False)),
                extra=({"ev": sp["ev"]} if sp.get("ev") is not None else None),
            )
            a.strategy_params = sp
        except Exception:
            logger.debug("entry-feature snapshot failed for %s", a.symbol,
                         exc_info=True)

    async def submit(self, actions: Iterable[TradeAction],
               action_type: str = "entry") -> None:
        engine = self.engine
        ctx = self.ctx
        if ctx.event_bus is not None:
            engine._ensure_event_loop()
        # Defence-in-depth market-hours gate. Every broker round-trip
        # MUST go through this method (entries, managed closes, AI
        # actions) so a single check here keeps the bot from sending
        # orders into pre-market / after-hours / weekend / holiday
        # windows where quote feeds are stale and fills are punitive.
        # Operators who explicitly want off-hours submission can set
        # HERMES_ALLOW_OFFHOURS_TRADES=true (see market_hours.py).
        from hermes.market_hours import should_block_trades
        blocked, reason = should_block_trades()
        if blocked:
            actions = list(actions)
            for a in actions:
                await ctx.db.logs.write_log(
                    a.strategy_id,
                    f"[OFF-HOURS BLOCKED] {a.symbol} {action_type} "
                    f"qty={a.quantity} — {reason}; not sent to broker",
                )
            if actions:
                logger.info("[OFF-HOURS] blocked %d %s action(s): %s",
                            len(actions), action_type, reason)
            return
        for a in actions:
            # Phase-0 outcome instrumentation: stamp every entry with a snapshot
            # of the knobs + market context that produced it, so the realized
            # P&L on close becomes a labelled training row. Pure observation —
            # never alters the action's economics or routing.
            if action_type in ("entry", "ai"):
                await engine._attach_entry_features(a)

            # Veto-suppression: if the overseer already vetoed this exact
            # entry within the TTL window, skip re-proposing it instead of
            # brute-forcing the same action through review every tick. Only
            # entries are suppressed — management closes/rolls must always be
            # allowed through, and AI-authored actions bypass review anyway.
            if ctx.overseer is not None and action_type == "entry":
                veto_side = (a.strategy_params or {}).get("side_type")
                if veto_side and str(veto_side).lower() in {"buy", "sell"}:
                    veto_side = None
                try:
                    veto_reason = await ctx.db.approvals.active_veto(
                        a.strategy_id, a.symbol, veto_side, a.expiry)
                except Exception:
                    logger.exception("[VETO] active_veto lookup failed for %s", a.symbol)
                    veto_reason = None
                if veto_reason:
                    logger.info("[VETO-SUPPRESSED] %s %s side=%s expiry=%s",
                                a.strategy_id, a.symbol, veto_side, a.expiry)
                    await ctx.db.logs.write_log(
                        a.strategy_id,
                        f"[VETO-SUPPRESSED] {a.symbol} {veto_side or ''} "
                        f"expiry={a.expiry} — skipped re-proposal "
                        f"(active AI veto: {veto_reason})",
                    )
                    continue

            if ctx.llm_out_of_loop:
                await engine._execute_or_queue(a, action_type)
                continue

            if ctx.event_bus is not None:
                if (ctx.overseer is not None and action_type != "ai"
                        and not getattr(a, "ai_authored", False)):
                    # Queue for AI review in the database
                    action_dict = dataclasses.asdict(a)
                    approval_id = await ctx.db.approvals.queue_for_approval(
                        action_dict, action_type=action_type, status="PENDING_AI_REVIEW"
                    )
                    # Yield to AI Overseer asynchronously
                    event = ReviewRequestEvent(
                        strategy_id=a.strategy_id,
                        symbol=a.symbol,
                        trade_action=a,
                        action_type=action_type,
                        approval_id=approval_id,
                    )
                    ctx.event_bus.emit(event)
                else:
                    # Bypasses AI review (either no overseer, or action is already AI-authored)
                    event = AIApprovalEvent(
                        strategy_id=a.strategy_id,
                        symbol=a.symbol,
                        verdict="APPROVE",
                        rationale="Auto-approved (AI-authored or no overseer).",
                        original_action=a,
                        action_type=action_type,
                    )
                    ctx.event_bus.emit(event)
                continue

            # AI override hook — overseer may VETO, MODIFY, or APPROVE the action.
            # review() is async; without awaiting it `a` becomes a coroutine and
            # VETO/MODIFY verdicts are silently dropped on this non-event-bus path.
            # AI-authored actions (e.g. overseer-proposed closes) skip review —
            # the overseer must not re-review its own decision.
            if ctx.overseer is not None and not getattr(a, "ai_authored", False):
                a = await ctx.overseer.review(a)
                if a is None:
                    continue

            await engine._execute_or_queue(a, action_type)

    async def _execute_or_queue(self, a: TradeAction, action_type: str, approval_id: Optional[int] = None) -> None:
        """Single order sink shared by both execution paths.

        Called from the synchronous ``submit()`` loop *and* from the event-bus
        ``handle_ai_approval()`` handler. Keeping both callers on one method is
        the point: the dedup guard, the pure-close routing and the dry-run
        guard can never drift between the two paths and let a money bug slip
        into one but not the other.

        In ``approval_mode`` the action is queued for human review (deduped on
        strategy/symbol/side/expiry). Otherwise a PENDING order is recorded and
        — unless the broker is in dry-run — sent to the broker, with pure
        management closes routed to ``close_trade_from_action`` so they update
        the original Trade row instead of inserting a ghost OPEN.
        """
        engine = self.engine
        ctx = self.ctx
        side_type = (a.strategy_params or {}).get("side_type")

        # No-human-in-the-loop carve-out (CLAUDE.md safety rule #2): an action
        # from ANY strategy skips the human approval queue when the operator has
        # explicitly armed the default-OFF ``alpha_autonomous_live`` switch *and*
        # autonomy is 'autonomous'. Every other gate — dry_run, paper/live,
        # off-hours, PortfolioRiskEngine — still applies; only the human approval
        # step is bypassed.
        cs = ctx.control_state
        autonomous_live_bypass = (
            cs is not None
            and str(getattr(cs, "autonomy", "")).lower() == "autonomous"
            and bool(getattr(cs, "alpha_autonomous_live", False))
        )

        if ctx.approval_mode and not autonomous_live_bypass:
            if approval_id is None:
                # Dedup guard: never re-queue a trade that already has a PENDING
                # approval for the same (strategy, symbol, side, expiry). Without
                # this, every tick re-generates and re-queues the same spread
                # because the approval hasn't been actioned yet.
                if await ctx.db.approvals.has_pending_approval(a.strategy_id, a.symbol,
                                                      side_type, a.expiry):
                    logger.info(
                        "[C2] Skipping duplicate — already PENDING: %s %s "
                        "side=%s expiry=%s",
                        a.strategy_id, a.symbol, side_type, a.expiry,
                    )
                    await ctx.db.logs.write_log(
                        a.strategy_id,
                        f"[DEDUP] {a.symbol} {side_type} expiry={a.expiry} "
                        f"already PENDING approval — skipped",
                    )
                    return

                # Queue for human review instead of firing directly.
                action_dict = dataclasses.asdict(a)
                await ctx.db.approvals.queue_for_approval(action_dict, action_type=action_type)
            else:
                # Transition the existing PENDING_AI_REVIEW row to PENDING
                await ctx.db.approvals.update_approval_status(approval_id, "PENDING")

            logger.info(
                "[C2] Trade queued for approval: %s %s strategy=%s side=%s expiry=%s",
                a.symbol, a.order_class, a.strategy_id, side_type, a.expiry,
            )
            await ctx.db.logs.write_log(
                a.strategy_id,
                f"[APPROVAL REQUIRED] {a.symbol} {a.order_class} "
                f"side={side_type} expiry={a.expiry} "
                f"qty={a.quantity} — awaiting human approval",
            )
            return

        if autonomous_live_bypass and ctx.approval_mode:
            await ctx.db.logs.write_log(
                a.strategy_id,
                f"[AUTO-EXECUTE] {a.symbol} {a.order_class} side={side_type} "
                f"expiry={a.expiry} qty={a.quantity} — approval_mode bypassed "
                f"(autonomous + alpha_autonomous_live); routing to broker",
            )

        await ctx.db.trades.record_pending_order(a)
        # Management actions whose legs are all *_to_close represent the close
        # of an existing trade, not a new entry. Route them to
        # ``close_trade_from_action`` which UPDATES the original Trade row
        # (status→CLOSED, exit_price, pnl, close_tag, close_reason) instead of
        # inserting a ghost OPEN row that the reconciler later flattens with a
        # generic 'RECONCILED_BROKER_FLAT' and pnl=NULL.
        #
        # Any management action that opens a leg (e.g. WHEEL_ROLL, which
        # buys-to-close + sells-to-open the same strike on the next month)
        # keeps the legacy path so the new short still gets a Trade row.
        is_pure_close = (
            action_type == "management"
            and bool(a.legs)
            and all("to_open" not in (leg.get("side") or "").lower()
                    for leg in a.legs)
        )
        close_method = getattr(ctx.db.trades, "close_trade_from_action", None)
        if getattr(ctx.broker, "dry_run", False):
            # Nothing was sent to the broker, so the PENDING row from
            # record_pending_order() above must be freed the same way a real
            # broker rejection frees it — otherwise it permanently occupies
            # this (strategy, symbol, side, expiry) capacity slot and every
            # future tick gets "[MM] BLOCKED ... at capacity" with no order
            # ever having reached the broker.
            dry_run_resp = {"errors": "dry_run=True — no broker order placed"}
            if is_pure_close and close_method is not None:
                await close_method(a, dry_run_resp)
            else:
                await ctx.db.trades.record_order_response(a, dry_run_resp)
            if approval_id is not None:
                await ctx.db.approvals.mark_approval_executed(
                    approval_id, success=False,
                    notes="dry_run=True — no broker order placed",
                )
            return
        try:
            resp = await ctx.broker.place_order_from_action(a)
        except Exception as exc:
            # Broker raised before we got an order id. Free the PENDING row so
            # capacity recovers; a Trade row was never written, nothing to roll
            # back.
            if is_pure_close and close_method is not None:
                await close_method(a, {"errors": str(exc)})
            else:
                await ctx.db.trades.record_order_response(a, {"errors": str(exc)})
            if approval_id is not None:
                await ctx.db.approvals.mark_approval_executed(
                    approval_id, success=False,
                    notes=f"broker raised: {exc}",
                )
            logger.exception("place_order failed for %s: %s", a.symbol, exc)
        else:
            if is_pure_close and close_method is not None:
                await close_method(a, resp)
            else:
                await ctx.db.trades.record_order_response(a, resp)
            if approval_id is not None:
                order = (resp or {}).get("order") if isinstance(resp, dict) else None
                order_status = ""
                if isinstance(order, dict):
                    order_status = str(order.get("status", "")).lower()
                from hermes.service1_agent.agent_approvals import _REJECTED_ORDER_STATUSES
                rejected = (
                    (isinstance(resp, dict) and "errors" in resp)
                    or order_status in _REJECTED_ORDER_STATUSES
                )
                if rejected:
                    await ctx.db.approvals.mark_approval_executed(
                        approval_id, success=False,
                        notes=f"broker rejected: {resp}",
                    )
                else:
                    await ctx.db.approvals.mark_approval_executed(approval_id, success=True)
            if resp and isinstance(resp, dict):
                oid = str(resp.get("order_id") or resp.get("id") or "")
                if oid:
                    engine._tracked_orders[oid] = {
                        "symbol": a.symbol,
                        "side": a.side,
                        "quantity": a.quantity
                    }
                    logger.info("[ENGINE] Registered order %s in reactive monitor", oid)
                    engine._ensure_order_monitor()

    # ── POP outcome calibration (slow heartbeat job) ──────────────────────────
    _POP_CAL_FIT_KEY = "pop_cal_last_fit"       # ISO ts of the last fit attempt
    _POP_CAL_REFIT_S = 6 * 3600

    @property
    def _POP_CAL_STATE_KEY(self) -> str:
        # Shared with the watcher's read-side sync (pop_calibration.py) so the
        # two processes can't drift on the settings key name.
        from hermes.ml.pop_calibration import POP_CAL_STATE_KEY
        return POP_CAL_STATE_KEY

    async def maybe_refit_pop_calibrator(self) -> None:
        """Refit the POP outcome calibrator from closed trades, throttled.

        Runs on the slow heartbeat. The fitted parameters are persisted in
        ``system_settings`` (agent-owned per the single-writer invariant) so a
        restart re-installs the last calibrator instead of running naked until
        the next refit window. Every failure path is non-fatal — calibration
        must never take down a trading tick.
        """
        import json
        from datetime import datetime, timezone

        from hermes.ml.calibration import PlattCalibrator
        from hermes.ml.pop_calibration import fit_pop_calibrator
        from hermes.ml.pop_engine import get_pop_calibrator, set_pop_calibrator

        ctx = self.ctx
        try:
            # Restart recovery: nothing installed in-process but a persisted
            # calibrator exists → re-install it before the throttle can skip.
            if get_pop_calibrator() is None:
                blob = await ctx.db.settings.get_setting(self._POP_CAL_STATE_KEY)
                if blob:
                    state = json.loads(blob)
                    set_pop_calibrator(PlattCalibrator.from_dict(state["calibrator"]))
                    logger.info("[POP-CAL] reinstalled persisted calibrator "
                                "(fitted_at=%s n=%s)",
                                state.get("fitted_at"), state.get("n"))

            raw = await ctx.db.settings.get_setting(self._POP_CAL_FIT_KEY)
            now = datetime.now(timezone.utc)
            if raw:
                try:
                    last = datetime.fromisoformat(raw)
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    if (now - last).total_seconds() < self._POP_CAL_REFIT_S:
                        return
                except ValueError:
                    pass

            result = await fit_pop_calibrator(ctx.db)
            await ctx.db.settings.set_setting(self._POP_CAL_FIT_KEY, now.isoformat(timespec="seconds"))
            if result is None:
                return                     # too few labelled rows / no improvement

            calibrator = result.pop("calibrator")
            set_pop_calibrator(calibrator)
            state = {"calibrator": calibrator.to_dict(), **result}
            await ctx.db.settings.set_setting(self._POP_CAL_STATE_KEY, json.dumps(state))
            await ctx.db.logs.write_log(
                "ENGINE",
                f"[POP-CAL] calibrator refit on {result['n']} closed trades "
                f"(wins={result['wins']} losses={result['losses']}, "
                f"log-loss {result['log_loss_raw']:.4f}→{result['log_loss_cal']:.4f})",
            )
        except Exception as exc:
            logger.warning("[POP-CAL] refit skipped: %s", exc)

    # ── slow heartbeat tick (operator guards wrapping the pipeline) ───────────
    # Owns the body of ``CascadingEngine._handle_clock_tick_internal``. The actual
    # trading work is delegated back to ``engine._run_tick_internal`` (and the
    # phase methods ``engine.sync_positions`` / ``engine.reconcile_orphans``) so
    # those remain the single seams tests monkeypatch.
    async def handle_clock_tick_internal(self, event: ClockTickEvent) -> None:
        engine = self.engine
        ctx = self.ctx
        from hermes.service1_agent.agent_risk import enforce_daily_loss_limit
        from hermes.service1_agent.agent_approvals import _execute_approved_action
        from hermes.market_hours import market_session, next_open
        from datetime import datetime, timezone
        import time

        if not ctx.control_state:
            logger.warning("[ENGINE] handle_clock_tick: control_state is not set on the engine.")
            return

        # 1. Circuit breaker check
        _CB_THRESHOLD = 5
        _CB_COOLDOWN_S = 300
        if engine._cb_fail_count >= _CB_THRESHOLD:
            if time.time() - engine._cb_tripped_at < _CB_COOLDOWN_S:
                logger.info("[CIRCUIT BREAKER] Cooling down, skipping clock tick.")
                return
            # Cooldown elapsed
            engine._cb_fail_count = 0
            engine._cb_tripped_at = 0.0
            logger.info("[CIRCUIT BREAKER] Cooldown elapsed — resuming ticks.")

        try:
            # 0. Backstop re-sync. Control state is normally updated by settings
            # events, but Redis pub/sub is fire-and-forget — a dropped one
            # could leave us trading on stale pause / kill-switch / lot state.
            # Re-hydrate from the DB on the slow clock cadence so a missed event
            # self-heals. Throttled by last_sync_ts so IPC-triggered ticks (which
            # already reloaded) don't re-read needlessly.
            from hermes.service1_agent.control_state import CONTROL_STATE_BACKSTOP_S
            _last = ctx.control_state.last_sync_ts
            if _last is None or (
                datetime.now(timezone.utc) - _last
            ).total_seconds() >= CONTROL_STATE_BACKSTOP_S:
                try:
                    await ctx.control_state.load_from_db(ctx.db, ctx.config)
                except Exception as exc:
                    logger.warning("[ENGINE] control_state backstop reload failed: %s", exc)

            # 2. Pause check
            if ctx.control_state.paused:
                logger.info("[ENGINE] heartbeat tick PAUSED mode=%s", ctx.control_state.mode)
                await ctx.db.logs.write_log("ENGINE", f"heartbeat tick PAUSED mode={ctx.control_state.mode}")
                return

            # 3. Daily loss check
            from hermes.service1_agent.agent_risk import resolve_max_daily_loss
            _max_daily_loss = resolve_max_daily_loss(ctx.control_state.max_daily_loss)
            if await enforce_daily_loss_limit(
                ctx.db, _max_daily_loss,
                currently_paused=ctx.control_state.paused, broker=ctx.broker.broker,
            ):
                ctx.control_state.paused = True
                return

            # 3b. HermesAlpha weekly kill switch — disables autonomous Alpha when
            # its trailing-week performance breaches a bound. Runs on the slow
            # heartbeat (not every reactive tick); never blocks the rest of the
            # tick on failure.
            try:
                from hermes.service1_agent.alpha_killswitch import enforce_alpha_killswitch
                await enforce_alpha_killswitch(
                    ctx.db, ctx.broker.broker, ctx.control_state, ctx.config)
            except Exception as exc:
                logger.warning("[ENGINE] alpha kill switch check failed: %s", exc)

            # 4. Clean stale pending orders & approvals
            try:
                expired = await ctx.db.trades.expire_stale_pending_orders(ctx.control_state.pending_order_ttl_s)
                if expired:
                    logger.info("Expired %d stale PENDING order(s)", expired)
                    await ctx.db.logs.write_log("ENGINE", f"expired {expired} stale PENDING order(s)")
            except Exception as exc:
                logger.warning("expire_stale_pending_orders failed: %s", exc)

            try:
                expired_approvals = await ctx.db.approvals.expire_stale_approvals()
                if expired_approvals:
                    logger.info("Auto-expired %d stale approval(s)", expired_approvals)
                    await ctx.db.logs.write_log("ENGINE", f"auto-expired {expired_approvals} stale approval(s) past deadline")
            except Exception as exc:
                logger.warning("expire_stale_approvals failed: %s", exc)

            # 4b. LLM client liveness check
            if ctx.overseer is not None:
                try:
                    await asyncio.wait_for(ctx.overseer.ping(), timeout=10.0)
                except Exception as ping_exc:
                    logger.warning("[ENGINE] Periodic LLM ping failed: %s", ping_exc)

            # 5. Execute approved actions
            try:
                approved_actions = await ctx.db.approvals.fetch_approved_actions()
                for item in approved_actions:
                    await _execute_approved_action(item, broker=ctx.broker.broker, db=ctx.db)
            except Exception as exc:
                logger.warning("Executing approved actions failed: %s", exc)

            # 5b. POP outcome calibration (throttled internally; never fatal)
            await self.maybe_refit_pop_calibrator()

            # 5c. Prediction ledger outcome backfilling (never fatal)
            try:
                from hermes.ml.ledger import backfill_prediction_outcomes
                marked = await backfill_prediction_outcomes(ctx.db, lookback_days=90)
                if marked > 0:
                    logger.info("[ENGINE] Backfilled %d outcomes in prediction ledger", marked)
            except Exception as exc:
                logger.warning("[ENGINE] Prediction ledger outcome backfilling failed: %s", exc)

            # 6. Heartbeat and Market-hours gate
            mkt = market_session()
            await ctx.db.logs.write_log(
                "ENGINE",
                f"heartbeat tick start mode={ctx.control_state.mode} market={mkt['session']} open={mkt['is_open']}"
            )

            if not mkt["trading_day"]:
                nxt = next_open()
                await ctx.db.logs.write_log(
                    "ENGINE",
                    f"market CLOSED — next open {nxt.strftime('%Y-%m-%d %H:%M ET')} ({mkt['et_date']} is not a trading day)"
                )
                return

            # 7. Execute entries/management tick loop
            unique_syms = set()
            for syms in ctx.control_state.watchlist.values():
                unique_syms.update(syms)
            current_watchlist = sorted(list(unique_syms | set(ctx.config.get("watchlist", []))))

            if mkt["is_open"]:
                stats = await engine._run_tick_internal(current_watchlist)
            else:
                await engine.sync_positions()
                await engine.reconcile_orphans()
                stats = {"managed": 0, "entries": 0, "note": f"all submissions skipped ({mkt['session']})"}

            # 8. Daily-bar ingestion — feeds bars_daily for chart rendering
            _BAR_INGEST_KEY = "bar_ingest_last_run"
            _bar_ingest_watchlist = sorted(list(set(current_watchlist)))
            if _bar_ingest_watchlist and hasattr(ctx.broker, "get_history"):
                _should_ingest = False
                try:
                    _last_ingest_raw = await ctx.db.settings.get_setting(_BAR_INGEST_KEY)
                    if not _last_ingest_raw:
                        _should_ingest = True
                    else:
                        from datetime import datetime, timezone
                        try:
                            _s = _last_ingest_raw
                            _last_ingest_dt = datetime.fromisoformat(
                                _s[:-1] + "+00:00" if _s.endswith("Z") else _s
                            )
                            if not _last_ingest_dt.tzinfo:
                                _last_ingest_dt = _last_ingest_dt.replace(tzinfo=timezone.utc)
                            _ingest_age_h = (datetime.now(timezone.utc) - _last_ingest_dt).total_seconds() / 3600
                            _should_ingest = _ingest_age_h >= 20
                        except ValueError:
                            _should_ingest = True
                except Exception as exc:
                    logger.warning("Failed to check daily bar ingest age: %s", exc)
                    _should_ingest = True

                if _should_ingest:
                    logger.info("Ingesting daily bars for %d symbols", len(_bar_ingest_watchlist))
                    from datetime import date, timedelta
                    import pandas as pd
                    _ingest_end = date.today()
                    _ingest_start = _ingest_end - timedelta(days=220)
                    _ingested = 0
                    for _sym in _bar_ingest_watchlist:
                        try:
                            _raw_bars = await ctx.broker.get_history(
                                _sym,
                                start=_ingest_start.isoformat(),
                                end=_ingest_end.isoformat(),
                            )
                            if not _raw_bars:
                                continue
                            _df = pd.DataFrame(_raw_bars)
                            if "date" not in _df.columns:
                                continue
                            _df = _df.rename(columns={"date": "ts"})
                            for _col in ("open", "high", "low", "close", "volume"):
                                if _col in _df.columns:
                                    _df[_col] = pd.to_numeric(_df[_col], errors="coerce")
                            _df = _df.dropna(subset=["close"])
                            _df = _df.set_index("ts")
                            await ctx.db.timeseries.save_daily_bars(_sym, _df)
                            _ingested += 1
                        except Exception as _bar_exc:
                            logger.warning("Bar ingest failed for %s: %s", _sym, _bar_exc)
                    from datetime import datetime, timezone
                    await ctx.db.settings.set_setting(_BAR_INGEST_KEY, datetime.now(timezone.utc).isoformat())
                    logger.info("Bar ingest complete: %d/%d symbols", _ingested, len(_bar_ingest_watchlist))
                    await ctx.db.logs.write_log(
                        "ENGINE",
                        f"bar ingest: {_ingested}/{len(_bar_ingest_watchlist)} symbols refreshed"
                    )

            # 9. Chart analysis
            _CHART_ANALYSIS_KEY = "chart_analysis_last_run"
            _CHART_ANALYSIS_INTERVAL_DAYS = 7
            db_watchlist = sorted(list(set(current_watchlist)))
            if ctx.overseer is not None and db_watchlist:
                _should_run_charts = False
                _age_days: float = 0.0
                try:
                    _recent_decisions = await ctx.db.decisions.recent_ai_decisions(
                        strategy_id="CHART",
                        limit=max(len(db_watchlist) * 2, 20)
                    )
                    _analyzed_syms = {d["symbol"] for d in _recent_decisions}
                    _missing_analysis = any(s not in _analyzed_syms for s in db_watchlist)

                    if _missing_analysis:
                        _should_run_charts = True
                        logger.info("Forcing chart analysis: some symbols in watchlist are missing analysis.")
                    else:
                        _last_chart_ts_raw = await ctx.db.settings.get_setting(_CHART_ANALYSIS_KEY)
                        if _last_chart_ts_raw:
                            def _parse_iso(s: Optional[str]) -> Optional[datetime]:
                                if not s:
                                    return None
                                try:
                                    normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
                                    datetime_fromisoformat = datetime.fromisoformat
                                    dt = datetime_fromisoformat(normalised)
                                    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                                except ValueError:
                                    return None
                            _last_chart_dt = _parse_iso(_last_chart_ts_raw)
                            if _last_chart_dt is None:
                                _should_run_charts = True
                            else:
                                _age_days = (
                                    datetime.now(timezone.utc) - _last_chart_dt
                                ).total_seconds() / 86400
                                _should_run_charts = _age_days >= _CHART_ANALYSIS_INTERVAL_DAYS
                        else:
                            _should_run_charts = True
                except Exception as exc:
                    logger.warning("Failed to check chart analysis timestamp: %s", exc)
                    _should_run_charts = True

                if _should_run_charts:
                    logger.info("Running chart vision analysis for %d symbols", len(db_watchlist))
                    try:
                        await ctx.overseer.analyze_charts(db_watchlist)
                        await ctx.db.settings.set_setting(_CHART_ANALYSIS_KEY, datetime.now(timezone.utc).isoformat())
                        await ctx.db.logs.write_log(
                            "ENGINE",
                            f"chart vision: analysed {len(db_watchlist)} symbols (7-month daily bars, next run in 7 days)"
                        )
                    except Exception as _ca_exc:
                        logger.warning("analyze_charts failed: %s", _ca_exc)
                else:
                    _days_left = max(0.0, _CHART_ANALYSIS_INTERVAL_DAYS - _age_days)
                    logger.debug("Chart analysis throttled — next run in %.1f day(s)", _days_left)

            # 10. Update live status indicators
            await ctx.db.settings.set_setting("tradier_last_ok_ts", datetime.now(timezone.utc).isoformat())
            await ctx.db.settings.set_setting("tradier_last_error", "")
            await ctx.db.settings.set_setting("market_session", mkt["session"])
            logger.info("tick complete: %s", stats)
            await ctx.db.logs.write_log("ENGINE", f"heartbeat tick complete: {stats}")
            engine._cb_fail_count = 0

        except Exception as exc:
            engine._cb_fail_count += 1
            if engine._cb_fail_count >= _CB_THRESHOLD:
                engine._cb_tripped_at = time.time()
            logger.exception("tick failed: %s", exc)
            try:
                exc_str = str(exc)[:500]
                await ctx.db.settings.set_setting("tradier_last_error", exc_str)
                await ctx.db.logs.write_log("ENGINE", f"tick failed: {exc}", level="ERROR")
            except Exception:
                logger.debug("tick-error DB write also failed", exc_info=True)
