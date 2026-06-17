"""
[Service-1: Hermes-Agent-Core]
CascadingEngine — the top-level orchestrator that drives execution priority
across the cascading strategies (sync → reconcile → manage → entries → AI).

The order/sizing primitives it composes now live in focused sibling modules
and are re-exported below, so existing ``from hermes.service1_agent.core
import ...`` call-sites keep working unchanged:

  * :class:`TradeAction`         → :mod:`.trade_action`
  * :class:`AsyncBrokerWrapper`  → :mod:`.broker_wrapper`
  * :class:`MoneyManager`, :class:`IronCondorBuilder` → :mod:`.money_manager`
  * :class:`AbstractStrategy`    → :mod:`.strategy_base`
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence

from hermes.events.bus import EventBus, ReviewRequestEvent, AIApprovalEvent, MarketDataEvent, OrderFillEvent

# Re-exported for backwards compatibility. These classes were split out of
# this module into focused siblings; importing them here keeps the public
# ``hermes.service1_agent.core`` surface stable for existing call-sites.
from .broker_wrapper import AsyncBrokerWrapper
from .money_manager import IronCondorBuilder, MoneyManager
from .strategy_base import AbstractStrategy
from .trade_action import TradeAction

if TYPE_CHECKING:
    # Imported only for type checking — resolves the forward references to
    # ``HermesOverseer`` without a runtime circular import (overseer.py
    # imports TradeAction from this package).
    from .overseer import HermesOverseer

logger = logging.getLogger("hermes.agent.core")

__all__ = [
    "AsyncBrokerWrapper",
    "TradeAction",
    "MoneyManager",
    "IronCondorBuilder",
    "AbstractStrategy",
    "CascadingEngine",
]


class CascadingEngine:
    """
    Pipeline order (per spec):
        1. Sync positions (broker → DB)
        2. Reconcile orphans
        3. Process exits / management for every strategy
        4. Execute entries in priority order: CS75 → CS7 → TastyTrade45 → Wheel
           — fully draining the watchlist for one strategy before moving on.
    """

    def __init__(self, broker, db, strategies: Sequence[AbstractStrategy],
                 overseer: Optional["HermesOverseer"] = None,
                 approval_mode: bool = False,
                 money_manager: Optional["MoneyManager"] = None,
                 config: Optional[Dict[str, Any]] = None,
                 event_bus: Optional[EventBus] = None,
                 llm_out_of_loop: bool = False):
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        # Sort by declared PRIORITY (1 highest)
        self.strategies = sorted(strategies, key=lambda s: s.PRIORITY)
        self.overseer = overseer
        # When True, submit() queues trades for human approval instead of
        # sending them to the broker directly.
        self.approval_mode = approval_mode
        # MoneyManager is shared across strategies; the engine also holds a
        # reference so tick() can refresh broker-side order counts before
        # capacity decisions run. Falls back to the first strategy's mm so
        # callers that haven't been updated yet still work.
        self.mm = money_manager or (strategies[0].mm if strategies else None)
        self.config = config or {}
        self.event_bus = event_bus
        self.llm_out_of_loop = llm_out_of_loop
        self._quote_cache: Dict[str, Dict[str, Any]] = {}
        if self.event_bus is not None:
            self.event_bus.subscribe(AIApprovalEvent, self.handle_ai_approval)
            self.event_bus.subscribe(MarketDataEvent, self.handle_market_data)
            self.event_bus.subscribe(OrderFillEvent, self.handle_order_fill)

    # 1
    async def sync_positions(self) -> None:
        positions = await self.broker.get_positions() or []
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
            orders = await self.broker.get_orders() or []
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
        except Exception:                              # noqa: BLE001
            logger.exception("[ENGINE] active-order leg fetch failed")
        await self.db.upsert_positions(positions, active_order_legs=active_legs)

    # 2
    async def reconcile_orphans(self) -> None:
        """Flag broker positions not tied to any strategy as MANUAL_ORPHAN."""
        tracked = await self.db.tracked_option_symbols()
        live = {p["symbol"] for p in await self.broker.get_positions() or []}
        orphans = live - tracked
        if orphans:
            await self.db.flag_orphans(orphans)

    # 3
    async def process_management(self) -> List[TradeAction]:
        actions: List[TradeAction] = []
        for s in self.strategies:
            try:
                actions.extend(await s.manage_positions())
            except Exception as exc:                     # noqa: BLE001
                logger.exception("Management failure in %s: %s", s.NAME, exc)
        return actions

    # 4
    async def _watchlist_for(self, strategy_id: str, default: Sequence[str]) -> List[str]:
        """Per-strategy watchlist with fallback to the engine-level default."""
        getter = getattr(self.db, "list_watchlist", None)
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
        except Exception as exc:                          # noqa: BLE001
            logger.exception("watchlist read failed for %s: %s", strategy_id, exc)
            return list(default)
        return (wl or []) or list(default)

    async def process_entries(self, watchlist: Sequence[str]) -> int:
        """Execute entries in priority order. Submits actions after each strategy
        to ensure MoneyManager capacity is updated for the next priority level.
        Returns total number of entry actions planned.
        """
        # Dedup watchlist to prevent multiple scans of the same symbol in one tick
        unique_watchlist = list(dict.fromkeys(watchlist))
        total_entries = 0
        max_per_tick = int(self.config.get("max_orders_per_tick", 5))
        tick_submitted = 0

        if self.config.get("portfolio_optimization"):
            # Gather proposed actions across all strategies first
            all_proposed_actions = []
            for s in self.strategies:
                try:
                    wl = await self._watchlist_for(s.strategy_id, unique_watchlist)
                    actions = await s.execute_entries(wl)
                    all_proposed_actions.extend(actions)
                except Exception as exc:
                    logger.exception("Entry proposal failure in %s: %s", s.NAME, exc)

            if not all_proposed_actions:
                return 0

            avail_bp = await self.mm.true_available_bp()
            optimized_actions = await self.mm.optimize_allocation(all_proposed_actions, avail_bp)

            if len(optimized_actions) > max_per_tick:
                logger.warning(
                    "[ENGINE] Optimized entries generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                    len(optimized_actions), max_per_tick, max_per_tick,
                )
                for a in optimized_actions[max_per_tick:]:
                    await self.db.write_log(
                        a.strategy_id,
                        f"[GUARD] {a.symbol} entry trimmed due to max_orders_per_tick={max_per_tick}"
                    )
                optimized_actions = optimized_actions[:max_per_tick]

            await self.submit(optimized_actions, action_type="entry")
            if optimized_actions:
                await self.mm.sync_broker_orders()
            return len(optimized_actions)

        for s in self.strategies:
            try:
                if tick_submitted >= max_per_tick:
                    logger.warning(
                        "[ENGINE] max_orders_per_tick=%d reached; skipping %s entries",
                        max_per_tick, s.NAME,
                    )
                    await self.db.write_log(
                        s.strategy_id,
                        f"[GUARD] max_orders_per_tick={max_per_tick} reached; "
                        f"{s.NAME} entries skipped this tick",
                    )
                    break

                wl = await self._watchlist_for(s.strategy_id, unique_watchlist)
                # Drain entire watchlist for THIS strategy.
                actions = await s.execute_entries(wl)

                # Cap to remaining budget for this tick.
                remaining = max_per_tick - tick_submitted
                if len(actions) > remaining:
                    logger.warning(
                        "[ENGINE] %s generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                        s.NAME, len(actions), remaining, max_per_tick,
                    )
                    await self.db.write_log(
                        s.strategy_id,
                        f"[GUARD] {s.NAME} generated {len(actions)} actions; "
                        f"trimmed to {remaining} (max_orders_per_tick={max_per_tick})",
                    )
                    actions = actions[:remaining]

                # Submit immediately so subsequent strategies see these as PENDING.
                await self.submit(actions, action_type="entry")
                tick_submitted += len(actions)
                total_entries += len(actions)

                # Re-sync broker orders so the next strategy's capacity check
                # reflects any orders just placed (fills between ticks are now visible).
                if actions:
                    await self.mm.sync_broker_orders()

            except Exception as exc:                     # noqa: BLE001
                logger.exception("Entry failure in %s: %s", s.NAME, exc)
        return total_entries

    async def _attach_entry_features(self, a: TradeAction) -> None:
        """Snapshot the resolved knobs + entry context onto an entry action.

        Best-effort and fail-open: any error here must never block a trade, so
        we swallow exceptions and simply leave ``entry_features`` unset. The
        snapshot rides in ``strategy_params`` so it survives both the direct
        broker path and the approval-queue round-trip (``dataclasses.asdict``),
        and is persisted by ``record_order_response``.
        """
        try:
            from .tunables import resolve as _resolve_tunables
            from .strategies._helpers import entry_feature_snapshot

            sp = dict(a.strategy_params or {})
            if "entry_features" in sp:        # already stamped (e.g. by a strategy)
                return
            try:
                knobs = (await _resolve_tunables(
                    self.db, self.config, group=a.strategy_id)).as_dict()
            except Exception:                                  # noqa: BLE001
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
            )
            a.strategy_params = sp
        except Exception:                                      # noqa: BLE001
            logger.debug("entry-feature snapshot failed for %s", a.symbol,
                         exc_info=True)

    async def submit(self, actions: Iterable[TradeAction],
               action_type: str = "entry") -> None:
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
                await self.db.write_log(
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
                await self._attach_entry_features(a)

            # Veto-suppression: if the overseer already vetoed this exact
            # entry within the TTL window, skip re-proposing it instead of
            # brute-forcing the same action through review every tick. Only
            # entries are suppressed — management closes/rolls must always be
            # allowed through, and AI-authored actions bypass review anyway.
            if self.overseer is not None and action_type == "entry":
                veto_side = (a.strategy_params or {}).get("side_type")
                if veto_side and str(veto_side).lower() in {"buy", "sell"}:
                    veto_side = None
                try:
                    veto_reason = await self.db.active_veto(
                        a.strategy_id, a.symbol, veto_side, a.expiry)
                except Exception:                                  # noqa: BLE001
                    logger.exception("[VETO] active_veto lookup failed for %s", a.symbol)
                    veto_reason = None
                if veto_reason:
                    logger.info("[VETO-SUPPRESSED] %s %s side=%s expiry=%s",
                                a.strategy_id, a.symbol, veto_side, a.expiry)
                    await self.db.write_log(
                        a.strategy_id,
                        f"[VETO-SUPPRESSED] {a.symbol} {veto_side or ''} "
                        f"expiry={a.expiry} — skipped re-proposal "
                        f"(active AI veto: {veto_reason})",
                    )
                    continue

            if self.llm_out_of_loop:
                await self._execute_or_queue(a, action_type)
                continue

            if self.event_bus is not None:
                if (self.overseer is not None and action_type != "ai"
                        and not getattr(a, "ai_authored", False)):
                    # Yield to AI Overseer asynchronously
                    event = ReviewRequestEvent(
                        strategy_id=a.strategy_id,
                        symbol=a.symbol,
                        trade_action=a,
                        action_type=action_type,
                    )
                    self.event_bus.emit(event)
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
                    self.event_bus.emit(event)
                continue

            # AI override hook — overseer may VETO, MODIFY, or APPROVE the action.
            # review() is async; without awaiting it `a` becomes a coroutine and
            # VETO/MODIFY verdicts are silently dropped on this non-event-bus path.
            # AI-authored actions (e.g. overseer-proposed closes) skip review —
            # the overseer must not re-review its own decision.
            if self.overseer is not None and not getattr(a, "ai_authored", False):
                a = await self.overseer.review(a)
                if a is None:
                    continue

            await self._execute_or_queue(a, action_type)

    async def _execute_or_queue(self, a: TradeAction, action_type: str) -> None:
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
        side_type = (a.strategy_params or {}).get("side_type")

        if self.approval_mode:
            # Dedup guard: never re-queue a trade that already has a PENDING
            # approval for the same (strategy, symbol, side, expiry). Without
            # this, every tick re-generates and re-queues the same spread
            # because the approval hasn't been actioned yet.
            if await self.db.has_pending_approval(a.strategy_id, a.symbol,
                                                  side_type, a.expiry):
                logger.info(
                    "[C2] Skipping duplicate — already PENDING: %s %s "
                    "side=%s expiry=%s",
                    a.strategy_id, a.symbol, side_type, a.expiry,
                )
                await self.db.write_log(
                    a.strategy_id,
                    f"[DEDUP] {a.symbol} {side_type} expiry={a.expiry} "
                    f"already PENDING approval — skipped",
                )
                return

            # Queue for human review instead of firing directly.
            action_dict = dataclasses.asdict(a)
            await self.db.queue_for_approval(action_dict, action_type=action_type)
            logger.info(
                "[C2] Trade queued for approval: %s %s strategy=%s side=%s expiry=%s",
                a.symbol, a.order_class, a.strategy_id, side_type, a.expiry,
            )
            await self.db.write_log(
                a.strategy_id,
                f"[APPROVAL REQUIRED] {a.symbol} {a.order_class} "
                f"side={side_type} expiry={a.expiry} "
                f"qty={a.quantity} — awaiting human approval",
            )
            return

        await self.db.record_pending_order(a)
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
        close_method = getattr(self.db, "close_trade_from_action", None)
        if getattr(self.broker, "dry_run", False):
            return
        try:
            resp = await self.broker.place_order_from_action(a)
        except Exception as exc:                           # noqa: BLE001
            # Broker raised before we got an order id. Free the PENDING row so
            # capacity recovers; a Trade row was never written, nothing to roll
            # back.
            if is_pure_close and close_method is not None:
                await close_method(a, {"errors": str(exc)})
            else:
                await self.db.record_order_response(a, {"errors": str(exc)})
            logger.exception("place_order failed for %s: %s", a.symbol, exc)
        else:
            if is_pure_close and close_method is not None:
                await close_method(a, resp)
            else:
                await self.db.record_order_response(a, resp)

    # ----- top level entry point used by main.py and the scheduler ----------
    async def _read_banned_symbols(self) -> set[str]:
        if not self.db:
            return set()
        try:
            raw = await self.db.get_setting("banned_symbols")
            if not raw:
                return set()
            return {s.strip().upper() for s in raw.split(",") if s.strip()}
        except Exception:
            logger.exception("[GOVERNANCE] Failed to read banned_symbols setting")
            return set()

    async def tick(self, watchlist: Sequence[str]) -> Dict[str, int]:
        await self.sync_positions()
        # Refresh real-time broker order counts to prevent duplicate entries.
        # mm may be None on legacy callers that haven't been updated yet;
        # skip rather than crash the entire tick.
        if self.mm is not None:
            await self.mm.sync_broker_orders()
        await self.reconcile_orphans()
        mgmt = await self.process_management()
        await self.submit(mgmt, action_type="management")

        # Filter out banned symbols under out-of-loop governance
        banned = await self._read_banned_symbols()
        if banned:
            original_len = len(watchlist)
            watchlist = [s for s in watchlist if s.upper() not in banned]
            if len(watchlist) < original_len:
                logger.info("[GOVERNANCE] Watchlist filtered by active AI risk restrictions. Banned symbols skipped: %s", banned)

        # Entries are now submitted internally strategy-by-strategy.
        num_entries = await self.process_entries(watchlist)
        # Authorize the overseer to inject "AI-only" trades after the rules-driven pass.
        ai_count = 0
        if self.overseer is not None:
            # Goal-aware parameter tuning & risk restrictions
            if self.llm_out_of_loop:
                # Run out-of-loop background policy adjustments asynchronously
                asyncio.create_task(self._maybe_tune_parameters())
            else:
                # Run inline blocking
                await self._maybe_tune_parameters()

            if self.event_bus is not None:
                # Asynchronously generate AI proposals without blocking the tick loop
                asyncio.create_task(self._async_propose(watchlist))
                # Symmetric exit path: let the overseer unwind open positions
                # too. Independent of the watchlist — closes act on the book.
                asyncio.create_task(self._async_propose_closes())
            else:
                ai_actions = await self.overseer.propose(watchlist)
                ai_actions = await self._gate_ai_actions(ai_actions)
                await self.submit(ai_actions, action_type="ai")
                ai_count = len(ai_actions)
                ai_closes = await self.overseer.propose_closes()
                ai_closes = await self._price_ai_closes(ai_closes)
                await self.submit(ai_closes, action_type="management")
        return {"managed": len(mgmt), "entries": num_entries, "ai": ai_count}

    async def _maybe_tune_parameters(self) -> None:
        """Run the overseer's goal-aware parameter tuning, throttled by interval.

        Defaults to once per hour (``param_tuning_interval_s``); set the
        interval to 0 to disable. Best-effort — a tuning failure must never
        break the trading tick.
        """
        interval = int(self.config.get("param_tuning_interval_s", 3600))
        if interval <= 0:
            return
        tuner = getattr(self.overseer, "propose_parameter_adjustments", None)
        if tuner is None:
            return
        try:
            import time
            now = time.time()
            last_raw = await self.db.get_setting("ai_last_param_tuning_ts")
            last = float(last_raw) if last_raw else 0.0
            if now - last < interval:
                return
            await self.db.set_setting("ai_last_param_tuning_ts", str(now))
            await tuner()

            # Execute risk restrictions check (banned symbols list)
            risk_tuner = getattr(self.overseer, "propose_risk_restrictions", None)
            if risk_tuner is not None:
                await risk_tuner()
        except Exception as exc:                                   # noqa: BLE001
            logger.exception("[PARAM-TUNE] tuning tick failed: %s", exc)

    async def handle_ai_approval(self, event: AIApprovalEvent) -> None:
        """Asynchronously executes or queues an action after AI approval."""
        a = event.original_action
        if a is None:
            logger.warning("AIApprovalEvent has no original_action; skipping.")
            return

        if event.verdict == "VETO":
            logger.info("[AI VETOED] Strategy=%s symbol=%s - %s", event.strategy_id, event.symbol, event.rationale)
            await self.db.write_log(
                event.strategy_id,
                f"[AI VETOED] {event.symbol} — {event.rationale}"
            )
            # Record a short-lived suppression so the rules engine stops
            # re-proposing this identical entry next tick (a veto consumes
            # no capacity, so without this it would brute-force the same
            # action and re-veto it every cycle). Best-effort: a failure
            # here must never block the tick.
            ttl = int(self.config.get("veto_suppression_s", 1800))
            if ttl > 0:
                veto_side = (a.strategy_params or {}).get("side_type")
                if veto_side and str(veto_side).lower() in {"buy", "sell"}:
                    veto_side = None
                try:
                    hits = await self.db.record_veto(
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
        await self._execute_or_queue(a, getattr(event, "action_type", "entry"))

    async def _async_propose(self, watchlist: Sequence[str]) -> None:
        """Asynchronously triggers the overseer to propose actions without blocking the tick loop."""
        try:
            ai_actions = await self.overseer.propose(watchlist)
            ai_actions = await self._gate_ai_actions(ai_actions)
            if ai_actions:
                await self.submit(ai_actions, action_type="ai")
        except Exception as exc:
            logger.exception("Error in async propose: %s", exc)

    async def _async_propose_closes(self) -> None:
        """Asynchronously let the overseer close positions without blocking the tick."""
        try:
            closes = await self.overseer.propose_closes()
            closes = await self._price_ai_closes(closes)
            if closes:
                await self.submit(closes, action_type="management")
        except Exception as exc:                                   # noqa: BLE001
            logger.exception("Error in async propose_closes: %s", exc)

    async def _broker_position_state(self) -> tuple[Dict[str, float], set]:
        """Live broker holdings + legs already worked by a resting order.

        Returns ``(qty_by_option_symbol, active_order_legs)`` where the qty is
        net and signed (shorts negative). Used to gate AI closes against the
        actual book: a DB trade is marked OPEN the instant Tradier *accepts*
        the entry — before it fills — so the short may not exist yet, and a
        close already resting at the broker must not be re-submitted. Both
        cases otherwise draw Tradier's "Buy To Cover ... unless closing a
        short position, please check open orders" rejection and leave orphans.
        """
        qty: Dict[str, float] = {}
        try:
            for p in await self.broker.get_positions() or []:
                sym = p.get("symbol")
                if not sym:
                    continue
                try:
                    qty[sym] = qty.get(sym, 0.0) + float(p.get("quantity") or 0)
                except (TypeError, ValueError):
                    continue
        except Exception:                                          # noqa: BLE001
            logger.exception("[AI-CLOSE] get_positions failed; treating book as empty")
        active_legs: set = set()
        try:
            active_statuses = {"open", "partially_filled", "pending", "accepted", "calculated"}
            for o in await self.broker.get_orders() or []:
                if str(o.get("status", "")).lower() not in active_statuses:
                    continue
                legs = o.get("leg") or []
                if isinstance(legs, dict):
                    legs = [legs]
                for leg in legs:
                    s = leg.get("option_symbol")
                    if s:
                        active_legs.add(s)
                top = o.get("option_symbol")
                if top:
                    active_legs.add(top)
        except Exception:                                          # noqa: BLE001
            logger.exception("[AI-CLOSE] get_orders failed; assuming no resting orders")
        return qty, active_legs

    async def _price_ai_closes(
        self, actions: Sequence[TradeAction]
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
        qty_map, active_legs = await self._broker_position_state()
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
                    await self.db.write_log(
                        a.strategy_id,
                        f"[AI-CLOSE] {a.symbol} trade_id={trade_id}: broker holds "
                        f"{held:g} of {short_sym} (need short ≥ {lots}); skip — "
                        f"position not (yet) held",
                    )
                    continue
                long_sym = long_leg.get("option_symbol") if long_leg else None
                if short_sym in active_legs or (long_sym and long_sym in active_legs):
                    await self.db.write_log(
                        a.strategy_id,
                        f"[AI-CLOSE] {a.symbol} trade_id={trade_id}: a resting order "
                        f"already works this position; skip — avoids duplicate cover",
                    )
                    continue
                # ----------------------------------------------------------

                quotes = await self.broker.get_quote(",".join(syms)) or []
                qmap = {q.get("symbol"): q for q in quotes}
                sq = qmap.get(short_leg.get("option_symbol"))
                if long_leg is not None:
                    lq = qmap.get(long_leg.get("option_symbol"))
                    debit, blocked, reason = AbstractStrategy.compute_close_debit(sq, lq, a.width)
                    if blocked:
                        await self.db.write_log(
                            a.strategy_id,
                            f"[AI-CLOSE] {a.symbol} trade_id="
                            f"{(a.strategy_params or {}).get('trade_id')}: "
                            f"close-debit blocked ({reason}); skip this tick",
                        )
                        continue
                else:
                    ask = float((sq or {}).get("ask") or 0)
                    if ask <= 0:
                        await self.db.write_log(
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
                await self.db.write_log(
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
        if self.mm is None:
            for a in actions:
                await self.db.write_log(
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
                    a, broker=self.broker, db=self.db, mm=self.mm)
            except Exception as exc:                              # noqa: BLE001
                logger.exception("[AI-GATE] error validating %s: %s", a.symbol, exc)
                await self.db.write_log(
                    a.strategy_id,
                    f"[AI-GATE] {a.symbol}: rejected — validation error: {exc}",
                )
                continue
            if validated is None:
                logger.info("[AI-GATE] REJECTED %s", reason)
                await self.db.write_log(a.strategy_id, f"[AI-GATE] REJECTED {reason}")
            else:
                logger.info("[AI-GATE] %s", reason)
                await self.db.write_log(a.strategy_id, f"[AI-GATE] {reason}")
                gated.append(validated)
        return gated

    async def handle_market_data(self, event: MarketDataEvent) -> None:
        """Evaluates strategies reactively when a new MarketDataEvent is received."""
        symbol = event.symbol
        
        # Get old price from cache if it exists
        old_price = None
        if symbol in self._quote_cache:
            old_price = self._quote_cache[symbol].get("price")

        # Update quote cache
        self._quote_cache[symbol] = {
            "price": event.price,
            "volume": event.volume,
            **event.data
        }
        
        # Guard: off-hours block
        from hermes.market_hours import should_block_trades
        blocked, reason = should_block_trades()
        if blocked:
            return

        # Run position management for this symbol across all strategies
        mgmt_actions = []
        for s in self.strategies:
            try:
                actions = await s.manage_positions()
                if actions:
                    # Filter actions to only close positions for the ticking symbol
                    symbol_actions = [a for a in actions if a.symbol == symbol]
                    mgmt_actions.extend(symbol_actions)
            except Exception as exc:
                logger.exception("Management failure in %s for %s: %s", s.NAME, symbol, exc)
                
        if mgmt_actions:
            await self.submit(mgmt_actions, action_type="management")

        # Check support/resistance crossing for entries
        if old_price is not None and old_price != event.price:
            try:
                analysis = await self.broker.analyze_symbol(symbol)
                key_levels = analysis.get("key_levels", [])
            except Exception as exc:
                logger.exception("Failed to analyze symbol %s on market data event: %s", symbol, exc)
                key_levels = []

            crossed = False
            new_price = event.price
            for lvl in key_levels:
                price_level = lvl.get("price")
                if price_level is not None:
                    if (old_price < price_level <= new_price) or (old_price > price_level >= new_price):
                        crossed = True
                        logger.info(
                            "[ENGINE] Price crossed support/resistance level %f for %s (old: %f, new: %f)",
                            price_level, symbol, old_price, new_price,
                        )
                        break

            if crossed:
                try:
                    await self.process_reactive_entries(symbol)
                except Exception as exc:
                    logger.exception("Failed to process reactive entries for %s: %s", symbol, exc)

    async def handle_order_fill(self, event: OrderFillEvent) -> None:
        """Reactively handles order fills by syncing positions and orders immediately."""
        logger.info(
            "[ENGINE] Order fill event received for order %s (%s %d shares/contracts of %s)",
            event.broker_order_id, event.side, event.quantity, event.symbol,
        )
        try:
            await self.sync_positions()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync positions on order fill event: %s", exc)

        try:
            if self.mm is not None:
                await self.mm.sync_broker_orders()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync broker orders on order fill event: %s", exc)

        try:
            await self.reconcile_orphans()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to reconcile orphans on order fill event: %s", exc)

    async def process_reactive_entries(self, symbol: str) -> None:
        """Executes entries reactively for a single symbol that crossed support/resistance.
        Ensures the symbol is in each strategy's watchlist before executing.
        """
        strategies_to_run = []
        for s in self.strategies:
            wl = await self._watchlist_for(s.strategy_id, [symbol])
            if symbol in wl:
                strategies_to_run.append(s)

        if not strategies_to_run:
            return

        max_per_tick = int(self.config.get("max_orders_per_tick", 5))

        if self.config.get("portfolio_optimization"):
            # Gather proposed actions across matching strategies
            all_proposed_actions = []
            for s in strategies_to_run:
                try:
                    actions = await s.execute_entries([symbol])
                    all_proposed_actions.extend(actions)
                except Exception as exc:
                    logger.exception("Reactive entry proposal failure in %s for %s: %s", s.NAME, symbol, exc)

            if not all_proposed_actions:
                return

            avail_bp = await self.mm.true_available_bp()
            optimized_actions = await self.mm.optimize_allocation(all_proposed_actions, avail_bp)

            if len(optimized_actions) > max_per_tick:
                logger.warning(
                    "[ENGINE] Reactive optimized entries generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                    len(optimized_actions), max_per_tick, max_per_tick,
                )
                for a in optimized_actions[max_per_tick:]:
                    await self.db.write_log(
                        a.strategy_id,
                        f"[GUARD] {a.symbol} reactive entry trimmed due to max_orders_per_tick={max_per_tick}"
                    )
                optimized_actions = optimized_actions[:max_per_tick]

            await self.submit(optimized_actions, action_type="entry")
            if optimized_actions:
                await self.mm.sync_broker_orders()
        else:
            tick_submitted = 0
            for s in strategies_to_run:
                try:
                    if tick_submitted >= max_per_tick:
                        logger.warning(
                            "[ENGINE] max_orders_per_tick=%d reached during reactive entries; skipping %s",
                            max_per_tick, s.NAME,
                        )
                        break

                    actions = await s.execute_entries([symbol])
                    remaining = max_per_tick - tick_submitted
                    if len(actions) > remaining:
                        actions = actions[:remaining]

                    await self.submit(actions, action_type="entry")
                    tick_submitted += len(actions)

                    if actions:
                        await self.mm.sync_broker_orders()
                except Exception as exc:
                    logger.exception("Reactive entry failure in %s for %s: %s", s.NAME, symbol, exc)
