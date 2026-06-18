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
from ._engine_runtime import EngineRuntimeMixin
from ._engine_reactive import EngineReactiveMixin
from ._engine_ai import EngineAIMixin
from ._engine_tuning import EngineTuningMixin

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


class CascadingEngine(
    EngineRuntimeMixin,
    EngineReactiveMixin,
    EngineAIMixin,
    EngineTuningMixin,
):
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
        self.queue = None
        self.loop_task = None
        from hermes.ipc import ipc
        self.ipc_client = ipc
        self._pending_futures: Dict[str, asyncio.Future] = {}
        self._tracked_orders: Dict[str, Dict[str, Any]] = {}
        self._order_monitor_task = None
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
            if resp and isinstance(resp, dict):
                oid = str(resp.get("order_id") or resp.get("id") or "")
                if oid:
                    self._tracked_orders[oid] = {
                        "symbol": a.symbol,
                        "side": a.side,
                        "quantity": a.quantity
                    }
                    logger.info("[ENGINE] Registered order %s in reactive monitor", oid)
                    self._ensure_order_monitor()

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
        return await self.publish_event("TICK", {"watchlist": watchlist})

    async def _run_tick_internal(self, watchlist: Sequence[str]) -> Dict[str, int]:
        await self.sync_positions()
        # Refresh real-time broker order counts to prevent duplicate entries.
        # mm may be None on legacy callers that haven't been updated yet;
        # skip rather than crash the entire tick.
        if self.mm is not None:
            await self.mm.sync_broker_orders()
        await self.reconcile_orphans()
        mgmt = await self.process_management()
        await self.submit(mgmt, action_type="management")
        # Exit-timing trajectory capture + advisory (Phase 3). Off by default;
        # only runs when exit_policy_mode is shadow/active. Best-effort.
        await self._maybe_capture_and_advise_exits(mgmt)

        # Filter out banned symbols under out-of-loop governance
        banned = await self._read_banned_symbols()
        if banned:
            original_len = len(watchlist)
            watchlist = [s for s in watchlist if s.upper() not in banned]
            if len(watchlist) < original_len:
                logger.info("[GOVERNANCE] Watchlist filtered by active AI risk restrictions. Banned symbols skipped: %s", banned)

        # Entries are now submitted internally strategy-by-strategy.
        num_entries = await self.process_entries(watchlist)
        # Outcome-driven knob tuning (Phase 2 bandit). Independent of the LLM
        # overseer — data-driven and gated by its own mode flag (off default).
        await self._maybe_run_bandit_tuner()
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

