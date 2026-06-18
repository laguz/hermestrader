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
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence

from hermes.clock import Clock, RealClock
from hermes.events.bus import EventBus, ClockTickEvent
from hermes.broker import BrokerAdapter

# Re-exported for backwards compatibility. These classes were split out of
# this module into focused siblings; importing them here keeps the public
# ``hermes.service1_agent.core`` surface stable for existing call-sites.
from .broker_wrapper import AsyncBrokerWrapper
from .money_manager import IronCondorBuilder, MoneyManager
from .strategy_base import AbstractStrategy
from .trade_action import TradeAction
from ._engine_reactive import ReactiveController
from ._engine_ai import AIController
from ._engine_pipeline import PipelineController
from .context import TickContext

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


_DEFAULT_BUS = object()


class CascadingEngine:
    """
    Pipeline order (per spec):
        1. Sync positions (broker → DB)
        2. Reconcile orphans
        3. Process exits / management for every strategy
        4. Execute entries in priority order: CS75 → CS7 → TastyTrade45 → Wheel
           — fully draining the watchlist for one strategy before moving on.
    """

    def __init__(self, broker: BrokerAdapter, db, strategies: Sequence[AbstractStrategy],
                 overseer: Optional["HermesOverseer"] = None,
                 approval_mode: bool = False,
                 money_manager: Optional["MoneyManager"] = None,
                 config: Optional[Dict[str, Any]] = None,
                 event_bus: Optional[EventBus] = _DEFAULT_BUS,
                 llm_out_of_loop: bool = False,
                 clock: Optional[Clock] = None):
        self.clock = clock or RealClock()
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        self.config = config or {}
        from .risk_engine import PortfolioRiskEngine
        self.risk_engine = PortfolioRiskEngine(broker, db, self.config)
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
        if event_bus is _DEFAULT_BUS:
            self.event_bus = EventBus()
        else:
            self.event_bus = event_bus
        self.llm_out_of_loop = llm_out_of_loop
        self._quote_cache: Dict[str, Dict[str, Any]] = {}
        from hermes.ipc import ipc
        self.ipc_client = ipc
        self.control_state = None
        self._cb_fail_count = 0
        self._cb_tripped_at = 0.0
        # Strong references to fire-and-forget background tasks. asyncio only
        # holds a *weak* reference to a bare ``create_task`` result, so a task
        # whose handle is discarded can be garbage-collected mid-await and
        # silently cancelled. Keep them here (mirrors scheduler._tasks /
        # overseer._worker_task) until they finish.
        self._bg_tasks: set[asyncio.Task] = set()

        # Three owned collaborators, all on the same back-reference pattern:
        # each holds a typed reference to the engine and reads mutable engine
        # state (db / broker / event_bus / config / mm / overseer / strategies /
        # ipc_client / _quote_cache) *through* it, so there is no second copy to
        # keep in sync. Cross-phase calls route through the engine's own methods,
        # which stay the single seam tests monkeypatch.
        #   * pipeline — tick phase bodies + slow heartbeat guard tick
        #   * reactive — event-loop/IPC/order-monitor runtime + reactive handlers
        #   * ai       — overseer proposals/closes/gating + bandit/exit tuning
        self.pipeline = PipelineController(self)
        self.reactive = ReactiveController(self)
        self.ai = AIController(self)

        if self.event_bus is not None:
            from hermes.events.bus import (
                ExecuteTickCommand,
                ExecuteClockTickCommand,
                SubmitTradeActionsCommand,
                SyncPositionsCommand,
                ReconcileOrphansCommand,
                ProcessManagementCommand,
                ProcessEntriesCommand,
            )
            self.event_bus.subscribe(ExecuteTickCommand, self.handle_execute_tick)
            self.event_bus.subscribe(ExecuteClockTickCommand, self.handle_execute_clock_tick)
            self.event_bus.subscribe(SubmitTradeActionsCommand, self.handle_submit_trade_actions)
            self.event_bus.subscribe(SyncPositionsCommand, self.handle_sync_positions)
            self.event_bus.subscribe(ReconcileOrphansCommand, self.handle_reconcile_orphans)
            self.event_bus.subscribe(ProcessManagementCommand, self.handle_process_management)
            self.event_bus.subscribe(ProcessEntriesCommand, self.handle_process_entries)

    # ── delegators to the owned collaborators ────────────────────────────────
    # These forward the engine's public/cross-called surface to the collaborator
    # that owns the body. Kept explicit (rather than __getattr__ on the engine)
    # so the engine's API stays greppable and remains the single seam tests
    # monkeypatch; the collaborators read engine state back through self.engine.
    def _ensure_event_loop(self) -> None:
        if self.event_bus is not None:
            self.event_bus.start()
        return self.reactive._ensure_event_loop()

    def _ensure_order_monitor(self) -> None:
        return self.reactive._ensure_order_monitor()

    async def publish_event(self, event_type, payload):
        return await self.reactive.publish_event(event_type, payload)

    async def handle_market_data(self, event):
        if self.event_bus is not None:
            self._ensure_event_loop()
            if getattr(event, "future", None) is None:
                event.future = asyncio.get_running_loop().create_future()
            self.event_bus.emit(event)
            await event.future
        else:
            await self.reactive._handle_market_data_internal(event)

    async def _handle_market_data_internal(self, event):
        return await self.reactive._handle_market_data_internal(event)

    async def handle_order_fill(self, event):
        if self.event_bus is not None:
            self._ensure_event_loop()
            if getattr(event, "future", None) is None:
                event.future = asyncio.get_running_loop().create_future()
            self.event_bus.emit(event)
            await event.future
        else:
            await self.reactive._handle_order_fill_internal(event)

    async def _handle_order_fill_internal(self, event):
        return await self.reactive._handle_order_fill_internal(event)

    async def process_reactive_entries(self, symbol):
        if self.event_bus is not None:
            self._ensure_event_loop()
            from hermes.events.bus import ProcessReactiveEntriesEvent
            ev = ProcessReactiveEntriesEvent(symbol=symbol)
            self.event_bus.emit(ev)
            await ev.future
        else:
            await self.reactive.process_reactive_entries(symbol)

    async def handle_ai_approval(self, event):
        if self.event_bus is not None:
            self._ensure_event_loop()
            if getattr(event, "future", None) is None:
                event.future = asyncio.get_running_loop().create_future()
            self.event_bus.emit(event)
            await event.future
        else:
            await self.ai._handle_ai_approval_internal(event)

    async def _handle_ai_approval_internal(self, event):
        return await self.ai._handle_ai_approval_internal(event)

    # ── Command handlers for event-driven orchestration ──────────────────────
    async def handle_execute_tick(self, command):
        try:
            res = await self._run_tick_internal(command.watchlist)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_execute_clock_tick(self, command):
        try:
            res = await self._handle_clock_tick_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_submit_trade_actions(self, command):
        try:
            if command.execute_directly or command.approval_id is not None:
                for a in command.actions:
                    await self._execute_or_queue(a, command.action_type, approval_id=command.approval_id)
            else:
                await self.submit(command.actions, action_type=command.action_type)
            if command.future and not command.future.done():
                command.future.set_result(None)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_sync_positions(self, command):
        try:
            res = await self.sync_positions()
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_reconcile_orphans(self, command):
        try:
            await self.reconcile_orphans()
            if command.future and not command.future.done():
                command.future.set_result(None)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_process_management(self, command):
        try:
            res = await self.process_management()
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_process_entries(self, command):
        try:
            res = await self.process_entries(command.watchlist)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    # Runtime loop/order-monitor state lives on the reactive collaborator (it is
    # genuine per-loop state). These proxies keep ``engine.<x>`` working as the
    # call-site/seam for the pipeline and external callers.
    @property
    def queue(self):
        return self.reactive.queue
    @queue.setter
    def queue(self, val):
        self.reactive.queue = val

    @property
    def loop_task(self):
        return self.reactive.loop_task
    @loop_task.setter
    def loop_task(self, val):
        self.reactive.loop_task = val

    @property
    def _pending_futures(self):
        return self.reactive._pending_futures
    @_pending_futures.setter
    def _pending_futures(self, val):
        self.reactive._pending_futures = val

    @property
    def _tracked_orders(self):
        return self.reactive._tracked_orders
    @_tracked_orders.setter
    def _tracked_orders(self, val):
        self.reactive._tracked_orders = val

    @property
    def _order_monitor_task(self):
        return self.reactive._order_monitor_task
    @_order_monitor_task.setter
    def _order_monitor_task(self, val):
        self.reactive._order_monitor_task = val

    @property
    def strategies(self):
        return self._strategies

    @strategies.setter
    def strategies(self, val):
        # Collaborators read ``engine.strategies`` directly; this setter only
        # preserves the PRIORITY sort invariant.
        self._strategies = sorted(val, key=lambda s: s.PRIORITY)

    async def _build_fallback_ctx(self, watchlist=None) -> TickContext:
        return await self.pipeline._build_fallback_ctx(watchlist)

    async def _async_propose(self, watchlist_or_ctx):
        if not isinstance(watchlist_or_ctx, TickContext):
            ctx = await self._build_fallback_ctx(watchlist_or_ctx)
        else:
            ctx = watchlist_or_ctx
        return await self.ai._async_propose(ctx)

    async def _async_propose_closes(self, ctx=None):
        if ctx is None:
            ctx = await self._build_fallback_ctx()
        return await self.ai._async_propose_closes(ctx)

    async def _price_ai_closes(self, ctx_or_actions, actions=None):
        if actions is None:
            actions = ctx_or_actions
            ctx = await self._build_fallback_ctx()
        else:
            ctx = ctx_or_actions
        return await self.ai._price_ai_closes(ctx, actions)

    async def _gate_ai_actions(self, actions):
        return await self.ai._gate_ai_actions(actions)

    def _spawn_bg(self, coro) -> asyncio.Task:
        """Schedule a fire-and-forget coroutine while holding a strong reference.

        Without the retained reference + done-callback, asyncio can collect the
        task before it completes (see ``self._bg_tasks``).
        """
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    # ── tick-phase delegators → PipelineController (engine.pipeline) ──────────
    # The bodies live in _engine_pipeline.py; these stay so the engine surface
    # is greppable and remains the single seam tests monkeypatch.
    async def sync_positions(self) -> tuple[List[Dict[str, Any]], set[str]]:
        return await self.pipeline.sync_positions()

    async def reconcile_orphans(self) -> None:
        return await self.pipeline.reconcile_orphans()

    async def process_management(self) -> List[TradeAction]:
        return await self.pipeline.process_management()

    async def _watchlist_for(self, strategy_id: str, default: Sequence[str]) -> List[str]:
        return await self.pipeline._watchlist_for(strategy_id, default)

    async def process_entries(self, watchlist: Sequence[str]) -> int:
        return await self.pipeline.process_entries(watchlist)

    async def _attach_entry_features(self, a: TradeAction) -> None:
        return await self.pipeline._attach_entry_features(a)

    async def submit(self, actions: Iterable[TradeAction],
               action_type: str = "entry") -> None:
        return await self.pipeline.submit(actions, action_type=action_type)

    async def _execute_or_queue(self, a: TradeAction, action_type: str, approval_id: Optional[int] = None) -> None:
        return await self.pipeline._execute_or_queue(a, action_type, approval_id=approval_id)

    # ----- top level entry point used by main.py and the scheduler ----------
    async def _read_banned_symbols(self) -> set[str]:
        return await self.pipeline._read_banned_symbols()

    async def tick(self, watchlist: Sequence[str]) -> Dict[str, int]:
        if self.event_bus is not None:
            self._ensure_event_loop()
            from hermes.events.bus import TickStartedEvent
            event = TickStartedEvent(watchlist=list(watchlist))
            self.event_bus.emit(event)
            return await event.future
        else:
            return await self._run_tick_internal(watchlist)

    async def _run_tick_internal(self, watchlist: Sequence[str]) -> Dict[str, int]:
        res = await self.sync_positions()
        if isinstance(res, tuple) and len(res) == 2:
            positions, active_legs = res
        else:
            positions = []
            active_legs = set()
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
        await self.ai._maybe_capture_and_advise_exits(mgmt)

        # Filter out banned symbols under out-of-loop governance
        banned = await self._read_banned_symbols()
        
        ctx = TickContext(
            timestamp=self.clock.utc_now(),
            watchlist=list(watchlist),
            banned_symbols=banned,
            positions=positions,
            active_order_legs=active_legs
        )

        if banned:
            original_len = len(watchlist)
            watchlist = [s for s in watchlist if s.upper() not in banned]
            if len(watchlist) < original_len:
                logger.info("[GOVERNANCE] Watchlist filtered by active AI risk restrictions. Banned symbols skipped: %s", banned)

        # Entries are now submitted internally strategy-by-strategy.
        num_entries = await self.process_entries(watchlist)
        # Outcome-driven knob tuning (Phase 2 bandit). Independent of the LLM
        # overseer — data-driven and gated by its own mode flag (off default).
        await self.ai._maybe_run_bandit_tuner()
        # Authorize the overseer to inject "AI-only" trades after the rules-driven pass.
        ai_count = 0
        if self.overseer is not None:
            # Goal-aware parameter tuning & risk restrictions
            if self.llm_out_of_loop:
                # Run out-of-loop background policy adjustments asynchronously
                self._spawn_bg(self.ai._maybe_tune_parameters())
            else:
                # Run inline blocking
                await self.ai._maybe_tune_parameters()

            if self.event_bus is not None:
                # Asynchronously generate AI proposals without blocking the tick loop
                self._spawn_bg(self._async_propose(ctx))
                # Symmetric exit path: let the overseer unwind open positions
                # too. Independent of the watchlist — closes act on the book.
                self._spawn_bg(self._async_propose_closes(ctx))
            else:
                ai_actions = await self.overseer.propose(watchlist)
                ai_actions = await self._gate_ai_actions(ai_actions)
                await self.submit(ai_actions, action_type="ai")
                ai_count = len(ai_actions)
                ai_closes = await self.overseer.propose_closes()
                ai_closes = await self._price_ai_closes(ctx, ai_closes)
                await self.submit(ai_closes, action_type="management")
        return {"managed": len(mgmt), "entries": num_entries, "ai": ai_count}

    async def handle_clock_tick(self, event: ClockTickEvent) -> None:
        if self.event_bus is not None:
            self._ensure_event_loop()
            if getattr(event, "future", None) is None:
                event.future = asyncio.get_running_loop().create_future()
            self.event_bus.emit(event)
            await event.future
        else:
            await self._handle_clock_tick_internal(event)

    async def _handle_clock_tick_internal(self, event: ClockTickEvent) -> None:
        return await self.pipeline.handle_clock_tick_internal(event)
