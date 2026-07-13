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
from .engine_context import EngineContext

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
        4. Execute entries in priority order:
           CS75 → CS7 → TastyTrade45 → Wheel → HermesAlpha
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
        from .risk_engine import PortfolioRiskEngine
        from hermes.ipc import ipc
        clock = clock or RealClock()
        config = config or {}
        # MoneyManager is shared across strategies; the engine also holds a
        # reference so tick() can refresh broker-side order counts before
        # capacity decisions run. Falls back to the first strategy's mm so
        # callers that haven't been updated yet still work.
        mm = money_manager or (strategies[0].mm if strategies else None)
        event_bus = EventBus() if event_bus is _DEFAULT_BUS else event_bus

        # Single source of truth for the shared dependency surface (db / broker /
        # mm / event_bus / config / clock / ipc_client / overseer / strategies /
        # risk_engine / control_state / approval_mode / llm_out_of_loop /
        # quote_cache). The engine and all three collaborators read these through
        # ``self.ctx``; the ``engine.<dep>`` proxy properties below keep external
        # call-sites (main.py / tests reassigning overseer/mm/control_state/…)
        # working and single-source.
        self.ctx = EngineContext(
            db=db,
            broker=AsyncBrokerWrapper(broker, db),
            config=config,
            clock=clock,
            event_bus=event_bus,
            ipc_client=ipc,
            overseer=overseer,
            mm=mm,
            risk_engine=PortfolioRiskEngine(broker, db, config, money_manager=mm),
            strategies=strategies,
            # When True, submit() queues trades for human approval instead of
            # sending them to the broker directly.
            approval_mode=approval_mode,
            llm_out_of_loop=llm_out_of_loop,
        )
        # Engine-only per-run runtime state (not shared via ctx): circuit-breaker
        # counters.
        self._cb_fail_count = 0
        self._cb_tripped_at = 0.0
        # Serializes the two operator-command drain triggers (IPC nudge vs
        # tick-start) so a command can't be applied twice concurrently. The bus
        # dispatches handlers as independent tasks, so they really can overlap.
        self._cmd_drain_lock = asyncio.Lock()

        # Three owned collaborators. Each reads the shared dependency surface
        # through ``engine.ctx`` and keeps the engine reference only for the few
        # orchestration callbacks (submit / sync_positions / sibling phases) and
        # the circuit-breaker counters above.
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
                DrainOperatorCommandsCommand,
            )
            self.event_bus.subscribe(ExecuteTickCommand, self.handle_execute_tick)
            self.event_bus.subscribe(ExecuteClockTickCommand, self.handle_execute_clock_tick)
            self.event_bus.subscribe(SubmitTradeActionsCommand, self.handle_submit_trade_actions)
            self.event_bus.subscribe(SyncPositionsCommand, self.handle_sync_positions)
            self.event_bus.subscribe(ReconcileOrphansCommand, self.handle_reconcile_orphans)
            self.event_bus.subscribe(ProcessManagementCommand, self.handle_process_management)
            self.event_bus.subscribe(ProcessEntriesCommand, self.handle_process_entries)
            self.event_bus.subscribe(DrainOperatorCommandsCommand, self.handle_drain_operator_commands)

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

    async def handle_drain_operator_commands(self, command):
        await self.drain_operator_commands()

    async def drain_operator_commands(self) -> int:
        """Apply PENDING operator commands, in submission order, in-process.

        This is the agent-side half of the single-writer command channel: the
        watcher only *enqueues* intents; here the agent applies each through the
        normal write path (``set_setting`` / ``decide_approval`` → ``record_event``
        → ledger + projection), so the agent stays the sole writer of canonical
        state. Apply is idempotent, so a row left PENDING by a crash between the
        write and ``mark_applied`` is safely re-applied on the next drain.
        """
        async with self._cmd_drain_lock:
            applied = 0
            for cmd in await self.db.commands.fetch_pending():
                cid = cmd["id"]
                try:
                    ctype = cmd["command_type"]
                    payload = cmd["payload"] or {}
                    if ctype == "SET_SETTING":
                        for key, value in (payload.get("settings") or {}).items():
                            await self.db.settings.set_setting(key, str(value))
                    elif ctype == "DECIDE_APPROVAL":
                        await self.db.approvals.decide_approval(
                            int(payload["approval_id"]), payload["decision"],
                            notes=payload.get("notes"))
                    else:
                        raise ValueError(f"unknown command_type {ctype!r}")
                    await self.db.commands.mark_applied(cid)
                    applied += 1
                except Exception as exc:
                    logger.exception("[CMD] operator command %d failed: %s", cid, exc)
                    await self.db.commands.mark_failed(cid, str(exc))
            return applied

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

    # ── shared dependency surface proxied to EngineContext ────────────────────
    # The live values live on ``self.ctx`` (single source). These read/write
    # proxies keep ``engine.<dep>`` working for external call-sites (main.py and
    # tests reassign overseer / mm / control_state / approval_mode / …) and for
    # the engine's own pure-orchestration bodies, without a second copy to sync.
    @property
    def db(self):
        return self.ctx.db

    @db.setter
    def db(self, val):
        self.ctx.db = val

    @property
    def broker(self):
        return self.ctx.broker

    @broker.setter
    def broker(self, val):
        self.ctx.broker = val

    @property
    def config(self):
        return self.ctx.config

    @config.setter
    def config(self, val):
        self.ctx.config = val

    @property
    def clock(self):
        return self.ctx.clock

    @clock.setter
    def clock(self, val):
        self.ctx.clock = val

    @property
    def event_bus(self):
        return self.ctx.event_bus

    @event_bus.setter
    def event_bus(self, val):
        self.ctx.event_bus = val

    @property
    def ipc_client(self):
        return self.ctx.ipc_client

    @ipc_client.setter
    def ipc_client(self, val):
        self.ctx.ipc_client = val

    @property
    def overseer(self):
        return self.ctx.overseer

    @overseer.setter
    def overseer(self, val):
        self.ctx.overseer = val

    @property
    def mm(self):
        return self.ctx.mm

    @mm.setter
    def mm(self, val):
        self.ctx.mm = val

    @property
    def risk_engine(self):
        return self.ctx.risk_engine

    @risk_engine.setter
    def risk_engine(self, val):
        self.ctx.risk_engine = val

    @property
    def approval_mode(self):
        return self.ctx.approval_mode

    @approval_mode.setter
    def approval_mode(self, val):
        self.ctx.approval_mode = val

    @property
    def llm_out_of_loop(self):
        return self.ctx.llm_out_of_loop

    @llm_out_of_loop.setter
    def llm_out_of_loop(self, val):
        self.ctx.llm_out_of_loop = val

    @property
    def control_state(self):
        return self.ctx.control_state

    @control_state.setter
    def control_state(self, val):
        self.ctx.control_state = val

    @property
    def _quote_cache(self):
        return self.ctx.quote_cache

    @_quote_cache.setter
    def _quote_cache(self, val):
        self.ctx.quote_cache = val

    @property
    def strategies(self):
        return self.ctx.strategies

    @strategies.setter
    def strategies(self, val):
        # ctx.strategies preserves the PRIORITY sort invariant.
        self.ctx.strategies = val

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

    async def execute_approved_actions(self) -> int:
        return await self.pipeline.execute_approved_actions()

    # ----- top level entry point used by main.py and the scheduler ----------
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
        # Apply any operator commands (pause/mode/approvals/settings) before the
        # order-sensitive trading pipeline runs, so this tick acts on the latest
        # operator intent. Prepended ahead of the pipeline, not interleaved.
        await self.drain_operator_commands()
        await self.sync_positions()
        # Refresh real-time broker order counts to prevent duplicate entries.
        # mm may be None on legacy callers that haven't been updated yet;
        # skip rather than crash the entire tick.
        if self.mm is not None:
            self.mm.clear_edge_stats_cache()
            await self.mm.sync_broker_orders()
        await self.reconcile_orphans()
        if self.risk_engine is not None:
            await self.risk_engine.record_portfolio_greeks()
        mgmt = await self.process_management()
        await self.submit(mgmt, action_type="management")

        # Entries are now submitted internally strategy-by-strategy. Phase 0 has
        # no overseer origination pass — the overseer only reviews (veto/modify)
        # the rules-driven actions on their way through ``submit``.
        num_entries = await self.process_entries(watchlist)
        return {"managed": len(mgmt), "entries": num_entries, "ai": 0}

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
