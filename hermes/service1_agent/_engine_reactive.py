"""
[Service-1: Hermes-Agent-Core] — reactive runtime + market-data/order-fill handlers.

Split out of ``core.py`` so the engine spine stays readable. ``ReactiveController``
is an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.reactive``). It owns two related concerns:

* the **event-loop / IPC / order-monitor runtime** — the background consumer
  loop (in-process queue or durable Redis Streams), the resting-order monitor,
  and the ``publish_event`` fan-out that turns engine calls into bus commands;
* the **reactive handlers** — evaluating strategies and entries when a
  ``MarketDataEvent`` / ``OrderFillEvent`` arrives.

It reads the shared dependency surface (``db`` / ``broker`` / ``event_bus`` /
``config`` / ``strategies`` / ``mm`` / ``ipc_client`` / ``quote_cache``) off the
:class:`~hermes.service1_agent.engine_context.EngineContext` (``self.ctx``), and
keeps ``self.engine`` only for the orchestration callbacks it routes through the
engine spine (``_watchlist_for`` / ``_read_banned_symbols``). Per-loop runtime
state (pending futures, tracked orders, loop / queue / monitor task handles)
lives on the controller itself.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict

from hermes.events.bus import (
    OrderFillEvent,
    TickStartedEvent,
    ClockTickEvent,
    AIApprovalEvent,
    MarketDataEvent,
    OrderTrackedEvent,
    ExecuteTickCommand,
    ExecuteClockTickCommand,
    ExecuteAIApprovalCommand,
    ExecuteMarketDataCommand,
    ExecuteOrderFillCommand,
    ProcessReactiveEntriesEvent,
    SubmitTradeActionsCommand,
    SyncPositionsCommand,
    ReconcileOrphansCommand,
    ProcessManagementCommand,
    ProcessEntriesCommand,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class ReactiveController:
    """Event-loop runtime + reactive market-data / order-fill handlers."""

    def __init__(self, engine: "CascadingEngine") -> None:
        self.engine = engine
        # Shared dependency surface (db / broker / mm / event_bus / config /
        # ipc_client / strategies / quote_cache …) read through the context;
        # ``self.engine`` is kept only for orchestration callbacks.
        self.ctx = engine.ctx
        # Per-loop runtime state (genuine controller-owned state, not deps).
        self._pending_futures: Dict[str, asyncio.Future] = {}
        self._tracked_orders: Dict[str, Dict[str, Any]] = {}
        self.loop_task = None
        self.queue = None
        self._order_monitor_task = None

        if self.ctx.event_bus is not None:
            bus = self.ctx.event_bus
            # Runtime: turn incoming events into publish_event fan-out.
            bus.subscribe(TickStartedEvent, self.handle_tick_started)
            bus.subscribe(ClockTickEvent, self.handle_clock_tick)
            bus.subscribe(AIApprovalEvent, self.handle_ai_approval)
            bus.subscribe(MarketDataEvent, self.handle_market_data)
            bus.subscribe(OrderFillEvent, self.handle_order_fill)
            bus.subscribe(OrderTrackedEvent, self.handle_order_tracked)
            # Reactive: command/event handlers.
            bus.subscribe(ExecuteMarketDataCommand, self.handle_execute_market_data)
            bus.subscribe(ExecuteOrderFillCommand, self.handle_execute_order_fill)
            bus.subscribe(ProcessReactiveEntriesEvent, self.handle_process_reactive_entries)

    # ── event-loop / IPC / order-monitor runtime ─────────────────────────────
    def _is_durable_loop(self) -> bool:
        ipc = self.ctx.ipc_client
        return ipc is not None and ipc.is_connected

    def _ensure_event_loop(self) -> None:
        if self._is_durable_loop():
            if self.loop_task is None or self.loop_task.done():
                self._pending_futures = {}
                self.loop_task = asyncio.create_task(self._redis_event_consumer_loop())
        else:
            if self.queue is None:
                self.queue = asyncio.Queue()
            if self.loop_task is None or self.loop_task.done():
                self.loop_task = asyncio.create_task(self._event_consumer_loop())

    def _ensure_order_monitor(self) -> None:
        if self._order_monitor_task is None or self._order_monitor_task.done():
            try:
                self._order_monitor_task = asyncio.create_task(self._order_monitor_loop())
            except RuntimeError:
                pass

    async def handle_tick_started(self, event: TickStartedEvent) -> None:
        fut = event.future or asyncio.get_running_loop().create_future()
        event.future = fut
        try:
            res = await self.publish_event("TICK", {"watchlist": event.watchlist})
            if not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise

    async def handle_clock_tick(self, event: ClockTickEvent) -> None:
        fut = event.future or asyncio.get_running_loop().create_future()
        event.future = fut
        try:
            res = await self.publish_event("CLOCK_TICK", {"event": event})
            if not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise

    async def handle_ai_approval(self, event: AIApprovalEvent) -> None:
        fut = event.future or asyncio.get_running_loop().create_future()
        event.future = fut
        try:
            res = await self.publish_event("AI_APPROVAL", {"event": event})
            if not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise

    async def handle_market_data(self, event: MarketDataEvent) -> None:
        fut = event.future or asyncio.get_running_loop().create_future()
        event.future = fut
        try:
            res = await self.publish_event("MARKET_DATA", {"event": event})
            if not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise

    async def handle_order_fill(self, event: OrderFillEvent) -> None:
        fut = event.future or asyncio.get_running_loop().create_future()
        event.future = fut
        try:
            res = await self.publish_event("ORDER_FILL", {"event": event})
            if not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise

    async def handle_order_tracked(self, event: OrderTrackedEvent) -> None:
        self._tracked_orders[event.order_id] = {
            "symbol": event.symbol,
            "side": event.side,
            "quantity": event.quantity
        }
        logger.info("[RUNTIME] Registered order %s in reactive monitor", event.order_id)
        self._ensure_order_monitor()

    async def _order_monitor_loop(self) -> None:
        logger.info("[ENGINE] Starting reactive order monitor loop")
        missing_counts = {}

        try:
            active_statuses = {"open", "partially_filled", "pending", "calculated", "accepted"}
            orders = await self.ctx.broker.get_orders() or []
            if isinstance(orders, list):
                for o in orders:
                    status = str(o.get("status", "")).lower()
                    if status in active_statuses:
                        oid = str(o.get("id") or o.get("order_id") or "")
                        if oid and oid not in self._tracked_orders:
                            self._tracked_orders[oid] = {
                                "symbol": str(o.get("symbol", "")).upper(),
                                "side": str(o.get("side", "")),
                                "quantity": int(o.get("quantity", 0))
                            }
                            logger.info("[ENGINE] Discovered existing active order %s in broker; tracking", oid)
        except Exception as exc:
            logger.error("[ENGINE] Failed initial order scan: %s", exc)

        while True:
            if not self._tracked_orders:
                await asyncio.sleep(1.0)
                continue

            try:
                orders = await self.ctx.broker.get_orders() or []
                if isinstance(orders, list):
                    orders_by_id = {}
                    for o in orders:
                        oid = str(o.get("id") or o.get("order_id") or "")
                        if oid:
                            orders_by_id[oid] = o

                    for oid in list(self._tracked_orders.keys()):
                        info = self._tracked_orders[oid]
                        if oid in orders_by_id:
                            missing_counts.pop(oid, None)
                            broker_order = orders_by_id[oid]
                            status = str(broker_order.get("status", "")).lower()

                            if status in {"filled", "canceled", "rejected", "expired"}:
                                logger.info("[ENGINE] Tracked order %s transitioned to terminal status: %s", oid, status)
                                event = OrderFillEvent(
                                    broker_order_id=oid,
                                    symbol=str(broker_order.get("symbol", info.get("symbol", ""))).upper(),
                                    side=str(broker_order.get("side", info.get("side", ""))),
                                    quantity=int(broker_order.get("quantity", info.get("quantity", 0))),
                                    price=float(broker_order.get("avg_fill_price") or broker_order.get("price") or 0.0),
                                    status=status
                                )
                                if self.ctx.event_bus:
                                    self.ctx.event_bus.emit(event)
                                self._tracked_orders.pop(oid, None)
                        else:
                            missing_counts[oid] = missing_counts.get(oid, 0) + 1
                            if missing_counts[oid] >= 3:
                                logger.info("[ENGINE] Tracked order %s was missing from broker for 3 checks, treating as filled", oid)
                                event = OrderFillEvent(
                                    broker_order_id=oid,
                                    symbol=info.get("symbol", "").upper(),
                                    side=info.get("side", ""),
                                    quantity=info.get("quantity", 0),
                                    price=0.0,
                                    status="filled"
                                )
                                if self.ctx.event_bus:
                                    self.ctx.event_bus.emit(event)
                                self._tracked_orders.pop(oid, None)
                                missing_counts.pop(oid, None)
            except Exception as exc:
                logger.error("[ENGINE] Error in order monitor loop: %s", exc)

            await asyncio.sleep(1.0)

    async def publish_event(self, event_type: str, payload: Dict[str, Any]) -> Any:
        self._ensure_event_loop()

        if self._is_durable_loop():
            import json
            client = self.ctx.ipc_client.client
            fut = asyncio.get_running_loop().create_future()

            serializable_payload = {k: v for k, v in payload.items() if k != "future"}

            msg_id = await client.xadd(
                "hermes_event_stream",
                {
                    "event_type": event_type,
                    "payload": json.dumps(serializable_payload)
                }
            )

            self._pending_futures[msg_id] = fut
            return await fut
        else:
            fut = asyncio.get_running_loop().create_future()
            payload_with_fut = dict(payload)
            payload_with_fut["future"] = fut
            await self.queue.put((event_type, payload_with_fut))
            return await fut

    async def _redis_event_consumer_loop(self) -> None:
        import json
        logger.info("[ENGINE] Starting Redis Streams background durable event loop consumer.")
        client = self.ctx.ipc_client.client

        try:
            await client.xgroup_create("hermes_event_stream", "hermes_engine_group", id="0", mkstream=True)
            logger.info("[ENGINE] Created Redis Stream consumer group 'hermes_engine_group'")
        except Exception as err:
            if "BUSYGROUP" not in str(err):
                logger.warning("[ENGINE] Redis Stream group create failed or already exists: %s", err)

        consumer_name = "engine_consumer"
        while True:
            try:
                response = await client.xreadgroup(
                    groupname="hermes_engine_group",
                    consumername=consumer_name,
                    streams={"hermes_event_stream": "0"},
                    count=5,
                    block=100
                )

                if not response:
                    response = await client.xreadgroup(
                        groupname="hermes_engine_group",
                        consumername=consumer_name,
                        streams={"hermes_event_stream": ">"},
                        count=5,
                        block=1000
                    )

                if not response:
                    continue

                for stream_name, messages in response:
                    for msg_id, payload in messages:
                        event_type = payload.get("event_type")
                        payload_json = payload.get("payload")
                        try:
                            data = json.loads(payload_json) if payload_json else {}

                            fut = self._pending_futures.get(msg_id)
                            if fut:
                                data["future"] = fut

                            await self._process_event(event_type, data)

                        except Exception as exc:
                            logger.exception("[ENGINE] Error processing durable event %s: %s", msg_id, exc)
                        finally:
                            try:
                                await client.xack("hermes_event_stream", "hermes_engine_group", msg_id)
                            except Exception:
                                logger.exception("[ENGINE] xack failed for %s", msg_id)
                            self._pending_futures.pop(msg_id, None)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("[ENGINE] Durable event consumer loop error: %s", exc)
                await asyncio.sleep(1.0)

    async def _event_consumer_loop(self) -> None:
        logger.info("[ENGINE] Starting background event loop consumer.")
        while True:
            try:
                event_type, payload = await self.queue.get()
                try:
                    await self._process_event(event_type, payload)
                except Exception as exc:
                    logger.exception(f"[ENGINE] Error processing event {event_type}: {exc}")
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception(f"[ENGINE] Event consumer loop error: {exc}")

    async def _process_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        fut = payload.get("future")
        bus = self.ctx.event_bus
        try:
            res = None
            if event_type == "TICK":
                watchlist = payload["watchlist"]
                cmd = ExecuteTickCommand(watchlist=watchlist)
                bus.emit(cmd)
                res = await cmd.future
            elif event_type == "CLOCK_TICK":
                cmd = ExecuteClockTickCommand(event=payload["event"])
                bus.emit(cmd)
                res = await cmd.future
            elif event_type == "AI_APPROVAL":
                cmd = ExecuteAIApprovalCommand(event=payload["event"])
                bus.emit(cmd)
                res = await cmd.future
            elif event_type == "MARKET_DATA":
                cmd = ExecuteMarketDataCommand(event=payload["event"])
                bus.emit(cmd)
                res = await cmd.future
            elif event_type == "ORDER_FILL":
                cmd = ExecuteOrderFillCommand(event=payload["event"])
                bus.emit(cmd)
                res = await cmd.future

            if fut and not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if fut and not fut.done():
                fut.set_exception(exc)
            raise

    # ── reactive market-data / order-fill handlers ───────────────────────────
    async def handle_execute_market_data(self, command: ExecuteMarketDataCommand) -> None:
        try:
            res = await self._handle_market_data_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_execute_order_fill(self, command: ExecuteOrderFillCommand) -> None:
        try:
            res = await self._handle_order_fill_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_process_reactive_entries(self, event: ProcessReactiveEntriesEvent) -> None:
        try:
            res = await self.process_reactive_entries(event.symbol)
            if event.future and not event.future.done():
                event.future.set_result(res)
        except Exception as exc:
            if event.future and not event.future.done():
                event.future.set_exception(exc)
            raise

    async def _handle_market_data_internal(self, event: MarketDataEvent) -> None:
        """Evaluates strategies reactively when a new MarketDataEvent is received."""
        symbol = event.symbol

        old_price = None
        if symbol in self.ctx.quote_cache:
            old_price = self.ctx.quote_cache[symbol].get("price")

        self.ctx.quote_cache[symbol] = {
            "price": event.price,
            "volume": event.volume,
            **event.data
        }

        self.ctx.broker.update_cached_quote(symbol, {
            "symbol": symbol,
            "price": event.price,
            "volume": event.volume,
            **event.data
        })

        from hermes.market_hours import should_block_trades
        blocked, reason = should_block_trades()
        if blocked:
            return

        mgmt_actions = []
        for s in self.ctx.strategies:
            try:
                actions = await s.manage_positions()
                if actions:
                    symbol_actions = [a for a in actions if a.symbol == symbol]
                    mgmt_actions.extend(symbol_actions)
            except Exception as exc:
                logger.exception("Management failure in %s for %s: %s", s.NAME, symbol, exc)

        if mgmt_actions:
            cmd = SubmitTradeActionsCommand(actions=mgmt_actions, action_type="management")
            self.ctx.event_bus.emit(cmd)
            await cmd.future

        if old_price is not None and old_price != event.price:
            try:
                analysis = await self.ctx.broker.analyze_symbol(symbol)
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
                    ev_entries = ProcessReactiveEntriesEvent(symbol=symbol)
                    self.ctx.event_bus.emit(ev_entries)
                    await ev_entries.future
                except Exception as exc:
                    logger.exception("Failed to process reactive entries for %s: %s", symbol, exc)

    async def _handle_order_fill_internal(self, event: OrderFillEvent) -> None:
        """Reactively handles order fills by syncing positions and orders immediately."""
        logger.info(
            "[ENGINE] Order fill event received for order %s (%s %d shares/contracts of %s)",
            event.broker_order_id, event.side, event.quantity, event.symbol,
        )
        try:
            cmd = SyncPositionsCommand()
            self.ctx.event_bus.emit(cmd)
            await cmd.future
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync positions on order fill event: %s", exc)

        try:
            if self.ctx.mm is not None:
                await self.ctx.mm.sync_broker_orders()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync broker orders on order fill event: %s", exc)

        try:
            cmd = ReconcileOrphansCommand()
            self.ctx.event_bus.emit(cmd)
            await cmd.future
        except Exception as exc:
            logger.exception("[ENGINE] Failed to reconcile orphans on order fill event: %s", exc)

        try:
            cmd = ProcessManagementCommand()
            self.ctx.event_bus.emit(cmd)
            mgmt = await cmd.future
            if mgmt:
                cmd_submit = SubmitTradeActionsCommand(actions=mgmt, action_type="management")
                self.ctx.event_bus.emit(cmd_submit)
                await cmd_submit.future
                logger.info("[ENGINE] Reactively processed management post order fill: submitted %d actions", len(mgmt))
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process management on order fill event: %s", exc)

        try:
            watchlist = await self.ctx.db.watchlist.all_watchlist_symbols()
            if watchlist:
                banned = await self.engine._read_banned_symbols()
                if banned:
                    watchlist = [s for s in watchlist if s.upper() not in banned]
                if watchlist:
                    cmd = ProcessEntriesCommand(watchlist=watchlist)
                    self.ctx.event_bus.emit(cmd)
                    num_entries = await cmd.future
                    logger.info("[ENGINE] Reactively processed entries post order fill: placed %d entries", num_entries)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process entries on order fill event: %s", exc)

    async def process_reactive_entries(self, symbol: str) -> None:
        """Executes entries reactively for a single symbol that crossed support/resistance."""
        async def _check_watchlist(s):
            wl = await self.engine._watchlist_for(s.strategy_id, [symbol])
            return s if symbol in wl else None

        check_results = await asyncio.gather(*[_check_watchlist(s) for s in self.ctx.strategies])
        strategies_to_run = [s for s in check_results if s is not None]

        if not strategies_to_run:
            return

        max_per_tick = int(self.ctx.config.get("max_orders_per_tick", 5))

        async def _run_reactive_entries(s):
            try:
                return s, await s.execute_entries([symbol])
            except Exception as exc:
                logger.exception("Reactive entry proposal failure in %s for %s: %s", s.NAME, symbol, exc)
                return s, []

        results = await asyncio.gather(*[_run_reactive_entries(s) for s in strategies_to_run])

        if self.ctx.config.get("portfolio_optimization"):
            all_proposed_actions = []
            for s, actions in results:
                all_proposed_actions.extend(actions)

            if not all_proposed_actions:
                return

            avail_bp = await self.ctx.mm.true_available_bp()
            optimized_actions = await self.ctx.mm.optimize_allocation(all_proposed_actions, avail_bp)

            if len(optimized_actions) > max_per_tick:
                logger.warning(
                    "[ENGINE] Reactive optimized entries generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                    len(optimized_actions), max_per_tick, max_per_tick,
                )
                for a in optimized_actions[max_per_tick:]:
                    await self.ctx.db.logs.write_log(
                        a.strategy_id,
                        f"[GUARD] {a.symbol} reactive entry trimmed due to max_orders_per_tick={max_per_tick}"
                    )
                optimized_actions = optimized_actions[:max_per_tick]

            cmd = SubmitTradeActionsCommand(actions=optimized_actions, action_type="entry")
            self.ctx.event_bus.emit(cmd)
            await cmd.future
            if optimized_actions:
                await self.ctx.mm.sync_broker_orders()
        else:
            tick_submitted = 0
            for s, actions in results:
                try:
                    if tick_submitted >= max_per_tick:
                        logger.warning(
                            "[ENGINE] max_orders_per_tick=%d reached during reactive entries; skipping %s",
                            max_per_tick, s.NAME,
                        )
                        break

                    scaled_actions = []
                    for action in actions:
                        requested_lots = action.quantity
                        if action.order_class == "multileg" and action.legs:
                            requested_lots = action.legs[0].get("quantity", 1)

                        if requested_lots <= 0:
                            continue

                        strat_id = action.strategy_id.upper()
                        max_lots_map = {
                            "CS7": 1,
                            "CS75": 1,
                            "TT45": 1,
                            "WHEEL": 5,
                            "HERMESALPHA": 1,
                        }
                        config_key = f"{strat_id.lower()}_max_lots"
                        max_lots = int(self.ctx.config.get(config_key) or max_lots_map.get(strat_id, 1))

                        requirement_per_lot = 0.0
                        if strat_id == "WHEEL":
                            if action.strategy_params.get("side_type") == "put" and action.legs:
                                opt_symbol = action.legs[0].get("option_symbol")
                                if opt_symbol:
                                    from .money_manager import parse_occ_strike
                                    strike = parse_occ_strike(opt_symbol)
                                    if strike:
                                        requirement_per_lot = strike * 100.0
                        else:
                            if action.width:
                                requirement_per_lot = action.width * 100.0

                        scaled = await self.ctx.mm.scale_quantity(
                            requested_lots=requested_lots,
                            requirement_per_lot=requirement_per_lot,
                            symbol=action.symbol,
                            side=action.side,
                            strategy_id=action.strategy_id,
                            max_lots=max_lots,
                            expiry=action.expiry,
                        )

                        if scaled > 0:
                            action.quantity = scaled
                            for leg in action.legs:
                                leg["quantity"] = scaled
                            scaled_actions.append(action)

                    remaining = max_per_tick - tick_submitted
                    if len(scaled_actions) > remaining:
                        scaled_actions = scaled_actions[:remaining]

                    cmd = SubmitTradeActionsCommand(actions=scaled_actions, action_type="entry")
                    self.ctx.event_bus.emit(cmd)
                    await cmd.future
                    tick_submitted += len(scaled_actions)

                    if scaled_actions:
                        await self.ctx.mm.sync_broker_orders()
                except Exception as exc:
                    logger.exception("Reactive entry failure in %s for %s: %s", s.NAME, symbol, exc)
