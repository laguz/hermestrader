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
engine spine (``_watchlist_for``). Per-loop runtime
state (pending futures, tracked orders, loop / queue / monitor task handles)
lives on the controller itself.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from hermes.events.bus import (
    OrderFillEvent,
    TickStartedEvent,
    ClockTickEvent,
    AIApprovalEvent,
    MarketDataEvent,
    OrderTrackedEvent,
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

    # _process_event used to round-trip back through the same EventBus (emit
    # an Execute*Command and await its future) that the *outer* handler used
    # to get here in the first place — a burst of concurrent events exhausted
    # the bus's dispatch semaphore with outer handlers all blocked on their
    # own redis round-trip, deadlocking the single-threaded durable consumer
    # loop (no exception, 0% CPU, no further ticks). _process_event and the
    # internal handlers it reaches now call each command's single subscriber
    # directly instead of re-entering the bus, so that deadlock can't form —
    # this bound remains as the backstop so one genuinely stuck message (a
    # wedged broker call, a hung DB write) can never wedge every future
    # message behind it.
    _PROCESS_EVENT_TIMEOUT_S = 90.0

    # Quotes are only useful fresh: a MARKET_DATA message that sat in the
    # stream longer than this is already superseded by newer ticks (and the
    # periodic heartbeat re-evaluates every position regardless), so the
    # durable consumer sheds it instead of processing it. Without shedding, a
    # transient stall (CPU-saturating ML retrain, market-open burst) leaves a
    # backlog where every message costs a full _PROCESS_EVENT_TIMEOUT_S:
    # producer-side bus handlers pile up awaiting their round-trip futures,
    # fresh quotes queue behind stale ones faster than 1-per-90s drains them,
    # and CLOCK_TICKs starve forever. Only MARKET_DATA is shed — TICK /
    # CLOCK_TICK / AI_APPROVAL / ORDER_FILL must run no matter how late.
    _MARKET_DATA_SHED_AFTER_S = 30.0

    # Caps the durable Redis stream so it can't grow unboundedly (xadd has no
    # implicit trim). Generous relative to normal throughput — the consumer
    # loop keeps PENDING at ~1 message under healthy operation — so this only
    # ever bites during a genuine sustained backlog, trimming stale entries
    # rather than growing Redis memory forever.
    _STREAM_MAXLEN = 10_000

    # Upper bound on how long publish_event waits for its round-trip future.
    # A future that never resolves (an entry trimmed before delivery, or any
    # leak path not yet found) would otherwise hold its EventBus dispatch
    # permit forever — 50 leaked permits and the whole bus freezes: no
    # CLOCK_TICK, no ML ticks, nothing, until the liveness watchdog kill-loops
    # the agent. Generous relative to the consumer's 90s per-event cap plus
    # realistic backlog queueing, so it only fires when the future is
    # genuinely orphaned; the consumer may still process the event late.
    _PUBLISH_RESULT_TIMEOUT_S = 300.0

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

    @staticmethod
    def _durable_msg_age_s(msg_id: Any) -> Optional[float]:
        """Age of a Redis Stream entry from its server-assigned id (ms-epoch)."""
        try:
            raw = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
            return max(0.0, time.time() - int(raw.split("-", 1)[0]) / 1000.0)
        except (ValueError, AttributeError):
            return None

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
                logger.error("order monitor failed to start — no running event loop",
                             exc_info=True)

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
                                    price=float(
                                        broker_order.get("avg_fill_price")
                                        if broker_order.get("avg_fill_price") is not None
                                        else (
                                            broker_order.get("price")
                                            if broker_order.get("price") is not None
                                            else 0.0
                                        )
                                    ),
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

    def _serialize_value(self, v: Any) -> Any:
        import dataclasses
        if dataclasses.is_dataclass(v):
            res = {"__event_class__": v.__class__.__name__}
            for f in dataclasses.fields(v):
                res[f.name] = self._serialize_value(getattr(v, f.name))
            return res
        elif isinstance(v, list):
            return [self._serialize_value(item) for item in v]
        elif isinstance(v, dict):
            return {k: self._serialize_value(val) for k, val in v.items()}
        else:
            return v

    def _deserialize_value(self, v: Any) -> Any:
        import dataclasses
        from hermes.events.bus import (
            OrderFillEvent,
            TickStartedEvent,
            ClockTickEvent,
            AIApprovalEvent,
            MarketDataEvent,
            OrderTrackedEvent,
        )
        from hermes.service1_agent.trade_action import TradeAction

        EVENT_CLASS_MAP = {
            "MarketDataEvent": MarketDataEvent,
            "OrderFillEvent": OrderFillEvent,
            "ClockTickEvent": ClockTickEvent,
            "AIApprovalEvent": AIApprovalEvent,
            "TickStartedEvent": TickStartedEvent,
            "OrderTrackedEvent": OrderTrackedEvent,
            "TradeAction": TradeAction,
        }

        if isinstance(v, dict):
            v = {k: self._deserialize_value(val) for k, val in v.items()}
            if "__event_class__" in v:
                cls_name = v.pop("__event_class__")
                cls = EVENT_CLASS_MAP.get(cls_name)
                if cls:
                    orig_timestamp = v.pop("timestamp", None)
                    valid_fields = {f.name for f in dataclasses.fields(cls) if f.init}
                    init_data = {field_name: val for field_name, val in v.items() if field_name in valid_fields}
                    inst = cls(**init_data)
                    if orig_timestamp is not None:
                        from datetime import datetime
                        try:
                            inst.timestamp = datetime.fromisoformat(orig_timestamp)
                        except Exception as e:
                            logger.debug("Failed to deserialize timestamp: %s", e)
                    return inst
                return v
            return v
        elif isinstance(v, list):
            return [self._deserialize_value(item) for item in v]
        else:
            return v

    async def publish_event(self, event_type: str, payload: Dict[str, Any]) -> Any:
        self._ensure_event_loop()

        if self._is_durable_loop():
            import json
            import uuid
            client = self.ctx.ipc_client.client
            fut = asyncio.get_running_loop().create_future()

            filtered_payload = {k: v for k, v in payload.items() if k != "future"}
            serializable_payload = self._serialize_value(filtered_payload)

            # The future must be registered BEFORE the xadd, keyed by a
            # client-generated correlation id rather than the server-assigned
            # msg_id: the consumer sits parked in xreadgroup(block=...), so
            # Redis can deliver the entry and the consumer can process, ack,
            # and pop it before this coroutine ever resumes from the xadd
            # await. Under the old post-xadd msg_id keying, losing that race
            # left this coroutine awaiting a future nobody would ever resolve,
            # permanently holding one EventBus dispatch permit per lost race —
            # a few minutes of MARKET_DATA traffic leaked all 50 permits and
            # froze every event type on the bus until the liveness watchdog
            # killed the process (the ~46-minute offline kill-loop of
            # 2026-07-08).
            corr_id = uuid.uuid4().hex
            self._pending_futures[corr_id] = fut

            # Bound the stream — xadd never trims on its own, so without maxlen
            # every event (including high-frequency MARKET_DATA ticks) stays in
            # Redis forever, even once acked. Approximate trimming (MAXLEN ~) is
            # the cheap form: Redis trims whole macro-nodes instead of walking
            # the stream, and the single in-flight consumer entry is always the
            # most recent add, so it's never at risk of being trimmed away.
            try:
                await client.xadd(
                    "hermes_event_stream",
                    {
                        "event_type": event_type,
                        "payload": json.dumps(serializable_payload, default=str),
                        "corr_id": corr_id,
                    },
                    maxlen=self._STREAM_MAXLEN,
                    approximate=True,
                )
            except BaseException:
                self._pending_futures.pop(corr_id, None)
                raise

            try:
                return await asyncio.wait_for(fut, timeout=self._PUBLISH_RESULT_TIMEOUT_S)
            except asyncio.TimeoutError:
                self._pending_futures.pop(corr_id, None)
                logger.error(
                    "[ENGINE] Durable %s round-trip returned no result within "
                    "%.0fs — abandoning the wait to release this dispatch's "
                    "bus permit (the consumer may still process the event "
                    "late).", event_type, self._PUBLISH_RESULT_TIMEOUT_S,
                )
                raise
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

                has_messages = False
                if response:
                    for _, messages in response:
                        if messages:
                            has_messages = True
                            break

                if not has_messages:
                    response = await client.xreadgroup(
                        groupname="hermes_engine_group",
                        consumername=consumer_name,
                        streams={"hermes_event_stream": ">"},
                        count=5,
                        block=1000
                    )
                    has_messages = False
                    if response:
                        for _, messages in response:
                            if messages:
                                has_messages = True
                                break

                if not has_messages:
                    continue

                for _stream_name, messages in response:
                    for msg_id, payload in messages:
                        event_type = payload.get("event_type")
                        payload_json = payload.get("payload")
                        # Futures are keyed by the publisher's corr_id
                        # (registered before the xadd — see publish_event);
                        # fall back to msg_id for entries a pre-corr_id build
                        # left in the stream.
                        fut_key = payload.get("corr_id") or msg_id
                        try:
                            if event_type == "MARKET_DATA":
                                age_s = self._durable_msg_age_s(msg_id)
                                if age_s is not None and age_s > self._MARKET_DATA_SHED_AFTER_S:
                                    logger.info(
                                        "[ENGINE] Shedding stale MARKET_DATA %s "
                                        "(%.0fs old > %.0fs) — superseded by fresher quotes.",
                                        msg_id, age_s, self._MARKET_DATA_SHED_AFTER_S,
                                    )
                                    fut = self._pending_futures.get(fut_key)
                                    if fut and not fut.done():
                                        fut.set_result(None)
                                    continue  # the finally block still acks + pops
                            raw_data = json.loads(payload_json) if payload_json else {}
                            data = self._deserialize_value(raw_data)

                            fut = self._pending_futures.get(fut_key)
                            if fut:
                                data["future"] = fut

                            try:
                                await asyncio.wait_for(
                                    self._process_event(event_type, data),
                                    timeout=self._PROCESS_EVENT_TIMEOUT_S,
                                )
                            except asyncio.TimeoutError:
                                logger.error(
                                    "[ENGINE] Processing durable event %s (%s) timed out after "
                                    "%ss — acking and continuing so the consumer loop isn't "
                                    "wedged forever.",
                                    msg_id, event_type, self._PROCESS_EVENT_TIMEOUT_S,
                                )
                                # wait_for cancels the inner coroutine on timeout, so
                                # _process_event's own except-block (which only catches
                                # Exception, not CancelledError) never gets to resolve
                                # fut — do it here or the original caller awaiting fut
                                # (publish_event's `return await fut`) hangs forever too.
                                if fut and not fut.done():
                                    fut.set_exception(TimeoutError(
                                        f"{event_type} processing timed out after "
                                        f"{self._PROCESS_EVENT_TIMEOUT_S}s"
                                    ))

                            # asyncio.wait_for swallows an outer cancellation that
                            # arrives at the exact instant the wrapped coroutine
                            # completes — it just returns the result instead of
                            # raising CancelledError. Without this check, a
                            # loop_task.cancel() that lands on this line is silently
                            # discarded and the consumer loop runs forever instead of
                            # stopping (this hung the shutdown path in tests and would
                            # do the same in production stop()).
                            current_task = asyncio.current_task()
                            if current_task is not None and current_task.cancelling():
                                raise asyncio.CancelledError()

                        except Exception as exc:
                            logger.exception("[ENGINE] Error processing durable event %s: %s", msg_id, exc)
                        finally:
                            try:
                                await client.xack("hermes_event_stream", "hermes_engine_group", msg_id)
                            except Exception:
                                logger.exception("[ENGINE] xack failed for %s", msg_id)
                            self._pending_futures.pop(fut_key, None)

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
                    try:
                        await asyncio.wait_for(
                            self._process_event(event_type, payload),
                            timeout=self._PROCESS_EVENT_TIMEOUT_S,
                        )
                    except asyncio.TimeoutError:
                        logger.error(
                            "[ENGINE] Processing event %s timed out after %ss — continuing "
                            "so the consumer loop isn't wedged forever.",
                            event_type, self._PROCESS_EVENT_TIMEOUT_S,
                        )
                        fut = payload.get("future")
                        if fut and not fut.done():
                            fut.set_exception(TimeoutError(
                                f"{event_type} processing timed out after "
                                f"{self._PROCESS_EVENT_TIMEOUT_S}s"
                            ))

                    # See the matching comment in _redis_event_consumer_loop:
                    # wait_for can silently swallow an outer cancellation that
                    # lands the instant the wrapped coroutine completes,
                    # leaving this loop unstoppable.
                    current_task = asyncio.current_task()
                    if current_task is not None and current_task.cancelling():
                        raise asyncio.CancelledError()
                except Exception as exc:
                    logger.exception(f"[ENGINE] Error processing event {event_type}: {exc}")
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception(f"[ENGINE] Event consumer loop error: {exc}")

    async def _process_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        # Every branch bypasses the EventBus/semaphore and calls its single
        # subscriber's internal method directly. Routing through
        # bus.emit()+cmd.future re-enters the very semaphore a MARKET_DATA
        # burst saturates (see the class docstring above) — every permit held
        # by outer MARKET_DATA dispatches awaiting *their own* durable
        # round-trip, so the durable consumer can't get a permit to dispatch
        # the Execute*Command it just emitted: a circular deadlock, bounded
        # only by _PROCESS_EVENT_TIMEOUT_S. CLOCK_TICK got this bypass first
        # (its starvation took the whole agent "offline" in the watcher);
        # MARKET_DATA/ORDER_FILL/AI_APPROVAL/TICK deadlocked the same way —
        # in production every single MARKET_DATA event burned the full 90s
        # budget in that wait and was then acked with nothing done, so no
        # reactive management ran at all during market hours. Each command
        # here has exactly one subscriber (a thin wrapper that calls the same
        # internal method), so calling it directly loses nothing.
        fut = payload.get("future")
        try:
            res = None
            if event_type == "TICK":
                res = await self.engine._run_tick_internal(payload["watchlist"])
            elif event_type == "CLOCK_TICK":
                res = await self.engine._handle_clock_tick_internal(payload["event"])
            elif event_type == "AI_APPROVAL":
                res = await self.engine._handle_ai_approval_internal(payload["event"])
            elif event_type == "MARKET_DATA":
                res = await self._handle_market_data_internal(payload["event"])
            elif event_type == "ORDER_FILL":
                res = await self._handle_order_fill_internal(payload["event"])
            else:
                logger.warning("[ENGINE] Unrecognized event_type %r; dropping", event_type)

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

    async def _strategies_holding_symbol(self, symbol: str) -> list:
        """Strategies with >=1 open position in ``symbol``.

        ``manage_positions()`` is a full broker-backed sweep (fetches live
        quotes for every open position the strategy holds, anywhere) — calling
        it for every strategy on every single market-data tick and filtering
        the *result* by symbol still pays the full broker round-trip cost for
        strategies with nothing open in this symbol. One ``all_open_trades()``
        DB read replaces up to ``len(strategies)`` broker sweeps per tick.
        Fails open (returns every strategy) on a DB error so a transient read
        failure can never silently skip a real exit check.
        """
        if not self.ctx.strategies:
            return []
        try:
            open_trades = await self.ctx.db.trades.all_open_trades() or []
        except Exception:
            logger.exception(
                "[ENGINE] failed to check open trades for %s reactive "
                "management; running the full strategy sweep", symbol,
            )
            return list(self.ctx.strategies)
        open_strategy_ids = {t.get("strategy_id") for t in open_trades
                              if t.get("symbol") == symbol}
        return [s for s in self.ctx.strategies if s.strategy_id in open_strategy_ids]

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
        for s in await self._strategies_holding_symbol(symbol):
            try:
                actions = await s.manage_positions()
                if actions:
                    symbol_actions = [a for a in actions if a.symbol == symbol]
                    mgmt_actions.extend(symbol_actions)
            except Exception as exc:
                logger.exception("Management failure in %s for %s: %s", s.NAME, symbol, exc)

        if mgmt_actions:
            # Direct handler call, not bus.emit()+await future: this runs
            # inside the durable consumer's processing budget, and the bus
            # semaphore may be fully held by outer MARKET_DATA dispatches
            # that only this consumer can unblock (see _process_event).
            cmd = SubmitTradeActionsCommand(actions=mgmt_actions, action_type="management")
            await self.engine.handle_submit_trade_actions(cmd)

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
                    # Direct call for the same semaphore-deadlock reason as
                    # the management branch above; handle_process_reactive_
                    # entries is this event's only subscriber and just wraps
                    # this same method.
                    await self.process_reactive_entries(symbol)
                except Exception as exc:
                    logger.exception("Failed to process reactive entries for %s: %s", symbol, exc)

    async def _handle_order_fill_internal(self, event: OrderFillEvent) -> None:
        """Reactively handles order fills by syncing positions and orders immediately."""
        logger.info(
            "[ENGINE] Order fill event received for order %s (%s %d shares/contracts of %s)",
            event.broker_order_id, event.side, event.quantity, event.symbol,
        )
        # Every step below calls its command's single-subscriber handler
        # directly, not bus.emit()+await future: this whole method runs
        # inside the durable consumer's processing budget, and the bus
        # semaphore may be fully held by outer MARKET_DATA dispatches that
        # only this consumer can unblock (see _process_event). The handlers
        # resolve each command's future themselves, so results still come
        # back through cmd.future.
        try:
            cmd = SyncPositionsCommand()
            await self.engine.handle_sync_positions(cmd)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync positions on order fill event: %s", exc)

        try:
            if self.ctx.mm is not None:
                await self.ctx.mm.sync_broker_orders()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync broker orders on order fill event: %s", exc)

        try:
            cmd = ReconcileOrphansCommand()
            await self.engine.handle_reconcile_orphans(cmd)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to reconcile orphans on order fill event: %s", exc)

        try:
            cmd = ProcessManagementCommand()
            await self.engine.handle_process_management(cmd)
            mgmt = await cmd.future
            if mgmt:
                cmd_submit = SubmitTradeActionsCommand(actions=mgmt, action_type="management")
                await self.engine.handle_submit_trade_actions(cmd_submit)
                logger.info("[ENGINE] Reactively processed management post order fill: submitted %d actions", len(mgmt))
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process management on order fill event: %s", exc)

        try:
            watchlist = await self.ctx.db.watchlist.all_watchlist_symbols()
            if watchlist:
                cmd = ProcessEntriesCommand(watchlist=watchlist)
                await self.engine.handle_process_entries(cmd)
                num_entries = await cmd.future
                logger.info("[ENGINE] Reactively processed entries post order fill: placed %d entries", num_entries)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process entries on order fill event: %s", exc)

    async def process_reactive_entries(self, symbol: str) -> None:
        """Executes entries reactively for a single symbol that crossed support/resistance."""
        async def _check_watchlist(s):
            try:
                wl = await self.engine._watchlist_for(s.strategy_id, [symbol])
            except Exception as exc:
                logger.exception(
                    "Watchlist lookup failure in %s for %s: %s", s.NAME, symbol, exc
                )
                return None
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
            for _s, actions in results:
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

            # Direct handler call — runs inside the durable consumer's budget,
            # where the bus semaphore may be saturated (see _process_event).
            cmd = SubmitTradeActionsCommand(actions=optimized_actions, action_type="entry")
            await self.engine.handle_submit_trade_actions(cmd)
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
                        _raw_max_lots = self.ctx.config.get(config_key)
                        max_lots = int(_raw_max_lots) if _raw_max_lots is not None else max_lots_map.get(strat_id, 1)

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
                            if action.width is not None:
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
                    await self.engine.handle_submit_trade_actions(cmd)
                    tick_submitted += len(scaled_actions)

                    if scaled_actions:
                        await self.ctx.mm.sync_broker_orders()
                except Exception as exc:
                    logger.exception("Reactive entry failure in %s for %s: %s", s.NAME, symbol, exc)
