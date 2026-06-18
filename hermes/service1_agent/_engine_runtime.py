"""
[Service-1: Hermes-Agent-Core] — event-loop / IPC / order-monitor runtime controller.

Split out of ``core.py`` to keep the engine's spine readable. ``RuntimeController``
is an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.runtime``); it shares the engine's hot tick state via
:class:`~hermes.service1_agent._engine_base._EngineCollaborator`, so ``self.X``
reads/writes the engine. Not meant to be used standalone.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from hermes.events.bus import OrderFillEvent
from ._engine_base import _EngineCollaborator

logger = logging.getLogger("hermes.agent.core")


class RuntimeController(_EngineCollaborator):
    def _is_durable_loop(self) -> bool:
        return self.ipc_client is not None and self.ipc_client.is_connected

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

    async def _order_monitor_loop(self) -> None:
        logger.info("[ENGINE] Starting reactive order monitor loop")
        missing_counts = {}
        
        # Initial scan for active/working orders
        try:
            active_statuses = {"open", "partially_filled", "pending", "calculated", "accepted"}
            orders = await self.broker.get_orders() or []
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
                orders = await self.broker.get_orders() or []
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
                                if self.event_bus:
                                    self.event_bus.emit(event)
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
                                if self.event_bus:
                                    self.event_bus.emit(event)
                                self._tracked_orders.pop(oid, None)
                                missing_counts.pop(oid, None)
            except Exception as exc:
                logger.error("[ENGINE] Error in order monitor loop: %s", exc)

            await asyncio.sleep(1.0)

    async def publish_event(self, event_type: str, payload: Dict[str, Any]) -> Any:
        self._ensure_event_loop()
        
        if self._is_durable_loop():
            import json
            client = self.ipc_client.client
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
        client = self.ipc_client.client
        
        try:
            await client.xgroup_create("hermes_event_stream", "hermes_engine_group", id="0", mkstream=True)
            logger.info("[ENGINE] Created Redis Stream consumer group 'hermes_engine_group'")
        except Exception as err:
            if "BUSYGROUP" not in str(err):
                logger.warning("[ENGINE] Redis Stream group create failed or already exists: %s", err)
                
        consumer_name = "engine_consumer"
        while True:
            try:
                # 1. Read unacknowledged/pending messages for recovery
                response = await client.xreadgroup(
                    groupname="hermes_engine_group",
                    consumername=consumer_name,
                    streams={"hermes_event_stream": "0"},
                    count=5,
                    block=100
                )
                
                # 2. Read new messages
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
                            # Ack regardless of outcome (at-most-once). Each durable
                            # message is the request half of one awaited
                            # ``publish_event`` call, and ``_process_event`` has already
                            # resolved that caller's future (result *or* exception). If
                            # we left a failed message un-acked it would be re-delivered
                            # next loop with its future already popped — re-running the
                            # tick's side effects (e.g. duplicate broker orders) with no
                            # awaiter to receive them. Dropping a failed tick is safer
                            # than silently replaying it.
                            try:
                                await client.xack("hermes_event_stream", "hermes_engine_group", msg_id)
                            except Exception:                      # noqa: BLE001
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
        try:
            res = None
            if event_type == "TICK":
                watchlist = payload["watchlist"]
                res = await self._run_tick_internal(watchlist)
            elif event_type == "CLOCK_TICK":
                res = await self._handle_clock_tick_internal(payload["event"])
            elif event_type == "AI_APPROVAL":
                res = await self._handle_ai_approval_internal(payload["event"])
            elif event_type == "MARKET_DATA":
                res = await self._handle_market_data_internal(payload["event"])
            elif event_type == "ORDER_FILL":
                res = await self._handle_order_fill_internal(payload["event"])
            if fut and not fut.done():
                fut.set_result(res)
        except Exception as exc:
            if fut and not fut.done():
                fut.set_exception(exc)
            raise
