from __future__ import annotations
from ._stubs import alias_db_namespaces

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from hermes.service1_agent.core import CascadingEngine
from hermes.service1_agent.strategy_base import AbstractStrategy


class DummyStrategy(AbstractStrategy):
    def __init__(self, strategy_id: str, priority: int, name: str):
        self.strategy_id = strategy_id
        self.PRIORITY = priority
        self.NAME = name
        self.mm = None

    async def execute_entries(self, watchlist) -> list:
        return []

    async def manage_positions(self) -> list:
        return []


@pytest.mark.asyncio
async def test_durable_loop_redis_streams_flow():
    # 1. Setup mock Redis client with a simple stateful stream mock
    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()
    mock_redis.xack = AsyncMock()

    stream_db = []
    
    async def mock_xadd(name, fields, id='*'):
        msg_id = f"1686984023000-{len(stream_db)}"
        stream_db.append((msg_id, fields))
        return msg_id
        
    mock_redis.xadd = AsyncMock(side_effect=mock_xadd)
    
    async def mock_xreadgroup(groupname, consumername, streams, count=None, block=None):
        stream_id = streams.get("hermes_event_stream")
        if stream_id == "0":
            return []
        elif stream_id == ">":
            if stream_db:
                # Return the stream messages and clear/consume them
                res = [("hermes_event_stream", list(stream_db))]
                stream_db.clear()
                return res
            else:
                await asyncio.sleep(0.01)
                return []
        return []
            
    mock_redis.xreadgroup = AsyncMock(side_effect=mock_xreadgroup)

    # 2. Mock IPC client
    mock_ipc = MagicMock()
    mock_ipc.is_connected = True
    mock_ipc.client = mock_redis

    # 3. Instantiate Engine with dummy strategy
    strat = DummyStrategy("CS75", 1, "CS75")
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    broker_mock = AsyncMock()
    
    engine = CascadingEngine(
        broker=broker_mock,
        db=db_mock,
        strategies=[strat],
        config={"portfolio_optimization": False}
    )
    
    # Override engine's ipc_client with our mock
    engine.ipc_client = mock_ipc

    # Mock _run_tick_internal to trace tick execution
    tick_run_future = asyncio.get_running_loop().create_future()
    async def mock_run_tick_internal(watchlist):
        assert watchlist == ["AAPL"]
        tick_run_future.set_result(True)
        return 42

    engine._run_tick_internal = AsyncMock(side_effect=mock_run_tick_internal)

    # 4. Trigger the event publish (which will write to Redis Stream)
    # Start the event loop
    engine._ensure_event_loop()
    
    # We await publish_event, which will write to the mock stream, map the future,
    # and wait for the consumer task to process it.
    publish_task = asyncio.create_task(engine.publish_event("TICK", {"watchlist": ["AAPL"]}))
    
    # Wait for the tick internal method to run
    await asyncio.wait_for(tick_run_future, timeout=2.0)
    
    # Await publish_event completion and verify return value
    res = await asyncio.wait_for(publish_task, timeout=2.0)
    assert res == 42  # Event loop resolves future to 42 (the return value of _run_tick_internal)

    # Verify Redis client calls
    mock_redis.xgroup_create.assert_called_once()
    mock_redis.xadd.assert_called_once_with(
        "hermes_event_stream",
        {"event_type": "TICK", "payload": '{"watchlist": ["AAPL"]}'}
    )
    mock_redis.xack.assert_called_once_with("hermes_event_stream", "hermes_engine_group", "1686984023000-0")

    # Clean up background task
    if engine.loop_task:
        engine.loop_task.cancel()
        try:
            await engine.loop_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_durable_loop_failed_tick_is_not_replayed():
    """A tick that raises must be acked and never re-executed.

    Regression: previously a failed message was left un-acked (only the success
    path called xack) while its future was popped in ``finally``. The next loop
    re-read the pending message, found no future, and re-ran the tick's side
    effects with no awaiter — i.e. a duplicate order-submitting tick.
    """
    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()

    # Model proper consumer-group semantics: new messages are delivered once via
    # ">" (which moves them into the Pending Entries List), pending unacked
    # messages are re-read via "0", and xack removes them from the PEL.
    new_msgs = []          # undelivered (msg_id, fields)
    pel: dict = {}         # delivered, unacked
    acked = []

    async def mock_xack(name, group, msg_id):
        pel.pop(msg_id, None)
        acked.append(msg_id)

    mock_redis.xack = AsyncMock(side_effect=mock_xack)

    async def mock_xadd(name, fields, id="*"):
        msg_id = f"1686984023000-{len(new_msgs) + len(pel) + len(acked)}"
        new_msgs.append((msg_id, fields))
        return msg_id

    mock_redis.xadd = AsyncMock(side_effect=mock_xadd)

    async def mock_xreadgroup(groupname, consumername, streams, count=None, block=None):
        stream_id = streams.get("hermes_event_stream")
        if stream_id == "0":
            return [("hermes_event_stream", list(pel.items()))] if pel else []
        elif stream_id == ">":
            if new_msgs:
                delivered = list(new_msgs)
                new_msgs.clear()
                for mid, fields in delivered:
                    pel[mid] = fields
                return [("hermes_event_stream", delivered)]
            await asyncio.sleep(0.01)
            return []
        return []

    mock_redis.xreadgroup = AsyncMock(side_effect=mock_xreadgroup)

    mock_ipc = MagicMock()
    mock_ipc.is_connected = True
    mock_ipc.client = mock_redis

    strat = DummyStrategy("CS75", 1, "CS75")
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    broker_mock = AsyncMock()

    engine = CascadingEngine(
        broker=broker_mock,
        db=db_mock,
        strategies=[strat],
        config={"portfolio_optimization": False},
    )
    engine.ipc_client = mock_ipc

    call_count = 0

    async def boom(watchlist):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("tick blew up")

    engine._run_tick_internal = AsyncMock(side_effect=boom)

    engine._ensure_event_loop()

    # The caller must observe the failure exactly once.
    with pytest.raises(RuntimeError, match="tick blew up"):
        await asyncio.wait_for(
            engine.publish_event("TICK", {"watchlist": ["AAPL"]}), timeout=2.0
        )

    # Give the consumer a few extra loops to (incorrectly) replay if the bug
    # were still present.
    await asyncio.sleep(0.1)

    assert call_count == 1, f"failed tick was replayed {call_count} times"
    assert acked.count("1686984023000-0") == 1, "failed message was not acked"

    if engine.loop_task:
        engine.loop_task.cancel()
        try:
            await engine.loop_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_durable_loop_dataclass_serialization():
    from hermes.events.bus import MarketDataEvent
    
    mock_redis = AsyncMock()
    mock_redis.xgroup_create = AsyncMock()
    mock_redis.xack = AsyncMock()

    stream_db = []
    
    async def mock_xadd(name, fields, id='*'):
        msg_id = f"1686984023000-{len(stream_db)}"
        stream_db.append((msg_id, fields))
        return msg_id
        
    mock_redis.xadd = AsyncMock(side_effect=mock_xadd)
    
    async def mock_xreadgroup(groupname, consumername, streams, count=None, block=None):
        stream_id = streams.get("hermes_event_stream")
        if stream_id == "0":
            return []
        elif stream_id == ">":
            if stream_db:
                res = [("hermes_event_stream", list(stream_db))]
                stream_db.clear()
                return res
            await asyncio.sleep(0.01)
            return []
        return []
            
    mock_redis.xreadgroup = AsyncMock(side_effect=mock_xreadgroup)

    mock_ipc = MagicMock()
    mock_ipc.is_connected = True
    mock_ipc.client = mock_redis

    strat = DummyStrategy("CS75", 1, "CS75")
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    broker_mock = AsyncMock()
    
    engine = CascadingEngine(
        broker=broker_mock,
        db=db_mock,
        strategies=[strat],
        config={"portfolio_optimization": False}
    )
    engine.ipc_client = mock_ipc

    processed_events = []
    async def mock_process_event(event_type, payload):
        processed_events.append((event_type, payload))
        fut = payload.get("future")
        if fut and not fut.done():
            fut.set_result(True)
        return True

    engine.reactive._process_event = AsyncMock(side_effect=mock_process_event)
    engine.reactive._ensure_event_loop()

    event = MarketDataEvent(symbol="AAPL", price=150.0, volume=1000)
    await engine.reactive.publish_event("MARKET_DATA", {"event": event})

    assert len(processed_events) == 1
    ev_type, payload = processed_events[0]
    assert ev_type == "MARKET_DATA"
    deserialized_event = payload["event"]
    assert isinstance(deserialized_event, MarketDataEvent)
    assert deserialized_event.symbol == "AAPL"
    assert deserialized_event.price == 150.0
    assert deserialized_event.volume == 1000

    if engine.reactive.loop_task:
        engine.reactive.loop_task.cancel()
        try:
            await engine.reactive.loop_task
        except asyncio.CancelledError:
            pass

