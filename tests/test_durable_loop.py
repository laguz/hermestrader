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
