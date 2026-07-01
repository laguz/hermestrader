"""
Unit tests for the AsyncIPC messaging system.
"""
from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.ipc import AsyncIPC

@pytest.mark.asyncio
async def test_mock_ipc_fallback_publish_subscribe():
    # Initialize with local DSN (connection will fail and fallback to mock)
    ipc = AsyncIPC("redis://localhost:9999/9")
    connected = await ipc.connect()
    
    # Under pytest it should automatically bypass real Redis connection and return False
    assert not connected
    assert not ipc.is_connected
    
    received_messages = []
    async def callback(data: dict):
        received_messages.append(data)
        
    await ipc.subscribe("test_channel", callback)
    
    # Publish to local mock channel
    payload = {"message": "hello world", "value": 42}
    receivers = await ipc.publish("test_channel", payload)
    
    # We registered 1 callback locally
    assert receivers == 1
    
    # Let the async callback execute
    await asyncio.sleep(0.1)
    
    assert len(received_messages) == 1
    assert received_messages[0] == payload
    
    # Unsubscribe
    await ipc.unsubscribe("test_channel", callback)
    
    # Publish again
    receivers = await ipc.publish("test_channel", payload)
    assert receivers == 0

@pytest.mark.asyncio
async def test_mock_ipc_multiple_subscribers():
    ipc = AsyncIPC("redis://localhost:9999/9")
    await ipc.connect()
    
    received1 = []
    received2 = []
    
    async def cb1(data):
        received1.append(data)
        
    async def cb2(data):
        received2.append(data)
        
    await ipc.subscribe("multi_channel", cb1)
    await ipc.subscribe("multi_channel", cb2)
    
    payload = {"data": "test"}
    receivers = await ipc.publish("multi_channel", payload)
    assert receivers == 2
    
    await asyncio.sleep(0.1)
    
    assert len(received1) == 1
    assert len(received2) == 1
    assert received1[0] == payload
    assert received2[0] == payload
    
    # Clean up
    await ipc.unsubscribe("multi_channel")


@pytest.mark.asyncio
async def test_redis_ipc_failure_raises_error():
    # Use a DSN that will fail to connect
    ipc = AsyncIPC("redis://localhost:9999/9")
    
    # Under pytest, connect() defaults to LocalMemoryIPCBackend (which returns False)
    connected = await ipc.connect()
    assert not connected
    assert not ipc.is_connected
    
    # If we bypass the pytest check, it will try connecting to Redis and fail, raising ConnectionError
    with pytest.raises(ConnectionError) as exc_info:
        await ipc.connect(bypass_pytest_check=True)
    
    assert "Failed to connect to Redis IPC" in str(exc_info.value)


@pytest.mark.asyncio
async def test_async_ipc_client_property():
    ipc = AsyncIPC("redis://localhost:9999/9")
    
    # 1. Not connected yet -> client is None
    assert ipc.client is None
    
    # 2. Connect via LocalMemoryIPCBackend -> client is None (no client attribute on local backend)
    await ipc.connect()
    assert ipc.client is None
    
    # 3. Connect to Redis with mock -> client is exposed
    mock_redis = MagicMock()
    mock_redis.ping = AsyncMock()
    
    with patch("redis.asyncio.from_url", return_value=mock_redis):
        redis_ipc = AsyncIPC("redis://localhost:6379/0")
        connected = await redis_ipc.connect(bypass_pytest_check=True)
        assert connected is True
        assert redis_ipc.client is mock_redis


@pytest.mark.asyncio
async def test_redis_ipc_publish_refuses_a_foreign_loop():
    """A production incident: hermes/ml/xgb_features.py retrains on a
    background thread and writes settings via run_maybe_async(), which calls
    asyncio.run() per write — a brand new loop on that thread. settings.py's
    set_setting() then does a best-effort `ipc.publish()` on the *process-wide*
    `hermes.ipc.ipc` singleton, whose Redis connection was actually opened on
    the main loop. Reusing that pooled connection from the background
    thread's loop corrupted it (redis-py raised "Task ... attached to a
    different loop", then "Event loop is closed"), silently breaking every
    later `xadd`/publish call on the main loop for the rest of the process's
    life — the agent looked alive but stopped making progress.

    publish() must detect the foreign loop and refuse instead of touching the
    shared client, so a background-thread caller degrades to a no-op instead
    of poisoning the connection for the main tick loop.
    """
    mock_redis = MagicMock()
    mock_redis.ping = AsyncMock()
    mock_redis.publish = AsyncMock(return_value=1)

    with patch("redis.asyncio.from_url", return_value=mock_redis):
        redis_ipc = AsyncIPC("redis://localhost:6379/0")
        connected = await redis_ipc.connect(bypass_pytest_check=True)
        assert connected is True

    foreign_loop_error: list = []

    def run_on_background_thread() -> None:
        # Mirrors run_maybe_async(): a fresh event loop per call, on a
        # thread that is not the one the redis client was connected on.
        async def _publish():
            return await redis_ipc.publish("hermes_agent_commands", {"key": "ml_last_ok_ts"})

        try:
            asyncio.run(_publish())
        except Exception as exc:  # noqa: BLE001 - captured for the assertion below
            foreign_loop_error.append(exc)

    t = threading.Thread(target=run_on_background_thread, daemon=True)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive()

    # Refused before ever touching the shared (foreign-loop) client.
    assert len(foreign_loop_error) == 1
    assert isinstance(foreign_loop_error[0], ConnectionError)
    mock_redis.publish.assert_not_called()

    # The connection is still healthy for its owning (main) loop afterwards.
    receivers = await redis_ipc.publish("hermes_agent_commands", {"key": "tradier_last_ok_ts"})
    assert receivers == 1
    mock_redis.publish.assert_awaited_once()


