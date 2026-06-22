"""
Unit tests for the AsyncIPC messaging system.
"""
from __future__ import annotations

import asyncio
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


