"""
Unit tests for the AsyncIPC messaging system.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch
import pytest

from hermes.ipc import AsyncIPC

@pytest.mark.anyio
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

@pytest.mark.anyio
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
