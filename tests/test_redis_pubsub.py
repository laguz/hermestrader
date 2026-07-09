from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.ipc import AsyncIPC


@pytest.mark.asyncio
async def test_redis_pubsub_loop_receives_notify():
    """Verify that AsyncIPC processes Redis pubsub messages and triggers subscribers."""
    # Mock Redis client and pubsub
    mock_redis = MagicMock()  # Synchronous pubsub() method requires MagicMock
    mock_pubsub = MagicMock()
    mock_redis.pubsub.return_value = mock_pubsub
    mock_redis.ping = AsyncMock()
    
    # Mock pubsub.listen() as an async generator
    async def mock_listen():
        yield {
            "type": "message",
            "channel": "agent_commands",
            "data": '{"action": "trigger_approvals"}'
        }
        # Wait until cancelled to prevent infinite loop
        await asyncio.sleep(100)
        
    mock_pubsub.listen = MagicMock(side_effect=mock_listen)
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.close = AsyncMock()
    
    # Patch from_url to return our mock_redis
    with patch("redis.asyncio.from_url", return_value=mock_redis):
        ipc = AsyncIPC("redis://localhost:6379/0")
        
        # Force connection via bypass_pytest_check=True
        connected = await ipc.connect(bypass_pytest_check=True)
        assert connected is True
        assert ipc.is_connected
        
        received_messages = []
        async def callback(data: dict):
            received_messages.append(data)
            
        await ipc.subscribe("agent_commands", callback)
        
        # Give the listener loop a moment to run
        await asyncio.sleep(0.2)
        
        # Verify that callback was called and message was received
        assert len(received_messages) == 1
        assert received_messages[0] == {"action": "trigger_approvals"}
        
        # Clean up
        await ipc.disconnect()


@pytest.mark.asyncio
async def test_decide_approval_emits_publish(db):
    """Verify that decide_approval publishes to IPC agent_commands when decision is made."""
    # Mock AsyncSession context to capture executes
    session_mock = AsyncMock()
    session_mock.add = MagicMock()
    session_mock.__aenter__.return_value = session_mock
    session_mock.__aexit__.return_value = False
    
    # Patch the AsyncSession instance context returned in decide_approval
    with patch.object(db, "AsyncSession", return_value=session_mock):
        # We need mock row return for select query
        result_mock = MagicMock()
        row_mock = MagicMock()
        row_mock.status = "PENDING"
        row_mock.notes = None
        result_mock.scalars.return_value.first.return_value = row_mock
        session_mock.execute.return_value = result_mock
        
        # Patch the global ipc instance at its source module
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            
            # Call decide_approval
            ok = await db.approvals.decide_approval(approval_id=42, decision="APPROVED")
            
            assert ok is True
            # Verify ipc.publish was called with correct channel and payload containing decided event
            mock_ipc.publish.assert_called_once()
            args, _ = mock_ipc.publish.call_args
            assert args[0] == "agent_commands"
            assert args[1]["event_type"] == "APPROVAL_DECIDED"
            assert args[1]["payload"]["approval_id"] == 42
            assert args[1]["payload"]["status"] == "APPROVED"
