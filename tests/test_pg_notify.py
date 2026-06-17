from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.db.models import HermesDB
from hermes.ipc import AsyncIPC


@pytest.mark.asyncio
async def test_pg_listen_loop_receives_notify():
    """Verify that AsyncIPC processes PostgreSQL notifications and triggers subscribers."""
    db_mock = MagicMock(spec=HermesDB)
    db_mock.async_engine = MagicMock()
    db_mock.async_engine.dialect.name = "postgresql"
    
    # Mock connection and context manager
    conn_mock = AsyncMock()
    ctx_mock = MagicMock()
    ctx_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx_mock.__aexit__ = AsyncMock(return_value=False)
    db_mock.async_engine.connect.return_value = ctx_mock
    
    # Mock the driver connection and notifies generator
    driver_conn_mock = MagicMock()
    fairy_mock = MagicMock()
    fairy_mock.driver_connection = driver_conn_mock
    conn_mock.get_raw_connection = AsyncMock(return_value=fairy_mock)
    
    # Fake a notify event generator
    fake_notify = MagicMock()
    fake_notify.channel = "agent_commands"
    fake_notify.payload = '{"action": "trigger_approvals"}'
    
    # An async generator to yield a single notification then block
    async def fake_notifies():
        yield fake_notify
        # Wait until cancelled to prevent infinite loop reconnecting in test
        await asyncio.sleep(100)

    driver_conn_mock.notifies = MagicMock(side_effect=fake_notifies)
    
    # Initialize and connect AsyncIPC
    ipc = AsyncIPC()
    
    # Force pytest bypass check to False to test real loop behavior
    connected = await ipc.connect(db_mock, bypass_pytest_check=True)
    assert connected is True
    
    received_messages = []
    async def callback(data: dict):
        received_messages.append(data)
        
    await ipc.subscribe("agent_commands", callback)
    
    # Wait for the generator to yield the notify
    await asyncio.sleep(0.2)
    
    # Verify that callback was called and message was received
    assert len(received_messages) == 1
    assert received_messages[0] == {"action": "trigger_approvals"}
    
    # Clean up
    await ipc.disconnect()


@pytest.mark.asyncio
async def test_decide_approval_emits_notify():
    """Verify that decide_approval executes NOTIFY query on agent_commands when database is PostgreSQL."""
    db = HermesDB("sqlite+aiosqlite:///:memory:")
    
    # Patch async_engine.dialect to pretend it is PostgreSQL
    with patch.object(db.async_engine.dialect, "name", "postgresql"):
        # Mock AsyncSession context to capture executes
        session_mock = AsyncMock()
        session_mock.__aenter__.return_value = session_mock
        session_mock.__aexit__.return_value = False
        
        # Patch the AsyncSession instance context returned in decide_approval
        with patch.object(db, "AsyncSession", return_value=session_mock):
            # We need mock row return for select query
            result_mock = MagicMock()
            row_mock = MagicMock()
            row_mock.status = "PENDING"
            result_mock.scalars.return_value.first.return_value = row_mock
            session_mock.execute.return_value = result_mock
            
            # Call decide_approval
            ok = await db.decide_approval(approval_id=42, decision="APPROVED")
            
            assert ok is True
            # Verify NOTIFY agent_commands text query was executed before commit
            calls = session_mock.execute.call_args_list
            assert len(calls) >= 2  # 1 for select, 1 for NOTIFY
            
            notify_called = False
            for call in calls:
                args, _ = call
                if len(args) > 0 and hasattr(args[0], "text") and "NOTIFY agent_commands" in args[0].text:
                    notify_called = True
                    break
            
            assert notify_called, "NOTIFY agent_commands query was not executed"
            session_mock.commit.assert_called_once()
