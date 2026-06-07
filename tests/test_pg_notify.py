from __future__ import annotations

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.db.models import HermesDB
from hermes.service1_agent.main import _pg_listen_loop, _TRIGGER_EVENT, _SHUTDOWN_EVENT


@pytest.fixture(autouse=True)
def reset_events():
    _TRIGGER_EVENT.clear()
    _SHUTDOWN_EVENT.clear()
    yield
    _TRIGGER_EVENT.clear()
    _SHUTDOWN_EVENT.clear()


@pytest.mark.anyio
async def test_pg_listen_loop_receives_notify():
    """Verify that _pg_listen_loop processes PostgreSQL notifications and sets the trigger event."""
    db_mock = MagicMock(spec=HermesDB)
    db_mock.async_engine = MagicMock()
    db_mock.async_engine.dialect.name = "postgresql"
    
    # Mock connection and context manager to prevent swallowing exceptions like CancelledError
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
    fake_notify.channel = "hermes_approvals"
    fake_notify.payload = "trigger_approvals"
    
    # An async generator to yield a single notification then block
    async def fake_notifies():
        yield fake_notify
        # Wait until cancelled to prevent infinite loop reconnecting in test
        await asyncio.sleep(100)

    driver_conn_mock.notifies = MagicMock(side_effect=fake_notifies)
    
    # Run loop as background task
    task = asyncio.create_task(_pg_listen_loop(db_mock, _TRIGGER_EVENT))
    
    # Wait for the generator to yield the notify
    await asyncio.sleep(0.2)
    
    # Verify that the trigger event was set immediately
    assert _TRIGGER_EVENT.is_set()
    
    # Clean up
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.anyio
async def test_decide_approval_emits_notify():
    """Verify that decide_approval executes NOTIFY query when database is PostgreSQL."""
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
            # Verify NOTIFY hermes_approvals text query was executed before commit
            calls = session_mock.execute.call_args_list
            assert len(calls) >= 2  # 1 for select, 1 for NOTIFY
            
            notify_called = False
            for call in calls:
                args, _ = call
                if len(args) > 0 and hasattr(args[0], "text") and "NOTIFY hermes_approvals" in args[0].text:
                    notify_called = True
                    break
            
            assert notify_called, "NOTIFY query was not executed"
            session_mock.commit.assert_called_once()
