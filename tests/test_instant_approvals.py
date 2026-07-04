from __future__ import annotations
from ._stubs import alias_db_namespaces

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.service1_agent.main import (
    _SHUTDOWN_EVENT,
    _TRIGGER_EVENT,
)


@pytest.fixture(autouse=True)
def reset_events():
    _SHUTDOWN_EVENT.clear()
    _TRIGGER_EVENT.clear()
    yield
    _SHUTDOWN_EVENT.clear()
    _TRIGGER_EVENT.clear()


@pytest.mark.anyio
async def test_agent_loop_runs_approvals_immediately_on_trigger():
    from hermes.events.bus import ClockTickEvent

    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    async def mock_get_setting(key, default=None):
        if key == "mode":
            return "paper"
        return None
    db_mock.get_setting.side_effect = mock_get_setting
    db_mock.get_settings.return_value = {}
    db_mock.fetch_approved_actions.return_value = []
    db_mock.tracked_option_symbols.return_value = []
    db_mock.list_all_watchlists.return_value = {}
    
    broker_mock = MagicMock()
    broker_mock.dry_run = True
    
    llm_mock = MagicMock()
    
    # Mock built-in modules/helpers inside main.py
    with patch("hermes.service1_agent.main.HermesDB", return_value=db_mock), \
         patch("hermes.service1_agent.main._build_broker", return_value=broker_mock), \
         patch("hermes.service1_agent.main._build_llm", return_value=(llm_mock, {}, True)), \
         patch("hermes.service1_agent.main._resolve_mode_credentials", return_value=("dummy_token", "dummy_account", "http://dummy")), \
         patch("hermes.service1_agent.main._read_overseer_settings", return_value={"paused": False, "approval_mode": True, "soul": "", "autonomy": "advisory", "strategy_enabled": {}, "llm_out_of_loop": False}), \
         patch("hermes.service1_agent.main.build") as build_mock, \
         patch("hermes.service1_agent.main.market_session") as mkt_mock, \
         patch("hermes.service1_agent.main.enforce_daily_loss_limit", return_value=False), \
         patch("hermes.service1_agent.main._execute_approved_action", new_callable=AsyncMock) as exec_mock:
         
        engine_mock = MagicMock()
        engine_mock.overseer = AsyncMock()
        build_mock.return_value = engine_mock
        
        # Wire handle_clock_tick subscription and execution simulation
        def mock_build(*args, **kwargs):
            eb = kwargs.get("event_bus")
            if eb:
                eb.subscribe(ClockTickEvent, engine_mock.handle_clock_tick)
            return engine_mock
        build_mock.side_effect = mock_build
        
        async def mock_handle_clock_tick(event):
            # Simulate real CascadingEngine clock tick approval execution
            actions = await db_mock.fetch_approved_actions()
            for item in actions:
                await exec_mock(item, broker=broker_mock, db=db_mock)
                
        engine_mock.handle_clock_tick = mock_handle_clock_tick
        
        mkt_mock.return_value = {
            "trading_day": False,
            "is_open": False,
            "session": "closed",
            "et_date": "2026-06-07",
            "et_time": "12:00"
        }
        
        # Start _run_async as a background task
        from hermes.service1_agent.main import _run_async
        conf = {"watchlist": [], "tick_interval_s": 100, "mode": "paper"}
        
        loop_task = asyncio.create_task(_run_async(chart_provider=None, conf=conf))
        
        # Let the loop initialize and enter sleep
        await asyncio.sleep(0.2)
        
        # Mock database return for approvals
        mock_approval_item = {"id": 123, "action_json": {}}
        db_mock.fetch_approved_actions.return_value = [mock_approval_item]
        
        # Reset exec_mock call count
        exec_mock.reset_mock()
        
        # Set the trigger event (as if watcher C2 approved a trade)
        _TRIGGER_EVENT.set()
        
        # Wait a moment for loop to wake up and process approvals
        await asyncio.sleep(0.2)
        
        # Verify it fetched and executed the action immediately
        db_mock.fetch_approved_actions.assert_called()
        exec_mock.assert_called_once_with(mock_approval_item, broker=broker_mock, db=db_mock)
        
        # Stop the agent loop cleanly
        _SHUTDOWN_EVENT.set()
        await asyncio.wait_for(loop_task, timeout=2.0)
