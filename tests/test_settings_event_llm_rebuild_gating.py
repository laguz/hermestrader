"""Regression tests for the 2026-07-16 LLM rebuild-on-heartbeat bug.

``_handle_settings_changed`` used to call ``_build_llm`` on *every*
SYSTEM_SETTING_CHANGED event and only compare config snapshots afterwards.
Status heartbeats whose value changes on each write — ``ml_last_ok_ts``,
rewritten by the MlRetrainTick cycle every 10 seconds — therefore rebuilt the
LLM client (and the engine strategy list) every 10s on the live paper agent,
spamming "LLM overseer: provider=..." logs continuously.

The fix gates the handler on ``LLM_CONFIG_KEYS`` / ``OVERSEER_CONFIG_KEYS``
(``agent_settings.py``): keys outside both sets are ignored, and only genuine
LLM config keys trigger a ``_build_llm``. The LLM status keys that
``_build_llm`` itself writes (llm_last_error, llm_active_provider,
llm_last_ok_ts) are deliberately outside ``LLM_CONFIG_KEYS`` so a rebuild
can't re-trigger the handler.
"""
from __future__ import annotations

from ._stubs import alias_db_namespaces

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes.service1_agent.main import _SHUTDOWN_EVENT, _TRIGGER_EVENT


@pytest.fixture(autouse=True)
def reset_events():
    _SHUTDOWN_EVENT.clear()
    _TRIGGER_EVENT.clear()
    yield
    _SHUTDOWN_EVENT.clear()
    _TRIGGER_EVENT.clear()


@asynccontextmanager
async def _running_agent():
    """Boot _run_async with the standard stubbed surroundings and yield
    (event_bus, build_llm_mock, engine_mock)."""
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    db_mock.get_setting.return_value = None
    db_mock.get_settings.return_value = {}
    db_mock.list_all_watchlists.return_value = {}

    broker_mock = MagicMock()
    broker_mock.dry_run = True
    llm_mock = MagicMock()

    with patch("hermes.service1_agent.main.HermesDB", return_value=db_mock), \
         patch("hermes.service1_agent.main._build_broker", return_value=broker_mock), \
         patch("hermes.service1_agent.main._build_llm",
               return_value=(llm_mock, {}, True)) as build_llm_mock, \
         patch("hermes.service1_agent.main._resolve_mode_credentials",
               return_value=("dummy_token", "dummy_account", "http://dummy")), \
         patch("hermes.service1_agent.main._read_overseer_settings",
               return_value={"paused": False, "approval_mode": True, "soul": "",
                             "autonomy": "advisory", "strategy_enabled": {},
                             "llm_out_of_loop": False}), \
         patch("hermes.service1_agent.main.build") as build_mock, \
         patch("hermes.service1_agent.main.market_session") as mkt_mock:

        engine_mock = MagicMock()
        engine_mock.overseer = None
        build_mock.return_value = engine_mock

        mkt_mock.return_value = {
            "trading_day": False, "is_open": False, "session": "closed",
            "et_date": "2026-07-16", "et_time": "12:00",
        }

        from hermes.service1_agent.main import _run_async
        conf = {"watchlist": [], "tick_interval_s": 100, "mode": "paper"}
        loop_task = asyncio.create_task(_run_async(chart_provider=None, conf=conf))
        await asyncio.sleep(0.2)

        event_bus = None
        for call in build_mock.call_args_list:
            if "event_bus" in call.kwargs:
                event_bus = call.kwargs["event_bus"]
        assert event_bus is not None

        try:
            yield event_bus, build_llm_mock, engine_mock
        finally:
            _SHUTDOWN_EVENT.set()
            await asyncio.wait_for(loop_task, timeout=2.0)


@pytest.mark.anyio
async def test_status_heartbeat_key_does_not_rebuild_llm():
    from hermes.db.events import SystemSettingChangedEvent

    async with _running_agent() as (event_bus, build_llm_mock, engine_mock):
        startup_calls = build_llm_mock.call_count
        for key in ("ml_last_ok_ts", "tradier_last_ok_ts", "llm_last_ok_ts",
                    "llm_last_error", "llm_active_provider", "obp_reserve"):
            event_bus.emit(SystemSettingChangedEvent(
                key=key, value="2026-07-16T12:00:00+00:00", updated_at="now"))
        await asyncio.sleep(0.3)

        assert build_llm_mock.call_count == startup_calls
        # The overseer-refresh tail must not have run either — approval_mode
        # is only assigned by the handler, so it stays an untouched MagicMock.
        assert not isinstance(engine_mock.approval_mode, bool)


@pytest.mark.anyio
async def test_llm_config_key_triggers_rebuild():
    from hermes.db.events import SystemSettingChangedEvent

    async with _running_agent() as (event_bus, build_llm_mock, engine_mock):
        startup_calls = build_llm_mock.call_count
        event_bus.emit(SystemSettingChangedEvent(
            key="llm_model", value="gemini-2.5-flash", updated_at="now"))
        await asyncio.sleep(0.3)

        assert build_llm_mock.call_count == startup_calls + 1
        assert engine_mock.approval_mode is True


@pytest.mark.anyio
async def test_dedicated_event_refreshes_overseer_without_llm_rebuild():
    from hermes.db.events import DoctrineUpdatedEvent

    async with _running_agent() as (event_bus, build_llm_mock, engine_mock):
        startup_calls = build_llm_mock.call_count
        event_bus.emit(DoctrineUpdatedEvent(doctrine_text="be careful", updated_at="now"))
        await asyncio.sleep(0.3)

        assert build_llm_mock.call_count == startup_calls
        assert engine_mock.approval_mode is True
