"""Regression tests for the confirmed bugs from the 2026-07-04 DeepSeek audit
(verified by hand against the actual source before fixing)."""
from __future__ import annotations

from ._stubs import alias_db_namespaces

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from hermes.clock import SimulatedClock
from hermes.service1_agent.main import _SHUTDOWN_EVENT, _TRIGGER_EVENT
from hermes.service1_agent.strategies.hermes_alpha import HermesAlpha


@pytest.fixture(autouse=True)
def reset_events():
    _SHUTDOWN_EVENT.clear()
    _TRIGGER_EVENT.clear()
    yield
    _SHUTDOWN_EVENT.clear()
    _TRIGGER_EVENT.clear()


# --- Bug: utcnow_iso() bypassed the virtual clock ---------------------------
def test_utcnow_iso_honors_simulated_clock():
    import hermes.utils as utils
    from datetime import datetime

    real_clock = utils._GLOBAL_CLOCK
    try:
        utils._GLOBAL_CLOCK = SimulatedClock(datetime(2020, 1, 1, 12, 0, 0))
        assert utils.utcnow_iso() == "2020-01-01T12:00:00+00:00"
    finally:
        utils._GLOBAL_CLOCK = real_clock


# --- Bug: engine.overseer accessed without a None guard --------------------
@pytest.mark.anyio
async def test_settings_changed_handler_survives_none_overseer(caplog):
    from hermes.db.events import SystemSettingChangedEvent

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
         patch("hermes.service1_agent.main._build_llm", return_value=(llm_mock, {}, True)), \
         patch("hermes.service1_agent.main._resolve_mode_credentials",
               return_value=("dummy_token", "dummy_account", "http://dummy")), \
         patch("hermes.service1_agent.main._read_overseer_settings",
               return_value={"paused": False, "approval_mode": True, "soul": "",
                             "autonomy": "advisory", "strategy_enabled": {},
                             "llm_out_of_loop": False}), \
         patch("hermes.service1_agent.main.build") as build_mock, \
         patch("hermes.service1_agent.main.market_session") as mkt_mock:

        engine_mock = MagicMock()
        engine_mock.overseer = None  # the crashing case: no overseer configured
        build_mock.return_value = engine_mock

        mkt_mock.return_value = {
            "trading_day": False, "is_open": False, "session": "closed",
            "et_date": "2026-06-07", "et_time": "12:00",
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

        caplog.clear()
        # "overseer_mode" is in OVERSEER_CONFIG_KEYS, so the handler runs its
        # overseer-refresh tail (keys outside the config sets are ignored).
        event_bus.emit(SystemSettingChangedEvent(key="overseer_mode", value="single", updated_at="now"))
        await asyncio.sleep(0.2)

        # EventBus._safe_invoke swallows handler exceptions and logs an error —
        # before the fix, the None overseer access raised AttributeError there.
        assert not any("Error in handler" in rec.message for rec in caplog.records)
        # Positive signal the handler ran to completion past the overseer access.
        assert engine_mock.approval_mode is True

        _SHUTDOWN_EVENT.set()
        await asyncio.wait_for(loop_task, timeout=2.0)


# --- Bug: stream_client.stop() called while stream_client is still None ----
@pytest.mark.anyio
async def test_mode_change_handler_survives_none_stream_client(caplog):
    from hermes.db.events import ModeChangedEvent

    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    db_mock.get_setting.return_value = None
    db_mock.get_settings.return_value = {}
    db_mock.list_all_watchlists.return_value = {}
    db_mock.tracked_option_symbols.return_value = []

    broker_mock = MagicMock()
    broker_mock.dry_run = True
    llm_mock = MagicMock()

    with patch("hermes.service1_agent.main.HermesDB", return_value=db_mock), \
         patch("hermes.service1_agent.main._build_broker", return_value=broker_mock), \
         patch("hermes.service1_agent.main._build_llm", return_value=(llm_mock, {}, True)), \
         patch("hermes.service1_agent.main._resolve_mode_credentials",
               return_value=("dummy_token", "dummy_account", "http://dummy")), \
         patch("hermes.service1_agent.main._read_overseer_settings",
               return_value={"paused": False, "approval_mode": True, "soul": "",
                             "autonomy": "advisory", "strategy_enabled": {},
                             "llm_out_of_loop": False}), \
         patch("hermes.service1_agent.main.build") as build_mock, \
         patch("hermes.service1_agent.main.market_session") as mkt_mock:

        engine_mock = MagicMock()
        engine_mock.overseer = AsyncMock()
        build_mock.return_value = engine_mock

        # ModeChangedEvent is subscribed well before the module-level
        # `stream_client = _build_stream_client(...)` assignment further down
        # in _run_async's startup — stall one of the awaits in between
        # (the tracked_option_symbols query) to hold that window open and
        # fire the mode-change event while stream_client is still None.
        never_resolves = asyncio.Event()

        async def _slow_tracked_symbols():
            await never_resolves.wait()
            return []

        db_mock.tracked_option_symbols.side_effect = _slow_tracked_symbols

        mkt_mock.return_value = {
            "trading_day": False, "is_open": False, "session": "closed",
            "et_date": "2026-06-07", "et_time": "12:00",
        }

        from hermes.service1_agent.main import _run_async
        conf = {"watchlist": [], "tick_interval_s": 100, "mode": "paper"}
        loop_task = asyncio.create_task(_run_async(chart_provider=None, conf=conf))

        # Give _run_async's startup a moment to reach the (still-pending)
        # tracked_option_symbols() await and register the ModeChangedEvent
        # subscription — stream_client is still None at this point.
        await asyncio.sleep(0.2)

        event_bus = None
        for call in build_mock.call_args_list:
            if "event_bus" in call.kwargs:
                event_bus = call.kwargs["event_bus"]
        assert event_bus is not None

        caplog.clear()
        event_bus.emit(ModeChangedEvent(mode="live", updated_at="now"))
        await asyncio.sleep(0.2)

        # _handle_mode_change catches its own exceptions locally and logs via
        # "Mode switch to %s failed" — that's the pre-fix signal (calling
        # .stop() on the still-None stream_client raised AttributeError there).
        assert not any("Mode switch to" in rec.message and "failed" in rec.message
                        for rec in caplog.records)

        never_resolves.set()
        _SHUTDOWN_EVENT.set()
        await asyncio.wait_for(loop_task, timeout=2.0)


# --- Bug: hermes_alpha width=0 silently overridden by default_width -------
@pytest.mark.asyncio
async def test_hermes_alpha_build_from_intent_honors_explicit_zero_width():
    broker = MagicMock()
    broker.current_date = datetime(2025, 6, 1)
    chain = [
        {"symbol": "SHORT100", "option_type": "put", "strike": 100.0,
         "bid": 3.0, "ask": 3.2, "greeks": {"delta": 0.16}},
        {"symbol": "LONG95", "option_type": "put", "strike": 95.0,
         "bid": 1.0, "ask": 1.2, "greeks": {"delta": 0.05}},
    ]
    broker.get_option_chains = AsyncMock(return_value=chain)
    strategy = HermesAlpha(broker, AsyncMock(), None, None, {})

    intent = {"side": "put", "target_delta": 0.16, "dte_min": 0, "dte_max": 60, "width": 0}
    with patch.object(strategy, "find_expiry_in_dte_range", new_callable=AsyncMock) as mock_expiry:
        mock_expiry.return_value = "2025-07-20"
        action = await strategy._build_from_intent("AAPL", intent, default_width=5, target_lots=1)

    # width=0 means the long leg target == the short strike itself, so
    # nearest_strike resolves back to the short leg and the structure is
    # rejected. Before the fix, "or default_width" replaced 0 with 5 and a
    # (wider than instructed) spread would have been built successfully.
    assert action is None


# --- Bug: analytics.py exit_price falsy-zero fabricated P&L ---------------
@pytest.mark.asyncio
async def test_get_strategy_performance_metrics_skips_unknown_exit_price():
    from types import SimpleNamespace
    from hermes.db.repositories.analytics import AnalyticsRepository

    # A closed trade with no recorded exit fill (exit_price=None, pnl=None) —
    # `t.exit_price or 0.0` used to coerce this into a fabricated $200 P&L via
    # _compute_realized_pnl's exit_price=0.0 path instead of being skipped.
    trade = SimpleNamespace(
        strategy_id="CS75", pnl=None, exit_price=None, entry_debit=None,
        entry_credit=2.0, lots=1, short_strike=None, long_strike=None,
        width=None, symbol="AAPL", closed_at=None,
    )

    class _FakeScalars:
        def all(self):
            return [trade]

    class _FakeResult:
        def scalars(self):
            return _FakeScalars()

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def execute(self, *args, **kwargs):
            return _FakeResult()

    db = MagicMock()
    db.AsyncSession = lambda: _FakeSession()
    repo = AnalyticsRepository(db)

    metrics = await repo.get_strategy_performance_metrics()

    assert metrics["CS75"]["closed_trades"] == 0
    assert metrics["CS75"]["total_pnl"] == 0.0


# --- Bug: circuit breaker silently degrades when db is None ---------------
@pytest.mark.asyncio
async def test_circuit_breaker_logs_when_db_is_none(caplog):
    from hermes.broker.circuit_breaker import CircuitBreaker
    import logging

    cb = CircuitBreaker(failure_threshold=1)
    with caplog.at_level(logging.WARNING):
        await cb.record_failure(db=None, error_msg="boom")

    assert cb.state == "OPEN"
    assert any("NOT automatically paused" in r.message for r in caplog.records)


# --- Bug: timeseries reset_index collision when index and column are "ts" -
def test_normalize_for_write_handles_ts_index_and_column_collision():
    from hermes.db.timeseries import TimeSeriesEngine

    df = pd.DataFrame({"ts": [datetime(2025, 1, 1)], "open": [1.0]})
    df.index = pd.Index([datetime(2025, 1, 1)], name="ts")

    rows = TimeSeriesEngine._normalize_for_write("AAPL", df, ["open", "high", "low", "close", "volume"])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


# --- Bug: mcp/server.py forced dry_run=False outside "live" mode ----------
@pytest.mark.asyncio
async def test_mcp_server_honors_dry_run_setting_for_paper_mode():
    import hermes.mcp.server as mcp_server

    mcp_server._BROKERS.clear()
    settings_mock = MagicMock()
    settings_mock.hermes_mode = "paper"
    settings_mock.hermes_dry_run = True
    settings_mock.get_tradier_credentials.return_value = ("tok", "acct", "http://x")
    try:
        with patch("hermes.config.settings", settings_mock):
            broker = await mcp_server._broker()
        # Previously forced to False whenever mode != "live", weakening the
        # dry_run default even though the operator asked for dry_run=True.
        assert broker.dry_run is True
    finally:
        mcp_server._BROKERS.clear()


# --- Bug: OllamaCloudLLM.timeout_s never applied to the client ------------
def test_ollama_cloud_llm_applies_timeout_to_client():
    with patch("ollama.Client") as client_cls:
        from hermes.llm.clients import OllamaCloudLLM
        OllamaCloudLLM(model="gpt-oss:120b", api_key="key", timeout_s=7.0)
        _, kwargs = client_cls.call_args
        assert kwargs.get("timeout") == 7.0
