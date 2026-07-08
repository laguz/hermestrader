import pytest
import logging
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
import asyncio

from hermes.service1_agent.core import MoneyManager, TradeAction
from hermes.service1_agent.strategies._credit_spread_base import AbstractStrategy, CreditSpreadStrategy
from hermes.portfolio.safety_gateway import SafetyGateway
from hermes.ml.ledger import backfill_prediction_outcomes
from hermes.ml.regime_weights import CachedRegimeWeightsLookup
from hermes.service1_agent.agent_reactive import prewarm_quote_chain_cache
from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
from hermes.service1_agent.strategies.hermes_alpha import HermesAlpha
from hermes.service1_agent.strategies.tt45 import TastyTrade45
from hermes.broker.mock_stream import MockStreamClient
from hermes.broker.tradier_stream import TradierStreamClient
from hermes.ipc import RedisIPCBackend
from hermes.service2_watcher.routes.status import get_status
from hermes.service1_agent.overseer import HermesOverseer

# Helper stub/mock classes
class DummyDB:
    def __init__(self):
        self.settings = AsyncMock()
        self.watchlist = AsyncMock()
        self.trades = AsyncMock()
        self.logs = AsyncMock()
        self.approvals = AsyncMock()
        self.AsyncSession = MagicMock()

class DummyBroker:
    def __init__(self):
        self.current_date = datetime(2025, 6, 1, tzinfo=timezone.utc)

@pytest.mark.asyncio
async def test_bug_1_money_manager_zero_price():
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL250620P00150000", "quantity": 1}],
        price=0.0, side="sell", quantity=1, width=0.0
    )
    mm = MoneyManager(DummyBroker(), DummyDB(), {})
    res = await mm.optimize_allocation([action], 1000000.0)
    assert len(res) == 1

def test_bug_2_credit_spread_base_zero_lots():
    class DummyCS(CreditSpreadStrategy):
        NAME = "CS75"
        PRIORITY = 1
        def execute_entries(self, watchlist): return []
        def manage_positions(self): return []

    strategy = DummyCS(DummyBroker(), DummyDB(), None, None, {})
    detailed_wl = {"AAPL": {"target_lots": 0}}
    sym, lots = strategy._parse_symbol("AAPL", detailed_wl, 5)
    assert sym == "AAPL"
    assert lots == 0

@pytest.mark.asyncio
async def test_bug_3_commands_publish_warn(caplog):
    from hermes.db.repositories.commands import CommandsRepository
    db = MagicMock()
    session_mock = AsyncMock()
    db.AsyncSession.return_value = session_mock
    repo = CommandsRepository(db)
    
    with patch("hermes.ipc.ipc.publish", side_effect=Exception("Redis down")), caplog.at_level(logging.WARNING):
        await repo.enqueue_command("TEST", {})
        assert any("Failed to publish agent command drain event to IPC" in r.message for r in caplog.records)

def test_bug_4_safety_gateway_zero_lots():
    sg = SafetyGateway({"safety_max_symbol_trades": 5, "safety_max_symbol_exposure_ratio": 0.5})
    symbol_open_trades = [{"symbol": "AAPL", "width": 5.0, "entry_credit": 1.0, "lots": 0}]
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL250620P00150000", "quantity": 1}],
        price=1.0, side="sell", quantity=1, width=5.0
    )
    report = sg.validate_action(action, {"option_buying_power": 10000.0}, symbol_open_trades)
    assert report.metrics["total_symbol_risk"] == 400.0

@pytest.mark.asyncio
async def test_bug_5_engine_pipeline_zero_quantity():
    from hermes.service1_agent._engine_pipeline import PipelineController
    ctx = MagicMock()
    ctx.db = DummyDB()
    engine_mock = MagicMock()
    engine_mock.ctx = ctx
    engine = PipelineController(engine_mock)
    order = {"quantity": 0, "id": "123", "side": "sell", "status": "filled", "tag": "HERMES_CS75", "symbol": "AAPL", "option_symbol": "AAPL250620P00150000"}
    
    mock_record = AsyncMock()
    ctx.db.trades.record_order_response = mock_record
    
    # Pass orphans and orders directly to _adopt_orphans
    await engine._adopt_orphans({"AAPL250620P00150000"}, [order])
    assert mock_record.call_count == 1
    action = mock_record.call_args[0][0]
    assert action.quantity == 0

@pytest.mark.asyncio
async def test_bug_6_ledger_zero_horizon():
    db = DummyDB()
    row = MagicMock()
    row.horizon_dte = 0
    row.ts = datetime(2026, 7, 1, tzinfo=timezone.utc)
    row.symbol = "AAPL"
    row.realized_outcome = None
    
    async def mock_execute(*args, **kwargs):
        res = MagicMock()
        res.scalars.return_value.all.return_value = [row]
        return res

    session = AsyncMock()
    db.AsyncSession.return_value = session
    session.__aenter__.return_value.execute = mock_execute
    db.daily_bars = AsyncMock(side_effect=Exception("DB error"))
    
    await backfill_prediction_outcomes(db, lookback_days=10)
    db.daily_bars.assert_called_once()

@pytest.mark.asyncio
async def test_bug_7_regime_weights_lookup_warn(caplog):
    db = DummyDB()
    lookup_fn = CachedRegimeWeightsLookup(db, event_bus=None)
    with patch("hermes.ml.regime_weights.lookup", side_effect=Exception("DB Down")), caplog.at_level(logging.WARNING):
        res = lookup_fn("3M", "AAPL")
        assert any("Failed to lookup regime weights" in r.message for r in caplog.records)
        assert res == [0.0, 1.0, 0.6, 0.3, 0.4]

@pytest.mark.asyncio
async def test_bug_8_agent_reactive_ml_warn(caplog):
    db = DummyDB()
    db.settings.set_setting.side_effect = Exception("DB settings failure")
    from hermes.service1_agent.agent_reactive import handle_ipc_command
    event_bus = MagicMock()
    with caplog.at_level(logging.WARNING):
        await handle_ipc_command({"action": "trigger_ml"}, MagicMock(), db, {}, event_bus, MagicMock())
        assert any("Failed to set ml_force_run setting reactively" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_9_broker_wrapper_write_log_warn(caplog):
    broker = DummyBroker()
    broker.place_order_from_action = AsyncMock(return_value={})
    db = DummyDB()
    db.logs.write_log.side_effect = Exception("Logs write failure")
    wrapper = AsyncBrokerWrapper(broker, db)
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL250620P00150000", "quantity": 1}],
        price=1.0, side="sell", quantity=1
    )
    with patch("hermes.portfolio.safety_gateway.SafetyGateway") as mock_sg:
        inst = mock_sg.return_value
        inst.check_limits.return_value = ([], {"decision": "APPROVED", "metrics": {}, "violations": []})
        with caplog.at_level(logging.WARNING):
            await wrapper.place_order_from_action(action)
            assert any("Failed to write Safety Gateway decision log to DB" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_10_hermes_alpha_zero_dte():
    db = DummyDB()
    broker = MagicMock()
    broker.get_option_chains = AsyncMock(return_value=[])
    broker.current_date = datetime(2025, 6, 1)
    strategy = HermesAlpha(broker, db, None, None, {})
    intent = {"side": "put", "target_delta": 0.16, "dte_min": 0, "dte_max": 0, "width": 5}
    with patch.object(strategy, "find_expiry_in_dte_range", new_callable=AsyncMock) as mock_expiry:
        mock_expiry.return_value = "2025-06-20"
        await strategy._build_from_intent("AAPL", intent, default_width=5, target_lots=5)
        mock_expiry.assert_called_once_with("AAPL", 0, 0, prefer="max")

@pytest.mark.asyncio
async def test_bug_11_money_manager_zero_delta():
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL250620P00150000", "quantity": 1}],
        price=1.0, side="sell", quantity=1,
        strategy_params={"delta": 0.0}
    )
    mm = MoneyManager(DummyBroker(), DummyDB(), {})
    res = await mm.optimize_allocation([action], 1000.0)
    assert len(res) == 1

@pytest.mark.asyncio
async def test_bug_12_tt45_zero_delta():
    db = DummyDB()
    broker = DummyBroker()
    chain = [
        {"symbol": "AAPL_SL", "strike": 150.0, "greeks": {"delta": 0.0}, "option_type": "put", "bid": 1.0, "ask": 1.1},
        {"symbol": "AAPL_LL", "strike": 145.0, "greeks": {"delta": -0.10}, "option_type": "put", "bid": 0.5, "ask": 0.6}
    ]
    broker.get_option_chains = AsyncMock(return_value=chain)
    
    ic_builder = MagicMock()
    strategy = TastyTrade45(broker, db, None, ic_builder, {})
    
    tunables = MagicMock()
    tunables.tt45_width = 5
    tunables.tt45_min_dte = 30
    tunables.tt45_max_dte = 45
    tunables.tt45_delta = 0.16
    tunables.tt45_delta_tol = 0.05
    strategy.load_tunables = AsyncMock(return_value=tunables)
    strategy.today = MagicMock(return_value=datetime(2025, 6, 1).date())
    strategy.find_active_ic_expiry = AsyncMock(return_value=None)
    strategy.find_expiry_in_dte_range = AsyncMock(return_value="2025-06-20")
    db.trades.open_legs = AsyncMock(return_value=[])
    
    with patch.object(strategy, "find_strike_by_delta", new_callable=AsyncMock) as mock_find_strike:
        mock_find_strike.return_value = {"symbol": "AAPL_SL", "strike": 150.0, "greeks": {"delta": 0.0}, "bid": 1.0, "ask": 1.1}
        
        await strategy.execute_entries(["AAPL"])
        assert ic_builder.plan.call_count == 1
        
        put_factory = ic_builder.plan.call_args[1]["put_action_factory"]
        action = await put_factory("AAPL", "2025-06-20", lots=1, width=5)
        assert action is not None
        assert action.strategy_params["short_delta"] == 0.0

@pytest.mark.asyncio
async def test_bug_13_engine_reactive_zero_avg_fill_price():
    from hermes.service1_agent._engine_reactive import ReactiveController
    
    broker_order = {
        "id": "123",
        "status": "filled",
        "avg_fill_price": 0.0,
        "price": 1.5,
        "symbol": "AAPL",
        "side": "sell",
        "quantity": 1
    }
    
    broker = MagicMock()
    broker.get_orders = AsyncMock(return_value=[broker_order])
    
    ctx = MagicMock()
    ctx.broker = broker
    ctx.event_bus = MagicMock()
    
    engine_mock = MagicMock()
    engine_mock.ctx = ctx
    
    engine = ReactiveController(engine_mock)
    engine._tracked_orders["123"] = {"symbol": "AAPL", "side": "sell", "quantity": 1}
    
    with patch("asyncio.sleep", side_effect=asyncio.CancelledError):
        with patch.object(ctx.event_bus, "emit") as mock_emit:
            try:
                await engine._order_monitor_loop()
            except asyncio.CancelledError:
                pass
            assert mock_emit.call_count == 1
            event = mock_emit.call_args[0][0]
            assert event.price == 0.0

@pytest.mark.asyncio
async def test_bug_14_mock_stream_db_err_debug(caplog):
    db = DummyDB()
    db.watchlist.all_watchlist_symbols.side_effect = Exception("Query failed")
    client = MockStreamClient(event_bus=MagicMock(), watchlist=["SPY"], db=db)
    with patch("asyncio.sleep", side_effect=asyncio.CancelledError):
        client._running = True
        with caplog.at_level(logging.DEBUG):
            await client._run_loop()
            assert any("Failed to query database for streaming symbols" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_15_tradier_stream_ws_close_debug(caplog):
    client = TradierStreamClient("token", "acc", "url", MagicMock(), [])
    client._ws = MagicMock()
    client._ws.close.side_effect = Exception("WS Close Failure")
    with caplog.at_level(logging.DEBUG):
        await client.stop()
        assert any("Failed to close websocket connection" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_16_18_ipc_close_debug(caplog):
    ipc = RedisIPCBackend("redis://localhost")
    ipc.client = MagicMock()
    ipc.client.aclose.side_effect = Exception("aclose fail")
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(ConnectionError):
            with patch("redis.asyncio.from_url", side_effect=Exception("Redis connection error")):
                await ipc.connect()
        assert any("Failed to close Redis client during connect error recovery" in r.message for r in caplog.records)

    with caplog.at_level(logging.DEBUG):
        ipc.client = MagicMock()
        ipc.client.aclose.side_effect = Exception("aclose fail")
        await ipc.disconnect()
        assert any("Failed to close Redis client during disconnect" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_19_agent_reactive_watchlist_err_debug(caplog):
    db = DummyDB()
    db.watchlist.list_all_watchlists.side_effect = Exception("Watchlist DB error")
    with caplog.at_level(logging.DEBUG):
        await prewarm_quote_chain_cache(MagicMock(), db, {}, asyncio.Event())
        assert any("Failed to list watchlists during quote warm-up" in r.message for r in caplog.records)

def test_bug_20_broker_wrapper_dt_err_debug(caplog):
    wrapper = AsyncBrokerWrapper(MagicMock(), None)
    broker = MagicMock()
    bad_dt = MagicMock(spec=datetime)
    bad_dt.timestamp.side_effect = Exception("Bad timestamp call")
    wrapper.broker = broker
    wrapper.broker.current_date = bad_dt
    with caplog.at_level(logging.DEBUG):
        wrapper._get_current_timestamp()
        assert any("Failed to extract timestamp from broker current_date" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_21_routes_status_json_err_warn(caplog):
    db = DummyDB()
    db.settings.get_setting_async = AsyncMock(return_value="invalid-json")
    db.logs.latest_log_ts_async = AsyncMock(return_value=None)
    db.approvals.list_approvals_async = AsyncMock(return_value=[])
    
    with caplog.at_level(logging.WARNING):
        with patch("hermes.service2_watcher.routes.status.db", db):
            await get_status()
            assert any("Failed to parse update_status setting" in r.message for r in caplog.records)

def test_bug_22_conftest_dispose_err_warn(caplog):
    from tests.conftest import _safe_dispose_async_engine
    engine = MagicMock()
    engine.dispose.side_effect = Exception("Engine dispose error")
    with caplog.at_level(logging.WARNING):
        _safe_dispose_async_engine(engine)
        assert any("Failed to dispose async engine" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_bug_23_25_overseer_debug_logs(caplog):
    overseer = HermesOverseer(None, DummyDB())
    with caplog.at_level(logging.DEBUG):
        overseer._safe_json("```json\n{invalid}\n```")
        assert any("Failed parsing json in code blocks" in r.message for r in caplog.records)
    with caplog.at_level(logging.DEBUG):
        overseer._safe_json("{invalid}")
        assert any("Failed parsing json in embedded section" in r.message for r in caplog.records)
    overseer.vision_enabled = True
    overseer.chart_provider = MagicMock()
    overseer.chart_provider.snapshot.side_effect = Exception("Vision failure")
    with caplog.at_level(logging.DEBUG):
        await overseer._consult_single(TradeAction("CS75", "AAPL", "option", [], 1.0, "sell"))
        assert any("Failed fetching chart snapshot" in r.message for r in caplog.records)

def test_bug_26_engine_reactive_deserialize_timestamp_debug(caplog):
    from hermes.service1_agent._engine_reactive import ReactiveController
    engine = ReactiveController(MagicMock())
    v = {"__event_class__": "ClockTickEvent", "timestamp": "bad-date-format"}
    with caplog.at_level(logging.DEBUG):
        engine._deserialize_value(v)
        assert any("Failed to deserialize timestamp" in r.message for r in caplog.records)
