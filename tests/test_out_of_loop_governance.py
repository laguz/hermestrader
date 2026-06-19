from __future__ import annotations
from ._stubs import alias_db_namespaces

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.service1_agent.agent_settings import _read_overseer_settings
from hermes.service1_agent.control_state import ControlState
from hermes.service1_agent.core import CascadingEngine, TradeAction
from hermes.service1_agent.overseer import HermesOverseer


@pytest.mark.anyio
async def test_llm_out_of_loop_defaults_true_when_unset():
    """The safe posture — LLM off the synchronous critical path — must be the
    default when the operator has never set ``llm_out_of_loop``.

    Both operational readers feed this value into the engine: ``main.py`` builds
    and reconciles the engine from ``_read_overseer_settings`` (main.py:266/358),
    and ``ControlState.load_from_db`` mirrors it for the reactive runtime. If a
    refactor ever flips either default to False, review would silently move back
    onto the synchronous submit loop. These assertions pin the default so that
    regression is caught here rather than in production.
    """
    # _read_overseer_settings: every setting absent → llm_out_of_loop True.
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    db_mock.get_setting = AsyncMock(return_value=None)
    cfg = await _read_overseer_settings(db_mock, conf={})
    assert cfg["llm_out_of_loop"] is True

    # ControlState.load_from_db: same default, even after being forced False.
    cs_db = AsyncMock()
    alias_db_namespaces(cs_db)
    cs_db.get_settings = AsyncMock(return_value={})
    cs_db.get_setting = AsyncMock(return_value=None)
    cs = ControlState()
    cs.llm_out_of_loop = False
    await cs.load_from_db(cs_db, conf={})
    assert cs.llm_out_of_loop is True


@pytest.mark.anyio
async def test_out_of_loop_bypasses_review():
    # 1. Setup engine with llm_out_of_loop = True
    broker_mock = MagicMock()
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    db_mock.active_veto.return_value = None
    strategy_mock = MagicMock()
    strategy_mock.PRIORITY = 1
    strategy_mock.mm = None
    
    overseer_mock = MagicMock()
    event_bus_mock = MagicMock()
    
    engine = CascadingEngine(
        broker=broker_mock,
        db=db_mock,
        strategies=[strategy_mock],
        overseer=overseer_mock,
        approval_mode=False,
        event_bus=event_bus_mock,
        llm_out_of_loop=True
    )
    
    # 2. Mock execute sink
    engine._execute_or_queue = AsyncMock()
    
    # 3. Create dummy TradeAction
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[],
        price=1.50,
        side="sell",
        quantity=1,
        order_type="credit"
    )
    
    # 4. Call submit under mocked should_block_trades
    with patch("hermes.market_hours.should_block_trades", return_value=(False, "")):
        await engine.submit([action], action_type="entry")
    
    # 5. Verify direct execution without review request event or LLM calls
    engine._execute_or_queue.assert_called_once_with(action, "entry")
    event_bus_mock.emit.assert_not_called()
    overseer_mock.review.assert_not_called()


@pytest.mark.anyio
async def test_risk_restrictions_filter_watchlist():
    # 1. Setup engine with llm_out_of_loop = True
    broker_mock = MagicMock()
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    strategy_mock = MagicMock()
    strategy_mock.PRIORITY = 1
    strategy_mock.mm = None
    
    engine = CascadingEngine(
        broker=broker_mock,
        db=db_mock,
        strategies=[strategy_mock],
        llm_out_of_loop=True
    )
    
    # 2. Mock all tick helper steps
    engine.sync_positions = AsyncMock()
    engine.reconcile_orphans = AsyncMock()
    engine.process_management = AsyncMock(return_value=[])
    engine.submit = AsyncMock()
    engine.process_entries = AsyncMock(return_value=0)
    engine.ai._maybe_tune_parameters = AsyncMock()
    
    # Mock banned symbols read to return AAPL and TSLA
    engine._read_banned_symbols = AsyncMock(return_value={"AAPL", "TSLA"})
    
    # 3. Tick with watchlist
    watchlist = ["AAPL", "MSFT", "TSLA"]
    await engine.tick(watchlist)
    
    # 4. Verify only MSFT passed through to process_entries
    engine.process_entries.assert_called_once_with(["MSFT"])


@pytest.mark.anyio
async def test_propose_risk_restrictions():
    # 1. Setup overseer with mocked db and llm
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    db_mock.list_all_watchlists.return_value = {"default": ["AAPL", "MSFT"]}
    db_mock.recent_logs.return_value = "AAPL looks volatile today"
    db_mock.get_setting.return_value = ""
    db_mock.get_strategy_performance_metrics.return_value = {}
    
    llm_mock = MagicMock()
    llm_mock.chat.return_value = '{"banned_symbols": ["AAPL", "GOOG"], "rationale": "High earnings risk on AAPL"}'
    
    overseer = HermesOverseer(
        llm_client=llm_mock,
        db=db_mock,
        autonomy="enforcing"
    )
    
    # 2. Call risk restrictions proposals
    res = await overseer.propose_risk_restrictions()
    
    # 3. Verify return and database writes
    # GOOG should be filtered out since it's not in the watchlist. Only AAPL is banned.
    assert res["banned_symbols"] == ["AAPL"]
    assert "High earnings risk on AAPL" in res["rationale"]
    
    db_mock.set_setting.assert_called_once_with("banned_symbols", "AAPL")
    db_mock.write_log.assert_called_once()
    db_mock.write_ai_decision.assert_called_once()


@pytest.mark.anyio
async def test_propose_risk_restrictions_advisory_is_noop():
    """Banning a symbol mutates live settings — a governance action reserved for
    enforcing/autonomous. Advisory must short-circuit before touching the LLM or
    the settings store, the same safety boundary as the parameter tuner."""
    db_mock = AsyncMock()
    alias_db_namespaces(db_mock)
    llm_mock = MagicMock()

    overseer = HermesOverseer(
        llm_client=llm_mock,
        db=db_mock,
        autonomy="advisory",
    )

    res = await overseer.propose_risk_restrictions()

    assert res["banned_symbols"] == []
    db_mock.set_setting.assert_not_called()
    llm_mock.chat.assert_not_called()
