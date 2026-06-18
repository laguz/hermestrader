from __future__ import annotations
from ._stubs import alias_db_namespaces

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.service1_agent.core import CascadingEngine, TradeAction
from hermes.service1_agent.overseer import HermesOverseer


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
    engine.tuning._maybe_tune_parameters = AsyncMock()
    
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
