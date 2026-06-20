from __future__ import annotations
from ._stubs import alias_db_namespaces

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from hermes.service1_agent.agent_settings import _read_overseer_settings
from hermes.service1_agent.control_state import ControlState
from hermes.service1_agent.core import CascadingEngine, TradeAction


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
