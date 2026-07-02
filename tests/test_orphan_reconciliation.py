"""reconcile_orphans() must adopt Hermes-tagged fills whose Trade row never
got written (regression for a broker position sitting invisible to every
strategy's TP/SL/time-exit — see AAPL Aug-7 270/275 put investigation), while
leaving genuine manual/foreign positions untouched.
"""
import pytest
from unittest.mock import AsyncMock, patch

from hermes.service1_agent.core import CascadingEngine
from ._stubs import StubDB, StubBroker


@pytest.mark.asyncio
async def test_orphan_with_hermes_tag_is_adopted_into_a_trade():
    db = StubDB()
    broker = StubBroker(
        positions=[
            {"symbol": "AAPL260807P00270000", "quantity": -1, "cost_basis": 0.0},
            {"symbol": "AAPL260807P00275000", "quantity": 1, "cost_basis": 0.0},
        ],
        orders=[
            {
                "id": "ord-9001",
                "symbol": "AAPL",
                "status": "filled",
                "tag": "HERMES-CS75",  # Tradier's on-wire _ -> - rewrite
                "price": 1.30,
                "quantity": 1,
                "side": "sell",
                "leg": [
                    {"option_symbol": "AAPL260807P00275000", "side": "sell_to_open", "quantity": 1},
                    {"option_symbol": "AAPL260807P00270000", "side": "buy_to_open", "quantity": 1},
                ],
            }
        ],
    )
    engine = CascadingEngine(broker=broker, db=db, strategies=[], approval_mode=False)

    with patch.object(db.trades, "record_order_response", new_callable=AsyncMock) as mock_record:
        await engine.reconcile_orphans()

    mock_record.assert_awaited_once()
    action, resp = mock_record.await_args.args
    assert action.strategy_id == "CS75"
    assert action.symbol == "AAPL"
    # Expiry must be recovered from the OCC leg symbols (2026-08-07), not left
    # NULL — risk_engine's capacity counting filters Trade rows by expiry, so
    # a NULL expiry would let CS75 re-enter the same expiry as if it were free.
    assert action.expiry == "2026-08-07"
    leg_symbols = {leg["option_symbol"] for leg in action.legs}
    assert leg_symbols == {"AAPL260807P00270000", "AAPL260807P00275000"}
    assert resp["order"]["status"] == "filled"

    adopted_logs = [t for t in db.logs if "[ORPHAN ADOPTED]" in t]
    assert len(adopted_logs) == 1
    assert "AAPL" in adopted_logs[0]

    # Nothing left to flag as a plain orphan — both legs were adopted.
    assert not any("orphan position" in t for t in db.logs)


@pytest.mark.asyncio
async def test_orphan_without_hermes_tag_is_left_alone_and_flagged():
    db = StubDB()
    broker = StubBroker(
        positions=[{"symbol": "TSLA260807C00300000", "quantity": -1, "cost_basis": 0.0}],
        orders=[
            {
                "id": "ord-manual-1",
                "symbol": "TSLA",
                "status": "filled",
                "tag": "",  # no Hermes tag -> operator's own manual trade
                "price": 2.00,
                "quantity": 1,
                "side": "sell",
                "leg": [{"option_symbol": "TSLA260807C00300000", "side": "sell_to_open", "quantity": 1}],
            }
        ],
    )
    engine = CascadingEngine(broker=broker, db=db, strategies=[], approval_mode=False)

    with patch.object(db.trades, "record_order_response", new_callable=AsyncMock) as mock_record:
        await engine.reconcile_orphans()

    mock_record.assert_not_awaited()
    assert any("orphan position: TSLA260807C00300000" in t for t in db.logs)


@pytest.mark.asyncio
async def test_orphan_with_no_matching_order_is_flagged_not_adopted():
    db = StubDB()
    broker = StubBroker(
        positions=[{"symbol": "IWM260707P00292000", "quantity": -1, "cost_basis": 0.0}],
        orders=[],  # order history doesn't go back far enough to find the fill
    )
    engine = CascadingEngine(broker=broker, db=db, strategies=[], approval_mode=False)

    with patch.object(db.trades, "record_order_response", new_callable=AsyncMock) as mock_record:
        await engine.reconcile_orphans()

    mock_record.assert_not_awaited()
    assert any("orphan position: IWM260707P00292000" in t for t in db.logs)
