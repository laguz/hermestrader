from __future__ import annotations

from datetime import datetime
import pytest

from hermes.db.models import PendingOrder, Trade, SystemSetting
from hermes.service1_agent.transaction_manager import TransactionManager
from hermes.db.events import EventStoreManager, OrderSubmittedEvent, OrderFilledEvent, CloseSubmittedEvent, CloseFilledEvent
from hermes.db.repositories.projections import ProjectionsRepository

# ``db`` / ``make_db`` fixtures (fresh throwaway Timescale DBs) come from
# tests/conftest.py.


@pytest.mark.asyncio
async def test_cqrs_event_sourcing_flow_and_replay(db, make_db):
    # Postgres enforces the trades.strategy_id FK; seed the registry first.
    await db.watchlist.ensure_strategies({"CS75": 1})
    # 1. Test placing an order (OrderSubmittedEvent)
    async with db.AsyncSession() as s:
        po = await TransactionManager.place_order(
            session=s,
            strategy_id="CS75",
            symbol="AAPL",
            side="sell",
            quantity=5,
            payload={"legs": [{"option_symbol": "AAPL260620P00150000", "quantity": 5}]}
        )
        await s.commit()

        assert po.id is not None
        assert po.status == "PENDING"

    # Verify order submitted event
    async with db.AsyncSession() as s:
        events = await EventStoreManager.load_events(s)
        assert len(events) == 1
        assert isinstance(events[0], OrderSubmittedEvent)
        assert events[0].id == po.id
        assert events[0].symbol == "AAPL"
        assert events[0].quantity == 5

    # 2. Test filling the order (OrderFilledEvent)
    trade_fields = {
        "strategy_id": "CS75",
        "symbol": "AAPL",
        "side_type": "put",
        "lots": 5,
        "entry_credit": 1.25,
        "opened_at": datetime.utcnow()
    }
    async with db.AsyncSession() as s:
        po_ret, trade = await TransactionManager.fill(
            session=s,
            strategy_id="CS75",
            symbol="AAPL",
            side="sell",
            lots=5,
            trade_fields=trade_fields
        )
        await s.commit()

        assert po_ret.status == "SUBMITTED"
        assert trade.id is not None
        assert trade.status == "OPEN"

    # Verify order filled event
    async with db.AsyncSession() as s:
        events = await EventStoreManager.load_events(s)
        assert len(events) == 2
        assert isinstance(events[1], OrderFilledEvent)
        assert events[1].pending_order_id == po.id
        assert events[1].trade_id == trade.id

    # 3. Test closing the position (CloseSubmittedEvent + CloseFilledEvent)
    async with db.AsyncSession() as s:
        # Fetch trade from database first to attach it to the session
        from sqlalchemy import select
        q = select(Trade).where(Trade.id == trade.id)
        res = await s.execute(q)
        db_trade = res.scalars().first()

        # Place the close pending order first
        po_close_pending = await TransactionManager.place_order(
            session=s,
            strategy_id="CS75",
            symbol="AAPL",
            side="buy",
            quantity=5,
            payload={"legs": [{"option_symbol": "AAPL260620P00150000", "quantity": 5}]}
        )
        await s.flush()

        po_close = await TransactionManager.close(
            session=s,
            strategy_id="CS75",
            symbol="AAPL",
            side="buy",
            lots=5,
            trade=db_trade,
            filled=True,
            exit_price=0.25,
            close_reason="TP",
            close_tag="TEST_CLOSE"
        )
        await s.commit()

    # Verify close events
    async with db.AsyncSession() as s:
        events = await EventStoreManager.load_events(s)
        # 1. OrderSubmitted (Entry)
        # 2. OrderFilled (Entry)
        # 3. OrderSubmitted (Close)
        # 4. CloseSubmitted
        # 5. CloseFilled
        assert len(events) == 5
        assert isinstance(events[2], OrderSubmittedEvent)
        assert isinstance(events[3], CloseSubmittedEvent)
        assert isinstance(events[4], CloseFilledEvent)
        assert events[3].trade_id == trade.id
        assert events[4].trade_id == trade.id

    # 4. Test Event Replay onto a clean database
    replay_db = make_db()
    await replay_db.watchlist.ensure_strategies({"CS75": 1})

    try:
        # Apply the loaded events to the clean replay database session
        async with replay_db.AsyncSession() as s_replay:
            for ev in events:
                await ProjectionsRepository.apply_event_projection(s_replay, ev)
            await s_replay.commit()

        # Query the replayed read models and assert they match original final state
        async with replay_db.AsyncSession() as s_replay:
            from sqlalchemy import select
            
            # Check PendingOrders
            q_po = select(PendingOrder).order_by(PendingOrder.id.asc())
            po_rows = (await s_replay.execute(q_po)).scalars().all()
            assert len(po_rows) == 2
            
            # Entry PendingOrder
            assert po_rows[0].id == po.id
            assert po_rows[0].status == "SUBMITTED"
            
            # Close PendingOrder
            assert po_rows[1].id == po_close_pending.id
            assert po_rows[1].status == "SUBMITTED"

            # Check Trades
            q_t = select(Trade).order_by(Trade.id.asc())
            t_rows = (await s_replay.execute(q_t)).scalars().all()
            assert len(t_rows) == 1
            assert t_rows[0].id == trade.id
            assert t_rows[0].status == "CLOSED"
            assert t_rows[0].exit_price == 0.25
            assert t_rows[0].close_reason == "TP"
            assert t_rows[0].pnl == (1.25 - 0.25) * 500.0  # pnl = (credit - debit) * 100 * lots

    finally:
        # The throwaway DB is disposed and dropped by the make_db fixture.
        pass
