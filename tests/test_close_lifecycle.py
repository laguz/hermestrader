"""Close-lifecycle tests — trades finalize CLOSED on *fill*, not acceptance.

Regression cover for the TSLA orphan bug: marking a trade CLOSED the instant
Tradier *accepted* a close (before it filled, or when it was rejected async)
left the DB saying closed while the broker still held the position. Now a
close goes OPEN → CLOSING on acceptance, and the position-sync reconciler
(``upsert_positions``) finalizes CLOSED on broker-flat, or reopens the trade
if the close never took.

Uses a real Timescale-backed HermesDB (the ``db`` fixture from conftest.py).
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest  # noqa: F401

from hermes.db.models import Trade
from hermes.service1_agent.core import TradeAction


SHORT = "TSLA260717C00445000"
LONG = "TSLA260717C00450000"


async def _open_trade(db, *, lots=3, entry_credit=1.50, width=5.0) -> int:
    await db.watchlist.ensure_strategies({"CS75": 1})
    async with db.AsyncSession() as s:
        # Explicit id so the close actions below can target a known trade.
        t = Trade(id=1, strategy_id="CS75", symbol="TSLA", side_type="call",
                  short_leg=SHORT, long_leg=LONG, width=width, lots=lots,
                  entry_credit=entry_credit, expiry=date.today() + timedelta(days=40),
                  status="OPEN")
        s.add(t)
        await s.commit()
        return int(t.id)


def _close_action(trade_id: int, lots=3, price=0.40) -> TradeAction:
    return TradeAction(
        strategy_id="CS75", symbol="TSLA", order_class="multileg",
        legs=[{"option_symbol": SHORT, "side": "buy_to_close", "quantity": lots},
              {"option_symbol": LONG, "side": "sell_to_close", "quantity": lots}],
        price=price, side="buy", quantity=1, order_type="debit",
        tag="HERMES_CS75_CLOSE_AI",
        strategy_params={"trade_id": trade_id, "close_reason": "AI_CLOSE",
                         "side_type": "call"},
    )


async def _status(db, trade_id: int) -> str:
    # Trade has a composite PK (id, opened_at), so fetch by id via query.
    from sqlalchemy import select
    async with db.AsyncSession() as s:
        r = await s.execute(select(Trade).filter(Trade.id == trade_id))
        return r.scalars().first().status


# ── close_trade_from_action: accept vs fill ──────────────────────────────────
async def test_close_acceptance_marks_closing_not_closed(db):
    tid = await _open_trade(db)
    # Tradier accepted the order ("ok") but it has not filled.
    await db.trades.close_trade_from_action(_close_action(tid), {"order": {"id": "1", "status": "ok"}})
    assert await _status(db, tid) == "CLOSING"
    # Close economics are stashed for when it finalizes.
    from sqlalchemy import select
    async with db.AsyncSession() as s:
        row = (await s.execute(select(Trade).filter(Trade.id == tid))).scalars().first()
        assert row.close_reason == "AI_CLOSE"
        assert row.exit_price is not None
        assert row.pnl is not None
        assert row.closed_at is None


async def test_close_confirmed_fill_marks_closed(db):
    tid = await _open_trade(db)
    await db.trades.close_trade_from_action(_close_action(tid), {"order": {"id": "1", "status": "filled"}})
    assert await _status(db, tid) == "CLOSED"


async def test_close_rejection_leaves_trade_open(db):
    tid = await _open_trade(db)
    await db.trades.close_trade_from_action(_close_action(tid), {"errors": "bad price"})
    assert await _status(db, tid) == "OPEN"


# ── reconcile finalizes / reopens CLOSING ────────────────────────────────────
async def test_reconcile_finalizes_closing_when_broker_flat(db):
    tid = await _open_trade(db)
    await db.trades.close_trade_from_action(_close_action(tid), {"order": {"status": "ok"}})
    assert await _status(db, tid) == "CLOSING"
    # Broker now shows the legs flat → the close filled.
    await db.trades.upsert_positions([], active_order_legs=set())
    assert await _status(db, tid) == "CLOSED"
    from sqlalchemy import select
    async with db.AsyncSession() as s:
        row = (await s.execute(select(Trade).filter(Trade.id == tid))).scalars().first()
        assert row.close_reason == "AI_CLOSE"   # stashed reason preserved, not RECONCILED


async def test_reconcile_reopens_closing_when_still_held_no_resting(db):
    tid = await _open_trade(db)
    await db.trades.close_trade_from_action(_close_action(tid), {"order": {"status": "ok"}})
    # Close was rejected async: broker still holds the short, no resting order.
    await db.trades.upsert_positions(
        [{"symbol": SHORT, "quantity": -3}, {"symbol": LONG, "quantity": 3}],
        active_order_legs=set())
    assert await _status(db, tid) == "OPEN"


async def test_reconcile_keeps_closing_while_order_rests(db):
    tid = await _open_trade(db)
    await db.trades.close_trade_from_action(_close_action(tid), {"order": {"status": "ok"}})
    # Position still held AND a close order is resting → leave it CLOSING.
    await db.trades.upsert_positions(
        [{"symbol": SHORT, "quantity": -3}, {"symbol": LONG, "quantity": 3}],
        active_order_legs={SHORT, LONG})
    assert await _status(db, tid) == "CLOSING"


async def test_reconcile_flattens_open_when_broker_flat(db):
    # Existing behaviour preserved: an OPEN trade the bot never closed gets
    # flattened as RECONCILED_BROKER_FLAT when the broker shows it gone.
    tid = await _open_trade(db)
    await db.trades.upsert_positions([], active_order_legs=set())
    from sqlalchemy import select
    async with db.AsyncSession() as s:
        row = (await s.execute(select(Trade).filter(Trade.id == tid))).scalars().first()
        assert row.status == "CLOSED"
        assert row.close_reason == "RECONCILED_BROKER_FLAT"


async def test_tracked_symbols_include_closing(db):
    tid = await _open_trade(db)
    await db.trades.close_trade_from_action(_close_action(tid), {"order": {"status": "ok"}})
    tracked = await db.trades.tracked_option_symbols()
    # A mid-close position is still tracked, so it isn't misflagged as an orphan.
    assert SHORT in tracked and LONG in tracked
