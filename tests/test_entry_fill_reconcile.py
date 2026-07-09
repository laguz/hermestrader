"""Entry fill-price reconciliation.

``entry_credit``/``entry_debit`` are recorded at submission from the order's
*limit* price and were never updated with the broker's actual average fill —
so realized P&L and TP/SL management ran on the limit, not what the account
actually received. ``apply_entry_fill_price`` (called from the reactive
order-fill handler) reconciles them, band-guarded because a bad write here
changes live exit math:

- a credit can only fill at the limit or better (higher); a debit at the
  limit or better (lower) — fills through the wrong side are anomalies;
- an implausibly large "improvement" (beyond 1.5× / a $0.10 allowance) is
  treated as a data artifact (e.g. a per-leg price reported on a multileg
  order) and ignored;
- ``Trade.broker_order_id`` is written only by the entry path, so a closing
  order's fill can never match and overwrite the entry price.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from hermes.db.models import Trade
from hermes.events.bus import OrderFillEvent
from hermes.service1_agent.core import CascadingEngine

from ._stubs import StubBroker

pytestmark = pytest.mark.asyncio

SHORT = "TSLA260717P00440000"
LONG = "TSLA260717P00435000"


async def _seed_trade(db, *, order_id: str = "ORD-1", status: str = "OPEN",
                      entry_credit=0.24, entry_debit=None, trade_id: int = 1) -> None:
    await db.watchlist.ensure_strategies({"CS7": 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=trade_id, strategy_id="CS7", symbol="TSLA", side_type="put",
            short_leg=SHORT, long_leg=LONG, width=5.0, lots=1,
            entry_credit=entry_credit, entry_debit=entry_debit,
            expiry=date.today() + timedelta(days=7),
            status=status, broker_order_id=order_id,
        ))
        await s.commit()


async def _entry_credit(db, trade_id: int = 1):
    from sqlalchemy import select
    async with db.AsyncSession() as s:
        row = (await s.execute(select(Trade).filter(Trade.id == trade_id))).scalars().first()
    return (float(row.entry_credit) if row.entry_credit is not None else None,
            float(row.entry_debit) if row.entry_debit is not None else None)


# ── credit side ──────────────────────────────────────────────────────────────
async def test_improved_credit_fill_updates_entry_credit(db):
    await _seed_trade(db, entry_credit=0.24)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.26) is True
    credit, debit = await _entry_credit(db)
    assert credit == pytest.approx(0.26)
    assert debit is None


async def test_fill_below_credit_limit_is_rejected(db):
    await _seed_trade(db, entry_credit=0.24)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.20) is False
    credit, _ = await _entry_credit(db)
    assert credit == pytest.approx(0.24)


async def test_per_leg_junk_price_is_rejected(db):
    # Tradier reporting the short leg's own price (1.20) instead of the
    # spread's net credit (0.24 limit) must not corrupt the row.
    await _seed_trade(db, entry_credit=0.24)
    assert await db.trades.apply_entry_fill_price("ORD-1", 1.20) is False
    credit, _ = await _entry_credit(db)
    assert credit == pytest.approx(0.24)


async def test_fill_at_limit_is_a_noop(db):
    await _seed_trade(db, entry_credit=0.24)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.24) is False
    credit, _ = await _entry_credit(db)
    assert credit == pytest.approx(0.24)


# ── debit side ───────────────────────────────────────────────────────────────
async def test_improved_debit_fill_updates_entry_debit(db):
    await _seed_trade(db, entry_credit=None, entry_debit=0.40)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.35) is True
    credit, debit = await _entry_credit(db)
    assert credit is None
    assert debit == pytest.approx(0.35)


async def test_fill_above_debit_limit_is_rejected(db):
    await _seed_trade(db, entry_credit=None, entry_debit=0.40)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.55) is False
    _, debit = await _entry_credit(db)
    assert debit == pytest.approx(0.40)


# ── guards ───────────────────────────────────────────────────────────────────
async def test_zero_price_unknown_order_and_closed_trade_are_ignored(db):
    await _seed_trade(db, entry_credit=0.24)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.0) is False
    assert await db.trades.apply_entry_fill_price("ORD-1", None) is False
    assert await db.trades.apply_entry_fill_price("NO-SUCH-ORDER", 0.26) is False
    assert await db.trades.apply_entry_fill_price(None, 0.26) is False

    await _seed_trade(db, order_id="ORD-2", status="CLOSED",
                      entry_credit=0.24, trade_id=2)
    assert await db.trades.apply_entry_fill_price("ORD-2", 0.26) is False
    credit, _ = await _entry_credit(db, trade_id=2)
    assert credit == pytest.approx(0.24)


async def test_trade_without_recorded_limit_is_left_alone(db):
    await _seed_trade(db, entry_credit=None, entry_debit=None)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.26) is False


# ── handler wiring ───────────────────────────────────────────────────────────
async def test_order_fill_handler_reconciles_entry_price(db):
    """The reactive fill handler must apply the broker's avg fill to the
    trade row (every later step in the handler is fail-open, so the
    reconcile is asserted through the DB, end to end)."""
    await _seed_trade(db, entry_credit=0.24)
    engine = CascadingEngine(broker=StubBroker(), db=db, strategies=[],
                             event_bus=None)
    event = OrderFillEvent(broker_order_id="ORD-1", symbol="TSLA",
                           side="sell", quantity=1, price=0.27,
                           status="filled")
    await engine._handle_order_fill_internal(event)
    credit, _ = await _entry_credit(db)
    assert credit == pytest.approx(0.27)


async def test_monitor_missing_order_fallback_price_zero_does_not_clobber(db):
    """The order monitor's 'missing for 3 checks' path emits price=0.0 —
    that must never overwrite a recorded credit."""
    await _seed_trade(db, entry_credit=0.24)
    engine = CascadingEngine(broker=StubBroker(), db=db, strategies=[],
                             event_bus=None)
    event = OrderFillEvent(broker_order_id="ORD-1", symbol="TSLA",
                           side="sell", quantity=1, price=0.0,
                           status="filled")
    await engine._handle_order_fill_internal(event)
    credit, _ = await _entry_credit(db)
    assert credit == pytest.approx(0.24)
