"""Execution-quality measurement: quote-mid at submission vs. actual fill.

"Slippage" previously existed only in the simulator (``mock_broker.py``) —
real fills were never compared to the market at submission time, so there was
no way to tell whether the edge computed at entry survived execution. These
tests pin the measurement chain:

- ``TradierBroker.get_action_net_mid`` — net bid/ask midpoint of an order's
  legs, in the same credit/debit convention as ``action.price``. Read-only;
  order placement is untouched.
- ``capture_submission_mid`` — stamps the mid on the action at submission,
  degrading to None ("slippage unknown") on any failure. A missing mid must
  never become a fabricated 0.0.
- ``TradesRepository`` — persists the mid alongside the pending order and on
  the Trade row, and computes fill-vs-mid ``entry_slippage`` (positive =
  filled worse than mid) when the broker's actual fill reconciles.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import select

from hermes.broker.tradier import TradierBroker
from hermes.db.orm import PendingOrder, Trade
from hermes.service1_agent.execution_quality import capture_submission_mid
from hermes.service1_agent.trade_action import TradeAction

from ._stubs import StubBroker

pytestmark = pytest.mark.asyncio

SHORT = "TSLA260717P00440000"
LONG = "TSLA260717P00435000"


def _tradier() -> TradierBroker:
    return TradierBroker({"tradier_access_token": "TEST-TOKEN",
                          "tradier_account_id": "TEST-ACCT"})


def _stub_quotes(broker, rows):
    async def fake_get_quote(symbols):
        return [dict(r) for r in rows]
    broker.get_quote = fake_get_quote


def _spread_action(*, order_type: str = "credit", side: str = "sell",
                   price=0.25, legs=None, strategy_params=None) -> TradeAction:
    return TradeAction(
        strategy_id="CS7", symbol="TSLA", order_class="multileg",
        legs=legs if legs is not None else [
            {"option_symbol": SHORT, "side": "sell_to_open", "quantity": 1},
            {"option_symbol": LONG, "side": "buy_to_open", "quantity": 1},
        ],
        price=price, side=side, order_type=order_type, tag="HERMES_CS7",
        expiry="2026-07-17", width=5.0,
        strategy_params=strategy_params or {"side_type": "put"},
    )


# ── net-mid computation (stub-broker, no network) ────────────────────────────
async def test_credit_spread_net_mid():
    broker = _tradier()
    _stub_quotes(broker, [
        {"symbol": SHORT, "bid": 1.00, "ask": 1.10},
        {"symbol": LONG, "bid": 0.70, "ask": 0.80},
    ])
    mid = await broker.get_action_net_mid(_spread_action())
    assert mid == pytest.approx(0.30)


async def test_debit_spread_net_mid():
    # DS0 shape: buy the expensive leg, sell the cheap one, order_type=debit —
    # the mid must come back as a positive debit, matching action.price.
    broker = _tradier()
    _stub_quotes(broker, [
        {"symbol": SHORT, "bid": 1.00, "ask": 1.10},
        {"symbol": LONG, "bid": 0.70, "ask": 0.80},
    ])
    action = _spread_action(order_type="debit", side="buy", legs=[
        {"option_symbol": SHORT, "side": "buy_to_open", "quantity": 1},
        {"option_symbol": LONG, "side": "sell_to_open", "quantity": 1},
    ])
    mid = await broker.get_action_net_mid(action)
    assert mid == pytest.approx(0.30)


async def test_missing_leg_quote_returns_none():
    broker = _tradier()
    _stub_quotes(broker, [{"symbol": SHORT, "bid": 1.00, "ask": 1.10}])
    assert await broker.get_action_net_mid(_spread_action()) is None


async def test_empty_market_returns_none():
    broker = _tradier()
    _stub_quotes(broker, [
        {"symbol": SHORT, "bid": 0.0, "ask": 0.0},
        {"symbol": LONG, "bid": 0.70, "ask": 0.80},
    ])
    assert await broker.get_action_net_mid(_spread_action()) is None


async def test_crossed_market_returns_none():
    broker = _tradier()
    _stub_quotes(broker, [
        {"symbol": SHORT, "bid": 1.20, "ask": 1.10},
        {"symbol": LONG, "bid": 0.70, "ask": 0.80},
    ])
    assert await broker.get_action_net_mid(_spread_action()) is None


async def test_non_positive_net_mid_returns_none():
    # A "credit" whose net mid comes out negative means the convention doesn't
    # fit the order shape — refuse rather than record garbage.
    broker = _tradier()
    _stub_quotes(broker, [
        {"symbol": SHORT, "bid": 0.70, "ask": 0.80},
        {"symbol": LONG, "bid": 1.00, "ask": 1.10},
    ])
    assert await broker.get_action_net_mid(_spread_action()) is None


async def test_equity_mid():
    broker = _tradier()
    _stub_quotes(broker, [{"symbol": "TSLA", "bid": 99.90, "ask": 100.10}])
    action = TradeAction(
        strategy_id="WHEEL", symbol="TSLA", order_class="equity", legs=[],
        price=100.0, side="buy", order_type="limit", tag="HERMES_WHEEL",
    )
    assert await broker.get_action_net_mid(action) == pytest.approx(100.0)


async def test_leg_without_option_symbol_returns_none():
    broker = _tradier()
    _stub_quotes(broker, [{"symbol": SHORT, "bid": 1.00, "ask": 1.10}])
    action = _spread_action(legs=[{"side": "sell_to_open", "quantity": 1}])
    assert await broker.get_action_net_mid(action) is None


# ── capture helper — must degrade to None, never block, never fabricate ─────
async def test_capture_stamps_strategy_params():
    broker = _tradier()
    _stub_quotes(broker, [
        {"symbol": SHORT, "bid": 1.00, "ask": 1.10},
        {"symbol": LONG, "bid": 0.70, "ask": 0.80},
    ])
    action = _spread_action()
    mid = await capture_submission_mid(broker, action)
    assert mid == pytest.approx(0.30)
    assert action.strategy_params["mid_at_submit"] == pytest.approx(0.30)


async def test_capture_degrades_when_broker_lacks_method():
    action = _spread_action()
    assert await capture_submission_mid(StubBroker(), action) is None
    assert "mid_at_submit" not in action.strategy_params


async def test_capture_degrades_when_quote_fetch_raises():
    broker = _tradier()

    async def boom(symbols):
        raise RuntimeError("tradier down")
    broker.get_quote = boom
    action = _spread_action()
    assert await capture_submission_mid(broker, action) is None
    assert "mid_at_submit" not in action.strategy_params


# ── persistence: pending order + trade row (DB-backed) ───────────────────────
async def test_record_pending_order_persists_mid_in_payload(db):
    await db.watchlist.ensure_strategies({"CS7": 1})
    action = _spread_action(
        strategy_params={"side_type": "put", "mid_at_submit": 0.28})
    await db.trades.record_pending_order(action)
    async with db.AsyncSession() as s:
        po = (await s.execute(select(PendingOrder))).scalars().first()
    assert po is not None
    assert po.payload.get("mid_at_submit") == pytest.approx(0.28)


async def test_record_pending_order_without_mid_stores_none(db):
    await db.watchlist.ensure_strategies({"CS7": 1})
    await db.trades.record_pending_order(_spread_action())
    async with db.AsyncSession() as s:
        po = (await s.execute(select(PendingOrder))).scalars().first()
    assert po.payload.get("mid_at_submit") is None


async def test_record_order_response_copies_mid_to_trade(db):
    await db.watchlist.ensure_strategies({"CS7": 1})
    action = _spread_action(
        strategy_params={"side_type": "put", "mid_at_submit": 0.28})
    await db.trades.record_pending_order(action)
    await db.trades.record_order_response(
        action, {"order": {"id": 77, "status": "ok"}})
    async with db.AsyncSession() as s:
        row = (await s.execute(select(Trade))).scalars().first()
    assert row is not None
    assert float(row.mid_at_submit) == pytest.approx(0.28)
    assert row.entry_slippage is None          # no fill yet — unknown, not 0.0


# ── fill-vs-mid slippage on reconcile (DB-backed) ────────────────────────────
async def _seed_trade(db, *, entry_credit=None, entry_debit=None,
                      mid_at_submit=None, trade_id: int = 1,
                      order_id: str = "ORD-1") -> None:
    await db.watchlist.ensure_strategies({"CS7": 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=trade_id, strategy_id="CS7", symbol="TSLA", side_type="put",
            short_leg=SHORT, long_leg=LONG, width=5.0, lots=1,
            entry_credit=entry_credit, entry_debit=entry_debit,
            mid_at_submit=mid_at_submit,
            expiry=date.today() + timedelta(days=7),
            status="OPEN", broker_order_id=order_id,
        ))
        await s.commit()


async def _row(db, trade_id: int = 1) -> Trade:
    async with db.AsyncSession() as s:
        return (await s.execute(
            select(Trade).filter(Trade.id == trade_id))).scalars().first()


async def test_credit_fill_records_slippage(db):
    await _seed_trade(db, entry_credit=0.25, mid_at_submit=0.28)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.26) is True
    row = await _row(db)
    assert float(row.entry_credit) == pytest.approx(0.26)
    assert float(row.entry_slippage) == pytest.approx(0.02)


async def test_credit_fill_at_limit_still_records_slippage(db):
    # A fill exactly at the limit doesn't move entry_credit (and keeps the
    # established False return), but the mid comparison is still real data.
    await _seed_trade(db, entry_credit=0.25, mid_at_submit=0.28)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.25) is False
    row = await _row(db)
    assert float(row.entry_credit) == pytest.approx(0.25)
    assert float(row.entry_slippage) == pytest.approx(0.03)


async def test_debit_fill_records_slippage(db):
    await _seed_trade(db, entry_debit=0.32, mid_at_submit=0.30)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.31) is True
    row = await _row(db)
    assert float(row.entry_debit) == pytest.approx(0.31)
    assert float(row.entry_slippage) == pytest.approx(0.01)


async def test_missing_mid_leaves_slippage_unknown(db):
    # No mid recorded at submission → slippage stays NULL, never a
    # fabricated 0.0 (falsy-zero convention: NULL means "unknown").
    await _seed_trade(db, entry_credit=0.25, mid_at_submit=None)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.26) is True
    row = await _row(db)
    assert float(row.entry_credit) == pytest.approx(0.26)
    assert row.entry_slippage is None


async def test_out_of_band_fill_records_no_slippage(db):
    # The band guard treats this fill as a data anomaly — comparing an
    # untrusted fill to the mid would poison the slippage series too.
    await _seed_trade(db, entry_credit=0.25, mid_at_submit=0.28)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.20) is False
    row = await _row(db)
    assert float(row.entry_credit) == pytest.approx(0.25)
    assert row.entry_slippage is None


async def test_slippage_reconcile_is_idempotent(db):
    await _seed_trade(db, entry_credit=0.25, mid_at_submit=0.28)
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.26) is True
    # Second delivery of the same fill event: entry_credit now equals the
    # fill, so it takes the at-limit path — slippage must not drift.
    assert await db.trades.apply_entry_fill_price("ORD-1", 0.26) is False
    row = await _row(db)
    assert float(row.entry_slippage) == pytest.approx(0.02)


# ── end-to-end through the order sink (engine + stub broker, DB-backed) ─────
async def test_execute_or_queue_captures_mid_end_to_end(db):
    from hermes.service1_agent.core import CascadingEngine

    await db.watchlist.ensure_strategies({"CS7": 1})
    broker = StubBroker()

    async def fake_net_mid(action):
        return 0.28
    broker.get_action_net_mid = fake_net_mid

    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             event_bus=None)
    await engine._execute_or_queue(_spread_action(), "entry")

    async with db.AsyncSession() as s:
        po = (await s.execute(select(PendingOrder))).scalars().first()
        trade = (await s.execute(select(Trade))).scalars().first()
    assert po.payload.get("mid_at_submit") == pytest.approx(0.28)
    assert float(trade.mid_at_submit) == pytest.approx(0.28)


async def test_execute_or_queue_survives_mid_capture_failure(db):
    from hermes.service1_agent.core import CascadingEngine

    await db.watchlist.ensure_strategies({"CS7": 1})
    broker = StubBroker()

    async def boom(action):
        raise RuntimeError("quote feed down")
    broker.get_action_net_mid = boom

    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             event_bus=None)
    await engine._execute_or_queue(_spread_action(), "entry")

    # The order must still have been recorded and placed; mid is unknown.
    assert len(broker.placed) == 1
    async with db.AsyncSession() as s:
        trade = (await s.execute(select(Trade))).scalars().first()
    assert trade is not None
    assert trade.mid_at_submit is None
