"""Order-working (stale-entry-order repricing) and slippage-feedback pricing.

Two features layered on top of the existing execution-quality measurement
(``mid_at_submit`` / ``entry_slippage``, previously unconsumed):

- **Order working** (``PipelineController.work_stale_entry_orders``) — an
  unfilled ENTRY limit order resting past ``order_work_after_s`` is
  cancelled and repriced toward the market by ``order_work_step`` per pass,
  up to ``order_work_max_steps``, then abandoned. Cancel-then-place, never
  blind resubmission — a failed cancel (most likely "already filled") must
  never be followed by a replacement placement.
- **Slippage feedback** — a per-symbol trailing median ``entry_slippage``
  (``execution_quality.estimate_symbol_slippage``) is subtracted from the
  quoted credit before a credit-spread strategy's min-credit-pct gate, so a
  symbol with a history of costly fills must clear its threshold net of that
  cost. Fewer than ``slippage_min_fills`` recorded fills → no adjustment.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import select

from hermes.db.orm import Trade
from hermes.service1_agent.core import CascadingEngine, IronCondorBuilder, MoneyManager
from hermes.service1_agent.execution_quality import estimate_symbol_slippage
from hermes.service1_agent.strategies import CreditSpreads7

from ._stubs import StubBroker, StubDB, _et_today

pytestmark = pytest.mark.asyncio

SHORT = "TSLA260717P00097000"
LONG = "TSLA260717P00096000"


@pytest.fixture(autouse=True)
def _market_open(monkeypatch):
    """Order-working respects the off-hours gate (by design), which reads
    the real wall clock — pin it open so these tests don't flake depending
    on when they happen to run. ``test_off_hours_is_a_full_noop`` overrides
    this back to blocked to test the gate itself."""
    import hermes.market_hours as market_hours
    monkeypatch.setattr(market_hours, "should_block_trades", lambda: (False, ""))


# ── helpers ──────────────────────────────────────────────────────────────────
async def _seed_trade(db, *, entry_credit, mid_at_submit=None, trade_id=1,
                      order_id="OLD-1", short_leg=SHORT, long_leg=LONG,
                      width=1.0, strategy_id="CS7") -> None:
    await db.watchlist.ensure_strategies({strategy_id: 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=trade_id, strategy_id=strategy_id, symbol="TSLA", side_type="put",
            short_leg=short_leg, long_leg=long_leg, width=width, lots=1,
            entry_credit=entry_credit, mid_at_submit=mid_at_submit,
            expiry=date.today() + timedelta(days=7),
            status="OPEN", broker_order_id=order_id,
        ))
        await s.commit()


async def _row(db, trade_id=1) -> Trade:
    async with db.AsyncSession() as s:
        return (await s.execute(
            select(Trade).filter(Trade.id == trade_id))).scalars().first()


def _resting_order(*, oid="OLD-1", price=0.30, tag="HERMES_CS7",
                   symbol="TSLA", short=SHORT, long=LONG, status="open"):
    return {
        "id": oid, "status": status, "tag": tag, "symbol": symbol,
        "price": price, "quantity": 1, "type": "credit", "class": "multileg",
        "leg": [
            {"option_symbol": short, "side": "sell_to_open", "quantity": 1},
            {"option_symbol": long, "side": "buy_to_open", "quantity": 1},
        ],
    }


def _engine(broker, db):
    return CascadingEngine(broker=broker, db=db, strategies=[], event_bus=None)


# ── TradesRepository.reprice_open_order (DB-backed) ─────────────────────────
async def test_reprice_open_order_updates_id_and_credit(db):
    await _seed_trade(db, entry_credit=0.30, mid_at_submit=0.32)
    ok = await db.trades.reprice_open_order("OLD-1", "NEW-1", 0.25, new_mid=0.29)
    assert ok is True
    row = await _row(db)
    assert row.broker_order_id == "NEW-1"
    assert float(row.entry_credit) == pytest.approx(0.25)
    assert float(row.mid_at_submit) == pytest.approx(0.29)


async def test_reprice_open_order_no_match_returns_false(db):
    await _seed_trade(db, entry_credit=0.30)
    assert await db.trades.reprice_open_order("NOPE", "NEW-1", 0.25) is False


# ── TradesRepository.recent_entry_slippage / estimate_symbol_slippage ───────
async def test_recent_entry_slippage_newest_first(db):
    await db.watchlist.ensure_strategies({"CS7": 1})
    async with db.AsyncSession() as s:
        for i, slip in enumerate([0.01, 0.02, 0.03]):
            s.add(Trade(
                id=i + 1, strategy_id="CS7", symbol="TSLA", side_type="put",
                short_leg=SHORT, long_leg=LONG, width=1.0, lots=1,
                entry_credit=0.20, entry_slippage=slip,
                expiry=date.today() + timedelta(days=7),
                status="CLOSED",
            ))
        await s.commit()
    values = await db.trades.recent_entry_slippage("TSLA", 20)
    assert sorted(values) == [0.01, 0.02, 0.03]


async def test_estimate_symbol_slippage_below_min_fills_is_none(db):
    await db.watchlist.ensure_strategies({"CS7": 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=1, strategy_id="CS7", symbol="TSLA", side_type="put",
            short_leg=SHORT, long_leg=LONG, width=1.0, lots=1,
            entry_credit=0.20, entry_slippage=0.05,
            expiry=date.today() + timedelta(days=7), status="CLOSED",
        ))
        await s.commit()
    assert await estimate_symbol_slippage(db, "TSLA", min_fills=10) is None


async def test_estimate_symbol_slippage_returns_median(db):
    await db.watchlist.ensure_strategies({"CS7": 1})
    async with db.AsyncSession() as s:
        for i, slip in enumerate([0.01, 0.05, 0.09]):
            s.add(Trade(
                id=i + 1, strategy_id="CS7", symbol="TSLA", side_type="put",
                short_leg=SHORT, long_leg=LONG, width=1.0, lots=1,
                entry_credit=0.20, entry_slippage=slip,
                expiry=date.today() + timedelta(days=7), status="CLOSED",
            ))
        await s.commit()
    assert await estimate_symbol_slippage(db, "TSLA", min_fills=3) == pytest.approx(0.05)


async def test_estimate_symbol_slippage_degrades_when_repo_lacks_method():
    class _NoHistoryDB:
        class trades:
            pass
    assert await estimate_symbol_slippage(_NoHistoryDB(), "TSLA", min_fills=1) is None


# ── work_stale_entry_orders (DB-backed engine pipeline) ──────────────────────
async def test_first_pass_only_registers_age_no_broker_calls(db):
    broker = StubBroker(orders=[_resting_order()])
    engine = _engine(broker, db)
    await _seed_trade(db, entry_credit=0.30)

    await engine.pipeline.work_stale_entry_orders()

    assert "OLD-1" in engine.pipeline._order_work_state
    assert engine.pipeline._order_work_state["OLD-1"]["steps"] == 0
    row = await _row(db)
    assert row.broker_order_id == "OLD-1"          # untouched — too young to work
    assert float(row.entry_credit) == pytest.approx(0.30)


async def test_stale_order_is_repriced_and_trade_relinked(db):
    broker = StubBroker(orders=[_resting_order(price=0.30)])
    cancelled = []
    broker.cancel_order = lambda oid: cancelled.append(oid) or {"status": "ok"}
    broker.place_order_from_action = lambda action: {"order": {"id": "NEW-1", "status": "open"}}

    engine = _engine(broker, db)
    await _seed_trade(db, entry_credit=0.30)

    await engine.pipeline.work_stale_entry_orders()          # registers first_seen
    engine.pipeline._order_work_state["OLD-1"]["first_seen"] -= 120  # simulate age

    await engine.pipeline.work_stale_entry_orders()

    assert cancelled == ["OLD-1"]
    assert "OLD-1" not in engine.pipeline._order_work_state
    assert engine.pipeline._order_work_state["NEW-1"]["steps"] == 1

    row = await _row(db)
    assert row.broker_order_id == "NEW-1"
    assert float(row.entry_credit) == pytest.approx(0.25)     # 0.30 - order_work_step(0.05)


async def test_reprice_never_steps_past_worst_price_floor(db):
    # CS7 min_credit_pct default 0.12, width=1.0 -> floor $0.12. Starting at
    # $0.13 (one nickel above the floor) the first step must land exactly on
    # the floor, not below it, then abandon on the next pass instead of
    # crossing it.
    broker = StubBroker(orders=[_resting_order(price=0.13)])
    cancelled = []
    broker.cancel_order = lambda oid: cancelled.append(oid) or {"status": "ok"}
    placed = []

    def place(action):
        placed.append(action.price)
        return {"order": {"id": f"NEW-{len(placed)}", "status": "open"}}
    broker.place_order_from_action = place

    engine = _engine(broker, db)
    await _seed_trade(db, entry_credit=0.13)

    await engine.pipeline.work_stale_entry_orders()
    engine.pipeline._order_work_state["OLD-1"]["first_seen"] -= 120
    await engine.pipeline.work_stale_entry_orders()             # step 1: 0.13 -> 0.12 (floor)

    assert placed == [0.12]
    row = await _row(db)
    assert float(row.entry_credit) == pytest.approx(0.12)
    assert row.broker_order_id == "NEW-1"

    # Step 2: already at the floor — must abandon, not price below it.
    engine.pipeline._order_work_state["NEW-1"]["first_seen"] -= 120
    broker._orders = [_resting_order(oid="NEW-1", price=0.12)]
    await engine.pipeline.work_stale_entry_orders()

    assert cancelled == ["OLD-1", "NEW-1"]
    assert placed == [0.12]                       # no second replacement placed
    assert "NEW-1" not in engine.pipeline._order_work_state
    row = await _row(db)
    assert float(row.entry_credit) == pytest.approx(0.12)      # never crossed the floor


async def test_abandons_after_max_steps_without_resubmitting(db):
    broker = StubBroker(orders=[_resting_order(price=0.30)])
    cancelled = []
    broker.cancel_order = lambda oid: cancelled.append(oid) or {"status": "ok"}
    placed = []
    broker.place_order_from_action = lambda action: placed.append(action) or {
        "order": {"id": "SHOULD-NOT-PLACE", "status": "open"}}

    engine = _engine(broker, db)
    await _seed_trade(db, entry_credit=0.30)

    engine.pipeline._order_work_state["OLD-1"] = {
        "first_seen": 0.0, "steps": 2, "original_price": 0.30,   # already at max_steps
    }

    await engine.pipeline.work_stale_entry_orders()

    assert cancelled == ["OLD-1"]
    assert placed == []                              # no replacement — abandoned
    assert "OLD-1" not in engine.pipeline._order_work_state
    row = await _row(db)
    assert row.broker_order_id == "OLD-1"             # DB row untouched
    assert float(row.entry_credit) == pytest.approx(0.30)


async def test_fill_race_cancel_failure_does_not_double_place(db):
    broker = StubBroker(orders=[_resting_order(price=0.30)])

    def cancel_order(oid):
        raise RuntimeError("order already filled")
    broker.cancel_order = cancel_order
    placed = []
    broker.place_order_from_action = lambda action: placed.append(action) or {
        "order": {"id": "SHOULD-NOT-PLACE", "status": "open"}}

    engine = _engine(broker, db)
    await _seed_trade(db, entry_credit=0.30)

    engine.pipeline._order_work_state["OLD-1"] = {
        "first_seen": 0.0, "steps": 0, "original_price": 0.30,
    }

    await engine.pipeline.work_stale_entry_orders()

    assert placed == []                               # cancel failed -> never placed
    assert "OLD-1" not in engine.pipeline._order_work_state
    row = await _row(db)
    assert row.broker_order_id == "OLD-1"              # left exactly as it was
    assert float(row.entry_credit) == pytest.approx(0.30)


async def test_ds0_entries_are_never_worked(db):
    broker = StubBroker(orders=[_resting_order(
        oid="DS0-1", price=0.10, tag="HERMES_DS0")])
    cancelled = []
    broker.cancel_order = lambda oid: cancelled.append(oid) or {"status": "ok"}

    engine = _engine(broker, db)
    await engine.pipeline.work_stale_entry_orders()
    engine.pipeline._order_work_state.pop("DS0-1", None)  # ensure not tracked
    await engine.pipeline.work_stale_entry_orders()

    assert cancelled == []
    assert "DS0-1" not in engine.pipeline._order_work_state


async def test_close_orders_are_never_worked(db):
    broker = StubBroker(orders=[_resting_order(
        oid="CLOSE-1", price=0.30, tag="HERMES_CS7_CLOSE_TP-50")])
    cancelled = []
    broker.cancel_order = lambda oid: cancelled.append(oid) or {"status": "ok"}

    engine = _engine(broker, db)
    await engine.pipeline.work_stale_entry_orders()
    await engine.pipeline.work_stale_entry_orders()

    assert cancelled == []
    assert "CLOSE-1" not in engine.pipeline._order_work_state


async def test_off_hours_is_a_full_noop(db, monkeypatch):
    import hermes.market_hours as market_hours
    monkeypatch.setattr(market_hours, "should_block_trades",
                        lambda: (True, "after-hours"))

    broker = StubBroker(orders=[_resting_order()])
    called = {"cancel": False, "get_orders": False}

    def get_orders():
        called["get_orders"] = True
        return [_resting_order()]
    broker.get_orders = get_orders
    broker.cancel_order = lambda oid: called.__setitem__("cancel", True) or {"status": "ok"}

    engine = _engine(broker, db)
    await engine.pipeline.work_stale_entry_orders()

    assert called == {"cancel": False, "get_orders": False}
    assert engine.pipeline._order_work_state == {}


# ── slippage-feedback entry pricing (offline, StubDB) ────────────────────────
def _cs7(broker, db):
    mm = MoneyManager(broker, db, {})
    return CreditSpreads7(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm), config={}, dry_run=False,
    )


def _put(expiry_ymd: str, strike: float, delta: float, mid: float) -> dict:
    occ = f"TSLA{expiry_ymd}P{int(strike * 1000):08d}"
    return {"symbol": occ, "strike": strike, "option_type": "put",
            "greeks": {"delta": -delta}, "bid": mid - 0.02, "ask": mid + 0.02}


def _analysis(levels):
    return {
        "symbol": "TSLA", "current_price": 100.0,
        "current_vol": 0.20, "avg_vol": 0.20,
        "key_levels": [{"price": p, "type": "support", "strength": 5}
                       for p in levels],
        "samples": 100, "period": "3m",
    }


def _chain(expiry: str):
    # short mid 0.20, long mid 0.06 -> credit 0.14; CS7 default min_credit_pct
    # 0.12 * width 1.0 = 0.12, so this clears the raw floor with $0.02 to spare.
    ymd = date.fromisoformat(expiry).strftime("%y%m%d")
    return [
        _put(ymd, 93.0, 0.21, 0.20),
        _put(ymd, 92.0, 0.17, 0.06),
    ]


def _expirations_for(*dte_values):
    today = _et_today()
    return [(today + timedelta(days=d)).isoformat() for d in dte_values]


async def test_no_slippage_history_is_byte_identical_to_baseline():
    expiry = _expirations_for(7)[0]
    broker = StubBroker(expirations=[expiry])
    broker.get_option_chains = lambda symbol, exp: _chain(expiry)
    broker.analyze_symbol = lambda symbol, period="6m": _analysis([93.0])

    s = _cs7(broker, StubDB())
    actions = await s.execute_entries(["TSLA"])
    puts = [a for a in actions if a.strategy_params.get("side_type") == "put"]
    assert len(puts) == 1
    assert puts[0].price == pytest.approx(0.14)


async def test_thin_slippage_history_below_min_fills_is_ignored():
    expiry = _expirations_for(7)[0]
    broker = StubBroker(expirations=[expiry])
    broker.get_option_chains = lambda symbol, exp: _chain(expiry)
    broker.analyze_symbol = lambda symbol, period="6m": _analysis([93.0])

    db = StubDB()
    # Only 3 recorded fills; slippage_min_fills default is 10 -> no adjustment.
    async def recent_entry_slippage(symbol, limit):
        return [0.05, 0.06, 0.07]
    db.recent_entry_slippage = recent_entry_slippage

    s = _cs7(broker, db)
    actions = await s.execute_entries(["TSLA"])
    puts = [a for a in actions if a.strategy_params.get("side_type") == "put"]
    assert len(puts) == 1                          # unaffected — same as no history


async def test_slippage_history_pushes_marginal_credit_below_floor():
    expiry = _expirations_for(7)[0]
    broker = StubBroker(expirations=[expiry])
    broker.get_option_chains = lambda symbol, exp: _chain(expiry)
    broker.analyze_symbol = lambda symbol, period="6m": _analysis([93.0])

    db = StubDB()
    # 10 fills, median slippage 0.03: net credit 0.14 - 0.03 = 0.11 < min 0.12.
    async def recent_entry_slippage(symbol, limit):
        return [0.03] * 10
    db.recent_entry_slippage = recent_entry_slippage

    s = _cs7(broker, db)
    actions = await s.execute_entries(["TSLA"])
    puts = [a for a in actions if a.strategy_params.get("side_type") == "put"]
    assert puts == []
    assert any("net of $0.03 slippage" in line for line in s.execution_logs)
