"""Phase-0 outcome instrumentation — entry-feature snapshots & outcome reader.

Covers the data loop that turns each closed trade into a labelled
``(context, knobs, realized outcome)`` row:

1. ``entry_feature_snapshot`` assembles the snapshot and derives DTE +
   credit/width, dropping ``None`` fields.
2. ``record_order_response`` persists ``strategy_params['entry_features']``
   onto the Trade row, and ``fetch_trade_outcomes`` reads it back paired with
   the realized P&L once the trade closes.
3. ``CascadingEngine._attach_entry_features`` stamps the snapshot (incl.
   resolved knobs) onto an entry action before it is routed.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from hermes.db.models import Trade
from hermes.service1_agent.core import CascadingEngine, TradeAction
from hermes.service1_agent.strategies._helpers import entry_feature_snapshot


SHORT = "TSLA260717C00445000"
LONG = "TSLA260717C00450000"

# ``db`` fixture (fresh throwaway Timescale DB) is provided by tests/conftest.py.


# --------------------------------------------------------------------------- #
# 1. snapshot builder
# --------------------------------------------------------------------------- #
def test_snapshot_derives_fields_and_drops_none():
    feats = entry_feature_snapshot(
        "CS75",
        {"cs75_pop_target": 0.75, "cs75_short_delta_max": 0.40},
        side_type="call",
        pop=0.78,
        short_delta=0.22,
        width=5.0,
        entry_credit=1.25,
        expiry=(date.today() + timedelta(days=40)).strftime("%Y-%m-%d"),
        ai_authored=False,
    )
    assert feats["strategy_id"] == "CS75"
    assert feats["knobs"]["cs75_pop_target"] == 0.75
    assert feats["pop"] == 0.78
    assert feats["credit_width_ratio"] == 0.25      # 1.25 / 5.0
    assert 39 <= feats["dte"] <= 41                  # ~40 DTE
    # spot/iv_rank not supplied → dropped, not stored as None.
    assert "spot" not in feats and "iv_rank" not in feats


def test_snapshot_handles_missing_inputs():
    feats = entry_feature_snapshot("WHEEL", None)
    assert feats["strategy_id"] == "WHEEL"
    assert "knobs" not in feats                      # None knobs dropped
    assert "credit_width_ratio" not in feats         # no width/credit
    assert feats["ai_authored"] is False


# --------------------------------------------------------------------------- #
# 2. persistence + outcome reader
# --------------------------------------------------------------------------- #
async def _seed_open_trade(db, features: dict, *, trade_id: int = 1) -> int:
    """Insert an OPEN spread carrying ``entry_features``.

    Sets ``id`` explicitly so the close path + reader can target a known trade
    without depending on the broker order-recording sequence (same pattern as
    test_close_lifecycle).
    """
    await db.watchlist.ensure_strategies({"CS75": 1})
    async with db.AsyncSession() as s:
        s.add(Trade(
            id=trade_id, strategy_id="CS75", symbol="TSLA", side_type="call",
            short_leg=SHORT, long_leg=LONG, width=5.0, lots=2,
            entry_credit=1.25, expiry=date.today() + timedelta(days=40),
            status="OPEN", entry_features=features,
        ))
        await s.commit()
    return trade_id


def _close_action() -> TradeAction:
    return TradeAction(
        strategy_id="CS75", symbol="TSLA", order_class="multileg",
        legs=[{"option_symbol": SHORT, "side": "buy_to_close", "quantity": 2},
              {"option_symbol": LONG, "side": "sell_to_close", "quantity": 2}],
        price=0.40, side="buy", quantity=1, order_type="debit",
        tag="HERMES_CS75_CLOSE_TP-50",
        strategy_params={"side_type": "call", "close_reason": "TP-50"},
    )


@pytest.mark.asyncio
async def test_entry_features_persist_and_resolve_to_outcome(db):
    feats = entry_feature_snapshot(
        "CS75", {"cs75_pop_target": 0.75}, side_type="call",
        pop=0.78, short_delta=0.22, width=5.0, entry_credit=1.25,
        expiry=(date.today() + timedelta(days=40)).strftime("%Y-%m-%d"),
    )
    await _seed_open_trade(db, feats)

    # Open trade carries the snapshot; not yet an outcome (no pnl).
    assert await db.trades.fetch_trade_outcomes() == []

    await db.trades.close_trade_from_action(
        _close_action(), {"order": {"status": "filled", "id": "def456"}})

    rows = await db.trades.fetch_trade_outcomes()
    assert len(rows) == 1
    row = rows[0]
    assert row["strategy_id"] == "CS75"
    assert row["won"] is True                         # credit 1.25 > exit 0.40
    assert row["realized_pnl"] == pytest.approx((1.25 - 0.40) * 2 * 100)
    assert row["close_reason"] == "TP-50"
    assert row["entry_features"]["pop"] == 0.78
    assert row["entry_features"]["knobs"]["cs75_pop_target"] == 0.75


@pytest.mark.asyncio
async def test_fetch_trade_outcomes_filters_by_strategy(db):
    await _seed_open_trade(db, {"schema": 1})
    await db.trades.close_trade_from_action(
        _close_action(), {"order": {"status": "filled", "id": "y"}})

    assert len(await db.trades.fetch_trade_outcomes(strategy_id="CS75")) == 1
    assert await db.trades.fetch_trade_outcomes(strategy_id="WHEEL") == []


# --------------------------------------------------------------------------- #
# 3. engine choke-point stamps knobs + context
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_engine_attaches_entry_features_with_resolved_knobs(db):
    await db.watchlist.ensure_strategies({"CS75": 1})
    engine = CascadingEngine(broker=object(), db=db, strategies=[])

    action = TradeAction(
        strategy_id="CS75", symbol="TSLA", order_class="multileg",
        legs=[{"option_symbol": SHORT, "side": "sell_to_open", "quantity": 1}],
        price=1.10, side="sell", order_type="credit", width=5.0,
        expiry=(date.today() + timedelta(days=40)).strftime("%Y-%m-%d"),
        strategy_params={"side_type": "call", "pop": 0.80, "short_delta": 0.19},
    )
    await engine._attach_entry_features(action)

    feats = action.strategy_params["entry_features"]
    assert feats["strategy_id"] == "CS75"
    assert feats["pop"] == 0.80
    assert feats["entry_credit"] == 1.10
    # Resolved knobs come from the CS75 tunable group (spec defaults here).
    assert "cs75_pop_target" in feats["knobs"]


@pytest.mark.asyncio
async def test_engine_does_not_overwrite_existing_features(db):
    await db.watchlist.ensure_strategies({"CS75": 1})
    engine = CascadingEngine(broker=object(), db=db, strategies=[])

    action = TradeAction(
        strategy_id="CS75", symbol="TSLA", order_class="multileg",
        legs=[], price=1.0, side="sell", order_type="credit",
        strategy_params={"entry_features": {"sentinel": True}},
    )
    await engine._attach_entry_features(action)
    assert action.strategy_params["entry_features"] == {"sentinel": True}
