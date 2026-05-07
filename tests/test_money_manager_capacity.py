"""Unit tests for MoneyManager's capacity logic.

Covers ``true_available_bp``, ``max_affordable_contracts``,
``side_aware_capacity``, and ``scale_quantity``. These four methods are
the kernel of every entry decision: if any of them is wrong, the agent
either sizes too small (missed alpha) or too large (margin call risk).
"""
from __future__ import annotations

import pytest

from hermes.service1_agent.core import MoneyManager

from ._stubs import StubBroker, StubDB


# ── true_available_bp ────────────────────────────────────────────────────────
def test_true_available_bp_subtracts_reserve():
    broker = StubBroker(option_buying_power=50_000.0)
    mm = MoneyManager(broker, StubDB(), config={"min_obp_reserve": 5_000.0})
    assert mm.true_available_bp() == 45_000.0


def test_true_available_bp_floor_at_zero():
    """Reserve > OBP shouldn't yield negative buying power."""
    broker = StubBroker(option_buying_power=1_000.0)
    mm = MoneyManager(broker, StubDB(), config={"min_obp_reserve": 5_000.0})
    assert mm.true_available_bp() == 0.0


def test_true_available_bp_zero_reserve_default():
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert mm.true_available_bp() == 10_000.0


# ── max_affordable_contracts ─────────────────────────────────────────────────
def test_max_affordable_floor_divides_bp_by_requirement():
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    # $10k BP, $500/lot → 20 lots fit.
    assert mm.max_affordable_contracts(500.0) == 20


def test_max_affordable_returns_zero_for_nonpositive_requirement():
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert mm.max_affordable_contracts(0.0) == 0
    assert mm.max_affordable_contracts(-100.0) == 0


def test_max_affordable_rounds_down():
    """$10k / $300/lot = 33.33; should yield 33, not 34."""
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert mm.max_affordable_contracts(300.0) == 33


# ── side_aware_capacity ──────────────────────────────────────────────────────
def test_side_aware_capacity_subtracts_open_pending_and_broker():
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 2},
    ])
    broker = StubBroker()
    mm = MoneyManager(broker, db, config={})
    # Pre-load broker-side cache as if sync_broker_orders had run.
    mm._broker_order_counts[("CS75", "AAPL", "put")] = 1
    # max_lots=10, open=2, broker=1, pending=0 → 7 remaining.
    assert mm.side_aware_capacity("CS75", "AAPL", "put", max_lots=10) == 7


def test_side_aware_capacity_floors_at_zero():
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 5},
    ])
    mm = MoneyManager(StubBroker(), db, config={})
    mm._broker_order_counts[("CS75", "AAPL", "put")] = 10
    # 5 + 10 = 15 > max 5 → returned 0, never negative.
    assert mm.side_aware_capacity("CS75", "AAPL", "put", max_lots=5) == 0


def test_side_aware_capacity_only_counts_matching_side():
    """Open call lots shouldn't reduce put capacity (or vice versa)."""
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "call", "lots": 5},
    ])
    mm = MoneyManager(StubBroker(), db, config={})
    assert mm.side_aware_capacity("CS75", "AAPL", "put", max_lots=5) == 5


# ── scale_quantity ───────────────────────────────────────────────────────────
def test_scale_quantity_clamps_to_min_of_request_bp_and_side():
    db = StubDB()
    db.set_open_trades("CS75", [{"symbol": "AAPL", "side_type": "put", "lots": 2}])
    broker = StubBroker(option_buying_power=2_000.0)
    mm = MoneyManager(broker, db, config={})
    # Asked for 10; bp_cap = 2000/300 = 6; side_cap = 5-2 = 3.
    # Result: min(10, 6, 3) = 3.
    assert mm.scale_quantity(
        requested_lots=10,
        requirement_per_lot=300.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=5,
    ) == 3


def test_scale_quantity_writes_block_log_when_zero_due_to_side_cap():
    db = StubDB()
    db.set_open_trades("CS75", [{"symbol": "AAPL", "side_type": "put", "lots": 5}])
    mm = MoneyManager(StubBroker(option_buying_power=100_000.0), db, config={})
    result = mm.scale_quantity(
        requested_lots=3,
        requirement_per_lot=500.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=5,
    )
    assert result == 0
    assert any("BLOCKED" in m for m in db.logs)


def test_scale_quantity_writes_block_log_when_zero_due_to_bp():
    db = StubDB()
    broker = StubBroker(option_buying_power=100.0)  # not enough for 1 lot
    mm = MoneyManager(broker, db, config={"min_obp_reserve": 0.0})
    result = mm.scale_quantity(
        requested_lots=2,
        requirement_per_lot=500.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=5,
    )
    assert result == 0
    assert any("insufficient BP" in m for m in db.logs)


def test_scale_quantity_returns_request_when_caps_are_high_enough():
    db = StubDB()
    mm = MoneyManager(StubBroker(option_buying_power=50_000.0), db, config={})
    result = mm.scale_quantity(
        requested_lots=2,
        requirement_per_lot=500.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=10,
    )
    assert result == 2
