"""Unit tests for MoneyManager's capacity logic.

Covers ``true_available_bp``, ``max_affordable_contracts``,
``side_aware_capacity``, and ``scale_quantity``. These four methods are
the kernel of every entry decision: if any of them is wrong, the agent
either sizes too small (missed alpha) or too large (margin call risk).

``side_aware_capacity`` and ``scale_quantity`` now REQUIRE an expiry —
capacity is always enforced per option chain, never globally across a
symbol. Tests below exercise both per-expiry isolation (full chain X
leaves chain Y untouched) and the loud-failure contract for callers
that forget to pass an expiry.
"""
from __future__ import annotations

import pytest

from hermes.service1_agent.core import MoneyManager

from ._stubs import StubBroker, StubDB

# A canonical expiry used by tests that don't care about the specific
# date — keeps the per-chain semantics explicit at every call site.
_EXP = "2025-06-20"


# ── true_available_bp ────────────────────────────────────────────────────────
async def test_true_available_bp_returns_full_option_buying_power():
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert await mm.true_available_bp() == 10_000.0


async def test_true_available_bp_floor_at_zero():
    """Negative broker-reported OBP should be clamped to 0."""
    broker = StubBroker(option_buying_power=-1_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert await mm.true_available_bp() == 0.0


# ── obp_reserve (operator capital fence) ─────────────────────────────────────
# The `obp_reserve` setting walls off a slice of buying power the operator
# never wants the agent to touch. true_available_bp() must subtract it from the
# broker's reported OBP before any sizing decision. If this breaks, the agent
# sizes into reserved capital — the worst kind of money-path bug.
async def test_true_available_bp_subtracts_obp_reserve():
    db = StubDB()
    db.settings["obp_reserve"] = "1500"
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, db, config={})
    # 10,000 OBP − 1,500 fenced = 8,500 truly available.
    assert await mm.true_available_bp() == 8_500.0


async def test_obp_reserve_exceeding_bp_floors_at_zero():
    """A reserve larger than OBP must clamp to 0, never go negative."""
    db = StubDB()
    db.settings["obp_reserve"] = "5000"
    broker = StubBroker(option_buying_power=1_000.0)
    mm = MoneyManager(broker, db, config={})
    assert await mm.true_available_bp() == 0.0


async def test_no_obp_reserve_returns_full_bp():
    """Default path (no reserve set) leaves broker OBP untouched."""
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert await mm.true_available_bp() == 10_000.0


async def test_malformed_obp_reserve_is_ignored():
    """A non-numeric reserve must not crash sizing — fall back to full OBP."""
    db = StubDB()
    db.settings["obp_reserve"] = "not-a-number"
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, db, config={})
    assert await mm.true_available_bp() == 10_000.0


async def test_obp_reserve_flows_through_to_affordable_lots():
    """The fence must shrink downstream lot sizing, not just the BP number."""
    db = StubDB()
    db.settings["obp_reserve"] = "8000"
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, db, config={})
    # Only 2,000 spendable after the fence; at 500/lot that's 4 lots, not 20.
    assert await mm.max_affordable_contracts(500.0) == 4


# ── max_affordable_contracts ─────────────────────────────────────────────────
async def test_max_affordable_floor_divides_bp_by_requirement():
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    # $10k BP, $500/lot → 20 lots fit.
    assert await mm.max_affordable_contracts(500.0) == 20


async def test_max_affordable_returns_zero_for_nonpositive_requirement():
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert await mm.max_affordable_contracts(0.0) == 0
    assert await mm.max_affordable_contracts(-100.0) == 0


async def test_max_affordable_rounds_down():
    """$10k / $300/lot = 33.33; should yield 33, not 34."""
    broker = StubBroker(option_buying_power=10_000.0)
    mm = MoneyManager(broker, StubDB(), config={})
    assert await mm.max_affordable_contracts(300.0) == 33


# ── side_aware_capacity ──────────────────────────────────────────────────────
async def test_side_aware_capacity_subtracts_open_pending_and_broker():
    """Open trades + cached broker orders on the same chain reduce capacity."""
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 2, "expiry": _EXP},
    ])
    broker = StubBroker()
    mm = MoneyManager(broker, db, config={})
    mm._broker_order_counts[("CS75", "AAPL", "put", _EXP)] = 1
    # max_lots=10, open=2, broker=1, pending=0 → 7 remaining on this chain.
    assert await mm.side_aware_capacity(
        "CS75", "AAPL", "put", max_lots=10, expiry=_EXP) == 7


async def test_side_aware_capacity_floors_at_zero():
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 5, "expiry": _EXP},
    ])
    mm = MoneyManager(StubBroker(), db, config={})
    mm._broker_order_counts[("CS75", "AAPL", "put", _EXP)] = 10
    # 5 + 10 = 15 > max 5 → returned 0, never negative.
    assert await mm.side_aware_capacity(
        "CS75", "AAPL", "put", max_lots=5, expiry=_EXP) == 0


async def test_side_aware_capacity_only_counts_matching_side():
    """Open call lots shouldn't reduce put capacity (or vice versa)."""
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "call", "lots": 5, "expiry": _EXP},
    ])
    mm = MoneyManager(StubBroker(), db, config={})
    assert await mm.side_aware_capacity(
        "CS75", "AAPL", "put", max_lots=5, expiry=_EXP) == 5


async def test_side_aware_capacity_isolates_chains():
    """A full chain must NOT exhaust capacity on a different chain —
    max_lots is enforced per option chain, always."""
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 12, "expiry": _EXP},
    ])
    mm = MoneyManager(StubBroker(), db, config={})
    # Expiry X is full (12/12) → no headroom there.
    assert await mm.side_aware_capacity(
        "CS75", "AAPL", "put", max_lots=12, expiry=_EXP) == 0
    # Expiry Y is untouched → fresh 12 lots available.
    assert await mm.side_aware_capacity(
        "CS75", "AAPL", "put", max_lots=12, expiry="2025-07-18") == 12


async def test_side_aware_capacity_requires_expiry():
    """The global (symbol-wide) mode was removed. Calling without an
    expiry must raise so accidental mis-calls fail loudly instead of
    silently summing across chains."""
    mm = MoneyManager(StubBroker(), StubDB(), config={})
    with pytest.raises(ValueError, match="expiry"):
        await mm.side_aware_capacity("CS75", "AAPL", "put", max_lots=10, expiry=None)
    with pytest.raises(ValueError, match="expiry"):
        await mm.side_aware_capacity("CS75", "AAPL", "put", max_lots=10, expiry="")


# ── scale_quantity ───────────────────────────────────────────────────────────
async def test_scale_quantity_clamps_to_min_of_request_bp_and_side():
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 2, "expiry": _EXP},
    ])
    broker = StubBroker(option_buying_power=2_000.0)
    mm = MoneyManager(broker, db, config={})
    # Asked for 10; bp_cap = 2000/300 = 6; side_cap = 5-2 = 3.
    # Result: min(10, 6, 3) = 3.
    assert await mm.scale_quantity(
        requested_lots=10,
        requirement_per_lot=300.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=5,
        expiry=_EXP,
    ) == 3


async def test_scale_quantity_writes_block_log_when_zero_due_to_side_cap():
    db = StubDB()
    db.set_open_trades("CS75", [
        {"symbol": "AAPL", "side_type": "put", "lots": 5, "expiry": _EXP},
    ])
    mm = MoneyManager(StubBroker(option_buying_power=100_000.0), db, config={})
    result = await mm.scale_quantity(
        requested_lots=3,
        requirement_per_lot=500.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=5,
        expiry=_EXP,
    )
    assert result == 0
    assert any("BLOCKED" in m for m in db.logs)


async def test_scale_quantity_writes_block_log_when_zero_due_to_bp():
    db = StubDB()
    broker = StubBroker(option_buying_power=100.0)  # not enough for 1 lot
    mm = MoneyManager(broker, db, config={})
    result = await mm.scale_quantity(
        requested_lots=2,
        requirement_per_lot=500.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=5,
        expiry=_EXP,
    )
    assert result == 0
    assert any("insufficient BP" in m for m in db.logs)


async def test_scale_quantity_returns_request_when_caps_are_high_enough():
    db = StubDB()
    mm = MoneyManager(StubBroker(option_buying_power=50_000.0), db, config={})
    result = await mm.scale_quantity(
        requested_lots=2,
        requirement_per_lot=500.0,
        symbol="AAPL", side="put",
        strategy_id="CS75", max_lots=10,
        expiry=_EXP,
    )
    assert result == 2


async def test_scale_quantity_requires_expiry():
    """Same loud-failure contract as side_aware_capacity."""
    mm = MoneyManager(StubBroker(option_buying_power=50_000.0), StubDB(), config={})
    with pytest.raises(ValueError, match="expiry"):
        await mm.scale_quantity(
            requested_lots=2, requirement_per_lot=500.0,
            symbol="AAPL", side="put",
            strategy_id="CS75", max_lots=10,
            expiry=None,
        )
