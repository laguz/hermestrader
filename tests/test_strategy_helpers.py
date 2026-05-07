"""Unit tests for AbstractStrategy helpers + the shared strategy helpers.

Covers ``find_expiry_in_dte_range``, ``find_strike_by_delta``,
``short_credit``, ``find_active_ic_expiry`` (the deterministic-ordering
fix), plus the module-level ``parse_occ`` / ``nearest_strike``.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from hermes.service1_agent.core import (
    AbstractStrategy, IronCondorBuilder, MoneyManager,
)
from hermes.service1_agent.strategies._helpers import nearest_strike, parse_occ

from ._stubs import StubBroker, StubDB, make_chain


class _Concrete(AbstractStrategy):
    NAME = "TEST"
    PRIORITY = 99

    def execute_entries(self, _wl):
        return []

    def manage_positions(self):
        return []


def _make_strategy(*, broker=None, db=None) -> _Concrete:
    broker = broker or StubBroker()
    db = db or StubDB()
    mm = MoneyManager(broker, db, config={})
    return _Concrete(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm), config={},
    )


# ── parse_occ ────────────────────────────────────────────────────────────────
def test_parse_occ_put():
    p = parse_occ("AAPL250620P00150000")
    assert p == {
        "underlying": "AAPL",
        "expiry": date(2025, 6, 20),
        "side": "put",
    }


def test_parse_occ_call():
    p = parse_occ("MSFT260101C00400000")
    assert p["side"] == "call"
    assert p["expiry"] == date(2026, 1, 1)


def test_parse_occ_returns_none_for_garbage():
    assert parse_occ("not-an-occ") is None
    assert parse_occ("") is None
    assert parse_occ(None) is None


# ── nearest_strike ───────────────────────────────────────────────────────────
def test_nearest_strike_picks_closest_in_chain():
    chain = make_chain("AAPL", "2025-06-20", spot=100.0, strike_step=1.0)
    near = nearest_strike(chain, "put", target=92.7)
    assert near is not None
    assert int(near["strike"]) == 93  # closest 1-pt strike to 92.7


def test_nearest_strike_filters_by_option_type():
    chain = make_chain("AAPL", "2025-06-20")
    near_put = nearest_strike(chain, "put", target=100.0)
    near_call = nearest_strike(chain, "call", target=100.0)
    assert near_put is not None and near_put["option_type"] == "put"
    assert near_call is not None and near_call["option_type"] == "call"


def test_nearest_strike_returns_none_for_empty_chain():
    assert nearest_strike([], "put", 100.0) is None


# ── find_expiry_in_dte_range ─────────────────────────────────────────────────
def test_find_expiry_picks_max_in_window():
    today = date.today()
    expirations = [(today + timedelta(days=d)).isoformat()
                   for d in (10, 20, 30, 40, 50)]
    broker = StubBroker(expirations=expirations)
    s = _make_strategy(broker=broker)
    chosen = s.find_expiry_in_dte_range("AAPL", min_dte=15, max_dte=45, prefer="max")
    # Window is 15–45 days; max in that window is +40.
    assert chosen == (today + timedelta(days=40)).isoformat()


def test_find_expiry_picks_min_when_requested():
    today = date.today()
    expirations = [(today + timedelta(days=d)).isoformat()
                   for d in (10, 20, 30, 40, 50)]
    broker = StubBroker(expirations=expirations)
    s = _make_strategy(broker=broker)
    chosen = s.find_expiry_in_dte_range("AAPL", min_dte=15, max_dte=45, prefer="min")
    assert chosen == (today + timedelta(days=20)).isoformat()


def test_find_expiry_returns_none_when_window_empty():
    today = date.today()
    expirations = [(today + timedelta(days=d)).isoformat() for d in (10, 200)]
    broker = StubBroker(expirations=expirations)
    s = _make_strategy(broker=broker)
    assert s.find_expiry_in_dte_range("AAPL", 30, 60) is None


# ── find_strike_by_delta ─────────────────────────────────────────────────────
def test_find_strike_by_delta_picks_closest_within_tolerance():
    chain = make_chain("AAPL", "2025-06-20", spot=100.0)
    s = _make_strategy()
    # Target 0.16 delta puts. Synthetic chain has linear delta, so a strike
    # near $87 should produce |Δ| ~ 0.16.
    pick = s.find_strike_by_delta(chain, "put", target_delta=0.16, tolerance=0.05)
    assert pick is not None
    actual_delta = abs(float(pick["greeks"]["delta"]))
    assert abs(actual_delta - 0.16) <= 0.05


def test_find_strike_by_delta_skips_options_with_no_greeks():
    """Tradier returns greeks=None for deep OTM / illiquid options."""
    chain = [
        {"option_type": "put", "strike": 90.0, "greeks": None},
        {"option_type": "put", "strike": 91.0, "greeks": {"delta": -0.16}},
    ]
    s = _make_strategy()
    pick = s.find_strike_by_delta(chain, "put", target_delta=0.16, tolerance=0.05)
    assert pick is not None
    assert pick["strike"] == 91.0


def test_find_strike_by_delta_returns_none_when_outside_tolerance():
    chain = [{"option_type": "put", "strike": 90.0, "greeks": {"delta": -0.50}}]
    s = _make_strategy()
    assert s.find_strike_by_delta(chain, "put", 0.16, tolerance=0.05) is None


# ── short_credit ─────────────────────────────────────────────────────────────
def test_short_credit_uses_midpoints():
    s = _make_strategy()
    short_leg = {"bid": 1.00, "ask": 1.20}  # mid 1.10
    long_leg  = {"bid": 0.40, "ask": 0.60}  # mid 0.50
    assert s.short_credit(short_leg, long_leg) == 0.60


def test_short_credit_can_be_negative():
    """If the short is wider than the long, credit goes negative — still
    just an arithmetic result, not a guard."""
    s = _make_strategy()
    short_leg = {"bid": 0.10, "ask": 0.20}  # mid 0.15
    long_leg  = {"bid": 0.50, "ask": 0.70}  # mid 0.60
    assert s.short_credit(short_leg, long_leg) == -0.45


# ── find_active_ic_expiry — determinism (issue #10 from review) ──────────────
def test_find_active_ic_picks_earliest_incomplete_expiry():
    db = StubDB()
    db.set_open_legs("TEST", "AAPL", [
        {"option_symbol": "AAPL250620P00090000", "side": "put", "expiry": "2025-06-20"},
        # Earlier expiry, also incomplete (only put open):
        {"option_symbol": "AAPL250516P00090000", "side": "put", "expiry": "2025-05-16"},
    ])
    s = _make_strategy(db=db)
    assert s.find_active_ic_expiry("AAPL") == "2025-05-16"


def test_find_active_ic_skips_complete_ic():
    db = StubDB()
    db.set_open_legs("TEST", "AAPL", [
        # Complete on 2025-05-16
        {"option_symbol": "AAPL250516P00090000", "side": "put", "expiry": "2025-05-16"},
        {"option_symbol": "AAPL250516C00110000", "side": "call", "expiry": "2025-05-16"},
        # Incomplete on 2025-06-20
        {"option_symbol": "AAPL250620P00090000", "side": "put", "expiry": "2025-06-20"},
    ])
    s = _make_strategy(db=db)
    assert s.find_active_ic_expiry("AAPL") == "2025-06-20"


def test_find_active_ic_returns_none_when_nothing_open():
    s = _make_strategy()  # default StubDB has no open legs
    assert s.find_active_ic_expiry("AAPL") is None
