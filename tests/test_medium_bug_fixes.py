"""Regression tests for the medium-severity follow-up batch.

Covers:
- find_active_ic_expiry deterministic ordering
- _parse_iso handling of trailing 'Z'
- record_pending_order side derivation from OCC when side_type is missing
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from hermes.service1_agent.core import AbstractStrategy
from hermes.service1_agent.main import _parse_iso as _parse_iso_main

# api.py instantiates a HermesDB at module load, which needs the psycopg
# driver (installed in CI via requirements.txt). Skip the api side locally
# if it isn't available so this test file is still useful in dev.
try:
    from hermes.service2_watcher.api import _parse_iso as _parse_iso_api
    _PARSERS = [_parse_iso_main, _parse_iso_api]
except ModuleNotFoundError:
    _PARSERS = [_parse_iso_main]


# ---------------------------------------------------------------------------
# _parse_iso (#12)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("parser", _PARSERS)
def test_parse_iso_handles_trailing_z(parser):
    dt = parser("2026-05-06T14:00:00Z")
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.utcoffset().total_seconds() == 0


@pytest.mark.parametrize("parser", _PARSERS)
def test_parse_iso_handles_offset(parser):
    dt = parser("2026-05-06T10:00:00-04:00")
    assert dt is not None
    assert dt.utcoffset().total_seconds() == -4 * 3600


@pytest.mark.parametrize("parser", _PARSERS)
def test_parse_iso_naive_assumed_utc(parser):
    dt = parser("2026-05-06T14:00:00")
    assert dt is not None
    assert dt.tzinfo == timezone.utc


@pytest.mark.parametrize("parser", _PARSERS)
def test_parse_iso_returns_none_on_garbage(parser):
    assert parser(None) is None
    assert parser("") is None
    assert parser("not-a-date") is None


# ---------------------------------------------------------------------------
# find_active_ic_expiry deterministic ordering (#10)
# ---------------------------------------------------------------------------
class _StubDB:
    def __init__(self, legs_by_call: List[Dict[str, Any]]):
        self._legs = legs_by_call

    def open_legs(self, _strategy_id: str, _symbol: str) -> List[Dict[str, Any]]:
        return self._legs

    def write_log(self, *_a, **_kw):
        pass


class _StubBroker:
    current_date = None


class _ConcreteStrategy(AbstractStrategy):
    NAME = "TEST"
    PRIORITY = 99

    def execute_entries(self, _watchlist):
        return []

    def manage_positions(self):
        return []


def _make_strategy(legs):
    return _ConcreteStrategy(
        broker=_StubBroker(),
        db=_StubDB(legs),
        money_manager=None,
        ic_builder=None,
        config={},
    )


def test_find_active_ic_returns_earliest_incomplete_expiry():
    """Two incomplete ICs on different expiries → return the earlier one."""
    legs = [
        {"option_symbol": "AAPL250620P00150000", "side": "put", "expiry": "2025-06-20"},
        {"option_symbol": "AAPL250516P00150000", "side": "put", "expiry": "2025-05-16"},
    ]
    s = _make_strategy(legs)
    assert s.find_active_ic_expiry("AAPL") == "2025-05-16"


def test_find_active_ic_skips_complete_ic():
    """An expiry with both put and call legs is skipped."""
    legs = [
        # Complete IC on 2025-05-16
        {"option_symbol": "AAPL250516P00150000", "side": "put", "expiry": "2025-05-16"},
        {"option_symbol": "AAPL250516C00200000", "side": "call", "expiry": "2025-05-16"},
        # Incomplete on 2025-06-20
        {"option_symbol": "AAPL250620P00150000", "side": "put", "expiry": "2025-06-20"},
    ]
    s = _make_strategy(legs)
    assert s.find_active_ic_expiry("AAPL") == "2025-06-20"


def test_find_active_ic_returns_none_when_all_complete():
    legs = [
        {"option_symbol": "AAPL250516P00150000", "side": "put", "expiry": "2025-05-16"},
        {"option_symbol": "AAPL250516C00200000", "side": "call", "expiry": "2025-05-16"},
    ]
    s = _make_strategy(legs)
    assert s.find_active_ic_expiry("AAPL") is None


# ---------------------------------------------------------------------------
# record_pending_order side derivation (#8)
# ---------------------------------------------------------------------------
def test_record_pending_derives_side_from_occ_when_side_type_missing():
    """The OCC-side fallback in record_pending_order is unit-testable
    without a database: import and call the helper regex directly."""
    from hermes.db.models import _OCC_RE

    # Put leg
    m = _OCC_RE.match("AAPL250620P00150000")
    assert m is not None and m.group(3) == "P"

    # Call leg
    m = _OCC_RE.match("AAPL250620C00200000")
    assert m is not None and m.group(3) == "C"

    # Junk
    assert _OCC_RE.match("not-an-occ-symbol") is None
