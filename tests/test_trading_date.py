"""Regression test for the UTC/ET trading-day boundary bug in DTE math.

``StrategyBase.today()`` used to take the UTC calendar date directly. Between
roughly 8pm and midnight Eastern, UTC has already rolled to the next calendar
day while the US trading day hasn't — so every DTE computed in that window
came out one day short (e.g. an exact-7-DTE expiry would compute as 6 DTE and
get skipped). Pinned via ``SimulatedClock`` so this doesn't depend on the
wall-clock time the suite happens to run at.
"""
from __future__ import annotations

from datetime import date, datetime

from hermes.clock import SimulatedClock
from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import CreditSpreads7

from ._stubs import StubBroker, StubDB


def _build(clock, **broker_kwargs):
    broker = StubBroker(**broker_kwargs)
    db = StubDB()
    mm = MoneyManager(broker, db, {})
    s = CreditSpreads7(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm), config={}, dry_run=False,
        clock=clock,
    )
    return s


# 2026-06-09 01:30 UTC == 2026-06-08 21:30 EDT (9:30 PM ET): UTC has already
# rolled over to June 9th but the ET trading day is still June 8th.
_LATE_EVENING_ET_UTC = datetime(2026, 6, 9, 1, 30)


def test_today_uses_eastern_trading_day_not_utc_calendar_day():
    s = _build(SimulatedClock(_LATE_EVENING_ET_UTC))
    assert s.today() == date(2026, 6, 8)


async def test_find_expiry_in_dte_range_matches_the_eastern_trading_day():
    # Exactly 7 DTE from the ET trading day (June 8th), not from the UTC
    # calendar day (June 9th) the naive bug would have used.
    s = _build(SimulatedClock(_LATE_EVENING_ET_UTC), expirations=["2026-06-15"])
    expiry = await s.find_expiry_in_dte_range("AAPL", min_dte=7, max_dte=7)
    assert expiry == "2026-06-15"
