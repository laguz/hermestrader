from __future__ import annotations

import pytest
from datetime import date, datetime, timedelta
from typing import Dict, Any, List

from hermes.event_calendar import (
    is_macro_event_day,
    is_macro_event_within_days,
    extract_earnings_dates,
    has_earnings_within_days,
)
from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import (
    CreditSpreads75,
    CreditSpreads7,
    TastyTrade45,
    WheelStrategy,
)
from tests._stubs import StubBroker, StubDB, make_trade, _et_today


# ── HELPER FOR BUILDING STRATEGIES ─────────────────────────────────────────

def _build_strat(strategy_cls, today_dt: datetime, *, expirations=None, config=None, db=None):
    broker = StubBroker(expirations=expirations)
    broker.current_date = today_dt
    
    db = db or StubDB()
    # Mock settings on the stub db to handle resolving tunables correctly
    config = config or {}
    for k, v in config.items():
        db.settings[k] = str(v)
        
    mm = MoneyManager(broker, db, config)
    strat = strategy_cls(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm),
        config=config, dry_run=False,
    )
    strat.current_date = today_dt
    return strat, broker, db


# ── EVENT CALENDAR CORE TESTS ──────────────────────────────────────────────

def test_macro_event_day_checks():
    # 2026-07-28 is a scheduled FOMC meeting
    fomc_day = date(2026, 7, 28)
    assert is_macro_event_day(fomc_day) is True

    # 2026-07-14 is a scheduled CPI release
    cpi_day = date(2026, 7, 14)
    assert is_macro_event_day(cpi_day) is True

    # 2026-07-20 has no scheduled macro events
    normal_day = date(2026, 7, 20)
    assert is_macro_event_day(normal_day) is False


def test_macro_event_within_days():
    # Today is 2026-07-25, FOMC is 2026-07-28 (3 days away)
    assert is_macro_event_within_days(date(2026, 7, 25), 7) is True
    # Today is 2026-07-25, checking only 2 days ahead (does not reach 28th)
    assert is_macro_event_within_days(date(2026, 7, 25), 2) is False


def test_extract_earnings_dates_defensive():
    # Test valid structure
    data = {
        "calendar": [
            {"symbol": "AAPL", "type": "earnings", "date": "2026-07-28"},
            {"symbol": "MSFT", "type": "earnings", "date": "2026-07-30"},
            {"symbol": "AAPL", "type": "dividend", "date": "2026-08-01"},  # non-earnings event
        ]
    }
    dates = extract_earnings_dates(data, "AAPL")
    assert dates == [date(2026, 7, 28)]

    # Test symbol mismatch / case insensitivity
    assert extract_earnings_dates(data, "aapl") == [date(2026, 7, 28)]
    assert extract_earnings_dates(data, "GOOG") == []

    # Test nested dict / lists format variation
    data_nested = {
        "results": [
            {"symbol": "TSLA", "event_date": "2026-07-28T16:00:00Z", "type": "earnings_call"}
        ]
    }
    assert extract_earnings_dates(data_nested, "TSLA") == [date(2026, 7, 28)]


# ── STRATEGY ENTRY GATING TESTS ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_entry_blocked_near_macro_event():
    # 2026-07-25 is 3 days before FOMC (2026-07-28). Macro window is 7 days.
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-01"],
        config={"cs75_macro_blackout_days": 7}
    )

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 0
    assert any("Entry blocked: macro event" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_entry_allowed_outside_macro_event():
    # 2026-07-19 is 9 days before FOMC. Macro window is 7 days.
    today_dt = datetime(2026, 7, 19, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-08-28"],
        config={"cs75_macro_blackout_days": 7}
    )

    actions = await strat.execute_entries(["AAPL"])
    # Allowed, should produce entry actions
    assert len(actions) > 0


@pytest.mark.asyncio
async def test_macro_window_independent_of_earnings_window():
    # 2026-07-25 is 3 days before FOMC. The earnings window is wide (7d) but
    # the macro window is the default 1d — FOMC 3 days out must NOT gate.
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-04"],  # 41 DTE — inside CS75's 39-45 entry window
        config={"cs75_event_blackout_days": 7, "cs75_macro_blackout_days": 1}
    )

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) > 0
    assert not any("Entry blocked: macro event" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_macro_window_blocks_even_with_earnings_window_off():
    # 2026-07-27 is 1 day before FOMC. Earnings window off, macro window 1d.
    today_dt = datetime(2026, 7, 27, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-01"],
        config={"cs75_event_blackout_days": 0, "cs75_macro_blackout_days": 1}
    )

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 0
    assert any("Entry blocked: macro event" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_tt45_ic_completion_not_gated():
    # On a macro blackout day TT45 must still complete an existing half-IC;
    # only brand-new condors are gated.
    today_dt = datetime(2026, 7, 28, 10, 0, 0)  # FOMC day
    strat, broker, db = _build_strat(
        TastyTrade45, today_dt,
        expirations=["2026-09-18"],
        config={"tt45_macro_blackout_days": 7}
    )

    async def fake_active_ic_expiry(symbol):
        return "2026-09-18"
    strat.find_active_ic_expiry = fake_active_ic_expiry

    gate_calls = []
    orig_gate = strat.is_event_gated
    async def spy_gate(symbol, earnings_days, macro_days=0):
        gate_calls.append(symbol)
        return await orig_gate(symbol, earnings_days, macro_days)
    strat.is_event_gated = spy_gate

    await strat.execute_entries(["AAPL"])
    # The completion path never consulted the event gate.
    assert gate_calls == []
    assert not any("Entry blocked" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_tt45_new_ic_still_gated():
    today_dt = datetime(2026, 7, 28, 10, 0, 0)  # FOMC day
    strat, broker, db = _build_strat(
        TastyTrade45, today_dt,
        expirations=["2026-09-18"],
        config={"tt45_macro_blackout_days": 7}
    )

    async def no_active_ic(symbol):
        return None
    strat.find_active_ic_expiry = no_active_ic

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 0
    assert any("Entry blocked: macro event" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_entry_blocked_near_earnings():
    # Today is 2026-07-18. Next macro event (FOMC 2026-07-28) is 10 days away (outside 7d window).
    # AAPL has earnings on 2026-07-21 (3 days away, inside 7d window).
    today_dt = datetime(2026, 7, 18, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-01"],
        config={"cs75_event_blackout_days": 7}
    )

    # Stub earnings date to be within the window (2026-07-21)
    async def mock_get_corporate_calendar(symbols):
        return {
            "calendar": [
                {"symbol": "AAPL", "type": "earnings", "date": "2026-07-21"}
            ]
        }
    broker.get_corporate_calendar = mock_get_corporate_calendar

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 0
    assert any("Entry blocked: AAPL has earnings" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_entry_allowed_outside_earnings():
    # Today is 2026-07-18. Next macro event is 10 days away.
    # AAPL has earnings on 2026-07-28 (10 days away, outside 7d window).
    today_dt = datetime(2026, 7, 18, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-01"],
        config={"cs75_event_blackout_days": 7}
    )

    async def mock_get_corporate_calendar(symbols):
        return {
            "calendar": [
                {"symbol": "AAPL", "type": "earnings", "date": "2026-07-28"}
            ]
        }
    broker.get_corporate_calendar = mock_get_corporate_calendar

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) > 0


@pytest.mark.asyncio
async def test_gate_fail_open_on_fetch_failure():
    # Today is 2026-07-18. Next macro event is 10 days away.
    today_dt = datetime(2026, 7, 18, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-01"],
        config={"cs75_event_blackout_days": 7}
    )

    # Mock the broker calendar endpoint to raise an exception
    async def mock_get_corporate_calendar(symbols):
        raise RuntimeError("Tradier calendar endpoint timeout")
    broker.get_corporate_calendar = mock_get_corporate_calendar

    actions = await strat.execute_entries(["AAPL"])
    
    # Check that we failed open (actions generated)
    assert len(actions) > 0
    
    # Assert loud warnings were logged
    warning_logged = False
    for log in strat.execution_logs:
        if "WARNING: Earnings calendar fetch failed for AAPL" in log:
            warning_logged = True
            break
    assert warning_logged is True


@pytest.mark.asyncio
async def test_exits_never_blocked():
    # Today is 2026-07-28 (FOMC day). Entry is gated, but exits must run.
    today_dt = datetime(2026, 7, 28, 10, 0, 0)
    
    db = StubDB()
    expiry_date = date(2026, 8, 27)
    trade = make_trade(
        "CS75", "AAPL",
        side_type="call",
        short_strike=105.0,
        long_strike=110.0,
        entry_credit=2.00,
        expiry=expiry_date
    )
    db.set_open_trades("CS75", [trade])

    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        config={"cs75_event_blackout_days": 7},
        db=db
    )

    # Mock get_quote for the specific OCC legs built by make_trade
    # AAPL260918C00105000 and AAPL260918C00110000
    broker.get_quote = lambda symbols: [
        {"symbol": s.strip(), "bid": 0.20, "ask": 0.30}
        for s in symbols.split(",")
    ]

    # Run exits check
    actions = await strat.manage_positions()
    
    # Exits should proceed normally despite being on a macro blackout day
    assert len(actions) == 1
    assert actions[0].side == "buy"
    assert "HERMES_CS75" in actions[0].tag
