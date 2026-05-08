from datetime import datetime, date
from hermes.market_hours import (
    ET, is_trading_day, market_session, is_market_open, next_open, session_label
)

def test_is_trading_day():
    # Tuesday, 2025-01-07 - Regular trading day
    assert is_trading_day(date(2025, 1, 7)) is True
    # Saturday, 2025-01-11 - Weekend
    assert is_trading_day(date(2025, 1, 11)) is False
    # Sunday, 2025-01-12 - Weekend
    assert is_trading_day(date(2025, 1, 12)) is False
    # Wednesday, 2025-01-01 - New Year's Day (Holiday)
    assert is_trading_day(date(2025, 1, 1)) is False

def test_is_market_open():
    # Tuesday, 2025-01-07 10:00 AM ET - Regular session
    now = datetime(2025, 1, 7, 10, 0, tzinfo=ET)
    assert is_market_open(now) is True

    # Tuesday, 2025-01-07 09:30 AM ET - Regular session start
    now = datetime(2025, 1, 7, 9, 30, tzinfo=ET)
    assert is_market_open(now) is True

    # Tuesday, 2025-01-07 04:00 PM ET - Regular session end
    now = datetime(2025, 1, 7, 16, 0, tzinfo=ET)
    assert is_market_open(now) is False

    # Tuesday, 2025-01-07 08:00 AM ET - Pre-market
    now = datetime(2025, 1, 7, 8, 0, tzinfo=ET)
    assert is_market_open(now) is False

    # Tuesday, 2025-01-07 05:00 PM ET - After-hours
    now = datetime(2025, 1, 7, 17, 0, tzinfo=ET)
    assert is_market_open(now) is False

    # Saturday, 2025-01-11 10:00 AM ET - Weekend
    now = datetime(2025, 1, 11, 10, 0, tzinfo=ET)
    assert is_market_open(now) is False

def test_market_session():
    # Regular session
    now = datetime(2025, 1, 7, 10, 0, tzinfo=ET)
    session = market_session(now)
    assert session["session"] == "regular"
    assert session["is_open"] is True
    assert session["trading_day"] is True

    # Pre-market
    now = datetime(2025, 1, 7, 8, 0, tzinfo=ET)
    session = market_session(now)
    assert session["session"] == "pre_market"
    assert session["is_open"] is False

    # After-hours
    now = datetime(2025, 1, 7, 17, 0, tzinfo=ET)
    session = market_session(now)
    assert session["session"] == "after_hours"
    assert session["is_open"] is False

    # Closed (early morning)
    now = datetime(2025, 1, 7, 2, 0, tzinfo=ET)
    session = market_session(now)
    assert session["session"] == "closed"
    assert session["is_open"] is False

    # Closed (late night)
    now = datetime(2025, 1, 7, 22, 0, tzinfo=ET)
    session = market_session(now)
    assert session["session"] == "closed"
    assert session["is_open"] is False

    # Weekend
    now = datetime(2025, 1, 11, 10, 0, tzinfo=ET)
    session = market_session(now)
    assert session["session"] == "closed"
    assert session["trading_day"] is False

def test_next_open():
    # Before open on a trading day
    now = datetime(2025, 1, 7, 8, 0, tzinfo=ET)
    expected = datetime(2025, 1, 7, 9, 30, tzinfo=ET)
    assert next_open(now) == expected

    # During regular session
    now = datetime(2025, 1, 7, 10, 0, tzinfo=ET)
    expected = datetime(2025, 1, 8, 9, 30, tzinfo=ET)
    assert next_open(now) == expected

    # After regular session
    now = datetime(2025, 1, 7, 17, 0, tzinfo=ET)
    expected = datetime(2025, 1, 8, 9, 30, tzinfo=ET)
    assert next_open(now) == expected

    # Friday after market close
    now = datetime(2025, 1, 10, 17, 0, tzinfo=ET)
    expected = datetime(2025, 1, 13, 9, 30, tzinfo=ET) # Next Monday
    assert next_open(now) == expected

    # Weekend
    now = datetime(2025, 1, 11, 10, 0, tzinfo=ET)
    expected = datetime(2025, 1, 13, 9, 30, tzinfo=ET) # Next Monday
    assert next_open(now) == expected

    # Before a holiday
    # Dec 31, 2024 (Tuesday) after close
    now = datetime(2024, 12, 31, 17, 0, tzinfo=ET)
    # Jan 1 is holiday, Jan 2 is Thursday
    expected = datetime(2025, 1, 2, 9, 30, tzinfo=ET)
    assert next_open(now) == expected

def test_session_label():
    now = datetime(2025, 1, 7, 10, 0, tzinfo=ET)
    label = session_label(now)
    assert "OPEN" in label
    assert "10:00 ET" in label
    assert "2025-01-07" in label

    now = datetime(2025, 1, 7, 8, 0, tzinfo=ET)
    label = session_label(now)
    assert "PRE-MARKET" in label

    now = datetime(2025, 1, 7, 17, 0, tzinfo=ET)
    label = session_label(now)
    assert "AFTER-HOURS" in label

    now = datetime(2025, 1, 11, 10, 0, tzinfo=ET)
    label = session_label(now)
    assert "CLOSED" in label
