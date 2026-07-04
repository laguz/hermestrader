"""
hermes/market_hours.py — US equity market session awareness.

Provides:
  - market_session()   : current session label + open flag
  - is_market_open()   : True only during regular hours
  - next_open()        : datetime of next regular-session open
  - should_block_trades(): defence-in-depth gate every order submission
                          path can call — returns (blocked, reason) so
                          the broker round-trip never happens outside
                          the regular session unless the operator has
                          explicitly opted in via HERMES_ALLOW_OFFHOURS_TRADES.

All times are US/Eastern.  No third-party calendar dependency —
holidays are maintained in NYSE_HOLIDAYS below.  Add each year's
dates as they are announced.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:                       # ZoneInfoNotFoundError or ImportError
    # Fallback: fixed UTC-5 offset (EST).  Daylight saving won't be honoured
    # but the market-hours gate will still be approximately correct.
    ET = timezone(timedelta(hours=-5))  # type: ignore[assignment]

# NYSE observed holidays — extend each year.
# Source: https://www.nyse.com/markets/hours-calendars
NYSE_HOLIDAYS: frozenset[date] = frozenset([
    # 2024
    date(2024, 1, 1),   # New Year's Day
    date(2024, 1, 15),  # MLK Day
    date(2024, 2, 19),  # Presidents' Day
    date(2024, 3, 29),  # Good Friday
    date(2024, 5, 27),  # Memorial Day
    date(2024, 6, 19),  # Juneteenth
    date(2024, 7, 4),   # Independence Day
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2024, 12, 25), # Christmas
    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 9),   # National Day of Mourning (Carter)
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
])

# NYSE early-close days (regular session ends 13:00 ET) — extend alongside
# NYSE_HOLIDAYS. Typically July 3, the day after Thanksgiving, and
# Christmas Eve, whenever they fall on a weekday and aren't already a full
# holiday (2026-07-03 is a full closure above, so it is not listed here).
NYSE_EARLY_CLOSES: frozenset[date] = frozenset([
    date(2024, 7, 3),
    date(2024, 11, 29),
    date(2024, 12, 24),
    date(2025, 7, 3),
    date(2025, 11, 28),
    date(2025, 12, 24),
    date(2026, 11, 27),
    date(2026, 12, 24),
])

# Session boundaries (Eastern time)
_PRE_OPEN    = time(4,  0)   # pre-market starts
_REGULAR_OPEN  = time(9, 30)   # regular session opens
_REGULAR_CLOSE = time(16,  0)  # regular session closes
_EARLY_CLOSE   = time(13,  0)  # regular session closes on NYSE_EARLY_CLOSES days
_AFTER_CLOSE   = time(20,  0)  # after-hours ends


def _now_et() -> datetime:
    from hermes.utils import now as clock_now
    return clock_now(ET)


def is_trading_day(d: Optional[date] = None) -> bool:
    """True if `d` is a weekday and not a NYSE holiday."""
    if d is None:
        d = _now_et().date()
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def market_session(now: Optional[datetime] = None) -> dict:
    """Return a dict describing the current market session.

    Keys:
      session   : "pre_market" | "regular" | "after_hours" | "closed"
      is_open   : bool  — True only during regular hours on a trading day
      et_time   : str   — current ET time HH:MM
      et_date   : str   — current ET date YYYY-MM-DD
      trading_day: bool — whether today is a trading day at all
    """
    if now is None:
        now = _now_et()
    elif now.tzinfo is None:
        now = now.replace(tzinfo=ET)

    today = now.date()
    t = now.time().replace(second=0, microsecond=0)
    trading = is_trading_day(today)
    regular_close = _EARLY_CLOSE if today in NYSE_EARLY_CLOSES else _REGULAR_CLOSE

    if not trading:
        session = "closed"
        is_open = False
    elif t < _PRE_OPEN:
        session = "closed"
        is_open = False
    elif t < _REGULAR_OPEN:
        session = "pre_market"
        is_open = False
    elif t < regular_close:
        session = "regular"
        is_open = True
    elif t < _AFTER_CLOSE:
        session = "after_hours"
        is_open = False
    else:
        session = "closed"
        is_open = False

    return {
        "session": session,
        "is_open": is_open,
        "et_time": now.strftime("%H:%M"),
        "et_date": today.isoformat(),
        "trading_day": trading,
    }


def is_market_open(now: Optional[datetime] = None) -> bool:
    """True only during the regular session (9:30–16:00 ET on trading days)."""
    return market_session(now)["is_open"]


def next_open(now: Optional[datetime] = None) -> datetime:
    """Return the datetime of the next regular-session open (ET)."""
    if now is None:
        now = _now_et()
    elif now.tzinfo is None:
        now = now.replace(tzinfo=ET)

    candidate = now.date()
    # If today's open hasn't happened yet, try today first.
    if (is_trading_day(candidate)
            and now.time() < _REGULAR_OPEN):
        return datetime.combine(candidate, _REGULAR_OPEN, tzinfo=ET)

    # Otherwise advance to the next trading day.
    candidate += timedelta(days=1)
    for _ in range(10):          # safety: skip up to 10 days (holiday runs)
        if is_trading_day(candidate):
            return datetime.combine(candidate, _REGULAR_OPEN, tzinfo=ET)
        candidate += timedelta(days=1)

    # Should never happen with a sane holiday list.
    return datetime.combine(candidate, _REGULAR_OPEN, tzinfo=ET)


_OFFHOURS_OVERRIDE_ENV = "HERMES_ALLOW_OFFHOURS_TRADES"


def offhours_trading_allowed() -> bool:
    """True if the operator has opted in to off-hours order submission.

    Read fresh from the environment every call so the watcher / agent
    pick up the toggle without restart. Accept the usual truthy spellings
    (``true``/``1``/``yes``/``on``); anything else is treated as off.
    """
    val = (os.environ.get(_OFFHOURS_OVERRIDE_ENV) or "").strip().lower()
    return val in {"true", "1", "yes", "on"}


def should_block_trades(now: Optional[datetime] = None) -> Tuple[bool, str]:
    """Defence-in-depth gate for every order-submission path.

    Returns ``(blocked, reason)``. ``blocked=True`` means the caller MUST
    NOT round-trip the broker. ``reason`` is a short human-readable
    string suitable for the C2 live feed.

    Rules (in priority order):

    * Operator override (``HERMES_ALLOW_OFFHOURS_TRADES=true``) → never block.
    * Not a trading day (weekend / NYSE holiday) → block.
    * Outside the regular 9:30–16:00 ET session → block.
    * Otherwise → allow.

    The strategy modules (``submit()``, ``_execute_approved_action()``,
    etc.) call this directly so a closed-market order can never reach the
    broker, regardless of which entry point produced it.
    """
    if offhours_trading_allowed():
        return False, "offhours override enabled"
    s = market_session(now)
    if not s["trading_day"]:
        return True, f"closed (not a trading day; {s['et_date']})"
    if not s["is_open"]:
        return True, f"closed ({s['session']} {s['et_time']} ET)"
    return False, ""


def session_label(now: Optional[datetime] = None) -> str:
    """Human-readable one-liner for logs and the LLM system prompt."""
    s = market_session(now)
    labels = {
        "regular":     "OPEN  — Regular session (9:30–16:00 ET)",
        "pre_market":  "PRE-MARKET (04:00–09:30 ET) — no entries",
        "after_hours": "AFTER-HOURS (16:00–20:00 ET) — no entries",
        "closed":      "CLOSED — market is not trading",
    }
    label = labels.get(s["session"], "UNKNOWN")
    return f"Market: {label}  [{s['et_time']} ET  {s['et_date']}]"
