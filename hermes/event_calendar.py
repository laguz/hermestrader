"""
Event risk calendar checking logic.

Answering:
- "does <symbol> have earnings within N days?"
- "is <date> a macro-event day (FOMC, CPI)?"
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Set

logger = logging.getLogger("hermes.event_calendar")

# ── STATIC MACRO CALENDAR DATES (2025 - 2027) ──────────────────────────────

# Federal Open Market Committee meeting dates
_FOMC_DATES: Set[str] = {
    # 2025
    "2025-01-28", "2025-01-29",
    "2025-03-18", "2025-03-19",
    "2025-05-06", "2025-05-07",
    "2025-06-17", "2025-06-18",
    "2025-07-29", "2025-07-30",
    "2025-09-16", "2025-09-17",
    "2025-10-28", "2025-10-29",
    "2025-12-09", "2025-12-10",
    # 2026
    "2026-01-27", "2026-01-28",
    "2026-03-17", "2026-03-18",
    "2026-04-28", "2026-04-29",
    "2026-06-16", "2026-06-17",
    "2026-07-28", "2026-07-29",
    "2026-09-15", "2026-09-16",
    "2026-10-27", "2026-10-28",
    "2026-12-08", "2026-12-09",
    # 2027
    "2027-01-26", "2027-01-27",
    "2027-03-16", "2027-03-17",
    "2027-04-26", "2027-04-27",
    "2027-06-07", "2027-06-08",
    "2027-07-26", "2027-07-27",
    "2027-09-13", "2027-09-14",
    "2027-11-01", "2027-11-02",
    "2027-12-13", "2027-12-14",
}

# Consumer Price Index release dates
_CPI_DATES: Set[str] = {
    # 2025
    "2025-01-15", "2025-02-12", "2025-03-12", "2025-04-10", "2025-05-13", "2025-06-11",
    "2025-07-11", "2025-08-13", "2025-09-10", "2025-10-24", "2025-11-13", "2025-12-18",
    # 2026
    "2026-01-13", "2026-02-11", "2026-03-11", "2026-04-10", "2026-05-12", "2026-06-10",
    "2026-07-14", "2026-08-12", "2026-09-11", "2026-10-14", "2026-11-10", "2026-12-10",
    # 2027 (estimated)
    "2027-01-13", "2027-02-10", "2027-03-10", "2027-04-14", "2027-05-12", "2027-06-09",
    "2027-07-14", "2027-08-11", "2027-09-15", "2027-10-13", "2027-11-10", "2027-12-15",
}

MACRO_EVENT_DATES: Set[date] = {
    datetime.strptime(d, "%Y-%m-%d").date() for d in (_FOMC_DATES | _CPI_DATES)
}


# ── CHECKERS ────────────────────────────────────────────────────────────────

def is_macro_event_day(d: date) -> bool:
    """Return True if the date has a scheduled FOMC meeting or CPI release."""
    return d in MACRO_EVENT_DATES


def is_macro_event_within_days(start_date: date, days: int) -> bool:
    """Return True if there is a macro event day in [start_date, start_date + days]."""
    for offset in range(days + 1):
        if is_macro_event_day(start_date + timedelta(days=offset)):
            return True
    return False


def extract_earnings_dates(data: Any, symbol: str) -> List[date]:
    """Defensively extract earnings dates from the corporate calendar response."""
    dates: List[date] = []
    if not data:
        return dates

    symbol_upper = symbol.upper()

    def process_item(item: Any):
        if not isinstance(item, dict):
            return
        
        # Check symbol match (some endpoints might return events for multiple symbols)
        item_symbol = item.get("symbol")
        if item_symbol and str(item_symbol).upper() != symbol_upper:
            return
        
        # Check type (ensure it's earnings)
        event_type = item.get("type", "")
        # If type is specified, check that it contains "earnings" (case-insensitive)
        if event_type and "earnings" not in str(event_type).lower():
            return
        
        # Find date field
        for key in ["date", "event_date", "earnings_date", "start_date"]:
            val = item.get(key)
            if val:
                try:
                    if isinstance(val, str):
                        # Extract YYYY-MM-DD
                        d = datetime.strptime(val[:10], "%Y-%m-%d").date()
                        dates.append(d)
                        break
                except (ValueError, TypeError):
                    continue

    def traverse(obj: Any):
        if isinstance(obj, list):
            for x in obj:
                if isinstance(x, dict):
                    process_item(x)
                else:
                    traverse(x)
        elif isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (list, dict)):
                    traverse(v)

    traverse(data)
    return dates


async def has_earnings_within_days(broker, symbol: str, current_date: date, days: int) -> bool:
    """Return True if the symbol has an upcoming earnings date in [current_date, current_date + days]."""
    try:
        data = await broker.get_corporate_calendar(symbol)
        earnings_dates = extract_earnings_dates(data, symbol)
        for e in earnings_dates:
            if 0 <= (e - current_date).days <= days:
                return True
    except Exception as exc:
        # Rethrow to allow the calling strategy to log details and fail-open.
        raise exc
    return False
