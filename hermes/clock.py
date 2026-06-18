from __future__ import annotations

import datetime
from typing import Protocol


class Clock(Protocol):
    """Protocol defining standard time interface for Hermes."""

    def utc_now(self) -> datetime.datetime:
        """Return timezone-naive current UTC time."""
        ...

    def date_today(self) -> datetime.date:
        """Return current date."""
        ...

    def now(self, tz: datetime.tzinfo | None = None) -> datetime.datetime:
        """Return current local/aware time."""
        ...


class RealClock:
    """Clock implementation using real system time."""

    def utc_now(self) -> datetime.datetime:
        return datetime.datetime.utcnow()

    def date_today(self) -> datetime.date:
        return datetime.date.today()

    def now(self, tz: datetime.tzinfo | None = None) -> datetime.datetime:
        return datetime.datetime.now(tz)


class SimulatedClock:
    """Clock implementation using a mutable simulated date and time."""

    def __init__(self, initial_dt: datetime.datetime):
        self._current_dt = initial_dt

    def utc_now(self) -> datetime.datetime:
        return self._current_dt

    def date_today(self) -> datetime.date:
        return self._current_dt.date()

    def now(self, tz: datetime.tzinfo | None = None) -> datetime.datetime:
        utc_dt = self._current_dt.replace(tzinfo=datetime.timezone.utc)
        if tz is not None:
            return utc_dt.astimezone(tz)
        return utc_dt

    def set_time(self, dt: datetime.datetime) -> None:
        """Update the simulated clock time."""
        self._current_dt = dt
