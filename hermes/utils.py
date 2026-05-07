from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def utcnow() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    """Return the current time in UTC as an ISO-8601 string."""
    return utcnow().isoformat(timespec="seconds")


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string into a timezone-aware datetime, or return None.

    Python <3.11's ``datetime.fromisoformat`` rejects the trailing ``Z``
    used by most external services; normalise it to ``+00:00`` first so
    timestamps round-tripped through other tools still parse.
    """
    if not s:
        return None
    try:
        normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(normalised)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def seconds_since(dt: Optional[datetime]) -> Optional[float]:
    """Return the number of seconds since `dt` until now."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (utcnow() - dt).total_seconds()
