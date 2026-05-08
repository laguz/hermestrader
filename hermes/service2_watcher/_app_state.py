"""Shared module-level state for the watcher's route modules.

Why this exists
---------------
``api.py`` was a 970-line single-file FastAPI app. Splitting routes into
``routes/*.py`` means every router needs the same handles: the DB
instance, the settings-key constants, a few small helpers
(``_seconds_since``, ``_parse_iso``, etc.), and a couple of derived
constants (``STALE_AFTER_S``, ``WATCHLIST`` from env).

Putting them here keeps the routers focused on their own endpoints —
they import what they need and stay short.

The leading underscore signals "internal to the watcher package"; nothing
outside ``hermes.service2_watcher`` should import from here.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from hermes.db.models import HermesDB

logger = logging.getLogger("hermes.c2.api")


# ── Environment ───────────────────────────────────────────────────────────────
DSN = os.environ.get(
    "HERMES_DSN",
    "postgresql+psycopg://hermes:hermes@localhost:5432/hermes",
)

# Optional comma-separated default watchlist read at boot. Per-strategy
# watchlists in the DB always win; this is the fallback when the DB lists
# are empty (e.g. fresh install, or analysis endpoints with no strategy
# context).
WATCHLIST = [
    s.strip().upper()
    for s in os.environ.get("HERMES_WATCHLIST", "").split(",")
    if s.strip()
]

TICK_INTERVAL_S = int(os.environ.get("HERMES_TICK_INTERVAL", 300))

# A heartbeat is "stale" after roughly two ticks. The +30s slack absorbs
# Postgres-side write latency; the floor of 60s prevents flapping when
# the operator runs a very short tick interval for testing.
STALE_AFTER_S = max(60, TICK_INTERVAL_S * 2 + 30)

DEFAULT_LLM_BASE_URL = "http://host.docker.internal:1234/v1"
MAX_SOUL_BYTES = 64 * 1024


# ── system_settings keys (mirrors service1_agent/main.py) ────────────────────
SETTING_MODE = "hermes_mode"
SETTING_TRADIER_OK_TS = "tradier_last_ok_ts"
SETTING_TRADIER_ERROR = "tradier_last_error"
SETTING_AGENT_STARTED = "agent_started_at"
SETTING_LLM_PROVIDER = "llm_provider"
SETTING_LLM_BASE_URL = "llm_base_url"
SETTING_LLM_MODEL = "llm_model"
SETTING_LLM_API_KEY = "llm_api_key"
SETTING_LLM_TEMPERATURE = "llm_temperature"
SETTING_LLM_VISION = "llm_vision"
SETTING_LLM_TIMEOUT = "llm_timeout_s"
SETTING_LLM_OK_TS = "llm_last_ok_ts"
SETTING_LLM_ERROR = "llm_last_error"
SETTING_SOUL = "soul_md"
SETTING_AUTONOMY = "agent_autonomy"
SETTING_PAUSED = "agent_paused"
SETTING_APPROVAL_MODE = "approval_mode"
SETTING_ML_OK_TS = "ml_last_ok_ts"
SETTING_ML_ERROR = "ml_last_error"


# ── Static dashboard assets ──────────────────────────────────────────────────
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)


# ── Per-strategy enable flag key generator ───────────────────────────────────
def strategy_enabled_key(sid: str) -> str:
    """Return the system_settings key for the per-strategy enable flag."""
    return f"strategy_{sid.lower()}_enabled"


# ── Time helpers ─────────────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string into a tz-aware datetime, or return None.

    Normalises trailing 'Z' to '+00:00' so the Python <3.11 stdlib accepts
    it (3.11+ handles 'Z' natively; the substitution is a no-op there).
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
    """Seconds elapsed since ``dt``, or None if ``dt`` is None."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (utcnow() - dt).total_seconds()


def read_version() -> str:
    """Best-effort read of the repo's ``VERSION`` file (or '/app/VERSION'
    inside the container). Returns 'dev' if neither is readable."""
    for p in (Path(__file__).resolve().parents[2] / "VERSION", Path("/app/VERSION")):
        try:
            return p.read_text().strip()
        except (FileNotFoundError, OSError):
            continue
    return "dev"


# ── Shared DB handle ─────────────────────────────────────────────────────────
# Created once at module import. Routers import ``db`` from here.
# HermesDB defensively creates ORM tables on construction, so this is safe
# even if schema.sql hasn't been applied yet.
db = HermesDB(DSN)
