"""Prose-drift guardrail for ARCHITECTURE.md.

`test_schema_parity.py` protects the *schema* — but nothing protected the
*prose*. ARCHITECTURE.md once described the cross-service IPC as "PG NOTIFY"
long after the code moved to Redis pub/sub (commit 9d959b5), and the data-flow
table can name a table the ORM no longer has. Both are silent rot: the docs
compile, the tests pass, and the map quietly stops matching the territory.

This is the cheap doc-lint that fails when that happens:

1. **No retired transport names.** Once a transport (PG NOTIFY, etc.) is ripped
   out of the code, its name must not survive in the architecture prose. When
   you migrate off a transport, add its spelling(s) to ``RETIRED_TRANSPORTS``.
2. **Every table named in "Where the data lives" actually exists.** Each table
   in that markdown table must be a real ORM table or a SQL-only `bars_*` table
   declared in schema.sql — catches renames/deletes that never reached the doc.
"""
from __future__ import annotations

import re
from pathlib import Path

from hermes.db.orm import Base

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURE_MD = REPO_ROOT / "ARCHITECTURE.md"
SCHEMA_SQL = REPO_ROOT / "hermes" / "db" / "schema.sql"

# Transports that have been removed from the code. Their names must no longer
# appear anywhere in ARCHITECTURE.md. Spellings are matched case-insensitively.
# Add to this set whenever you rip a transport/mechanism out of the stack.
RETIRED_TRANSPORTS = (
    "PG NOTIFY",
    "LISTEN/NOTIFY",
    "pg_notify",
    "Postgres NOTIFY",
    "PostgreSQL LISTEN",
)

# Tables that exist ONLY as raw Postgres/TimescaleDB tables in schema.sql (never
# modelled as ORM classes). Mirrors SQL_ONLY_TABLES in test_schema_parity.py.
SQL_ONLY_TABLES = {"bars_daily", "bars_intraday"}


def _authoritative_tables() -> set[str]:
    """The full set of table names the code actually defines."""
    return set(Base.metadata.tables.keys()) | SQL_ONLY_TABLES


def _data_lives_tables() -> set[str]:
    """Table names listed in the 'Where the data lives' markdown table.

    The first column of that table holds a backtick-wrapped table name. We scan
    rows between the '## Where the data lives' heading and the next '## ' heading
    and pull the leading `code` token from each.
    """
    text = ARCHITECTURE_MD.read_text(encoding="utf-8")
    section = re.search(
        r"^##\s+Where the data lives\b(.*?)(?=^##\s)",
        text,
        re.DOTALL | re.MULTILINE,
    )
    assert section, "ARCHITECTURE.md is missing the 'Where the data lives' section"

    tables: set[str] = set()
    for line in section.group(1).splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        first_col = line.split("|")[1].strip()
        m = re.fullmatch(r"`([a-z_][a-z0-9_]*)`", first_col)
        if m:
            tables.add(m.group(1))
    return tables


def test_no_retired_transports_in_architecture():
    """ARCHITECTURE.md must not name a transport the code has removed."""
    text = ARCHITECTURE_MD.read_text(encoding="utf-8").lower()
    leaked = [name for name in RETIRED_TRANSPORTS if name.lower() in text]
    assert not leaked, (
        f"ARCHITECTURE.md still names retired transport(s): {leaked}. "
        "The IPC layer moved to Redis pub/sub — update the prose, not this list "
        "(unless you re-added the transport)."
    )


def test_documented_tables_exist():
    """Every table named in 'Where the data lives' must exist in the code."""
    documented = _data_lives_tables()
    assert documented, "Parsed zero tables from 'Where the data lives' — parser drift?"

    known = _authoritative_tables()
    unknown = documented - known
    assert not unknown, (
        f"ARCHITECTURE.md documents table(s) the code no longer has: {sorted(unknown)}. "
        f"Known tables: {sorted(known)}."
    )
