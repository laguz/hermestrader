"""Writer-ownership guardrail.

Two services share one TimescaleDB. The property that keeps that safe is
**single-writer ownership of canonical state**: Service-1 (the agent) is the
sole writer of the event-sourced read models and time series; Service-2 (the
watcher) is read-only against those tables. ARCHITECTURE.md ("Where the data
lives") documents the full writer map; this test makes the invariant executable
so it can never silently rot back into a free-for-all.

What is genuinely shared — and why it is safe to share — is narrow and
deliberate:

- ``system_settings`` — a key/value store with last-write-wins semantics and no
  cross-row invariants. Both services write it; shared keys (mode, autonomy,
  pause, learning modes) use read-before-seed discipline at boot
  (``main.py`` reads the operator's last value before writing it back).
- ``bot_logs``        — append-only audit; multiple appenders never contend on a
  row.
- ``strategy_watchlists`` — Service-2 is the *sole* writer; the agent only reads
  it. (Listed as "both" in older docs — that was wrong; this test pins it.)
- ``strategies``      — an idempotent registry seed (``ensure_strategies``,
  upsert-on-conflict); either side may seed it harmlessly.
- ``pending_approvals`` — a status-transition handoff: the operator owns the
  PENDING -> APPROVED/REJECTED transition (``decide_approval``); the agent owns
  insert, -> EXECUTED, and veto -> REJECTED. Disjoint transitions, not a race.

The dangerous tables — ``trades``, ``pending_orders``, positions,
``predictions``, ``ai_decisions``, ``bars_*``, ``event_ledger`` — have exactly
one writer (the agent), and the watcher must never reach them with a write.

This guard enforces that by scanning the watcher's source:

1. **No write-verb repository method outside the operator allowlist.** Every
   ``db.<repo>.<method>(`` call whose method name looks like a mutation must be
   one of the explicitly-blessed operator writes. A new agent-owned write method
   called from the watcher fails here automatically — no blocklist to maintain.
2. **No raw write SQL.** The watcher's only raw SQL is read queries; an
   ``INSERT INTO`` / ``DELETE FROM`` / ``UPDATE … SET`` issued from the watcher
   would bypass the repository layer and is forbidden outright.
"""
from __future__ import annotations

import re
from pathlib import Path

WATCHER_DIR = Path(__file__).resolve().parents[1] / "hermes" / "service2_watcher"

# Repository methods Service-2 is allowed to call that mutate state. Each maps
# to a table the watcher legitimately owns or shares (see the module docstring);
# anything else that looks like a write is a violation.
OPERATOR_ALLOWED_WRITES = {
    "set_setting",       # system_settings  — KV, last-write-wins
    "write_log",         # bot_logs         — append-only audit
    "set_watchlist",     # strategy_watchlists — Service-2 is sole writer
    "add_to_watchlist",  # strategy_watchlists — (same table; not used today)
    "ensure_strategies", # strategies       — idempotent registry seed
    "decide_approval",   # pending_approvals — operator-owned status transition
}

# A method name "looks like a write" if it starts with one of these verbs. This
# is intentionally broad: better to force a new mutating method onto the
# allowlist above (a deliberate, reviewed act) than to miss one.
_WRITE_VERB_RE = re.compile(
    r"^(set_|write_|record_|save_|upsert_|mark_|update_|delete_|add_"
    r"|flag_|decide_|apply_|rebuild|ensure_|clear_|prune_|purge_|seed_)"
)

# `db.<repo>.<method>(` — the only DB access shape the watcher uses (it talks to
# the module-level `db` singleton from `_app_state`).
_DB_CALL_RE = re.compile(r"\bdb\.[a-z_]+\.([a-z_]+)\s*\(")

# Raw write SQL. `UPDATE … SET` requires the SET clause so prose like
# "update target/max lots" never matches.
_RAW_WRITE_SQL_RE = re.compile(
    r"(insert\s+into|delete\s+from|update\s+\w+\s+set\b)", re.IGNORECASE
)


def _watcher_py_files():
    return [p for p in WATCHER_DIR.rglob("*.py") if "__pycache__" not in p.parts]


def test_watcher_only_calls_allowlisted_writes():
    """No write-verb repository method outside OPERATOR_ALLOWED_WRITES."""
    offenders: dict[str, set[str]] = {}
    for path in _watcher_py_files():
        src = path.read_text(encoding="utf-8")
        for method in _DB_CALL_RE.findall(src):
            if _WRITE_VERB_RE.match(method) and method not in OPERATOR_ALLOWED_WRITES:
                rel = str(path.relative_to(WATCHER_DIR.parents[1]))
                offenders.setdefault(rel, set()).add(method)
    assert not offenders, (
        "Service-2 (watcher) called a mutating repository method that is not in "
        "the operator allowlist. Either it is a read (rename it so it doesn't "
        "start with a write verb) or it writes an agent-owned table (forbidden — "
        "route it through the agent). Offenders: "
        + "; ".join(f"{f}: {sorted(m)}" for f, m in sorted(offenders.items()))
    )


def test_watcher_issues_no_raw_write_sql():
    """The watcher's raw SQL must be read-only — no INSERT/UPDATE/DELETE."""
    offenders: list[str] = []
    for path in _watcher_py_files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            # Skip comments and docstring prose; we only care about real SQL,
            # which lives in string literals passed to execute().
            if _RAW_WRITE_SQL_RE.search(line) and ('"' in line or "'" in line):
                rel = str(path.relative_to(WATCHER_DIR.parents[1]))
                offenders.append(f"{rel}:{lineno}: {line.strip()}")
    assert not offenders, (
        "Service-2 (watcher) issued raw write SQL, bypassing single-writer "
        "ownership. Canonical state is the agent's to write. Offenders:\n"
        + "\n".join(offenders)
    )
