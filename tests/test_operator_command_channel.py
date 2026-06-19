"""Operator command channel — the watcher enqueues, the agent applies.

These pin the single-writer property the channel exists to protect: the watcher
never writes canonical state; it appends an intent to ``operator_commands`` and
``CascadingEngine.drain_operator_commands`` applies it *in the agent process* via
the normal write path. Companion guard: ``tests/test_writer_ownership.py``.

The drain-logic tests use the stub pattern (no DB). The round-trip test uses the
``db`` fixture and skips when no Timescale server is reachable.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from hermes.service1_agent.core import CascadingEngine
from ._stubs import StubBroker


# ── stub command/settings/approval namespaces for the drain-logic tests ──────
class _StubCommands:
    def __init__(self, pending: List[Dict[str, Any]]):
        self._pending = list(pending)
        self.applied: List[int] = []
        self.failed: List[tuple[int, str]] = []

    async def fetch_pending(self, limit: int = 100):
        done = set(self.applied) | {cid for cid, _ in self.failed}
        return [c for c in self._pending if c["id"] not in done][:limit]

    async def mark_applied(self, command_id: int):
        self.applied.append(command_id)

    async def mark_failed(self, command_id: int, error: str):
        self.failed.append((command_id, error))


class _StubSettings:
    def __init__(self):
        self.store: Dict[str, str] = {}
        self.order: List[str] = []

    async def set_setting(self, key: str, value: str):
        self.store[key] = value
        self.order.append(key)


class _StubApprovals:
    def __init__(self):
        self.decisions: List[tuple[int, str, Any]] = []

    async def decide_approval(self, approval_id: int, decision: str, notes=None):
        self.decisions.append((approval_id, decision, notes))
        return True


class _DrainStubDB:
    def __init__(self, pending: List[Dict[str, Any]]):
        self.commands = _StubCommands(pending)
        self.settings = _StubSettings()
        self.approvals = _StubApprovals()


def _engine(stub_db) -> CascadingEngine:
    # event_bus=None skips bus subscriptions; strategies=[] is fine because the
    # drain path touches only db.commands/settings/approvals.
    return CascadingEngine(StubBroker(), stub_db, [], event_bus=None, config={})


@pytest.mark.anyio
async def test_drain_applies_settings_in_order_and_marks_applied():
    pending = [
        {"id": 1, "command_type": "SET_SETTING",
         "payload": {"settings": {"hermes_mode": "live"}}},
        {"id": 2, "command_type": "SET_SETTING",
         "payload": {"settings": {"cs75_target_lots": "3", "cs75_max_lots": "5"}}},
    ]
    db = _DrainStubDB(pending)
    applied = await _engine(db).drain_operator_commands()

    assert applied == 2
    assert db.settings.store == {
        "hermes_mode": "live", "cs75_target_lots": "3", "cs75_max_lots": "5"}
    # Submission order is preserved (id 1 before id 2).
    assert db.settings.order[0] == "hermes_mode"
    assert db.commands.applied == [1, 2]
    assert db.commands.failed == []


@pytest.mark.anyio
async def test_drain_applies_approval_decision():
    pending = [{"id": 7, "command_type": "DECIDE_APPROVAL",
                "payload": {"approval_id": 42, "decision": "APPROVED", "notes": "ok"}}]
    db = _DrainStubDB(pending)
    applied = await _engine(db).drain_operator_commands()

    assert applied == 1
    assert db.approvals.decisions == [(42, "APPROVED", "ok")]
    assert db.commands.applied == [7]


@pytest.mark.anyio
async def test_drain_marks_unknown_command_failed_and_continues():
    pending = [
        {"id": 1, "command_type": "BOGUS", "payload": {}},
        {"id": 2, "command_type": "SET_SETTING",
         "payload": {"settings": {"agent_paused": "true"}}},
    ]
    db = _DrainStubDB(pending)
    applied = await _engine(db).drain_operator_commands()

    # The bad command is isolated; the good one still applies.
    assert applied == 1
    assert [cid for cid, _ in db.commands.failed] == [1]
    assert db.commands.applied == [2]
    assert db.settings.store == {"agent_paused": "true"}


# ── DB-backed round-trip (skips without a Timescale server) ──────────────────
@pytest.mark.anyio
async def test_pending_command_survives_and_drains_through_real_db(db):
    """A command issued while the agent is 'down' is still PENDING and applied
    on the next drain — and the apply goes through the real event-sourced write
    path (system_settings row materialized in the agent process)."""
    cmd_id = await db.commands.enqueue_setting("agent_paused", "true")

    pending = await db.commands.fetch_pending()
    assert any(c["id"] == cmd_id for c in pending)
    # Not yet applied — the watcher only enqueued it.
    assert await db.settings.get_setting("agent_paused") is None

    applied = await _engine(db).drain_operator_commands()

    assert applied == 1
    assert await db.settings.get_setting("agent_paused") == "true"
    assert await db.commands.fetch_pending() == []
