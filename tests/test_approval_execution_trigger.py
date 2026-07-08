"""Approved actions must actually reach the broker promptly.

Two regressions pinned here:

1. The watcher publishes an IPC ``trigger_approvals`` signal right after an
   operator decides an approval, specifically so the trade reaches the broker
   within seconds. The handler used to only refresh an in-memory cache
   (``ControlState.approved_actions``, never read anywhere) and never actually
   called the broker — every approval silently waited for the next slow
   heartbeat tick (``tick_interval_s``, e.g. 900s) instead.
2. ``PipelineController.execute_approved_actions`` (shared by that IPC handler
   and the heartbeat) must isolate each action in its own try/except: one
   action raising must not silently drop every approval queued after it in
   the same batch.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from hermes.service1_agent._engine_pipeline import PipelineController
from hermes.service1_agent.agent_reactive import handle_ipc_command


def _make_pipeline(fetch_return, exec_side_effect=None):
    db = SimpleNamespace(approvals=SimpleNamespace(
        fetch_approved_actions=AsyncMock(return_value=fetch_return)))
    broker = SimpleNamespace(broker=MagicMock())
    ctx = SimpleNamespace(db=db, broker=broker)
    engine = SimpleNamespace(ctx=ctx)
    pipeline = PipelineController(engine)
    exec_mock = AsyncMock(side_effect=exec_side_effect)
    return pipeline, db, exec_mock


@pytest.mark.anyio
async def test_execute_approved_actions_isolates_per_item_failures(monkeypatch):
    items = [{"id": 1}, {"id": 2}, {"id": 3}]

    def _side_effect(item, *, broker, db):
        if item["id"] == 2:
            raise RuntimeError("broker hiccup")
        return "executed"

    pipeline, db, exec_mock = _make_pipeline(items, exec_side_effect=_side_effect)
    monkeypatch.setattr(
        "hermes.service1_agent.agent_approvals._execute_approved_action", exec_mock)

    attempted = await pipeline.execute_approved_actions()

    # All three must be attempted — item 2 raising must not stop 3 from running.
    assert attempted == 3
    assert exec_mock.call_count == 3
    assert [c.args[0]["id"] for c in exec_mock.call_args_list] == [1, 2, 3]


@pytest.mark.anyio
async def test_ipc_trigger_approvals_executes_broker_action_not_just_cache():
    control_state = MagicMock()
    control_state.refresh_approvals = AsyncMock()
    db = MagicMock()
    event_bus = MagicMock()
    engine = MagicMock()
    engine.execute_approved_actions = AsyncMock(return_value=1)

    await handle_ipc_command(
        {"action": "trigger_approvals"}, control_state, db, {}, event_bus, engine)

    engine.execute_approved_actions.assert_awaited_once()
