"""Regression tests for the 2026-07-08 audit: IPC-published events used the
Pydantic class name (e.g. ``"ModeChangedEvent"``) as ``event_type``, but
``hermes.db.events.EVENT_TYPE_TO_CLASS``/``deserialize_event`` key on the
registry's SCREAMING_SNAKE_CASE string (e.g. ``"MODE_CHANGED"``). Every event
published this way — settings changes, watchlist changes, approval
decisions — silently failed to deserialize on the receiving end
(``hermes.service1_agent.agent_reactive.handle_ipc_command``), logging
"[IPC] Unknown event type" and never reaching ``event_bus.emit``. That broke
the reactive propagation path documented in ``ControlState``
(``update_with_event``) and ``main.py``'s ``_handle_mode_change`` /
``_handle_settings_changed`` subscribers, silently downgrading every settings
change to the 60s ``CONTROL_STATE_BACKSTOP_S`` poll instead of near-instant
reactive delivery.

Each test below is confirmed to fail when the fix (using
``CLASS_TO_EVENT_TYPE[ev.__class__]`` instead of ``ev.__class__.__name__`` or
a hardcoded class-name string) is reverted.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes.db.events import deserialize_event


def _patched_session(db, row=None):
    """Mock ``AsyncSession`` whose every ``execute(...)`` resolves to a plain
    (synchronous) result object — an unconfigured ``AsyncMock.execute()``
    return value is itself an ``AsyncMock``, so ``.scalars().first()`` would
    resolve to an unawaited coroutine instead of ``row``/``None``. This
    stands in for every incidental select along the write path (e.g. the
    ``SystemSetting``/``Strategy`` lookup inside event projections), not just
    the one the test cares about.
    """
    session_mock = AsyncMock()
    session_mock.add = MagicMock()
    session_mock.__aenter__.return_value = session_mock
    session_mock.__aexit__.return_value = False
    result_mock = MagicMock()
    result_mock.scalars.return_value.first.return_value = row
    session_mock.execute.return_value = result_mock
    return patch.object(db, "AsyncSession", return_value=session_mock)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key,value",
    [
        ("hermes_mode", "live"),
        ("agent_paused", "true"),
        ("agent_autonomy", "autonomous"),
        ("soul_md", "doctrine text"),
        ("strategy_cs75_enabled", "false"),
        ("some_other_setting", "42"),
    ],
)
async def test_set_setting_publishes_a_registry_resolvable_event_type(db, key, value):
    with _patched_session(db):
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            await db.settings.set_setting(key, value)

            mock_ipc.publish.assert_called_once()
            args, _ = mock_ipc.publish.call_args
            published_event_type = args[1]["event_type"]
            published_payload = args[1]["payload"]

            event = deserialize_event(published_event_type, published_payload)
            assert event is not None, (
                f"published event_type {published_event_type!r} for key={key!r} "
                "did not round-trip through deserialize_event — the receiving "
                "agent process would log '[IPC] Unknown event type' and drop it"
            )


@pytest.mark.asyncio
async def test_set_watchlist_publishes_a_registry_resolvable_event_type(db):
    with _patched_session(db):
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            await db.watchlist.set_watchlist("CS75", ["AAPL", "MSFT"])

            mock_ipc.publish.assert_called_once()
            args, _ = mock_ipc.publish.call_args
            published_event_type = args[1]["event_type"]
            published_payload = args[1]["payload"]

            event = deserialize_event(published_event_type, published_payload)
            assert event is not None
            assert event.strategy_id == "CS75"


@pytest.mark.asyncio
async def test_decide_approval_publishes_a_registry_resolvable_event_type(db):
    row = MagicMock()
    row.status = "PENDING"
    row.notes = None
    with _patched_session(db, row=row):
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            ok = await db.approvals.decide_approval(approval_id=42, decision="APPROVED")
            assert ok is True

            mock_ipc.publish.assert_called_once()
            args, _ = mock_ipc.publish.call_args
            published_event_type = args[1]["event_type"]
            published_payload = args[1]["payload"]

            event = deserialize_event(published_event_type, published_payload)
            assert event is not None
            assert event.approval_id == 42


@pytest.mark.asyncio
async def test_mark_approval_executed_publishes_a_registry_resolvable_event_type(db):
    row = MagicMock()
    with _patched_session(db, row=row):
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            await db.approvals.mark_approval_executed(approval_id=7, success=True)

            mock_ipc.publish.assert_called_once()
            args, _ = mock_ipc.publish.call_args
            published_event_type = args[1]["event_type"]
            published_payload = args[1]["payload"]

            event = deserialize_event(published_event_type, published_payload)
            assert event is not None
            assert event.approval_id == 7
