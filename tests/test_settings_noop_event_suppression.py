"""Regression tests for the 2026-07-09 settings feedback loop.

PR #219 fixed IPC event publishes to use registry event types
(``CLASS_TO_EVENT_TYPE``) instead of class names. That made the agent's own
``SYSTEM_SETTING_CHANGED`` publishes deserializable for the first time — and
armed a self-sustaining feedback loop that the class-name bug had been
accidentally breaking:

    set_setting → IPC publish → agent_reactive.handle_ipc_command
    → event_bus.emit(SystemSettingChangedEvent)
    → main._handle_settings_changed → _build_llm
    → set_setting("llm_last_error", "")   ← same value, every time
    → IPC publish → …

Observed live on the paper agent at ~50 ``llm_last_error`` writes/second,
flooding ``event_ledger`` (13k+ rows in minutes) until the process wedged.

The fix: ``SettingsRepository.set_setting`` is a no-op when the stored value
is unchanged — no event recorded, nothing published. Nothing changed, so
there is no "changed" event to emit; the loop's rewrite of an identical
status value terminates instead of re-triggering itself.

Each test is confirmed to fail when the early-return in ``set_setting`` is
reverted.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes.db.events import deserialize_event


def _patched_session(db, row=None):
    """Mock ``AsyncSession`` whose every ``execute(...)`` resolves to a plain
    (synchronous) result object holding ``row`` (see the identical helper in
    ``test_ipc_event_type_registry_mismatch.py``)."""
    session_mock = AsyncMock()
    session_mock.add = MagicMock()
    session_mock.__aenter__.return_value = session_mock
    session_mock.__aexit__.return_value = False
    result_mock = MagicMock()
    result_mock.scalars.return_value.first.return_value = row
    session_mock.execute.return_value = result_mock
    return patch.object(db, "AsyncSession", return_value=session_mock), session_mock


@pytest.mark.asyncio
async def test_unchanged_value_records_and_publishes_nothing(db):
    row = MagicMock()
    row.value = ""
    ctx, session = _patched_session(db, row=row)
    with ctx:
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            await db.settings.set_setting("llm_last_error", "")

            mock_ipc.publish.assert_not_called()
            session.commit.assert_not_called()
            session.add.assert_not_called()


@pytest.mark.asyncio
async def test_unchanged_special_key_is_also_suppressed(db):
    # The special-key branches (agent_paused → PauseChangedEvent, etc.) sit
    # after the same guard; an unchanged toggle must not emit either.
    row = MagicMock()
    row.value = "true"
    ctx, session = _patched_session(db, row=row)
    with ctx:
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            await db.settings.set_setting("agent_paused", "true")

            mock_ipc.publish.assert_not_called()
            session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_changed_value_still_records_and_publishes(db):
    row = MagicMock()
    row.value = "old error text"
    ctx, session = _patched_session(db, row=row)
    with ctx:
        with patch("hermes.ipc.ipc") as mock_ipc:
            mock_ipc.publish = AsyncMock()
            await db.settings.set_setting("llm_last_error", "")

            mock_ipc.publish.assert_called_once()
            session.commit.assert_called_once()
            args, _ = mock_ipc.publish.call_args
            event = deserialize_event(args[1]["event_type"], args[1]["payload"])
            assert event is not None
            assert event.key == "llm_last_error"
            assert event.value == ""


@pytest.mark.asyncio
async def test_repeat_write_appends_a_single_ledger_event(db):
    """End-to-end against a real DB: the second identical write is a no-op.

    This is the loop-breaking property itself — the handler's rewrite of an
    identical status value must not append another SYSTEM_SETTING_CHANGED
    event (which would be published and re-trigger the handler).
    """
    from sqlalchemy import func, select

    from hermes.db.orm import EventLedger

    with patch("hermes.ipc.ipc") as mock_ipc:
        mock_ipc.publish = AsyncMock()
        await db.settings.set_setting("llm_last_error", "")
        await db.settings.set_setting("llm_last_error", "")

        assert mock_ipc.publish.call_count == 1

    async with db.AsyncSession() as s:
        n = (await s.execute(
            select(func.count()).select_from(EventLedger)
            .where(EventLedger.event_type == "SYSTEM_SETTING_CHANGED"))).scalar()
    assert n == 1

    assert await db.settings.get_setting("llm_last_error") == ""
