"""Operator toggle for the outcome-driven learning subsystems.

Verifies GET reports current modes (defaulting to off) and PUT validates and
writes ``bandit_tuner_mode`` / ``exit_policy_mode`` via system_settings.
"""
from __future__ import annotations
from ._stubs import alias_db_namespaces

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from hermes.service2_watcher.routes import agent as agent_routes
from hermes.service2_watcher.routes.agent import (
    LearningBody, get_learning, set_learning,
)


@pytest.mark.asyncio
async def test_get_learning_defaults_to_off():
    mock_db = AsyncMock()
    alias_db_namespaces(mock_db)
    mock_db.get_setting.return_value = None                # nothing set yet
    with patch.object(agent_routes, "db", mock_db):
        out = await get_learning()
    assert out["bandit_tuner_mode"] == "off"
    assert out["exit_policy_mode"] == "off"
    assert out["valid_modes"] == ["active", "off", "shadow"]


@pytest.mark.asyncio
async def test_get_learning_reports_current_modes():
    mock_db = AsyncMock()
    alias_db_namespaces(mock_db)
    mock_db.get_setting.side_effect = ["shadow", "active"]  # bandit, exit
    with patch.object(agent_routes, "db", mock_db):
        out = await get_learning()
    assert out == {
        "bandit_tuner_mode": "shadow",
        "exit_policy_mode": "active",
        "valid_modes": ["active", "off", "shadow"],
    }


@pytest.mark.asyncio
async def test_set_learning_writes_both_modes():
    mock_db = AsyncMock()
    alias_db_namespaces(mock_db)
    with patch.object(agent_routes, "db", mock_db):
        out = await set_learning(
            LearningBody(bandit_tuner_mode="shadow", exit_policy_mode="shadow"))
    assert out["updated"] == {"bandit_tuner_mode": "shadow",
                              "exit_policy_mode": "shadow"}
    # The watcher enqueues operator intents; the agent applies them (single
    # writer). It must not write system_settings directly anymore.
    mock_db.enqueue_setting.assert_any_call("bandit_tuner_mode", "shadow")
    mock_db.enqueue_setting.assert_any_call("exit_policy_mode", "shadow")
    mock_db.set_setting.assert_not_called()


@pytest.mark.asyncio
async def test_set_learning_updates_one_leaves_other_untouched():
    mock_db = AsyncMock()
    alias_db_namespaces(mock_db)
    with patch.object(agent_routes, "db", mock_db):
        out = await set_learning(LearningBody(exit_policy_mode="off"))
    assert out["updated"] == {"exit_policy_mode": "off"}
    # Only the exit key was enqueued.
    keys = [c.args[0] for c in mock_db.enqueue_setting.call_args_list]
    assert keys == ["exit_policy_mode"]


@pytest.mark.asyncio
async def test_set_learning_rejects_invalid_mode():
    mock_db = AsyncMock()
    alias_db_namespaces(mock_db)
    with patch.object(agent_routes, "db", mock_db):
        with pytest.raises(HTTPException) as exc:
            await set_learning(LearningBody(bandit_tuner_mode="on"))
    assert exc.value.status_code == 400
    mock_db.enqueue_setting.assert_not_called()


@pytest.mark.asyncio
async def test_set_learning_requires_a_field():
    mock_db = AsyncMock()
    alias_db_namespaces(mock_db)
    with patch.object(agent_routes, "db", mock_db):
        with pytest.raises(HTTPException) as exc:
            await set_learning(LearningBody())
    assert exc.value.status_code == 400
