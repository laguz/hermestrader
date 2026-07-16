"""Regression tests: the overseer health chip must only go green for a real LLM.

MockLLM's chat() always succeeds instantly, and ``_mark_llm_ok`` refreshes
``llm_last_ok_ts`` on every successful call — so freshness alone showed the
operator "gemini healthy" while the agent had silently fallen back to the mock
(e.g. provider configured but api_key missing). ``_build_llm`` now records the
provider it *actually* wired under ``llm_active_provider`` ("mock" on any
fallback), and the watcher's ``llm_ok`` roll-up refuses to go green when the
active provider is the mock.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from hermes.service1_agent.agent_construction import _build_llm
from hermes.service1_agent.agent_settings import SETTING_LLM_ACTIVE_PROVIDER
from hermes.service1_agent.mock_broker import MockLLM


def _db(settings: dict):
    db = MagicMock()
    written: dict = {}

    async def get_setting(key):
        return settings.get(key)

    async def set_setting(key, value):
        written[key] = value

    db.settings.get_setting = AsyncMock(side_effect=get_setting)
    db.settings.set_setting = AsyncMock(side_effect=set_setting)
    return db, written


async def test_gemini_without_api_key_records_mock_fallback():
    db, written = _db({"llm_provider": "gemini", "llm_model": "gemini-2.5-flash"})
    client, snapshot, _ = await _build_llm(db)
    assert isinstance(client, MockLLM)
    assert written.get(SETTING_LLM_ACTIVE_PROVIDER) == "mock"
    assert snapshot["active_provider"] == "mock"


async def test_gemini_with_api_key_records_real_provider():
    db, written = _db({
        "llm_provider": "gemini",
        "llm_model": "gemini-2.5-flash",
        "llm_api_key": "test-key",
    })
    client, snapshot, _ = await _build_llm(db)
    assert not isinstance(client, MockLLM)
    assert written.get(SETTING_LLM_ACTIVE_PROVIDER) == "gemini"
    assert snapshot["active_provider"] == "gemini"


async def test_mock_provider_records_mock():
    db, written = _db({"llm_provider": "mock"})
    client, snapshot, _ = await _build_llm(db)
    assert isinstance(client, MockLLM)
    assert written.get(SETTING_LLM_ACTIVE_PROVIDER) == "mock"
    assert snapshot["active_provider"] == "mock"
