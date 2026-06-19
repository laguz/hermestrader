"""Guards the provider-agnostic LLM layer against silent drift.

The overseer consumes any object with ``.chat(messages, images=...)``; the
*only* place that maps an operator-selected provider to a concrete client is
``_build_llm``. The provider vocabulary, however, lives in three spots that
must stay in sync: ``VALID_LLM_PROVIDERS`` (what the watcher API accepts),
``LLM_PROVIDER_BASE_URLS`` (hosted endpoints), and ``_build_llm``'s branches.

If a provider is added to ``VALID_LLM_PROVIDERS`` but never wired into
``_build_llm``, the watcher would accept it and the agent would silently fall
back to ``MockLLM`` — a real order-placing system running on a no-op overseer.
These tests fail loudly in that case.
"""
from __future__ import annotations

import pytest

from hermes.common import VALID_LLM_PROVIDERS
from hermes.llm import OllamaCloudLLM, OpenAICompatibleLLM
from hermes.service1_agent.agent_construction import _build_llm
from hermes.service1_agent.mock_broker import MockLLM

from ._stubs import StubDB


# Expected concrete client per provider, plus the minimal valid config each
# needs to build successfully. Keep this in lockstep with VALID_LLM_PROVIDERS —
# the first test enforces that.
_EXPECTED = {
    "mock":         (MockLLM,             {}),
    "local":        (OpenAICompatibleLLM, {"llm_base_url": "http://localhost:1234/v1",
                                           "llm_model": "m"}),
    "gemini":       (OpenAICompatibleLLM, {"llm_model": "gemini-x", "llm_api_key": "k"}),
    "claude":       (OpenAICompatibleLLM, {"llm_model": "claude-x", "llm_api_key": "k"}),
    "ollama_cloud": (OllamaCloudLLM,      {"llm_model": "gpt-oss", "llm_api_key": "k"}),
}


def test_every_valid_provider_has_a_build_mapping():
    """Adding a provider to the vocabulary must come with a build expectation —
    otherwise the new provider isn't actually exercised below."""
    assert set(_EXPECTED) == set(VALID_LLM_PROVIDERS)


def test_clients_are_exported_from_the_package():
    """Both shipped transports are part of the package's public API (the
    construction code and tests import them from ``hermes.llm``, not the
    submodule)."""
    import hermes.llm as pkg
    assert {"OpenAICompatibleLLM", "OllamaCloudLLM"} <= set(pkg.__all__)


@pytest.mark.parametrize("provider", list(_EXPECTED))
async def test_provider_builds_expected_client(provider):
    expected_cls, config = _EXPECTED[provider]
    db = StubDB()
    db.settings["llm_provider"] = provider
    for key, value in config.items():
        db.settings[key] = value

    client, snapshot, _vision = await _build_llm(db)

    assert isinstance(client, expected_cls), (
        f"provider {provider!r} built {type(client).__name__}, expected "
        f"{expected_cls.__name__}"
    )
    assert snapshot["provider"] == provider
    # Non-mock providers must NOT silently degrade to the MockLLM fallback.
    if provider != "mock":
        assert not isinstance(client, MockLLM)
