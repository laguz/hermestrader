"""Unit tests for the centralized overseer-mode vocabulary in ``hermes.common``.

The selector (``HermesOverseer._consult``), the settings reader
(``agent_settings``), the in-memory control state, and the watcher API all
route mode handling through these helpers, so the legacy ``monolithic`` alias
and the unknown-value fallback live in exactly one place.
"""
from __future__ import annotations

import pytest

from hermes.common import (
    DEFAULT_OVERSEER_MODE,
    VALID_OVERSEER_MODES,
    canonical_overseer_mode,
    normalize_overseer_mode,
)


@pytest.mark.parametrize("value, expected", [
    ("single", "single"),
    ("committee", "committee"),
    ("COMMITTEE", "committee"),       # case-insensitive
    ("  Committee  ", "committee"),   # trimmed
    ("monolithic", "single"),         # legacy alias
    ("MONOLITHIC", "single"),
    # Unknown / empty → None (caller decides how to handle).
    ("", None),
    (None, None),
    ("comittee", None),               # typo, not silently accepted
    ("ensemble", None),
])
def test_canonical_overseer_mode(value, expected):
    assert canonical_overseer_mode(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("committee", "committee"),
    ("monolithic", "single"),
    ("", DEFAULT_OVERSEER_MODE),
    (None, DEFAULT_OVERSEER_MODE),
    ("comittee", DEFAULT_OVERSEER_MODE),   # typo → safe default, never crashes
])
def test_normalize_overseer_mode(value, expected):
    assert normalize_overseer_mode(value) == expected


def test_default_mode_is_valid():
    assert DEFAULT_OVERSEER_MODE in VALID_OVERSEER_MODES


def test_normalize_always_returns_a_valid_mode():
    for value in ["single", "committee", "monolithic", "", None, "junk", "  "]:
        assert normalize_overseer_mode(value) in VALID_OVERSEER_MODES
