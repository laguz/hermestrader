"""Unit tests for the centralized overseer-mode vocabulary in ``hermes.common``.

The selector (``HermesOverseer._consult``), the settings reader
(``agent_settings``), the in-memory control state, and the watcher API all
route mode handling through these helpers, so the vocabulary and the
unknown-value fallback live in exactly one place.

Phase 0 ships a single review mode (``single``); any other stored value
(including the retired ``committee`` / ``monolithic`` rows) is unrecognised and
resolves to the default.
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
    ("SINGLE", "single"),             # case-insensitive
    ("  Single  ", "single"),         # trimmed
    # Unknown / empty / retired → None (caller decides how to handle).
    ("", None),
    (None, None),
    ("committee", None),              # retired mode, no longer accepted
    ("ensemble", None),
    ("monolithic", None),            # retired pre-rename value, no longer an alias
])
def test_canonical_overseer_mode(value, expected):
    assert canonical_overseer_mode(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("single", "single"),
    ("", DEFAULT_OVERSEER_MODE),
    (None, DEFAULT_OVERSEER_MODE),
    ("committee", DEFAULT_OVERSEER_MODE),   # retired → safe default, never crashes
    # A legacy ``monolithic`` row still resolves to single — now via the
    # unknown-value default, not a dedicated alias.
    ("monolithic", DEFAULT_OVERSEER_MODE),
])
def test_normalize_overseer_mode(value, expected):
    assert normalize_overseer_mode(value) == expected


def test_default_mode_is_valid():
    assert DEFAULT_OVERSEER_MODE in VALID_OVERSEER_MODES


def test_normalize_always_returns_a_valid_mode():
    for value in ["single", "committee", "monolithic", "", None, "junk", "  "]:
        # "committee"/"monolithic" are retired values, included here to prove
        # they still normalize to a valid mode rather than crashing.
        assert normalize_overseer_mode(value) in VALID_OVERSEER_MODES
