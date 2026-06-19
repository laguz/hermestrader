"""Unit tests for the centralized order-tag matchers in ``hermes.common``.

These pin the ``HERMES`` tag contract and the Tradier ``_``↔``-`` sanitiser
quirk in one place. Every matcher in the codebase routes through these helpers
(broker-order sync, realized-PnL close-reason recovery), so a regression here
is a regression everywhere — see CLAUDE.md safety rule #5.
"""
from __future__ import annotations

import pytest

from hermes.common import (
    close_reason_from_tag,
    is_hermes_tag,
    strategy_id_from_tag,
)


@pytest.mark.parametrize("tag, expected", [
    # Underscore form (as Hermes places it).
    ("HERMES_CS75", "CS75"),
    ("HERMES_TT45", "TT45"),
    ("HERMES_HermesAlpha", "HermesAlpha"),  # case preserved
    # Hyphen form (as Tradier rewrites it on the round-trip).
    ("HERMES-CS75", "CS75"),
    ("HERMES-TT45", "TT45"),
    # Suffixes after the strategy id are stripped, either separator.
    ("HERMES_WHEEL_v1", "WHEEL"),
    ("HERMES-CS7-2023-10-27", "CS7"),
    ("HERMES_CS75_CLOSE_TP-50", "CS75"),
    ("HERMES-CS75-CLOSE-TP-50", "CS75"),
    # Non-matches.
    ("", None),
    (None, None),
    ("HERMES_", None),        # prefix only, no strategy
    ("HERMES-", None),
    ("HERMES", None),         # no separator at all
    ("NOTHERMES_CS75", None),
    ("manual-trade", None),
])
def test_strategy_id_from_tag(tag, expected):
    assert strategy_id_from_tag(tag) == expected


@pytest.mark.parametrize("tag, expected", [
    ("HERMES_CS75", True),
    ("HERMES-CS75", True),  # both separator forms count as Hermes tags
    ("HERMES_CS75_CLOSE_TP-50", True),
    ("HERMES_", False),     # prefix only, no strategy
    ("", False),
    (None, False),
    ("manual", False),
])
def test_is_hermes_tag(tag, expected):
    assert is_hermes_tag(tag) is expected


@pytest.mark.parametrize("tag, expected", [
    # Both separator forms recover the same reason.
    ("HERMES_CS75_CLOSE_TP-50", "TP_50"),
    ("HERMES-CS75-CLOSE-TP-50", "TP_50"),
    ("HERMES_CS75_CLOSE_AI", "AI"),
    ("HERMES_CS75_CLOSE_SL-2.5x", "SL-2.5x".replace("-", "_")),
    # Entry tags carry no close reason.
    ("HERMES_CS75", None),
    ("HERMES-CS75", None),
    ("", None),
    (None, None),
])
def test_close_reason_from_tag(tag, expected):
    assert close_reason_from_tag(tag) == expected


def test_close_reason_separator_round_trip_is_stable():
    """The underscore and hyphen forms of a tag yield identical reasons."""
    under = close_reason_from_tag("HERMES_CS75_CLOSE_EXIT-POLICY-REACTIVE")
    hyphen = close_reason_from_tag("HERMES-CS75-CLOSE-EXIT-POLICY-REACTIVE")
    assert under == hyphen


def test_orm_helper_delegates_to_common():
    """The ORM's private helper is a thin re-export of the common matcher."""
    from hermes.db.orm import _close_reason_from_tag
    assert _close_reason_from_tag("HERMES-CS75-CLOSE-TP-50") == close_reason_from_tag(
        "HERMES-CS75-CLOSE-TP-50"
    )
