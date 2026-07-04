"""Unit tests for the centralized order-tag matchers in ``hermes.common``.

These pin the ``HERMES`` tag contract and the Tradier ``_``↔``-`` sanitiser
quirk in one place. Every matcher in the codebase routes through these helpers
(broker-order sync, realized-PnL close-reason recovery), so a regression here
is a regression everywhere — see CLAUDE.md safety rule #5.
"""
from __future__ import annotations

from pathlib import Path as _Path
from re import compile as _re_compile

import pytest

from hermes.common import (
    close_reason_from_tag,
    is_close_tag,
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


@pytest.mark.parametrize("tag, expected", [
    # Close tags, both separator forms.
    ("HERMES_CS75_CLOSE_TP-50", True),
    ("HERMES-CS75-CLOSE-TP-50", True),
    ("HERMES_TT45_CLOSE_AI", True),
    ("HERMES_HermesAlpha_CLOSE_EXIT-POLICY-REACTIVE", True),
    # Entry tags are not closes.
    ("HERMES_CS75", False),
    ("HERMES-CS75", False),
    ("HERMES_WHEEL", False),
    # CLOSE must be a whole field, not an incidental substring.
    ("HERMES_CLOSET", False),
    ("HERMES_FORECLOSE_X", False),
    # Empty / non-Hermes.
    ("", False),
    (None, False),
    ("manual-trade", False),
])
def test_is_close_tag(tag, expected):
    assert is_close_tag(tag) is expected


def test_is_close_tag_agrees_with_close_reason():
    """A tag has a recoverable close reason iff it's a close tag."""
    for tag in ("HERMES_CS75_CLOSE_TP-50", "HERMES-CS75-CLOSE-TP-50",
                "HERMES_CS75", "HERMES-CS75", "", None):
        assert is_close_tag(tag) is (close_reason_from_tag(tag) is not None)


def test_orm_helper_delegates_to_common():
    """The ORM's private helper is a thin re-export of the common matcher."""
    from hermes.db.orm import _close_reason_from_tag
    assert _close_reason_from_tag("HERMES-CS75-CLOSE-TP-50") == close_reason_from_tag(
        "HERMES-CS75-CLOSE-TP-50"
    )


# ---------------------------------------------------------------------------
# Guard: no module may re-derive the tag separator handling inline.
#
# The matchers above are the *only* place allowed to know about the Tradier
# ``_``↔``-`` quirk. Behavioural tests can't catch a new call-site that opens
# a bypass — they only test the helpers. This source-level guard fails the
# build if any module under ``hermes/`` (other than the contract itself)
# re-implements the separator handling inline, so a future matcher can't
# silently forget one of the two forms. See CLAUDE.md safety rule #5.
# ---------------------------------------------------------------------------

# Each pattern is a signature of inline tag parsing that bypasses the helpers.
# Order-*construction* (f"HERMES_{NAME}", f"...{NAME}_CLOSE_{reason}") never
# matches: these require a quote adjacent to the separator, which only parsing
# literals have. The outbound Tradier sanitiser (a char-class ``re.sub``) and
# the canonical ``marker = "_CLOSE_"`` live in their own files and are exempt.
_BYPASS_SIGNATURES = {
    "separator swap": _re_compile(r"""\.replace\(\s*["'][-_]["']\s*,\s*["'][-_]["']"""),
    "inline HERMES prefix match": _re_compile(r"""\.startswith\(\s*["']HERMES[-_]"""),
    "inline CLOSE-field literal": _re_compile(r"""["'][-_]CLOSE[-_]["']"""),
}

# Modules permitted to know the raw tag shape: the contract and nothing else.
_TAG_CONTRACT_FILES = {"common.py"}


def test_no_module_reimplements_tag_separator_handling():
    import hermes

    pkg_root = _Path(hermes.__file__).resolve().parent
    offenders = []
    for path in pkg_root.rglob("*.py"):
        if path.name in _TAG_CONTRACT_FILES:
            continue
        source = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(source.splitlines(), start=1):
            for label, pattern in _BYPASS_SIGNATURES.items():
                if pattern.search(line):
                    rel = path.relative_to(pkg_root.parent)
                    offenders.append(f"{rel}:{lineno} ({label}): {line.strip()}")

    assert not offenders, (
        "Inline order-tag parsing detected — route through the helpers in "
        "hermes.common (strategy_id_from_tag / is_close_tag / "
        "close_reason_from_tag) so both the HERMES_ and HERMES- forms stay "
        "handled in one place (CLAUDE.md safety rule #5):\n  "
        + "\n  ".join(offenders)
    )
