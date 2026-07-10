"""
hermes/common.py — Single source of truth for constants shared between
service1_agent (main.py, strategies.py) and service2_watcher (api.py).

Import from here rather than redefining in each module so a future
change only needs to happen in one place.
"""
from __future__ import annotations

import re as _re
from typing import Optional as _Optional


# ---------------------------------------------------------------------------
# Trading modes
# ---------------------------------------------------------------------------
VALID_MODES = ("paper", "live")

# ---------------------------------------------------------------------------
# Strategy registry — order defines cascading priority (1 = highest).
# Must stay in sync with the strategies registered in service1_agent/main.py.
# ---------------------------------------------------------------------------
STRATEGIES: tuple[str, ...] = ("CS75", "CS7", "TT45", "WHEEL", "HERMESALPHA", "DS0")
STRATEGY_PRIORITIES: dict[str, int] = {"CS75": 1, "CS7": 2, "TT45": 3, "WHEEL": 4, "HERMESALPHA": 5, "DS0": 6}

# ---------------------------------------------------------------------------
# LLM / Overseer
# ---------------------------------------------------------------------------
VALID_LLM_PROVIDERS: tuple[str, ...] = (
    "mock", "local", "ollama_cloud", "gemini", "claude",
)
VALID_AUTONOMY: tuple[str, ...] = ("advisory", "enforcing", "autonomous")

# Overseer review path. Phase 0 ships a single review mode (``single`` — one LLM
# call); the multi-agent committee mode is admitted only once single review
# shows a measurable blind spot (see REBUILD.md). The two helpers below are the
# *single* place that knows this vocabulary — settings reads, the watcher API,
# and the review router all route through them so the mode handling can't drift
# apart. Any other value still stored in ``system_settings`` (e.g. a retired
# ``committee`` / ``monolithic`` row) is unrecognised here and resolves to
# ``DEFAULT_OVERSEER_MODE`` (``single``) via :func:`normalize_overseer_mode`.
VALID_OVERSEER_MODES: tuple[str, ...] = ("single",)
DEFAULT_OVERSEER_MODE = "single"


def canonical_overseer_mode(value):
    """Resolve ``value`` to a canonical overseer mode, or ``None`` if unknown.

    Lowercases/trims. Returns ``None`` for empty or unrecognised values so
    callers that want to reject bad input (e.g. the watcher API) can; callers
    that want a safe default should use :func:`normalize_overseer_mode`.
    """
    mode = (value or "").strip().lower()
    return mode if mode in VALID_OVERSEER_MODES else None


def normalize_overseer_mode(value) -> str:
    """Canonical overseer mode, falling back to :data:`DEFAULT_OVERSEER_MODE`
    for empty/unknown values. Use for settings reads and the review router."""
    return canonical_overseer_mode(value) or DEFAULT_OVERSEER_MODE

# Default OpenAI-compatible base URLs for hosted providers. Ollama Cloud,
# Gemini and Claude all expose an OpenAI `/chat/completions` shim, so the
# agent reuses the OpenAICompatibleLLM client by pointing it at these
# endpoints. The operator only supplies an API key and picks a model — the
# URL is pre-filled so they can't get it subtly wrong.
LLM_PROVIDER_BASE_URLS: dict[str, str] = {
    "ollama_cloud": "https://api.ollama.com/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai",
    "claude": "https://api.anthropic.com/v1",
}

# Default LLM call timeout — generous for local models cold-loading multi-GB
# GGUF weights (LM Studio / Ollama on consumer hardware).
DEFAULT_LLM_TIMEOUT_S: float = 120.0

# ---------------------------------------------------------------------------
# Cross-service IPC (Redis pub/sub) contract
# ---------------------------------------------------------------------------
# One multiplexed channel carries signals from the watcher (service-2,
# publisher) to the agent (service-1, subscriber). Two payload shapes ride it:
#   * command signals    — ``{"action": <IPC_ACTION_*>}``
#   * event notifications — ``{"event_type": <name>, "payload": {...}}``
#     (``event_type`` is resolved back to a class via ``EVENT_TYPE_TO_CLASS``,
#      so those names are already centralized by the event registry.)
# The channel name and the free-form action vocabulary are pinned here so the
# publisher and subscriber can't drift on a typo.
IPC_CHANNEL_AGENT_COMMANDS = "agent_commands"
IPC_ACTION_TRIGGER_APPROVALS = "trigger_approvals"
IPC_ACTION_SYNC_SETTINGS = "sync_settings"
IPC_ACTION_TRIGGER_ML = "trigger_ml"
# Nudge the agent to drain the durable ``operator_commands`` queue now. The row
# is the source of truth; this signal only shortens the latency to apply.
IPC_ACTION_DRAIN_COMMANDS = "drain_commands"

# ---------------------------------------------------------------------------
# OCC option symbol regex — shared between MoneyManager broker-order parsing,
# the pending-order side derivation in HermesDB, and tests.  Centralised here
# so a change to the OCC format (or its strict-match policy) only needs to
# land in one place.  Format: SYMBOL YYMMDD P|C STRIKE(8 digits, padded).
# Example: ``AAPL250620P00150000`` → underlying=AAPL exp=2025-06-20 put $150.
# ---------------------------------------------------------------------------
OCC_RE = _re.compile(r"^([A-Z]+)(\d{6})([PC])(\d{8})$")

# ---------------------------------------------------------------------------
# Order tag contract — the single place that knows the ``HERMES`` tag shape
# and the Tradier ``_``↔``-`` sanitiser quirk.
#
# Hermes tags every order it places: entries as ``HERMES_<STRAT>`` and closes
# as ``HERMES_<STRAT>_CLOSE_<REASON>`` (e.g. ``HERMES_CS75_CLOSE_TP-50``).
# Tradier's tag sanitiser rewrites ``_`` to ``-`` on the wire, so the same tag
# round-trips back as ``HERMES-<STRAT>`` / ``HERMES-<STRAT>-CLOSE-<REASON>``.
# Every matcher MUST go through the helpers below so no call-site re-derives
# the separator handling and silently forgets a form. See CLAUDE.md safety
# rule #5.
# ---------------------------------------------------------------------------
HERMES_TAG_PREFIX = "HERMES"


def strategy_id_from_tag(tag: _Optional[str]) -> _Optional[str]:
    """Extract the strategy id from a Hermes order tag.

    Accepts either separator form (``HERMES_CS75`` or ``HERMES-CS75``) since
    Tradier rewrites ``_``→``-`` on the round-trip. Returns ``None`` for empty
    tags, non-Hermes tags, or a bare ``HERMES`` prefix with no strategy.
    """
    if not tag:
        return None
    normalised = str(tag).replace("_", "-")
    prefix = HERMES_TAG_PREFIX + "-"
    if not normalised.startswith(prefix):
        return None
    strategy_id = normalised[len(prefix):].split("-", 1)[0]
    return strategy_id or None


def close_reason_from_tag(tag: _Optional[str]) -> _Optional[str]:
    """Recover the close reason a strategy embedded in a close-order tag.

    Closes are tagged ``HERMES_<STRAT>_CLOSE_<REASON>``; accept either
    separator around the ``CLOSE`` marker for the Tradier round-trip. Returns
    ``None`` when the tag carries no close reason (e.g. an entry tag).
    """
    if not tag:
        return None
    norm = str(tag).replace("-", "_")
    marker = "_CLOSE_"
    idx = norm.find(marker)
    if idx == -1:
        return None
    suffix = norm[idx + len(marker):].strip()
    return suffix or None


def is_close_tag(tag: _Optional[str]) -> bool:
    """True if ``tag`` marks a position-closing order.

    Closes are tagged ``HERMES_<STRAT>_CLOSE_<REASON>``; accept either
    separator form for the Tradier ``_``↔``-`` round-trip. Matches ``CLOSE`` as
    a whole tag field so an entry tag (or a symbol that merely contains the
    letters) doesn't false-positive. Returns ``False`` for entry/empty tags.
    """
    if not tag:
        return False
    return "CLOSE" in str(tag).replace("-", "_").split("_")
