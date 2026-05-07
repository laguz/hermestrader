"""
hermes/common.py — Single source of truth for constants shared between
service1_agent (main.py, strategies.py) and service2_watcher (api.py).

Import from here rather than redefining in each module so a future
change only needs to happen in one place.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Trading modes
# ---------------------------------------------------------------------------
VALID_MODES = ("paper", "live")

# ---------------------------------------------------------------------------
# Strategy registry — order defines cascading priority (1 = highest).
# Must stay in sync with the strategies registered in service1_agent/main.py.
# ---------------------------------------------------------------------------
STRATEGIES: tuple[str, ...] = ("CS75", "CS7", "TT45", "WHEEL")
STRATEGY_PRIORITIES: dict[str, int] = {"CS75": 1, "CS7": 2, "TT45": 3, "WHEEL": 4}

# ---------------------------------------------------------------------------
# LLM / Overseer
# ---------------------------------------------------------------------------
VALID_LLM_PROVIDERS: tuple[str, ...] = ("mock", "local", "ollama_cloud")
VALID_AUTONOMY: tuple[str, ...] = ("advisory", "enforcing", "autonomous")

# Default LLM call timeout — generous for local models cold-loading multi-GB
# GGUF weights (LM Studio / Ollama on consumer hardware).
DEFAULT_LLM_TIMEOUT_S: float = 120.0

# ---------------------------------------------------------------------------
# OCC option symbol regex — shared between MoneyManager broker-order parsing,
# the pending-order side derivation in HermesDB, and tests.  Centralised here
# so a change to the OCC format (or its strict-match policy) only needs to
# land in one place.  Format: SYMBOL YYMMDD P|C STRIKE(8 digits, padded).
# Example: ``AAPL250620P00150000`` → underlying=AAPL exp=2025-06-20 put $150.
# ---------------------------------------------------------------------------
import re as _re
OCC_RE = _re.compile(r"^([A-Z]+)(\d{6})([PC])(\d{8})$")
