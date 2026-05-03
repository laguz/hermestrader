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
