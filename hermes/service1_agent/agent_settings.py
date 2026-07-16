"""
[Service-1: Hermes-Agent-Core] — operator/runtime settings keys and reader.

Split out of ``main.py`` so the agent entry point keeps only the run loop and
process wiring. ``main`` re-imports these names, so existing call-sites and
test monkeypatches (``hermes.service1_agent.main.X``) keep working unchanged.
"""
from __future__ import annotations

from typing import Any, Dict

from hermes.common import (
    STRATEGY_PRIORITIES,
    VALID_AUTONOMY,
    normalize_overseer_mode,
)


# Settings keys shared with the watcher (see hermes/service2_watcher/api.py).
SETTING_MODE = "hermes_mode"               # "paper" | "live"
SETTING_TRADIER_OK_TS = "tradier_last_ok_ts"
SETTING_TRADIER_ERROR = "tradier_last_error"
SETTING_AGENT_STARTED_AT = "agent_started_at"

# LLM overseer settings — written by the watcher's /api/llm endpoints.
SETTING_LLM_PROVIDER = "llm_provider"           # "mock" | "local"
SETTING_LLM_BASE_URL = "llm_base_url"
SETTING_LLM_MODEL = "llm_model"
SETTING_LLM_API_KEY = "llm_api_key"             # often empty for LM Studio / Ollama
SETTING_LLM_TEMPERATURE = "llm_temperature"
SETTING_LLM_VISION = "llm_vision"               # "true" | "false"
SETTING_LLM_TIMEOUT = "llm_timeout_s"           # seconds; bump on cold-load setups
SETTING_LLM_OK_TS = "llm_last_ok_ts"
SETTING_LLM_ERROR = "llm_last_error"
# The provider actually wired into the overseer — "mock" whenever _build_llm
# fell back (missing key/model, build failure) regardless of the configured
# llm_provider. The watcher's health chip goes green only on a real provider.
SETTING_LLM_ACTIVE_PROVIDER = "llm_active_provider"

# Operator doctrine + agent control — written by the C2 panel.
SETTING_SOUL = "soul_md"
SETTING_AUTONOMY = "agent_autonomy"
SETTING_PAUSED = "agent_paused"
SETTING_APPROVAL_MODE = "approval_mode"   # "true" | "false"
SETTING_LLM_OUT_OF_LOOP = "llm_out_of_loop" # "true" | "false"
# Daily-loss kill switch. Dollar amount of realized loss (positive number) that
# auto-pauses the agent for the rest of the session. "" / "0" / unset disables.
# Falls back to the HERMES_MAX_DAILY_LOSS env var when no setting is stored.


# Per-strategy enable/disable flags — written by the C2 panel.
# Key pattern: "strategy_{id}_enabled"  value: "true" | "false"
def _strategy_enabled_key(strategy_id: str) -> str:
    return f"strategy_{strategy_id.lower()}_enabled"


async def _read_overseer_settings(db, conf: Dict[str, Any]) -> Dict[str, Any]:
    """Return the operator-driven overseer config (soul, autonomy, paused, approval_mode).

    Defaults pull from `conf` (env vars) the very first time so nothing
    surprising happens on first boot. After that, C2 panel writes win.
    """
    autonomy = ((await db.settings.get_setting(SETTING_AUTONOMY))
                or conf.get("ai_autonomy") or "advisory").lower()
    if autonomy not in VALID_AUTONOMY:
        autonomy = "advisory"
    soul = (await db.settings.get_setting(SETTING_SOUL)) or ""
    paused = ((await db.settings.get_setting(SETTING_PAUSED)) or "false").lower() == "true"
    approval_mode = ((await db.settings.get_setting(SETTING_APPROVAL_MODE)) or "true").lower() == "true"
    llm_out_of_loop = ((await db.settings.get_setting(SETTING_LLM_OUT_OF_LOOP)) or "true").lower() == "true"
    overseer_mode = normalize_overseer_mode(await db.settings.get_setting("overseer_mode"))
    # Per-strategy enable flags — default to enabled for all known strategies.
    strategy_enabled = {
        sid: ((await db.settings.get_setting(_strategy_enabled_key(sid))) or "true").lower() != "false"
        for sid in STRATEGY_PRIORITIES
    }
    return {
        "autonomy": autonomy,
        "soul": soul,
        "paused": paused,
        "approval_mode": approval_mode,
        "llm_out_of_loop": llm_out_of_loop,
        "overseer_mode": overseer_mode,
        "strategy_enabled": strategy_enabled,
    }
