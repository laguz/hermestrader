"""
[Service-1: Hermes-Agent-Core] — broker / LLM / engine construction.

Split out of ``main.py`` so the agent entry point keeps only the run loop and
process wiring. ``main`` re-imports these names, so existing call-sites and
test monkeypatches (``hermes.service1_agent.main.X``) keep working unchanged.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from hermes.common import DEFAULT_LLM_TIMEOUT_S, LLM_PROVIDER_BASE_URLS
from hermes.utils import decrypt_value
from hermes.db.models import HermesDB
from hermes.service1_agent.core import (
    CascadingEngine, IronCondorBuilder, MoneyManager,
)
from hermes.service1_agent.overseer import HermesOverseer
from hermes.service1_agent.strategies import (
    CreditSpreads7, CreditSpreads75, HermesAlpha, TastyTrade45, WheelStrategy,
)

from .agent_settings import (
    SETTING_LLM_API_KEY, SETTING_LLM_BASE_URL, SETTING_LLM_ERROR,
    SETTING_LLM_MODEL, SETTING_LLM_PROVIDER, SETTING_LLM_TEMPERATURE,
    SETTING_LLM_TIMEOUT, SETTING_LLM_VISION,
)

log = logging.getLogger("hermes.agent.main")


def _live_armed() -> bool:
    """True when the operator has explicitly armed real-money live orders.

    Going live is gated behind ``HERMES_LIVE_ARMED=true`` so that merely setting
    the mode to "live" never routes real orders by accident. Without arming,
    live mode runs preview-only (dry_run). Accepts the usual truthy spellings.
    """
    return os.environ.get("HERMES_LIVE_ARMED", "").strip().lower() in (
        "1", "true", "yes", "on",
    )


async def _build_llm(db) -> Tuple[Any, Dict[str, Any], bool]:
    """Build the LLM overseer client from current settings.

    Returns (client, snapshot, vision_enabled). `snapshot` is the dict of
    config values used so the tick loop can detect changes and rebuild.
    """
    provider = ((await db.get_setting(SETTING_LLM_PROVIDER)) or "mock").lower()
    base_url = ((await db.get_setting(SETTING_LLM_BASE_URL)) or "").strip()
    model = ((await db.get_setting(SETTING_LLM_MODEL)) or "").strip()
    api_key = decrypt_value(((await db.get_setting(SETTING_LLM_API_KEY)) or "").strip()) or None
    temperature_raw = ((await db.get_setting(SETTING_LLM_TEMPERATURE)) or "0.2").strip()
    try:
        temperature = float(temperature_raw)
    except ValueError:
        temperature = 0.2
    timeout_raw = ((await db.get_setting(SETTING_LLM_TIMEOUT)) or str(DEFAULT_LLM_TIMEOUT_S)).strip()
    try:
        timeout_s = max(5.0, float(timeout_raw))
    except ValueError:
        timeout_s = DEFAULT_LLM_TIMEOUT_S
    vision = ((await db.get_setting(SETTING_LLM_VISION)) or "true").lower() != "false"
    snapshot = {
        "provider": provider,
        "base_url": base_url,
        "model": model,
        # Store a hash of the key, not just bool, so that updating the key
        # value (e.g. wrong → correct) is detected as a config change and
        # triggers an LLM client rebuild on the next tick.
        "api_key_hash": hash(api_key or ""),
        "temperature": temperature,
        "timeout_s": timeout_s,
        "vision": vision,
    }

    if provider == "ollama_cloud":
        # Use the native Ollama Python library — Ollama Cloud auth works
        # differently from the OpenAI-compatible shim and requires the
        # official client (as documented at api.ollama.com).
        if not model or not api_key:
            log.warning("ollama_cloud requires both model and api_key — falling back to MockLLM")
        else:
            try:
                from hermes.llm.clients import OllamaCloudLLM
                client = OllamaCloudLLM(
                    model=model,
                    api_key=api_key,
                    temperature=temperature,
                    max_tokens=1024,
                    timeout_s=timeout_s,
                )
                log.info("LLM overseer: provider=ollama_cloud model=%s vision=%s timeout=%.0fs",
                         model, vision, timeout_s)
                try:
                    await db.set_setting(SETTING_LLM_ERROR, "")
                except Exception:                               # noqa: BLE001
                    pass
                return client, snapshot, vision
            except Exception as exc:                            # noqa: BLE001
                log.exception("Failed to build OllamaCloudLLM (model=%s): %s", model, exc)
                try:
                    await db.set_setting(SETTING_LLM_ERROR, f"build failed: {exc}")
                except Exception:                               # noqa: BLE001
                    pass

    elif provider in ("local", "gemini", "claude") and model:
        # All three speak the OpenAI /chat/completions protocol, so a single
        # client covers them. `local` points at a self-hosted server; gemini
        # and claude use the vendor's OpenAI-compatible endpoint (URL filled in
        # from LLM_PROVIDER_BASE_URLS when the operator didn't override it) and
        # require an api_key.
        effective_base = base_url or LLM_PROVIDER_BASE_URLS.get(provider, "")
        needs_key = provider in ("gemini", "claude")
        if not effective_base:
            log.warning("%s requires a base_url — falling back to MockLLM", provider)
        elif needs_key and not api_key:
            log.warning("%s requires an api_key — falling back to MockLLM", provider)
        else:
            try:
                from hermes.llm import OpenAICompatibleLLM
                client = OpenAICompatibleLLM(
                    base_url=effective_base, model=model,
                    api_key=api_key, temperature=temperature,
                    timeout_s=timeout_s,
                )
                log.info("LLM overseer: provider=%s model=%s base=%s vision=%s timeout=%.0fs",
                         provider, model, effective_base, vision, timeout_s)
                try:
                    await db.set_setting(SETTING_LLM_ERROR, "")
                except Exception:                               # noqa: BLE001
                    pass
                return client, snapshot, vision
            except Exception as exc:                            # noqa: BLE001
                log.exception("Failed to build LLM client (provider=%s): %s", provider, exc)
                try:
                    await db.set_setting(SETTING_LLM_ERROR, f"build failed: {exc}")
                except Exception:                               # noqa: BLE001
                    pass

    # Fallback — mock LLM keeps the overseer operational without a backend.
    from hermes.service1_agent.mock_broker import MockLLM
    log.info("LLM overseer: using MockLLM (provider=%s)", provider)
    return MockLLM(), snapshot, vision


def build(broker, llm_client, chart_provider, config: Dict[str, Any],
          *, vision_enabled: bool = True,
          autonomy: Optional[str] = None,
          soul: Optional[str] = None,
          approval_mode: bool = True,
          strategy_enabled: Optional[Dict[str, bool]] = None,
          llm_out_of_loop: bool = False,
          overseer_mode: str = "monolithic",
          event_bus = None) -> CascadingEngine:
    db = HermesDB(os.environ.get("HERMES_DSN",
                                 "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
    mm = MoneyManager(broker, db, config)
    ic = IronCondorBuilder(mm)

    overseer = HermesOverseer(
        llm_client=llm_client, db=db, vision_enabled=vision_enabled,
        chart_provider=chart_provider,
        autonomy=(autonomy or config.get("ai_autonomy", "advisory")),
        soul=soul,
        overseer_mode=overseer_mode,
        event_bus=event_bus,
    )

    enabled = strategy_enabled or {}
    common = dict(broker=broker, db=db, money_manager=mm, ic_builder=ic,
                  config=config, overseer=overseer,
                  dry_run=config.get("dry_run", False))
    all_strategies = [
        CreditSpreads75(**common),
        CreditSpreads7(**common),
        TastyTrade45(**common),
        WheelStrategy(**common),
        HermesAlpha(**common),
    ]
    # Filter out strategies the operator has disabled from the C2 panel.
    active_strategies = [s for s in all_strategies
                         if enabled.get(s.NAME, True)]
    if len(active_strategies) < len(all_strategies):
        disabled = [s.NAME for s in all_strategies if not enabled.get(s.NAME, True)]
        log.info("Strategies disabled by C2 panel: %s", disabled)

    return CascadingEngine(broker, db, active_strategies, overseer=overseer,
                           approval_mode=approval_mode, money_manager=mm,
                           config=config, event_bus=event_bus,
                           llm_out_of_loop=llm_out_of_loop)


# ---------------------------------------------------------------------------
# Broker construction — supports per-mode credentials so the watcher's toggle
# can flip between sandbox (paper) and live without restart.
# ---------------------------------------------------------------------------
def _resolve_mode_credentials(mode: str) -> Tuple[str, str, str]:
    """Return (token, account_id, base_url) for the requested mode."""
    from hermes.config import settings
    orig_mode = settings.hermes_mode
    try:
        settings.hermes_mode = mode
        return settings.get_tradier_credentials()
    finally:
        settings.hermes_mode = orig_mode


def _build_broker(conf: Dict[str, Any], mode: str):
    """Build the broker for `mode`. Falls back to MockBroker only when
    *no* Tradier credentials of any kind are present in the environment."""
    from hermes.config import settings
    if settings.hermes_use_mcp_broker:
        from hermes.broker.mcp_client import MCPBrokerClient
        log.info("Initializing MCPBrokerClient mode=%s", mode)
        return MCPBrokerClient(conf)

    has_any_tradier = any(
        os.environ.get(k) for k in (
            "TRADIER_ACCESS_TOKEN", "TRADIER_PAPER_TOKEN", "TRADIER_LIVE_TOKEN",
            "TRADIER_API_KEY",
        )
    )
    if not has_any_tradier:
        from hermes.service1_agent.mock_broker import MockBroker
        log.warning("No Tradier credentials present — using MockBroker")
        return MockBroker(conf)

    from hermes.broker.tradier import TradierBroker
    token, account, url = _resolve_mode_credentials(mode)
    cfg = dict(conf)
    # Paper mode hits the sandbox, which is harmless, so preview is never needed.
    # Live mode honors the operator's dry_run, but real orders additionally
    # require an explicit arming flag (HERMES_LIVE_ARMED=true). Absent it, we
    # force dry_run so flipping the mode to "live" can never silently route real
    # money — going live must be a deliberate act, not a default.
    dry_run = conf.get("dry_run", False) if mode == "live" else False
    if mode == "live" and not dry_run and not _live_armed():
        dry_run = True
        log.warning(
            "LIVE mode selected but HERMES_LIVE_ARMED is not set — forcing "
            "dry_run (preview-only). Set HERMES_LIVE_ARMED=true to place real "
            "orders."
        )
    cfg.update({
        "tradier_access_token": token,
        "tradier_account_id": account,
        "tradier_base_url": url,
        "dry_run": dry_run,
    })
    log.info("Initializing TradierBroker mode=%s base=%s dry_run=%s armed=%s",
             mode, url, cfg["dry_run"], _live_armed())
    return TradierBroker(cfg)


def _build_stream_client(broker, db, event_bus, watchlist_syms: set):
    """Build stream client directly inside the agent based on the broker class."""
    from hermes.broker.tradier import TradierBroker
    if isinstance(broker, TradierBroker):
        from hermes.broker.tradier_stream import TradierStreamClient
        log.info("Initializing direct TradierStreamClient")
        return TradierStreamClient(
            token=broker.token,
            account_id=broker.account_id,
            base_url=broker.base_url,
            event_bus=event_bus,
            watchlist=list(watchlist_syms)
        )
    else:
        from hermes.broker.mock_stream import MockStreamClient
        log.info("Initializing localized MockStreamClient")
        return MockStreamClient(
            event_bus=event_bus,
            watchlist=list(watchlist_syms),
            db=db
        )


async def _load_and_validate_runtime_config(db, conf: Dict[str, Any]):
    from hermes.config_schema import RuntimeConfig
    
    obp_reserve_val = await db.get_setting("obp_reserve")
    tick_interval_val = await db.get_setting("tick_interval") or await db.get_setting("tick_interval_s")
    bandit_val = await db.get_setting("bandit_tuner_mode")
    exit_val = await db.get_setting("exit_policy_mode")

    config_data = {}
    if obp_reserve_val is not None and str(obp_reserve_val).strip() != "":
        config_data["obp_reserve"] = float(str(obp_reserve_val).strip())
    else:
        config_data["obp_reserve"] = float(os.environ.get("HERMES_OBP_RESERVE", conf.get("obp_reserve", 0.0)))

    if tick_interval_val is not None and str(tick_interval_val).strip() != "":
        config_data["tick_interval"] = int(str(tick_interval_val).strip())
    else:
        config_data["tick_interval"] = int(os.environ.get("HERMES_TICK_INTERVAL", conf.get("tick_interval_s", 3600)))

    if bandit_val is not None and str(bandit_val).strip() != "":
        config_data["bandit_tuner_mode"] = str(bandit_val).strip().lower()
    else:
        config_data["bandit_tuner_mode"] = os.environ.get("HERMES_BANDIT_TUNER_MODE", "off").lower()

    if exit_val is not None and str(exit_val).strip() != "":
        config_data["exit_policy_mode"] = str(exit_val).strip().lower()
    else:
        config_data["exit_policy_mode"] = os.environ.get("HERMES_EXIT_POLICY_MODE", "off").lower()

    return RuntimeConfig(**config_data)
