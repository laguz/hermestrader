"""
[Service-1: Hermes-Agent-Core] — Entry point.
Wires broker → DB → strategies → cascading engine → overseer, then ticks
on a schedule. Runs as its own process.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from hermes.db.models import HermesDB
from hermes.service1_agent.core import CascadingEngine, IronCondorBuilder, MoneyManager
from hermes.service1_agent.overseer import HermesOverseer
from hermes.service1_agent.strategies import (
    CreditSpreads7, CreditSpreads75, TastyTrade45, WheelStrategy,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("hermes.agent.main")

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

# Generous default — LM Studio cold-loading a multi-GB GGUF on consumer
# hardware can take 60-90 seconds before the first token arrives. 120s is
# a good middle ground for most setups.
DEFAULT_LLM_TIMEOUT_S = 120.0

# Operator doctrine + agent control — written by the watcher.
SETTING_SOUL = "soul_md"
SETTING_AUTONOMY = "agent_autonomy"
SETTING_PAUSED = "agent_paused"

VALID_MODES = ("paper", "live")
VALID_LLM_PROVIDERS = ("mock", "local")
VALID_AUTONOMY = ("advisory", "enforcing", "autonomous")

# Cascading priority — must match service2_watcher/api.py STRATEGY_PRIORITIES.
STRATEGY_PRIORITIES = {"CS75": 1, "CS7": 2, "TT45": 3, "WHEEL": 4}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _build_llm(db) -> Tuple[Any, Dict[str, Any], bool]:
    """Build the LLM overseer client from current settings.

    Returns (client, snapshot, vision_enabled). `snapshot` is the dict of
    config values used so the tick loop can detect changes and rebuild.
    """
    provider = (db.get_setting(SETTING_LLM_PROVIDER) or "mock").lower()
    base_url = (db.get_setting(SETTING_LLM_BASE_URL) or "").strip()
    model = (db.get_setting(SETTING_LLM_MODEL) or "").strip()
    api_key = (db.get_setting(SETTING_LLM_API_KEY) or "").strip() or None
    temperature_raw = (db.get_setting(SETTING_LLM_TEMPERATURE) or "0.2").strip()
    try:
        temperature = float(temperature_raw)
    except ValueError:
        temperature = 0.2
    timeout_raw = (db.get_setting(SETTING_LLM_TIMEOUT) or str(DEFAULT_LLM_TIMEOUT_S)).strip()
    try:
        timeout_s = max(5.0, float(timeout_raw))
    except ValueError:
        timeout_s = DEFAULT_LLM_TIMEOUT_S
    vision = (db.get_setting(SETTING_LLM_VISION) or "true").lower() != "false"
    snapshot = {
        "provider": provider,
        "base_url": base_url,
        "model": model,
        "has_api_key": bool(api_key),
        "temperature": temperature,
        "timeout_s": timeout_s,
        "vision": vision,
    }

    if provider == "local" and base_url and model:
        try:
            from hermes.llm import OpenAICompatibleLLM
            client = OpenAICompatibleLLM(
                base_url=base_url, model=model,
                api_key=api_key, temperature=temperature,
                timeout_s=timeout_s,
            )
            log.info("LLM overseer: local model=%s base=%s vision=%s timeout=%.0fs",
                     model, base_url, vision, timeout_s)
            return client, snapshot, vision
        except Exception as exc:                                # noqa: BLE001
            log.exception("Failed to build local LLM client: %s", exc)
            try:
                db.set_setting(SETTING_LLM_ERROR, f"build failed: {exc}")
            except Exception:                                   # noqa: BLE001
                pass

    # Fallback — mock LLM keeps the overseer operational without a backend.
    from hermes.service1_agent.mock_broker import MockLLM
    log.info("LLM overseer: using MockLLM (provider=%s)", provider)
    return MockLLM(), snapshot, vision


def _read_overseer_settings(db, conf: Dict[str, Any]) -> Dict[str, Any]:
    """Return the operator-driven overseer config (soul, autonomy, paused).

    Defaults pull from `conf` (env vars) the very first time so nothing
    surprising happens on first boot. After that, watcher writes win.
    """
    autonomy = (db.get_setting(SETTING_AUTONOMY)
                or conf.get("ai_autonomy") or "advisory").lower()
    if autonomy not in VALID_AUTONOMY:
        autonomy = "advisory"
    soul = db.get_setting(SETTING_SOUL) or ""
    paused = (db.get_setting(SETTING_PAUSED) or "false").lower() == "true"
    return {"autonomy": autonomy, "soul": soul, "paused": paused}


def build(broker, llm_client, chart_provider, config: Dict[str, Any],
          *, vision_enabled: bool = True,
          autonomy: Optional[str] = None,
          soul: Optional[str] = None) -> CascadingEngine:
    db = HermesDB(os.environ.get("HERMES_DSN",
                                 "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
    mm = MoneyManager(broker, db, config)
    ic = IronCondorBuilder(mm)

    overseer = HermesOverseer(
        llm_client=llm_client, db=db, vision_enabled=vision_enabled,
        chart_provider=chart_provider,
        autonomy=(autonomy or config.get("ai_autonomy", "advisory")),
        soul=soul,
    )

    common = dict(broker=broker, db=db, money_manager=mm, ic_builder=ic,
                  config=config, overseer=overseer,
                  dry_run=config.get("dry_run", False))
    strategies = [
        CreditSpreads75(**common),
        CreditSpreads7(**common),
        TastyTrade45(**common),
        WheelStrategy(**common),
    ]
    return CascadingEngine(broker, db, strategies, overseer=overseer)


# ---------------------------------------------------------------------------
# Broker construction — supports per-mode credentials so the watcher's toggle
# can flip between sandbox (paper) and live without restart.
# ---------------------------------------------------------------------------
def _resolve_mode_credentials(mode: str) -> Tuple[str, str, str]:
    """Return (token, account_id, base_url) for the requested mode.

    Order of resolution per mode:
      1. Mode-specific env vars (TRADIER_PAPER_* / TRADIER_LIVE_*)
      2. Generic TRADIER_ACCESS_TOKEN/TRADIER_ACCOUNT_ID with a mode-derived URL
    """
    mode = mode.lower().strip()
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {VALID_MODES}")

    if mode == "paper":
        token = os.environ.get("TRADIER_PAPER_TOKEN") or os.environ.get("TRADIER_ACCESS_TOKEN")
        account = os.environ.get("TRADIER_PAPER_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = os.environ.get("TRADIER_PAPER_BASE_URL", "https://sandbox.tradier.com/v1")
    else:
        token = os.environ.get("TRADIER_LIVE_TOKEN") or os.environ.get("TRADIER_ACCESS_TOKEN")
        account = os.environ.get("TRADIER_LIVE_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
        url = os.environ.get("TRADIER_LIVE_BASE_URL", "https://api.tradier.com/v1")

    if not token or not account:
        raise RuntimeError(
            f"missing Tradier credentials for mode={mode!r}; set "
            f"TRADIER_{mode.upper()}_TOKEN and TRADIER_{mode.upper()}_ACCOUNT_ID "
            "(or fall back to TRADIER_ACCESS_TOKEN/TRADIER_ACCOUNT_ID)."
        )
    return token, account, url


def _build_broker(conf: Dict[str, Any], mode: str):
    """Build the broker for `mode`. Falls back to MockBroker only when
    *no* Tradier credentials of any kind are present in the environment."""
    has_any_tradier = any(
        os.environ.get(k) for k in (
            "TRADIER_ACCESS_TOKEN", "TRADIER_PAPER_TOKEN", "TRADIER_LIVE_TOKEN",
        )
    )
    if not has_any_tradier:
        from hermes.service1_agent.mock_broker import MockBroker
        log.warning("No Tradier credentials present — using MockBroker")
        return MockBroker(conf)

    from hermes.broker.tradier import TradierBroker
    token, account, url = _resolve_mode_credentials(mode)
    cfg = dict(conf)
    cfg.update({
        "tradier_access_token": token,
        "tradier_account_id": account,
        "tradier_base_url": url,
        # In live mode we honor whatever dry_run the operator configured; in
        # paper mode we never need preview mode because sandbox is harmless.
        "dry_run": conf.get("dry_run", False) if mode == "live" else False,
    })
    log.info("Initializing TradierBroker mode=%s base=%s dry_run=%s",
             mode, url, cfg["dry_run"])
    return TradierBroker(cfg)


# ---------------------------------------------------------------------------
# Tick loop — re-reads the desired mode each iteration so the watcher's
# toggle takes effect within one tick interval.
# ---------------------------------------------------------------------------
def run(chart_provider, conf: Dict[str, Any]) -> None:
    db = HermesDB(os.environ.get("HERMES_DSN",
                                 "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
    # Seed the strategies registry — required before any watchlist row can be
    # inserted (FK from strategy_watchlists.strategy_id). Idempotent.
    try:
        db.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:                                      # noqa: BLE001
        log.exception("ensure_strategies failed at startup: %s", exc)
    # Initial mode comes from settings (so the operator's last toggle wins
    # across restarts) and falls back to env config on first ever boot.
    initial_mode = (db.get_setting(SETTING_MODE) or conf.get("mode") or "paper").lower()
    if initial_mode not in VALID_MODES:
        initial_mode = "paper"
    db.set_setting(SETTING_MODE, initial_mode)
    db.set_setting(SETTING_AGENT_STARTED_AT, _utcnow_iso())

    current_mode = initial_mode
    broker = _build_broker(conf, current_mode)

    # LLM client is built from settings rather than the hard-coded MockLLM
    # passed to run() — that lets the watcher swap providers at runtime.
    current_llm, current_llm_snapshot, current_vision = _build_llm(db)

    # Operator doctrine + autonomy + pause are tracked together. The first
    # snapshot also seeds defaults from env/conf when the watcher hasn't
    # written anything yet.
    current_overseer_cfg = _read_overseer_settings(db, conf)
    db.set_setting(SETTING_AUTONOMY, current_overseer_cfg["autonomy"])
    db.set_setting(SETTING_PAUSED, "true" if current_overseer_cfg["paused"] else "false")
    if db.get_setting(SETTING_SOUL) is None:
        db.set_setting(SETTING_SOUL, "")

    engine = build(broker, current_llm, chart_provider, conf,
                   vision_enabled=current_vision,
                   autonomy=current_overseer_cfg["autonomy"],
                   soul=current_overseer_cfg["soul"])
    watchlist = conf["watchlist"]
    interval_s = int(conf.get("tick_interval_s", 300))
    log.info("Hermes Agent started mode=%s autonomy=%s paused=%s soul=%dB",
             current_mode, current_overseer_cfg["autonomy"],
             current_overseer_cfg["paused"], len(current_overseer_cfg["soul"]))

    while True:
        try:
            # 1) Mode reconciliation — pick up any toggle the watcher made.
            desired_mode = (db.get_setting(SETTING_MODE) or current_mode).lower()
            if desired_mode not in VALID_MODES:
                desired_mode = current_mode
            if desired_mode != current_mode:
                log.warning("mode change requested: %s → %s", current_mode, desired_mode)
                try:
                    broker = _build_broker(conf, desired_mode)
                    engine = build(broker, current_llm, chart_provider, conf,
                                   vision_enabled=current_vision)
                    current_mode = desired_mode
                    db.write_log("ENGINE", f"mode switched to {current_mode}")
                except Exception as exc:                          # noqa: BLE001
                    log.exception("mode switch to %s failed: %s", desired_mode, exc)
                    db.set_setting(SETTING_TRADIER_ERROR, f"mode switch failed: {exc}")
                    # Keep ticking on the previous broker.

            # 1b) LLM reconciliation — same idea: pick up any /api/llm change
            #     the watcher made and rebuild the overseer with it.
            new_llm, new_snapshot, new_vision = _build_llm(db)
            llm_changed = new_snapshot != current_llm_snapshot

            # 1c) Soul / autonomy / pause reconciliation. Any of these
            #     warrant an engine rebuild because the overseer is
            #     constructed once with those values.
            new_overseer_cfg = _read_overseer_settings(db, conf)
            overseer_changed = new_overseer_cfg != current_overseer_cfg

            if llm_changed or overseer_changed:
                if llm_changed:
                    log.warning("LLM config change: %s → %s",
                                current_llm_snapshot, new_snapshot)
                if overseer_changed:
                    log.warning("Overseer config change: %s → %s",
                                current_overseer_cfg, new_overseer_cfg)
                current_llm = new_llm
                current_llm_snapshot = new_snapshot
                current_vision = new_vision
                current_overseer_cfg = new_overseer_cfg
                engine = build(broker, current_llm, chart_provider, conf,
                               vision_enabled=current_vision,
                               autonomy=current_overseer_cfg["autonomy"],
                               soul=current_overseer_cfg["soul"])
                if llm_changed:
                    db.write_log("ENGINE",
                                 f"LLM swapped: provider={new_snapshot['provider']} "
                                 f"model={new_snapshot['model'] or '-'}")
                if overseer_changed:
                    db.write_log("ENGINE",
                                 f"Overseer reconfigured: autonomy={new_overseer_cfg['autonomy']} "
                                 f"paused={new_overseer_cfg['paused']} "
                                 f"soul={len(new_overseer_cfg['soul'])}B")

            # 1d) Hard pause — skip the engine but keep the heartbeat so
            #     the watcher's System health card distinguishes "paused"
            #     from "stopped".
            if current_overseer_cfg["paused"]:
                db.write_log("ENGINE", f"heartbeat tick PAUSED mode={current_mode}")
                time.sleep(interval_s)
                continue

            # 2) Heartbeat — guarantees the watcher sees a fresh log line each
            #    tick even when no strategy fires.
            db.write_log("ENGINE", f"heartbeat tick start mode={current_mode}")

            # 3) The actual tick. Any successful tick implies Tradier is
            #    reachable (the very first thing tick() does is broker.get_positions()).
            stats = engine.tick(watchlist)
            db.set_setting(SETTING_TRADIER_OK_TS, _utcnow_iso())
            db.set_setting(SETTING_TRADIER_ERROR, "")
            log.info("tick complete: %s", stats)
            db.write_log("ENGINE", f"heartbeat tick complete: {stats}")
        except Exception as exc:                                  # noqa: BLE001
            log.exception("tick failed: %s", exc)
            try:
                db.set_setting(SETTING_TRADIER_ERROR, str(exc)[:500])
                db.write_log("ENGINE", f"tick failed: {exc}", level="ERROR")
            except Exception:                                     # noqa: BLE001
                pass
        time.sleep(interval_s)


if __name__ == "__main__":
    conf = {
        "watchlist": os.environ.get("HERMES_WATCHLIST", "AAPL,SPY,QQQ").split(","),
        "min_obp_reserve": float(os.environ.get("HERMES_MIN_OBP_RESERVE", 5000.0)),
        "ai_autonomy": os.environ.get("HERMES_AI_AUTONOMY", "advisory"),
        "tick_interval_s": int(os.environ.get("HERMES_TICK_INTERVAL", 300)),
        "dry_run": os.environ.get("HERMES_DRY_RUN", "true").lower() == "true",
        # Initial mode if no setting is stored yet — paper is the safe default.
        "mode": os.environ.get("HERMES_MODE", "paper").lower(),
    }
    charts = None
    run(charts, conf)
