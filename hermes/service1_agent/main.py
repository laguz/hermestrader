"""
[Service-1: Hermes-Agent-Core] — Entry point.
Wires broker → DB → strategies → cascading engine → overseer, then ticks
on a schedule. Runs as its own process.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from hermes.common import (
    DEFAULT_LLM_TIMEOUT_S,
    STRATEGY_PRIORITIES,
    VALID_AUTONOMY,
    VALID_MODES,
)
from hermes.utils import decrypt_value
from hermes.market_hours import market_session, next_open, session_label
from hermes.db.models import HermesDB
from hermes.service1_agent.core import CascadingEngine, IronCondorBuilder, MoneyManager
from hermes.service1_agent.overseer import HermesOverseer
from hermes.service1_agent.strategies import (
    CreditSpreads7, CreditSpreads75, TastyTrade45, WheelStrategy,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("hermes.agent.main")

_SHUTDOWN_EVENT = threading.Event()

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

# Operator doctrine + agent control — written by the C2 panel.
SETTING_SOUL = "soul_md"
SETTING_AUTONOMY = "agent_autonomy"
SETTING_PAUSED = "agent_paused"
SETTING_APPROVAL_MODE = "approval_mode"   # "true" | "false"

# Per-strategy enable/disable flags — written by the C2 panel.
# Key pattern: "strategy_{id}_enabled"  value: "true" | "false"
def _strategy_enabled_key(strategy_id: str) -> str:
    return f"strategy_{strategy_id.lower()}_enabled"

# VALID_MODES, VALID_AUTONOMY, DEFAULT_LLM_TIMEOUT_S,
# and STRATEGY_PRIORITIES are imported from hermes.common above.


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string into a timezone-aware datetime, or return None.

    Python <3.11's ``datetime.fromisoformat`` rejects the trailing ``Z``
    used by most external services; normalise it to ``+00:00`` first so
    timestamps round-tripped through other tools still parse.
    """
    if not s:
        return None
    try:
        normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(normalised)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


# Tradier order statuses that mean the broker did NOT accept the order; the
# approval row must NOT be flipped to EXECUTED for any of these.
_REJECTED_ORDER_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}


def _execute_approved_action(item: Dict[str, Any], *, broker, db) -> str:
    """Execute one C2-approved action and reconcile its approval row.

    Returns one of: ``"executed"``, ``"preview"``, ``"rejected"``, ``"failed"``.
    Exposed at module scope so the lifecycle is unit-testable without
    standing up the full tick loop.

    The approval row's final state must always reflect what the broker
    actually did:

    * ``dry_run=True`` → no broker call; mark FAILED with a preview note so
      the C2 UI cannot mistake a preview for a live order.
    * Broker raises  → ``record_order_response`` rolls the PendingOrder
      back to REJECTED so capacity recovers; approval marked FAILED.
    * Broker returns ``errors`` / a rejected status → approval marked
      FAILED; ``record_order_response`` already wrote ``[ORDER REJECTED]``.
    * Clean response → approval marked EXECUTED and ``[C2 EXECUTED]`` is
      written for the operator feed.
    """
    from hermes.service1_agent.core import TradeAction

    approval_id = item["id"]
    action_json = item["action_json"]
    try:
        action = TradeAction(**action_json)
    except Exception as exc:                                   # noqa: BLE001
        log.exception("[C2] Failed to rebuild TradeAction id=%d: %s",
                      approval_id, exc)
        db.mark_approval_executed(
            approval_id, success=False,
            notes=f"action rebuild error: {exc}",
        )
        return "failed"

    # Market-hours gate — C2-approved trades must respect the same
    # off-hours block as strategy-emitted ones. Leave the approval row
    # in PENDING (do NOT mark FAILED) so the next tick during regular
    # session picks it up automatically.
    from hermes.market_hours import should_block_trades
    blocked, reason = should_block_trades()
    if blocked:
        log.info("[C2] OFF-HOURS — deferring approval id=%d (%s)",
                 approval_id, reason)
        db.write_log(
            action.strategy_id,
            f"[C2 DEFERRED] {action.symbol} approval_id={approval_id} — "
            f"{reason}; will execute on next tick during regular session",
        )
        return "deferred"

    broker_dry_run = bool(getattr(broker, "dry_run", False))
    if broker_dry_run:
        # No broker call happens — don't pretend it did.  Skip
        # record_pending_order so capacity isn't consumed by a row
        # that will never settle.
        db.mark_approval_executed(
            approval_id, success=False,
            notes="dry_run=True — no broker order placed",
        )
        log.info("[C2] dry_run preview only — approval id=%d "
                 "NOT submitted to broker", approval_id)
        db.write_log(
            action.strategy_id,
            f"[C2 PREVIEW] {action.symbol} {action.order_class} "
            f"qty={action.quantity} approval_id={approval_id} — "
            f"dry_run=True, no order sent to broker",
        )
        return "preview"

    db.record_pending_order(action)
    try:
        resp = broker.place_order_from_action(action)
    except Exception as exc:                                   # noqa: BLE001
        db.record_order_response(action, {"errors": str(exc)})
        db.mark_approval_executed(
            approval_id, success=False,
            notes=f"broker raised: {exc}",
        )
        log.exception("[C2] place_order_from_action raised for "
                      "approval id=%d: %s", approval_id, exc)
        db.write_log(
            action.strategy_id,
            f"[C2 FAILED] {action.symbol} approval_id={approval_id} "
            f"broker raised: {exc}",
        )
        return "failed"

    db.record_order_response(action, resp)

    order = (resp or {}).get("order") if isinstance(resp, dict) else None
    order_status = ""
    if isinstance(order, dict):
        order_status = str(order.get("status", "")).lower()
    rejected = (
        (isinstance(resp, dict) and "errors" in resp)
        or order_status in _REJECTED_ORDER_STATUSES
    )

    if rejected:
        # record_order_response already wrote [ORDER REJECTED].
        db.mark_approval_executed(
            approval_id, success=False,
            notes=f"broker rejected: {resp}",
        )
        log.warning("[C2] broker rejected approval id=%d: %s",
                    approval_id, resp)
        db.write_log(
            action.strategy_id,
            f"[C2 REJECTED] {action.symbol} approval_id={approval_id}",
        )
        return "rejected"

    db.mark_approval_executed(approval_id, success=True)
    log.info("[C2] Executed approved trade: %s %s strategy=%s id=%d",
             action.symbol, action.order_class, action.strategy_id, approval_id)
    db.write_log(
        action.strategy_id,
        f"[C2 EXECUTED] {action.symbol} {action.order_class} "
        f"qty={action.quantity} approval_id={approval_id}",
    )
    return "executed"


def _build_llm(db) -> Tuple[Any, Dict[str, Any], bool]:
    """Build the LLM overseer client from current settings.

    Returns (client, snapshot, vision_enabled). `snapshot` is the dict of
    config values used so the tick loop can detect changes and rebuild.
    """
    provider = (db.get_setting(SETTING_LLM_PROVIDER) or "mock").lower()
    base_url = (db.get_setting(SETTING_LLM_BASE_URL) or "").strip()
    model = (db.get_setting(SETTING_LLM_MODEL) or "").strip()
    api_key = decrypt_value((db.get_setting(SETTING_LLM_API_KEY) or "").strip()) or None
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
                    db.set_setting(SETTING_LLM_ERROR, "")
                except Exception:                               # noqa: BLE001
                    pass
                return client, snapshot, vision
            except Exception as exc:                            # noqa: BLE001
                log.exception("Failed to build OllamaCloudLLM (model=%s): %s", model, exc)
                try:
                    db.set_setting(SETTING_LLM_ERROR, f"build failed: {exc}")
                except Exception:                               # noqa: BLE001
                    pass

    elif provider == "local" and base_url and model:
        try:
            from hermes.llm import OpenAICompatibleLLM
            client = OpenAICompatibleLLM(
                base_url=base_url, model=model,
                api_key=api_key, temperature=temperature,
                timeout_s=timeout_s,
            )
            log.info("LLM overseer: provider=local model=%s base=%s vision=%s timeout=%.0fs",
                     model, base_url, vision, timeout_s)
            try:
                db.set_setting(SETTING_LLM_ERROR, "")
            except Exception:                                   # noqa: BLE001
                pass
            return client, snapshot, vision
        except Exception as exc:                                # noqa: BLE001
            log.exception("Failed to build LLM client (provider=%s): %s", provider, exc)
            try:
                db.set_setting(SETTING_LLM_ERROR, f"build failed: {exc}")
            except Exception:                                   # noqa: BLE001
                pass

    # Fallback — mock LLM keeps the overseer operational without a backend.
    from hermes.service1_agent.mock_broker import MockLLM
    log.info("LLM overseer: using MockLLM (provider=%s)", provider)
    return MockLLM(), snapshot, vision


def _read_overseer_settings(db, conf: Dict[str, Any]) -> Dict[str, Any]:
    """Return the operator-driven overseer config (soul, autonomy, paused, approval_mode).

    Defaults pull from `conf` (env vars) the very first time so nothing
    surprising happens on first boot. After that, C2 panel writes win.
    """
    autonomy = (db.get_setting(SETTING_AUTONOMY)
                or conf.get("ai_autonomy") or "advisory").lower()
    if autonomy not in VALID_AUTONOMY:
        autonomy = "advisory"
    soul = db.get_setting(SETTING_SOUL) or ""
    paused = (db.get_setting(SETTING_PAUSED) or "false").lower() == "true"
    approval_mode = (db.get_setting(SETTING_APPROVAL_MODE) or "true").lower() == "true"
    # Per-strategy enable flags — default to enabled for all known strategies.
    strategy_enabled = {
        sid: (db.get_setting(_strategy_enabled_key(sid)) or "true").lower() != "false"
        for sid in STRATEGY_PRIORITIES
    }
    return {
        "autonomy": autonomy,
        "soul": soul,
        "paused": paused,
        "approval_mode": approval_mode,
        "strategy_enabled": strategy_enabled,
    }


def build(broker, llm_client, chart_provider, config: Dict[str, Any],
          *, vision_enabled: bool = True,
          autonomy: Optional[str] = None,
          soul: Optional[str] = None,
          approval_mode: bool = True,
          strategy_enabled: Optional[Dict[str, bool]] = None,
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
    ]
    # Filter out strategies the operator has disabled from the C2 panel.
    active_strategies = [s for s in all_strategies
                         if enabled.get(s.NAME, True)]
    if len(active_strategies) < len(all_strategies):
        disabled = [s.NAME for s in all_strategies if not enabled.get(s.NAME, True)]
        log.info("Strategies disabled by C2 panel: %s", disabled)

    return CascadingEngine(broker, db, active_strategies, overseer=overseer,
                           approval_mode=approval_mode, money_manager=mm,
                           config=config, event_bus=event_bus)


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
    asyncio.run(_run_async(chart_provider, conf))


async def _run_async(chart_provider, conf: Dict[str, Any]) -> None:
    db = HermesDB(os.environ.get("HERMES_DSN",
                                 "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
    # Apply schema migrations before anything else so fresh deployments are
    # never left running against a stale schema (e.g. missing expires_at).
    try:
        await asyncio.to_thread(db.run_migrations)
    except Exception as exc:                                      # noqa: BLE001
        log.exception("run_migrations failed at startup: %s", exc)
    # Startup update check and soul syncing
    try:
        from hermes.utils import sync_soul_file_to_db, check_for_updates
        import threading
        await asyncio.to_thread(sync_soul_file_to_db, db)
        threading.Thread(target=check_for_updates, daemon=True).start()
    except Exception as exc:                                      # noqa: BLE001
        log.exception("Agent startup update/soul sync failed: %s", exc)
    # Seed the strategies registry — required before any watchlist row can be
    # inserted (FK from strategy_watchlists.strategy_id). Idempotent.
    try:
        await asyncio.to_thread(db.ensure_strategies, STRATEGY_PRIORITIES)
    except Exception as exc:                                      # noqa: BLE001
        log.exception("ensure_strategies failed at startup: %s", exc)
    # Initial mode comes from settings (so the operator's last toggle wins
    # across restarts) and falls back to env config on first ever boot.
    initial_mode = (await asyncio.to_thread(db.get_setting, SETTING_MODE) or conf.get("mode") or "paper").lower()
    if initial_mode not in VALID_MODES:
        initial_mode = "paper"
    await asyncio.to_thread(db.set_setting, SETTING_MODE, initial_mode)
    await asyncio.to_thread(db.set_setting, SETTING_AGENT_STARTED_AT, _utcnow_iso())

    current_mode = initial_mode
    broker = _build_broker(conf, current_mode)

    # LLM client is built from settings rather than the hard-coded MockLLM
    # passed to run() — that lets the watcher swap providers at runtime.
    current_llm, current_llm_snapshot, current_vision = _build_llm(db)

    # Operator doctrine + autonomy + pause are tracked together. The first
    # snapshot also seeds defaults from env/conf when the watcher hasn't
    # written anything yet.
    current_overseer_cfg = _read_overseer_settings(db, conf)
    await asyncio.to_thread(db.set_setting, SETTING_AUTONOMY, current_overseer_cfg["autonomy"])
    await asyncio.to_thread(db.set_setting, SETTING_PAUSED, "true" if current_overseer_cfg["paused"] else "false")
    if await asyncio.to_thread(db.get_setting, SETTING_SOUL) is None:
        await asyncio.to_thread(db.set_setting, SETTING_SOUL, "")

    # Initialize Event Bus
    from hermes.events.bus import EventBus
    event_bus = EventBus()
    event_bus.start()

    engine = build(broker, current_llm, chart_provider, conf,
                   vision_enabled=current_vision,
                   autonomy=current_overseer_cfg["autonomy"],
                   soul=current_overseer_cfg["soul"],
                   approval_mode=current_overseer_cfg["approval_mode"],
                   strategy_enabled=current_overseer_cfg["strategy_enabled"],
                   event_bus=event_bus)

    # Start the async Overseer background task if present
    if engine.overseer is not None:
        await engine.overseer.start()

    # Start Tradier WebSocket Stream Client
    from hermes.broker.tradier_stream import TradierStreamClient
    token, account, url = _resolve_mode_credentials(current_mode)
    
    # Track watchlist symbols + active DB option legs
    watchlist_syms = set(conf.get("watchlist", []))
    try:
        watchlist_syms.update(await asyncio.to_thread(db.tracked_option_symbols))
    except Exception:
        pass

    stream_client = TradierStreamClient(
        token=token,
        account_id=account,
        base_url=url,
        event_bus=event_bus,
        watchlist=list(watchlist_syms)
    )
    
    is_mock = "mock" in str(type(broker)).lower()
    if not is_mock:
        await stream_client.start()
    
    interval_s = int(conf.get("tick_interval_s", 300))
    log.info("Hermes Agent started mode=%s autonomy=%s paused=%s soul=%dB",
             current_mode, current_overseer_cfg["autonomy"],
             current_overseer_cfg["paused"], len(current_overseer_cfg["soul"]))

    # Circuit breaker: pause broker calls after N consecutive tick failures.
    _CB_THRESHOLD = 5          # consecutive failures before tripping
    _CB_COOLDOWN_S = 300       # seconds to wait before re-attempting
    _cb_fail_count = 0
    _cb_tripped_at: float = 0.0

    while True:
        # Circuit breaker
        if _cb_fail_count >= _CB_THRESHOLD:
            if _cb_tripped_at == 0.0:
                _cb_tripped_at = time.time()
                log.error(
                    "[CIRCUIT BREAKER] %d consecutive tick failures — pausing broker "
                    "calls for %ds", _cb_fail_count, _CB_COOLDOWN_S,
                )
                try:
                    await asyncio.to_thread(
                        db.set_setting,
                        SETTING_TRADIER_ERROR,
                        f"circuit breaker tripped after {_cb_fail_count} failures"
                    )
                    await asyncio.to_thread(
                        db.write_log,
                        "ENGINE",
                        f"[CIRCUIT BREAKER] pausing {_CB_COOLDOWN_S}s after "
                        f"{_cb_fail_count} consecutive failures",
                        level="ERROR"
                    )
                except Exception:                                     # noqa: BLE001
                    pass
            if time.time() - _cb_tripped_at < _CB_COOLDOWN_S:
                await asyncio.sleep(interval_s)
                continue
            # Cooldown elapsed — reset and try again.
            _cb_fail_count = 0
            _cb_tripped_at = 0.0
            log.info("[CIRCUIT BREAKER] cooldown elapsed — resuming tick loop")

        try:
            # 1) Mode reconciliation — pick up any toggle the watcher made.
            desired_mode = (await asyncio.to_thread(db.get_setting, SETTING_MODE) or current_mode).lower()
            if desired_mode not in VALID_MODES:
                desired_mode = current_mode
            if desired_mode != current_mode:
                log.warning("mode change requested: %s → %s", current_mode, desired_mode)
                try:
                    broker = _build_broker(conf, desired_mode)
                    
                    # Stop old stream client
                    if not is_mock:
                        await stream_client.stop()
                        
                    # Rebuild stream client for the new mode credentials
                    token, account, url = _resolve_mode_credentials(desired_mode)
                    is_mock = "mock" in str(type(broker)).lower()
                    
                    watchlist_syms = set(conf.get("watchlist", []))
                    try:
                        watchlist_syms.update(await asyncio.to_thread(db.tracked_option_symbols))
                    except Exception:
                        pass
                        
                    stream_client = TradierStreamClient(
                        token=token,
                        account_id=account,
                        base_url=url,
                        event_bus=event_bus,
                        watchlist=list(watchlist_syms)
                    )
                    if not is_mock:
                        await stream_client.start()
                    
                    engine = build(broker, current_llm, chart_provider, conf,
                                   vision_enabled=current_vision,
                                   autonomy=current_overseer_cfg["autonomy"],
                                   soul=current_overseer_cfg["soul"],
                                   approval_mode=current_overseer_cfg["approval_mode"],
                                   strategy_enabled=current_overseer_cfg["strategy_enabled"],
                                   event_bus=event_bus)
                    current_mode = desired_mode
                    await asyncio.to_thread(db.write_log, "ENGINE", f"mode switched to {current_mode}")
                except Exception as exc:                          # noqa: BLE001
                    log.exception("mode switch to %s failed: %s", desired_mode, exc)
                    await asyncio.to_thread(db.set_setting, SETTING_TRADIER_ERROR, f"mode switch failed: {exc}")

            # 1b) LLM reconciliation
            new_llm, new_snapshot, new_vision = _build_llm(db)
            llm_changed = new_snapshot != current_llm_snapshot

            # 1c) Soul / autonomy / pause reconciliation
            new_overseer_cfg = _read_overseer_settings(db, conf)
            overseer_changed = new_overseer_cfg != current_overseer_cfg

            if llm_changed or overseer_changed:
                if llm_changed:
                    log.warning("LLM config change: %s → %s",
                                current_llm_snapshot, new_snapshot)
                if overseer_changed:
                    log.warning("Overseer config change: %s → %s",
                                current_overseer_cfg, new_overseer_cfg)
                
                # Stop old overseer background task
                if engine.overseer is not None:
                    await engine.overseer.stop()
                    
                current_llm = new_llm
                current_llm_snapshot = new_snapshot
                current_vision = new_vision
                current_overseer_cfg = new_overseer_cfg
                
                engine = build(broker, current_llm, chart_provider, conf,
                               vision_enabled=current_vision,
                               autonomy=current_overseer_cfg["autonomy"],
                               soul=current_overseer_cfg["soul"],
                               approval_mode=current_overseer_cfg["approval_mode"],
                               strategy_enabled=current_overseer_cfg["strategy_enabled"],
                               event_bus=event_bus)
                               
                if engine.overseer is not None:
                    await engine.overseer.start()
                    
                if llm_changed:
                    await asyncio.to_thread(
                        db.write_log, "ENGINE",
                        f"LLM swapped: provider={new_snapshot['provider']} "
                        f"model={new_snapshot['model'] or '-'}"
                    )
                if overseer_changed:
                    await asyncio.to_thread(
                        db.write_log, "ENGINE",
                        f"Overseer reconfigured: autonomy={new_overseer_cfg['autonomy']} "
                        f"paused={new_overseer_cfg['paused']} "
                        f"soul={len(new_overseer_cfg['soul'])}B"
                    )

            # 1d) Dynamic Watchlist Refresh
            try:
                all_wls = await asyncio.to_thread(db.list_all_watchlists)
                unique_syms = set()
                for syms in all_wls.values():
                    unique_syms.update(syms)
                db_watchlist = sorted(unique_syms)
                current_watchlist = sorted(list(unique_syms | set(conf.get("watchlist", []))))
            except Exception as wl_exc:
                log.warning("Dynamic watchlist refresh failed: %s", wl_exc)
                current_watchlist = conf.get("watchlist", [])
                db_watchlist = []

            # Update WebSocket stream subscriptions to include watchlist + open trade options
            if not is_mock:
                try:
                    wl_syms = set(current_watchlist)
                    wl_syms.update(await asyncio.to_thread(db.tracked_option_symbols))
                    stream_client.update_watchlist(list(wl_syms))
                except Exception as exc:
                    log.warning("Failed to update WebSocket watchlist: %s", exc)

            # 1d) Hard pause check
            if current_overseer_cfg["paused"]:
                await asyncio.to_thread(db.write_log, "ENGINE", f"heartbeat tick PAUSED mode={current_mode}")
                await asyncio.sleep(interval_s)
                continue

            # 1d-lot) Refresh per-strategy lot settings
            _LOT_KEYS = [
                "cs75_target_lots", "cs75_max_lots",
                "cs7_target_lots",  "cs7_max_lots",
                "tt45_target_lots", "tt45_max_lots",
                "wheel_max_lots",
            ]
            _LOT_DEFAULTS = {
                "cs75_target_lots": 10, "cs75_max_lots": 10,
                "cs7_target_lots":  10, "cs7_max_lots":  10,
                "tt45_target_lots":  5, "tt45_max_lots":  5,
                "wheel_max_lots":    5,
            }
            try:
                for _k in _LOT_KEYS:
                    _raw = await asyncio.to_thread(db.get_setting, _k)
                    if _raw is not None:
                        try:
                            conf[_k] = int(_raw)
                        except (ValueError, TypeError):
                            conf[_k] = _LOT_DEFAULTS[_k]
                    else:
                        conf.setdefault(_k, _LOT_DEFAULTS[_k])
            except Exception as _exc:                            # noqa: BLE001
                log.warning("lot-settings refresh failed: %s", _exc)

            # 1e) Stale pending-order cleanup
            try:
                _ttl_raw = await asyncio.to_thread(db.get_setting, "pending_order_ttl_s")
                _pending_ttl_s = int(_ttl_raw) if _ttl_raw else 3600
            except (TypeError, ValueError):
                _pending_ttl_s = 3600
            try:
                expired = await asyncio.to_thread(db.expire_stale_pending_orders, _pending_ttl_s)
                if expired:
                    log.info("Expired %d stale PENDING order(s)", expired)
                    await asyncio.to_thread(db.write_log, "ENGINE", f"expired {expired} stale PENDING order(s)")
            except Exception as exc:                          # noqa: BLE001
                log.warning("expire_stale_pending_orders failed: %s", exc)

            # 1e-ii) Stale approval cleanup
            try:
                expired_approvals = await asyncio.to_thread(db.expire_stale_approvals)
                if expired_approvals:
                    log.info("Auto-expired %d stale approval(s)", expired_approvals)
                    await asyncio.to_thread(
                        db.write_log, "ENGINE",
                        f"auto-expired {expired_approvals} stale approval(s) past deadline"
                    )
            except Exception as exc:                          # noqa: BLE001
                log.warning("expire_stale_approvals failed: %s", exc)

            # 1f) Execute C2-approved orders
            if current_overseer_cfg["approval_mode"]:
                try:
                    approved = await asyncio.to_thread(db.fetch_approved_actions)
                    for item in approved:
                        await asyncio.to_thread(_execute_approved_action, item, broker=broker, db=db)
                except Exception as exc:                       # noqa: BLE001
                    log.warning("fetch_approved_actions failed: %s", exc)

            # 2) Heartbeat
            mkt = market_session()
            await asyncio.to_thread(
                db.write_log, "ENGINE",
                f"heartbeat tick start mode={current_mode} "
                f"market={mkt['session']} open={mkt['is_open']}"
            )

            # 3) Market-hours gate
            if not mkt["trading_day"]:
                nxt = next_open()
                await asyncio.to_thread(
                    db.write_log, "ENGINE",
                    f"market CLOSED — next open {nxt.strftime('%Y-%m-%d %H:%M ET')} "
                    f"({mkt['et_date']} is not a trading day)"
                )
                await asyncio.sleep(interval_s)
                continue

            if mkt["is_open"]:
                # Full tick: management + entries
                stats = await asyncio.to_thread(engine.tick, current_watchlist)
            else:
                await asyncio.to_thread(engine.sync_positions)
                await asyncio.to_thread(engine.reconcile_orphans)
                stats = {"managed": 0, "entries": 0,
                         "note": f"all submissions skipped ({mkt['session']})"}

            # 4) Chart analysis
            _CHART_ANALYSIS_KEY = "chart_analysis_last_run"
            _CHART_ANALYSIS_INTERVAL_DAYS = 7
            if chart_provider is not None and engine.overseer is not None and db_watchlist:
                _should_run_charts = False
                _age_days: float = 0.0
                try:
                    _recent_decisions = await asyncio.to_thread(
                        db.recent_ai_decisions,
                        strategy_id="CHART",
                        limit=max(len(db_watchlist) * 2, 20)
                    )
                    _analyzed_syms = {d["symbol"] for d in _recent_decisions}
                    _missing_analysis = any(s not in _analyzed_syms for s in db_watchlist)

                    if _missing_analysis:
                        _should_run_charts = True
                        log.info("Forcing chart analysis: some symbols in watchlist are missing analysis.")
                    else:
                        _last_chart_ts_raw = await asyncio.to_thread(db.get_setting, _CHART_ANALYSIS_KEY)
                        if _last_chart_ts_raw:
                            _last_chart_dt = _parse_iso(_last_chart_ts_raw)
                            if _last_chart_dt is None:
                                _should_run_charts = True
                            else:
                                _age_days = (
                                    datetime.now(timezone.utc) - _last_chart_dt
                                ).total_seconds() / 86400
                                _should_run_charts = _age_days >= _CHART_ANALYSIS_INTERVAL_DAYS
                        else:
                            _should_run_charts = True
                except Exception:                               # noqa: BLE001
                    _should_run_charts = True

                if _should_run_charts:
                    log.info("Running chart vision analysis for %d symbols", len(db_watchlist))
                    try:
                        await asyncio.to_thread(engine.overseer.analyze_charts, db_watchlist)
                        await asyncio.to_thread(db.set_setting, _CHART_ANALYSIS_KEY, _utcnow_iso())
                        await asyncio.to_thread(
                            db.write_log, "ENGINE",
                            f"chart vision: analysed {len(db_watchlist)} symbols "
                            f"(7-month daily bars, next run in 7 days)"
                        )
                    except Exception as _ca_exc:                # noqa: BLE001
                        log.warning("analyze_charts failed: %s", _ca_exc)
                else:
                    _days_left = max(0.0, _CHART_ANALYSIS_INTERVAL_DAYS - _age_days)
                    log.debug("Chart analysis throttled — next run in %.1f day(s)", _days_left)

            await asyncio.to_thread(db.set_setting, SETTING_TRADIER_OK_TS, _utcnow_iso())
            await asyncio.to_thread(db.set_setting, SETTING_TRADIER_ERROR, "")
            await asyncio.to_thread(db.set_setting, "market_session", mkt["session"])
            log.info("tick complete: %s  [%s]", stats, session_label())
            await asyncio.to_thread(
                db.write_log, "ENGINE",
                f"heartbeat tick complete: {stats} | {session_label()}"
            )
            _cb_fail_count = 0
        except Exception as exc:                                  # noqa: BLE001
            _cb_fail_count += 1
            log.exception("tick failed: %s", exc)
            try:
                exc_str = str(exc)[:500]
                llm_keywords = ("api.ollama.com", "openai", "LLMConnection",
                                "chat/completions", "llm", "unauthorized")
                is_llm_err = any(kw.lower() in exc_str.lower() for kw in llm_keywords)
                if is_llm_err:
                    await asyncio.to_thread(db.set_setting, SETTING_LLM_ERROR, exc_str)
                else:
                    await asyncio.to_thread(db.set_setting, SETTING_TRADIER_ERROR, exc_str)
                await asyncio.to_thread(db.write_log, "ENGINE", f"tick failed: {exc}", level="ERROR")
            except Exception:                                     # noqa: BLE001
                pass

        # Incremental sleep to check for shutdown signals
        slept = 0
        while slept < interval_s:
            if _SHUTDOWN_EVENT.is_set():
                break
            await asyncio.sleep(1)
            slept += 1
        if _SHUTDOWN_EVENT.is_set():
            log.info("Agent loop detected shutdown signal. Exiting.")
            break

    # Clean up stream client, overseer, event bus on exit
    if not is_mock:
        await stream_client.stop()
    if engine.overseer is not None:
        await engine.overseer.stop()
    await event_bus.stop()


if __name__ == "__main__":
    conf = {
        "watchlist": [s for s in os.environ.get("HERMES_WATCHLIST", "").split(",") if s.strip()],
        "ai_autonomy": os.environ.get("HERMES_AI_AUTONOMY", "advisory"),
        "tick_interval_s": int(os.environ.get("HERMES_TICK_INTERVAL", 300)),
        "dry_run": os.environ.get("HERMES_DRY_RUN", "true").lower() == "true",
        # Initial mode if no setting is stored yet — paper is the safe default.
        "mode": os.environ.get("HERMES_MODE", "paper").lower(),
    }

    # Chart provider — renders dark-theme candlestick PNG snapshots from
    # TimescaleDB bars and caches them for the LLM vision layer.
    # Gracefully degrades to None if matplotlib isn't installed.
    _chart_provider = None
    try:
        from hermes.charts.provider import HermesChartProvider
        _chart_db = HermesDB(os.environ.get("HERMES_DSN",
                                            "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
        _chart_provider = HermesChartProvider(_chart_db, lookback_days=210, cache_ttl_s=300)
        _chart_provider.start(conf["watchlist"])
        log.info("HermesChartProvider started — warming up charts for %s", conf["watchlist"])
    except ImportError:
        log.warning("matplotlib not installed — chart vision disabled (pip install matplotlib)")
    except Exception as _chart_exc:                              # noqa: BLE001
        log.warning("HermesChartProvider init failed — vision disabled: %s", _chart_exc)

    # ML Predictor — background thread for daily XGBoost forecasting.
    try:
        from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer
        _ml_db = HermesDB(os.environ.get("HERMES_DSN",
                                         "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
        _ml_broker = _build_broker(conf, conf.get("mode", "paper"))
        _ml_predictor = AsyncXGBPredictor(_ml_db, FeatureEngineer(), _ml_broker, conf["watchlist"])
        _ml_predictor.start()
        log.info("AsyncXGBPredictor started — daily forecasting enabled for %s", conf["watchlist"])
    except ImportError:
        log.warning("xgboost or pandas not installed — ML predictor disabled (pip install xgboost pandas)")
    except Exception as _ml_exc:                                  # noqa: BLE001
        log.warning("AsyncXGBPredictor init failed: %s", _ml_exc)

    run(_chart_provider, conf)


def start_agent_thread() -> threading.Thread:
    """Helper to spin up the agent loop in a background thread of the watcher process."""
    conf = {
        "watchlist": [s for s in os.environ.get("HERMES_WATCHLIST", "").split(",") if s.strip()],
        "ai_autonomy": os.environ.get("HERMES_AI_AUTONOMY", "advisory"),
        "tick_interval_s": int(os.environ.get("HERMES_TICK_INTERVAL", 300)),
        "dry_run": os.environ.get("HERMES_DRY_RUN", "true").lower() == "true",
        "mode": os.environ.get("HERMES_MODE", "paper").lower(),
    }
    
    def target():
        log.info("Agent background thread starting setup...")
        _chart_provider = None
        try:
            from hermes.charts.provider import HermesChartProvider
            _chart_db = HermesDB(os.environ.get("HERMES_DSN",
                                                "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
            _chart_provider = HermesChartProvider(_chart_db, lookback_days=210, cache_ttl_s=300)
            _chart_provider.start(conf["watchlist"])
            log.info("HermesChartProvider started in agent thread")
        except ImportError:
            log.warning("matplotlib not installed — chart vision disabled (pip install matplotlib)")
        except Exception as exc:
            log.warning("HermesChartProvider init failed: %s", exc)

        try:
            from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer
            _ml_db = HermesDB(os.environ.get("HERMES_DSN",
                                             "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
            _ml_broker = _build_broker(conf, conf.get("mode", "paper"))
            _ml_predictor = AsyncXGBPredictor(_ml_db, FeatureEngineer(), _ml_broker, conf["watchlist"])
            _ml_predictor.start()
            log.info("AsyncXGBPredictor started in agent thread")
        except ImportError:
            log.warning("xgboost or pandas not installed — ML predictor disabled (pip install xgboost pandas)")
        except Exception as exc:
            log.warning("AsyncXGBPredictor init failed: %s", exc)

        _SHUTDOWN_EVENT.clear()
        run(_chart_provider, conf)

    thread = threading.Thread(target=target, name="HermesAgentThread", daemon=True)
    thread.start()
    return thread
