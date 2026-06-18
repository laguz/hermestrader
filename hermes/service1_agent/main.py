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
    LLM_PROVIDER_BASE_URLS,
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
    CreditSpreads7, CreditSpreads75, HermesAlpha, TastyTrade45, WheelStrategy,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("hermes.agent.main")

# Helper clusters split out of this module. Re-imported here so the public
# surface (`from ...main import X`) and test monkeypatches
# (`patch("...main.X")`) keep resolving, and the run loop below can keep
# calling them by bare name.
from .agent_settings import (  # noqa: F401,E402
    SETTING_MODE, SETTING_TRADIER_OK_TS, SETTING_TRADIER_ERROR,
    SETTING_AGENT_STARTED_AT, SETTING_LLM_PROVIDER, SETTING_LLM_BASE_URL,
    SETTING_LLM_MODEL, SETTING_LLM_API_KEY, SETTING_LLM_TEMPERATURE,
    SETTING_LLM_VISION, SETTING_LLM_TIMEOUT, SETTING_LLM_OK_TS,
    SETTING_LLM_ERROR, SETTING_SOUL, SETTING_AUTONOMY, SETTING_PAUSED,
    SETTING_APPROVAL_MODE, SETTING_LLM_OUT_OF_LOOP, SETTING_MAX_DAILY_LOSS,
    _strategy_enabled_key, _read_overseer_settings,
)
from .agent_risk import (  # noqa: F401,E402
    resolve_max_daily_loss, _open_position_pnl, enforce_daily_loss_limit,
)
from .agent_approvals import (  # noqa: F401,E402
    _REJECTED_ORDER_STATUSES, _execute_approved_action,
)
from .agent_construction import (  # noqa: F401,E402
    _live_armed, _resolve_mode_credentials, _build_broker,
    _build_stream_client, _build_llm, build,
    _load_and_validate_runtime_config,
)

class ShutdownEvent(threading.Event):
    def set(self) -> None:
        super().set()
        global _ASYNC_TRIGGER_EVENT
        if _ASYNC_TRIGGER_EVENT is not None:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(_ASYNC_TRIGGER_EVENT.set)
            except RuntimeError:
                pass


class TriggerEvent(threading.Event):
    def set(self) -> None:
        super().set()
        global _ASYNC_TRIGGER_EVENT
        if _ASYNC_TRIGGER_EVENT is not None:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(_ASYNC_TRIGGER_EVENT.set)
            except RuntimeError:
                pass


_SHUTDOWN_EVENT = ShutdownEvent()
_TRIGGER_EVENT = TriggerEvent()
_ASYNC_TRIGGER_EVENT: Optional[asyncio.Event] = None


def set_trigger() -> None:
    """Trigger the agent loop to wake up immediately.
    Sets both threading and asyncio events.
    """
    _TRIGGER_EVENT.set()




async def _interruptible_sleep(seconds: float) -> None:
    """Sleep that wakes up immediately on shutdown or trigger signals."""
    steps = int(seconds * 10)
    for _ in range(steps):
        if _SHUTDOWN_EVENT.is_set() or _TRIGGER_EVENT.is_set():
            break
        await asyncio.sleep(0.1)


async def _cache_prewarm_loop(broker_getter, db, conf):
    """Background loop to periodically pre-warm the broker data cache for watchlist symbols."""
    from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
    
    # Wait a few seconds after startup before the first run to let other tasks initialize
    await asyncio.sleep(5)
    
    while True:
        if _SHUTDOWN_EVENT.is_set():
            break
        try:
            current_broker = broker_getter()
            wrapper = AsyncBrokerWrapper(current_broker, db)
            cache = wrapper._shared_cache
            
            # Get watchlist symbols
            watchlist_syms = set(conf.get("watchlist", []))
            try:
                all_wls = await db.list_all_watchlists()
                for syms in all_wls.values():
                    watchlist_syms.update(syms)
            except Exception:
                pass
                
            symbols = sorted(list(watchlist_syms))
            if symbols:
                now_ts = wrapper._get_current_timestamp()
                log.info("[PRE-WARM] Refreshing quote/chain cache for watchlist: %s", symbols)
                
                # 1. Fetch & cache quotes directly to bypass wrapper cache check
                try:
                    quotes = await wrapper.broker.get_quote(",".join(symbols))
                    if quotes and isinstance(quotes, list):
                        for q in quotes:
                            sym = q.get("symbol")
                            if sym:
                                cache.set_quote(sym, q, now_ts)
                except Exception as q_exc:
                    log.debug("[PRE-WARM] Quote fetch failed: %s", q_exc)
                    
                # 2. Fetch & cache expirations & chains (for nearest expirations)
                for sym in symbols:
                    if _SHUTDOWN_EVENT.is_set():
                        break
                    try:
                        expirations = await wrapper.broker.get_option_expirations(sym)
                        if expirations:
                            cache.set_expirations(sym, expirations, now_ts)
                            
                            # Determine simulated or real 'today' for DTE calculations
                            today = datetime.utcnow().date()
                            if hasattr(wrapper.broker, "current_date") and wrapper.broker.current_date:
                                today = wrapper.broker.current_date.date()
                                
                            valid_expiries = []
                            for e in expirations:
                                try:
                                    d = datetime.strptime(str(e), "%Y-%m-%d").date()
                                    dte = (d - today).days
                                    # Cache option chains between 5 and 50 DTE
                                    if 5 <= dte <= 50:
                                        valid_expiries.append((dte, e))
                                except Exception:
                                    continue
                                    
                            valid_expiries.sort()
                            # Warm up option chains for the 2 nearest relevant expirations
                            for _, exp in valid_expiries[:2]:
                                if _SHUTDOWN_EVENT.is_set():
                                    break
                                try:
                                    chain = await wrapper.broker.get_option_chains(sym, exp)
                                    if chain:
                                        cache.set_chain(sym, exp, chain, now_ts)
                                except Exception as c_exc:
                                    log.debug("[PRE-WARM] Chain fetch failed for %s %s: %s", sym, exp, c_exc)
                    except Exception as exp_exc:
                        log.debug("[PRE-WARM] Expirations fetch failed for %s: %s", sym, exp_exc)
                        
        except Exception as exc:
            log.warning("[PRE-WARM] General cache pre-warm tick failed: %s", exc)
            
        # Sleep for 120 seconds, checking shutdown status periodically (every 5 seconds)
        for _ in range(24):
            if _SHUTDOWN_EVENT.is_set():
                break
            await asyncio.sleep(5)


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












# ---------------------------------------------------------------------------
# Tick loop — re-reads the desired mode each iteration so the watcher's
# toggle takes effect within one tick interval.
# ---------------------------------------------------------------------------
def run(chart_provider, conf: Dict[str, Any]) -> None:
    asyncio.run(_run_async(chart_provider, conf))


async def _run_async(chart_provider, conf: Dict[str, Any]) -> None:
    global _ASYNC_TRIGGER_EVENT
    _ASYNC_TRIGGER_EVENT = asyncio.Event()
    stream_client = None

    import signal
    def handle_signal(sig, frame):
        log.info("Received signal %s, setting shutdown event...", sig)
        _SHUTDOWN_EVENT.set()
        set_trigger()

    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except ValueError:
        # signal only works in main thread, ignore if running under testing
        pass

    db = HermesDB(os.environ.get("HERMES_DSN",
                                 "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
    # Apply schema migrations before anything else so fresh deployments are
    # never left running against a stale schema (e.g. missing expires_at).
    try:
        await db.run_migrations()
    except Exception as exc:                                      # noqa: BLE001
        log.exception("run_migrations failed at startup: %s", exc)
    # Startup update check and soul syncing
    try:
        from hermes.utils import sync_soul_file_to_db, check_for_updates
        import threading
        await sync_soul_file_to_db(db)
        threading.Thread(target=check_for_updates, daemon=True).start()
    except Exception as exc:                                      # noqa: BLE001
        log.exception("Agent startup update/soul sync failed: %s", exc)
    try:
        await db.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:                                      # noqa: BLE001
        log.exception("ensure_strategies failed at startup: %s", exc)

    # Phase 3 Configuration Validation at startup
    try:
        runtime_config = await _load_and_validate_runtime_config(db, conf)
        log.info("Runtime settings validated successfully: %s", runtime_config.model_dump())
        
        # Persist defaults/fallbacks back to DB if they were not there
        if await db.get_setting("obp_reserve") is None:
            await db.set_setting("obp_reserve", str(runtime_config.obp_reserve))
        if await db.get_setting("tick_interval") is None:
            await db.set_setting("tick_interval", str(runtime_config.tick_interval))
        if await db.get_setting("bandit_tuner_mode") is None:
            await db.set_setting("bandit_tuner_mode", runtime_config.bandit_tuner_mode)
        if await db.get_setting("exit_policy_mode") is None:
            await db.set_setting("exit_policy_mode", runtime_config.exit_policy_mode)
    except Exception as exc:
        log.error("Fatal startup settings validation error: %s", exc)
        raise

    # Initial mode comes from settings (so the operator's last toggle wins
    # across restarts) and falls back to env config on first ever boot.
    initial_mode = (await db.get_setting(SETTING_MODE) or conf.get("mode") or "paper").lower()
    if initial_mode not in VALID_MODES:
        initial_mode = "paper"
    await db.set_setting(SETTING_MODE, initial_mode)
    await db.set_setting(SETTING_AGENT_STARTED_AT, _utcnow_iso())

    current_mode = initial_mode
    broker = _build_broker(conf, current_mode)
    is_mock = "mock" in str(type(broker)).lower()

    # LLM client is built from settings rather than the hard-coded MockLLM
    # passed to run() — that lets the watcher swap providers at runtime.
    current_llm, current_llm_snapshot, current_vision = await _build_llm(db)

    # Operator doctrine + autonomy + pause are tracked together. The first
    # snapshot also seeds defaults from env/conf when the watcher hasn't
    # written anything yet.
    current_overseer_cfg = await _read_overseer_settings(db, conf)
    await db.set_setting(SETTING_AUTONOMY, current_overseer_cfg["autonomy"])
    await db.set_setting(SETTING_PAUSED, "true" if current_overseer_cfg["paused"] else "false")
    if await db.get_setting(SETTING_SOUL) is None:
        await db.set_setting(SETTING_SOUL, "")
    if await db.get_setting(SETTING_LLM_OUT_OF_LOOP) is None:
        await db.set_setting(SETTING_LLM_OUT_OF_LOOP, "true")

    # Initialize Event Bus
    from hermes.events.bus import EventBus, OrderFillEvent
    event_bus = EventBus()
    event_bus.start()
    event_bus.subscribe(OrderFillEvent, lambda ev: set_trigger())

    engine = build(broker, current_llm, chart_provider, conf,
                   vision_enabled=current_vision,
                   autonomy=current_overseer_cfg["autonomy"],
                   soul=current_overseer_cfg["soul"],
                   approval_mode=current_overseer_cfg["approval_mode"],
                   strategy_enabled=current_overseer_cfg["strategy_enabled"],
                   llm_out_of_loop=current_overseer_cfg["llm_out_of_loop"],
                   overseer_mode=current_overseer_cfg.get("overseer_mode", "monolithic"),
                   event_bus=event_bus)

    # Start the async Overseer background task if present
    if engine.overseer is not None:
        await engine.overseer.start()

    from hermes.ipc import ipc
    await ipc.connect(db)

    async def _ipc_callback(data: dict):
        action = data.get("action")
        if action == "trigger_approvals":
            log.info("[IPC] Received trigger approvals signal")
            set_trigger()
        elif action == "sync_settings":
            log.info("[IPC] Received sync settings signal")
            set_trigger()
        elif action == "trigger_ml":
            log.info("[IPC] Received trigger ML signal")
            try:
                await db.set_setting("ml_force_run", "true")
            except Exception:
                pass
            set_trigger()

    await ipc.subscribe("agent_commands", _ipc_callback)

    # Track watchlist symbols + active DB option legs
    watchlist_syms = set(conf.get("watchlist", []))
    try:
        watchlist_syms.update(await db.tracked_option_symbols())
    except Exception:
        pass

    stream_client = _build_stream_client(broker, db, event_bus, watchlist_syms)
    await stream_client.start()
    
    # Spawn background pre-warming task for the option chain and quote cache
    prewarm_task = asyncio.create_task(_cache_prewarm_loop(lambda: broker, db, conf))
    
    interval_s = runtime_config.tick_interval
    log.info("Hermes Agent started mode=%s autonomy=%s paused=%s soul=%dB",
             current_mode, current_overseer_cfg["autonomy"],
             current_overseer_cfg["paused"], len(current_overseer_cfg["soul"]))

    # Circuit breaker: pause broker calls after N consecutive tick failures.
    _CB_THRESHOLD = 5          # consecutive failures before tripping
    _CB_COOLDOWN_S = 300       # seconds to wait before re-attempting
    _cb_fail_count = 0
    _cb_tripped_at: float = 0.0
    triggered = False

    while True:
        try:
            runtime_config = await _load_and_validate_runtime_config(db, conf)
            interval_s = runtime_config.tick_interval
        except Exception as exc:
            log.error("Tick settings validation failed: %s", exc)

        if _TRIGGER_EVENT.is_set():
            triggered = True
            _TRIGGER_EVENT.clear()

        # Circuit breaker
        if _cb_fail_count >= _CB_THRESHOLD:
            if _cb_tripped_at == 0.0:
                _cb_tripped_at = time.time()
                log.error(
                    "[CIRCUIT BREAKER] %d consecutive tick failures — pausing broker "
                    "calls for %ds", _cb_fail_count, _CB_COOLDOWN_S,
                )
                try:
                    await db.set_setting(
                        SETTING_TRADIER_ERROR,
                        f"circuit breaker tripped after {_cb_fail_count} failures"
                    )
                    await db.write_log(
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
            if triggered:
                log.info("[C2-TRIGGER] Waking up early to execute manual approvals")
                try:
                    desired_mode = (await db.get_setting(SETTING_MODE) or current_mode).lower()
                    if desired_mode not in VALID_MODES:
                        desired_mode = current_mode
                    if desired_mode != current_mode:
                        log.warning("mode change requested during trigger: %s → %s", current_mode, desired_mode)
                        broker = _build_broker(conf, desired_mode)
                        from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
                        AsyncBrokerWrapper.clear_cache()
                        engine = build(broker, current_llm, chart_provider, conf,
                                       vision_enabled=current_vision,
                                       autonomy=current_overseer_cfg["autonomy"],
                                       soul=current_overseer_cfg["soul"],
                                       approval_mode=current_overseer_cfg["approval_mode"],
                                       strategy_enabled=current_overseer_cfg["strategy_enabled"],
                                       llm_out_of_loop=current_overseer_cfg["llm_out_of_loop"],
                                       overseer_mode=current_overseer_cfg.get("overseer_mode", "monolithic"),
                                       event_bus=event_bus)
                        current_mode = desired_mode
                        await db.write_log("ENGINE", f"mode switched to {current_mode}")

                    approved = await db.fetch_approved_actions()
                    if approved:
                        log.info("[C2-TRIGGER] Executing %d approved action(s)", len(approved))
                        for item in approved:
                            await _execute_approved_action(item, broker=broker, db=db)
                    else:
                        log.info("[C2-TRIGGER] No approved actions found in queue")
                except Exception as exc:
                    log.exception("[C2-TRIGGER] Failed to process triggered approvals: %s", exc)

                triggered = False
                try:
                    await asyncio.wait_for(_ASYNC_TRIGGER_EVENT.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass
                finally:
                    _ASYNC_TRIGGER_EVENT.clear()
                if _SHUTDOWN_EVENT.is_set():
                    log.info("Agent loop detected shutdown signal during trigger sleep. Exiting.")
                    break
                continue

            # 1) Mode reconciliation — pick up any toggle the watcher made.
            desired_mode = (await db.get_setting(SETTING_MODE) or current_mode).lower()
            if desired_mode not in VALID_MODES:
                desired_mode = current_mode
            if desired_mode != current_mode:
                log.warning("mode change requested: %s → %s", current_mode, desired_mode)
                try:
                    broker = _build_broker(conf, desired_mode)
                    
                    # Stop and rebuild stream client
                    await stream_client.stop()
                    is_mock = "mock" in str(type(broker)).lower()
                    
                    watchlist_syms = set(conf.get("watchlist", []))
                    try:
                        watchlist_syms.update(await db.tracked_option_symbols())
                    except Exception:
                        pass
                        
                    stream_client = _build_stream_client(broker, db, event_bus, watchlist_syms)
                    await stream_client.start()
                    
                    engine = build(broker, current_llm, chart_provider, conf,
                                   vision_enabled=current_vision,
                                   autonomy=current_overseer_cfg["autonomy"],
                                   soul=current_overseer_cfg["soul"],
                                   approval_mode=current_overseer_cfg["approval_mode"],
                                   strategy_enabled=current_overseer_cfg["strategy_enabled"],
                                   llm_out_of_loop=current_overseer_cfg["llm_out_of_loop"],
                                   overseer_mode=current_overseer_cfg.get("overseer_mode", "monolithic"),
                                   event_bus=event_bus)
                    # Clear options/quotes cache on mode switch to prevent stale paper data in live mode
                    from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
                    AsyncBrokerWrapper.clear_cache()
                    current_mode = desired_mode
                    await db.write_log("ENGINE", f"mode switched to {current_mode}")
                except Exception as exc:                          # noqa: BLE001
                    log.exception("mode switch to %s failed: %s", desired_mode, exc)
                    await db.set_setting(SETTING_TRADIER_ERROR, f"mode switch failed: {exc}")

            # 1b) LLM reconciliation
            new_llm, new_snapshot, new_vision = await _build_llm(db)
            llm_changed = new_snapshot != current_llm_snapshot

            # 1c) Soul / autonomy / pause reconciliation
            new_overseer_cfg = await _read_overseer_settings(db, conf)
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
                               llm_out_of_loop=current_overseer_cfg["llm_out_of_loop"],
                               overseer_mode=current_overseer_cfg.get("overseer_mode", "monolithic"),
                               event_bus=event_bus)
                               
                if engine.overseer is not None:
                    await engine.overseer.start()
                    
                if llm_changed:
                    await db.write_log(
                        "ENGINE",
                        f"LLM swapped: provider={new_snapshot['provider']} "
                        f"model={new_snapshot['model'] or '-'}"
                    )
                if overseer_changed:
                    await db.write_log(
                        "ENGINE",
                        f"Overseer reconfigured: autonomy={new_overseer_cfg['autonomy']} "
                        f"paused={new_overseer_cfg['paused']} "
                        f"soul={len(new_overseer_cfg['soul'])}B"
                    )

            # 1d) Dynamic Watchlist Refresh
            try:
                all_wls = await db.list_all_watchlists()
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
            try:
                wl_syms = set(current_watchlist)
                wl_syms.update(await db.tracked_option_symbols())
                stream_client.update_watchlist(list(wl_syms))
            except Exception as exc:
                log.warning("Failed to update WebSocket watchlist: %s", exc)

            # 1c-kill) Daily-loss kill switch — auto-halt the tick if the day's
            # drawdown (realized today + open-position unrealized) breaches the
            # configured limit. Re-evaluated every tick, so a same-day resume
            # that is still under water trips again.
            _max_daily_loss = resolve_max_daily_loss(await db.get_setting(SETTING_MAX_DAILY_LOSS))
            if await enforce_daily_loss_limit(
                db, _max_daily_loss,
                currently_paused=current_overseer_cfg["paused"], broker=broker,
            ):
                current_overseer_cfg["paused"] = True
                try:
                    await asyncio.wait_for(_ASYNC_TRIGGER_EVENT.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass
                finally:
                    _ASYNC_TRIGGER_EVENT.clear()
                continue

            # 1d) Hard pause check
            if current_overseer_cfg["paused"]:
                await db.write_log("ENGINE", f"heartbeat tick PAUSED mode={current_mode}")
                try:
                    await asyncio.wait_for(_ASYNC_TRIGGER_EVENT.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass
                finally:
                    _ASYNC_TRIGGER_EVENT.clear()
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
                    _raw = await db.get_setting(_k)
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
                _ttl_raw = await db.get_setting("pending_order_ttl_s")
                _pending_ttl_s = int(_ttl_raw) if _ttl_raw else 3600
            except (TypeError, ValueError):
                _pending_ttl_s = 3600
            try:
                expired = await db.expire_stale_pending_orders(_pending_ttl_s)
                if expired:
                    log.info("Expired %d stale PENDING order(s)", expired)
                    await db.write_log("ENGINE", f"expired {expired} stale PENDING order(s)")
            except Exception as exc:                          # noqa: BLE001
                log.warning("expire_stale_pending_orders failed: %s", exc)

            # 1e-ii) Stale approval cleanup
            try:
                expired_approvals = await db.expire_stale_approvals()
                if expired_approvals:
                    log.info("Auto-expired %d stale approval(s)", expired_approvals)
                    await db.write_log(
                        "ENGINE",
                        f"auto-expired {expired_approvals} stale approval(s) past deadline"
                    )
            except Exception as exc:                          # noqa: BLE001
                log.warning("expire_stale_approvals failed: %s", exc)

            # 1f) Execute C2-approved orders
            if current_overseer_cfg["approval_mode"]:
                try:
                    approved = await db.fetch_approved_actions()
                    for item in approved:
                        await _execute_approved_action(item, broker=broker, db=db)
                except Exception as exc:                       # noqa: BLE001
                    log.warning("fetch_approved_actions failed: %s", exc)

            # 2) Heartbeat
            mkt = market_session()
            await db.write_log(
                "ENGINE",
                f"heartbeat tick start mode={current_mode} "
                f"market={mkt['session']} open={mkt['is_open']}"
            )

            # 3) Market-hours gate
            if not mkt["trading_day"]:
                nxt = next_open()
                await db.write_log(
                    "ENGINE",
                    f"market CLOSED — next open {nxt.strftime('%Y-%m-%d %H:%M ET')} "
                    f"({mkt['et_date']} is not a trading day)"
                )
                try:
                    await asyncio.wait_for(_ASYNC_TRIGGER_EVENT.wait(), timeout=interval_s)
                except asyncio.TimeoutError:
                    pass
                finally:
                    _ASYNC_TRIGGER_EVENT.clear()
                continue

            if mkt["is_open"]:
                # Full tick: management + entries
                stats = await engine.tick(current_watchlist)
            else:
                await engine.sync_positions()
                await engine.reconcile_orphans()
                stats = {"managed": 0, "entries": 0,
                         "note": f"all submissions skipped ({mkt['session']})"}

            # 4) Chart analysis
            _CHART_ANALYSIS_KEY = "chart_analysis_last_run"
            _CHART_ANALYSIS_INTERVAL_DAYS = 7
            if chart_provider is not None and engine.overseer is not None and db_watchlist:
                _should_run_charts = False
                _age_days: float = 0.0
                try:
                    _recent_decisions = await db.recent_ai_decisions(
                        strategy_id="CHART",
                        limit=max(len(db_watchlist) * 2, 20)
                    )
                    _analyzed_syms = {d["symbol"] for d in _recent_decisions}
                    _missing_analysis = any(s not in _analyzed_syms for s in db_watchlist)

                    if _missing_analysis:
                        _should_run_charts = True
                        log.info("Forcing chart analysis: some symbols in watchlist are missing analysis.")
                    else:
                        _last_chart_ts_raw = await db.get_setting(_CHART_ANALYSIS_KEY)
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
                        await engine.overseer.analyze_charts(db_watchlist)
                        await db.set_setting(_CHART_ANALYSIS_KEY, _utcnow_iso())
                        await db.write_log(
                            "ENGINE",
                            f"chart vision: analysed {len(db_watchlist)} symbols "
                            f"(7-month daily bars, next run in 7 days)"
                        )
                    except Exception as _ca_exc:                # noqa: BLE001
                        log.warning("analyze_charts failed: %s", _ca_exc)
                else:
                    _days_left = max(0.0, _CHART_ANALYSIS_INTERVAL_DAYS - _age_days)
                    log.debug("Chart analysis throttled — next run in %.1f day(s)", _days_left)

            await db.set_setting(SETTING_TRADIER_OK_TS, _utcnow_iso())
            await db.set_setting(SETTING_TRADIER_ERROR, "")
            await db.set_setting("market_session", mkt["session"])
            log.info("tick complete: %s  [%s]", stats, session_label())
            await db.write_log(
                "ENGINE",
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
                    await db.set_setting(SETTING_LLM_ERROR, exc_str)
                else:
                    await db.set_setting(SETTING_TRADIER_ERROR, exc_str)
                await db.write_log("ENGINE", f"tick failed: {exc}", level="ERROR")
            except Exception:                                     # noqa: BLE001
                pass

        try:
            await asyncio.wait_for(_ASYNC_TRIGGER_EVENT.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass
        finally:
            _ASYNC_TRIGGER_EVENT.clear()
        if _SHUTDOWN_EVENT.is_set():
            log.info("Agent loop detected shutdown signal. Exiting.")
            break

    # Clean up stream client, overseer, event bus on exit
    if stream_client:
        await stream_client.stop()
    if engine.overseer is not None:
        await engine.overseer.stop()
    await event_bus.stop()

    try:
        await ipc.unsubscribe("agent_commands", _ipc_callback)
        await ipc.disconnect()
    except Exception:
        pass


if __name__ == "__main__":
    conf = {
        "watchlist": [s for s in os.environ.get("HERMES_WATCHLIST", "").split(",") if s.strip()],
        "ai_autonomy": os.environ.get("HERMES_AI_AUTONOMY", "advisory"),
        "tick_interval_s": int(os.environ.get("HERMES_TICK_INTERVAL", 3600)),
        # How long an overseer VETO suppresses re-proposal of the identical
        # entry (seconds). 0 disables suppression. Repeat vetoes extend it.
        "veto_suppression_s": int(os.environ.get("HERMES_VETO_SUPPRESSION_S", 1800)),
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
        "tick_interval_s": int(os.environ.get("HERMES_TICK_INTERVAL", 3600)),
        # How long an overseer VETO suppresses re-proposal of the identical
        # entry (seconds). 0 disables suppression. Repeat vetoes extend it.
        "veto_suppression_s": int(os.environ.get("HERMES_VETO_SUPPRESSION_S", 1800)),
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
