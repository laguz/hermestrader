"""
[Service-1: Hermes-Agent-Core] — Entry point.
Wires broker → DB → strategies → cascading engine → overseer, then ticks
on a schedule. Runs as its own process.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Any, Dict, Optional

from hermes.common import (
    IPC_CHANNEL_AGENT_COMMANDS,
    STRATEGY_PRIORITIES,
    VALID_MODES,
)
from hermes.db.models import HermesDB
from hermes.service1_agent.core import IronCondorBuilder
from hermes.service1_agent.strategies import (
    CreditSpreads75, CreditSpreads7, TastyTrade45, WheelStrategy, HermesAlpha,
)
from hermes.market_hours import market_session

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("hermes.agent.main")

# Helper clusters split out of this module. Re-imported here so the public
# surface (`from ...main import X`) and test monkeypatches
# (`patch("...main.X")`) keep resolving, and the run loop below can keep
# calling them by bare name.
from .agent_settings import (
    SETTING_MODE, SETTING_TRADIER_ERROR,
    SETTING_AGENT_STARTED_AT,
    SETTING_SOUL, SETTING_AUTONOMY, SETTING_PAUSED,
    SETTING_LLM_OUT_OF_LOOP,
    _read_overseer_settings,
)
from .agent_risk import resolve_max_daily_loss, enforce_daily_loss_limit
from .agent_approvals import _execute_approved_action
from .agent_construction import (
    _live_armed, _resolve_mode_credentials, _build_broker,
    _build_stream_client, _build_llm, build,
    _load_and_validate_runtime_config,
)
from .agent_reactive import (
    prewarm_quote_chain_cache, handle_ipc_command,
)

class ShutdownEvent(threading.Event):
    def set(self) -> None:
        super().set()
        global _ASYNC_TRIGGER_EVENT, _ASYNC_SHUTDOWN_EVENT
        if _ASYNC_TRIGGER_EVENT is not None:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(_ASYNC_TRIGGER_EVENT.set)
            except RuntimeError:
                pass
        if _ASYNC_SHUTDOWN_EVENT is not None:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(_ASYNC_SHUTDOWN_EVENT.set)
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
_ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None


def set_trigger() -> None:
    """Trigger the agent loop to wake up immediately.
    Sets both threading and asyncio events.
    """
    _TRIGGER_EVENT.set()




# VALID_MODES, VALID_AUTONOMY, DEFAULT_LLM_TIMEOUT_S,
# and STRATEGY_PRIORITIES are imported from hermes.common above.










from hermes.utils import utcnow_iso as _utcnow_iso
















# ---------------------------------------------------------------------------
# Tick loop — re-reads the desired mode each iteration so the watcher's
# toggle takes effect within one tick interval.
# ---------------------------------------------------------------------------
def run(chart_provider, conf: Dict[str, Any]) -> None:
    asyncio.run(_run_async(chart_provider, conf))


async def _run_async(chart_provider, conf: Dict[str, Any]) -> None:
    # Register the main event loop globally for cross-thread run_maybe_async routing
    from hermes.ml.predictor_config import set_main_loop
    set_main_loop(asyncio.get_running_loop())

    global _ASYNC_TRIGGER_EVENT, _ASYNC_SHUTDOWN_EVENT
    _ASYNC_TRIGGER_EVENT = asyncio.Event()
    _ASYNC_SHUTDOWN_EVENT = asyncio.Event()
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
    # Apply schema migrations before anything else
    try:
        await db.run_migrations()
    except Exception as exc:
        log.exception("run_migrations failed at startup: %s", exc)
    # Startup update check and soul syncing
    try:
        from hermes.utils import sync_soul_file_to_db, check_for_updates
        import threading
        await sync_soul_file_to_db(db)
        threading.Thread(target=check_for_updates, daemon=True).start()
    except Exception as exc:
        log.exception("Agent startup update/soul sync failed: %s", exc)
    try:
        await db.watchlist.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:
        log.exception("ensure_strategies failed at startup: %s", exc)

    if chart_provider is not None:
        chart_provider.start(conf["watchlist"])
        log.info("HermesChartProvider started — warming up charts for %s", conf["watchlist"])

    # Phase 3 Configuration Validation at startup
    try:
        runtime_config = await _load_and_validate_runtime_config(db, conf)
        log.info("Runtime settings validated successfully: %s", runtime_config.model_dump())
        
        # Persist defaults/fallbacks back to DB if they were not there
        if await db.settings.get_setting("obp_reserve") is None:
            await db.settings.set_setting("obp_reserve", str(runtime_config.obp_reserve))
        if await db.settings.get_setting("tick_interval") is None:
            await db.settings.set_setting("tick_interval", str(runtime_config.tick_interval))
    except Exception as exc:
        log.error("Fatal startup settings validation error: %s", exc)
        raise

    # Initial mode comes from settings (so the operator's last toggle wins
    # across restarts) and falls back to env config on first ever boot.
    initial_mode = (await db.settings.get_setting(SETTING_MODE) or conf.get("mode") or "paper").lower()
    if initial_mode not in VALID_MODES:
        initial_mode = "paper"
    await db.settings.set_setting(SETTING_MODE, initial_mode)
    await db.settings.set_setting(SETTING_AGENT_STARTED_AT, _utcnow_iso())

    current_mode = initial_mode
    broker = _build_broker(conf, current_mode)

    # LLM client is built from settings
    current_llm, current_llm_snapshot, current_vision = await _build_llm(db)

    # Operator doctrine + autonomy + pause are tracked together.
    current_overseer_cfg = await _read_overseer_settings(db, conf)
    await db.settings.set_setting(SETTING_AUTONOMY, current_overseer_cfg["autonomy"])
    await db.settings.set_setting(SETTING_PAUSED, "true" if current_overseer_cfg["paused"] else "false")
    if await db.settings.get_setting(SETTING_SOUL) is None:
        await db.settings.set_setting(SETTING_SOUL, "")
    if await db.settings.get_setting(SETTING_LLM_OUT_OF_LOOP) is None:
        await db.settings.set_setting(SETTING_LLM_OUT_OF_LOOP, "true")

    # Initialize Event Bus
    from hermes.events.bus import EventBus
    event_bus = EventBus()
    event_bus.start()

    # Instantiate ControlState
    from hermes.service1_agent.control_state import ControlState
    control_state = ControlState()
    await control_state.load_from_db(db, conf)

    engine = build(broker, current_llm, chart_provider, conf,
                   vision_enabled=current_vision,
                   autonomy=current_overseer_cfg["autonomy"],
                   soul=current_overseer_cfg["soul"],
                   approval_mode=current_overseer_cfg["approval_mode"],
                   strategy_enabled=current_overseer_cfg["strategy_enabled"],
                   llm_out_of_loop=current_overseer_cfg["llm_out_of_loop"],
                   overseer_mode=current_overseer_cfg.get("overseer_mode", "single"),
                   event_bus=event_bus)

    engine.control_state = control_state

    # Start the async Overseer background task if present
    if engine.overseer is not None:
        await engine.overseer.start()

    # Reactive settings/event subscriptions for ControlState and engine updates
    from hermes.db.events import (
        WatchlistChangedEvent, ModeChangedEvent, StrategyToggledEvent,
        AutonomyChangedEvent, PauseChangedEvent, ApprovalDecidedEvent,
        DoctrineUpdatedEvent, SystemSettingChangedEvent
    )

    # Subscriptions for updating control state reactively
    async def _handle_control_state_event(ev):
        control_state.update_with_event(ev)

    for cls in (WatchlistChangedEvent, ModeChangedEvent, StrategyToggledEvent,
                AutonomyChangedEvent, PauseChangedEvent, ApprovalDecidedEvent,
                DoctrineUpdatedEvent, SystemSettingChangedEvent):
        event_bus.subscribe(cls, _handle_control_state_event)

    # Reactively handle mode changes
    async def _handle_mode_change(ev: ModeChangedEvent) -> None:
        nonlocal current_mode, broker, stream_client
        desired_mode = ev.mode.lower()
        if desired_mode not in VALID_MODES:
            return
        if desired_mode != current_mode:
            log.warning("Mode change requested reactively: %s → %s", current_mode, desired_mode)
            try:
                new_broker = _build_broker(conf, desired_mode)
                
                # Update broker references on engine and strategies in-place
                engine.broker.broker = new_broker
                engine.mm.broker.broker = new_broker
                for s in engine.strategies:
                    s.broker.broker = new_broker

                # Stop and rebuild stream client
                if stream_client is not None:
                    await stream_client.stop()
                watchlist_syms = set(conf.get("watchlist", []))
                try:
                    watchlist_syms.update(await db.trades.tracked_option_symbols())
                except Exception as exc:
                    log.warning("Failed to query tracked option symbols for watchlist: %s", exc)
                stream_client = _build_stream_client(new_broker, db, event_bus, watchlist_syms)
                await stream_client.start()

                from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
                AsyncBrokerWrapper.clear_cache()
                current_mode = desired_mode
                await db.logs.write_log("ENGINE", f"mode switched to {current_mode}")
            except Exception as exc:
                log.exception("Mode switch to %s failed: %s", desired_mode, exc)
                await db.settings.set_setting(SETTING_TRADIER_ERROR, f"mode switch failed: {exc}")

    event_bus.subscribe(ModeChangedEvent, _handle_mode_change)

    # Reactively handle settings changes (LLM and Overseer parameters)
    async def _handle_settings_changed(ev) -> None:
        nonlocal current_llm, current_llm_snapshot, current_vision
        new_llm, new_snapshot, new_vision = await _build_llm(db)
        if new_snapshot != current_llm_snapshot:
            log.warning("LLM config change reactively: %s → %s", current_llm_snapshot, new_snapshot)
            if engine.overseer is not None:
                await engine.overseer.stop()

            current_llm = new_llm
            current_llm_snapshot = new_snapshot
            current_vision = new_vision

            if engine.overseer is not None:
                engine.overseer.llm = current_llm
                engine.overseer.vision_enabled = current_vision
                await engine.overseer.start()

            await db.logs.write_log(
                "ENGINE",
                f"LLM swapped reactively: provider={new_snapshot['provider']} model={new_snapshot['model'] or '-'}"
            )

        new_overseer_cfg = await _read_overseer_settings(db, conf)
        if engine.overseer is not None:
            engine.overseer.autonomy = new_overseer_cfg["autonomy"]
            engine.overseer.soul = new_overseer_cfg["soul"]
            engine.overseer.overseer_mode = new_overseer_cfg.get("overseer_mode", "single")
        engine.approval_mode = new_overseer_cfg["approval_mode"]
        engine.llm_out_of_loop = new_overseer_cfg["llm_out_of_loop"]

        # Re-build engine active strategies list based on new settings
        common = dict(broker=engine.broker, db=db, money_manager=engine.mm, ic_builder=IronCondorBuilder(engine.mm),
                      config=conf, overseer=engine.overseer, dry_run=conf.get("dry_run", False))
        all_strategies = [
            CreditSpreads75(**common),
            CreditSpreads7(**common),
            TastyTrade45(**common),
            WheelStrategy(**common),
            HermesAlpha(**common),
        ]
        enabled = new_overseer_cfg["strategy_enabled"]
        engine.strategies = sorted([s for s in all_strategies if enabled.get(s.NAME, True)], key=lambda s: s.PRIORITY)

    for cls in (SystemSettingChangedEvent, DoctrineUpdatedEvent, StrategyToggledEvent, AutonomyChangedEvent, PauseChangedEvent):
        event_bus.subscribe(cls, _handle_settings_changed)

    # Connect IPC
    from hermes.ipc import ipc
    await ipc.connect(db)

    async def _ipc_callback(data: dict):
        await handle_ipc_command(data, control_state, db, conf, event_bus)

    await ipc.subscribe(IPC_CHANNEL_AGENT_COMMANDS, _ipc_callback)

    # Track watchlist symbols + active DB option legs
    watchlist_syms = set(conf.get("watchlist", []))
    try:
        watchlist_syms.update(await db.trades.tracked_option_symbols())
    except Exception as exc:
        log.warning("Failed to load tracked option symbols: %s", exc)

    stream_client = _build_stream_client(broker, db, event_bus, watchlist_syms)
    await stream_client.start()
    
    # Event-driven cache pre-warming
    from hermes.events.bus import CacheWarmTick

    async def _handle_cache_warm_tick(event: CacheWarmTick) -> None:
        await prewarm_quote_chain_cache(engine, db, conf, _SHUTDOWN_EVENT)

    event_bus.subscribe(CacheWarmTick, _handle_cache_warm_tick)

    # Initialize and wire ML Predictor
    try:
        from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer
        _ml_db = HermesDB(os.environ.get("HERMES_DSN",
                                         "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"))
        _ml_broker = _build_broker(conf, conf.get("mode", "paper"))
        _ml_predictor = AsyncXGBPredictor(_ml_db, FeatureEngineer(), _ml_broker, conf["watchlist"])
        _ml_predictor.start(event_bus=event_bus)
        # Strategies share `conf` by reference: expose the in-process
        # prediction cache so POP scoring sees the calibrated predicted_prob
        # and quantile bands that the persisted predictions row lacks.
        conf["xgb_predict_latest"] = _ml_predictor.predict_latest
        log.info("AsyncXGBPredictor started under EventBus forecasting.")
    except ImportError:
        log.warning("xgboost or pandas not installed — ML predictor disabled")
    except Exception as _ml_exc:
        log.warning("AsyncXGBPredictor init failed: %s", _ml_exc)

    # Initialize and wire DB-backed regime weights lookup if enabled
    try:
        regime_weights_env = os.environ.get("HERMES_REGIME_WEIGHTS", "false").lower() == "true"
        regime_weights_setting = (await db.settings.get_setting("regime_weights_enabled") or "false").lower() == "true"
        if regime_weights_env or regime_weights_setting:
            from hermes.ml import pop_engine, regime_weights
            regime_weights.ensure_table(db)
            lookup_fn = regime_weights.make_lookup_fn(db, event_bus)
            if hasattr(lookup_fn, "initialize"):
                await lookup_fn.initialize()
            pop_engine.set_regime_weight_lookup(lookup_fn)
            log.info("DB-backed regime weights lookup wired and warmed up.")
        else:
            log.info("DB-backed regime weights lookup is disabled (gate is off).")
    except Exception as _rw_exc:
        log.warning("DB-backed regime weights lookup init failed, falling back to static defaults: %s", _rw_exc)

    # Wire and start the Scheduler
    from hermes.service1_agent.scheduler import Scheduler
    scheduler = Scheduler(event_bus, runtime_config.tick_interval)
    scheduler.start()

    log.info("Hermes Agent started mode=%s autonomy=%s paused=%s soul=%dB",
             current_mode, current_overseer_cfg["autonomy"],
             current_overseer_cfg["paused"], len(current_overseer_cfg["soul"]))

    # Trigger initial pre-warm immediately
    event_bus.emit(CacheWarmTick())

    # Monitor trigger event for immediate wakeups (e.g. for approvals / test triggers)
    async def _trigger_monitor_loop() -> None:
        from hermes.events.bus import ClockTickEvent
        while not _ASYNC_SHUTDOWN_EVENT.is_set():
            try:
                await _ASYNC_TRIGGER_EVENT.wait()
                _ASYNC_TRIGGER_EVENT.clear()
                _TRIGGER_EVENT.clear()
                if not _ASYNC_SHUTDOWN_EVENT.is_set():
                    log.info("[TRIGGER] Wakeup signal received, emitting ClockTickEvent immediately")
                    event_bus.emit(ClockTickEvent())
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Error in trigger monitor loop: %s", e)

    trigger_task = asyncio.create_task(_trigger_monitor_loop())

    # Wait for the shutdown signal
    await _ASYNC_SHUTDOWN_EVENT.wait()

    # Clean up trigger monitor, stream client, scheduler, overseer, event bus on exit
    trigger_task.cancel()
    try:
        await trigger_task
    except asyncio.CancelledError:
        pass

    await scheduler.stop()
    if stream_client:
        await stream_client.stop()
    if engine.overseer is not None:
        await engine.overseer.stop()
    await event_bus.stop()

    try:
        await ipc.unsubscribe(IPC_CHANNEL_AGENT_COMMANDS, _ipc_callback)
        await ipc.disconnect()
    except Exception as exc:
        log.warning("Failed to unsubscribe or disconnect from IPC during shutdown: %s", exc)


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
    except ImportError:
        log.warning("matplotlib not installed — chart vision disabled (pip install matplotlib)")
    except Exception as _chart_exc:
        log.warning("HermesChartProvider init failed — vision disabled: %s", _chart_exc)

    # ML Predictor — will be started inside _run_async reactively.
    log.info("ML Predictor setup completed — will run reactively inside _run_async")

    run(_chart_provider, conf)
