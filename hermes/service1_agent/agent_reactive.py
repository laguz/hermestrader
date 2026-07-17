"""
[Service-1: Hermes-Agent-Core] — event-driven handlers split out of the run loop.

These are the two largest, fully self-contained reactive handlers from
``main._run_async``: the quote/chain cache pre-warm (fired by ``CacheWarmTick``)
and the IPC command callback (fired by Redis pub/sub on the agent-commands
channel). Both read their dependencies through explicit parameters and mutate no
run-loop state, so the bodies moved out of the run loop unchanged — ``main``
keeps a thin closure that forwards to them.

The stateful broker/LLM-swap handlers (``_handle_mode_change`` /
``_handle_settings_changed``) intentionally stay in ``main``: they mutate the
run loop's ``nonlocal`` state and call helpers that tests monkeypatch on the
``main`` module, so they must resolve names in ``main``'s namespace.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from hermes.common import (
    IPC_ACTION_DRAIN_COMMANDS,
    IPC_ACTION_SYNC_SETTINGS,
    IPC_ACTION_TRIGGER_APPROVALS,
    IPC_ACTION_TRIGGER_ML,
)
from hermes.market_hours import ET

# Reuse the run loop's logger name so operator log filters (e.g. "[PRE-WARM]"
# under hermes.agent.main) keep matching after the extraction.
log = logging.getLogger("hermes.agent.main")


from hermes.utils import utcnow_iso as _utcnow_iso



async def prewarm_quote_chain_cache(engine, db, conf: Dict[str, Any], shutdown_event) -> None:
    """Refresh the shared quote/chain cache for every watchlist symbol.

    Fired by ``CacheWarmTick``. Best-effort: every fetch is individually
    guarded so a single bad symbol never aborts the warm, and ``shutdown_event``
    is polled so a stopping agent bails out of the (potentially long) chain
    sweep promptly.
    """
    from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
    try:
        current_broker = engine.broker.broker
        wrapper = AsyncBrokerWrapper(current_broker, db)
        cache = wrapper._shared_cache

        watchlist_syms = set(conf.get("watchlist", []))
        try:
            all_wls = await db.watchlist.list_all_watchlists()
            for syms in all_wls.values():
                watchlist_syms.update(syms)
        except Exception as e:
            log.debug("Failed to list watchlists during quote warm-up: %s", e)

        symbols = sorted(list(watchlist_syms))
        if symbols:
            now_ts = wrapper._get_current_timestamp()
            log.info("[PRE-WARM] Refreshing quote/chain cache for watchlist: %s", symbols)

            try:
                quotes = await wrapper.broker.get_quote(",".join(symbols))
                if quotes and isinstance(quotes, list):
                    for q in quotes:
                        sym = q.get("symbol")
                        if sym:
                            cache.set_quote(sym, q, now_ts)
            except Exception as q_exc:
                log.debug("[PRE-WARM] Quote fetch failed: %s", q_exc)

            for sym in symbols:
                if shutdown_event.is_set():
                    break
                try:
                    expirations = await wrapper.broker.get_option_expirations(sym)
                    if expirations:
                        cache.set_expirations(sym, expirations, now_ts)

                        # ET trading day, not the raw UTC date — after ~8pm ET
                        # the UTC rollover shifts every DTE by one and expiries
                        # at the 5/50 window edges get mis-included/excluded.
                        today = datetime.now(timezone.utc).astimezone(ET).date()
                        if hasattr(wrapper.broker, "current_date") and wrapper.broker.current_date:
                            today = wrapper.broker.current_date.date()

                        valid_expiries = []
                        for e in expirations:
                            try:
                                d = datetime.strptime(str(e), "%Y-%m-%d").date()
                                dte = (d - today).days
                                if 5 <= dte <= 50:
                                    valid_expiries.append((dte, e))
                            except Exception:
                                continue

                        valid_expiries.sort()
                        for _, exp in valid_expiries[:2]:
                            if shutdown_event.is_set():
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


async def handle_ipc_command(data: dict, control_state, db, conf: Dict[str, Any], event_bus, engine) -> None:
    """Dispatch an inbound IPC message from the agent-commands channel.

    Either a serialized DB event (``event_type`` + ``payload``) to re-emit on the
    in-process bus, or one of the trigger ``action`` verbs (approvals / settings
    sync / ML retrain).
    """
    action = data.get("action")
    event_type = data.get("event_type")
    payload = data.get("payload")

    if event_type and payload is not None:
        if event_type == "CLOCK_TICK_EVENT":
            from hermes.events.bus import ClockTickEvent
            event_bus.emit(ClockTickEvent())
        elif event_type == "CACHE_WARM_TICK":
            from hermes.events.bus import CacheWarmTick
            event_bus.emit(CacheWarmTick())
        elif event_type == "ML_RETRAIN_TICK":
            from hermes.events.bus import MlRetrainTick
            event_bus.emit(MlRetrainTick())
        elif event_type == "CHART_REFRESH_TICK":
            from hermes.events.bus import ChartRefreshTick
            event_bus.emit(ChartRefreshTick())
        else:
            from hermes.db.events import deserialize_event
            try:
                event = deserialize_event(event_type, payload)
                if event:
                    event_bus.emit(event)
                else:
                    log.error("[IPC] Unknown event type %s", event_type)
            except Exception as exc:
                log.error("[IPC] Failed to deserialize event %s: %s", event_type, exc)
    elif action == IPC_ACTION_DRAIN_COMMANDS:
        log.info("[IPC] Received drain operator-commands signal reactively")
        from hermes.events.bus import DrainOperatorCommandsCommand
        event_bus.emit(DrainOperatorCommandsCommand())
    elif action == IPC_ACTION_TRIGGER_APPROVALS:
        log.info("[IPC] Received trigger approvals signal reactively")
        # This must actually send the approved action(s) to the broker, not
        # just refresh a cache: it's the whole reason the watcher fires this
        # signal right after an operator decision, instead of leaving the
        # trade to wait for the next slow heartbeat tick (tick_interval_s,
        # e.g. 300s).
        await engine.execute_approved_actions()
    elif action == IPC_ACTION_SYNC_SETTINGS:
        log.info("[IPC] Received sync settings signal reactively")
        await control_state.load_from_db(db, conf)
        from hermes.db.events import ModeChangedEvent
        event_bus.emit(ModeChangedEvent(mode=control_state.mode, updated_at=_utcnow_iso()))
    elif action == IPC_ACTION_TRIGGER_ML:
        log.info("[IPC] Received trigger ML signal reactively")
        try:
            await db.settings.set_setting("ml_force_run", "true")
        except Exception as e:
            log.warning("Failed to set ml_force_run setting reactively: %s", e)
        from hermes.events.bus import MlRetrainTick
        event_bus.emit(MlRetrainTick(force=True))
