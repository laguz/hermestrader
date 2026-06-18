"""
[Service-1: Hermes-Agent-Core] — scheduled clock-tick / heartbeat controller.

Split out of ``core.py`` so the engine spine stays pure orchestration.
``ClockController`` owns the body of the slow heartbeat tick — the operational
guards that wrap the trading pipeline: circuit breaker, pause / kill-switch,
daily-loss limit, stale-order & approval cleanup, approved-action execution,
the market-hours gate, weekly chart-vision analysis, and live status writes.

It is an owned collaborator of
:class:`~hermes.service1_agent.core.CascadingEngine` (``engine.clock_ctrl``) and
reaches engine state through a typed ``self.engine`` back-reference. The actual
trading work is delegated back to ``engine._run_tick_internal`` (and the phase
methods ``engine.sync_positions`` / ``engine.reconcile_orphans``) so those remain
the single seams tests monkeypatch.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from hermes.events.bus import ClockTickEvent

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class ClockController:
    """Owns the slow heartbeat tick body (``CascadingEngine._handle_clock_tick_internal``)."""

    def __init__(self, engine: "CascadingEngine") -> None:
        self.engine = engine

    async def handle_clock_tick_internal(self, event: ClockTickEvent) -> None:
        engine = self.engine
        from hermes.service1_agent.agent_risk import enforce_daily_loss_limit
        from hermes.service1_agent.agent_approvals import _execute_approved_action
        from hermes.market_hours import market_session, next_open
        from datetime import datetime, timezone
        import time

        if not engine.control_state:
            logger.warning("[ENGINE] handle_clock_tick: control_state is not set on the engine.")
            return

        # 1. Circuit breaker check
        _CB_THRESHOLD = 5
        _CB_COOLDOWN_S = 300
        if engine._cb_fail_count >= _CB_THRESHOLD:
            if time.time() - engine._cb_tripped_at < _CB_COOLDOWN_S:
                logger.info("[CIRCUIT BREAKER] Cooling down, skipping clock tick.")
                return
            # Cooldown elapsed
            engine._cb_fail_count = 0
            engine._cb_tripped_at = 0.0
            logger.info("[CIRCUIT BREAKER] Cooldown elapsed — resuming ticks.")

        try:
            # 0. Backstop re-sync. Control state is normally updated by settings
            # events, but Postgres NOTIFY is fire-and-forget — a dropped one
            # could leave us trading on stale pause / kill-switch / lot state.
            # Re-hydrate from the DB on the slow clock cadence so a missed event
            # self-heals. Throttled by last_sync_ts so IPC-triggered ticks (which
            # already reloaded) don't re-read needlessly.
            from hermes.service1_agent.control_state import CONTROL_STATE_BACKSTOP_S
            _last = engine.control_state.last_sync_ts
            if _last is None or (
                datetime.now(timezone.utc) - _last
            ).total_seconds() >= CONTROL_STATE_BACKSTOP_S:
                try:
                    await engine.control_state.load_from_db(engine.db, engine.config)
                except Exception as exc:                          # noqa: BLE001
                    logger.warning("[ENGINE] control_state backstop reload failed: %s", exc)

            # 2. Pause check
            if engine.control_state.paused:
                logger.info("[ENGINE] heartbeat tick PAUSED mode=%s", engine.control_state.mode)
                await engine.db.logs.write_log("ENGINE", f"heartbeat tick PAUSED mode={engine.control_state.mode}")
                return

            # 3. Daily loss check
            from hermes.service1_agent.agent_risk import resolve_max_daily_loss
            _max_daily_loss = resolve_max_daily_loss(engine.control_state.max_daily_loss)
            if await enforce_daily_loss_limit(
                engine.db, _max_daily_loss,
                currently_paused=engine.control_state.paused, broker=engine.broker.broker,
            ):
                engine.control_state.paused = True
                return

            # 4. Clean stale pending orders & approvals
            try:
                expired = await engine.db.trades.expire_stale_pending_orders(engine.control_state.pending_order_ttl_s)
                if expired:
                    logger.info("Expired %d stale PENDING order(s)", expired)
                    await engine.db.logs.write_log("ENGINE", f"expired {expired} stale PENDING order(s)")
            except Exception as exc:
                logger.warning("expire_stale_pending_orders failed: %s", exc)

            try:
                expired_approvals = await engine.db.approvals.expire_stale_approvals()
                if expired_approvals:
                    logger.info("Auto-expired %d stale approval(s)", expired_approvals)
                    await engine.db.logs.write_log("ENGINE", f"auto-expired {expired_approvals} stale approval(s) past deadline")
            except Exception as exc:
                logger.warning("expire_stale_approvals failed: %s", exc)

            # 5. Execute approved actions
            try:
                approved_actions = await engine.db.approvals.fetch_approved_actions()
                for item in approved_actions:
                    await _execute_approved_action(item, broker=engine.broker.broker, db=engine.db)
            except Exception as exc:
                logger.warning("Executing approved actions failed: %s", exc)

            # 6. Heartbeat and Market-hours gate
            mkt = market_session()
            await engine.db.logs.write_log(
                "ENGINE",
                f"heartbeat tick start mode={engine.control_state.mode} market={mkt['session']} open={mkt['is_open']}"
            )

            if not mkt["trading_day"]:
                nxt = next_open()
                await engine.db.logs.write_log(
                    "ENGINE",
                    f"market CLOSED — next open {nxt.strftime('%Y-%m-%d %H:%M ET')} ({mkt['et_date']} is not a trading day)"
                )
                return

            # 7. Execute entries/management tick loop
            unique_syms = set()
            for syms in engine.control_state.watchlist.values():
                unique_syms.update(syms)
            current_watchlist = sorted(list(unique_syms | set(engine.config.get("watchlist", []))))

            if mkt["is_open"]:
                stats = await engine._run_tick_internal(current_watchlist)
            else:
                await engine.sync_positions()
                await engine.reconcile_orphans()
                stats = {"managed": 0, "entries": 0, "note": f"all submissions skipped ({mkt['session']})"}

            # 8. Chart analysis
            _CHART_ANALYSIS_KEY = "chart_analysis_last_run"
            _CHART_ANALYSIS_INTERVAL_DAYS = 7
            db_watchlist = sorted(list(set(current_watchlist)))
            if engine.overseer is not None and db_watchlist:
                _should_run_charts = False
                _age_days: float = 0.0
                try:
                    _recent_decisions = await engine.db.decisions.recent_ai_decisions(
                        strategy_id="CHART",
                        limit=max(len(db_watchlist) * 2, 20)
                    )
                    _analyzed_syms = {d["symbol"] for d in _recent_decisions}
                    _missing_analysis = any(s not in _analyzed_syms for s in db_watchlist)

                    if _missing_analysis:
                        _should_run_charts = True
                        logger.info("Forcing chart analysis: some symbols in watchlist are missing analysis.")
                    else:
                        _last_chart_ts_raw = await engine.db.settings.get_setting(_CHART_ANALYSIS_KEY)
                        if _last_chart_ts_raw:
                            def _parse_iso(s: Optional[str]) -> Optional[datetime]:
                                if not s:
                                    return None
                                try:
                                    normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
                                    datetime_fromisoformat = datetime.fromisoformat
                                    dt = datetime_fromisoformat(normalised)
                                    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                                except ValueError:
                                    return None
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
                except Exception:
                    _should_run_charts = True

                if _should_run_charts:
                    logger.info("Running chart vision analysis for %d symbols", len(db_watchlist))
                    try:
                        await engine.overseer.analyze_charts(db_watchlist)
                        await engine.db.settings.set_setting(_CHART_ANALYSIS_KEY, datetime.now(timezone.utc).isoformat())
                        await engine.db.logs.write_log(
                            "ENGINE",
                            f"chart vision: analysed {len(db_watchlist)} symbols (7-month daily bars, next run in 7 days)"
                        )
                    except Exception as _ca_exc:
                        logger.warning("analyze_charts failed: %s", _ca_exc)
                else:
                    _days_left = max(0.0, _CHART_ANALYSIS_INTERVAL_DAYS - _age_days)
                    logger.debug("Chart analysis throttled — next run in %.1f day(s)", _days_left)

            # 9. Update live status indicators
            await engine.db.settings.set_setting("tradier_last_ok_ts", datetime.now(timezone.utc).isoformat())
            await engine.db.settings.set_setting("tradier_last_error", "")
            await engine.db.settings.set_setting("market_session", mkt["session"])
            logger.info("tick complete: %s", stats)
            await engine.db.logs.write_log("ENGINE", f"heartbeat tick complete: {stats}")
            engine._cb_fail_count = 0

        except Exception as exc:
            engine._cb_fail_count += 1
            if engine._cb_fail_count >= _CB_THRESHOLD:
                engine._cb_tripped_at = time.time()
            logger.exception("tick failed: %s", exc)
            try:
                exc_str = str(exc)[:500]
                await engine.db.settings.set_setting("tradier_last_error", exc_str)
                await engine.db.logs.write_log("ENGINE", f"tick failed: {exc}", level="ERROR")
            except Exception:
                pass
