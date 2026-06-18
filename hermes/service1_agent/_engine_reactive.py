"""
[Service-1: Hermes-Agent-Core] — reactive market-data / order-fill handlers controller.

Split out of ``core.py`` to keep the engine's spine readable. ``ReactiveController``
is an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.reactive``); it operates on injected dependencies and handles events/commands.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Sequence

from hermes.events.bus import (
    MarketDataEvent,
    OrderFillEvent,
    ExecuteMarketDataCommand,
    ExecuteOrderFillCommand,
    ProcessReactiveEntriesEvent,
    SubmitTradeActionsCommand,
    EvaluateReactiveExitEvent,
    SyncPositionsCommand,
    ReconcileOrphansCommand,
    ProcessManagementCommand,
    ProcessEntriesCommand,
)
from ._engine_base import _EngineCollaborator

logger = logging.getLogger("hermes.agent.core")


class ReactiveController(_EngineCollaborator):
    def __init__(self, db, broker, event_bus, config, strategies=None, mm=None, quote_cache=None, clock=None) -> None:
        super().__init__(db, broker, event_bus, config, clock)
        self.strategies = strategies or []
        self.mm = mm
        self._quote_cache = quote_cache if quote_cache is not None else {}

        if self.event_bus is not None:
            self.event_bus.subscribe(ExecuteMarketDataCommand, self.handle_execute_market_data)
            self.event_bus.subscribe(ExecuteOrderFillCommand, self.handle_execute_order_fill)
            self.event_bus.subscribe(ProcessReactiveEntriesEvent, self.handle_process_reactive_entries)

    async def handle_execute_market_data(self, command: ExecuteMarketDataCommand) -> None:
        try:
            res = await self._handle_market_data_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_execute_order_fill(self, command: ExecuteOrderFillCommand) -> None:
        try:
            res = await self._handle_order_fill_internal(command.event)
            if command.future and not command.future.done():
                command.future.set_result(res)
        except Exception as exc:
            if command.future and not command.future.done():
                command.future.set_exception(exc)
            raise

    async def handle_process_reactive_entries(self, event: ProcessReactiveEntriesEvent) -> None:
        try:
            res = await self.process_reactive_entries(event.symbol)
            if event.future and not event.future.done():
                event.future.set_result(res)
        except Exception as exc:
            if event.future and not event.future.done():
                event.future.set_exception(exc)
            raise

    async def _read_banned_symbols(self) -> set[str]:
        if not self.db:
            return set()
        try:
            raw = await self.db.settings.get_setting("banned_symbols")
            if not raw:
                return set()
            return {s.strip().upper() for s in raw.split(",") if s.strip()}
        except Exception:
            logger.exception("[GOVERNANCE] Failed to read banned_symbols setting")
            return set()

    async def _watchlist_for(self, strategy_id: str, default: Sequence[str]) -> List[str]:
        getter = getattr(self.db.watchlist, "list_watchlist", None)
        if getter is None:
            return list(default)
        try:
            import inspect
            if inspect.iscoroutinefunction(getter):
                wl = await getter(strategy_id)
            else:
                wl = getter(strategy_id)
                if inspect.iscoroutine(wl):
                    wl = await wl
        except Exception as exc:
            logger.exception("watchlist read failed for %s: %s", strategy_id, exc)
            return list(default)
        return (wl or []) or list(default)

    async def _handle_market_data_internal(self, event: MarketDataEvent) -> None:
        """Evaluates strategies reactively when a new MarketDataEvent is received."""
        symbol = event.symbol
        
        old_price = None
        if symbol in self._quote_cache:
            old_price = self._quote_cache[symbol].get("price")

        self._quote_cache[symbol] = {
            "price": event.price,
            "volume": event.volume,
            **event.data
        }
        
        self.broker.update_cached_quote(symbol, {
            "symbol": symbol,
            "price": event.price,
            "volume": event.volume,
            **event.data
        })
        
        from hermes.market_hours import should_block_trades
        blocked, reason = should_block_trades()
        if blocked:
            return

        mgmt_actions = []
        for s in self.strategies:
            try:
                actions = await s.manage_positions()
                if actions:
                    symbol_actions = [a for a in actions if a.symbol == symbol]
                    mgmt_actions.extend(symbol_actions)
            except Exception as exc:
                logger.exception("Management failure in %s for %s: %s", s.NAME, symbol, exc)
                
        if mgmt_actions:
            cmd = SubmitTradeActionsCommand(actions=mgmt_actions, action_type="management")
            self.event_bus.emit(cmd)
            await cmd.future

        ev_exit = EvaluateReactiveExitEvent(symbol=symbol, mgmt_actions=mgmt_actions)
        self.event_bus.emit(ev_exit)
        await ev_exit.future

        if old_price is not None and old_price != event.price:
            try:
                analysis = await self.broker.analyze_symbol(symbol)
                key_levels = analysis.get("key_levels", [])
            except Exception as exc:
                logger.exception("Failed to analyze symbol %s on market data event: %s", symbol, exc)
                key_levels = []

            crossed = False
            new_price = event.price
            for lvl in key_levels:
                price_level = lvl.get("price")
                if price_level is not None:
                    if (old_price < price_level <= new_price) or (old_price > price_level >= new_price):
                        crossed = True
                        logger.info(
                            "[ENGINE] Price crossed support/resistance level %f for %s (old: %f, new: %f)",
                            price_level, symbol, old_price, new_price,
                        )
                        break

            if crossed:
                try:
                    ev_entries = ProcessReactiveEntriesEvent(symbol=symbol)
                    self.event_bus.emit(ev_entries)
                    await ev_entries.future
                except Exception as exc:
                    logger.exception("Failed to process reactive entries for %s: %s", symbol, exc)

    async def _handle_order_fill_internal(self, event: OrderFillEvent) -> None:
        """Reactively handles order fills by syncing positions and orders immediately."""
        logger.info(
            "[ENGINE] Order fill event received for order %s (%s %d shares/contracts of %s)",
            event.broker_order_id, event.side, event.quantity, event.symbol,
        )
        try:
            cmd = SyncPositionsCommand()
            self.event_bus.emit(cmd)
            await cmd.future
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync positions on order fill event: %s", exc)

        try:
            if self.mm is not None:
                await self.mm.sync_broker_orders()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync broker orders on order fill event: %s", exc)

        try:
            cmd = ReconcileOrphansCommand()
            self.event_bus.emit(cmd)
            await cmd.future
        except Exception as exc:
            logger.exception("[ENGINE] Failed to reconcile orphans on order fill event: %s", exc)

        try:
            cmd = ProcessManagementCommand()
            self.event_bus.emit(cmd)
            mgmt = await cmd.future
            if mgmt:
                cmd_submit = SubmitTradeActionsCommand(actions=mgmt, action_type="management")
                self.event_bus.emit(cmd_submit)
                await cmd_submit.future
                logger.info("[ENGINE] Reactively processed management post order fill: submitted %d actions", len(mgmt))
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process management on order fill event: %s", exc)

        try:
            watchlist = await self.db.watchlist.all_watchlist_symbols()
            if watchlist:
                banned = await self._read_banned_symbols()
                if banned:
                    watchlist = [s for s in watchlist if s.upper() not in banned]
                if watchlist:
                    cmd = ProcessEntriesCommand(watchlist=watchlist)
                    self.event_bus.emit(cmd)
                    num_entries = await cmd.future
                    logger.info("[ENGINE] Reactively processed entries post order fill: placed %d entries", num_entries)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process entries on order fill event: %s", exc)

    async def process_reactive_entries(self, symbol: str) -> None:
        """Executes entries reactively for a single symbol that crossed support/resistance."""
        async def _check_watchlist(s):
            wl = await self._watchlist_for(s.strategy_id, [symbol])
            return s if symbol in wl else None

        check_results = await asyncio.gather(*[_check_watchlist(s) for s in self.strategies])
        strategies_to_run = [s for s in check_results if s is not None]

        if not strategies_to_run:
            return

        max_per_tick = int(self.config.get("max_orders_per_tick", 5))

        async def _run_reactive_entries(s):
            try:
                return s, await s.execute_entries([symbol])
            except Exception as exc:
                logger.exception("Reactive entry proposal failure in %s for %s: %s", s.NAME, symbol, exc)
                return s, []

        results = await asyncio.gather(*[_run_reactive_entries(s) for s in strategies_to_run])

        if self.config.get("portfolio_optimization"):
            all_proposed_actions = []
            for s, actions in results:
                all_proposed_actions.extend(actions)

            if not all_proposed_actions:
                return

            avail_bp = await self.mm.true_available_bp()
            optimized_actions = await self.mm.optimize_allocation(all_proposed_actions, avail_bp)

            if len(optimized_actions) > max_per_tick:
                logger.warning(
                    "[ENGINE] Reactive optimized entries generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                    len(optimized_actions), max_per_tick, max_per_tick,
                )
                for a in optimized_actions[max_per_tick:]:
                    await self.db.logs.write_log(
                        a.strategy_id,
                        f"[GUARD] {a.symbol} reactive entry trimmed due to max_orders_per_tick={max_per_tick}"
                    )
                optimized_actions = optimized_actions[:max_per_tick]

            cmd = SubmitTradeActionsCommand(actions=optimized_actions, action_type="entry")
            self.event_bus.emit(cmd)
            await cmd.future
            if optimized_actions:
                await self.mm.sync_broker_orders()
        else:
            tick_submitted = 0
            for s, actions in results:
                try:
                    if tick_submitted >= max_per_tick:
                        logger.warning(
                            "[ENGINE] max_orders_per_tick=%d reached during reactive entries; skipping %s",
                            max_per_tick, s.NAME,
                        )
                        break

                    scaled_actions = []
                    for action in actions:
                        requested_lots = action.quantity
                        if action.order_class == "multileg" and action.legs:
                            requested_lots = action.legs[0].get("quantity", 1)

                        if requested_lots <= 0:
                            continue

                        strat_id = action.strategy_id.upper()
                        max_lots_map = {
                            "CS7": 1,
                            "CS75": 1,
                            "TT45": 1,
                            "WHEEL": 5,
                            "HERMESALPHA": 1,
                        }
                        config_key = f"{strat_id.lower()}_max_lots"
                        max_lots = int(self.config.get(config_key) or max_lots_map.get(strat_id, 1))

                        requirement_per_lot = 0.0
                        if strat_id == "WHEEL":
                            if action.strategy_params.get("side_type") == "put" and action.legs:
                                opt_symbol = action.legs[0].get("option_symbol")
                                if opt_symbol:
                                    from .money_manager import parse_occ_strike
                                    strike = parse_occ_strike(opt_symbol)
                                    if strike:
                                        requirement_per_lot = strike * 100.0
                        else:
                            if action.width:
                                requirement_per_lot = action.width * 100.0

                        scaled = await self.mm.scale_quantity(
                            requested_lots=requested_lots,
                            requirement_per_lot=requirement_per_lot,
                            symbol=action.symbol,
                            side=action.side,
                            strategy_id=action.strategy_id,
                            max_lots=max_lots,
                            expiry=action.expiry,
                        )

                        if scaled > 0:
                            action.quantity = scaled
                            for leg in action.legs:
                                leg["quantity"] = scaled
                            scaled_actions.append(action)

                    remaining = max_per_tick - tick_submitted
                    if len(scaled_actions) > remaining:
                        scaled_actions = scaled_actions[:remaining]

                    cmd = SubmitTradeActionsCommand(actions=scaled_actions, action_type="entry")
                    self.event_bus.emit(cmd)
                    await cmd.future
                    tick_submitted += len(scaled_actions)

                    if scaled_actions:
                        await self.mm.sync_broker_orders()
                except Exception as exc:
                    logger.exception("Reactive entry failure in %s for %s: %s", s.NAME, symbol, exc)
