"""
[Service-1: Hermes-Agent-Core] — reactive market-data / order-fill handlers controller.

Split out of ``core.py`` to keep the engine's spine readable. ``ReactiveController``
is an owned collaborator of :class:`~hermes.service1_agent.core.CascadingEngine`
(``engine.reactive``); it shares the engine's hot tick state.
Not meant to be used standalone.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from hermes.events.bus import MarketDataEvent, OrderFillEvent
from ._engine_base import _EngineCollaborator

if TYPE_CHECKING:
    from .core import CascadingEngine

logger = logging.getLogger("hermes.agent.core")


class ReactiveController(_EngineCollaborator):
    async def handle_market_data(self, event: MarketDataEvent) -> None:
        await self.engine.publish_event("MARKET_DATA", {"event": event})

    async def _handle_market_data_internal(self, event: MarketDataEvent) -> None:
        """Evaluates strategies reactively when a new MarketDataEvent is received."""
        symbol = event.symbol
        
        # Get old price from cache if it exists
        old_price = None
        if symbol in self.engine._quote_cache:
            old_price = self.engine._quote_cache[symbol].get("price")

        # Update quote cache
        self.engine._quote_cache[symbol] = {
            "price": event.price,
            "volume": event.volume,
            **event.data
        }
        
        # Update shared quote cache in broker wrapper to prevent outbound REST calls
        self.engine.broker.update_cached_quote(symbol, {
            "symbol": symbol,
            "price": event.price,
            "volume": event.volume,
            **event.data
        })
        
        # Guard: off-hours block
        from hermes.market_hours import should_block_trades
        blocked, reason = should_block_trades()
        if blocked:
            return

        # Run position management for this symbol across all strategies
        mgmt_actions = []
        for s in self.engine.strategies:
            try:
                actions = await s.manage_positions()
                if actions:
                    # Filter actions to only close positions for the ticking symbol
                    symbol_actions = [a for a in actions if a.symbol == symbol]
                    mgmt_actions.extend(symbol_actions)
            except Exception as exc:
                logger.exception("Management failure in %s for %s: %s", s.NAME, symbol, exc)
                
        if mgmt_actions:
            await self.engine.submit(mgmt_actions, action_type="management")

        # Evaluate continuous exit policy reactively for trades containing this ticking option leg
        await self.engine.tuning._maybe_evaluate_reactive_exit(symbol, mgmt_actions)

        # Check support/resistance crossing for entries
        if old_price is not None and old_price != event.price:
            try:
                analysis = await self.engine.broker.analyze_symbol(symbol)
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
                    await self.engine.process_reactive_entries(symbol)
                except Exception as exc:
                    logger.exception("Failed to process reactive entries for %s: %s", symbol, exc)

    async def handle_order_fill(self, event: OrderFillEvent) -> None:
        await self.engine.publish_event("ORDER_FILL", {"event": event})

    async def _handle_order_fill_internal(self, event: OrderFillEvent) -> None:
        """Reactively handles order fills by syncing positions and orders immediately."""
        logger.info(
            "[ENGINE] Order fill event received for order %s (%s %d shares/contracts of %s)",
            event.broker_order_id, event.side, event.quantity, event.symbol,
        )
        try:
            await self.engine.sync_positions()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync positions on order fill event: %s", exc)

        try:
            if self.engine.mm is not None:
                await self.engine.mm.sync_broker_orders()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync broker orders on order fill event: %s", exc)

        try:
            await self.engine.reconcile_orphans()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to reconcile orphans on order fill event: %s", exc)

        try:
            mgmt = await self.engine.process_management()
            if mgmt:
                await self.engine.submit(mgmt, action_type="management")
                logger.info("[ENGINE] Reactively processed management post order fill: submitted %d actions", len(mgmt))
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process management on order fill event: %s", exc)

        try:
            watchlist = await self.engine.db.watchlist.all_watchlist_symbols()
            if watchlist:
                banned = await self.engine._read_banned_symbols()
                if banned:
                    watchlist = [s for s in watchlist if s.upper() not in banned]
                if watchlist:
                    num_entries = await self.engine.process_entries(watchlist)
                    logger.info("[ENGINE] Reactively processed entries post order fill: placed %d entries", num_entries)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process entries on order fill event: %s", exc)

    async def process_reactive_entries(self, symbol: str) -> None:
        """Executes entries reactively for a single symbol that crossed support/resistance.
        Ensures the symbol is in each strategy's watchlist before executing.
        """
        # Determine matching strategies concurrently
        async def _check_watchlist(s):
            wl = await self.engine._watchlist_for(s.strategy_id, [symbol])
            return s if symbol in wl else None

        check_results = await asyncio.gather(*[_check_watchlist(s) for s in self.engine.strategies])
        strategies_to_run = [s for s in check_results if s is not None]

        if not strategies_to_run:
            return

        max_per_tick = int(self.engine.config.get("max_orders_per_tick", 5))

        # Gather proposed actions concurrently
        async def _run_reactive_entries(s):
            try:
                return s, await s.execute_entries([symbol])
            except Exception as exc:
                logger.exception("Reactive entry proposal failure in %s for %s: %s", s.NAME, symbol, exc)
                return s, []

        results = await asyncio.gather(*[_run_reactive_entries(s) for s in strategies_to_run])

        if self.engine.config.get("portfolio_optimization"):
            all_proposed_actions = []
            for s, actions in results:
                all_proposed_actions.extend(actions)

            if not all_proposed_actions:
                return

            avail_bp = await self.engine.mm.true_available_bp()
            optimized_actions = await self.engine.mm.optimize_allocation(all_proposed_actions, avail_bp)

            if len(optimized_actions) > max_per_tick:
                logger.warning(
                    "[ENGINE] Reactive optimized entries generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                    len(optimized_actions), max_per_tick, max_per_tick,
                )
                for a in optimized_actions[max_per_tick:]:
                    await self.engine.db.logs.write_log(
                        a.strategy_id,
                        f"[GUARD] {a.symbol} reactive entry trimmed due to max_orders_per_tick={max_per_tick}"
                    )
                optimized_actions = optimized_actions[:max_per_tick]

            await self.engine.submit(optimized_actions, action_type="entry")
            if optimized_actions:
                await self.engine.mm.sync_broker_orders()
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

                    # Sequentially re-scale and check capacity for each proposed entry
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
                        max_lots = int(self.engine.config.get(config_key) or max_lots_map.get(strat_id, 1))

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

                        scaled = await self.engine.mm.scale_quantity(
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

                    await self.engine.submit(scaled_actions, action_type="entry")
                    tick_submitted += len(scaled_actions)

                    if scaled_actions:
                        await self.engine.mm.sync_broker_orders()
                except Exception as exc:
                    logger.exception("Reactive entry failure in %s for %s: %s", s.NAME, symbol, exc)
