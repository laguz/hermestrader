"""
[Service-1: Hermes-Agent-Core] — reactive market-data / order-fill handlers mixin for ``CascadingEngine``.

Split out of ``core.py`` to keep the engine's spine readable. These methods
run as part of :class:`~hermes.service1_agent.core.CascadingEngine` (composed
via inheritance); they reference engine state on ``self`` and are not meant to
be used standalone.
"""
from __future__ import annotations

import logging

from hermes.events.bus import MarketDataEvent, OrderFillEvent

logger = logging.getLogger("hermes.agent.core")


class EngineReactiveMixin:
    async def handle_market_data(self, event: MarketDataEvent) -> None:
        await self.publish_event("MARKET_DATA", {"event": event})

    async def _handle_market_data_internal(self, event: MarketDataEvent) -> None:
        """Evaluates strategies reactively when a new MarketDataEvent is received."""
        symbol = event.symbol
        
        # Get old price from cache if it exists
        old_price = None
        if symbol in self._quote_cache:
            old_price = self._quote_cache[symbol].get("price")

        # Update quote cache
        self._quote_cache[symbol] = {
            "price": event.price,
            "volume": event.volume,
            **event.data
        }
        
        # Update shared quote cache in broker wrapper to prevent outbound REST calls
        self.broker.update_cached_quote(symbol, {
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
        for s in self.strategies:
            try:
                actions = await s.manage_positions()
                if actions:
                    # Filter actions to only close positions for the ticking symbol
                    symbol_actions = [a for a in actions if a.symbol == symbol]
                    mgmt_actions.extend(symbol_actions)
            except Exception as exc:
                logger.exception("Management failure in %s for %s: %s", s.NAME, symbol, exc)
                
        if mgmt_actions:
            await self.submit(mgmt_actions, action_type="management")

        # Evaluate continuous exit policy reactively for trades containing this ticking option leg
        await self._maybe_evaluate_reactive_exit(symbol, mgmt_actions)

        # Check support/resistance crossing for entries
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
                    await self.process_reactive_entries(symbol)
                except Exception as exc:
                    logger.exception("Failed to process reactive entries for %s: %s", symbol, exc)

    async def handle_order_fill(self, event: OrderFillEvent) -> None:
        await self.publish_event("ORDER_FILL", {"event": event})

    async def _handle_order_fill_internal(self, event: OrderFillEvent) -> None:
        """Reactively handles order fills by syncing positions and orders immediately."""
        logger.info(
            "[ENGINE] Order fill event received for order %s (%s %d shares/contracts of %s)",
            event.broker_order_id, event.side, event.quantity, event.symbol,
        )
        try:
            await self.sync_positions()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync positions on order fill event: %s", exc)

        try:
            if self.mm is not None:
                await self.mm.sync_broker_orders()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to sync broker orders on order fill event: %s", exc)

        try:
            await self.reconcile_orphans()
        except Exception as exc:
            logger.exception("[ENGINE] Failed to reconcile orphans on order fill event: %s", exc)

        try:
            mgmt = await self.process_management()
            if mgmt:
                await self.submit(mgmt, action_type="management")
                logger.info("[ENGINE] Reactively processed management post order fill: submitted %d actions", len(mgmt))
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process management on order fill event: %s", exc)

        try:
            watchlist = await self.db.all_watchlist_symbols()
            if watchlist:
                banned = await self._read_banned_symbols()
                if banned:
                    watchlist = [s for s in watchlist if s.upper() not in banned]
                if watchlist:
                    num_entries = await self.process_entries(watchlist)
                    logger.info("[ENGINE] Reactively processed entries post order fill: placed %d entries", num_entries)
        except Exception as exc:
            logger.exception("[ENGINE] Failed to process entries on order fill event: %s", exc)

    async def process_reactive_entries(self, symbol: str) -> None:
        """Executes entries reactively for a single symbol that crossed support/resistance.
        Ensures the symbol is in each strategy's watchlist before executing.
        """
        strategies_to_run = []
        for s in self.strategies:
            wl = await self._watchlist_for(s.strategy_id, [symbol])
            if symbol in wl:
                strategies_to_run.append(s)

        if not strategies_to_run:
            return

        max_per_tick = int(self.config.get("max_orders_per_tick", 5))

        if self.config.get("portfolio_optimization"):
            # Gather proposed actions across matching strategies
            all_proposed_actions = []
            for s in strategies_to_run:
                try:
                    actions = await s.execute_entries([symbol])
                    all_proposed_actions.extend(actions)
                except Exception as exc:
                    logger.exception("Reactive entry proposal failure in %s for %s: %s", s.NAME, symbol, exc)

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
                    await self.db.write_log(
                        a.strategy_id,
                        f"[GUARD] {a.symbol} reactive entry trimmed due to max_orders_per_tick={max_per_tick}"
                    )
                optimized_actions = optimized_actions[:max_per_tick]

            await self.submit(optimized_actions, action_type="entry")
            if optimized_actions:
                await self.mm.sync_broker_orders()
        else:
            tick_submitted = 0
            for s in strategies_to_run:
                try:
                    if tick_submitted >= max_per_tick:
                        logger.warning(
                            "[ENGINE] max_orders_per_tick=%d reached during reactive entries; skipping %s",
                            max_per_tick, s.NAME,
                        )
                        break

                    actions = await s.execute_entries([symbol])
                    remaining = max_per_tick - tick_submitted
                    if len(actions) > remaining:
                        actions = actions[:remaining]

                    await self.submit(actions, action_type="entry")
                    tick_submitted += len(actions)

                    if actions:
                        await self.mm.sync_broker_orders()
                except Exception as exc:
                    logger.exception("Reactive entry failure in %s for %s: %s", s.NAME, symbol, exc)
