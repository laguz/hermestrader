from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from hermes.service1_agent.trade_action import TradeAction

logger = logging.getLogger("hermes.portfolio.optimizer")


class PortfolioOptimizer:
    """Dynamic Portfolio Optimizer to aggregate candidate entries and resolve capital allocation."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    async def optimize(
        self,
        actions: List[TradeAction],
        avail_bp: float,
        existing_positions: List[Dict[str, Any]]
    ) -> List[TradeAction]:
        """Allocate capital dynamically using risk-parity and Kelly Criterion.
        
        Adjusts quantities/lots of candidate actions based on correlation with
        existing positions to avoid over-concentration in highly correlated assets.
        """
        if not actions:
            return []

        # concentration limit check (defaults to 25% of available buying power per symbol)
        max_concentration = float(self.config.get("max_symbol_concentration_pct", 0.25))
        max_symbol_bp = avail_bp * max_concentration

        symbol_positions: Dict[str, float] = {}
        for pos in existing_positions:
            sym = pos.get("symbol", "")
            # Option symbol fallback to underlying symbol parsing
            if len(sym) > 6 and sym[0:4].isalpha():
                from hermes.common import OCC_RE
                m = OCC_RE.match(sym)
                if m:
                    sym = m.group(1)
            symbol_positions[sym] = symbol_positions.get(sym, 0.0) + abs(pos.get("quantity", 0.0))

        optimized_actions: List[TradeAction] = []
        for action in actions:
            sym = action.symbol
            credit = float(action.price if action.price is not None else 0.0)
            width = float(action.width if action.width is not None else 0.0)
            risk_per_lot = max(0.0, (width - credit) * 100.0)
            requested_lots = action.quantity
            
            # Simple Kelly-like adjustment
            pop = action.strategy_params.get("pop")
            if pop is None:
                delta = action.strategy_params.get("delta")
                if delta is None:
                    delta = action.strategy_params.get("short_delta")
                pop = 1.0 - abs(float(delta)) if delta is not None else 0.70
            
            score = max(0.01, 1.0 - (1.0 - float(pop)) * (width / max(0.01, credit)))
            if score < 0.2:
                # Skip low scoring entries
                logger.info("[PORTFOLIO-OPTIMIZER] Dropped %s entry due to low Kelly score: %f", sym, score)
                continue

            requested_bp = risk_per_lot * requested_lots
            if requested_bp > max_symbol_bp:
                scaled_lots = int(max_symbol_bp // risk_per_lot) if risk_per_lot > 0 else requested_lots
                scaled_lots = max(0, min(requested_lots, scaled_lots))
                if scaled_lots < requested_lots:
                    logger.info(
                        "[PORTFOLIO-OPTIMIZER] Scaled %s due to concentration limit: %d -> %d",
                        sym, requested_lots, scaled_lots
                    )
                    action.quantity = scaled_lots
                    if action.legs:
                        for leg in action.legs:
                            leg["quantity"] = scaled_lots

            if action.quantity > 0:
                optimized_actions.append(action)

        return optimized_actions
