"""
[Service-1: Hermes-Agent-Core]
MoneyManager — true available BP, dynamic scaling, side-aware sizing — and
IronCondorBuilder, which pairs two vertical spreads through the MoneyManager's
capacity contract.

Both sit one layer above the broker/order primitives
(:mod:`broker_wrapper`, :mod:`trade_action`) and below the strategies that
consume them.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from hermes.common import OCC_RE

from .broker_wrapper import AsyncBrokerWrapper
from .trade_action import TradeAction

logger = logging.getLogger("hermes.agent.money_manager")


def parse_occ_strike(symbol: str) -> Optional[float]:
    """Parse the strike price from an OCC option symbol."""
    m = OCC_RE.match(symbol or "")
    if not m:
        return None
    _, _, _, strike_str = m.groups()
    return float(strike_str) / 1000.0


_DEFAULT_MAX_LOTS = {
    "CS7": 1,
    "CS75": 1,
    "TT45": 1,
    "WHEEL": 5,
    "HERMESALPHA": 1,
}


def resolve_entry_sizing(action: TradeAction,
                         config: Dict[str, Any]) -> tuple[int, int, float]:
    """(requested_lots, max_lots, requirement_per_lot) for one entry action.

    Single source of the per-action sizing preamble used by both the tick-path
    RiskEngine and the reactive entry path — the falsy-zero ``max_lots`` bug
    had to be fixed once per copy when this logic lived in each. A
    ``{strategy}_max_lots`` config value of 0 must be honored, not replaced
    with the default. WHEEL puts are cash-secured, so their requirement is the
    short strike, not a spread width.
    """
    requested_lots = action.quantity
    if action.order_class == "multileg" and action.legs:
        requested_lots = action.legs[0].get("quantity", 1)

    strat_id = action.strategy_id.upper()
    _raw_max_lots = config.get(f"{strat_id.lower()}_max_lots")
    max_lots = (int(_raw_max_lots) if _raw_max_lots is not None
                else _DEFAULT_MAX_LOTS.get(strat_id, 1))

    requirement_per_lot = 0.0
    if strat_id == "WHEEL":
        if action.strategy_params.get("side_type") == "put" and action.legs:
            opt_symbol = action.legs[0].get("option_symbol")
            if opt_symbol:
                strike = parse_occ_strike(opt_symbol)
                if strike:
                    requirement_per_lot = strike * 100.0
    else:
        if action.width is not None:
            requirement_per_lot = action.width * 100.0

    return requested_lots, max_lots, requirement_per_lot


# ---------------------------------------------------------------------------
# MoneyManager — true available BP, dynamic scaling, side-aware sizing
# ---------------------------------------------------------------------------
class MoneyManager:
    """
    Implements the prompt's money-management contract:
      * True Available BP = full option_buying_power reported by the broker
      * Dynamic scaling when requirement > true available BP
      * Side-aware sizing: max_lots - (open_contracts + pending_orders) per (symbol, side)
    """

    def __init__(self, broker, db, config: Dict[str, Any]):
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        self.config = config or {}
        # In-memory cache of active broker-side orders, refreshed every tick.
        # Map: (strategy_id, symbol, side_type, expiry_iso) -> lots
        # expiry_iso is YYYY-MM-DD; an empty string is used when the OCC
        # symbol cannot be parsed so the entry is still countable globally.
        self._broker_order_counts: Dict[Tuple[str, str, str, str], int] = {}

    # OCC option symbol regex lives in hermes.common so DB-side parsing in
    # record_pending_order shares one definition with this matcher.
    _OCC_RE = OCC_RE

    async def sync_broker_orders(self) -> None:
        """Fetch all active orders from the broker and cache their counts.

        Delegates to the normalized active orders broker interface, cleanly
        encapsulating broker-specific fields/tags within the broker wrapper.
        """
        self._broker_order_counts = {}
        try:
            orders = await self.broker.get_normalized_active_orders()
        except Exception:
            logger.exception("[MM] Failed to fetch broker orders for sync")
            return

        for o in orders:
            key = (o["strategy_id"], o["symbol"], o["side_type"], o["expiry_iso"])
            self._broker_order_counts[key] = self._broker_order_counts.get(key, 0) + o["lots"]
            logger.debug("[MM] Sync found active broker order: %s %s %s %s lots=%d",
                         o["strategy_id"], o["symbol"], o["side_type"], o["expiry_iso"], o["lots"])

    async def true_available_bp(self) -> float:
        balances = await self.broker.get_account_balances() or {}
        available = max(0.0, float(balances.get("option_buying_power", 0.0)))
        
        try:
            reserve_val = await self.db.settings.get_setting("obp_reserve")
            if reserve_val:
                reserve = float(str(reserve_val).strip())
                available = max(0.0, available - reserve)
                logger.debug(
                    "[MM] true_available_bp: subtracted reserve=%.2f, net_obp=%.2f",
                    reserve, available
                )
        except Exception as e:
            logger.debug("[MM] Failed to fetch or parse obp_reserve: %s", e)

        logger.debug(
            "[MM] true_available_bp: obp=%.2f account_type=%s",
            available, balances.get("account_type"),
        )
        return available

    async def max_affordable_contracts(self, requirement_per_contract: float) -> int:
        if requirement_per_contract <= 0:
            return 0
        bp = await self.true_available_bp()
        return int(bp // requirement_per_contract)

    async def side_aware_capacity(
        self,
        strategy_id: str,
        symbol: str,
        side: str,
        max_lots: int,
        expiry: str,
    ) -> int:
        """max_lots - (open + pending + broker_active) for (strategy, symbol, side, expiry).

        ``max_lots`` is **always enforced per option chain** — filling 12
        lots in expiry X still leaves a fresh ``max_lots`` budget in
        expiry Y. The previous global (symbol-wide) mode was removed
        because every production strategy was already calling per-expiry,
        and the global fallback only ever showed up by accident — turning
        a chain-scoped cap into a symbol-wide one in tests.

        ``expiry`` MUST be a non-empty ISO ``YYYY-MM-DD`` string.
        Passing ``None`` or empty raises ``ValueError`` so accidental
        mis-calls fail loudly instead of silently summing across chains.
        """
        if not expiry:
            raise ValueError(
                "side_aware_capacity requires an expiry (YYYY-MM-DD); the "
                "global symbol-wide cap mode has been removed."
            )
        side = side.lower()
        symbol = symbol.upper()
        # 1. Check DB for filled contracts (status='OPEN') in this chain
        open_qty = await self.db.trades.count_open_contracts(strategy_id, symbol, side, expiry)
        # 2. Check DB for pending internal orders (pre-submission or approval queue) in this chain
        pending = await self.db.trades.count_pending_orders(strategy_id, symbol, side, expiry)
        # 3. Check cached broker-side active orders (resting limits) in this chain
        broker_qty = self._broker_order_counts.get(
            (strategy_id, symbol, side, expiry), 0)

        total_used = open_qty + pending + broker_qty
        remaining = max_lots - total_used

        if broker_qty > 0:
            logger.debug("[MM] side_aware_capacity %s %s %s exp=%s: open=%d pending=%d broker=%d total=%d max=%d",
                         strategy_id, symbol, side, expiry,
                         open_qty, pending, broker_qty, total_used, max_lots)

        return max(0, remaining)

    async def scale_quantity(
        self,
        requested_lots: int,
        requirement_per_lot: float,
        symbol: str,
        side: str,
        strategy_id: str,
        max_lots: int,
        expiry: str,
    ) -> int:
        """Apply BP cap and per-expiry side capacity; never exceed requested.

        ``expiry`` is required — capacity is always enforced per option
        chain. See ``side_aware_capacity`` for the rationale.
        """
        if not expiry:
            raise ValueError(
                "scale_quantity requires an expiry (YYYY-MM-DD); capacity "
                "is always enforced per option chain."
            )
        if self.config.get("portfolio_optimization"):
            bp_cap = 999_999
        elif requirement_per_lot <= 0.0:
            bp_cap = 999_999
        else:
            bp_cap = await self.max_affordable_contracts(requirement_per_lot)
        side_cap = await self.side_aware_capacity(strategy_id, symbol, side, max_lots, expiry)
        scaled = min(requested_lots, bp_cap, side_cap)
        if scaled == 0 and requested_lots > 0:
            # Write a DB-visible log so the C2 live feed shows the block reason.
            if side_cap == 0:
                reason = (f"at capacity exp={expiry} "
                          f"(open+pending={max_lots}/{max_lots})")
            elif bp_cap == 0:
                balances = await self.broker.get_account_balances() or {}
                avail = max(0.0, float(balances.get("option_buying_power", 0.0)))
                acct_type = balances.get("account_type", "?")
                reason = (
                    f"insufficient BP (avail=${avail:,.0f} "
                    f"need=${requirement_per_lot:,.0f}/lot acct_type={acct_type})"
                )
            else:
                reason = f"bp_cap={bp_cap} side_cap={side_cap}"
            await self.db.logs.write_log(
                strategy_id,
                f"[MM] BLOCKED {symbol} {side.upper()}: {reason} — 0 lots available",
            )
        elif scaled < requested_lots:
            logger.info(
                "[MM] Scaled %s/%s %s %d→%d (bp_cap=%d side_cap=%d)",
                strategy_id, symbol, side, requested_lots, scaled, bp_cap, side_cap,
            )
            await self.db.logs.write_log(
                strategy_id,
                f"[MM] Scaled {symbol} {side.upper()} {requested_lots}→{scaled} lots "
                f"(bp_cap={bp_cap} side_cap={side_cap})",
            )
        return max(0, scaled)

    async def optimize_allocation(self, actions: List[TradeAction], avail_bp: float) -> List[TradeAction]:
        """Globally allocate lots across all proposed entries using fractional Kelly allocation.

        Formula:
          Score_i = max(0.01, 1.0 - (1.0 - POP_i) * (width_i / credit_i))
        """
        if not actions:
            return []

        kelly_fraction = self.config.get("kelly_fraction", 0.5)
        
        # 1. Calculate parameters and scores for all actions
        actions_with_info = []
        free_actions = []

        for action in actions:
            # Determine requested lots
            requested_lots = action.quantity
            if action.order_class == "multileg" and action.legs:
                requested_lots = action.legs[0].get("quantity", 1)

            if requested_lots <= 0:
                continue

            # POP
            pop = action.strategy_params.get("pop")
            if pop is None:
                delta_val = action.strategy_params.get("delta")
                if delta_val is None:
                    delta_val = action.strategy_params.get("short_delta")
                delta = delta_val
                if delta is not None:
                    pop = 1.0 - abs(float(delta))
                else:
                    strat = (action.strategy_id or "").upper()
                    if "CS75" in strat:
                        pop = 0.75
                    elif "CS7" in strat:
                        pop = 0.75
                    elif "TT45" in strat:
                        pop = 0.84
                    elif "WHEEL" in strat:
                        pop = 0.60
                    else:
                        pop = 0.70
            pop = float(pop)

            # Credit (price per lot)
            credit = float(action.price if action.price is not None else 0.01)

            # Width & Margin per lot
            width = action.width
            margin_per_lot = 0.0

            if action.order_class == "multileg":
                if width is None:
                    # Parse width from legs if missing
                    strikes = []
                    for leg in action.legs:
                        opt_sym = leg.get("option_symbol")
                        if opt_sym:
                            strike = parse_occ_strike(opt_sym)
                            if strike is not None:
                                strikes.append(strike)
                    if len(strikes) >= 2:
                        width = abs(strikes[0] - strikes[1])
                    else:
                        # Fallback default width. "CS7" is a substring of
                        # "CS75", so the wider strategy must match first.
                        strat = (action.strategy_id or "").upper()
                        if "CS75" in strat:
                            width = 5.0
                        elif "CS7" in strat:
                            width = 1.0
                        else:
                            width = 5.0
                margin_per_lot = width * 100.0
            elif action.order_class == "option":
                # Single option. Determine if PUT or CALL from legs or OCC
                is_put = False
                strike = None
                if action.legs:
                    opt_sym = action.legs[0].get("option_symbol")
                    if opt_sym:
                        strike = parse_occ_strike(opt_sym)
                        m = OCC_RE.match(opt_sym)
                        if m:
                            _, _, pc, _ = m.groups()
                            is_put = (pc == "P")
                if not is_put:
                    side_type = action.strategy_params.get("side_type")
                    if side_type:
                        is_put = (side_type.lower() == "put")
                
                if is_put:
                    if strike is None:
                        strike = 100.0
                    width = strike
                    margin_per_lot = strike * 100.0
                else:
                    width = 0.0
                    margin_per_lot = 0.0
            elif action.order_class == "equity":
                margin_per_lot = credit * 100.0
                width = credit

            # If margin per lot is 0, it doesn't consume BP. Allocate full requested.
            if margin_per_lot <= 0.0:
                free_actions.append((action, requested_lots))
                continue

            # Compute Kelly Score
            if credit <= 0:
                score = 0.01
            else:
                score = max(0.01, 1.0 - (1.0 - pop) * (width / credit))

            actions_with_info.append((action, score, margin_per_lot, pop, credit, requested_lots))

        # 2. Sort actions by score, credit, pop (descending)
        sorted_actions = sorted(actions_with_info, key=lambda item: (-item[1], -item[4], -item[3]))

        # 3. Sequentially allocate lots based on remaining BP and Kelly targets
        remaining_bp = avail_bp
        allocated_actions = []

        # Add all free actions first
        for action, requested_lots in free_actions:
            action.quantity = requested_lots
            for leg in action.legs:
                leg["quantity"] = requested_lots
            allocated_actions.append(action)

        for action, score, margin_per_lot, _pop, credit, requested_lots in sorted_actions:
            # Fractional Kelly target margin = kelly_fraction * score * avail_bp
            target_margin = kelly_fraction * score * avail_bp
            target_lots = int(round(target_margin, 2) // margin_per_lot)

            # Clamp by requested_lots and remaining BP
            allocated_lots = min(requested_lots, target_lots)
            max_affordable = int(round(remaining_bp, 2) // margin_per_lot)
            allocated_lots = min(allocated_lots, max_affordable)

            if allocated_lots > 0:
                action.quantity = allocated_lots
                for leg in action.legs:
                    leg["quantity"] = allocated_lots
                allocated_actions.append(action)
                remaining_bp -= allocated_lots * margin_per_lot
                logger.info(
                    "[MM-OPT] Allocated %d lots to %s (%s) — score=%.4f margin/lot=%.2f credit=%.2f remaining_bp=%.2f",
                    allocated_lots, action.symbol, action.strategy_id, score, margin_per_lot, credit, remaining_bp
                )
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[MM-OPT] Allocated {allocated_lots} lots to {action.symbol} (requested {requested_lots}) "
                    f"via Kelly Optimizer (score={score:.2f}, margin/lot=${margin_per_lot:,.0f})"
                )
            else:
                reason = "insufficient remaining BP" if max_affordable == 0 else f"Kelly score sizing target lots was 0 (target_margin={target_margin:.2f})"
                logger.info(
                    "[MM-OPT] Skipped %s (%s) — %s (score=%.4f target_lots=%d requested=%d remaining_bp=%.2f)",
                    action.symbol, action.strategy_id, reason, score, target_lots, requested_lots, remaining_bp
                )
                await self.db.logs.write_log(
                    action.strategy_id,
                    f"[MM-OPT] BLOCKED {action.symbol} entry: {reason} — 0 lots allocated"
                )

        return allocated_actions




# ---------------------------------------------------------------------------
# IronCondorBuilder — capital-efficient pairing of two vertical spreads
# ---------------------------------------------------------------------------
class IronCondorBuilder:
    """
    Building blocks are vertical spreads. For a target lot size N the builder
    attempts BOTH N put-spreads and N call-spreads on the same expiry. Margin is
    the single riskiest side (since both sides cannot be ITM simultaneously).

    Two modes:
      * Mode A (Initial): no open side — try to open both sides at once.
      * Mode B (Completion): one side already open on an expiry — add the missing
        side to convert the spread into an Iron Condor.
    """

    def __init__(self, money_manager: MoneyManager):
        self.mm = money_manager

    async def plan(
        self,
        *,
        strategy_id: str,
        symbol: str,
        expiry: str,
        target_lots: int,
        width: float,
        max_lots: int,
        existing_sides: Sequence[str],
        put_action_factory,
        call_action_factory,
        multiplier: int = 100,
    ) -> List[TradeAction]:
        """
        Returns a list of TradeAction(s) to open. May be empty if BP/caps prevent it.
        existing_sides: sides already open on this expiry, e.g. {'put'} or {} or {'put','call'}.
        multiplier: contract size read from the option chain (default 100 for standard equity options).
        """
        existing = {s.lower() for s in existing_sides}
        if {"put", "call"}.issubset(existing):
            # Both sides already open — nothing to do, log so operator can see.
            await self.mm.db.logs.write_log(
                strategy_id,
                f"[IC] {symbol} {expiry}: full IC already open on both sides; skip",
            )
            return []

        sides_to_open: List[str] = []
        if not existing:                          # Mode A
            sides_to_open = ["put", "call"]
        else:                                     # Mode B
            sides_to_open = ["call"] if "put" in existing else ["put"]

        # Single-sided margin governs BP — calculate once on the riskiest side.
        # Use the chain's multiplier rather than the hardcoded 100 so micro
        # options (multiplier=10) and other non-standard contracts are handled.
        requirement_per_lot = width * float(multiplier)
        if existing:
            # Mode B: The margin requirement is already covered by the existing side
            requirement_per_lot = 0.0

        actions: List[TradeAction] = []
        for side in sides_to_open:
            lots = await self.mm.scale_quantity(
                requested_lots=target_lots,
                requirement_per_lot=requirement_per_lot,
                symbol=symbol,
                side=side,
                strategy_id=strategy_id,
                max_lots=max_lots,
                expiry=expiry,
            )
            if lots < 1:
                # scale_quantity already wrote a BLOCKED log; nothing more needed.
                continue
            factory = put_action_factory if side == "put" else call_action_factory
            if asyncio.iscoroutinefunction(factory):
                action = await factory(symbol=symbol, expiry=expiry, lots=lots, width=width)
            else:
                action = factory(symbol=symbol, expiry=expiry, lots=lots, width=width)
            if action is not None:
                actions.append(action)
        return actions
