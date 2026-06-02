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
from typing import Any, Dict, List, Sequence, Tuple

from hermes.common import OCC_RE

from .broker_wrapper import AsyncBrokerWrapper
from .trade_action import TradeAction

logger = logging.getLogger("hermes.agent.money_manager")


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

        Hermes-authored orders carry a tag like ``HERMES_CS75`` that Tradier's
        order endpoint sanitises to ``HERMES-CS75`` (only [A-Za-z0-9-] is
        permitted). We accept either form so the matcher survives the
        sanitisation round-trip.
        """
        self._broker_order_counts = {}
        try:
            orders = await self.broker.get_orders() or []
            if not isinstance(orders, list):
                logger.warning("[MM] get_orders returned non-list: %r", orders)
                orders = []
        except Exception:
            logger.exception("[MM] Failed to fetch broker orders for sync")
            return

        active_statuses = {"open", "partially_filled", "pending", "calculated", "accepted"}
        for o in orders:
            status = str(o.get("status", "")).lower()
            if status not in active_statuses:
                continue

            tag = str(o.get("tag", "") or "")
            # Tradier's tag sanitiser converts '_' to '-' so 'HERMES_CS75'
            # arrives back as 'HERMES-CS75'. Normalise to hyphens for matching.
            normalised_tag = tag.replace("_", "-")
            if not normalised_tag.startswith("HERMES-"):
                continue
            strategy_id = normalised_tag[len("HERMES-"):].split("-", 1)[0]
            if not strategy_id:
                continue
            symbol = str(o.get("symbol", "")).upper()

            # Multileg orders return their legs under "leg"; single-leg option
            # orders carry option_symbol at the top level (no "leg" array).
            legs = o.get("leg") or []
            if isinstance(legs, dict):
                legs = [legs]
            if not legs:
                top_opt = o.get("option_symbol")
                if top_opt:
                    legs = [{"option_symbol": top_opt,
                             "quantity": o.get("quantity", 1)}]

            lots = int(o.get("quantity", 1) or 1)
            side_type = "unknown"
            expiry_iso = ""
            for leg in legs:
                occ_sym = str(leg.get("option_symbol", "") or "")
                m = self._OCC_RE.match(occ_sym)
                if not m:
                    continue
                side_type = "put" if m.group(3) == "P" else "call"
                # OCC expiry is YYMMDD in group 2 → normalise to YYYY-MM-DD
                yymmdd = m.group(2)
                expiry_iso = f"20{yymmdd[0:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"
                break

            if side_type != "unknown":
                key = (strategy_id, symbol, side_type, expiry_iso)
                self._broker_order_counts[key] = self._broker_order_counts.get(key, 0) + lots
                logger.debug("[MM] Sync found active broker order: %s %s %s %s lots=%d",
                             strategy_id, symbol, side_type, expiry_iso, lots)

    async def true_available_bp(self) -> float:
        balances = await self.broker.get_account_balances() or {}
        available = max(0.0, float(balances.get("option_buying_power", 0.0)))
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
        open_qty = await self.db.count_open_contracts(strategy_id, symbol, side, expiry)
        # 2. Check DB for pending internal orders (pre-submission or approval queue) in this chain
        pending = await self.db.count_pending_orders(strategy_id, symbol, side, expiry)
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
        if requirement_per_lot <= 0.0:
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
            await self.db.write_log(
                strategy_id,
                f"[MM] BLOCKED {symbol} {side.upper()}: {reason} — 0 lots available",
            )
        elif scaled < requested_lots:
            logger.info(
                "[MM] Scaled %s/%s %s %d→%d (bp_cap=%d side_cap=%d)",
                strategy_id, symbol, side, requested_lots, scaled, bp_cap, side_cap,
            )
            await self.db.write_log(
                strategy_id,
                f"[MM] Scaled {symbol} {side.upper()} {requested_lots}→{scaled} lots "
                f"(bp_cap={bp_cap} side_cap={side_cap})",
            )
        return max(0, scaled)


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

    @staticmethod
    def margin_requirement(width: float, lots: int, multiplier: int = 100) -> float:
        """Single-side margin for an iron condor on equal-width spreads.

        `multiplier` is the contract size from the option chain (standard
        equity options = 100; micro options may differ).  Defaults to 100 so
        existing callers that don't pass it continue to work correctly.
        """
        return float(width) * int(multiplier) * int(lots)

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
            await self.mm.db.write_log(
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
