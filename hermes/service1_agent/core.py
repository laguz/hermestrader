"""
[Service-1: Hermes-Agent-Core]
Core abstractions: TradeAction, AbstractStrategy, IronCondorBuilder,
MoneyManager and the CascadingEngine that drives execution priority.
"""
from __future__ import annotations

import dataclasses
import logging
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger("hermes.agent.core")


# ---------------------------------------------------------------------------
# TradeAction — single canonical order envelope used by every strategy
# ---------------------------------------------------------------------------
@dataclass
class TradeAction:
    """Order routing envelope. Strategies build these; TradeManager submits them."""
    strategy_id: str
    symbol: str
    order_class: str                       # 'multileg' | 'equity' | 'option'
    legs: List[Dict[str, Any]]             # [{'option_symbol','side','quantity'}, ...]
    price: Optional[float]                 # net credit (sell) or debit (buy)
    side: str                              # 'sell' | 'buy'
    quantity: int = 1                      # overall order qty (legs carry per-leg qty)
    duration: str = "day"
    order_type: str = "credit"             # 'credit' | 'debit' | 'limit' | 'market'
    tag: Optional[str] = None
    strategy_params: Dict[str, Any] = field(default_factory=dict)
    dte: Optional[int] = None
    expiry: Optional[str] = None
    width: Optional[float] = None
    # AI override metadata — set when HermesOverseer authored or modified the action
    ai_authored: bool = False
    ai_rationale: Optional[str] = None


# ---------------------------------------------------------------------------
# MoneyManager — true available BP, dynamic scaling, side-aware sizing
# ---------------------------------------------------------------------------
class MoneyManager:
    """
    Implements the prompt's money-management contract:
      * True Available BP = OBP - min_obp_reserve
      * Dynamic scaling when requirement > true available BP
      * Side-aware sizing: max_lots - (open_contracts + pending_orders) per (symbol, side)
    """

    def __init__(self, broker, db, config: Dict[str, Any]):
        self.broker = broker
        self.db = db
        self.config = config or {}

    def true_available_bp(self) -> float:
        balances = self.broker.get_account_balances() or {}
        obp = float(balances.get("option_buying_power", 0.0))
        reserve = float(self.config.get("min_obp_reserve", 0.0))
        available = max(0.0, obp - reserve)
        logger.debug(
            "[MM] true_available_bp: obp=%.2f reserve=%.2f available=%.2f account_type=%s",
            obp, reserve, available, balances.get("account_type"),
        )
        return available

    def max_affordable_contracts(self, requirement_per_contract: float) -> int:
        if requirement_per_contract <= 0:
            return 0
        bp = self.true_available_bp()
        return int(bp // requirement_per_contract)

    def side_aware_capacity(
        self,
        strategy_id: str,
        symbol: str,
        side: str,
        max_lots: int,
    ) -> int:
        """max_lots - (open_contracts + pending_orders) for (symbol, side)."""
        side = side.lower()
        open_qty = self.db.count_open_contracts(strategy_id, symbol, side)
        pending = self.db.count_pending_orders(strategy_id, symbol, side)
        remaining = max_lots - (open_qty + pending)
        return max(0, remaining)

    def scale_quantity(
        self,
        requested_lots: int,
        requirement_per_lot: float,
        symbol: str,
        side: str,
        strategy_id: str,
        max_lots: int,
    ) -> int:
        """Apply BP cap and side-aware capacity; never exceed requested."""
        bp_cap = self.max_affordable_contracts(requirement_per_lot)
        side_cap = self.side_aware_capacity(strategy_id, symbol, side, max_lots)
        scaled = min(requested_lots, bp_cap, side_cap)
        if scaled == 0 and requested_lots > 0:
            # Write a DB-visible log so the C2 live feed shows the block reason.
            if side_cap == 0:
                reason = f"at capacity (open+pending={max_lots}/{max_lots})"
            elif bp_cap == 0:
                balances = self.broker.get_account_balances() or {}
                raw_obp = float(balances.get("option_buying_power", 0.0))
                reserve = float(self.config.get("min_obp_reserve", 0.0))
                avail = max(0.0, raw_obp - reserve)
                acct_type = balances.get("account_type", "?")
                reason = (
                    f"insufficient BP (raw_obp=${raw_obp:,.0f} reserve=${reserve:,.0f} "
                    f"avail=${avail:,.0f} need=${requirement_per_lot:,.0f}/lot "
                    f"acct_type={acct_type})"
                )
            else:
                reason = f"bp_cap={bp_cap} side_cap={side_cap}"
            self.db.write_log(
                strategy_id,
                f"[MM] BLOCKED {symbol} {side.upper()}: {reason} — 0 lots available",
            )
        elif scaled < requested_lots:
            logger.info(
                "[MM] Scaled %s/%s %s %d→%d (bp_cap=%d side_cap=%d)",
                strategy_id, symbol, side, requested_lots, scaled, bp_cap, side_cap,
            )
            self.db.write_log(
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

    def plan(
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
            self.mm.db.write_log(
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
        actions: List[TradeAction] = []
        for side in sides_to_open:
            lots = self.mm.scale_quantity(
                requested_lots=target_lots,
                requirement_per_lot=requirement_per_lot,
                symbol=symbol,
                side=side,
                strategy_id=strategy_id,
                max_lots=max_lots,
            )
            if lots < 1:
                # scale_quantity already wrote a BLOCKED log; nothing more needed.
                continue
            factory = put_action_factory if side == "put" else call_action_factory
            action = factory(symbol=symbol, expiry=expiry, lots=lots, width=width)
            if action is not None:
                actions.append(action)
        return actions


# ---------------------------------------------------------------------------
# AbstractStrategy — base class for every cascading strategy
# ---------------------------------------------------------------------------
class AbstractStrategy(ABC):
    PRIORITY: int = 99
    NAME: str = "ABSTRACT"

    def __init__(
        self,
        broker,
        db,
        money_manager: MoneyManager,
        ic_builder: IronCondorBuilder,
        config: Dict[str, Any],
        dry_run: bool = False,
        overseer: Optional["HermesOverseer"] = None,
    ):
        self.broker = broker
        self.db = db
        self.mm = money_manager
        self.ic = ic_builder
        self.config = config or {}
        self.dry_run = dry_run
        self.overseer = overseer
        self.strategy_id = self.NAME
        self.execution_logs: List[str] = []

    # ---- shared helpers ----------------------------------------------------
    def now(self) -> datetime:
        if hasattr(self.broker, "current_date") and self.broker.current_date:
            return self.broker.current_date
        return datetime.utcnow()

    def today(self) -> date:
        return self.now().date()

    def _log(self, msg: str) -> None:
        ts = self.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} [{self.NAME}] {msg}"
        self.execution_logs.append(line)
        logger.info(line)
        self.db.write_log(self.strategy_id, msg)

    def find_expiry_in_dte_range(self, symbol: str, min_dte: int, max_dte: int,
                                 prefer: str = "max") -> Optional[str]:
        expirations = self.broker.get_option_expirations(symbol) or []
        today = self.today()
        candidates: List[date] = []
        for e in expirations:
            d = e if isinstance(e, date) else datetime.strptime(str(e), "%Y-%m-%d").date()
            dte = (d - today).days
            if min_dte <= dte <= max_dte:
                candidates.append(d)
        if not candidates:
            return None
        chosen = max(candidates) if prefer == "max" else min(candidates)
        return chosen.strftime("%Y-%m-%d")

    def find_active_ic_expiry(self, symbol: str) -> Optional[str]:
        """Return the expiry of an incomplete Iron Condor for this strategy and symbol."""
        open_legs = self.db.open_legs(self.strategy_id, symbol)
        expiry_sides = {}
        for leg in open_legs:
            exp = leg.get("expiry")
            if exp:
                expiry_sides.setdefault(exp, set()).add(leg.get("side", "").lower())
                
        # If any expiry has exactly 1 side (put OR call, but not both), return it.
        # This prioritizes completing an IC over starting a new one.
        for exp, sides in expiry_sides.items():
            if len(sides) == 1:
                return exp
        return None

    def find_strike_by_delta(self, chain, option_type: str, target_delta: float,
                             tolerance: float = 0.05) -> Optional[Dict[str, Any]]:
        best, best_diff = None, math.inf
        for o in chain:
            if o.get("option_type") != option_type:
                continue
            # Tradier returns greeks=null for deep OTM / illiquid options;
            # guard with `or {}` so we treat missing greeks as delta=0.0
            greeks = o.get("greeks") or {}
            raw_delta = greeks.get("delta")
            if raw_delta is None:
                continue          # skip options with no greek data at all
            d = abs(float(raw_delta))
            diff = abs(d - target_delta)
            if diff < best_diff and diff <= tolerance:
                best_diff, best = diff, o
        return best

    def short_credit(self, short_leg, long_leg) -> float:
        sm = (float(short_leg["bid"]) + float(short_leg["ask"])) / 2.0
        lm = (float(long_leg["bid"]) + float(long_leg["ask"])) / 2.0
        return round(sm - lm, 2)

    # ---- API expected by the cascading engine ------------------------------
    @abstractmethod
    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]: ...

    @abstractmethod
    def manage_positions(self) -> List[TradeAction]: ...


# ---------------------------------------------------------------------------
# CascadingEngine — top-level orchestrator
# ---------------------------------------------------------------------------
class CascadingEngine:
    """
    Pipeline order (per spec):
        1. Sync positions (broker → DB)
        2. Reconcile orphans
        3. Process exits / management for every strategy
        4. Execute entries in priority order: CS75 → CS7 → TastyTrade45 → Wheel
           — fully draining the watchlist for one strategy before moving on.
    """

    def __init__(self, broker, db, strategies: Sequence[AbstractStrategy],
                 overseer: Optional["HermesOverseer"] = None,
                 approval_mode: bool = False):
        self.broker = broker
        self.db = db
        # Sort by declared PRIORITY (1 highest)
        self.strategies = sorted(strategies, key=lambda s: s.PRIORITY)
        self.overseer = overseer
        # When True, submit() queues trades for human approval instead of
        # sending them to the broker directly.
        self.approval_mode = approval_mode

    # 1
    def sync_positions(self) -> None:
        positions = self.broker.get_positions() or []
        self.db.upsert_positions(positions)

    # 2
    def reconcile_orphans(self) -> None:
        """Flag broker positions not tied to any strategy as MANUAL_ORPHAN."""
        tracked = self.db.tracked_option_symbols()
        live = {p["symbol"] for p in self.broker.get_positions() or []}
        orphans = live - tracked
        if orphans:
            self.db.flag_orphans(orphans)

    # 3
    def process_management(self) -> List[TradeAction]:
        actions: List[TradeAction] = []
        for s in self.strategies:
            try:
                actions.extend(s.manage_positions())
            except Exception as exc:                     # noqa: BLE001
                logger.exception("Management failure in %s: %s", s.NAME, exc)
        return actions

    # 4
    def _watchlist_for(self, strategy_id: str, default: Sequence[str]) -> List[str]:
        """Per-strategy watchlist with fallback to the engine-level default."""
        getter = getattr(self.db, "list_watchlist", None)
        if getter is None:
            return list(default)
        try:
            wl = getter(strategy_id) or []
        except Exception as exc:                          # noqa: BLE001
            logger.exception("watchlist read failed for %s: %s", strategy_id, exc)
            return list(default)
        return wl or list(default)

    def process_entries(self, watchlist: Sequence[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        for s in self.strategies:
            try:
                wl = self._watchlist_for(s.strategy_id, watchlist)
                # Drain entire watchlist for THIS strategy before moving on.
                actions.extend(s.execute_entries(wl))
            except Exception as exc:                     # noqa: BLE001
                logger.exception("Entry failure in %s: %s", s.NAME, exc)
        return actions

    def submit(self, actions: Iterable[TradeAction],
               action_type: str = "entry") -> None:
        for a in actions:
            # AI override hook — overseer may VETO, MODIFY, or APPROVE the action.
            if self.overseer is not None:
                a = self.overseer.review(a)
                if a is None:
                    continue

            if self.approval_mode:
                # Dedup guard: never re-queue a trade that already has a PENDING
                # approval for the same (strategy, symbol, side, expiry).
                # Without this, every tick re-generates and re-queues the same
                # spread because the approval hasn't been actioned yet.
                side_type = (a.strategy_params or {}).get("side_type")
                if self.db.has_pending_approval(a.strategy_id, a.symbol,
                                                side_type, a.expiry):
                    logger.info(
                        "[C2] Skipping duplicate — already PENDING: %s %s "
                        "side=%s expiry=%s",
                        a.strategy_id, a.symbol, side_type, a.expiry,
                    )
                    self.db.write_log(
                        a.strategy_id,
                        f"[DEDUP] {a.symbol} {side_type} expiry={a.expiry} "
                        f"already PENDING approval — skipped",
                    )
                    continue

                # Queue for human review instead of firing directly.
                action_dict = dataclasses.asdict(a)
                self.db.queue_for_approval(action_dict, action_type=action_type)
                logger.info(
                    "[C2] Trade queued for approval: %s %s strategy=%s side=%s expiry=%s",
                    a.symbol, a.order_class, a.strategy_id, side_type, a.expiry,
                )
                self.db.write_log(
                    a.strategy_id,
                    f"[APPROVAL REQUIRED] {a.symbol} {a.order_class} "
                    f"side={side_type} expiry={a.expiry} "
                    f"qty={a.quantity} — awaiting human approval",
                )
            else:
                self.db.record_pending_order(a)
                if not getattr(self.broker, "dry_run", False):
                    resp = self.broker.place_order_from_action(a)
                    self.db.record_order_response(a, resp)

    # ----- top level entry point used by main.py and the scheduler ----------
    def tick(self, watchlist: Sequence[str]) -> Dict[str, int]:
        self.sync_positions()
        self.reconcile_orphans()
        mgmt = self.process_management()
        self.submit(mgmt, action_type="management")
        entries = self.process_entries(watchlist)
        self.submit(entries, action_type="entry")
        # Authorize the overseer to inject "AI-only" trades after the rules-driven pass.
        if self.overseer is not None:
            ai_actions = self.overseer.propose(watchlist) or []
            self.submit(ai_actions, action_type="ai")
        return {"managed": len(mgmt), "entries": len(entries)}
