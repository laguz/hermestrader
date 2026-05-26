"""
[Service-1: Hermes-Agent-Core]
Core abstractions: TradeAction, AbstractStrategy, IronCondorBuilder,
MoneyManager and the CascadingEngine that drives execution priority.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

from hermes.common import OCC_RE
from hermes.events.bus import EventBus, ReviewRequestEvent, AIApprovalEvent, MarketDataEvent

if TYPE_CHECKING:
    # Imported only for type checking — resolves the F821 forward references
    # to ``HermesOverseer`` without re-introducing a runtime circular import
    # (overseer.py imports TradeAction from this module).
    from .overseer import HermesOverseer

logger = logging.getLogger("hermes.agent.core")

import inspect

class AsyncBrokerWrapper:
    """Wraps a synchronous or asynchronous broker to present a unified async interface."""
    def __init__(self, broker, db=None):
        self.broker = broker
        self.db = db
        from hermes.broker.circuit_breaker import CircuitBreaker
        if not hasattr(AsyncBrokerWrapper, "_shared_cb"):
            AsyncBrokerWrapper._shared_cb = CircuitBreaker()

    def __getattr__(self, name):
        if name == "place_order_from_action":
            return self._place_order_from_action_wrapped

        attr = getattr(self.broker, name)
        if callable(attr):
            async def _async_wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
                    return await attr(*args, **kwargs)
                res = attr(*args, **kwargs)
                if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                    return await res
                return res
            return _async_wrapper
        return attr

    async def _place_order_from_action_wrapped(self, action):
        from hermes.broker.circuit_breaker import CircuitBreakerError
        cb = AsyncBrokerWrapper._shared_cb
        state = cb.check_state()
        if state == "OPEN":
            logger.error("Circuit breaker is OPEN. Fast-failing order placement.")
            raise CircuitBreakerError("Circuit breaker is OPEN. Orders are blocked.")

        try:
            attr = getattr(self.broker, "place_order_from_action")
            if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
                res = await attr(action)
            else:
                res = attr(action)
                if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                    res = await res

            rejected = False
            if isinstance(res, dict):
                if "errors" in res or "error" in res:
                    rejected = True
                order = res.get("order")
                if isinstance(order, dict):
                    status = str(order.get("status", "")).lower()
                    if status in {"rejected", "error", "expired", "canceled", "cancelled"}:
                        rejected = True

            if rejected:
                await cb.record_failure(self.db, f"Order rejected: {res}")
                if cb.state == "OPEN":
                    raise CircuitBreakerError("Order rejected and circuit breaker tripped to OPEN.")
            else:
                cb.record_success()

            return res

        except Exception as e:
            if not isinstance(e, CircuitBreakerError):
                await cb.record_failure(self.db, f"Order placement exception: {e}")
            raise


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
        self.broker = AsyncBrokerWrapper(broker, db)
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
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.db.write_log(self.strategy_id, msg))
        except RuntimeError:
            asyncio.run(self.db.write_log(self.strategy_id, msg))

    async def find_expiry_in_dte_range(self, symbol: str, min_dte: int, max_dte: int,
                                 prefer: str = "max") -> Optional[str]:
        expirations = await self.broker.get_option_expirations(symbol) or []
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

    async def find_active_ic_expiry(self, symbol: str) -> Optional[str]:
        """Return the expiry of an incomplete Iron Condor for this strategy and symbol.

        Deterministic ordering: when multiple incomplete ICs exist, return
        the earliest expiry so completion is prioritised on the trade
        closest to its DTE deadline. Without sorting, dict iteration order
        determined the choice — stable in CPython but reads as non-obvious.
        """
        open_legs = await self.db.open_legs(self.strategy_id, symbol)
        expiry_sides: Dict[str, set] = {}
        for leg in open_legs:
            exp = leg.get("expiry")
            if exp:
                expiry_sides.setdefault(exp, set()).add(leg.get("side", "").lower())

        # Sort expiries chronologically; ISO YYYY-MM-DD strings sort
        # correctly without parsing.
        for exp in sorted(expiry_sides):
            if len(expiry_sides[exp]) == 1:
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

    @staticmethod
    def compute_close_debit(short_quote, long_quote, width):
        """Sane debit-to-close for a vertical spread.

        Returns ``(debit, blocked, reason)``.

        Closing a credit spread costs ``short_ask − long_bid`` per share.
        Two failure modes were observed in production:

        1. **Stale / missing quote** — Tradier returns ``bid=0`` on
           illiquid contracts (especially pre-market on the long-side
           protection leg). The naive formula then collapses to
           ``short_ask`` which can be many multiples of the spread
           width, looking like a max-loss SL trigger when the real
           debit is bounded by ``width``.
        2. **Bid-ask asymmetry vs. entry** — entry credit uses
           mid-mid (``short_credit``) but the original close calc used
           worst-of (``ask − bid``). Compounded with (1) this fires
           panic-priced SL closes on transient quote glitches.

        Guards: refuse the calculation when either leg is missing a
        positive bid AND ask, or when the resulting debit exceeds the
        spread width by more than 10% (impossible on a real spread).
        """
        if not (short_quote and long_quote):
            return None, True, "missing quote leg"
        try:
            sb = float(short_quote.get("bid") or 0)
            sa = float(short_quote.get("ask") or 0)
            lb = float(long_quote.get("bid") or 0)
            la = float(long_quote.get("ask") or 0)
            w = float(width or 0)
        except (TypeError, ValueError):
            return None, True, "quote parse error"

        if sa <= 0 or lb <= 0:
            return None, True, (
                f"stale quote: short_ask={sa} long_bid={lb} "
                f"(short_bid={sb} long_ask={la})"
            )

        debit = max(0.01, round(sa - lb, 2))
        # An honest spread debit cannot exceed its width by any
        # meaningful margin. 10% slack tolerates wide bid-ask noise on
        # one-lot orders without permitting the phantom $4.14-on-$1
        # blowouts that triggered the IWM SL false-positive.
        if w > 0 and debit > w * 1.10:
            return None, True, (
                f"phantom debit ${debit:.2f} > width ${w:.2f} × 1.10 "
                f"(short_ask={sa} long_bid={lb})"
            )
        return debit, False, ""

    # ---- API expected by the cascading engine ------------------------------
    @abstractmethod
    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]: ...

    @abstractmethod
    async def manage_positions(self) -> List[TradeAction]: ...


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
                 approval_mode: bool = False,
                 money_manager: Optional["MoneyManager"] = None,
                 config: Optional[Dict[str, Any]] = None,
                 event_bus: Optional[EventBus] = None):
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        # Sort by declared PRIORITY (1 highest)
        self.strategies = sorted(strategies, key=lambda s: s.PRIORITY)
        self.overseer = overseer
        # When True, submit() queues trades for human approval instead of
        # sending them to the broker directly.
        self.approval_mode = approval_mode
        # MoneyManager is shared across strategies; the engine also holds a
        # reference so tick() can refresh broker-side order counts before
        # capacity decisions run. Falls back to the first strategy's mm so
        # callers that haven't been updated yet still work.
        self.mm = money_manager or (strategies[0].mm if strategies else None)
        self.config = config or {}
        self.event_bus = event_bus
        self._quote_cache: Dict[str, Dict[str, Any]] = {}
        if self.event_bus is not None:
            self.event_bus.subscribe(AIApprovalEvent, self.handle_ai_approval)
            self.event_bus.subscribe(MarketDataEvent, self.handle_market_data)

    # 1
    async def sync_positions(self) -> None:
        positions = await self.broker.get_positions() or []
        if not isinstance(positions, list):
            logger.warning("[ENGINE] get_positions returned non-list: %r", positions)
            positions = []
        # Resting/accepted orders haven't created positions yet; the
        # reconciler must treat their legs as still-alive coverage so
        # just-submitted spreads aren't flipped to CLOSED before fill.
        active_legs: set = set()
        try:
            active_statuses = {"open", "partially_filled", "pending",
                                "accepted", "calculated"}
            orders = await self.broker.get_orders() or []
            if not isinstance(orders, list):
                logger.warning("[ENGINE] get_orders returned non-list: %r", orders)
                orders = []
            for o in orders:
                if str(o.get("status", "")).lower() not in active_statuses:
                    continue
                legs = o.get("leg") or []
                if isinstance(legs, dict):
                    legs = [legs]
                for leg in legs:
                    sym = leg.get("option_symbol")
                    if sym:
                        active_legs.add(sym)
                top = o.get("option_symbol")
                if top:
                    active_legs.add(top)
        except Exception:                              # noqa: BLE001
            logger.exception("[ENGINE] active-order leg fetch failed")
        await self.db.upsert_positions(positions, active_order_legs=active_legs)

    # 2
    async def reconcile_orphans(self) -> None:
        """Flag broker positions not tied to any strategy as MANUAL_ORPHAN."""
        tracked = await self.db.tracked_option_symbols()
        live = {p["symbol"] for p in await self.broker.get_positions() or []}
        orphans = live - tracked
        if orphans:
            await self.db.flag_orphans(orphans)

    # 3
    async def process_management(self) -> List[TradeAction]:
        actions: List[TradeAction] = []
        for s in self.strategies:
            try:
                actions.extend(await s.manage_positions())
            except Exception as exc:                     # noqa: BLE001
                logger.exception("Management failure in %s: %s", s.NAME, exc)
        return actions

    # 4
    async def _watchlist_for(self, strategy_id: str, default: Sequence[str]) -> List[str]:
        """Per-strategy watchlist with fallback to the engine-level default."""
        getter = getattr(self.db, "list_watchlist", None)
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
        except Exception as exc:                          # noqa: BLE001
            logger.exception("watchlist read failed for %s: %s", strategy_id, exc)
            return list(default)
        return (wl or []) or list(default)

    async def process_entries(self, watchlist: Sequence[str]) -> int:
        """Execute entries in priority order. Submits actions after each strategy
        to ensure MoneyManager capacity is updated for the next priority level.
        Returns total number of entry actions planned.
        """
        # Dedup watchlist to prevent multiple scans of the same symbol in one tick
        unique_watchlist = list(dict.fromkeys(watchlist))
        total_entries = 0
        max_per_tick = int(self.config.get("max_orders_per_tick", 5))
        tick_submitted = 0

        for s in self.strategies:
            try:
                if tick_submitted >= max_per_tick:
                    logger.warning(
                        "[ENGINE] max_orders_per_tick=%d reached; skipping %s entries",
                        max_per_tick, s.NAME,
                    )
                    await self.db.write_log(
                        s.strategy_id,
                        f"[GUARD] max_orders_per_tick={max_per_tick} reached; "
                        f"{s.NAME} entries skipped this tick",
                    )
                    break

                wl = await self._watchlist_for(s.strategy_id, unique_watchlist)
                # Drain entire watchlist for THIS strategy.
                actions = await s.execute_entries(wl)

                # Cap to remaining budget for this tick.
                remaining = max_per_tick - tick_submitted
                if len(actions) > remaining:
                    logger.warning(
                        "[ENGINE] %s generated %d actions; trimming to %d (max_orders_per_tick=%d)",
                        s.NAME, len(actions), remaining, max_per_tick,
                    )
                    await self.db.write_log(
                        s.strategy_id,
                        f"[GUARD] {s.NAME} generated {len(actions)} actions; "
                        f"trimmed to {remaining} (max_orders_per_tick={max_per_tick})",
                    )
                    actions = actions[:remaining]

                # Submit immediately so subsequent strategies see these as PENDING.
                await self.submit(actions, action_type="entry")
                tick_submitted += len(actions)
                total_entries += len(actions)

                # Re-sync broker orders so the next strategy's capacity check
                # reflects any orders just placed (fills between ticks are now visible).
                if actions:
                    await self.mm.sync_broker_orders()

            except Exception as exc:                     # noqa: BLE001
                logger.exception("Entry failure in %s: %s", s.NAME, exc)
        return total_entries

    async def submit(self, actions: Iterable[TradeAction],
               action_type: str = "entry") -> None:
        # Defence-in-depth market-hours gate. Every broker round-trip
        # MUST go through this method (entries, managed closes, AI
        # actions) so a single check here keeps the bot from sending
        # orders into pre-market / after-hours / weekend / holiday
        # windows where quote feeds are stale and fills are punitive.
        # Operators who explicitly want off-hours submission can set
        # HERMES_ALLOW_OFFHOURS_TRADES=true (see market_hours.py).
        from hermes.market_hours import should_block_trades
        blocked, reason = should_block_trades()
        if blocked:
            actions = list(actions)
            for a in actions:
                await self.db.write_log(
                    a.strategy_id,
                    f"[OFF-HOURS BLOCKED] {a.symbol} {action_type} "
                    f"qty={a.quantity} — {reason}; not sent to broker",
                )
            if actions:
                logger.info("[OFF-HOURS] blocked %d %s action(s): %s",
                            len(actions), action_type, reason)
            return
        for a in actions:
            if self.event_bus is not None:
                if self.overseer is not None and action_type != "ai":
                    # Yield to AI Overseer asynchronously
                    event = ReviewRequestEvent(
                        strategy_id=a.strategy_id,
                        symbol=a.symbol,
                        trade_action=a
                    )
                    self.event_bus.emit(event)
                else:
                    # Bypasses AI review (either no overseer, or action is already AI-authored)
                    event = AIApprovalEvent(
                        strategy_id=a.strategy_id,
                        symbol=a.symbol,
                        verdict="APPROVE",
                        rationale="Auto-approved (AI-authored or no overseer).",
                        original_action=a
                    )
                    self.event_bus.emit(event)
                continue

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
                if await self.db.has_pending_approval(a.strategy_id, a.symbol,
                                                side_type, a.expiry):
                    logger.info(
                        "[C2] Skipping duplicate — already PENDING: %s %s "
                        "side=%s expiry=%s",
                        a.strategy_id, a.symbol, side_type, a.expiry,
                    )
                    await self.db.write_log(
                        a.strategy_id,
                        f"[DEDUP] {a.symbol} {side_type} expiry={a.expiry} "
                        f"already PENDING approval — skipped",
                    )
                    continue

                # Queue for human review instead of firing directly.
                action_dict = dataclasses.asdict(a)
                await self.db.queue_for_approval(action_dict, action_type=action_type)
                logger.info(
                    "[C2] Trade queued for approval: %s %s strategy=%s side=%s expiry=%s",
                    a.symbol, a.order_class, a.strategy_id, side_type, a.expiry,
                )
                await self.db.write_log(
                    a.strategy_id,
                    f"[APPROVAL REQUIRED] {a.symbol} {a.order_class} "
                    f"side={side_type} expiry={a.expiry} "
                    f"qty={a.quantity} — awaiting human approval",
                )
            else:
                await self.db.record_pending_order(a)
                # Management actions whose legs are all *_to_close represent
                # the close of an existing trade, not a new entry. Route
                # them to ``close_trade_from_action`` which UPDATES the
                # original Trade row (status→CLOSED, exit_price, pnl,
                # close_tag, close_reason) instead of inserting a ghost
                # OPEN row that the reconciler later flattens with a
                # generic 'RECONCILED_BROKER_FLAT' and pnl=NULL.
                #
                # Any management action that opens a leg (e.g. WHEEL_ROLL,
                # which buys-to-close + sells-to-open the same strike on
                # the next month) keeps the legacy path so the new short
                # still gets a Trade row.
                is_pure_close = (
                    action_type == "management"
                    and bool(a.legs)
                    and all("to_open" not in (leg.get("side") or "").lower()
                            for leg in a.legs)
                )
                close_method = getattr(self.db, "close_trade_from_action", None)
                if not getattr(self.broker, "dry_run", False):
                    try:
                        resp = await self.broker.place_order_from_action(a)
                    except Exception as exc:                       # noqa: BLE001
                        # Broker raised before we got an order id. Free the
                        # PENDING row so capacity recovers; a Trade row was
                        # never written, nothing to roll back.
                        if is_pure_close and close_method is not None:
                            await close_method(a, {"errors": str(exc)})
                        else:
                            await self.db.record_order_response(
                                a, {"errors": str(exc)})
                        logger.exception("place_order failed for %s: %s",
                                          a.symbol, exc)
                    else:
                        if is_pure_close and close_method is not None:
                            await close_method(a, resp)
                        else:
                            await self.db.record_order_response(a, resp)

    # ----- top level entry point used by main.py and the scheduler ----------
    async def tick(self, watchlist: Sequence[str]) -> Dict[str, int]:
        await self.sync_positions()
        # Refresh real-time broker order counts to prevent duplicate entries.
        # mm may be None on legacy callers that haven't been updated yet;
        # skip rather than crash the entire tick.
        if self.mm is not None:
            await self.mm.sync_broker_orders()
        await self.reconcile_orphans()
        mgmt = await self.process_management()
        await self.submit(mgmt, action_type="management")
        # Entries are now submitted internally strategy-by-strategy.
        num_entries = await self.process_entries(watchlist)
        # Authorize the overseer to inject "AI-only" trades after the rules-driven pass.
        ai_count = 0
        if self.overseer is not None:
            if self.event_bus is not None:
                # Asynchronously generate AI proposals without blocking the tick loop
                asyncio.create_task(self._async_propose(watchlist))
            else:
                ai_actions = await asyncio.to_thread(self.overseer.propose, watchlist)
                await self.submit(ai_actions, action_type="ai")
                ai_count = len(ai_actions)
        return {"managed": len(mgmt), "entries": num_entries, "ai": ai_count}

    async def handle_ai_approval(self, event: AIApprovalEvent) -> None:
        """Asynchronously executes or queues an action after AI approval."""
        a = event.original_action
        if a is None:
            logger.warning("AIApprovalEvent has no original_action; skipping.")
            return

        if event.verdict == "VETO":
            logger.info("[AI VETOED] Strategy=%s symbol=%s - %s", event.strategy_id, event.symbol, event.rationale)
            await self.db.write_log(
                event.strategy_id,
                f"[AI VETOED] {event.symbol} — {event.rationale}"
            )
            return

        if event.verdict == "MODIFY":
            # Apply modifications
            if event.modifications:
                for k, v in event.modifications.items():
                    if hasattr(a, k):
                        setattr(a, k, v)
                a.ai_authored = True
                a.ai_rationale = event.rationale

        # Now proceed to order placement / human approval queue (equivalent to the rest of submit)
        if self.approval_mode:
            # Dedup check
            side_type = (a.strategy_params or {}).get("side_type")
            has_pending = await self.db.has_pending_approval(
                a.strategy_id, a.symbol, side_type, a.expiry
            )
            if has_pending:
                logger.info(
                    "[C2] Skipping duplicate — already PENDING: %s %s side=%s expiry=%s",
                    a.strategy_id, a.symbol, side_type, a.expiry,
                )
                await self.db.write_log(
                    a.strategy_id,
                    f"[DEDUP] {a.symbol} {side_type} expiry={a.expiry} already PENDING approval — skipped"
                )
                return

            # Queue for human review
            action_dict = dataclasses.asdict(a)
            await self.db.queue_for_approval(action_dict, action_type="entry")
            logger.info(
                "[C2] Trade queued for approval: %s %s strategy=%s side=%s expiry=%s",
                a.symbol, a.order_class, a.strategy_id, side_type, a.expiry,
            )
            await self.db.write_log(
                a.strategy_id,
                f"[APPROVAL REQUIRED] {a.symbol} {a.order_class} "
                f"side={side_type} expiry={a.expiry} "
                f"qty={a.quantity} — awaiting human approval"
            )
        else:
            await self.db.record_pending_order(a)
            
            # Determine if pure close
            is_pure_close = (
                bool(a.legs)
                and all("to_open" not in (leg.get("side") or "").lower()
                        for leg in a.legs)
            )
            
            close_method = getattr(self.db, "close_trade_from_action", None)
            
            try:
                resp = await self.broker.place_order_from_action(a)
            except Exception as exc:
                if is_pure_close and close_method is not None:
                    await close_method(a, {"errors": str(exc)})
                else:
                    await self.db.record_order_response(a, {"errors": str(exc)})
                logger.exception("place_order failed for %s: %s", a.symbol, exc)
            else:
                if is_pure_close and close_method is not None:
                    await close_method(a, resp)
                else:
                    await self.db.record_order_response(a, resp)

    async def _async_propose(self, watchlist: Sequence[str]) -> None:
        """Asynchronously triggers the overseer to propose actions without blocking the tick loop."""
        try:
            ai_actions = await self.overseer.propose(watchlist)
            if ai_actions:
                await self.submit(ai_actions, action_type="ai")
        except Exception as exc:
            logger.exception("Error in async propose: %s", exc)

    async def handle_market_data(self, event: MarketDataEvent) -> None:
        """Evaluates strategies reactively when a new MarketDataEvent is received."""
        symbol = event.symbol
        
        # Update quote cache
        self._quote_cache[symbol] = {
            "price": event.price,
            "volume": event.volume,
            **event.data
        }
        
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

        # Run entries for this symbol across strategies in priority order
        for s in self.strategies:
            try:
                wl = await self._watchlist_for(s.strategy_id, [symbol])
                if symbol not in wl:
                    continue
                
                actions = await s.execute_entries([symbol])
                if actions:
                    await self.submit(actions, action_type="entry")
                    if self.mm is not None:
                        await self.mm.sync_broker_orders()
            except Exception as exc:
                logger.exception("Entry failure in %s for %s: %s", s.NAME, symbol, exc)
