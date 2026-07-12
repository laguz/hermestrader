"""
[Service-1: Hermes-Agent-Core]
AbstractStrategy — the base class every cascading strategy subclasses.

Holds the shared helpers (expiry/strike selection, credit/debit math, logging)
and the two abstract hooks the CascadingEngine drives: ``execute_entries`` and
``manage_positions``.
"""
from __future__ import annotations

import asyncio
import logging
import math
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from hermes.clock import Clock, RealClock
from hermes.market_hours import ET as _ET
from .broker_wrapper import AsyncBrokerWrapper
from .money_manager import IronCondorBuilder, MoneyManager
from .trade_action import TradeAction

if TYPE_CHECKING:
    # Imported only for type checking — resolves the F821 forward references
    # to ``HermesOverseer`` without re-introducing a runtime circular import
    # (overseer.py imports TradeAction from this package).
    from .overseer import HermesOverseer

logger = logging.getLogger("hermes.agent.strategy")

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
        clock: Optional[Clock] = None,
    ):
        self.clock = clock or RealClock()
        self.broker = AsyncBrokerWrapper(broker, db)
        self.db = db
        self.mm = money_manager
        self.ic = ic_builder
        self.config = config or {}
        self.dry_run = dry_run
        self.overseer = overseer
        self.strategy_id = self.NAME
        self.execution_logs: List[str] = []
        # The event loop holds only a weak reference to tasks, so the bare
        # create_task() in _log() could be garbage-collected before the DB
        # write runs, silently dropping log lines. Keep a strong reference
        # until each task completes.
        self._pending_log_tasks: set[asyncio.Task] = set()

        # Decorate execute_entries to automatically apply throttle and log predictions to the ledger
        orig_execute = self.execute_entries
        async def wrapped_execute(watchlist):
            actions = await orig_execute(watchlist)
            return await self._process_and_throttle_actions(actions)
        self.execute_entries = wrapped_execute

    # ---- shared helpers ----------------------------------------------------
    async def load_tunables(self):
        """Resolve this strategy's tunables (settings > env config > default).

        Returns a :class:`~hermes.service1_agent.tunables.Tunables` carrying
        only this strategy's group (keyed off ``NAME``). Call once at the top
        of ``execute_entries`` / ``manage_positions`` and read parameters off
        the result rather than hardcoding literals. Imported lazily to avoid
        any import-cycle risk with the strategy package.
        """
        from .tunables import resolve
        return await resolve(self.db, self.config, group=self.NAME)

    def now(self) -> datetime:
        if hasattr(self.broker, "current_date") and self.broker.current_date:
            return self.broker.current_date
        inner = getattr(self.broker, "broker", None)
        if inner and hasattr(inner, "current_date") and inner.current_date:
            return inner.current_date
        return self.clock.utc_now()

    def today(self) -> date:
        """The current US Eastern trading-calendar date.

        ``self.now()`` is UTC (naive unless a test broker's ``current_date``
        says otherwise). Between ~8pm and midnight ET, UTC has already rolled
        to the next calendar day while the trading day hasn't — take the date
        from the ET conversion, not the raw UTC instant, or every DTE
        computation is off by one during that window.
        """
        now = self.now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.astimezone(_ET).date()

    def _log(self, msg: str) -> None:
        ts = self.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} [{self.NAME}] {msg}"
        self.execution_logs.append(line)
        logger.info(line)
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self.db.logs.write_log(self.strategy_id, msg))
            self._pending_log_tasks.add(task)
            task.add_done_callback(self._pending_log_tasks.discard)
        except RuntimeError:
            from hermes.ml.predictor_config import run_maybe_async
            run_maybe_async(self.db.logs.write_log, self.strategy_id, msg)

    async def find_expiry_in_dte_range(self, symbol: str, min_dte: int, max_dte: int,
                                 prefer: str = "max") -> Optional[str]:
        expirations = await self.broker.get_option_expirations(symbol) or []
        today = self.today()
        candidates: List[date] = []
        for e in expirations:
            try:
                d = e if isinstance(e, date) else datetime.strptime(str(e), "%Y-%m-%d").date()
                dte = (d - today).days
                if min_dte <= dte <= max_dte:
                    candidates.append(d)
            except (ValueError, TypeError):
                logger.warning("[STRATEGY] Skipping invalid expiration format: %r", e)
                continue
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
        open_legs = await self.db.trades.open_legs(self.strategy_id, symbol)
        expiry_sides: Dict[str, set] = {}
        for leg in open_legs:
            exp = leg.get("expiry")
            if exp:
                try:
                    datetime.strptime(str(exp), "%Y-%m-%d")
                    expiry_sides.setdefault(str(exp), set()).add(leg.get("side", "").lower())
                except (ValueError, TypeError):
                    logger.warning("[ENGINE] Skipping invalid active IC expiry from DB: %r", exp)
                    continue

        # Sort expiries chronologically; ISO YYYY-MM-DD strings sort
        # correctly without parsing.
        for exp in sorted(expiry_sides):
            if len(expiry_sides[exp]) == 1:
                return exp
        return None

    async def find_strike_by_delta(self, chain, option_type: str, target_delta: float,
                                   tolerance: float = 0.05) -> Optional[Dict[str, Any]]:
        best, best_diff = None, math.inf
        for o in chain:
            if o.get("option_type") != option_type:
                continue
            # Tradier returns greeks=null for deep OTM / illiquid options;
            # guard with `or {}` so we treat missing greeks as delta=0.0
            greeks = o.get("greeks") or {}
            raw_delta = greeks.get("delta")
            
            # Fallback to local Greeks calculation if broker delta is missing
            if raw_delta is None:
                from hermes.service1_agent.strategies._helpers import parse_occ
                from hermes.greeks import implied_volatility, black_scholes_greeks
                
                occ_info = parse_occ(o.get("symbol", ""))
                if occ_info:
                    symbol = occ_info["underlying"]
                    expiry_date = occ_info["expiry"]
                    strike = float(o.get("strike") or 0)
                    
                    try:
                        quotes = await self.broker.get_quote(symbol)
                        if quotes:
                            spot = float(
                                quotes[0].get("last") if quotes[0].get("last") is not None
                                else quotes[0].get("close") if quotes[0].get("close") is not None
                                else 0.0
                            )
                            if spot > 0:
                                today = self.today()
                                dte = (expiry_date - today).days
                                T = dte / 365.0
                                if T > 0:
                                    bid = float(o.get("bid") or 0)
                                    ask = float(o.get("ask") or 0)
                                    if bid > 0 and ask > 0:
                                        mid = (bid + ask) / 2.0
                                        # Solve for implied volatility, then compute delta
                                        sigma = implied_volatility(mid, spot, strike, T, 0.05, option_type)
                                        if sigma > 0:
                                            local_greeks = black_scholes_greeks(spot, strike, T, 0.05, sigma, option_type)
                                            raw_delta = local_greeks.get("delta")
                    except Exception as exc:
                        logger.warning("Failed to calculate delta for %s: %s", o.get("symbol"), exc)

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

        Returns ``(mid_debit, exec_debit, blocked, reason)``.

        Two debits are returned because they serve different purposes:
        - ``mid_debit``  — mid(short) − mid(long), matches how entry credit is
          measured (``short_credit`` uses mid-mid).  Used for SL/TP decisions
          so wide bid-ask spreads on TSLA/high-IV names don't produce false
          stop-loss triggers on profitable positions.
        - ``exec_debit`` — short_ask − long_bid, the real worst-case execution
          cost.  Used as the order limit price so the close actually fills.

        Guards: refuse both when either leg is missing a positive bid AND ask,
        or when exec_debit exceeds the spread width by more than 10% (which
        only happens when long_bid is stale/zero — the phantom check).
        """
        if not (short_quote and long_quote):
            return None, None, True, "missing quote leg"
        try:
            sb = float(short_quote.get("bid") or 0)
            sa = float(short_quote.get("ask") or 0)
            lb = float(long_quote.get("bid") or 0)
            la = float(long_quote.get("ask") or 0)
            w = float(width or 0)
        except (TypeError, ValueError):
            return None, None, True, "quote parse error"

        if sa <= 0 or la <= 0 or sb <= 0 or lb <= 0:
            return None, None, True, (
                f"stale quote: short={sb}/{sa} long={lb}/{la}"
            )

        exec_debit = max(0.01, round(sa - lb, 2))
        # Phantom check: an honest spread debit cannot exceed its width by any
        # meaningful margin (10% slack). Only exec_debit can hit this because
        # long_bid can be near-zero on illiquid legs; mid_debit is self-bounding.
        if w > 0 and exec_debit > w * 1.10:
            return None, None, True, (
                f"phantom debit ${exec_debit:.2f} > width ${w:.2f} × 1.10 "
                f"(short_ask={sa} long_bid={lb})"
            )

        mid_debit = max(0.01, round(((sa + sb) / 2) - ((la + lb) / 2), 2))
        return mid_debit, exec_debit, False, ""

    def is_morning_unreliable(self, now_dt: Optional[datetime] = None) -> bool:
        """True if the current time is between 9:30 AM and 10:30 AM Eastern Time."""
        if now_dt is None:
            now_dt = self.now()

        try:
            from zoneinfo import ZoneInfo
            ET = ZoneInfo("America/New_York")
        except Exception:
            from datetime import timezone as dt_timezone, timedelta
            ET = dt_timezone(timedelta(hours=-5))

        # Ensure timezone-aware for zoneinfo conversion
        from datetime import timezone as dt_timezone, time
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=dt_timezone.utc)

        now_et = now_dt.astimezone(ET)
        current_time = now_et.time()

        # 9:30 AM to 10:30 AM Eastern Time
        return time(9, 30) <= current_time < time(10, 30)

    async def is_event_gated(self, symbol: str, blackout_days: int) -> bool:
        try:
            blackout_days = int(blackout_days)
        except (TypeError, ValueError):
            blackout_days = 0

        if blackout_days <= 0:
            return False

        from hermes.event_calendar import is_macro_event_within_days, has_earnings_within_days

        today = self.today()
        if is_macro_event_within_days(today, blackout_days):
            self._log(f"⚠️ Entry blocked: macro event (FOMC/CPI) scheduled within {blackout_days} days.")
            return True

        try:
            if await has_earnings_within_days(self.broker, symbol, today, blackout_days):
                self._log(f"⚠️ Entry blocked: {symbol} has earnings scheduled within {blackout_days} days.")
                return True
        except Exception as exc:
            logger.error("[EVENT CALENDAR] Earnings calendar fetch failed for %s: %s", symbol, exc, exc_info=True)
            self._log(f"⚠️ WARNING: Earnings calendar fetch failed for {symbol} ({exc}). Gate failing open; entry qualification degraded.")

        return False

    async def _fetch_current_atm_iv(self, symbol: str) -> Optional[float]:
        try:
            expirations = await self.broker.get_option_expirations(symbol)
            if not expirations:
                return None
            
            today = self.today()
            valid_expiries = []
            for exp in expirations:
                try:
                    d = datetime.strptime(exp, "%Y-%m-%d").date()
                    valid_expiries.append((d, exp))
                except (ValueError, TypeError):
                    pass
            
            if not valid_expiries:
                return None
            
            # Sort by DTE proximity to 30 days
            best_expiry = min(valid_expiries, key=lambda x: abs((x[0] - today).days - 30))[1]
            
            chain = await self.broker.get_option_chains(symbol, best_expiry)
            if not chain:
                return None
            
            # Fetch current stock price (spot price)
            spot = await self.broker.last_price(symbol)
            if spot is None:
                quotes = await self.broker.get_quote(symbol)
                if quotes and len(quotes) > 0:
                    spot = quotes[0].get("last")
                if spot is None:
                    # Fallback to median strike in chain
                    strikes = [o.get("strike") for o in chain if o.get("strike") is not None]
                    if strikes:
                        import statistics
                        spot = statistics.median(strikes)
            
            if spot is None:
                return None
            
            # Find the strike closest to spot
            atm_strike = min(
                (o.get("strike") for o in chain if o.get("strike") is not None),
                key=lambda s: abs(s - spot),
                default=None
            )
            if atm_strike is None:
                return None
            
            ivs = []
            for o in chain:
                if o.get("strike") == atm_strike:
                    greeks = o.get("greeks") or {}
                    iv = greeks.get("mid_iv")
                    if iv is None:
                        iv = greeks.get("smv_vol")
                    if iv is not None:
                        try:
                            ivs.append(float(iv))
                        except (ValueError, TypeError):
                            pass
            
            if ivs:
                import statistics
                return float(statistics.mean(ivs))
            return None
        except Exception as exc:
            logger.debug("[IV GATING] Failed to fetch current ATM IV for %s: %s", symbol, exc)
            return None

    async def is_ivr_gated(self, symbol: str, min_ivr: float) -> bool:
        try:
            min_ivr = float(min_ivr)
        except (TypeError, ValueError):
            min_ivr = 0.0

        if min_ivr <= 0.0:
            return False

        current_iv = await self._fetch_current_atm_iv(symbol)
        if current_iv is None:
            logger.debug("[IV GATING] Missing current ATM IV for %s; degrading to no gating.", symbol)
            return False

        # Get historical IVs from the TimescaleDB timeseries repository
        history = await self.db.timeseries.get_implied_vol_history(symbol, lookback_days=365)
        if not history:
            logger.debug("[IV GATING] Missing historical IV data for %s; degrading to no gating.", symbol)
            # Save today's observation anyway so we start building history
            await self.db.timeseries.save_implied_vol(symbol, current_iv)
            return False

        # Extract only the IV values and combine with current_iv
        all_ivs = [iv for _, iv in history] + [current_iv]
        min_iv = min(all_ivs)
        max_iv = max(all_ivs)

        if max_iv > min_iv:
            ivr = 100.0 * (current_iv - min_iv) / (max_iv - min_iv)
            if ivr < min_ivr:
                self._log(f"⚠️ Entry blocked: {symbol} IV rank {ivr:.1f}% is below threshold {min_ivr:.1f}%.")
                return True
        
        # Save today's observation to the implied_volatility history hypertable
        await self.db.timeseries.save_implied_vol(symbol, current_iv)
        return False

    async def _process_and_throttle_actions(self, actions: List[TradeAction]) -> List[TradeAction]:
        if not actions:
            return actions

        throttle_mult = await self.get_throttle_multiplier()

        for action in actions:
            action.strategy_params["throttle_mult"] = throttle_mult

            # Resolve/write prediction to the ledger
            pop = action.strategy_params.get("pop")
            if pop is None:
                delta = action.strategy_params.get("delta")
                if delta is None:
                    delta = action.strategy_params.get("short_delta")
                pop = 1.0 - abs(float(delta)) if delta is not None else 0.70

            dte = action.dte or 7
            spot = action.strategy_params.get("spot")
            if spot is None:
                try:
                    spot = await self.broker.last_price(action.symbol)
                except Exception:
                    pass
            if spot is None:
                spot = 100.0

            await self.write_prediction_to_ledger(action.symbol, float(pop), float(spot), int(dte))

        return actions

    async def get_throttle_multiplier(self) -> float:
        try:
            t = await self.load_tunables()
            raw_window = t.get(f"{self.KEY_PREFIX}throttle_window")
            window = int(raw_window) if raw_window is not None else 0
        except Exception:
            window = 0

        if window <= 0:
            return 1.0

        try:
            raw_drift = t.get(f"{self.KEY_PREFIX}throttle_drift_threshold")
            drift_threshold = float(raw_drift) if raw_drift is not None else 0.0
            
            raw_floor = t.get(f"{self.KEY_PREFIX}throttle_floor_mult")
            floor_mult = float(raw_floor) if raw_floor is not None else 1.0
        except Exception:
            drift_threshold = 0.0
            floor_mult = 1.0

        floor_mult = min(1.0, max(0.0, floor_mult))

        from hermes.ml.ledger import PredictionLedger
        if PredictionLedger is None:
            return 1.0

        try:
            from sqlalchemy import select
            stmt = (
                select(PredictionLedger)
                .filter(
                    PredictionLedger.model_name == self.NAME,
                    PredictionLedger.realized_outcome.is_not(None)
                )
                .order_by(PredictionLedger.ts.desc())
                .limit(window)
            )
            async with self.db.AsyncSession() as session:
                res = await session.execute(stmt)
                rows = res.scalars().all()

            if len(rows) < window:
                logger.debug(
                    "[THROTTLE] %s has insufficient history (%d/%d closed predictions); returning multiplier 1.0",
                    self.NAME, len(rows), window
                )
                return 1.0

            outcomes = [float(r.realized_outcome) for r in rows if r.realized_outcome is not None]
            probs = [float(r.predicted_prob or 0.0) for r in rows]

            if not outcomes:
                return 1.0

            realized_win_rate = sum(outcomes) / len(outcomes)
            calibrated_pop = sum(probs) / len(probs)

            drift = calibrated_pop - realized_win_rate
            if drift > drift_threshold:
                logger.warning(
                    "[THROTTLE] %s underperforming: realized win rate %.1f%% drifts below calibrated POP %.1f%% by %.1f%% (threshold %.1f%%). Scaling size by %.2f",
                    self.NAME, realized_win_rate * 100, calibrated_pop * 100, drift * 100, drift_threshold * 100, floor_mult
                )
                return floor_mult

            return 1.0
        except Exception as exc:
            logger.warning("[THROTTLE] %s failed to compute throttle: %s. Fails open to 1.0.", self.NAME, exc)
            return 1.0

    async def write_prediction_to_ledger(self, symbol: str, pop: float, spot: float, horizon_dte: int) -> None:
        from hermes.ml.ledger import write_record, LedgerRecord
        try:
            await write_record(self.db, LedgerRecord(
                symbol=symbol,
                model_name=self.NAME,
                horizon_dte=horizon_dte,
                model_hash=None,
                schema_hash=None,
                schema_stage="strategy_qualification",
                predicted_prob=pop,
                predicted_prob_lo=None,
                predicted_prob_hi=None,
                predicted_return=None,
                spot=spot,
                feature_vector={},
            ))
            logger.debug("[THROTTLE] Logged prediction to ledger for %s: %s (POP=%.2f DTE=%d)", self.NAME, symbol, pop, horizon_dte)
        except Exception as exc:
            logger.warning("[THROTTLE] Failed to write prediction to ledger for %s: %s", self.NAME, exc)

    # ---- API expected by the cascading engine ------------------------------
    @abstractmethod
    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]: ...

    @abstractmethod
    async def manage_positions(self) -> List[TradeAction]: ...
