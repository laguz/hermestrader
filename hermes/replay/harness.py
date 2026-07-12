"""Replay driver: SimulatedClock + ReplayBroker + ReplayDB around the real engine.

The engine is production code, unmodified: ``CascadingEngine`` is built with
``event_bus=None`` (synchronous tick path), ``overseer=None`` (no LLM),
``approval_mode=False`` (entries route straight to the simulated broker), and
the same strategy set ``make_strategies`` gives the live agent. The harness
only steps time, settles expiries, and snapshots equity between ticks.

Safety: construction asserts the broker is a :class:`ReplayBroker` and the DB
is a :class:`ReplayDB` — there is no code path here that can build a Tradier
client or open a writable database connection.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timezone
from typing import Any, Dict, List, Optional, Sequence

import hermes.utils as _utils
from hermes.clock import SimulatedClock
from hermes.market_hours import ET
from hermes.service1_agent.agent_construction import make_strategies
from hermes.service1_agent.core import CascadingEngine, IronCondorBuilder, MoneyManager

from .broker import ReplayBroker, _parse_occ
from .data import ReplayDataSource
from .memdb import ReplayDB
from .report import build_report

logger = logging.getLogger("hermes.replay.harness")

_OFFHOURS_ENV = "HERMES_ALLOW_OFFHOURS_TRADES"


@dataclass
class ReplayConfig:
    symbols: List[str]
    start: date
    end: date
    strategies: Optional[List[str]] = None          # NAME filter; None = all
    tick_times_et: Sequence[str] = ("10:35", "13:00", "15:05")
    starting_bp: float = 100_000.0
    slippage_frac: float = 0.0
    engine_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayResult:
    trades: List[Dict[str, Any]]
    fills: List[Dict[str, Any]]
    settlements: List[Dict[str, Any]]
    equity_curve: List[Dict[str, Any]]              # {ts, per_strategy: {...}, total}
    report: Dict[str, Any]
    ticks: int


def _tick_instants(day: date, tick_times_et: Sequence[str]) -> List[datetime]:
    """ET wall-clock tick times on ``day`` as naive-UTC instants, ascending."""
    out = []
    for hhmm in tick_times_et:
        hh, mm = hhmm.split(":")
        et_dt = datetime.combine(day, dt_time(int(hh), int(mm)), tzinfo=ET)
        out.append(et_dt.astimezone(timezone.utc).replace(tzinfo=None))
    return sorted(out)


class ReplayHarness:
    def __init__(self, data: ReplayDataSource, cfg: ReplayConfig):
        self.data = data
        self.cfg = cfg

        first_ticks = _tick_instants(cfg.start, cfg.tick_times_et)
        self.clock = SimulatedClock(first_ticks[0])

        config: Dict[str, Any] = {
            "dry_run": False,
            "watchlist": list(cfg.symbols),
            "max_orders_per_tick": 20,
        }
        config.update(cfg.engine_config or {})

        self.broker = ReplayBroker(data, config, starting_bp=cfg.starting_bp,
                                   slippage_frac=cfg.slippage_frac)
        self.broker.set_time(first_ticks[0])
        self.db = ReplayDB(self.clock)

        # The wrapper's shared circuit breaker treats a rejected order as a
        # broker failure and pauses after 3 in a row. In replay a rejection is
        # a normal modeled non-fill (the real broker would let the limit order
        # rest instead), so give this run a breaker that can't trip.
        from hermes.broker.circuit_breaker import CircuitBreaker
        from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
        AsyncBrokerWrapper._shared_cb = CircuitBreaker(failure_threshold=1_000_000_000)

        assert isinstance(self.broker, ReplayBroker)
        assert isinstance(self.db, ReplayDB)

        mm = MoneyManager(self.broker, self.db, config)
        ic = IronCondorBuilder(mm)
        common = dict(broker=self.broker, db=self.db, money_manager=mm,
                      ic_builder=ic, config=config, overseer=None,
                      dry_run=False, clock=self.clock)
        all_strategies = make_strategies(common)
        wanted = ({s.upper() for s in cfg.strategies}
                  if cfg.strategies else None)
        strategies = [s for s in all_strategies
                      if wanted is None or s.NAME.upper() in wanted]
        if wanted:
            unknown = wanted - {s.NAME.upper() for s in all_strategies}
            if unknown:
                raise ValueError(f"unknown strategy filter(s): {sorted(unknown)}")

        self.engine = CascadingEngine(
            self.broker, self.db, strategies, overseer=None,
            approval_mode=False, money_manager=mm, config=config,
            event_bus=None, clock=self.clock)

    # ── settlement pricing ────────────────────────────────────────────────────
    def _settlement_value(self, trade: Dict[str, Any]) -> Optional[float]:
        """Per-share closing price of a trade's spread at expiry settlement."""
        exp = trade.get("expiry")
        intrinsics: Dict[str, float] = {}
        for leg_key in ("short_leg", "long_leg"):
            occ = trade.get(leg_key)
            if not occ:
                intrinsics[leg_key] = 0.0
                continue
            info = _parse_occ(occ)
            if info is None:
                intrinsics[leg_key] = 0.0
                continue
            spot = self.data.close_on(info["underlying"], exp or info["expiry"])
            if spot is None:
                return None
            if info["option_type"] == "call":
                intrinsics[leg_key] = max(0.0, spot - info["strike"])
            else:
                intrinsics[leg_key] = max(0.0, info["strike"] - spot)
        if trade.get("entry_credit") is not None and trade.get("entry_debit") is None:
            return round(max(0.0, intrinsics["short_leg"] - intrinsics["long_leg"]), 4)
        return round(max(0.0, intrinsics["long_leg"] - intrinsics["short_leg"]), 4)

    # ── equity marking ────────────────────────────────────────────────────────
    def _mark_open_trade(self, trade: Dict[str, Any]) -> float:
        """Unrealized P&L (dollars) of one OPEN trade at current sim prices."""
        mids: Dict[str, float] = {}
        for leg_key in ("short_leg", "long_leg"):
            occ = trade.get(leg_key)
            q = self.broker._quote_option(occ) if occ else None
            mids[leg_key] = ((q["bid"] + q["ask"]) / 2.0) if q else 0.0
        lots = int(trade.get("lots") or 0)
        close_debit = mids["short_leg"] - mids["long_leg"]
        if trade.get("entry_credit") is not None and trade.get("entry_debit") is None:
            return (float(trade["entry_credit"] or 0) - close_debit) * lots * 100.0
        entry_debit = float(trade.get("entry_debit") or 0)
        return (-close_debit - entry_debit) * lots * 100.0

    def _equity_snapshot(self, ts: datetime) -> Dict[str, Any]:
        per: Dict[str, float] = {}
        for t in self.db.all_trade_rows():
            sid = t["strategy_id"]
            per.setdefault(sid, 0.0)
            if t["status"] == "CLOSED" and t.get("pnl") is not None:
                per[sid] += float(t["pnl"])
            elif t["status"] in ("OPEN", "CLOSING"):
                per[sid] += self._mark_open_trade(t)
        return {"ts": ts, "per_strategy": {k: round(v, 2) for k, v in per.items()},
                "total": round(sum(per.values()), 2)}

    # ── run ───────────────────────────────────────────────────────────────────
    async def run(self) -> ReplayResult:
        days = self.data.trading_days(self.cfg.start, self.cfg.end)
        if not days:
            raise ValueError(
                f"no trading-day bars between {self.cfg.start} and {self.cfg.end}")

        prev_env = os.environ.get(_OFFHOURS_ENV)
        prev_clock = _utils._GLOBAL_CLOCK
        # The submit() market-hours gate checks the *wall* clock; the replay
        # process runs at arbitrary real-world times, so use the documented
        # operator override for the duration of the run. Orders only ever
        # reach the ReplayBroker.
        os.environ[_OFFHOURS_ENV] = "true"
        _utils._GLOBAL_CLOCK = self.clock

        equity_curve: List[Dict[str, Any]] = []
        ticks = 0
        try:
            for day in days:
                for sim_dt in _tick_instants(day, self.cfg.tick_times_et):
                    self.clock.set_time(sim_dt)
                    self.broker.set_time(sim_dt)
                    self.broker.settle_expired()
                    self.db.settle_expired(
                        self.broker._today_et(), self._settlement_value)
                    await self.engine.tick(list(self.cfg.symbols))
                    ticks += 1
                    equity_curve.append(self._equity_snapshot(sim_dt))
        finally:
            if prev_env is None:
                os.environ.pop(_OFFHOURS_ENV, None)
            else:
                os.environ[_OFFHOURS_ENV] = prev_env
            _utils._GLOBAL_CLOCK = prev_clock
            monitor = self.engine.reactive._order_monitor_task
            if monitor is not None and not monitor.done():
                monitor.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor

        trades = self.db.all_trade_rows()
        report = build_report(trades, equity_curve)
        return ReplayResult(trades=trades, fills=list(self.broker.fills),
                            settlements=list(self.broker.settlements),
                            equity_curve=equity_curve, report=report,
                            ticks=ticks)
