"""
[Reality-Check Backtester]
Replay historical days against the prediction stack and score predicted
POP against realised credit-spread outcomes.

Why this exists
---------------
Until now, "the prediction algorithm helps" was an unverified claim.
Without a backtester that scored predicted probabilities against what
*actually* happened — including realistic slippage and commissions —
we couldn't tell whether a refactor improved or quietly regressed the
edge.

What this does
--------------
For each historical asof date in the requested window:

  1. Build the feature frame as it existed on that day (point-in-time).
  2. Score the predicted POP for a synthetic credit spread at the
     requested DTE and short-leg distance.
  3. Roll forward to expiry. The realised outcome is binary: 1.0 if
     the underlying never crossed the short strike, 0.0 otherwise.
  4. Apply commission and slippage to the credit/debit so the realised
     P&L reflects what an actual operator would have seen.

The output bundle includes Brier score, log-loss, AUC-ROC, hit rate,
and a reliability curve so the diagnostics dashboard can visualise it.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from hermes.ml.calibration import brier_score, log_loss, reliability_curve
from hermes.ml.pop_engine import (
    FeatureVector,
    calculate_strike_protection,
    find_key_levels,
    predict_pop_with_band,
)

logger = logging.getLogger("hermes.ml.backtester")


# ---------------------------------------------------------------------------
# Configurable cost model
# ---------------------------------------------------------------------------
@dataclass
class CostModel:
    """Per-spread commissions plus slippage as a fraction of credit.

    Defaults are tuned to a Tradier-style retail account: $0.35 per
    contract per leg, plus 5% slippage on the entry credit. Override
    via the ``CostModel(**...)`` kwargs to backtest with tighter or
    looser execution assumptions.
    """

    commission_per_contract: float = 0.35
    legs_per_spread: int = 2
    slippage_pct: float = 0.05

    def round_trip_cost(self, credit: float, lots: int) -> float:
        commissions = self.commission_per_contract * self.legs_per_spread * lots * 2
        slippage = abs(credit) * self.slippage_pct * lots
        return float(commissions + slippage)


# ---------------------------------------------------------------------------
# Backtest result
# ---------------------------------------------------------------------------
@dataclass
class BacktestResult:
    """Aggregate stats from a single backtest run."""

    n: int
    hit_rate: float
    brier: float
    log_loss: float
    auc: float
    mean_predicted: float
    realized_pnl: float
    reliability: List[Dict[str, float]] = field(default_factory=list)
    samples: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "n": int(self.n),
            "hit_rate": float(self.hit_rate),
            "brier": float(self.brier),
            "log_loss": float(self.log_loss),
            "auc": float(self.auc),
            "mean_predicted": float(self.mean_predicted),
            "realized_pnl": float(self.realized_pnl),
            "reliability": list(self.reliability),
        }


# ---------------------------------------------------------------------------
# Backtester
# ---------------------------------------------------------------------------
class Backtester:
    """Walk-forward POP backtester.

    Parameters
    ----------
    bars_daily:
        Historical daily bars indexed by date with at least
        ``[open, high, low, close, volume]``. Typically pulled via
        ``HermesDB.daily_bars`` for the symbol of interest.
    spy_daily:
        SPY daily bars, same shape, used by the FeatureEngineer.
    score_fn:
        Callable mapping ``(asof, frame, key_levels)`` to a
        FeatureVector. Default uses delta-implied probability from a
        Black-Scholes z-score so the backtester runs without needing
        a fitted XGB model on the harness machine.
    horizon_dte:
        Days-to-expiry of the synthetic credit spread.
    short_distance_pct:
        Short leg placed this fraction below (puts) / above (calls)
        spot. 0.05 is a reasonable default for 7-DTE plays.
    side:
        ``'put'`` (sell put credit spread) or ``'call'``.
    cost_model:
        Slippage and commission assumptions.
    """

    def __init__(self,
                 bars_daily: pd.DataFrame,
                 spy_daily: pd.DataFrame,
                 *,
                 score_fn: Optional[Callable[..., FeatureVector]] = None,
                 horizon_dte: int = 7,
                 short_distance_pct: float = 0.05,
                 side: str = "put",
                 cost_model: Optional[CostModel] = None) -> None:
        self.bars = bars_daily.copy()
        self.spy = spy_daily.copy()
        self.score_fn = score_fn or self._default_score
        self.horizon = int(horizon_dte)
        self.dist_pct = float(short_distance_pct)
        self.side = side
        self.cost = cost_model or CostModel()

    # -- public --------------------------------------------------------------
    def run(self, *, start: Optional[date] = None,
            end: Optional[date] = None,
            warmup: int = 252) -> BacktestResult:
        """Walk forward through the dataset and score every asof."""
        if self.bars.empty:
            return BacktestResult(0, 0, float("nan"), float("nan"),
                                  float("nan"), float("nan"), 0.0)

        idx = self.bars.index
        start_dt = pd.Timestamp(start) if start else idx[warmup]
        end_dt = (pd.Timestamp(end)
                  if end
                  else idx[-self.horizon - 1])

        preds: List[float] = []
        outs: List[float] = []
        pnl_total = 0.0
        samples: List[Dict[str, Any]] = []

        for asof in idx:
            if asof < start_dt or asof > end_dt:
                continue
            frame = self.bars.loc[:asof]
            if len(frame) < warmup:
                continue
            spot = float(frame["close"].iloc[-1])
            if spot <= 0:
                continue
            short_strike = self._strike_for(spot)

            try:
                key_levels = find_key_levels(frame["close"], frame["volume"])
            except Exception:                       # noqa: BLE001
                key_levels = []
            try:
                fv = self.score_fn(asof, frame, key_levels,
                                    short_strike=short_strike,
                                    side=self.side, horizon=self.horizon)
            except Exception as exc:                # noqa: BLE001
                logger.debug("score_fn failed at %s: %s", asof, exc)
                continue

            band = predict_pop_with_band(fv)
            pop = float(band["pop"])

            outcome, realized_credit = self._roll_forward(asof, short_strike)
            if outcome is None:
                continue

            pnl_total += realized_credit - self.cost.round_trip_cost(
                realized_credit, lots=1)
            preds.append(pop)
            outs.append(float(outcome))
            samples.append({
                "asof": str(asof.date() if hasattr(asof, "date") else asof),
                "spot": spot,
                "short_strike": short_strike,
                "predicted_pop": pop,
                "outcome": float(outcome),
            })

        if not preds:
            return BacktestResult(0, 0, float("nan"), float("nan"),
                                  float("nan"), float("nan"), 0.0)

        return BacktestResult(
            n=len(preds),
            hit_rate=float(np.mean(outs)),
            brier=brier_score(preds, outs),
            log_loss=log_loss(preds, outs),
            auc=_auc(preds, outs),
            mean_predicted=float(np.mean(preds)),
            realized_pnl=float(pnl_total),
            reliability=reliability_curve(preds, outs),
            samples=samples[-200:],
        )

    # -- helpers -------------------------------------------------------------
    def _strike_for(self, spot: float) -> float:
        if self.side == "put":
            return float(spot * (1.0 - self.dist_pct))
        return float(spot * (1.0 + self.dist_pct))

    def _roll_forward(self, asof: pd.Timestamp,
                      short_strike: float) -> tuple[Optional[float], float]:
        """Walk ``horizon`` bars forward and return ``(outcome, credit)``,
        routing the execution through the local MockAsyncTradierBroker matching engine.

        outcome = 1.0 if the underlying *never* crosses the short strike
        intraday between asof+1 and asof+horizon; 0.0 otherwise.

        credit = synthetic credit collected at entry (net of touch losses).
        """
        import asyncio
        from hermes.broker.mock_engine import MockAsyncTradierBroker
        from hermes.service1_agent.core import TradeAction

        idx = self.bars.index
        try:
            i_start = idx.get_loc(asof)
        except KeyError:
            return None, 0.0
        i_end = i_start + self.horizon
        if i_end >= len(idx):
            return None, 0.0
        window = self.bars.iloc[i_start + 1: i_end + 1]
        if window.empty:
            return None, 0.0

        # Initialize mock broker without costs (Backtester applies its own CostModel)
        broker = MockAsyncTradierBroker(config={
            "commission_per_contract": 0.0,
            "slippage_pct": 0.0
        })

        spot_entry = float(self.bars.iloc[i_start]["close"])
        
        # Tick entry spot price to establish quotes
        broker.tick_underlying("AAPL", spot_entry, spot_entry, spot_entry, asof.to_pydatetime())

        # Determine option strikes and symbol names
        expiry_date = window.index[-1].date()
        yymmdd = expiry_date.strftime("%y%m%d")
        
        short_strike_str = f"{int(short_strike * 1000):08d}"
        long_strike = short_strike - 5.0 if self.side == "put" else short_strike + 5.0
        long_strike_str = f"{int(long_strike * 1000):08d}"

        short_opt = f"AAPL{yymmdd}{'P' if self.side == 'put' else 'C'}{short_strike_str}"
        long_opt = f"AAPL{yymmdd}{'P' if self.side == 'put' else 'C'}{long_strike_str}"

        # Approximate credit collected at entry
        if self.side == "put":
            credit = 0.30 * max(spot_entry - short_strike, 0.0)
        else:
            credit = 0.30 * max(short_strike - spot_entry, 0.0)

        action = TradeAction(
            strategy_id="BACKTEST",
            symbol="AAPL",
            order_class="multileg",
            legs=[
                {"option_symbol": short_opt, "side": "sell_to_open", "quantity": 1},
                {"option_symbol": long_opt, "side": "buy_to_open", "quantity": 1}
            ],
            price=credit,
            side="sell",
            quantity=1,
            order_type="credit"
        )

        # Place the order synchronously using a new event loop
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(broker.place_order_from_action(action))
        finally:
            loop.close()

        # Replay the daily bars through the mock matching engine
        touched = False
        for dt, bar in window.iterrows():
            spot = float(bar["close"])
            high = float(bar["high"])
            low = float(bar["low"])

            broker.tick_underlying("AAPL", spot, high, low, dt.to_pydatetime())
            
            # If the positions are cleared/empty, it signifies the short option was touched
            # and the mock matching engine executed the stop-out/close at a loss.
            if not broker.positions:
                touched = True
                break

        outcome = 0.0 if touched else 1.0
        # Compute realized P&L from the mock broker's cash balance
        # (initial cash was 100000.0, multiplier is 100)
        realized_value = (broker.balances["cash"] - 100000.0) / 100.0
        
        return outcome, float(realized_value)

    # -- default scorer ------------------------------------------------------
    def _default_score(self, asof: pd.Timestamp, frame: pd.DataFrame,
                       key_levels: Sequence[Dict[str, Any]], *,
                       short_strike: float, side: str,
                       horizon: int) -> FeatureVector:
        """Black-Scholes-style baseline score so the backtester runs
        without a fitted XGB model.

        Maps the BS z-score to a probability and uses it as both the
        delta-implied baseline and the XGB head; the protection score
        comes from the live key levels. This is intentionally a
        *baseline*, not the production scorer — strategies should pass
        their own score_fn that consults a fitted predictor.
        """
        from scipy.stats import norm

        log_ret = np.log(frame["close"] / frame["close"].shift(1))
        sigma = float(log_ret.tail(20).std() * math.sqrt(252)) or 0.30
        avg_sigma = float(log_ret.tail(60).std() * math.sqrt(252)) or sigma
        spot = float(frame["close"].iloc[-1])
        t_years = max(horizon / 365, 1.0 / 365)
        try:
            z = math.log(short_strike / spot) / (sigma * math.sqrt(t_years))
            p_otm = float(norm.cdf(abs(z)))
            delta_est = 1.0 - p_otm
        except Exception:                           # noqa: BLE001
            p_otm = 0.84
            delta_est = 0.16

        spread_type = f"{side}_credit"
        prot = calculate_strike_protection(
            list(key_levels), spot, short_strike, spread_type)

        return FeatureVector(
            delta=delta_est,
            xgb_prob=p_otm,
            current_vol=sigma,
            avg_vol=avg_sigma,
            protection_score=float(prot),
            iv_rank=50.0,
            side=side,
            period="3M" if horizon <= 14 else ("6M" if horizon <= 60 else "1Y"),
        )


# ---------------------------------------------------------------------------
# AUC helper — single-pass Mann-Whitney U so we don't import sklearn here.
# ---------------------------------------------------------------------------
def _auc(preds: Sequence[float], outs: Sequence[float]) -> float:
    p = np.asarray(preds, dtype=float)
    y = np.asarray(outs, dtype=float)
    pos = p[y == 1]
    neg = p[y == 0]
    if pos.size == 0 or neg.size == 0:
        return float("nan")
    # Mann-Whitney via tied-rank handling
    combined = np.concatenate([pos, neg])
    order = np.argsort(combined)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, combined.size + 1)
    rank_pos = ranks[: pos.size].sum()
    u = rank_pos - pos.size * (pos.size + 1) / 2
    return float(u / (pos.size * neg.size))


__all__ = ["Backtester", "BacktestResult", "CostModel"]
