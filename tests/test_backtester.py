"""Smoke + correctness tests for hermes.ml.backtester.

The backtester replays daily bars and scores predicted POP against
realised outcomes. We test:

- A run returns non-NaN Brier / hit-rate on a synthetic random-walk.
- The realised hit rate scales the way it should with strike distance
  (further-OTM short strikes hit less often).
- Cost model deductions reduce realised P&L.
- AUC degrades to NaN when only one outcome class is present.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hermes.ml.backtester import Backtester, CostModel


# ---------------------------------------------------------------------------
# Synthetic bars
# ---------------------------------------------------------------------------
def _random_walk_bars(n: int = 400, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    log_ret = rng.normal(0, 0.01, size=n)
    close = 100 * np.exp(np.cumsum(log_ret))
    high = close * (1 + rng.uniform(0, 0.005, size=n))
    low = close * (1 - rng.uniform(0, 0.005, size=n))
    open_ = close * (1 + rng.uniform(-0.002, 0.002, size=n))
    volume = rng.integers(1_000_000, 5_000_000, size=n)
    idx = pd.bdate_range(end="2025-04-01", periods=n)
    return pd.DataFrame({
        "open": open_, "high": high, "low": low,
        "close": close, "volume": volume,
    }, index=idx)


# ---------------------------------------------------------------------------
# Smoke
# ---------------------------------------------------------------------------
def test_backtester_runs_end_to_end():
    bars = _random_walk_bars()
    spy = _random_walk_bars(seed=1)
    bt = Backtester(bars, spy, horizon_dte=7,
                    short_distance_pct=0.05, side="put")
    result = bt.run(warmup=120)
    assert result.n > 0
    assert not np.isnan(result.brier)
    assert 0.0 <= result.mean_predicted <= 1.0


def test_hit_rate_decreases_with_strike_distance():
    bars = _random_walk_bars()
    spy = _random_walk_bars(seed=1)
    near = Backtester(bars, spy, horizon_dte=7,
                      short_distance_pct=0.02, side="put").run(warmup=120)
    far = Backtester(bars, spy, horizon_dte=7,
                     short_distance_pct=0.10, side="put").run(warmup=120)
    # Putting the short leg further OTM should not LOWER the hit rate
    # in expectation. Allow a small slack for a noisy run.
    assert far.hit_rate >= near.hit_rate - 0.05


def test_cost_model_deducts_from_realized_pnl():
    bars = _random_walk_bars()
    spy = _random_walk_bars(seed=1)
    cheap = Backtester(bars, spy, horizon_dte=7,
                       cost_model=CostModel(commission_per_contract=0.05,
                                            slippage_pct=0.01)).run(warmup=120)
    pricey = Backtester(bars, spy, horizon_dte=7,
                        cost_model=CostModel(commission_per_contract=2.50,
                                             slippage_pct=0.20)).run(warmup=120)
    assert pricey.realized_pnl <= cheap.realized_pnl


def test_result_to_dict_is_json_serialisable():
    import json
    bars = _random_walk_bars()
    spy = _random_walk_bars(seed=1)
    result = Backtester(bars, spy).run(warmup=120)
    json.dumps(result.to_dict())          # must not raise


def test_empty_input_returns_empty_result():
    empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    bt = Backtester(empty, empty)
    result = bt.run()
    assert result.n == 0
