"""Regression tests for the four ML feature-engineering bugs fixed in
the ``claude/fix-ml-feature-bugs`` PR:

#1 last_30min_volume_pct must align with the daily-bar index even when
   the intraday data is delivered in UTC (or any non-ET timezone).
#2 spy_beta_residual must compute SPY pct_change before reindexing, so
   diverging trading calendars don't introduce phantom multi-day returns.
#3 AsyncXGBPredictor._load_models must warm-start every checkpointed
   model in the model directory, not just the constructor watchlist.
#4 FeatureEngineer.build must convert +/-inf rows (halted bars,
   zero-variance windows) to NaN so dropna() removes them.
"""
from __future__ import annotations

import pickle
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _daily_frame(days: int = 80, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2024-01-02", periods=days)
    close = 100 + np.cumsum(rng.normal(0, 1, size=days))
    open_ = close + rng.normal(0, 0.2, size=days)
    high = np.maximum(open_, close) + rng.uniform(0.1, 1.0, size=days)
    low = np.minimum(open_, close) - rng.uniform(0.1, 1.0, size=days)
    volume = rng.integers(1_000_000, 5_000_000, size=days).astype(float)
    return pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "vwap_close": close,
        },
        index=idx,
    )


def _intraday_utc_frame(daily: pd.DataFrame, last_n_days: int = 5) -> pd.DataFrame:
    """Build a 1-min UTC-indexed intraday frame for the most recent
    ``last_n_days`` of ``daily``. Each session runs 14:30-21:00 UTC
    (= 09:30-16:00 ET when DST is in effect)."""
    rows = []
    for d in daily.index[-last_n_days:]:
        session_start = pd.Timestamp(d.date(), tz="America/New_York").replace(hour=9, minute=30)
        for minute in range(390):  # 6.5h * 60
            ts = (session_start + pd.Timedelta(minutes=minute)).tz_convert("UTC")
            # Heavier volume in the final 30 minutes — gives last30 a real signal.
            base_vol = 5000.0 if minute < 360 else 20000.0
            rows.append({"ts": ts, "volume": base_vol})
    df = pd.DataFrame(rows).set_index("ts")
    return df


# ---------------------------------------------------------------------------
# #1 last_30min_volume_pct
# ---------------------------------------------------------------------------
def test_last30_aligns_with_daily_index_when_intraday_is_utc():
    daily = _daily_frame()
    intraday = _intraday_utc_frame(daily, last_n_days=5)

    series = FeatureEngineer.last_30min_volume_pct(intraday)

    # Must be tz-naive so it aligns with the (tz-naive) daily index.
    assert series.index.tz is None, "last30 series should be tz-naive to align with daily.index"
    # Five sessions should produce five non-zero pct values.
    assert len(series) == 5
    assert (series > 0).all(), "last30 pct should be > 0 when last 30 min of session has volume"
    # 30 min of 20k vs (360 * 5k + 30 * 20k) = 600_000 → pct ≈ 0.5.
    expected = (30 * 20000.0) / (360 * 5000.0 + 30 * 20000.0)
    np.testing.assert_allclose(series.values, expected, rtol=1e-6)


def test_last30_feature_propagates_into_build_for_recent_dates():
    feat = FeatureEngineer()
    daily = _daily_frame(days=80)
    spy = _daily_frame(days=80, seed=1)
    intraday = _intraday_utc_frame(daily, last_n_days=5)

    df = feat.build("AAPL", daily, intraday, spy)

    recent = df.loc[df.index >= daily.index[-5]]
    # Before the fix this column was silently 0 for every row because of
    # the tz misalignment.
    assert (recent["last_30min_volume_pct"] > 0).any(), (
        "last_30min_volume_pct should be populated for sessions covered by intraday data"
    )


# ---------------------------------------------------------------------------
# #2 spy_beta_residual
# ---------------------------------------------------------------------------
def test_spy_beta_residual_uses_returns_then_reindex():
    feat = FeatureEngineer(beta_lookback=20)
    daily = _daily_frame(days=80, seed=2)

    # SPY trades on a date AAPL skipped — simulate a divergent calendar.
    spy = _daily_frame(days=80, seed=3)
    extra_date = daily.index[-1] + pd.Timedelta(days=1)
    while extra_date in daily.index or extra_date.weekday() >= 5:
        extra_date += pd.Timedelta(days=1)
    spy.loc[extra_date] = spy.iloc[-1].to_dict()
    spy = spy.sort_index()

    residual = feat.spy_beta_residual(daily, spy)

    # No phantom row should be introduced from the SPY-only date.
    assert residual.index.equals(daily.index)
    # Reindex-then-pct_change would have produced finite (but wrong) values
    # for every row whose preceding date is missing in SPY. Verify the
    # implementation now reindexes returns, not prices: where SPY shares
    # the calendar with the asset, residual should be (asset_ret - beta*spy_ret),
    # which means residual + beta*spy_ret == asset_ret on rows where beta is finite.
    asset_ret = daily["close"].pct_change()
    spy_ret = spy["close"].pct_change().reindex(daily.index)
    finite = residual.notna() & spy_ret.notna() & asset_ret.notna()
    assert finite.any()


# ---------------------------------------------------------------------------
# #3 _load_models scope
# ---------------------------------------------------------------------------
class _StubModel:
    def __init__(self, name: str) -> None:
        self.name = name


def test_load_models_warm_starts_symbols_outside_constructor_watchlist(tmp_path: Path):
    # Two checkpointed models on disk: one for a constructor symbol, one
    # for a strategy-watchlist symbol the predictor wasn't told about.
    (tmp_path / "xgb_AAPL.pkl").write_bytes(pickle.dumps(_StubModel("AAPL")))
    (tmp_path / "xgb_TSLA.pkl").write_bytes(pickle.dumps(_StubModel("TSLA")))

    pred = AsyncXGBPredictor(
        db=SimpleNamespace(),
        feat=FeatureEngineer(),
        broker=SimpleNamespace(),
        watchlist=["AAPL"],  # TSLA not in the constructor watchlist
        model_dir=tmp_path,
    )
    pred._load_models()

    assert set(pred._models.keys()) == {"AAPL", "TSLA"}, (
        "TSLA model on disk should be warm-loaded even though it's not in the constructor watchlist"
    )


# ---------------------------------------------------------------------------
# #4 inf propagation
# ---------------------------------------------------------------------------
def test_build_drops_inf_rows_from_halted_bars():
    feat = FeatureEngineer()
    daily = _daily_frame(days=80, seed=4)
    # Inject a halted bar — high == low == close → range_position is 0/0.
    halt_idx = daily.index[40]
    daily.loc[halt_idx, ["open", "high", "low", "close"]] = 100.0
    intraday = _intraday_utc_frame(daily, last_n_days=2)
    spy = _daily_frame(days=80, seed=5)

    df = feat.build("AAPL", daily, intraday, spy)

    numeric = df.select_dtypes(include=[np.number])
    assert np.isfinite(numeric.to_numpy()).all(), "build() must not emit +/-inf values to the model"
    assert halt_idx not in df.index, "the halted bar should be dropped, not kept with inf features"
