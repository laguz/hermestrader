"""
[XGBoost-Feature-Engine] — pure feature-engineering layer.

Split out of ``xgb_features.py`` to separate the stateless, fully-testable
feature math from the threaded ``AsyncXGBPredictor`` (lifecycle, DB, model
persistence). Everything here depends only on numpy/pandas/math — no DB, no
threads, no XGBoost — so it can be exercised in isolation.

``xgb_features`` re-exports ``FeatureRow`` / ``FeatureEngineer`` / ``hv_rank``,
so existing imports (``from hermes.ml.xgb_features import FeatureEngineer``)
keep resolving unchanged.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time as dtime

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Feature engineering — unchanged from v1
# ---------------------------------------------------------------------------
@dataclass
class FeatureRow:
    symbol: str
    asof: datetime
    overnight_gap: float
    vol_norm_5d_momentum: float
    spy_beta_residual: float
    intraday_return: float
    vwap_distance: float
    range_position: float
    volume_zscore_20d: float
    last_30min_volume_pct: float
    realized_vol_5d: float
    day_of_week: int
    month: int


class FeatureEngineer:
    """All features defined exactly per spec.

    Inputs:
      bars_daily: DataFrame indexed by date with columns
                  [open, high, low, close, volume, vwap_close]
      bars_intraday: DataFrame indexed by timestamp (1-minute) for last_30min vol
      spy_daily: same shape as bars_daily, for the beta residual.
    """

    def __init__(self, beta_lookback: int = 60, vol_window: int = 20):
        self.beta_lookback = beta_lookback
        self.vol_window = vol_window

    @staticmethod
    def overnight_gap(daily: pd.DataFrame) -> pd.Series:
        return (daily["open"] - daily["close"].shift(1)) / daily["close"].shift(1)

    def vol_norm_5d_momentum(self, daily: pd.DataFrame) -> pd.Series:
        log_ret = np.log(daily["close"] / daily["close"].shift(1))
        std20 = log_ret.rolling(self.vol_window).std()
        return (daily["close"] - daily["close"].shift(5)) / (std20 * daily["close"].shift(5))

    def spy_beta_residual(self, daily: pd.DataFrame, spy: pd.DataFrame) -> pd.Series:
        ret = daily["close"].pct_change()
        # Compute SPY returns first, then align — reindexing the price series
        # before pct_change would compute returns across non-adjacent rows
        # whenever the two calendars diverge.
        spy_ret = spy["close"].pct_change().reindex(daily.index)
        beta = (
            ret.rolling(self.beta_lookback).cov(spy_ret)
            / spy_ret.rolling(self.beta_lookback).var()
        )
        return ret - beta * spy_ret

    @staticmethod
    def intraday_return(daily: pd.DataFrame) -> pd.Series:
        return (daily["close"] - daily["open"]) / daily["open"]

    @staticmethod
    def vwap_distance(daily: pd.DataFrame) -> pd.Series:
        return (daily["close"] - daily["vwap_close"]) / daily["close"]

    @staticmethod
    def range_position(daily: pd.DataFrame) -> pd.Series:
        return (daily["close"] - daily["low"]) / (daily["high"] - daily["low"])

    def volume_zscore(self, daily: pd.DataFrame) -> pd.Series:
        sma = daily["volume"].rolling(self.vol_window).mean()
        sd = daily["volume"].rolling(self.vol_window).std()
        z = (daily["volume"] - sma) / sd.replace(0, np.nan)
        return z.where(sd > 0, 0.0)

    @staticmethod
    def last_30min_volume_pct(intraday: pd.DataFrame) -> pd.Series:
        if intraday is None or intraday.empty:
            return pd.Series(dtype=float, name="last30_pct")
        intraday = intraday.copy()

        if not isinstance(intraday.index, pd.DatetimeIndex):
            try:
                intraday.index = pd.to_datetime(intraday.index)
            except Exception:
                return pd.Series(dtype=float, name="last30_pct")

        idx_et = intraday.index
        if idx_et.tz is None:
            idx_et = idx_et.tz_localize("UTC").tz_convert("America/New_York")
        else:
            idx_et = idx_et.tz_convert("America/New_York")

        times = idx_et.time
        last30_mask = (times >= dtime(15, 30)) & (times < dtime(16, 0))
        et_dates = pd.Index([t.date() for t in idx_et])

        per_day = intraday["volume"].groupby(et_dates).sum()
        per_last30 = intraday["volume"][last30_mask].groupby(et_dates[last30_mask]).sum()
        out = (per_last30 / per_day).rename("last30_pct")
        out.index = pd.DatetimeIndex(pd.to_datetime(out.index))
        return out

    @staticmethod
    def realized_vol_5d(daily: pd.DataFrame) -> pd.Series:
        log_ret = np.log(daily["close"] / daily["close"].shift(1))
        return log_ret.rolling(5).std() * math.sqrt(252)

    @staticmethod
    def seasonality(daily: pd.DataFrame):
        return daily.index.dayofweek, daily.index.month

    def build(self, symbol: str, daily: pd.DataFrame, intraday: pd.DataFrame,
              spy: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame(index=daily.index)
        df["overnight_gap"] = self.overnight_gap(daily)
        df["vol_norm_5d_momentum"] = self.vol_norm_5d_momentum(daily)
        df["spy_beta_residual"] = self.spy_beta_residual(daily, spy)
        df["intraday_return"] = self.intraday_return(daily)
        df["vwap_distance"] = self.vwap_distance(daily)
        df["range_position"] = self.range_position(daily)
        df["volume_zscore_20d"] = self.volume_zscore(daily)
        df["last_30min_volume_pct"] = self.last_30min_volume_pct(intraday)
        df["last_30min_volume_pct"] = df["last_30min_volume_pct"].fillna(0)
        df["realized_vol_5d"] = self.realized_vol_5d(daily)
        dow, month = self.seasonality(daily)
        df["day_of_week"] = dow
        df["month"] = month
        df["symbol"] = symbol
        numeric_cols = df.columns.difference(["symbol"])
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
        return df.dropna()


# ---------------------------------------------------------------------------
# HV Rank — retained for callers that still want a HV proxy.
# ---------------------------------------------------------------------------
def hv_rank(daily: pd.DataFrame, window: int = 252, lookback: int = 365) -> pd.Series:
    log_ret = np.log(daily["close"] / daily["close"].shift(1))
    hv = log_ret.rolling(window).std() * math.sqrt(252)
    rolling_min = hv.rolling(lookback).min()
    rolling_max = hv.rolling(lookback).max()
    rank = (hv - rolling_min) / (rolling_max - rolling_min)
    return rank.fillna(0).clip(0, 1) * 100
