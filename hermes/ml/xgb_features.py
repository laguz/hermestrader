"""
[XGBoost-Feature-Engine]
Computes the 10-feature alpha set the spec mandates and trains/serves an XGBoost
regressor for next-day return / price prediction. Background prediction loop is
threaded so it cannot stall the Human Watcher UI.
"""
from __future__ import annotations

import logging
import math
import pickle
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

logger = logging.getLogger("hermes.ml.xgb")


# ---------------------------------------------------------------------------
# Feature engineering
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

    # 1. Overnight gap
    @staticmethod
    def overnight_gap(daily: pd.DataFrame) -> pd.Series:
        return (daily["open"] - daily["close"].shift(1)) / daily["close"].shift(1)

    # 2. Vol-normalised 5-day momentum
    def vol_norm_5d_momentum(self, daily: pd.DataFrame) -> pd.Series:
        log_ret = np.log(daily["close"] / daily["close"].shift(1))
        std20 = log_ret.rolling(self.vol_window).std()
        return (daily["close"] - daily["close"].shift(5)) / (std20 * daily["close"].shift(5))

    # 3. SPY beta residual
    def spy_beta_residual(self, daily: pd.DataFrame, spy: pd.DataFrame) -> pd.Series:
        ret = daily["close"].pct_change()
        spy_ret = spy["close"].reindex(daily.index).pct_change()
        beta = (
            ret.rolling(self.beta_lookback).cov(spy_ret)
            / spy_ret.rolling(self.beta_lookback).var()
        )
        return ret - beta * spy_ret

    # 4. Intraday return
    @staticmethod
    def intraday_return(daily: pd.DataFrame) -> pd.Series:
        return (daily["close"] - daily["open"]) / daily["open"]

    # 5. VWAP distance at 3:59pm — caller must store closing-period VWAP
    @staticmethod
    def vwap_distance(daily: pd.DataFrame) -> pd.Series:
        return (daily["close"] - daily["vwap_close"]) / daily["close"]

    # 6. Range position
    @staticmethod
    def range_position(daily: pd.DataFrame) -> pd.Series:
        return (daily["close"] - daily["low"]) / (daily["high"] - daily["low"])

    # 7. Volume z-score (20d)
    def volume_zscore(self, daily: pd.DataFrame) -> pd.Series:
        sma = daily["volume"].rolling(self.vol_window).mean()
        sd = daily["volume"].rolling(self.vol_window).std()
        return (daily["volume"] - sma) / sd

    # 8. Last-30-minute volume % of total daily volume
    @staticmethod
    def last_30min_volume_pct(intraday: pd.DataFrame) -> pd.Series:
        intraday = intraday.copy()
        intraday["date"] = intraday.index.date
        last30_mask = intraday.index.time >= dtime(15, 30)
        per_day = intraday.groupby("date")["volume"].sum()
        per_last30 = intraday[last30_mask].groupby("date")["volume"].sum()
        out = (per_last30 / per_day).rename("last30_pct")
        out.index = pd.to_datetime(out.index)
        return out

    # 9. Realised volatility 5d (annualised)
    @staticmethod
    def realized_vol_5d(daily: pd.DataFrame) -> pd.Series:
        log_ret = np.log(daily["close"] / daily["close"].shift(1))
        return log_ret.rolling(5).std() * math.sqrt(252)

    # 10. Seasonality (day-of-week, month)
    @staticmethod
    def seasonality(daily: pd.DataFrame):
        return daily.index.dayofweek, daily.index.month

    # ---- combine -----------------------------------------------------------
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
        df["realized_vol_5d"] = self.realized_vol_5d(daily)
        dow, month = self.seasonality(daily)
        df["day_of_week"] = dow
        df["month"] = month
        df["symbol"] = symbol
        return df.dropna()


# ---------------------------------------------------------------------------
# HV Rank — 365-day rolling Historical Volatility proxy for IV Rank
# ---------------------------------------------------------------------------
def hv_rank(daily: pd.DataFrame, window: int = 252, lookback: int = 365) -> pd.Series:
    """Standard HV (annualised) ranked over a 365-day rolling window."""
    log_ret = np.log(daily["close"] / daily["close"].shift(1))
    hv = log_ret.rolling(window).std() * math.sqrt(252)
    rolling_min = hv.rolling(lookback).min()
    rolling_max = hv.rolling(lookback).max()
    rank = (hv - rolling_min) / (rolling_max - rolling_min)
    return rank.fillna(0).clip(0, 1) * 100


# ---------------------------------------------------------------------------
# Async XGBoost predictor
# ---------------------------------------------------------------------------
# Directory where trained models are checkpointed.  Uses /tmp so it survives
# container-internal restarts but is intentionally ephemeral across cold boots
# (a model trained on stale bars is better than no model on warm restart).
_MODEL_DIR = Path("/tmp/hermes_xgb_models")


class AsyncXGBPredictor:
    """Trains per-symbol XGBoost regressors in a background thread.

    Models are checkpointed to _MODEL_DIR after each retrain and reloaded on
    startup so the predictor is immediately useful even before the first
    retrain_interval elapses.

    Usage:
        p = AsyncXGBPredictor(db, feature_engineer, symbols=[...])
        p.start()                       # non-blocking
        p.predict_latest("AAPL")        # returns last cached prediction
    """

    def __init__(self, db, feat_eng: FeatureEngineer, symbols: Sequence[str],
                 retrain_interval_s: int = 60 * 60, predict_interval_s: int = 60,
                 model_dir: Optional[Path] = None):
        self.db = db
        self.feat = feat_eng
        self.symbols = list(symbols)
        self.retrain_interval = retrain_interval_s
        self.predict_interval = predict_interval_s
        self._model_dir = model_dir or _MODEL_DIR
        self._models: Dict[str, Any] = {}
        self._last_pred: Dict[str, Dict[str, Any]] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- public --------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._load_models()   # warm-start from any checkpointed models
        self._thread = threading.Thread(target=self._loop, name="xgb-predictor", daemon=True)
        self._thread.start()

    def _model_path(self, symbol: str) -> Path:
        return self._model_dir / f"xgb_{symbol}.pkl"

    def _load_models(self) -> None:
        """Load any previously checkpointed models so the predictor is
        immediately useful before the first retrain cycle completes."""
        try:
            self._model_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning("Cannot create model dir %s: %s", self._model_dir, exc)
            return
        for sym in self.symbols:
            p = self._model_path(sym)
            if p.exists():
                try:
                    with p.open("rb") as f:
                        self._models[sym] = pickle.load(f)  # noqa: S301
                    logger.info("Loaded checkpointed xgb model for %s from %s", sym, p)
                except Exception as exc:                     # noqa: BLE001
                    logger.warning("Failed to load model for %s: %s", sym, exc)

    def _save_model(self, symbol: str, model: Any) -> None:
        """Persist a trained model to disk for warm-start on next boot."""
        try:
            self._model_dir.mkdir(parents=True, exist_ok=True)
            p = self._model_path(symbol)
            with p.open("wb") as f:
                pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)
            logger.debug("Checkpointed xgb model for %s → %s", symbol, p)
        except Exception as exc:                             # noqa: BLE001
            logger.warning("Failed to checkpoint model for %s: %s", symbol, exc)

    def stop(self) -> None:
        self._stop.set()

    def predict_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._last_pred.get(symbol)

    # -- background loop -----------------------------------------------------
    def _loop(self) -> None:
        last_train = 0.0
        while not self._stop.is_set():
            try:
                now = time.time()
                if now - last_train > self.retrain_interval:
                    self._retrain_all()
                    last_train = now
                self._predict_all()
            except Exception as exc:                                   # noqa: BLE001
                logger.exception("xgb loop error: %s", exc)
            self._stop.wait(self.predict_interval)

    def _retrain_all(self) -> None:
        try:
            import xgboost as xgb  # imported lazily so dev env doesn't require it
        except ImportError:
            logger.warning("xgboost not installed; skipping retrain.")
            return
        for sym in self.symbols:
            data = self._feature_frame(sym)
            if data is None or len(data) < 60:
                continue
            X, y = data.drop(columns=["target"]), data["target"]
            model = xgb.XGBRegressor(
                n_estimators=400, max_depth=4, learning_rate=0.05,
                subsample=0.85, colsample_bytree=0.8, n_jobs=2,
                objective="reg:squarederror",
            )
            model.fit(X, y)
            self._models[sym] = model
            self._save_model(sym, model)
            logger.info("Trained xgb model for %s (%d rows)", sym, len(X))

    def _predict_all(self) -> None:
        for sym, model in self._models.items():
            data = self._feature_frame(sym, drop_target=True)
            if data is None or data.empty:
                continue
            x_last = data.iloc[[-1]]
            yhat = float(model.predict(x_last)[0])
            spot = float(self.db.last_price(sym) or 0.0)
            predicted_price = round(spot * (1 + yhat), 4)
            self._last_pred[sym] = {
                "asof": datetime.utcnow(),
                "predicted_return": yhat,
                "predicted_price": predicted_price,
                "spot": spot,
            }
            self.db.write_prediction(sym, yhat, predicted_price)

    # -- helpers -------------------------------------------------------------
    def _feature_frame(self, symbol: str, drop_target: bool = False) -> Optional[pd.DataFrame]:
        bars_daily = self.db.daily_bars(symbol, lookback_days=400)
        bars_intraday = self.db.intraday_bars(symbol, lookback_days=10)
        spy_daily = self.db.daily_bars("SPY", lookback_days=400)
        if bars_daily is None or bars_daily.empty or spy_daily is None:
            return None
        feats = self.feat.build(symbol, bars_daily, bars_intraday, spy_daily)
        # drop string column for model input
        feats = feats.drop(columns=[c for c in ("symbol",) if c in feats.columns])
        if drop_target:
            return feats
        target = bars_daily["close"].pct_change().shift(-1).reindex(feats.index)
        feats = feats.assign(target=target).dropna()
        return feats
