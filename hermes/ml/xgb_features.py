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
from datetime import datetime, time as dtime, timedelta, timezone
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
        # Compute SPY returns first, then align — reindexing the price series
        # before pct_change would compute returns across non-adjacent rows
        # whenever the two calendars diverge.
        spy_ret = spy["close"].pct_change().reindex(daily.index)
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
        if intraday is None or intraday.empty:
            return pd.Series(dtype=float, name="last30_pct")
        intraday = intraday.copy()

        # Ensure the index is a DatetimeIndex before accessing .date / .time
        if not isinstance(intraday.index, pd.DatetimeIndex):
            try:
                intraday.index = pd.to_datetime(intraday.index)
            except Exception:
                return pd.Series(dtype=float, name="last30_pct")

        # The 15:30-16:00 window is in US/Eastern. Convert (or assume-localize)
        # the index to ET so the mask matches the regular-session close
        # regardless of how the broker delivered the timestamps.
        idx_et = intraday.index
        if idx_et.tz is None:
            idx_et = idx_et.tz_localize("UTC").tz_convert("America/New_York")
        else:
            idx_et = idx_et.tz_convert("America/New_York")

        times = idx_et.time
        last30_mask = (times >= dtime(15, 30)) & (times < dtime(16, 0))
        # Group on the ET calendar date — using the raw index date would split
        # the closing window across two UTC days in winter sessions.
        et_dates = pd.Index([t.date() for t in idx_et])

        per_day = intraday["volume"].groupby(et_dates).sum()
        per_last30 = intraday["volume"][last30_mask].groupby(et_dates[last30_mask]).sum()
        out = (per_last30 / per_day).rename("last30_pct")
        # Emit a tz-naive DatetimeIndex of ET calendar dates so the result
        # aligns with the (typically naive) daily-bar index in build().
        out.index = pd.DatetimeIndex(pd.to_datetime(out.index))
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
        df["last_30min_volume_pct"] = df["last_30min_volume_pct"].fillna(0)
        df["realized_vol_5d"] = self.realized_vol_5d(daily)
        dow, month = self.seasonality(daily)
        df["day_of_week"] = dow
        df["month"] = month
        df["symbol"] = symbol
        # Halted bars (high == low), zero-variance windows, and zero SPY
        # variance can introduce +/-inf into individual features. XGBoost
        # would happily fit those rows; coerce them to NaN so dropna() drops
        # the whole row instead.
        numeric_cols = df.columns.difference(["symbol"])
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
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
    """Threaded wrapper around the XGBoost engine.
    
    Sleeps most of the day, wakes up to predict at prediction_interval_s.
    Retrains entirely every retrain_interval_s.
    """
    def __init__(self, db: Any, feat: FeatureEngineer, broker: Any,
                 watchlist: Sequence[str],
                 retrain_interval_s: float = 7 * 24 * 3600,
                 predict_interval_s: float = 24 * 3600,
                 model_dir: Optional[Path] = None):
        self.db = db
        self.feat = feat
        self.broker = broker
        self.symbols = list(watchlist)
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
        immediately useful before the first retrain cycle completes.

        Scans the model directory rather than iterating the constructor
        watchlist — strategy watchlists can add symbols beyond ``self.symbols``,
        and those models also need to warm-start after a restart.
        """
        try:
            self._model_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning("Cannot create model dir %s: %s", self._model_dir, exc)
            return
        for p in sorted(self._model_dir.glob("xgb_*.pkl")):
            sym = p.stem[len("xgb_"):]
            if not sym:
                continue
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
        from hermes.market_hours import ET
        from datetime import datetime
        
        last_train = 0.0
        last_pred_tuple = None
        
        while not self._stop.is_set():
            try:
                now = time.time()
                now_et = datetime.now(ET)
                force_run = (self.db.get_setting("ml_force_run") == "true")
                
                should_predict = force_run
                if not should_predict:
                    hour = now_et.hour
                    # 9 AM to 4 PM (16:00) on weekdays
                    if now_et.weekday() < 5 and 9 <= hour <= 16:
                        current_tuple = (now_et.year, now_et.month, now_et.day, hour)
                        if last_pred_tuple != current_tuple:
                            should_predict = True
                            last_pred_tuple = current_tuple
                            
                should_retrain = force_run or (now - last_train > self.retrain_interval)
                
                if should_predict or should_retrain:
                    # Sync history before running models
                    self._sync_history()
                    
                    retrain_warnings = []
                    predict_warnings = []
                    
                    if should_retrain:
                        retrain_warnings = self._retrain_all()
                        last_train = now
                        
                    # Always predict if we just retrained or if the hourly schedule hit
                    predict_warnings = self._predict_all()
                    
                    if force_run:
                        try:
                            self.db.set_setting("ml_force_run", "false")
                        except Exception:
                            pass
                    
                    try:
                        from datetime import timezone
                        self.db.set_setting("ml_last_ok_ts", datetime.now(timezone.utc).isoformat())
                        
                        all_warns = retrain_warnings + predict_warnings
                        if all_warns:
                            # If we had warnings, surface them so the user knows why output is missing
                            self.db.set_setting("ml_last_error", "; ".join(all_warns)[:500])
                        else:
                            self.db.set_setting("ml_last_error", "")
                    except Exception:                               # noqa: BLE001
                        pass
            except Exception as exc:                                   # noqa: BLE001
                logger.exception("xgb loop error: %s", exc)
                try:
                    self.db.set_setting("ml_last_error", str(exc)[:500])
                except Exception:                               # noqa: BLE001
                    pass
                    
            # Wake up every 10s to check for schedule or manual triggers
            self._stop.wait(10)

    def _get_active_symbols(self) -> list[str]:
        active = set(self.symbols)
        active.add("SPY") # Essential for beta residual
        
        try:
            strategy_lists = self.db.list_all_watchlists()
            for sym_list in strategy_lists.values():
                active.update(sym_list)
        except Exception as e:
            logger.warning(f"Failed to fetch strategy watchlists: {e}")
            
        return list(active)

    def _sync_history(self) -> None:
        """Fetch missing daily and intraday history from the broker and save to db."""
        if not hasattr(self.db, 'save_daily_bars'):
            logger.warning("HermesDB missing save_daily_bars method. Cannot sync history.")
            return

        from datetime import date, timedelta
        end_date = date.today()
        start_date = end_date - timedelta(days=400)
        
        symbols_to_sync = self._get_active_symbols()
        
        for sym in symbols_to_sync:
            try:
                # 1. Sync Daily Bars
                daily_bars = self.broker.get_history(sym, interval="daily", start=start_date.isoformat(), end=end_date.isoformat())
                if daily_bars:
                    # Convert to dataframe
                    df_daily = pd.DataFrame(daily_bars)
                    if not df_daily.empty:
                        # Rename 'date' to 'ts' if necessary
                        if 'date' in df_daily.columns:
                            df_daily = df_daily.rename(columns={'date': 'ts'})
                        # Add missing vwap_close as approximation
                        if 'vwap_close' not in df_daily.columns and 'close' in df_daily.columns:
                            df_daily['vwap_close'] = df_daily['close']
                        self.db.save_daily_bars(sym, df_daily)
                
                # 2. Sync Intraday Bars
                # Tradier history endpoint doesn't really support intraday easily via get_history 
                # (requires timesales), but we attempt a fallback to 1min or 5min if possible.
                try:
                    intra_start = end_date - timedelta(days=10)
                    intra_bars = self.broker.get_history(sym, interval="1min", start=intra_start.isoformat(), end=end_date.isoformat())
                    if intra_bars:
                        df_intra = pd.DataFrame(intra_bars)
                        if not df_intra.empty:
                            if 'date' in df_intra.columns:
                                df_intra = df_intra.rename(columns={'date': 'ts'})
                            self.db.save_intraday_bars(sym, df_intra)
                except Exception as e:
                    logger.debug(f"Failed to sync intraday history for {sym}: {e}")

            except Exception as e:
                logger.error(f"Failed to sync history for {sym}: {e}")
                
        logger.info("History sync complete.")

    def _retrain_all(self) -> list[str]:
        warnings = []
        try:
            import xgboost as xgb  # imported lazily so dev env doesn't require it
        except ImportError:
            msg = "xgboost not installed; skipping retrain."
            logger.warning(msg)
            return [msg]
        
        trained_count = 0
        active_symbols = self._get_active_symbols()
        for sym in active_symbols:
            data = self._feature_frame(sym)
            if data is None:
                warnings.append(f"{sym}: No data")
                continue
            if len(data) < 60:
                warnings.append(f"{sym}: Need 60 bars, got {len(data)}")
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
            trained_count += 1
            
        if trained_count == 0 and self.symbols:
            warnings.insert(0, "No models trained")
        return warnings

    def _predict_all(self) -> list[str]:
        warnings = []
        predicted_count = 0
        for sym, model in self._models.items():
            data = self._feature_frame(sym, drop_target=True)
            if data is None or data.empty:
                continue
            x_last = data.iloc[[-1]]
            yhat = float(model.predict(x_last)[0])
            spot = float(self.db.last_price(sym) or 0.0)
            predicted_price = round(spot * (1 + yhat), 4)
            self._last_pred[sym] = {
                "asof": datetime.now(timezone.utc),
                "predicted_return": yhat,
                "predicted_price": predicted_price,
                "spot": spot,
            }
            self.db.write_prediction(sym, yhat, predicted_price, spot)
            predicted_count += 1
            
        active_symbols = self._get_active_symbols()
        if not self._models and active_symbols:
            warnings.append("No models available to predict")
            
        return warnings

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
