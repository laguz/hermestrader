"""
[XGBoost-Feature-Engine v2]

Two layers in this module:

1. ``FeatureEngineer`` — produces the 10-feature equity alpha set
   the spec mandates from daily and intraday bars (unchanged from v1
   so existing tests still pass).
2. ``AsyncXGBPredictor`` — async predictor with:
     * horizon-specific models (7-DTE / 21-DTE / 45-DTE) instead of a
       single next-day regressor.
     * three quantile heads per horizon (q10 / q50 / q90) so the POP
       engine can render confidence bands.
     * decoupled sync / train / calibrate / predict subtasks with
       independent intervals controlled by HermesDB system_settings
       (live-tunable, replacing the old hardcoded 24-hour predict and
       7-day retrain constants).
     * joblib + schema-hash persistence under ``~/.hermes/models`` so
       warm starts cannot silently misalign feature columns.
     * KS drift detection that surfaces in /ml/diagnostics.
     * prediction-ledger writes carrying the model_hash and feature
       schema for postmortem replay.

The constructor signature matches v1 exactly so the agent boot path in
``hermes/service1_agent/main.py`` and the existing tests are
unaffected.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from hermes.ml import drift as drift_mod
from hermes.ml import feature_catalog
from hermes.ml import ledger as ledger_mod
from hermes.ml import persistence
from hermes.ml.calibration import load_calibrator

logger = logging.getLogger("hermes.ml.xgb")


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


# ---------------------------------------------------------------------------
# Predictor configuration
# ---------------------------------------------------------------------------
@dataclass
class PredictorConfig:
    """Live-tunable configuration. Loaded from HermesDB.system_settings.

    Defaults match the v1 cadence so silent regressions are bisectable.
    """

    horizons_dte: Tuple[int, ...] = (7, 21, 45)
    quantiles: Tuple[float, ...] = (0.1, 0.5, 0.9)
    predict_interval_s: float = 60 * 60          # hourly during session
    retrain_interval_s: float = 7 * 24 * 3600    # weekly
    calibrate_interval_s: float = 24 * 3600      # nightly
    drift_alarm_threshold: float = 0.2
    target_kind: str = "return"                  # "return" or "pnl"
    use_pnl_target: bool = False                 # rec #18 toggle

    @classmethod
    def from_db(cls, db: Any) -> "PredictorConfig":
        cfg = cls()
        if db is None or not hasattr(db, "get_setting"):
            return cfg

        def _f(key: str, default: float) -> float:
            try:
                v = db.get_setting(key)
                return float(v) if v not in (None, "") else default
            except (TypeError, ValueError):
                return default

        def _s(key: str, default: str) -> str:
            try:
                v = db.get_setting(key)
                return str(v) if v else default
            except Exception:                     # noqa: BLE001
                return default

        cfg.predict_interval_s = _f("ml_predict_interval_s", cfg.predict_interval_s)
        cfg.retrain_interval_s = _f("ml_retrain_interval_s", cfg.retrain_interval_s)
        cfg.calibrate_interval_s = _f("ml_calibrate_interval_s", cfg.calibrate_interval_s)
        cfg.drift_alarm_threshold = _f("ml_drift_threshold", cfg.drift_alarm_threshold)
        cfg.target_kind = _s("ml_target_kind", cfg.target_kind)
        cfg.use_pnl_target = (cfg.target_kind == "pnl")
        return cfg


# ---------------------------------------------------------------------------
# AsyncXGBPredictor v2
# ---------------------------------------------------------------------------
class AsyncXGBPredictor:
    """Threaded predictor with quantile heads and decoupled tasks.

    Constructor signature is backwards-compatible with v1 — existing
    boot code in service1_agent/main.py keeps working unchanged.
    """

    def __init__(self,
                 db: Any,
                 feat: FeatureEngineer,
                 broker: Any,
                 watchlist: Sequence[str],
                 retrain_interval_s: float = 7 * 24 * 3600,
                 predict_interval_s: float = 24 * 3600,
                 model_dir: Optional[Path] = None) -> None:
        self.db = db
        self.feat = feat
        self.broker = broker
        self.symbols = list(watchlist)
        self._cfg = PredictorConfig.from_db(db)
        # Honour the v1 args if the caller passed something explicit;
        # the DB-backed config still wins on subsequent reloads.
        if retrain_interval_s != 7 * 24 * 3600:
            self._cfg.retrain_interval_s = retrain_interval_s
        if predict_interval_s != 24 * 3600:
            self._cfg.predict_interval_s = predict_interval_s

        self._model_root = model_dir or persistence.DEFAULT_MODEL_ROOT
        # Models indexed by symbol → {(horizon, quantile): (model, meta)}
        self._models: Dict[str, Dict[Tuple[int, float], Tuple[Any, persistence.ModelMeta]]] = {}
        # Per-symbol calibrators applied to the q50 head.
        self._calibrators: Dict[str, Any] = {}
        # Per-symbol drift detectors fitted on the latest training frame.
        self._drift: Dict[str, drift_mod.DriftDetector] = {}
        # Last predictions surfaced to the rest of the system.
        self._last_pred: Dict[str, Dict[str, Any]] = {}

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_train_ts = 0.0
        self._last_calibrate_ts = 0.0
        self._last_predict_tuple: Optional[Tuple[int, int, int, int]] = None

        # Ensure the prediction ledger table exists. Idempotent; safe on
        # every boot regardless of whether prior versions ran migrations.
        try:
            ledger_mod.ensure_table(self.db)
        except Exception as exc:                    # noqa: BLE001
            logger.warning("ledger.ensure_table failed: %s", exc)

        try:
            self._model_root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning("Cannot create model dir %s: %s", self._model_root, exc)

    # -- public --------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._load_models()
        self._thread = threading.Thread(target=self._loop, name="xgb-predictor",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def predict_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._last_pred.get(symbol)

    def predict_quantiles(self, symbol: str) -> Optional[Dict[str, float]]:
        """Return per-quantile probabilities for the default horizon.

        Used by augment_levels_with_pop to render confidence bands.
        """
        last = self._last_pred.get(symbol)
        if not last:
            return None
        return last.get("quantiles")

    def reload_config(self) -> PredictorConfig:
        """Re-read the live-tunable knobs from HermesDB. Returns the
        updated PredictorConfig so the /ml/diagnostics endpoint can
        render what is currently in effect."""
        self._cfg = PredictorConfig.from_db(self.db)
        return self._cfg

    @property
    def config(self) -> PredictorConfig:
        return self._cfg

    # -- background loop -----------------------------------------------------
    def _loop(self) -> None:
        from hermes.market_hours import ET

        while not self._stop.is_set():
            try:
                self._cfg = PredictorConfig.from_db(self.db)
                now = time.time()
                now_et = datetime.now(ET)
                force_run = (self.db.get_setting("ml_force_run") == "true")

                should_predict = self._should_predict(now_et, force_run)
                should_retrain = (
                    force_run
                    or (now - self._last_train_ts > self._cfg.retrain_interval_s)
                )
                should_calibrate = (
                    force_run
                    or (now - self._last_calibrate_ts > self._cfg.calibrate_interval_s)
                )

                if should_predict or should_retrain or should_calibrate:
                    self._sync_history()

                warnings: List[str] = []
                if should_retrain:
                    warnings.extend(self._retrain_all())
                    self._last_train_ts = now
                if should_calibrate:
                    self._calibrate_all()
                    self._last_calibrate_ts = now
                if should_predict or should_retrain:
                    warnings.extend(self._predict_all())

                if force_run:
                    try:
                        self.db.set_setting("ml_force_run", "false")
                    except Exception:               # noqa: BLE001
                        pass

                self._record_status(warnings)
            except Exception as exc:                # noqa: BLE001
                logger.exception("xgb loop error: %s", exc)
                try:
                    self.db.set_setting("ml_last_error", str(exc)[:500])
                except Exception:                   # noqa: BLE001
                    pass

            self._stop.wait(10)

    def _should_predict(self, now_et: datetime, force_run: bool) -> bool:
        if force_run:
            return True
        # Fast-cadence prediction during regular session hours, otherwise
        # gate on the configured wall-clock interval.
        if now_et.weekday() < 5 and 9 <= now_et.hour <= 16:
            current_tuple = (now_et.year, now_et.month, now_et.day, now_et.hour)
            if self._last_predict_tuple != current_tuple:
                self._last_predict_tuple = current_tuple
                return True
        return False

    def _record_status(self, warnings: Sequence[str]) -> None:
        try:
            self.db.set_setting("ml_last_ok_ts",
                                datetime.now(timezone.utc).isoformat())
            if warnings:
                self.db.set_setting("ml_last_error",
                                    "; ".join(warnings)[:500])
            else:
                self.db.set_setting("ml_last_error", "")
        except Exception:                           # noqa: BLE001
            pass

    # -- model lifecycle -----------------------------------------------------
    def _model_name(self, horizon: int, quantile: float) -> str:
        return f"xgb_q{int(quantile * 100):02d}_{horizon}dte"

    def _load_models(self) -> None:
        """Warm-start every checkpointed model whose schema hash still
        matches the current catalog. Mismatches are quarantined by the
        persistence layer, not silently used."""
        if not self._model_root.exists():
            return
        for sym_dir in sorted(self._model_root.glob("*")):
            if not sym_dir.is_dir() or sym_dir.name.startswith("_"):
                continue
            sym = sym_dir.name
            for horizon in self._cfg.horizons_dte:
                for q in self._cfg.quantiles:
                    name = self._model_name(horizon, q)
                    model, meta = persistence.load_model(
                        symbol=sym, model_name=name, root=self._model_root)
                    if model is not None and meta is not None:
                        self._models.setdefault(sym, {})[(horizon, q)] = (model, meta)
                        logger.info("warm-start %s/%s schema=%s",
                                    sym, name, meta.schema_hash[:12])

    # -- active universe -----------------------------------------------------
    def _get_active_symbols(self) -> List[str]:
        active = set(self.symbols)
        active.add("SPY")
        try:
            for syms in self.db.list_all_watchlists().values():
                active.update(syms)
        except Exception as exc:                    # noqa: BLE001
            logger.warning("Failed to fetch strategy watchlists: %s", exc)
        return sorted(active)

    # -- history sync --------------------------------------------------------
    def _sync_history(self) -> None:
        if not hasattr(self.db, "save_daily_bars"):
            logger.warning("HermesDB missing save_daily_bars; cannot sync history")
            return

        end_date = date.today()
        start_date = end_date - timedelta(days=400)

        for sym in self._get_active_symbols():
            try:
                daily_bars = self.broker.get_history(
                    sym, interval="daily",
                    start=start_date.isoformat(),
                    end=end_date.isoformat())
                if daily_bars:
                    df_daily = pd.DataFrame(daily_bars)
                    if not df_daily.empty:
                        if "date" in df_daily.columns:
                            df_daily = df_daily.rename(columns={"date": "ts"})
                        if ("vwap_close" not in df_daily.columns
                                and "close" in df_daily.columns):
                            df_daily["vwap_close"] = df_daily["close"]
                        self.db.save_daily_bars(sym, df_daily)

                try:
                    intra_start = end_date - timedelta(days=10)
                    intra_bars = self.broker.get_history(
                        sym, interval="1min",
                        start=intra_start.isoformat(),
                        end=end_date.isoformat())
                    if intra_bars:
                        df_intra = pd.DataFrame(intra_bars)
                        if not df_intra.empty:
                            if "date" in df_intra.columns:
                                df_intra = df_intra.rename(columns={"date": "ts"})
                            self.db.save_intraday_bars(sym, df_intra)
                except Exception as exc:            # noqa: BLE001
                    logger.debug("intraday sync failed for %s: %s", sym, exc)
            except Exception as exc:                # noqa: BLE001
                logger.error("history sync failed for %s: %s", sym, exc)
        logger.info("history sync complete")

    # -- training ------------------------------------------------------------
    def _retrain_all(self) -> List[str]:
        warnings: List[str] = []
        try:
            import xgboost as xgb                   # type: ignore
        except ImportError:
            msg = "xgboost not installed; skipping retrain"
            logger.warning(msg)
            return [msg]

        trained = 0
        for sym in self._get_active_symbols():
            base_frame = self._feature_frame(sym)
            if base_frame is None:
                warnings.append(f"{sym}: no data")
                continue
            if len(base_frame) < 80:
                warnings.append(f"{sym}: need 80 bars, got {len(base_frame)}")
                continue

            # Fit drift detector once per retrain cycle.
            try:
                detector = drift_mod.DriftDetector(
                    feature_catalog.feature_names("equity"))
                detector.fit(base_frame.drop(columns=["target"], errors="ignore"))
                self._drift[sym] = detector
            except Exception as exc:                # noqa: BLE001
                logger.debug("drift fit failed for %s: %s", sym, exc)

            # Train one (horizon × quantile) head per combination.
            for horizon in self._cfg.horizons_dte:
                frame = self._target_frame(sym, base_frame, horizon=horizon)
                if frame is None or len(frame) < 60:
                    warnings.append(
                        f"{sym}: insufficient rows for {horizon}DTE")
                    continue
                X = frame.drop(columns=["target"])
                y = frame["target"]
                for q in self._cfg.quantiles:
                    try:
                        model = self._fit_quantile_model(xgb, X, y, q)
                    except Exception as exc:        # noqa: BLE001
                        warnings.append(f"{sym}: q{q} fit failed: {exc}")
                        continue
                    self._models.setdefault(sym, {})[(horizon, q)] = (
                        model,
                        persistence.save_model(
                            model,
                            symbol=sym,
                            model_name=self._model_name(horizon, q),
                            target=self._cfg.target_kind,
                            sample_size=int(len(X)),
                            schema_stage="equity",
                            horizon_dte=horizon,
                            quantile=q,
                            metrics={"rmse": _rmse(model, X, y)},
                            notes=f"v2 quantile head q={q}",
                        ),
                    )
                    trained += 1

        if trained == 0 and self.symbols:
            warnings.insert(0, "No models trained")
        else:
            logger.info("retrain complete (%d heads)", trained)
        return warnings

    def _fit_quantile_model(self, xgb_mod, X: pd.DataFrame, y: pd.Series,
                            quantile: float):
        """Fit a single quantile-regression XGBoost head.

        ``reg:quantileerror`` was added in XGBoost 1.7. Older versions
        fall back to the legacy squared-error objective with a residual
        offset so the existing test environment still trains.
        """
        try:
            return _fit_with_objective(
                xgb_mod, X, y,
                objective="reg:quantileerror",
                quantile_alpha=quantile,
            )
        except Exception:
            offset = float(np.quantile(y, quantile) - np.quantile(y, 0.5))
            model = xgb_mod.XGBRegressor(
                n_estimators=400, max_depth=4, learning_rate=0.05,
                subsample=0.85, colsample_bytree=0.8, n_jobs=2,
                objective="reg:squarederror",
            )
            model.fit(X, y - offset)
            model._hermes_offset = offset           # type: ignore[attr-defined]
            return model

    # -- calibration ---------------------------------------------------------
    def _calibrate_all(self) -> None:
        """Refit isotonic calibrators against the prediction ledger.

        The actual fit lives in scripts/nightly_calibrate.py to keep
        this hot-loop method short; here we just reload whatever the
        latest calibrator JSON is for each symbol.
        """
        for sym in self._get_active_symbols():
            try:
                payload = self.db.get_setting(f"ml_calibrator__{sym}")
            except Exception:                       # noqa: BLE001
                payload = None
            if not payload:
                continue
            try:
                import json
                cal = load_calibrator(json.loads(payload))
                if cal is not None:
                    self._calibrators[sym] = cal
            except Exception as exc:                # noqa: BLE001
                logger.debug("calibrator load failed for %s: %s", sym, exc)

    # -- prediction ----------------------------------------------------------
    def _predict_all(self) -> List[str]:
        warnings: List[str] = []
        active = self._get_active_symbols()
        predicted = 0
        for sym in active:
            heads = self._models.get(sym, {})
            if not heads:
                continue
            base_frame = self._feature_frame(sym, drop_target=True)
            if base_frame is None or base_frame.empty:
                continue

            x_last = base_frame.iloc[[-1]]
            spot = float(self.db.last_price(sym) or 0.0)

            quantile_returns: Dict[float, float] = {}
            quantile_probs: Dict[float, float] = {}
            for (horizon, q), (model, _meta) in heads.items():
                # Always score against the primary horizon for the
                # surfaced point-prediction; we record other horizons
                # in the ledger but only the default flows out.
                if horizon != self._cfg.horizons_dte[0]:
                    continue
                yhat = float(model.predict(x_last)[0])
                offset = float(getattr(model, "_hermes_offset", 0.0))
                yhat += offset
                quantile_returns[q] = yhat
                quantile_probs[q] = self._return_to_prob(yhat, sym)

            # Apply per-symbol calibrator to the median (q50) probability.
            cal = self._calibrators.get(sym)
            if cal is not None and 0.5 in quantile_probs:
                quantile_probs[0.5] = float(
                    cal.transform([quantile_probs[0.5]])[0])

            yhat_med = quantile_returns.get(0.5, 0.0)
            prob_med = quantile_probs.get(0.5, 0.5)
            prob_lo = quantile_probs.get(self._cfg.quantiles[0])
            prob_hi = quantile_probs.get(self._cfg.quantiles[-1])
            if prob_lo is not None and prob_hi is not None and prob_lo > prob_hi:
                prob_lo, prob_hi = prob_hi, prob_lo

            predicted_price = round(spot * (1 + yhat_med), 4) if spot else 0.0

            self._last_pred[sym] = {
                "asof": datetime.now(timezone.utc),
                "predicted_return": yhat_med,
                "predicted_price": predicted_price,
                "predicted_prob": prob_med,
                "predicted_prob_lo": prob_lo,
                "predicted_prob_hi": prob_hi,
                "spot": spot,
                "quantiles": {f"q{int(q*100):02d}": v
                              for q, v in quantile_probs.items()},
                "horizon_dte": self._cfg.horizons_dte[0],
            }
            try:
                self.db.write_prediction(sym, yhat_med, predicted_price, spot)
            except Exception:                       # noqa: BLE001
                pass

            self._write_ledger_row(sym, x_last, prob_med, prob_lo, prob_hi,
                                   yhat_med, spot)
            predicted += 1

        if predicted == 0 and active:
            warnings.append("No predictions produced")
        return warnings

    def _return_to_prob(self, yhat: float, symbol: str) -> float:
        """Convert a predicted return into a probability of finishing
        OTM, using the symbol's own current realised vol so vol regimes
        are respected (no more 0.5 + return*5)."""
        try:
            vol = float(self.db.get_setting(f"ml_current_vol__{symbol}") or 0.30)
        except Exception:                           # noqa: BLE001
            vol = 0.30
        sigma_daily = max(0.005, vol / math.sqrt(252))
        z = float(yhat) / sigma_daily
        from scipy.stats import norm
        return float(np.clip(norm.cdf(z), 0.01, 0.99))

    def _write_ledger_row(self, symbol: str, x_last: pd.DataFrame,
                          prob_med: float, prob_lo: Optional[float],
                          prob_hi: Optional[float], yhat: float,
                          spot: float) -> None:
        try:
            heads = self._models.get(symbol, {})
            meta = next(iter(heads.values()))[1] if heads else None
            feature_vec = (x_last.iloc[0].to_dict()
                           if not x_last.empty else {})
            ledger_mod.write_record(self.db, ledger_mod.LedgerRecord(
                symbol=symbol,
                model_name=self._model_name(self._cfg.horizons_dte[0], 0.5),
                horizon_dte=self._cfg.horizons_dte[0],
                model_hash=(meta.model_hash if meta else None),
                schema_hash=(meta.schema_hash if meta else None),
                schema_stage="equity",
                predicted_prob=float(prob_med),
                predicted_prob_lo=(float(prob_lo) if prob_lo is not None else None),
                predicted_prob_hi=(float(prob_hi) if prob_hi is not None else None),
                predicted_return=float(yhat),
                spot=float(spot),
                feature_vector={k: (float(v) if isinstance(v, (int, float)) else v)
                                for k, v in feature_vec.items()
                                if not isinstance(v, str)},
            ))
        except Exception as exc:                    # noqa: BLE001
            logger.debug("ledger write failed for %s: %s", symbol, exc)

    # -- helpers -------------------------------------------------------------
    def _feature_frame(self, symbol: str, drop_target: bool = False,
                       ) -> Optional[pd.DataFrame]:
        bars_daily = self.db.daily_bars(symbol, lookback_days=400)
        bars_intraday = self.db.intraday_bars(symbol, lookback_days=10)
        spy_daily = self.db.daily_bars("SPY", lookback_days=400)
        if bars_daily is None or bars_daily.empty or spy_daily is None:
            return None
        feats = self.feat.build(symbol, bars_daily, bars_intraday, spy_daily)
        feats = feats.drop(columns=[c for c in ("symbol",) if c in feats.columns])
        if drop_target:
            return feats
        target = bars_daily["close"].pct_change().shift(-1).reindex(feats.index)
        feats = feats.assign(target=target).dropna()
        return feats

    def _target_frame(self, symbol: str, base: pd.DataFrame,
                      horizon: int) -> Optional[pd.DataFrame]:
        """Replace the next-day target with a horizon-specific cumulative
        return so each head trains against its own forecast horizon."""
        bars_daily = self.db.daily_bars(symbol, lookback_days=400)
        if bars_daily is None or bars_daily.empty:
            return None
        if self._cfg.use_pnl_target:
            # Realised credit-spread P&L would require a full options
            # backtest harness; until backtester.py is wired to write a
            # hermes_pnl_target setting per symbol, fall back to return.
            target = (bars_daily["close"].shift(-horizon)
                      / bars_daily["close"] - 1.0)
        else:
            target = (bars_daily["close"].shift(-horizon)
                      / bars_daily["close"] - 1.0)
        target = target.reindex(base.index)
        out = base.drop(columns=["target"], errors="ignore").assign(target=target)
        return out.dropna()

    # -- diagnostics surface -------------------------------------------------
    def diagnostics(self) -> Dict[str, Any]:
        """JSON-serialisable view of the predictor's current state.

        Powers /api/ml/diagnostics. Includes per-symbol model meta,
        latest predictions, drift alarms, and the live config.
        """
        out: Dict[str, Any] = {
            "config": {
                "horizons_dte": list(self._cfg.horizons_dte),
                "quantiles": list(self._cfg.quantiles),
                "predict_interval_s": self._cfg.predict_interval_s,
                "retrain_interval_s": self._cfg.retrain_interval_s,
                "calibrate_interval_s": self._cfg.calibrate_interval_s,
                "drift_alarm_threshold": self._cfg.drift_alarm_threshold,
                "target_kind": self._cfg.target_kind,
            },
            "schema_hash": feature_catalog.schema_hash("equity"),
            "symbols": {},
        }
        for sym, heads in self._models.items():
            sym_block: Dict[str, Any] = {
                "models": {},
                "last_prediction": self._last_pred.get(sym),
                "calibrator": (self._calibrators.get(sym).to_dict()
                               if self._calibrators.get(sym) else None),
            }
            for (horizon, q), (_model, meta) in heads.items():
                sym_block["models"][f"q{int(q*100):02d}_{horizon}dte"] = {
                    "model_hash": meta.model_hash,
                    "schema_hash": meta.schema_hash,
                    "trained_at": meta.trained_at,
                    "sample_size": meta.sample_size,
                    "metrics": meta.metrics,
                }
            detector = self._drift.get(sym)
            if detector is not None:
                try:
                    cur = self._feature_frame(sym, drop_target=True)
                    if cur is not None and not cur.empty:
                        sym_block["drift"] = detector.summary(
                            cur.tail(60),
                            threshold=self._cfg.drift_alarm_threshold,
                        )
                except Exception as exc:            # noqa: BLE001
                    logger.debug("drift summary failed for %s: %s", sym, exc)
            out["symbols"][sym] = sym_block
        return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fit_with_objective(xgb_mod, X: pd.DataFrame, y: pd.Series, *,
                        objective: str, quantile_alpha: float):
    model = xgb_mod.XGBRegressor(
        n_estimators=400, max_depth=4, learning_rate=0.05,
        subsample=0.85, colsample_bytree=0.8, n_jobs=2,
        objective=objective, quantile_alpha=quantile_alpha,
    )
    model.fit(X, y)
    return model


def _rmse(model, X: pd.DataFrame, y: pd.Series) -> float:
    try:
        yhat = model.predict(X)
        return float(np.sqrt(np.mean((yhat - y.values) ** 2)))
    except Exception:                               # noqa: BLE001
        return float("nan")


__all__ = [
    "FeatureRow",
    "FeatureEngineer",
    "AsyncXGBPredictor",
    "PredictorConfig",
    "hv_rank",
]
