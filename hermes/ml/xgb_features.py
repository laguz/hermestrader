"""
[XGBoost-Feature-Engine v2]

The pure feature-engineering layer (``FeatureRow`` / ``FeatureEngineer``)
now lives in ``hermes/ml/feature_engineer.py`` and is re-exported
here, so ``from hermes.ml.xgb_features import FeatureEngineer`` keeps working.
This module owns the predictor layer:

   ``AsyncXGBPredictor`` — async predictor with:
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
     * prediction-ledger writes carrying the model_hash and feature
       schema for postmortem replay.

The constructor signature matches v1 exactly so the agent boot path in
``hermes/service1_agent/main.py`` and the existing tests are
unaffected.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from hermes.ml import ledger as ledger_mod
from hermes.ml import persistence
from hermes.ml.calibration import load_calibrator
# Pure feature-engineering layer — moved to its own module; re-exported below
# so existing `from hermes.ml.xgb_features import FeatureEngineer` imports work.
from hermes.ml.feature_engineer import FeatureRow, FeatureEngineer
# Live-tunable predictor config — moved to its own module; re-exported below so
# existing `from hermes.ml.xgb_features import PredictorConfig` imports work.
from hermes.ml.predictor_config import PredictorConfig, run_maybe_async
# Training and inference concerns live in owned collaborators (back-reference to
# this predictor); see predictor_training.py / predictor_inference.py.
from hermes.ml.predictor_inference import PredictorInference
from hermes.ml.predictor_training import PredictorTrainer


logger = logging.getLogger("hermes.ml.xgb")


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
        self._cfg = PredictorConfig()
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
        # Last predictions surfaced to the rest of the system.
        self._last_pred: Dict[str, Dict[str, Any]] = {}

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_train_ts = 0.0
        self._last_calibrate_ts = 0.0
        self._last_predict_tuple: Optional[Tuple[int, int, int, int]] = None
        # Reactive path (MlRetrainTick fires every 10s) must not start a new
        # cycle while the previous one is still in the executor — sync/train
        # can run long enough to overlap and pile up unboundedly otherwise.
        self._cycle_in_progress = False

        # Ensure the prediction ledger table exists. Idempotent; safe on
        # every boot regardless of whether prior versions ran migrations.
        try:
            ledger_mod.ensure_table(self.db)
        except Exception as exc:
            logger.warning("ledger.ensure_table failed: %s", exc)

        try:
            self._model_root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning("Cannot create model dir %s: %s", self._model_root, exc)

        # Owned collaborators: training and inference read this predictor's
        # state and mutate its shared caches (_models / _last_pred)
        # through back-references. The scheduling loop below routes to them.
        self.trainer = PredictorTrainer(self)
        self.inference = PredictorInference(self)

    # -- public --------------------------------------------------------------
    def start(self, event_bus: Optional[Any] = None) -> None:
        self._load_models()
        try:
            self._main_loop = asyncio.get_running_loop()
            from hermes.ml.predictor_config import set_main_loop
            set_main_loop(self._main_loop)
        except RuntimeError:
            self._main_loop = None

        if event_bus is not None:
            from hermes.events.bus import MlRetrainTick
            event_bus.subscribe(MlRetrainTick, self.handle_ml_retrain_tick)
            logger.info("AsyncXGBPredictor registered to MlRetrainTick on EventBus.")
        else:
            if self._thread and self._thread.is_alive():
                return
            self._thread = threading.Thread(target=self._loop, name="xgb-predictor",
                                            daemon=True)
            self._thread.start()

    async def handle_ml_retrain_tick(self, event: Any) -> None:
        import asyncio
        if self._cycle_in_progress:
            # A cycle is still running in the executor. Drop this tick rather
            # than queuing another one — the next tick (10s later) will pick
            # up any pending ml_force_run flag once the current cycle ends.
            logger.debug("ML cycle already in progress; skipping tick.")
            return
        self._cycle_in_progress = True
        try:
            loop = asyncio.get_running_loop()
            force = getattr(event, "force", False)
            await loop.run_in_executor(None, self._run_ml_cycle, force)
        finally:
            self._cycle_in_progress = False

    def _run_ml_cycle(self, force: bool = False) -> None:
        from hermes.market_hours import ET
        try:
            self._cfg = PredictorConfig.from_db(self.db)
            now = time.time()
            now_et = datetime.now(ET)
            force_run = force or (run_maybe_async(self.db.get_setting, "ml_force_run") == "true")

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
                warnings.extend(self.trainer.retrain_all())
                self._last_train_ts = now
            if should_calibrate:
                self._calibrate_all()
                self._last_calibrate_ts = now
            if should_predict or should_retrain:
                warnings.extend(self.inference.predict_all())

            if force_run:
                try:
                    run_maybe_async(self.db.set_setting, "ml_force_run", "false")
                except Exception as exc:
                    logger.warning("Failed to reset ml_force_run: %s", exc)

            self._record_status(warnings)
        except Exception as exc:
            logger.exception("xgb cycle error: %s", exc)
            try:
                run_maybe_async(self.db.set_setting, "ml_last_error", str(exc)[:500])
            except Exception as e:
                logger.warning("Failed to record ml_last_error: %s", e)

    def stop(self) -> None:
        self._stop.set()

    def predict_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._last_pred.get(symbol)

    @property
    def config(self) -> PredictorConfig:
        return self._cfg

    # -- background loop -----------------------------------------------------
    def _loop(self) -> None:
        while not self._stop.is_set():
            self._run_ml_cycle()
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
            run_maybe_async(self.db.set_setting, "ml_last_ok_ts",
                            datetime.now(timezone.utc).isoformat())
            if warnings:
                run_maybe_async(self.db.set_setting, "ml_last_error",
                                "; ".join(warnings)[:500])
            else:
                run_maybe_async(self.db.set_setting, "ml_last_error", "")
        except Exception as exc:
            logger.warning("Failed to record ML status: %s", exc)

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
            wls = run_maybe_async(self.db.list_all_watchlists)
            for syms in wls.values():
                active.update(syms)
        except Exception as exc:
            logger.warning("Failed to fetch strategy watchlists: %s", exc)
        return sorted(active)

    # -- history sync --------------------------------------------------------
    # Bounds how many symbols sync concurrently per cycle — high enough to
    # collapse N sequential broker round-trips into one, low enough not to
    # hammer the Tradier rate limit.
    _HISTORY_SYNC_CONCURRENCY = 5
    # MCPBrokerClient._call_mcp has no timeout of its own — a stalled sandbox
    # response hangs the stdio round-trip forever. Bound each call here so one
    # bad symbol can't wedge the whole gather (and with it _run_ml_cycle,
    # _cycle_in_progress, and every ClockTickEvent behind it on the bus).
    _HISTORY_FETCH_TIMEOUT_S = 30.0

    async def _sync_one_symbol(self, sym: str, start_date: date, end_date: date,
                               intra_start: date) -> None:
        try:
            daily_bars = await asyncio.wait_for(
                self.broker.get_history(
                    sym, interval="daily",
                    start=start_date.isoformat(),
                    end=end_date.isoformat()),
                timeout=self._HISTORY_FETCH_TIMEOUT_S)
            if daily_bars:
                if isinstance(daily_bars, list):
                    df_daily = pd.DataFrame(daily_bars)
                    if not df_daily.empty:
                        if "date" in df_daily.columns:
                            df_daily = df_daily.rename(columns={"date": "ts"})
                        if ("vwap_close" not in df_daily.columns
                                and "close" in df_daily.columns):
                            df_daily["vwap_close"] = df_daily["close"]
                        await self.db.save_daily_bars(sym, df_daily)
                else:
                    logger.error("history sync failed for %s: %s", sym, daily_bars)
                    return
        except Exception as exc:
            logger.error("history sync failed for %s: %s", sym, exc)
            return

        try:
            intra_bars = await asyncio.wait_for(
                self.broker.get_history(
                    sym, interval="1min",
                    start=intra_start.isoformat(),
                    end=end_date.isoformat()),
                timeout=self._HISTORY_FETCH_TIMEOUT_S)
            if intra_bars:
                if isinstance(intra_bars, list):
                    df_intra = pd.DataFrame(intra_bars)
                    if not df_intra.empty:
                        if "date" in df_intra.columns:
                            df_intra = df_intra.rename(columns={"date": "ts"})
                        await self.db.save_intraday_bars(sym, df_intra)
                else:
                    logger.debug("intraday sync failed for %s: %s", sym, intra_bars)
        except Exception as exc:
            logger.debug("intraday sync failed for %s: %s", sym, exc)

    async def _sync_history_async(self, symbols: List[str]) -> None:
        end_date = date.today()
        start_date = end_date - timedelta(days=400)
        intra_start = end_date - timedelta(days=10)
        sem = asyncio.Semaphore(self._HISTORY_SYNC_CONCURRENCY)

        async def _bounded(sym: str) -> None:
            async with sem:
                await self._sync_one_symbol(sym, start_date, end_date, intra_start)

        try:
            await asyncio.gather(*(_bounded(sym) for sym in symbols))
        finally:
            # _sync_history wraps this whole coroutine in a fresh
            # asyncio.run() every cycle, on a threadpool executor thread —
            # a new event loop each time. If self.broker (MCPBrokerClient,
            # in mcp-broker mode) is left holding an open stdio session/owner
            # Task when this coroutine returns, asyncio.run()'s own teardown
            # force-cancels whatever's still running on this loop before
            # closing it — including that owner Task, mid-cleanup, on a loop
            # that's about to disappear. Closing explicitly, on this same
            # loop, before asyncio.run() gets a chance to force it, is what
            # avoids that: the next cycle's fresh asyncio.run() then starts
            # with nothing left over to reconcile.
            try:
                await self.broker.close()
            except Exception as exc:
                logger.debug("Error closing ML broker after history sync: %s", exc)

    def _sync_history(self) -> None:
        if not hasattr(self.db, "save_daily_bars"):
            logger.warning("HermesDB missing save_daily_bars; cannot sync history")
            return

        symbols = self._get_active_symbols()
        main_loop = getattr(self, "_main_loop", None)
        if main_loop is not None and main_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self._sync_history_async(symbols), main_loop
            )
            try:
                future.result()
            except Exception as exc:
                logger.error("ML history sync failed on main loop: %s", exc)
        else:
            asyncio.run(self._sync_history_async(symbols))
        logger.info("history sync complete")

    # -- calibration ---------------------------------------------------------
    def _calibrate_all(self) -> None:
        """Refit isotonic calibrators against the prediction ledger.

        The actual fit lives in scripts/nightly_calibrate.py to keep
        this hot-loop method short; here we just reload whatever the
        latest calibrator and meta-learner JSON is for each symbol.
        """
        from hermes.ml.pop_engine import set_meta_learner
        from hermes.ml.meta_learner import MetaLearner
        import json

        for sym in self._get_active_symbols():
            try:
                payload = run_maybe_async(self.db.get_setting, f"ml_calibrator__{sym}")
            except Exception:
                payload = None
            if payload:
                try:
                    cal = load_calibrator(json.loads(payload))
                    if cal is not None:
                        self._calibrators[sym] = cal
                except Exception as exc:
                    logger.debug("calibrator load failed for %s: %s", sym, exc)

            try:
                meta_payload = run_maybe_async(self.db.get_setting, f"ml_meta_learner__{sym}")
            except Exception:
                meta_payload = None
            if meta_payload:
                try:
                    meta = MetaLearner.from_json(meta_payload)
                    set_meta_learner(meta, sym)
                except Exception as exc:
                    logger.debug("meta-learner load failed for %s: %s", sym, exc)

    # -- helpers -------------------------------------------------------------
    def _feature_frame(self, symbol: str, drop_target: bool = False,
                       ) -> Optional[pd.DataFrame]:
        bars_daily = run_maybe_async(self.db.daily_bars, symbol, lookback_days=400)
        bars_intraday = run_maybe_async(self.db.intraday_bars, symbol, lookback_days=10)
        spy_daily = run_maybe_async(self.db.daily_bars, "SPY", lookback_days=400)
        if bars_daily is None or bars_daily.empty or spy_daily is None:
            return None
        bars_daily = bars_daily.copy()
        if "vwap_close" in bars_daily.columns:
            bars_daily["vwap_close"] = bars_daily["vwap_close"].fillna(bars_daily["close"])
        spy_daily = spy_daily.copy()
        if "vwap_close" in spy_daily.columns:
            spy_daily["vwap_close"] = spy_daily["vwap_close"].fillna(spy_daily["close"])
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
        bars_daily = run_maybe_async(self.db.daily_bars, symbol, lookback_days=400)
        if bars_daily is None or bars_daily.empty:
            return None
        if self._cfg.use_pnl_target:
            # Realised credit-spread P&L would require a full options
            # backtest harness to write a hermes_pnl_target setting per
            # symbol; none exists yet, so fall back to return.
            target = (bars_daily["close"].shift(-horizon)
                      / bars_daily["close"] - 1.0)
        else:
            target = (bars_daily["close"].shift(-horizon)
                      / bars_daily["close"] - 1.0)
        target = target.reindex(base.index)
        out = base.drop(columns=["target"], errors="ignore").assign(target=target)
        return out.dropna()



__all__ = [
    "FeatureRow",
    "FeatureEngineer",
    "AsyncXGBPredictor",
    "PredictorConfig",
]
