"""
[XGBoost-Feature-Engine v2] — predictor configuration.

Split out of ``xgb_features.py`` so the live-tunable knobs read from
HermesDB.system_settings live in one small, testable place. Re-exported from
``xgb_features`` so ``from hermes.ml.xgb_features import PredictorConfig`` keeps
working.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Tuple


_MAIN_LOOP = None


def set_main_loop(loop):
    global _MAIN_LOOP
    _MAIN_LOOP = loop


def run_maybe_async(func, *args, **kwargs):
    """Run an async or sync function from a synchronous context."""
    import asyncio
    import inspect

    if inspect.iscoroutinefunction(func):
        coro = func(*args, **kwargs)
    else:
        res = func(*args, **kwargs)
        if inspect.iscoroutine(res):
            coro = res
        else:
            return res

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    global _MAIN_LOOP
    if _MAIN_LOOP is not None and _MAIN_LOOP.is_running() and current_loop is not _MAIN_LOOP:
        future = asyncio.run_coroutine_threadsafe(coro, _MAIN_LOOP)
        return future.result()

    return asyncio.run(coro)


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
    target_kind: str = "return"                  # "return" or "pnl"
    use_pnl_target: bool = False                 # rec #18 toggle

    @classmethod
    def from_db(cls, db: Any) -> "PredictorConfig":
        cfg = cls()
        if db is None or not hasattr(db, "get_setting"):
            return cfg

        def _f(key: str, default: float) -> float:
            try:
                v = run_maybe_async(db.get_setting, key)
                return float(v) if v not in (None, "") else default
            except (TypeError, ValueError):
                return default

        def _s(key: str, default: str) -> str:
            try:
                v = run_maybe_async(db.get_setting, key)
                return str(v) if v else default
            except Exception:
                return default

        cfg.predict_interval_s = _f("ml_predict_interval_s", cfg.predict_interval_s)
        cfg.retrain_interval_s = _f("ml_retrain_interval_s", cfg.retrain_interval_s)
        cfg.calibrate_interval_s = _f("ml_calibrate_interval_s", cfg.calibrate_interval_s)
        cfg.target_kind = _s("ml_target_kind", cfg.target_kind)
        cfg.use_pnl_target = (cfg.target_kind == "pnl")
        return cfg
