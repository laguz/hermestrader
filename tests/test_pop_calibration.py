"""POP outcome-calibration loop.

The engine's predicted POP is fitted against the book's own realized
win/loss outcomes (Platt scaling on closed trades' entry_features.pop vs
pnl>0) and applied as the last step of ``predict_pop``. These tests pin:

- an overconfident book deflates POP; the correction is installed/cleared
  cleanly and predict_pop reflects it,
- the conservative fit guards (min samples, both classes represented,
  schema-1 rows excluded, must improve log-loss),
- the heartbeat wiring: throttle, persistence, restart recovery.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from hermes.ml.calibration import PlattCalibrator
from hermes.ml.pop_calibration import extract_calibration_rows, fit_pop_calibrator
from hermes.ml.pop_engine import (
    FeatureVector,
    get_pop_calibrator,
    predict_pop,
    set_pop_calibrator,
)


@pytest.fixture(autouse=True)
def _clean_calibrator():
    set_pop_calibrator(None)
    yield
    set_pop_calibrator(None)


def _trade(pop: float, pnl: float, schema: int = 2) -> dict:
    return {"entry_features": {"schema": schema, "pop": pop}, "pnl": pnl}


def _overconfident_book(n: int = 60, pop: float = 0.85, win_rate: float = 0.60):
    """Engine said 85% but only 60% of trades won."""
    wins = int(n * win_rate)
    return [_trade(pop, +10.0) for _ in range(wins)] + \
           [_trade(pop, -25.0) for _ in range(n - wins)]


class _StubCalDB:
    def __init__(self, trades):
        self._trades = trades
        self.trades = self

    async def closed_trades_entry_features(self, limit=500):
        return self._trades[:limit]


# ── fit guards ───────────────────────────────────────────────────────────────
def test_extract_rows_skips_schema1_and_degenerate():
    rows = [
        _trade(0.88, +5.0, schema=1),      # old inflated regime — excluded
        _trade(0.80, +5.0),
        _trade(1.0, +5.0),                 # degenerate pop — excluded
        {"entry_features": None, "pnl": 5.0},
        {"entry_features": {"schema": 2, "pop": 0.7}, "pnl": None},
        _trade(0.70, -9.0),
    ]
    pops, outcomes = extract_calibration_rows(rows)
    assert pops == [0.80, 0.70]
    assert outcomes == [1.0, 0.0]


def test_fit_deferred_below_min_samples_or_single_class():
    async def run(trades):
        return await fit_pop_calibrator(_StubCalDB(trades))

    assert asyncio.run(run(_overconfident_book(n=10))) is None
    all_wins = [_trade(0.85, +10.0) for _ in range(60)]
    assert asyncio.run(run(all_wins)) is None


def test_fit_deflates_overconfident_book():
    result = asyncio.run(fit_pop_calibrator(_StubCalDB(_overconfident_book())))
    assert result is not None
    cal = result["calibrator"]
    corrected = float(cal.transform([0.85])[0])
    assert corrected == pytest.approx(0.60, abs=0.03)
    assert result["log_loss_cal"] <= result["log_loss_raw"]
    assert result["n"] == 60 and result["wins"] == 36 and result["losses"] == 24


# ── predict_pop application ──────────────────────────────────────────────────
def test_predict_pop_applies_installed_calibrator_and_clears():
    fv = FeatureVector(delta=0.15, xgb_prob=0.5, current_vol=0.25,
                       avg_vol=0.25, protection_score=1.0)
    raw = predict_pop(fv)
    assert raw == pytest.approx(0.85, abs=1e-6)

    result = asyncio.run(fit_pop_calibrator(_StubCalDB(_overconfident_book())))
    set_pop_calibrator(result["calibrator"])
    calibrated = predict_pop(fv)
    assert calibrated == pytest.approx(0.60, abs=0.03)

    set_pop_calibrator(None)
    assert predict_pop(fv) == pytest.approx(raw, abs=1e-12)


# ── heartbeat wiring ─────────────────────────────────────────────────────────
def _engine_with_trades(trades):
    from hermes.service1_agent.core import CascadingEngine
    from ._stubs import StubBroker, StubDB

    db = StubDB()
    fetch_calls = []

    async def _fetch(limit=500):
        fetch_calls.append(limit)
        return trades[:limit]

    db.closed_trades_entry_features = _fetch
    engine = CascadingEngine(broker=StubBroker(), db=db, strategies=[],
                             approval_mode=False)
    return engine, db, fetch_calls


async def test_heartbeat_refit_installs_persists_and_throttles():
    trades = _overconfident_book()
    engine, db, fetch_calls = _engine_with_trades(trades)

    await engine.pipeline.maybe_refit_pop_calibrator()
    cal = get_pop_calibrator()
    assert cal is not None
    assert float(cal.transform([0.85])[0]) == pytest.approx(0.60, abs=0.03)
    state = json.loads(db.settings["pop_calibration"])
    assert state["n"] == 60
    assert "pop_cal_last_fit" in db.settings
    assert len(fetch_calls) == 1

    # Second call inside the 6h throttle window must not hit the DB again.
    await engine.pipeline.maybe_refit_pop_calibrator()
    assert len(fetch_calls) == 1


async def test_sync_from_settings_installs_updates_and_tolerates_garbage():
    """The watcher-side read path: install on first sight of the persisted
    blob, no-op while unchanged, re-install on change, never crash or clear
    the installed calibrator on garbage/missing blobs."""
    import hermes.ml.pop_calibration as pc

    class _SettingsDB:
        def __init__(self):
            self.value = None
            self.settings = self

        async def get_setting(self, key):
            assert key == pc.POP_CAL_STATE_KEY
            return self.value

    db = _SettingsDB()
    pc._synced_state_raw = None
    try:
        # Nothing persisted yet → no install.
        assert await pc.sync_pop_calibrator_from_settings(db) is False
        assert get_pop_calibrator() is None

        first = PlattCalibrator(a=1.2, b=-0.4)
        db.value = json.dumps({"calibrator": first.to_dict(), "n": 40,
                               "fitted_at": "2026-07-03T00:00:00+00:00"})
        assert await pc.sync_pop_calibrator_from_settings(db) is True
        assert get_pop_calibrator().to_dict() == first.to_dict()

        # Unchanged blob → no re-install churn.
        assert await pc.sync_pop_calibrator_from_settings(db) is False

        # Refit landed → new params picked up.
        second = PlattCalibrator(a=0.9, b=0.1)
        db.value = json.dumps({"calibrator": second.to_dict(), "n": 80,
                               "fitted_at": "2026-07-04T00:00:00+00:00"})
        assert await pc.sync_pop_calibrator_from_settings(db) is True
        assert get_pop_calibrator().to_dict() == second.to_dict()

        # Garbage blob → ignored, installed calibrator untouched.
        db.value = "{not json"
        assert await pc.sync_pop_calibrator_from_settings(db) is False
        assert get_pop_calibrator().to_dict() == second.to_dict()
    finally:
        pc._synced_state_raw = None


def test_state_key_shared_between_agent_and_watcher_sync():
    from hermes.ml.pop_calibration import POP_CAL_STATE_KEY
    from hermes.service1_agent._engine_pipeline import PipelineController

    assert PipelineController._POP_CAL_STATE_KEY.fget(object()) == POP_CAL_STATE_KEY


async def test_heartbeat_restart_recovery_reinstalls_persisted_calibrator():
    engine, db, _fetch_calls = _engine_with_trades([])
    fitted = PlattCalibrator.fit([0.85] * 40 + [0.85] * 20,
                                 [1.0] * 40 + [0.0] * 20)
    db.settings["pop_calibration"] = json.dumps(
        {"calibrator": fitted.to_dict(), "n": 60, "fitted_at": "2026-07-01T00:00:00+00:00"})
    db.settings["pop_cal_last_fit"] = "2026-07-03T00:00:00+00:00"

    set_pop_calibrator(None)
    await engine.pipeline.maybe_refit_pop_calibrator()
    cal = get_pop_calibrator()
    assert cal is not None
    assert cal.to_dict() == fitted.to_dict()
