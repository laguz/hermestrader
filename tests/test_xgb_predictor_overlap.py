"""Regression test for the MlRetrainTick overlap guard.

The reactive ML path fires MlRetrainTick every 10s (hermes/service1_agent/
scheduler.py). _run_ml_cycle can legitimately run longer than that (broker
history sync + xgboost fits), so handle_ml_retrain_tick must refuse to start
a second cycle while one is still executing — otherwise EventBus piles up
unbounded dispatch tasks until the process is killed (observed in
production: hermes-live-agent-1 crash-looping with hundreds of thousands of
pending asyncio tasks).
"""
import asyncio
import threading
import time

import pytest

from hermes.ml.feature_engineer import FeatureEngineer
from hermes.ml.xgb_features import AsyncXGBPredictor


class _StubDB:
    """Bare-minimum stand-in — AsyncXGBPredictor's __init__ only touches
    attributes guarded by try/except (ledger.ensure_table), so a plain
    object with nothing on it is enough to construct the predictor."""


class _StubBroker:
    pass


@pytest.fixture
def predictor(tmp_path):
    return AsyncXGBPredictor(
        db=_StubDB(),
        feat=FeatureEngineer(),
        broker=_StubBroker(),
        watchlist=["SPY"],
        model_dir=tmp_path,
    )


async def test_overlapping_ticks_run_cycle_once(predictor):
    # _run_ml_cycle executes in a worker thread (run_in_executor), so the
    # handshake with the test needs thread-safe signals, not asyncio.Event.
    calls = []
    started = threading.Event()
    release = threading.Event()

    def slow_cycle(force: bool = False):
        calls.append(force)
        started.set()
        # Block the executor thread until the test lets it go, simulating a
        # cycle that outlives the 10s tick cadence.
        release.wait(timeout=2)

    predictor._run_ml_cycle = slow_cycle

    class _Tick:
        force = False

    first = asyncio.create_task(predictor.handle_ml_retrain_tick(_Tick()))
    await asyncio.get_event_loop().run_in_executor(None, started.wait, 1)

    # A second tick arrives while the first cycle is still "running".
    await predictor.handle_ml_retrain_tick(_Tick())

    release.set()
    await asyncio.wait_for(first, timeout=2)

    assert calls == [False]  # second tick was dropped, not queued


async def test_cycle_flag_resets_after_completion(predictor):
    predictor._run_ml_cycle = lambda force=False: None

    class _Tick:
        force = False

    await predictor.handle_ml_retrain_tick(_Tick())
    assert predictor._cycle_in_progress is False

    await predictor.handle_ml_retrain_tick(_Tick())
    assert predictor._cycle_in_progress is False
