"""Regression test: ``_calibrate_all`` must install the persisted meta-learner.

``set_meta_learner``'s signature is ``(model, symbol)``; ``_calibrate_all``
once called it as ``(symbol, model)``, so ``symbol.upper()`` hit the
MetaLearner object, raised AttributeError, and the broad except logged it at
debug and moved on. Net effect: the meta-learners trained nightly by
``scripts/nightly_calibrate.py`` and persisted to ``ml_meta_learner__<SYM>``
were silently discarded every reload, and POP scoring stayed on the
cold-start fallback forever.
"""
import pytest

from hermes.ml.feature_engineer import FeatureEngineer
from hermes.ml.meta_learner import MetaLearner
from hermes.ml.pop_engine import get_meta_learner, set_meta_learner
from hermes.ml.xgb_features import AsyncXGBPredictor


class _StubDB:
    """Only ``get_setting`` matters here; everything else the predictor
    touches during construction is guarded by try/except."""

    def __init__(self, settings):
        self._settings = settings

    async def get_setting(self, key):
        return self._settings.get(key)


@pytest.fixture
def _clean_meta_registry():
    yield
    # pop_engine's registry is module-global — don't leak into other tests.
    set_meta_learner(None, "SPY")


def test_calibrate_all_installs_persisted_meta_learner(
        tmp_path, _clean_meta_registry):
    fitted = MetaLearner(
        feature_names=("xgb_prob",), weights=[1.25], intercept=-0.1)
    db = _StubDB({"ml_meta_learner__SPY": fitted.to_json()})
    predictor = AsyncXGBPredictor(
        db=db,
        feat=FeatureEngineer(),
        broker=object(),
        watchlist=["SPY"],
        model_dir=tmp_path,
    )

    predictor._calibrate_all()

    installed = get_meta_learner("SPY")
    assert installed.weights == [1.25]
    assert installed.intercept == pytest.approx(-0.1)
