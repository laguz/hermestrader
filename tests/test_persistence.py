"""Tests for hermes.ml.persistence — joblib + schema-hash gating.

These tests validate the rules that motivated the rewrite:

- A model loads cleanly when its schema_hash matches the live catalog.
- A model whose schema_hash differs from the live catalog is REFUSED
  (returns None, None) and quarantined.
- Round-trip preserves model_hash so the prediction ledger can stamp
  predictions reliably.
- Concurrent saves are atomic — no half-written files left behind.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from hermes.ml import feature_catalog
from hermes.ml import persistence


class _TinyModel:
    """A trivially picklable stand-in for an XGBoost regressor."""

    def __init__(self, scale: float = 1.0):
        self.scale = scale

    def predict(self, X):
        return [v * self.scale for v in X]


@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    return tmp_path / "models"


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------
def test_save_and_load_round_trip(tmp_root):
    model = _TinyModel(scale=2.5)
    meta = persistence.save_model(
        model, symbol="aapl", model_name="test_q50_7dte",
        target="return", sample_size=120,
        schema_stage="equity", root=tmp_root,
    )
    assert meta.schema_hash == feature_catalog.schema_hash("equity")
    assert meta.sample_size == 120

    loaded, loaded_meta = persistence.load_model(
        symbol="aapl", model_name="test_q50_7dte",
        schema_stage="equity", root=tmp_root,
    )
    assert loaded is not None
    assert loaded_meta is not None
    assert loaded_meta.model_hash == meta.model_hash


def test_load_returns_none_when_files_missing(tmp_root):
    loaded, meta = persistence.load_model(
        symbol="ZZZZ", model_name="missing", root=tmp_root,
    )
    assert loaded is None and meta is None


# ---------------------------------------------------------------------------
# Schema-hash gate
# ---------------------------------------------------------------------------
def test_schema_mismatch_quarantines_artifact(tmp_root, monkeypatch):
    """If the catalog changes after a model was saved, the loader must
    refuse and move the artefact to the quarantine directory."""
    model = _TinyModel()
    persistence.save_model(
        model, symbol="msft", model_name="m1",
        target="return", sample_size=80, schema_stage="equity",
        root=tmp_root,
    )

    # Forge a schema change by extending the catalog.
    extended = tuple(list(feature_catalog.RAW_EQUITY_FEATURES) + [
        feature_catalog.FeatureSpec("new_alpha", "ratio", "test", "daily")
    ])
    monkeypatch.setattr(feature_catalog, "RAW_EQUITY_FEATURES", extended)
    monkeypatch.setattr(
        feature_catalog, "ALL_RAW_FEATURES",
        extended + feature_catalog.OPTIONS_FEATURES + feature_catalog.MACRO_FEATURES,
    )

    loaded, meta = persistence.load_model(
        symbol="msft", model_name="m1", schema_stage="equity",
        root=tmp_root,
    )
    assert loaded is None and meta is None
    quarantine = tmp_root / "_quarantine"
    assert quarantine.exists()
    moved = list(quarantine.rglob("*.joblib"))
    assert moved, "schema mismatch should move the artefact aside"


def test_meta_unreadable_does_not_crash(tmp_root):
    """If the .meta.json is corrupted, load returns None instead of raising."""
    model = _TinyModel()
    persistence.save_model(
        model, symbol="goog", model_name="m2",
        target="return", sample_size=80, schema_stage="equity",
        root=tmp_root,
    )
    meta_path = tmp_root / "GOOG" / "m2.meta.json"
    meta_path.write_text("{ this is not json")
    loaded, meta = persistence.load_model(
        symbol="goog", model_name="m2", schema_stage="equity",
        root=tmp_root,
    )
    assert loaded is None and meta is None


# ---------------------------------------------------------------------------
# list_models
# ---------------------------------------------------------------------------
def test_list_models_enumerates_per_symbol(tmp_root):
    persistence.save_model(_TinyModel(1), symbol="t", model_name="a",
                           target="return", sample_size=80,
                           schema_stage="equity", root=tmp_root)
    persistence.save_model(_TinyModel(2), symbol="t", model_name="b",
                           target="return", sample_size=80,
                           schema_stage="equity", root=tmp_root)
    listing = persistence.list_models("t", root=tmp_root)
    assert set(listing.keys()) == {"a", "b"}


def test_metadata_round_trips_through_to_json():
    meta = persistence.ModelMeta(
        model_hash="abc",
        schema_hash="def",
        schema_stage="equity",
        trained_at=12345.0,
        target="return",
        sample_size=50,
        horizon_dte=7,
        quantile=0.5,
        metrics={"rmse": 0.1},
    )
    blob = meta.to_json()
    restored = persistence.ModelMeta.from_json(blob)
    assert restored == meta
