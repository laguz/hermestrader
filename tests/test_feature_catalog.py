"""Tests for hermes.ml.feature_catalog.

The catalog is the wire contract between every component in the ML
stack — its hash must be deterministic, and renaming a feature must
change the hash so persisted models are quarantined.
"""
from __future__ import annotations

import dataclasses
import json

import pytest

from hermes.ml import feature_catalog
from hermes.ml.feature_catalog import (
    FeatureSpec,
    META_FEATURES,
    RAW_EQUITY_FEATURES,
    catalog_dict,
    feature_names,
    schema_hash,
    specs_for,
)


# ---------------------------------------------------------------------------
# schema_hash
# ---------------------------------------------------------------------------
def test_schema_hash_is_deterministic():
    """Hash is content-only, not import-order dependent."""
    assert schema_hash("equity") == schema_hash("equity")
    assert schema_hash("raw") == schema_hash("raw")


def test_schema_hash_differs_per_stage():
    h_equity = schema_hash("equity")
    h_options = schema_hash("options")
    h_meta = schema_hash("meta")
    assert h_equity != h_options
    assert h_options != h_meta


def test_schema_hash_changes_when_feature_added(monkeypatch):
    """A renamed/added feature must invalidate the hash so cached
    models are quarantined rather than silently misaligned."""
    original = list(feature_catalog.RAW_EQUITY_FEATURES)
    extended = tuple(original + [
        FeatureSpec("new_feature", "ratio", "test", "daily")
    ])
    h_before = schema_hash("equity")
    monkeypatch.setattr(feature_catalog, "RAW_EQUITY_FEATURES", extended)
    monkeypatch.setattr(
        feature_catalog, "ALL_RAW_FEATURES",
        extended + feature_catalog.OPTIONS_FEATURES + feature_catalog.MACRO_FEATURES,
    )
    h_after = schema_hash("equity")
    assert h_before != h_after


def test_schema_hash_ignores_description_changes(monkeypatch):
    """Description tweaks should NOT invalidate cached models."""
    original = list(feature_catalog.RAW_EQUITY_FEATURES)
    rewritten = tuple(
        dataclasses.replace(s, description=f"{s.description} (updated)")
        for s in original
    )
    h_before = schema_hash("equity")
    monkeypatch.setattr(feature_catalog, "RAW_EQUITY_FEATURES", rewritten)
    monkeypatch.setattr(
        feature_catalog, "ALL_RAW_FEATURES",
        rewritten + feature_catalog.OPTIONS_FEATURES + feature_catalog.MACRO_FEATURES,
    )
    h_after = schema_hash("equity")
    assert h_before == h_after


# ---------------------------------------------------------------------------
# feature_names / specs_for
# ---------------------------------------------------------------------------
def test_feature_names_known_stages():
    eq = feature_names("equity")
    opt = feature_names("options")
    macro = feature_names("macro")
    meta = feature_names("meta")
    assert "overnight_gap" in eq
    # Options + macro stages are intentionally empty until their respective
    # feeds (options-IV, macro) are wired into the feature engineer.
    assert opt == []
    assert macro == []
    assert "xgb_prob" in meta


def test_feature_names_rejects_unknown_stage():
    with pytest.raises(KeyError):
        feature_names("nonsense")


def test_raw_aggregates_equity_options_macro():
    raw = feature_names("raw")
    for name in feature_names("equity"):
        assert name in raw
    for name in feature_names("options"):
        assert name in raw
    for name in feature_names("macro"):
        assert name in raw


def test_specs_for_returns_dataclass_instances():
    rows = specs_for("equity")
    assert all(isinstance(r, FeatureSpec) for r in rows)
    assert len(rows) == len(RAW_EQUITY_FEATURES)


# ---------------------------------------------------------------------------
# catalog_dict — diagnostics payload
# ---------------------------------------------------------------------------
def test_catalog_dict_is_json_serialisable():
    payload = catalog_dict()
    json.dumps(payload)                   # must not raise
    assert "equity" in payload
    assert "options" in payload
    assert "meta" in payload
    assert isinstance(payload["equity"], list)


def test_meta_feature_names_match_default_meta_learner_features():
    """The meta-learner's DEFAULT_FEATURES tuple must intersect with
    catalog META_FEATURES so the predictor and the meta-learner agree
    on column ordering."""
    from hermes.ml.meta_learner import DEFAULT_FEATURES
    catalog_meta_names = {s.name for s in META_FEATURES}
    for name in DEFAULT_FEATURES:
        assert name in catalog_meta_names, (
            f"{name!r} declared in DEFAULT_FEATURES but missing from catalog"
        )
