"""Machine-learning forecasting layer.

The v2 stack is split into single-responsibility modules so each layer
can evolve independently:

- ``feature_catalog``  — declarative feature catalog plus deterministic
  schema-hash. Renaming a feature invalidates persisted models.
- ``xgb_features``     — FeatureEngineer (raw equity alpha set) and
  AsyncXGBPredictor (multi-horizon, multi-quantile threaded predictor
  with decoupled sync/train/calibrate/predict subtasks).
- ``persistence``      — joblib-backed model storage with sidecar meta
  (model_hash, schema_hash, sample_size). Refuses to load on schema
  mismatch and quarantines the artefact.
- ``calibration``      — IsotonicCalibrator + PlattCalibrator plus
  Brier / log-loss / reliability-curve helpers.
- ``meta_learner``     — stacking logistic regression over delta,
  XGB probability, protection score, IV rank, and vol ratio.
- ``pop_engine``       — consumer-facing POP surface. Accepts a
  FeatureVector, produces calibrated probabilities with confidence
  bands. Backwards-compatible shim for legacy positional callers.
- ``regime_weights``   — DB-backed weights with Bayesian posterior
  updates from realised credit-spread outcomes.
- ``ledger``           — long-running PredictionLedger ORM table that
  records every published prediction tagged with model_hash and
  schema_hash for postmortem replay.

The options-IV feature stage (``feature_catalog.OPTIONS_FEATURES``) is
currently empty: it was sourced from an IV cache that was never wired into
the predictor. Re-add it once a real options-IV feed is connected.

Optional dependencies: xgboost, scikit-learn, pandas, joblib. Without
them the agent runs without ML predictions; strategies fall back to
chain-only strike selection.
"""
