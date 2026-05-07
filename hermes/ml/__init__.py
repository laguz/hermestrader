"""Machine-learning forecasting layer.

- ``xgb_features.FeatureEngineer`` builds the 10-feature alpha set the spec
  mandates from daily and intraday bars plus SPY for beta-residual.
- ``xgb_features.AsyncXGBPredictor`` runs an XGBoost regressor in a daemon
  thread, predicts every hour during the regular session, and retrains
  on a fixed cadence. Models are checkpointed to disk so a restart
  warm-starts from the last trained state.
- ``pop_engine`` centralises probability-of-profit calculations consumed
  by the strategies' strike selection.

Optional dependencies: xgboost, scikit-learn, pandas. Without them the
agent runs without ML predictions; strategies fall back to chain-only
strike selection.
"""
