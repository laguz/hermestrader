"""Probability-of-profit (POP) layer.

Phase 0 ships a single module:

- ``pop_engine`` — the consumer-facing POP surface. It discovers S/R key
  levels and scores credit-spread strikes with a log-odds combiner over
  delta, S/R protection, and the vol regime (weighted by regime weights,
  static by default or a DB-backed lookup when one is wired). This is
  *chain-only* POP — no XGB forecast, no meta-learner stacking.

The XGB predictor stack, regime-weight learning, the bandit / exit-policy
tuners, and the drift / ledger / attribution / backtester diagnostics were
removed in the Phase-0 teardown; they are re-introduced only when chain-only
POP is live and a refinement is shown to improve real entry decisions (see
``REBUILD.md``). ``pop_engine`` already degrades to this chain-only path, so
that re-introduction is purely additive.

Optional dependencies: numpy, pandas, scipy, scikit-learn (for key-level
clustering). Without an upstream forecast, ``augment_levels_with_pop`` scores
levels off delta / protection / vol alone.
"""
