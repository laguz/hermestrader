"""Phase-1 offline attribution — per-knob / per-feature expectancy.

Exercises ``hermes.ml.attribution.attribute_outcomes`` against synthetic
``fetch_trade_outcomes`` rows: headline stats, per-strategy split, market-
feature bucketing, knob grouping, unattributed counting, and small-sample
flagging.
"""
from __future__ import annotations

import math

from hermes.ml.attribution import (
    attribute_outcomes,
    _bucket_continuous,
    _summarize,
)


def _row(strategy, pnl, *, pop=None, knobs=None, hold_days=None,
         attributed=True):
    ef = None
    if attributed:
        ef = {"strategy_id": strategy, "pop": pop, "knobs": knobs or {}}
    return {
        "trade_id": id((strategy, pnl, pop)),
        "strategy_id": strategy,
        "symbol": "TSLA",
        "side_type": "call",
        "realized_pnl": pnl,
        "won": pnl > 0,
        "hold_days": hold_days,
        "close_reason": "TP-50" if pnl > 0 else "SL",
        "entry_features": ef,
    }


# --------------------------------------------------------------------------- #
# unit: bucketing + summary
# --------------------------------------------------------------------------- #
def test_bucket_continuous_ranges():
    edges = (0.70, 0.75, 0.80)
    assert _bucket_continuous(0.65, edges) == "<0.7"
    assert _bucket_continuous(0.72, edges) == "0.7–0.75"
    assert _bucket_continuous(0.75, edges) == "0.75–0.8"   # lower-inclusive
    assert _bucket_continuous(0.95, edges) == "≥0.8"


def test_summarize_math():
    s = _summarize([100.0, 100.0, -50.0])
    assert s["n"] == 3
    assert s["wins"] == 2 and s["losses"] == 1
    assert s["win_rate"] == round(2 / 3, 4)
    assert s["expectancy"] == round(150 / 3, 2)            # mean P&L
    assert s["total_pnl"] == 150.0
    assert s["profit_factor"] == 4.0                       # 200 / 50


def test_summarize_profit_factor_infinite_without_losses():
    s = _summarize([10.0, 20.0])
    assert math.isinf(s["profit_factor"])


# --------------------------------------------------------------------------- #
# report structure
# --------------------------------------------------------------------------- #
def test_overall_and_unattributed_counts():
    rows = [
        _row("CS75", 100.0, pop=0.82),
        _row("CS75", -40.0, pop=0.72),
        _row("CS7", 50.0, attributed=False),               # no snapshot
        {"strategy_id": "CS75", "realized_pnl": None,       # unpriced → dropped
         "entry_features": {}},
    ]
    rep = attribute_outcomes(rows)
    assert rep["n_trades"] == 3                            # the None-pnl row drops
    assert rep["n_attributed"] == 2
    assert rep["n_unattributed"] == 1
    assert rep["overall"]["total_pnl"] == 110.0           # 100 - 40 + 50


def test_by_strategy_split_and_hold_days():
    rows = [
        _row("CS75", 100.0, pop=0.82, hold_days=10.0),
        _row("CS75", -40.0, pop=0.72, hold_days=20.0),
        _row("WHEEL", 30.0, pop=0.55, hold_days=5.0),
    ]
    rep = attribute_outcomes(rows)
    assert set(rep["by_strategy"]) == {"CS75", "WHEEL"}
    cs75 = rep["by_strategy"]["CS75"]
    assert cs75["summary"]["n"] == 2
    assert cs75["avg_hold_days"] == 15.0
    assert rep["by_strategy"]["WHEEL"]["summary"]["total_pnl"] == 30.0


def test_feature_bucketing_separates_pop_ranges():
    rows = [
        _row("CS75", 100.0, pop=0.82),                     # 0.8–0.85 win
        _row("CS75", 80.0, pop=0.83),                      # 0.8–0.85 win
        _row("CS75", -50.0, pop=0.72),                     # 0.7–0.75 loss
    ]
    rep = attribute_outcomes(rows, min_bucket_n=2)
    pop_buckets = {b["bucket"]: b for b in rep["by_strategy"]["CS75"]["features"]["pop"]}
    assert pop_buckets["0.8–0.85"]["win_rate"] == 1.0
    assert pop_buckets["0.8–0.85"]["expectancy"] == 90.0
    assert pop_buckets["0.7–0.75"]["win_rate"] == 0.0
    assert pop_buckets["0.7–0.75"]["low_sample"] is True   # n=1 < 2
    assert pop_buckets["0.8–0.85"]["low_sample"] is False  # n=2


def test_knob_grouping_by_exact_value():
    # Two distinct pop_target settings; attribute outcomes to each "arm".
    rows = [
        _row("CS75", 60.0, pop=0.8, knobs={"cs75_pop_target": 0.75}),
        _row("CS75", 40.0, pop=0.8, knobs={"cs75_pop_target": 0.75}),
        _row("CS75", -30.0, pop=0.7, knobs={"cs75_pop_target": 0.80}),
    ]
    rep = attribute_outcomes(rows, min_bucket_n=1)
    knob = {b["bucket"]: b for b in rep["by_strategy"]["CS75"]["knobs"]["cs75_pop_target"]}
    assert set(knob) == {"0.75", "0.8"}
    assert knob["0.75"]["expectancy"] == 50.0              # (60+40)/2
    assert knob["0.8"]["win_rate"] == 0.0


def test_single_value_knob_with_low_sample_is_suppressed():
    # One trade, one knob value → tells us nothing, should not surface.
    rows = [_row("CS75", 60.0, pop=0.8, knobs={"cs75_pop_target": 0.75})]
    rep = attribute_outcomes(rows, min_bucket_n=5)
    assert rep["by_strategy"]["CS75"]["knobs"] == {}


def test_empty_input():
    rep = attribute_outcomes([])
    assert rep["n_trades"] == 0
    assert rep["overall"]["n"] == 0
    assert rep["by_strategy"] == {}
