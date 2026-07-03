"""POP-engine honesty regressions.

The legacy combiner's uncentered vol-ratio and protection terms added a
constant ~+0.7 log-odds that inflated POP 10–20 points above the
delta-implied probability (a 43Δ short scored 76% "POP" in production),
and the entry gate ran on that inflated overlay instead of the chain's
own implied delta. These tests pin the fixes:

- neutral inputs → POP == 1-|delta| exactly (no constant optimism),
- entry gating uses the actual chain delta at the candidate expiry,
- stale / in-process prediction handling around the POP inputs.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from hermes.ml.pop_engine import FeatureVector, augment_levels_with_pop, predict_pop
from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import CreditSpreads7

from ._stubs import StubBroker, StubDB


# ── combiner honesty ─────────────────────────────────────────────────────────
def _neutral_fv(delta: float, **overrides) -> FeatureVector:
    base = dict(delta=delta, xgb_prob=0.5, current_vol=0.25, avg_vol=0.25,
                protection_score=1.0)
    base.update(overrides)
    return FeatureVector(**base)


def test_neutral_inputs_yield_delta_implied_pop():
    """With neutral XGB, vol ratio 1 and baseline protection, POP must equal
    the delta-implied probability — no constant optimism offset."""
    for delta in (0.10, 0.16, 0.25, 0.4338):
        pop = predict_pop(_neutral_fv(delta))
        assert pop == pytest.approx(1.0 - delta, abs=1e-9), f"delta={delta}"


def test_43_delta_short_scores_near_57_not_76():
    """Field regression: a 0.4338Δ short call was scored 76% POP in
    production. Honest scoring puts it at its market-implied ~56.6%."""
    pop = predict_pop(_neutral_fv(0.4338, side="call"))
    assert pop == pytest.approx(0.5662, abs=1e-4)
    assert pop < 0.75


def test_elevated_vol_and_protection_still_move_pop():
    """Centering must not neuter the features — deviations from neutral
    still shift POP in the documented direction."""
    base = predict_pop(_neutral_fv(0.25))
    high_vol = predict_pop(_neutral_fv(0.25, current_vol=0.50))
    protected = predict_pop(_neutral_fv(0.25, protection_score=2.0))
    assert high_vol > base
    assert protected > base


def test_augment_stashes_xgb_prob_on_analysis():
    analysis = {
        "current_price": 100.0, "current_vol": 0.20, "avg_vol": 0.20,
        "key_levels": [{"price": 90.0, "type": "support", "strength": 5}],
    }
    out = augment_levels_with_pop(analysis, {"predicted_prob": 0.62})
    assert out["xgb_prob"] == pytest.approx(0.62)
    assert "protection" in out["key_levels"][0]


# ── strategy gate on chain delta ─────────────────────────────────────────────
def _expirations_for(*dte_values):
    today = date.today()
    return [(today + timedelta(days=d)).isoformat() for d in dte_values]


def _cs7(broker):
    db = StubDB()
    mm = MoneyManager(broker, db, {})
    return CreditSpreads7(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm), config={}, dry_run=False,
    ), db


async def test_entry_gate_rejects_high_delta_strike_despite_inflated_level_pop():
    """A level whose overlay POP claims 0.80 but whose nearest chain strike
    carries a 0.425Δ (implied POP ~57.5%) must NOT pass a 0.75 gate."""
    broker = StubBroker(expirations=_expirations_for(7))
    # Stub chain: put delta at strike 97 = 0.5 + (97-100)/40 = 0.425.
    broker.analyze_symbol = lambda symbol, period="6m": {
        "symbol": symbol, "current_price": 100.0,
        "current_vol": 0.20, "avg_vol": 0.20,
        "key_levels": [{"price": 97.0, "type": "support", "strength": 5,
                        "pop": 0.80}],
        "samples": 100, "period": period,
    }
    s, db = _cs7(broker)
    actions = await s.execute_entries(["AAPL"])
    assert actions == []
    # execution_logs is appended synchronously by _log (db.logs goes via a
    # fire-and-forget task that may not have run yet inside an async test).
    assert any("no ≥75% POP" in line for line in s.execution_logs)


async def test_entry_gate_passes_and_records_honest_chain_pop():
    """A 25Δ chain strike (implied POP 0.75) passes the 0.75 gate and the
    recorded strategy_params.pop is the honest number, not the overlay's."""
    broker = StubBroker(expirations=_expirations_for(7))
    broker.analyze_symbol = lambda symbol, period="6m": {
        "symbol": symbol, "current_price": 100.0,
        "current_vol": 0.20, "avg_vol": 0.20,
        # Overlay claims a wildly inflated 0.99 — must not leak into the row.
        "key_levels": [{"price": 90.0, "type": "support", "strength": 5,
                        "pop": 0.99}],
        "samples": 100, "period": period,
    }
    s, db = _cs7(broker)
    actions = await s.execute_entries(["AAPL"])
    puts = [a for a in actions if a.strategy_params.get("side_type") == "put"]
    assert len(puts) == 1
    recorded = puts[0].strategy_params["pop"]
    short_delta = puts[0].strategy_params["short_delta"]
    assert recorded == pytest.approx(1.0 - short_delta, abs=1e-6)
    assert recorded != pytest.approx(0.99, abs=1e-3)


# ── prediction freshness / in-process source ────────────────────────────────
def test_drop_stale_pred_neutralises_old_predictions():
    broker = StubBroker()
    s, _ = _cs7(broker)
    now = datetime.now(timezone.utc)
    fresh = {"predicted_return": 0.02, "asof": now - timedelta(hours=2)}
    stale = {"predicted_return": 0.02, "asof": now - timedelta(days=3)}
    legacy = {"predicted_return": 0.02}          # pre-upgrade row: no asof
    assert s._drop_stale_pred(dict(fresh)) == fresh
    assert s._drop_stale_pred(dict(stale)) == {}
    assert s._drop_stale_pred(dict(legacy)) == legacy


def test_in_process_predictor_preferred_over_db_row():
    broker = StubBroker()
    in_proc = {"predicted_prob": 0.61, "predicted_return": 0.01,
               "asof": datetime.now(timezone.utc)}
    db = StubDB()
    mm = MoneyManager(broker, db, {})
    s = CreditSpreads7(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm),
        config={"xgb_predict_latest": lambda sym: in_proc}, dry_run=False,
    )
    assert s._latest_xgb_pred("AAPL") == in_proc
    # Hook absent → None so the caller falls back to the DB row.
    s.config = {}
    assert s._latest_xgb_pred("AAPL") is None
