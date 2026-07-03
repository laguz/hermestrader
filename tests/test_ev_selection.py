"""EV-based strike selection.

Among candidates clearing the POP floor, the winner is the highest expected
value under the strategy's own TP/SL policy — not the POP-closest-to-target
strike. These tests pin:

- the per-strategy EV arithmetic (CS75's 50%-capture / 2.5× stop, CS7's
  2%-of-width TP / 3× stop),
- selection preferring a farther strike whose EV is higher despite a POP
  further from target,
- a min-credit failure on one candidate no longer killing the whole side
  when another qualifying candidate prices fine,
- ev recorded on strategy_params.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import CreditSpreads7, CreditSpreads75

from ._stubs import StubBroker, StubDB


def _expirations_for(*dte_values):
    today = date.today()
    return [(today + timedelta(days=d)).isoformat() for d in dte_values]


def _build(strategy_cls, broker):
    db = StubDB()
    mm = MoneyManager(broker, db, {})
    return strategy_cls(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm), config={}, dry_run=False,
    )


def _put(expiry_ymd: str, strike: float, delta: float, mid: float) -> dict:
    occ = f"AAPL{expiry_ymd}P{int(strike * 1000):08d}"
    return {"symbol": occ, "strike": strike, "option_type": "put",
            "greeks": {"delta": -delta}, "bid": mid - 0.02, "ask": mid + 0.02}


def _analysis(levels):
    return {
        "symbol": "AAPL", "current_price": 100.0,
        "current_vol": 0.20, "avg_vol": 0.20,
        "key_levels": [{"price": p, "type": "support", "strength": 5}
                       for p in levels],
        "samples": 100, "period": "3m",
    }


# ── EV arithmetic ────────────────────────────────────────────────────────────
async def test_cs75_ev_matches_policy_arithmetic():
    s = _build(CreditSpreads75, StubBroker())
    t = await s.load_tunables()
    # 43 DTE, credit 1.25, width 5: TP captures 50% of credit, SL loses
    # min(1.5×credit, width−credit) = 1.875. At pop=0.75 the EV nets zero —
    # the canonical break-even anchor for a 25%-of-width credit.
    ev = s._expected_value(pop=0.75, credit=1.25, width=5.0, dte=43, t=t)
    assert ev == pytest.approx(0.75 * 0.625 - 0.25 * 1.875, abs=1e-9)
    assert ev == pytest.approx(0.0, abs=1e-9)


async def test_cs7_ev_matches_policy_arithmetic():
    s = _build(CreditSpreads7, StubBroker())
    t = await s.load_tunables()
    # 7 DTE, credit 0.24, width 1: TP profit = 0.24 − 0.02×1 = 0.22;
    # SL loss = min(2×0.24, 1−0.24) = 0.48.
    ev = s._expected_value(pop=0.80, credit=0.24, width=1.0, dte=7, t=t)
    assert ev == pytest.approx(0.80 * 0.22 - 0.20 * 0.48, abs=1e-9)


# ── selection behavior ───────────────────────────────────────────────────────
def _two_level_chain(expiry: str):
    """97-strike: pop 0.75 exactly (POP-closest-to-target), credit 0.16 →
    EV ≈ 0.025. 93-strike: pop ~0.80 (farther from target — protection from
    the 97 support lifts it above 1−|Δ|), credit 0.18 → EV ≈ 0.056.
    POP-proximity picks 97; EV must pick 93."""
    ymd = date.fromisoformat(expiry).strftime("%y%m%d")
    return [
        _put(ymd, 97.0, 0.25, 0.26),   # short candidate A (level 97)
        _put(ymd, 96.0, 0.21, 0.10),   # A's long leg
        _put(ymd, 93.0, 0.21, 0.24),   # short candidate B (level 93)
        _put(ymd, 92.0, 0.17, 0.06),   # B's long leg
    ]


async def test_selection_prefers_higher_ev_over_pop_proximity():
    expiry = _expirations_for(7)[0]
    broker = StubBroker(expirations=[expiry])
    broker.get_option_chains = lambda symbol, exp: _two_level_chain(expiry)
    broker.analyze_symbol = lambda symbol, period="6m": _analysis([97.0, 93.0])

    s = _build(CreditSpreads7, broker)
    actions = await s.execute_entries(["AAPL"])
    puts = [a for a in actions if a.strategy_params.get("side_type") == "put"]
    assert len(puts) == 1
    sp = puts[0].strategy_params
    assert sp["short_leg"].endswith("P00093000")
    # Winner is the farther strike: its POP sits above target (0.80 vs the
    # 97-strike's exactly-at-target 0.75) yet its EV is higher.
    pop_b = sp["pop"]
    assert pop_b > 0.76
    assert sp["ev"] > 0
    # EV matches CS7 policy arithmetic for the winning candidate
    # (credit 0.18, width 1): pop×(credit − 0.02×w) − (1−pop)×min(2×credit, w−credit).
    expected_ev = pop_b * (0.18 - 0.02) - (1 - pop_b) * min(2 * 0.18, 1 - 0.18)
    assert sp["ev"] == pytest.approx(expected_ev, abs=1e-4)


async def test_min_credit_failure_on_one_candidate_does_not_kill_the_side():
    """Old flow: POP picked the 97-strike, its thin credit failed the floor,
    the whole side was skipped. Now the 93-strike still qualifies."""
    expiry = _expirations_for(7)[0]
    ymd = date.fromisoformat(expiry).strftime("%y%m%d")
    chain = [
        _put(ymd, 97.0, 0.25, 0.10),   # credit 0.04 < min 0.12 → rejected
        _put(ymd, 96.0, 0.21, 0.06),
        _put(ymd, 93.0, 0.21, 0.24),   # credit 0.18 ≥ min → wins
        _put(ymd, 92.0, 0.17, 0.06),
    ]
    broker = StubBroker(expirations=[expiry])
    broker.get_option_chains = lambda symbol, exp: chain
    broker.analyze_symbol = lambda symbol, period="6m": _analysis([97.0, 93.0])

    s = _build(CreditSpreads7, broker)
    actions = await s.execute_entries(["AAPL"])
    puts = [a for a in actions if a.strategy_params.get("side_type") == "put"]
    assert len(puts) == 1
    assert puts[0].strategy_params["short_leg"].endswith("P00093000")


async def test_all_candidates_failing_credit_logs_credit_reason():
    expiry = _expirations_for(7)[0]
    ymd = date.fromisoformat(expiry).strftime("%y%m%d")
    chain = [
        _put(ymd, 97.0, 0.25, 0.10),   # credit 0.04 < min 0.12
        _put(ymd, 96.0, 0.21, 0.06),
    ]
    broker = StubBroker(expirations=[expiry])
    broker.get_option_chains = lambda symbol, exp: chain
    broker.analyze_symbol = lambda symbol, period="6m": _analysis([97.0])

    s = _build(CreditSpreads7, broker)
    actions = await s.execute_entries(["AAPL"])
    assert [a for a in actions
            if a.strategy_params.get("side_type") == "put"] == []
    assert any("credit $0.04 < min" in line for line in s.execution_logs)
