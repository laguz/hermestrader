"""Phase-2 Thompson-bandit knob tuner.

Covers the pure decision layer (reward shaping, arm posteriors, Thompson
convergence, proposal assembly) and the engine integration (shadow audits but
never mutates; active applies actionable+changed proposals, clamped to grids
and gated by autonomy).
"""
from __future__ import annotations

import os
import random
from datetime import date, datetime, timedelta

import pytest

from hermes.db.models import HermesDB, Trade
from hermes.ml.bandit import (
    KnobBandit,
    LEARNABLE_KNOBS,
    normalized_reward,
    propose_knob_updates,
)
from hermes.service1_agent.core import CascadingEngine


# --------------------------------------------------------------------------- #
# reward shaping
# --------------------------------------------------------------------------- #
def test_normalized_reward_is_bounded_and_centred():
    assert normalized_reward(0.0) == 0.5                      # breakeven
    assert normalized_reward(10_000.0, risk_scale=500) > 0.99  # big win → ~1
    assert normalized_reward(-10_000.0, risk_scale=500) < 0.01  # big loss → ~0
    # Monotone in P&L.
    assert normalized_reward(50) < normalized_reward(150)


# --------------------------------------------------------------------------- #
# arm posteriors + Thompson selection
# --------------------------------------------------------------------------- #
def test_bandit_update_routes_to_nearest_arm():
    b = KnobBandit("cs75_pop_target", (0.70, 0.75, 0.80, 0.85))
    b.update(0.79, 1.0)                                       # nearest 0.80
    arms = {a["value"]: a for a in b.summary()}
    assert arms[0.80]["n"] == 1
    assert arms[0.80]["alpha"] == 2.0                         # 1 prior + reward
    assert arms[0.75]["n"] == 0


def test_thompson_converges_to_the_winning_arm():
    b = KnobBandit("cs75_pop_target", (0.70, 0.75, 0.80, 0.85))
    for _ in range(40):
        b.update(0.80, 0.95)                                  # 0.80 wins
        b.update(0.70, 0.05)                                  # 0.70 loses
    rng = random.Random(7)
    picks = [b.select(rng) for _ in range(200)]
    assert picks.count(0.80) > 150                            # dominant choice


# --------------------------------------------------------------------------- #
# proposal assembly
# --------------------------------------------------------------------------- #
def _outcome(strategy, pnl, knobs, *, width=5.0):
    return {
        "strategy_id": strategy,
        "realized_pnl": pnl,
        "won": pnl > 0,
        "entry_features": {"width": width, "knobs": knobs},
    }


def test_proposals_cover_every_learnable_knob():
    props = propose_knob_updates([], {}, seed=1)
    keys = {(p["strategy_id"], p["key"]) for p in props}
    expected = {(s, k) for s, knobs in LEARNABLE_KNOBS.items() for k in knobs}
    assert keys == expected
    # Cold start: every proposed value is a valid grid arm, nothing actionable.
    for p in props:
        arms = [a["value"] for a in p["arms"]]
        assert p["proposed"] in arms
        assert p["actionable"] is False                       # 0 obs < default 20


def test_proposal_ranks_the_profitable_setting_highest():
    # Asserting the learned *ranking* (deterministic) rather than a single
    # Thompson draw — the bandit is *meant* to occasionally explore an untried
    # arm, so a one-shot pick is not a stable contract.
    rows = []
    for _ in range(30):
        rows.append(_outcome("CS75", 80.0, {"cs75_pop_target": 0.80}))
        rows.append(_outcome("CS75", -120.0, {"cs75_pop_target": 0.70}))
    props = {p["key"]: p for p in propose_knob_updates(
        rows, {"cs75_pop_target": 0.70}, min_observations=10, seed=1)}
    pop = props["cs75_pop_target"]

    arms = {a["value"]: a for a in pop["arms"]}
    # The profitable setting earns the highest posterior mean of all arms.
    best = max(pop["arms"], key=lambda a: a["posterior_mean"])
    assert best["value"] == 0.80
    assert arms[0.80]["posterior_mean"] > arms[0.70]["posterior_mean"]
    assert pop["n_obs"] == 60
    assert pop["actionable"] is True                          # 60 >= 10
    assert pop["proposed"] in arms                            # always a valid arm


# --------------------------------------------------------------------------- #
# engine integration: shadow vs active
# --------------------------------------------------------------------------- #
@pytest.fixture
def db():
    f = "test_bandit.db"
    if os.path.exists(f):
        os.remove(f)
    inst = HermesDB(f"sqlite:///{f}")
    yield inst
    inst.engine.dispose()
    if os.path.exists(f):
        os.remove(f)


class _StubOverseer:
    def __init__(self, autonomy):
        self.autonomy = autonomy


async def _seed_closed_trades(db, n=6, *, pop_value=0.80, pnl=90.0):
    await db.ensure_strategies({"CS75": 1})
    async with db.AsyncSession() as s:
        for i in range(n):
            s.add(Trade(
                id=i + 1, strategy_id="CS75", symbol="TSLA", side_type="call",
                short_leg=f"TSLA260717C0044{i}000", width=5.0, lots=1,
                entry_credit=1.25, pnl=pnl, status="CLOSED", close_reason="TP-50",
                opened_at=date.today(),
                entry_features={"width": 5.0, "knobs": {"cs75_pop_target": pop_value,
                                                        "cs75_sl_mult": 2.5}},
            ))
        await s.commit()


@pytest.mark.asyncio
async def test_shadow_mode_audits_without_mutating(db):
    await _seed_closed_trades(db)
    await db.set_setting("bandit_tuner_mode", "shadow")
    engine = CascadingEngine(
        broker=object(), db=db, strategies=[],
        config={"bandit_min_observations": 3, "bandit_tuning_interval_s": 0})

    await engine.tuning._maybe_run_bandit_tuner()

    # Audited but no knob mutated.
    assert await db.get_setting("cs75_pop_target") is None
    decisions = await db.recent_ai_decisions(strategy_id="BANDIT")
    assert len(decisions) == 1
    assert decisions[0]["decision"]["mode"] == "shadow"
    assert decisions[0]["decision"]["applied"] == {}


@pytest.mark.asyncio
async def test_active_mode_applies_actionable_changes(db):
    await _seed_closed_trades(db)
    await db.set_setting("bandit_tuner_mode", "active")
    engine = CascadingEngine(
        broker=object(), db=db, strategies=[],
        overseer=_StubOverseer("enforcing"),
        config={"bandit_min_observations": 3, "bandit_tuning_interval_s": 0})

    await engine.tuning._maybe_run_bandit_tuner()

    # An actionable + changed knob was written to settings.
    applied_val = await db.get_setting("cs75_pop_target")
    assert applied_val is not None
    assert float(applied_val) in LEARNABLE_KNOBS["CS75"]["cs75_pop_target"]


@pytest.mark.asyncio
async def test_active_mode_blocked_when_autonomy_advisory(db):
    await _seed_closed_trades(db)
    await db.set_setting("bandit_tuner_mode", "active")
    engine = CascadingEngine(
        broker=object(), db=db, strategies=[],
        overseer=_StubOverseer("advisory"),
        config={"bandit_min_observations": 3, "bandit_tuning_interval_s": 0})

    await engine.tuning._maybe_run_bandit_tuner()

    # Advisory autonomy never mutates a live setting, even in active mode.
    assert await db.get_setting("cs75_pop_target") is None


@pytest.mark.asyncio
async def test_off_mode_is_a_noop(db):
    await _seed_closed_trades(db)
    # No bandit_tuner_mode set → defaults to off.
    engine = CascadingEngine(
        broker=object(), db=db, strategies=[],
        config={"bandit_min_observations": 3, "bandit_tuning_interval_s": 0})

    await engine.tuning._maybe_run_bandit_tuner()

    assert await db.recent_ai_decisions(strategy_id="BANDIT") == []
    assert await db.get_setting("bandit_last_run_ts") is None
