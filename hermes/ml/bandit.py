"""
[Thompson-Bandit] — outcome-driven knob selection over closed trades.

Why this exists
---------------
Phases 0–1 record each trade's knobs + context and report which settings made
money. This module is the **decision layer**: a Thompson-sampling bandit that
picks, per strategy and per knob, which value to use next — learning from the
realized P&L of trades taken under each value.

It is deliberately the smallest thing that closes the loop:

- **One bandit per (strategy, knob).** Each candidate knob value is an *arm*.
  The arm grids (``LEARNABLE_KNOBS``) are a hard safety boundary — every value
  sits inside the knob's existing tunable range, so the bandit can never pick a
  setting an operator couldn't have set by hand.
- **Beta posteriors with a saturating P&L reward.** A trade's realized P&L is
  squashed to a reward in ``[0, 1]`` (0.5 = breakeven) and folded into the
  arm's Beta(α, β). Big wins push α, big losses push β; magnitude matters, not
  just win/loss. Beta + Thompson gives principled exploration under the sparse,
  slow reward that options trades produce.
- **Cold-start safe.** With no data every arm is Beta(1, 1) → uniform random,
  i.e. pure exploration. The *caller* decides whether to act on that (see
  ``actionable`` / ``min_observations``); this module only proposes.

This module is pure: it takes outcome rows + current settings and returns
proposals. It never touches the DB or mutates a setting — the engine does that,
under a mode flag, clamped again on the way out.
"""
from __future__ import annotations

import math
import random
from typing import Any, Dict, List, Optional, Sequence

# ---------------------------------------------------------------------------
# Action space — the candidate value grid per knob, per strategy.
# Every value MUST sit inside that knob's tunable min/max (see tunables.py).
# This registry is the bandit's hard safety boundary: it can only ever select a
# value listed here. Curated to entry-stringency + exit knobs; widths, lots and
# DTE windows are intentionally left to the operator.
# ---------------------------------------------------------------------------
LEARNABLE_KNOBS: Dict[str, Dict[str, Sequence[float]]] = {
    "CS75": {
        "cs75_pop_target": (0.70, 0.75, 0.80, 0.85),
        "cs75_short_delta_max": (0.30, 0.35, 0.40, 0.45),
        "cs75_min_credit_pct_far": (0.20, 0.25, 0.30, 0.35),
        "cs75_tp_pct_far": (0.40, 0.50, 0.60),
        "cs75_sl_mult": (2.0, 2.5, 3.0),
    },
    "CS7": {
        "cs7_pop_target": (0.70, 0.75, 0.80, 0.85),
        "cs7_short_delta_max": (0.35, 0.40, 0.45),
        "cs7_min_credit_pct": (0.10, 0.12, 0.15, 0.20),
        "cs7_sl_mult": (2.5, 3.0, 3.5),
    },
    "TT45": {
        "tt45_delta": (0.12, 0.16, 0.20),
        "tt45_challenged_delta": (0.25, 0.30, 0.35),
    },
    "WHEEL": {
        "wheel_delta": (0.25, 0.30, 0.35),
        "wheel_min_pop": (0.45, 0.50, 0.55, 0.60),
    },
}

# Default reward scale (dollars) when a trade carries no width to size risk by.
_DEFAULT_RISK_SCALE = 100.0


def normalized_reward(pnl: float, *, risk_scale: float = _DEFAULT_RISK_SCALE) -> float:
    """Squash realized P&L to a reward in ``[0, 1]`` (0.5 == breakeven).

    ``tanh`` saturates, so a single outlier can't dominate the posterior while
    direction and rough magnitude are preserved. ``risk_scale`` sets the
    sensitivity — typically the spread's per-contract max loss (width × 100).
    """
    scale = risk_scale if risk_scale and risk_scale > 0 else _DEFAULT_RISK_SCALE
    return 0.5 + 0.5 * math.tanh(float(pnl) / scale)


def _nearest(value: float, arms: Sequence[float]) -> float:
    return min(arms, key=lambda a: abs(a - value))


class KnobBandit:
    """Beta-Bernoulli Thompson sampler over one knob's candidate values."""

    def __init__(self, key: str, arms: Sequence[float]):
        self.key = key
        self.arms: List[float] = list(arms)
        # Uniform Beta(1, 1) prior per arm — cold start is pure exploration.
        self._alpha: Dict[float, float] = {a: 1.0 for a in self.arms}
        self._beta: Dict[float, float] = {a: 1.0 for a in self.arms}
        self._n: Dict[float, int] = {a: 0 for a in self.arms}

    def update(self, value: float, reward: float) -> None:
        """Fold one trade's reward into the arm nearest the value it used."""
        arm = _nearest(float(value), self.arms)
        r = max(0.0, min(1.0, float(reward)))
        self._alpha[arm] += r
        self._beta[arm] += 1.0 - r
        self._n[arm] += 1

    def select(self, rng: Optional[random.Random] = None) -> float:
        """Thompson pick: sample θ ~ Beta(α, β) per arm, return the argmax."""
        rng = rng or random
        best_arm, best_theta = self.arms[0], -1.0
        for arm in self.arms:
            theta = rng.betavariate(self._alpha[arm], self._beta[arm])
            if theta > best_theta:
                best_arm, best_theta = arm, theta
        return best_arm

    @property
    def total_n(self) -> int:
        return sum(self._n.values())

    def summary(self) -> List[Dict[str, Any]]:
        """Per-arm posterior, newest-first by observation count then value."""
        out = []
        for arm in self.arms:
            a, b = self._alpha[arm], self._beta[arm]
            out.append({
                "value": arm,
                "n": self._n[arm],
                "alpha": round(a, 4),
                "beta": round(b, 4),
                "posterior_mean": round(a / (a + b), 4),   # E[reward]
            })
        return out


def _knob_value(row: Dict[str, Any], key: str) -> Optional[float]:
    ef = row.get("entry_features")
    if not isinstance(ef, dict):
        return None
    knobs = ef.get("knobs") or {}
    val = knobs.get(key)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _risk_scale(row: Dict[str, Any]) -> float:
    ef = row.get("entry_features") or {}
    width = ef.get("width") if isinstance(ef, dict) else None
    try:
        if width:
            return float(width) * 100.0
    except (TypeError, ValueError):
        pass
    return _DEFAULT_RISK_SCALE


def propose_knob_updates(
    outcomes: Sequence[Dict[str, Any]],
    current_settings: Optional[Dict[str, Any]] = None,
    *,
    min_observations: int = 20,
    seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Train per-knob bandits on ``outcomes`` and return Thompson proposals.

    ``outcomes`` are ``HermesDB.fetch_trade_outcomes`` rows. ``current_settings``
    maps knob key → its live value (for the ``current``/``changed`` fields).

    Returns one proposal dict per learnable knob that has a candidate grid::

        {
          "strategy_id", "key",
          "current": float|None,        # live setting (if known)
          "proposed": float,            # Thompson-selected arm
          "changed": bool,              # proposed != current
          "n_obs": int,                 # attributed trades for this knob
          "actionable": bool,           # n_obs >= min_observations
          "arms": [ {value, n, alpha, beta, posterior_mean}, ... ],
        }

    ``actionable`` is the engine's gate for *applying* a change: below
    ``min_observations`` the bandit still proposes (so shadow mode can show its
    exploration) but the caller should not mutate a live setting on thin data.
    """
    current = current_settings or {}
    rng = random.Random(seed)
    proposals: List[Dict[str, Any]] = []

    for strategy_id, knobs in LEARNABLE_KNOBS.items():
        sid_rows = [r for r in outcomes
                    if str(r.get("strategy_id") or "") == strategy_id
                    and r.get("realized_pnl") is not None]
        for key, arms in knobs.items():
            bandit = KnobBandit(key, arms)
            for r in sid_rows:
                val = _knob_value(r, key)
                if val is None:
                    continue
                reward = normalized_reward(
                    float(r["realized_pnl"]), risk_scale=_risk_scale(r))
                bandit.update(val, reward)

            proposed = bandit.select(rng)
            cur_raw = current.get(key)
            try:
                cur = float(cur_raw) if cur_raw is not None else None
            except (TypeError, ValueError):
                cur = None
            n_obs = bandit.total_n
            proposals.append({
                "strategy_id": strategy_id,
                "key": key,
                "current": cur,
                "proposed": proposed,
                "changed": cur is None or abs(proposed - cur) > 1e-9,
                "n_obs": n_obs,
                "actionable": n_obs >= min_observations,
                "arms": bandit.summary(),
            })

    return proposals
