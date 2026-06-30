"""
[Exit-Policy] — offline batch value estimation for exit timing.

Why this exists
---------------
Entry sizing is a one-shot choice (the Phase-2 bandit's domain). *Exit* is a
sequential one: every management tick the rules ask "hold or close?" of each
open position. That's a genuine MDP, and the place where a learned policy can
beat a fixed take-profit / stop-loss / time-exit rule — if it has data.

This module is the **learner**. Given the exit-state trajectories captured in
``exit_ticks`` (one ``(pnl%, dte)`` observation per open trade per tick, plus
whether it was held or closed), it estimates, per discretized state ``s``:

- ``Q(s, close)`` — value of closing now ≈ the P&L fraction you lock in at
  ``s`` (the mean ``unrealized_pnl_pct`` of observations in that bucket).
- ``Q(s, hold)``  — value of continuing ≈ the mean *final* P&L fraction of
  completed trajectories that were observed holding at ``s``.

It recommends **close** at ``s`` when ``Q(s, close) > Q(s, hold) + margin`` with
enough completed-trajectory support — i.e. "historically, positions here that
were held ended up worse than their current mark." Deliberately tabular and
explainable (the codebase's house style); a function approximator / Fitted-Q is
the natural upgrade once there's enough data to need generalization.

Pure: trains from rows, recommends from a policy dict. No I/O, no trade actions.
"""
from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence, Tuple

# State bucket edges. P&L fraction runs from deeply negative (losing more than
# the credit collected) up to ~1.0 (nearly max profit); DTE shrinks to expiry.
PNL_EDGES: Tuple[float, ...] = (-0.5, 0.0, 0.25, 0.5, 0.75)
DTE_EDGES: Tuple[int, ...] = (7, 14, 21, 30, 45)


def _bucket(value: float, edges: Sequence[float]) -> str:
    def _fmt(x: float) -> str:
        return f"{x:g}"
    if value < edges[0]:
        return f"<{_fmt(edges[0])}"
    for lo, hi in zip(edges, edges[1:]):
        if lo <= value < hi:
            return f"{_fmt(lo)}–{_fmt(hi)}"
    return f"≥{_fmt(edges[-1])}"


def state_key(pnl_pct: float, dte: float) -> str:
    """Discretize ``(pnl%, dte)`` into a human-readable state label."""
    return f"pnl:{_bucket(pnl_pct, PNL_EDGES)}|dte:{_bucket(dte, DTE_EDGES)}"


def _f(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def train_exit_policy(
    ticks: Sequence[Dict[str, Any]],
    *,
    min_support: int = 10,
    margin: float = 0.05,
) -> Dict[str, Any]:
    """Estimate a continuous exit policy from exit-state trajectories.

    ``ticks`` are ``HermesDB.fetch_exit_ticks`` rows.
    """
    by_trade: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for r in ticks:
        by_trade[r.get("trade_id")].append(r)

    # Final P&L fraction for completed trajectories (those with a 'close' tick).
    final_pnl: Dict[Any, float] = {}
    for tid, rows in by_trade.items():
        rows_sorted = sorted(rows, key=lambda r: r.get("ts") or 0)
        closes = [r for r in rows_sorted if r.get("action") == "close"]
        if closes:
            fv = _f(closes[-1].get("unrealized_pnl_pct"))
            if fv is not None:
                final_pnl[tid] = fv

    close_vals: Dict[str, List[float]] = defaultdict(list)
    hold_vals: Dict[str, List[float]] = defaultdict(list)

    X_train = []
    y_train = []

    n_ticks = 0
    for tid, rows in by_trade.items():
        for r in rows:
            pnl = _f(r.get("unrealized_pnl_pct"))
            dte = _f(r.get("dte"))
            if pnl is None or dte is None:
                continue
            n_ticks += 1
            s = state_key(pnl, dte)
            close_vals[s].append(pnl)
            if r.get("action") == "hold" and tid in final_pnl:
                hold_vals[s].append(final_pnl[tid])
                X_train.append([pnl, dte])
                y_train.append(final_pnl[tid])

    # Fit a continuous regression model if we have sufficient observations
    coef = None
    intercept = None
    if len(X_train) >= min_support:
        try:
            import numpy as np
            from sklearn.linear_model import LinearRegression
            X_arr = np.array(X_train)
            y_arr = np.array(y_train)
            X_poly = np.stack([
                X_arr[:, 0],
                X_arr[:, 1],
                X_arr[:, 0]**2,
                X_arr[:, 1]**2,
                X_arr[:, 0] * X_arr[:, 1]
            ], axis=1)
            model = LinearRegression()
            model.fit(X_poly, y_arr)
            coef = model.coef_.tolist()
            intercept = float(model.intercept_)
        except Exception:
            pass

    # Compute support and empirical averages per state bucket for fallback/reporting
    support_counts = {s: len(hv) for s, hv in hold_vals.items()}
    empirical_hold = {s: round(mean(hv), 4) for s, hv in hold_vals.items() if hv}

    # Backward-compatible states dict for diagnostics
    states: Dict[str, Any] = {}
    for s in set(close_vals) | set(hold_vals):
        cv = close_vals.get(s, [])
        hv = hold_vals.get(s, [])
        q_close = round(mean(cv), 4) if cv else None
        q_hold = round(mean(hv), 4) if hv else None
        recommend_action = "hold"
        if (q_close is not None and q_hold is not None
                and len(hv) >= min_support and q_close > q_hold + margin):
            recommend_action = "close"
        states[s] = {
            "q_close": q_close,
            "q_hold": q_hold,
            "n_close": len(cv),
            "n_hold": len(hv),
            "recommend": recommend_action,
        }

    return {
        "min_support": min_support,
        "margin": margin,
        "n_ticks": n_ticks,
        "n_completed_trajectories": len(final_pnl),
        "states": states,
        "coef": coef,
        "intercept": intercept,
        "support_counts": support_counts,
        "empirical_hold": empirical_hold,
    }


def recommend(policy: Dict[str, Any], pnl_pct: float, dte: float) -> Dict[str, Any]:
    """Look up the advisory action for a live ``(pnl%, dte)`` state using continuous model."""
    s = state_key(pnl_pct, dte)
    coef = policy.get("coef")
    intercept = policy.get("intercept")

    q_close = pnl_pct
    q_hold = None

    if coef is not None and intercept is not None:
        try:
            import numpy as np
            X_poly = np.array([pnl_pct, dte, pnl_pct**2, dte**2, pnl_pct * dte])
            q_hold = float(X_poly @ np.array(coef) + intercept)
        except Exception:
            pass

    # Fallback to empirical hold if prediction failed or model not fitted
    if q_hold is None:
        empirical_hold = policy.get("empirical_hold") or {}
        q_hold = empirical_hold.get(s)

    support_counts = policy.get("support_counts") or {}
    support = support_counts.get(s, 0)

    action = "hold"
    confident = False
    if q_close is not None and q_hold is not None:
        if q_close > q_hold + policy.get("margin", 0.05):
            if support >= policy.get("min_support", 10):
                action = "close"
                confident = True

    return {
        "state": s,
        "action": action,
        "q_close": q_close,
        "q_hold": q_hold,
        "support": support,
        "confident": confident,
    }
