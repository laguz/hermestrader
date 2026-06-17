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
    """Estimate a tabular hold/close policy from exit-state trajectories.

    ``ticks`` are ``HermesDB.fetch_exit_ticks`` rows. Returns::

        {
          "min_support", "margin",
          "n_ticks", "n_completed_trajectories",
          "states": { state_key: {
              "q_close", "q_hold",        # mean P&L fraction (or None)
              "n_close", "n_hold",        # observation counts
              "recommend",                # 'hold' | 'close'
          } },
        }

    A state only recommends ``close`` when both Q values are known, the
    completed-trajectory support (``n_hold``) meets ``min_support``, and closing
    beats holding by more than ``margin``.
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

    close_vals: Dict[str, List[float]] = defaultdict(list)   # immediate value
    hold_vals: Dict[str, List[float]] = defaultdict(list)    # held-out final

    n_ticks = 0
    for tid, rows in by_trade.items():
        for r in rows:
            pnl = _f(r.get("unrealized_pnl_pct"))
            dte = _f(r.get("dte"))
            if pnl is None or dte is None:
                continue
            n_ticks += 1
            s = state_key(pnl, dte)
            close_vals[s].append(pnl)               # closing locks ~current pnl
            if r.get("action") == "hold" and tid in final_pnl:
                hold_vals[s].append(final_pnl[tid])  # what holding led to

    states: Dict[str, Any] = {}
    for s in set(close_vals) | set(hold_vals):
        cv, hv = close_vals.get(s, []), hold_vals.get(s, [])
        q_close = round(mean(cv), 4) if cv else None
        q_hold = round(mean(hv), 4) if hv else None
        recommend = "hold"
        if (q_close is not None and q_hold is not None
                and len(hv) >= min_support and q_close > q_hold + margin):
            recommend = "close"
        states[s] = {
            "q_close": q_close,
            "q_hold": q_hold,
            "n_close": len(cv),
            "n_hold": len(hv),
            "recommend": recommend,
        }

    return {
        "min_support": min_support,
        "margin": margin,
        "n_ticks": n_ticks,
        "n_completed_trajectories": len(final_pnl),
        "states": states,
    }


def recommend(policy: Dict[str, Any], pnl_pct: float, dte: float) -> Dict[str, Any]:
    """Look up the advisory action for a live ``(pnl%, dte)`` state.

    Returns ``{state, action, q_close, q_hold, support, confident}``. An unseen
    state (no training data) yields ``action='hold'`` with ``confident=False`` —
    the policy never recommends closing on a state it has not observed.
    """
    s = state_key(pnl_pct, dte)
    st = (policy.get("states") or {}).get(s)
    if not st:
        return {"state": s, "action": "hold", "q_close": None, "q_hold": None,
                "support": 0, "confident": False}
    action = st["recommend"]
    support = st["n_hold"]
    return {
        "state": s,
        "action": action,
        "q_close": st["q_close"],
        "q_hold": st["q_hold"],
        "support": support,
        "confident": action == "close" and support >= policy.get("min_support", 10),
    }
