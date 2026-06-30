"""
[Outcome-Attribution] — turn closed-trade outcomes into per-knob expectancy.

Why this exists
---------------
Phase 0 records an ``entry_features`` snapshot on every trade — the resolved
tunables ("knobs") plus the market context that produced the fill. Once those
trades close, ``HermesDB.fetch_trade_outcomes`` yields a labelled
``(context, knobs, realized P&L)`` row per trade.

This module is the **offline evaluator**: it groups those rows and answers the
question the static rules can't — *which entry conditions and which knob
settings actually made money?* e.g. "CS75 entries at POP 0.70–0.75 returned an
expectancy of $X over N trades; 0.80–0.85 returned $Y." That breakdown is what
a human operator (today) and the Phase-2 contextual bandit (later) use to
decide where to move each knob.

Design
------
- **Pure functions, no I/O, no numpy.** The caller fetches rows; we only
  compute. That keeps it trivially testable and cheap to call from a request.
- **Continuous market features** (POP, short delta, credit/width, DTE) are
  bucketed into human-meaningful ranges. **Knobs** are grouped by their exact
  value — each distinct setting is a candidate "arm".
- **Nothing is hidden, but small samples are flagged.** A bucket with fewer
  than ``min_bucket_n`` trades is still reported, with ``low_sample: true`` so
  a 100%-win-rate-off-two-trades artifact can't be mistaken for an edge.
"""
from __future__ import annotations

from datetime import datetime, timezone
from statistics import median, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Human-meaningful bucket edges for the continuous market features carried on
# the entry_features snapshot. Values fall into half-open [lo, hi) ranges;
# anything below the first / above the last edge gets its own open-ended bucket.
FEATURE_EDGES: Dict[str, Sequence[float]] = {
    "pop": (0.60, 0.70, 0.75, 0.80, 0.85, 0.90),
    "short_delta": (0.05, 0.10, 0.15, 0.20, 0.30, 0.40),
    "credit_width_ratio": (0.10, 0.15, 0.20, 0.25, 0.35, 0.50),
    "dte": (5, 10, 21, 30, 45, 60),
}

# Continuous features we attribute (the rest of entry_features is metadata).
_CONTINUOUS_FEATURES: Tuple[str, ...] = tuple(FEATURE_EDGES.keys())


def _bucket_continuous(value: float, edges: Sequence[float]) -> str:
    """Label the half-open range ``value`` falls into.

    ``edges = (a, b, c)`` yields buckets ``<a``, ``a–b``, ``b–c``, ``≥c``.
    """
    def _fmt(x: float) -> str:
        return f"{x:g}"

    if value < edges[0]:
        return f"<{_fmt(edges[0])}"
    for lo, hi in zip(edges, edges[1:]):
        if lo <= value < hi:
            return f"{_fmt(lo)}–{_fmt(hi)}"
    return f"≥{_fmt(edges[-1])}"


def _summarize(pnls: List[float]) -> Dict[str, Any]:
    """Aggregate realized-P&L stats for one group of trades."""
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    losses = n - wins
    total = sum(pnls)
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = -sum(p for p in pnls if p < 0)      # positive magnitude
    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / n, 4) if n else None,
        "expectancy": round(total / n, 2) if n else None,    # mean P&L / trade
        "total_pnl": round(total, 2),
        "median_pnl": round(median(pnls), 2) if n else None,
        "std_pnl": round(pstdev(pnls), 2) if n > 1 else 0.0,
        # Profit factor: gross profit / gross loss. ``inf`` when there are wins
        # and no losing dollars; ``None`` when there's nothing to divide.
        "profit_factor": (
            round(gross_win / gross_loss, 3) if gross_loss > 0
            else (float("inf") if gross_win > 0 else None)
        ),
    }


def _numeric(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _attribute_feature(
    rows: List[Dict[str, Any]],
    extract,
    *,
    bucketer,
    min_bucket_n: int,
) -> List[Dict[str, Any]]:
    """Bucket ``rows`` by ``bucketer(extract(row))`` and summarize each bucket.

    ``extract`` pulls the raw value off a row; rows whose value is missing are
    skipped. Buckets are returned sorted by descending trade count, each tagged
    ``low_sample`` when it has fewer than ``min_bucket_n`` trades.
    """
    groups: Dict[Any, List[float]] = {}
    sort_key: Dict[Any, float] = {}
    for row in rows:
        raw = extract(row)
        val = _numeric(raw)
        if val is None:
            continue
        label, order = bucketer(val)
        groups.setdefault(label, []).append(float(row["realized_pnl"]))
        sort_key.setdefault(label, order)

    out: List[Dict[str, Any]] = []
    for label, pnls in groups.items():
        summary = _summarize(pnls)
        summary["bucket"] = label
        summary["low_sample"] = summary["n"] < min_bucket_n
        summary["_order"] = sort_key[label]
        out.append(summary)
    out.sort(key=lambda b: b["_order"])
    for b in out:
        b.pop("_order", None)
    return out


def attribute_outcomes(
    rows: Sequence[Dict[str, Any]],
    *,
    min_bucket_n: int = 5,
) -> Dict[str, Any]:
    """Build the full attribution report from ``fetch_trade_outcomes`` rows.

    Returns a JSON-serializable bundle::

        {
          "generated_at": iso8601,
          "n_trades": int,            # rows with a realized P&L
          "n_attributed": int,        # rows that also carried entry_features
          "n_unattributed": int,      # closed before Phase-0 (no snapshot)
          "min_bucket_n": int,
          "overall": <summary>,
          "by_strategy": { strategy_id: {
              "summary": <summary>,
              "avg_hold_days": float|None,
              "features": { feature_name: [ <bucket>, ... ] },
              "knobs":    { knob_name:    [ <bucket>, ... ] },
          } },
        }

    A ``<summary>`` is the dict from ``_summarize`` (n, win_rate, expectancy,
    total_pnl, profit_factor, …). ``features`` covers the continuous market
    context; ``knobs`` covers each tunable, grouped by exact value so every
    distinct setting shows its realized expectancy.
    """
    priced = [r for r in rows if _numeric(r.get("realized_pnl")) is not None]
    attributed = [r for r in priced if isinstance(r.get("entry_features"), dict)]

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_trades": len(priced),
        "n_attributed": len(attributed),
        "n_unattributed": len(priced) - len(attributed),
        "min_bucket_n": min_bucket_n,
        "overall": _summarize([float(r["realized_pnl"]) for r in priced]),
        "by_strategy": {},
    }

    # Group priced rows by strategy for the headline summary; only the
    # attributed subset can be broken down by feature/knob.
    strategies = sorted({str(r.get("strategy_id") or "UNKNOWN") for r in priced})
    for sid in strategies:
        sid_priced = [r for r in priced
                      if str(r.get("strategy_id") or "UNKNOWN") == sid]
        sid_attr = [r for r in attributed
                    if str(r.get("strategy_id") or "UNKNOWN") == sid]

        holds = [_numeric(r.get("hold_days")) for r in sid_priced]
        holds = [h for h in holds if h is not None]
        avg_hold = round(sum(holds) / len(holds), 2) if holds else None

        # Continuous market features off the snapshot top level.
        features: Dict[str, List[Dict[str, Any]]] = {}
        for feat in _CONTINUOUS_FEATURES:
            edges = FEATURE_EDGES[feat]
            buckets = _attribute_feature(
                sid_attr,
                lambda r, f=feat: (r.get("entry_features") or {}).get(f),
                bucketer=lambda v, e=edges: (_bucket_continuous(v, e), v),
                min_bucket_n=min_bucket_n,
            )
            if buckets:
                features[feat] = buckets

        # Knobs: group by exact (rounded) value — each setting is one arm.
        knob_names = set()
        for r in sid_attr:
            knobs = (r.get("entry_features") or {}).get("knobs") or {}
            knob_names.update(knobs.keys())
        knob_report: Dict[str, List[Dict[str, Any]]] = {}
        for knob in sorted(knob_names):
            buckets = _attribute_feature(
                sid_attr,
                lambda r, k=knob: ((r.get("entry_features") or {}).get("knobs") or {}).get(k),
                bucketer=lambda v: (f"{round(v, 4):g}", v),
                min_bucket_n=min_bucket_n,
            )
            # Only surface a knob that actually varied or has enough data to
            # judge — a single-value knob with one trade tells us nothing.
            if buckets and (len(buckets) > 1 or buckets[0]["n"] >= min_bucket_n):
                knob_report[knob] = buckets

        report["by_strategy"][sid] = {
            "summary": _summarize([float(r["realized_pnl"]) for r in sid_priced]),
            "avg_hold_days": avg_hold,
            "features": features,
            "knobs": knob_report,
        }

    return report
