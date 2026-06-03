#!/usr/bin/env python3
"""
[ML Efficacy Report]
Does the XGBoost layer earn its rent? This read-only script measures whether
the stored predictions actually track reality and whether model confidence
correlates with trade outcomes. It changes nothing — no settings, no trades,
no schema — it only reads and reports.

Three sections, each independently guarded so a thin/empty DB degrades to
"insufficient data" rather than fabricating numbers:

  A. Directional accuracy + calibration
     Source: the prediction_ledger (rich store with backfilled
     ``realized_outcome``). Reports hit-rate, Brier score and log-loss,
     overall and per symbol. Reuses hermes.ml.calibration.{brier_score,
     log_loss} so the numbers match what nightly_calibrate optimises.

  B. Price-magnitude accuracy
     Source: the ``predictions`` hypertable joined to ``bars_daily`` at a
     fixed horizon. Reports MAE/RMSE of ``predicted_price`` vs the realised
     close, plus directional hit-rate of ``predicted_return``'s sign. This
     is the fallback signal when the ledger is empty.

  C. Confidence → P&L linkage
     Source: closed ``trades`` joined to the latest pre-entry prediction for
     the same symbol. Buckets realised P&L by model confidence (terciles) and
     reports mean P&L + win-rate per bucket, plus the Pearson correlation.
     This is the question that actually matters: does higher model confidence
     buy better trades?

Usage
-----
    python -m scripts.ml_efficacy                 # print report
    python -m scripts.ml_efficacy --out docs/ml_efficacy_findings.md
    python -m scripts.ml_efficacy --horizon-days 7 --min-rows 30

Exit codes: 0 always on a clean read (including "insufficient data"); 1 only
on an unrecoverable error (bad DSN, import failure).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from bisect import bisect_left
from datetime import timedelta
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("hermes.scripts.ml_efficacy")

SECTION_RULE = "=" * 70


# ---------------------------------------------------------------------------
# Loaders (synchronous ORM / SQL reads)
# ---------------------------------------------------------------------------
def _load_ledger_outcomes(session) -> List[Dict[str, Any]]:
    """``[{symbol, prob, outcome, ts}]`` for ledger rows with a realised outcome."""
    try:
        from hermes.ml.ledger import PredictionLedger
    except Exception:                                              # noqa: BLE001
        return []
    if PredictionLedger is None:
        return []
    rows = (session.query(PredictionLedger)
            .filter(PredictionLedger.realized_outcome.isnot(None),
                    PredictionLedger.predicted_prob.isnot(None))
            .all())
    out = []
    for r in rows:
        out.append({
            "symbol": r.symbol,
            "prob": float(r.predicted_prob),
            "outcome": float(r.realized_outcome),
            "ts": r.ts,
        })
    return out


def _load_ledger_confidence(session) -> List[Tuple[str, Any, float]]:
    """``[(symbol, ts, confidence)]`` from the ledger; confidence = |prob-0.5|."""
    try:
        from hermes.ml.ledger import PredictionLedger
    except Exception:                                              # noqa: BLE001
        return []
    if PredictionLedger is None:
        return []
    rows = (session.query(PredictionLedger)
            .filter(PredictionLedger.predicted_prob.isnot(None))
            .all())
    return [(r.symbol, r.ts, abs(float(r.predicted_prob) - 0.5)) for r in rows]


def _load_predictions(session) -> List[Dict[str, Any]]:
    from hermes.db.models import Prediction
    rows = session.query(Prediction).all()
    out = []
    for r in rows:
        out.append({
            "symbol": r.symbol,
            "ts": r.ts,
            "predicted_return": None if r.predicted_return is None else float(r.predicted_return),
            "predicted_price": None if r.predicted_price is None else float(r.predicted_price),
            "spot": None if r.spot is None else float(r.spot),
        })
    return out


def _coerce_ts(ts):
    """Normalise a bar timestamp to datetime (some drivers return ISO strings)."""
    if isinstance(ts, str):
        from datetime import datetime
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return ts
    return ts


def _load_bars(session) -> Dict[str, List[Tuple[Any, float]]]:
    """``{symbol: [(ts, close), …]}`` sorted by ts (for horizon lookups)."""
    from sqlalchemy import text
    res = session.execute(
        text("SELECT symbol, ts, close FROM bars_daily WHERE close IS NOT NULL ORDER BY symbol, ts"))
    bars: Dict[str, List[Tuple[Any, float]]] = {}
    for symbol, ts, close in res:
        bars.setdefault(symbol, []).append((_coerce_ts(ts), float(close)))
    return bars


def _load_closed_trades(session) -> List[Dict[str, Any]]:
    from hermes.db.models import Trade
    rows = (session.query(Trade)
            .filter(Trade.status == "CLOSED", Trade.pnl.isnot(None))
            .all())
    return [{"symbol": r.symbol, "opened_at": r.opened_at, "pnl": float(r.pnl)} for r in rows]


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------
def _close_at_or_after(series: List[Tuple[Any, float]], target_ts) -> Optional[float]:
    """First close at ts >= target_ts (series sorted by ts)."""
    if not series:
        return None
    lo = bisect_left([t for t, _ in series], target_ts)
    if lo >= len(series):
        return None
    return series[lo][1]


def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    mx, my = mean(xs), mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def _terciles(values: List[float]) -> Tuple[float, float]:
    s = sorted(values)
    n = len(s)
    return s[n // 3], s[2 * n // 3]


# ---------------------------------------------------------------------------
# Section builders — each returns a list of report lines.
# ---------------------------------------------------------------------------
def _section_directional(ledger: List[Dict[str, Any]], min_rows: int) -> List[str]:
    from hermes.ml.calibration import brier_score, log_loss
    lines = ["", SECTION_RULE, "A. DIRECTIONAL ACCURACY + CALIBRATION (prediction_ledger)", SECTION_RULE]
    if len(ledger) < min_rows:
        lines.append(f"  insufficient data: {len(ledger)} outcome-bearing rows (<{min_rows}).")
        return lines

    probs = [r["prob"] for r in ledger]
    outs = [r["outcome"] for r in ledger]
    hits = sum(1 for p, o in zip(probs, outs) if (p >= 0.5) == (o >= 0.5))
    base_rate = mean(outs)
    lines.append(f"  rows={len(ledger)}  hit_rate={hits/len(ledger):.1%}  "
                 f"base_rate(up)={base_rate:.1%}")
    lines.append(f"  brier={brier_score(probs, outs):.4f}  "
                 f"log_loss={log_loss(probs, outs):.4f}  "
                 f"(brier 0.25 = coin-flip; lower is better)")

    # Per-symbol breakdown for symbols with enough rows.
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in ledger:
        by_sym.setdefault(r["symbol"], []).append(r)
    sym_lines = []
    for sym, rs in sorted(by_sym.items()):
        if len(rs) < max(10, min_rows // 3):
            continue
        p = [x["prob"] for x in rs]
        o = [x["outcome"] for x in rs]
        h = sum(1 for a, b in zip(p, o) if (a >= 0.5) == (b >= 0.5))
        sym_lines.append(f"    {sym:<6} n={len(rs):<4} hit={h/len(rs):.0%}  brier={brier_score(p, o):.4f}")
    if sym_lines:
        lines.append("  per symbol:")
        lines.extend(sym_lines)
    return lines


def _section_magnitude(predictions: List[Dict[str, Any]],
                       bars: Dict[str, List[Tuple[Any, float]]],
                       horizon_days: int, min_rows: int) -> List[str]:
    lines = ["", SECTION_RULE,
             f"B. PRICE-MAGNITUDE ACCURACY (predictions vs bars_daily, +{horizon_days}d)",
             SECTION_RULE]
    abs_errs: List[float] = []
    sq_errs: List[float] = []
    dir_hits = 0
    dir_total = 0
    for p in predictions:
        series = bars.get(p["symbol"])
        if not series or p["spot"] is None:
            continue
        realised = _close_at_or_after(series, p["ts"] + timedelta(days=horizon_days))
        if realised is None:
            continue
        if p["predicted_price"] is not None:
            err = p["predicted_price"] - realised
            abs_errs.append(abs(err))
            sq_errs.append(err * err)
        if p["predicted_return"] is not None and p["spot"]:
            realised_ret = (realised - p["spot"]) / p["spot"]
            if p["predicted_return"] != 0 and realised_ret != 0:
                dir_total += 1
                if (p["predicted_return"] > 0) == (realised_ret > 0):
                    dir_hits += 1

    if len(abs_errs) < min_rows and dir_total < min_rows:
        evaluated = max(len(abs_errs), dir_total)
        lines.append(f"  insufficient data: {evaluated} predictions with a realised "
                     f"+{horizon_days}d bar (<{min_rows}).")
        return lines
    if abs_errs:
        mae = mean(abs_errs)
        rmse = (mean(sq_errs)) ** 0.5
        lines.append(f"  price error over {len(abs_errs)} preds:  MAE=${mae:.2f}  RMSE=${rmse:.2f}")
    if dir_total:
        lines.append(f"  directional hit-rate over {dir_total} preds:  "
                     f"{dir_hits/dir_total:.1%}  (50% = no edge)")
    return lines


def _section_pnl_linkage(conf_points: List[Tuple[str, Any, float]],
                         trades: List[Dict[str, Any]], min_rows: int) -> List[str]:
    lines = ["", SECTION_RULE, "C. CONFIDENCE → P&L LINKAGE (closed trades vs pre-entry confidence)", SECTION_RULE]
    if not conf_points:
        lines.append("  insufficient data: no predictions to attach confidence from.")
        return lines
    if len(trades) < min_rows:
        lines.append(f"  insufficient data: {len(trades)} closed trades (<{min_rows}).")
        return lines

    # Index confidence points per symbol, sorted by ts, for as-of lookups.
    by_sym: Dict[str, List[Tuple[Any, float]]] = {}
    for sym, ts, conf in conf_points:
        by_sym.setdefault(sym, []).append((ts, conf))
    for sym in by_sym:
        by_sym[sym].sort(key=lambda x: x[0])

    paired: List[Tuple[float, float]] = []  # (confidence, pnl)
    for t in trades:
        series = by_sym.get(t["symbol"])
        if not series:
            continue
        idx = bisect_left([ts for ts, _ in series], t["opened_at"])
        # latest prediction at ts <= opened_at
        if idx == 0:
            continue
        conf = series[idx - 1][1]
        paired.append((conf, t["pnl"]))

    if len(paired) < min_rows:
        lines.append(f"  insufficient data: only {len(paired)} trades had a prior "
                     f"prediction for their symbol (<{min_rows}).")
        return lines

    confs = [c for c, _ in paired]
    pnls = [p for _, p in paired]
    r = _pearson(confs, pnls)
    lines.append(f"  matched {len(paired)} closed trades to a pre-entry prediction")
    lines.append(f"  Pearson(confidence, pnl) = {r:.3f}" if r is not None
                 else "  Pearson(confidence, pnl) = n/a")

    lo, hi = _terciles(confs)
    buckets = {"low": [], "mid": [], "high": []}
    for c, p in paired:
        key = "low" if c <= lo else ("high" if c > hi else "mid")
        buckets[key].append(p)
    lines.append("  P&L by confidence tercile:")
    for name in ("low", "mid", "high"):
        b = buckets[name]
        if not b:
            continue
        wr = sum(1 for x in b if x > 0) / len(b)
        lines.append(f"    {name:<5} n={len(b):<4} mean_pnl=${mean(b):>9.2f}  win_rate={wr:.0%}")
    lines.append("  interpretation: if 'high' confidence does not beat 'low' on mean")
    lines.append("  P&L and win-rate, the model's confidence is not pricing trade quality.")
    return lines


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _safe(session, loader, default):
    """Run a loader, returning ``default`` if its table is missing/unreadable.

    ``prediction_ledger`` in particular is created lazily on the ML layer's
    first write, so a fresh DB legitimately lacks it — that's "no data", not
    an error. SQLAlchemy aborts the transaction on a failed statement, so we
    roll back before the next loader runs.
    """
    try:
        return loader(session)
    except Exception as exc:                                       # noqa: BLE001
        logger.debug("loader %s skipped: %s", getattr(loader, "__name__", loader), exc)
        session.rollback()
        return default


def build_report(db, horizon_days: int, min_rows: int) -> Tuple[List[str], bool]:
    """Return (lines, any_data). ``any_data`` is False on a wholly empty DB."""
    with db.Session() as session:
        ledger = _safe(session, _load_ledger_outcomes, [])
        ledger_conf = _safe(session, _load_ledger_confidence, [])
        predictions = _safe(session, _load_predictions, [])
        bars = _safe(session, _load_bars, {})
        trades = _safe(session, _load_closed_trades, [])

    # Confidence source for Section C: ledger if present, else predictions.
    if ledger_conf:
        conf_points = ledger_conf
        conf_src = "prediction_ledger (|prob-0.5|)"
    else:
        conf_points = [(p["symbol"], p["ts"], abs(p["predicted_return"]))
                       for p in predictions if p["predicted_return"] is not None]
        conf_src = "predictions (|predicted_return|)"

    header = [
        SECTION_RULE,
        "HERMESTRADER — ML EFFICACY REPORT",
        SECTION_RULE,
        f"  ledger_outcomes={len(ledger)}  predictions={len(predictions)}  "
        f"closed_trades={len(trades)}  symbols_with_bars={len(bars)}",
        f"  horizon={horizon_days}d  min_rows={min_rows}  confidence_source={conf_src}",
    ]
    any_data = bool(ledger or predictions or trades)
    if not any_data:
        header.append("")
        header.append("  INSUFFICIENT DATA: the DB has no predictions and no closed trades.")
        header.append("  Run the agent (paper mode is fine) to accumulate history, then re-run.")
        return header, False

    lines = list(header)
    lines += _section_directional(ledger, min_rows)
    lines += _section_magnitude(predictions, bars, horizon_days, min_rows)
    lines += _section_pnl_linkage(conf_points, trades, min_rows)
    lines += ["", SECTION_RULE,
              "Read-only report — no settings, trades, or schema were modified.",
              SECTION_RULE]
    return lines, True


def _write_markdown(path: str, lines: List[str]) -> None:
    body = "\n".join(lines)
    md = ("# ML Efficacy Findings\n\n"
          "_Generated by `python -m scripts.ml_efficacy`. Read-only analysis of "
          "whether the XGBoost layer's predictions track reality and correlate "
          "with trade P&L._\n\n"
          "```\n" + body + "\n```\n")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(md)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn", default=None, help="Override HERMES_DSN")
    parser.add_argument("--horizon-days", type=int, default=7,
                        help="Calendar days ahead to evaluate price predictions")
    parser.add_argument("--min-rows", type=int, default=30,
                        help="Minimum rows for a section to report (else 'insufficient data')")
    parser.add_argument("--out", default=None,
                        help="Also write a Markdown findings file to this path")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        from hermes.db.models import HermesDB
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("import failed: %s", exc)
        return 1

    dsn = args.dsn or os.environ.get(
        "HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")

    try:
        db = HermesDB(dsn)
        lines, _ = build_report(db, args.horizon_days, args.min_rows)
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("efficacy report failed: %s", exc)
        return 1

    print("\n".join(lines))
    if args.out:
        _write_markdown(args.out, lines)
        logger.info("wrote findings to %s", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
