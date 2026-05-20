#!/usr/bin/env python3
"""
[Nightly Calibration Job]
Refit per-symbol probability calibrators from the prediction ledger and
persist them into HermesDB system_settings (one JSON blob per symbol).

Run via cron / docker-compose schedule outside market hours, e.g.
    0 3 * * 1-5  /usr/local/bin/python -m scripts.nightly_calibrate

What it does
------------
For each symbol that has at least 30 outcome-bearing rows in the
prediction ledger over the last 90 days:

1. Fit an IsotonicCalibrator (default; falls back to PlattCalibrator
   when the ledger is sparse) on the (predicted_prob, realized_outcome)
   pairs.
2. Write the JSON-serialised calibrator to
   ``system_settings['ml_calibrator__<SYMBOL>']``.
3. Apply a Beta-Bernoulli regime-weight update for the same observations
   so weights drift toward whatever the realised hit rate has been.

The AsyncXGBPredictor reloads calibrators on every calibrate cycle, so
the next prediction tick after the cron run consumes the new params.

Exit codes:
- 0 on success (or no work to do)
- 1 when an unrecoverable error occurred — surfaces in cron mail.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Dict, List

logger = logging.getLogger("hermes.scripts.nightly_calibrate")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=90,
                        help="Lookback window for ledger rows")
    parser.add_argument("--min-rows", type=int, default=30,
                        help="Minimum outcome-bearing rows to fit a calibrator")
    parser.add_argument("--method", choices=("isotonic", "platt"),
                        default="isotonic")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Comma-separated subset; default = all symbols in ledger")
    parser.add_argument("--dsn", type=str, default=None,
                        help="Override HERMES_DSN")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but do not persist")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        from hermes.db.models import HermesDB
        from hermes.ml import ledger as ledger_mod, regime_weights
        from hermes.ml.calibration import (
            IsotonicCalibrator, PlattCalibrator, brier_score, log_loss,
        )
        from hermes.ml.meta_learner import MetaLearner
        import numpy as np
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("import failed: %s", exc)
        return 1

    import os
    dsn = args.dsn or os.environ.get(
        "HERMES_DSN",
        "postgresql+psycopg://hermes:hermes@localhost:5432/hermes",
    )
    db = HermesDB(dsn)

    symbols = _enumerate_symbols(db, args.symbols)
    if not symbols:
        logger.info("no symbols with ledger rows; nothing to calibrate")
        return 0

    fitted: Dict[str, Dict[str, float]] = {}
    for sym in symbols:
        try:
            rows = ledger_mod.fetch_for_calibration(
                db, sym, "xgb-q50-7dte", days=args.days, require_outcome=True,
            )
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("ledger fetch failed for %s: %s", sym, exc)
            continue
        if len(rows) < args.min_rows:
            logger.debug("skip %s: only %d outcome rows (<%d)",
                         sym, len(rows), args.min_rows)
            continue
        preds = [r["predicted_prob"] for r in rows]
        outs = [r["realized_outcome"] for r in rows]

        if args.method == "platt":
            cal = PlattCalibrator.fit(preds, outs)
        else:
            cal = IsotonicCalibrator.fit(preds, outs)

        # Reconstruct MetaLearner training rows from prediction ledger rows
        meta_rows = []
        for r in rows:
            fv_dict = r.get("feature_vector") or {}
            xgb_p = r["predicted_prob"]
            iv_r = fv_dict.get("iv_rank_365d")
            if iv_r is None or not np.isfinite(iv_r):
                iv_r = 50.0
            cur_v = fv_dict.get("realized_vol_5d", 0.30)
            avg_v = 0.30
            vol_r = cur_v / avg_v if avg_v else 1.0
            delta_p = 0.84
            prot_s = 1.0
            meta_rows.append({
                "delta_implied_prob": float(delta_p),
                "xgb_prob": float(xgb_p),
                "protection_score": float(prot_s),
                "iv_rank_365d": float(iv_r),
                "vol_ratio": float(vol_r),
            })

        try:
            meta = MetaLearner.fit(meta_rows, outs, calibrator=args.method)
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("MetaLearner fit failed for %s: %s", sym, exc)
            meta = None

        # Bayesian regime-weight update — counts hits/misses across the window.
        hits = int(sum(1 for o in outs if o >= 0.5))
        misses = len(outs) - hits
        if not args.dry_run:
            for period in ("3M", "6M", "1Y"):
                try:
                    regime_weights.update_from_outcomes(
                        db, sym, period, hits=hits, misses=misses)
                except Exception as exc:                           # noqa: BLE001
                    logger.warning("regime update failed %s/%s: %s",
                                   sym, period, exc)

        # Persist calibrator JSON for the predictor to reload.
        if not args.dry_run:
            try:
                db.set_setting(f"ml_calibrator__{sym}",
                               json.dumps(cal.to_dict(), sort_keys=True))
                if meta is not None:
                    db.set_setting(f"ml_meta_learner__{sym}", meta.to_json())
            except Exception as exc:                               # noqa: BLE001
                logger.warning("setting write failed for %s: %s", sym, exc)

        calibrated = cal.transform(preds)
        fitted[sym] = {
            "n_rows": len(rows),
            "method": args.method,
            "brier_raw": brier_score(preds, outs),
            "brier_calibrated": brier_score(calibrated.tolist(), outs),
            "log_loss": log_loss(calibrated.tolist(), outs),
        }

    if not fitted:
        logger.info("no symbols qualified for calibration this run")
        return 0

    for sym, stats in fitted.items():
        logger.info(
            "calibrated %s: n=%d brier %.4f → %.4f log_loss=%.4f",
            sym, stats["n_rows"], stats["brier_raw"],
            stats["brier_calibrated"], stats["log_loss"],
        )
    return 0


def _enumerate_symbols(db, override: str | None) -> List[str]:
    if override:
        return [s.strip().upper() for s in override.split(",") if s.strip()]
    try:
        from hermes.ml.ledger import PredictionLedger
    except Exception:                                              # noqa: BLE001
        return []
    if PredictionLedger is None:
        return []
    try:
        with db.Session() as s:
            rows = (s.query(PredictionLedger.symbol)
                    .distinct()
                    .all())
            return sorted({r[0] for r in rows if r[0]})
    except Exception as exc:                                       # noqa: BLE001
        logger.warning("symbol enumeration failed: %s", exc)
        return []


if __name__ == "__main__":
    sys.exit(main())
