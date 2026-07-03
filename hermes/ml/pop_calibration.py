"""
[POP outcome calibration]
Fit the POP engine's predicted probability against the book's own realized
win/loss outcomes.

Every entry stamps ``entry_features`` (including the ``pop`` the engine
claimed at decision time) onto its Trade row, and every close records a
realized ``pnl`` — so each closed trade is a labelled calibration row:
"the engine said p, the trade won/lost". Platt-scaling that mapping turns
systematic over- or under-confidence into a two-parameter correction that
``pop_engine.predict_pop`` applies to every score.

Deliberately conservative for a system that places real orders:

- only ``schema >= 2`` snapshots train (schema-1 rows carry the old inflated
  overlay POP; mixing regimes would double-deflate the honest scores);
- no fit below ``min_samples`` labelled rows or ``min_class`` examples of
  each outcome (a streak of all-wins must not launch POP toward 0.99);
- the fitted calibrator is installed only if it improves training log-loss
  over the identity — otherwise the engine keeps running uncalibrated.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from hermes.ml.calibration import PlattCalibrator, brier_score, log_loss

logger = logging.getLogger("hermes.ml.pop_cal")

MIN_SAMPLES = 30
MIN_CLASS = 5
MAX_SAMPLES = 500

# system_settings key holding the fitted calibrator params + fit stats.
# Written only by the agent (single-writer invariant); the watcher reads it
# so its dashboard POP matches what the agent actually gates on.
POP_CAL_STATE_KEY = "pop_calibration"


def extract_calibration_rows(trades: List[Dict[str, Any]]) -> tuple[List[float], List[float]]:
    """Project closed-trade rows onto (predicted_pop, won) pairs.

    Skips rows without a usable snapshot: schema < 2, missing/degenerate
    ``pop``, or missing ``pnl``.
    """
    pops: List[float] = []
    outcomes: List[float] = []
    for row in trades:
        feats = row.get("entry_features") or {}
        try:
            if int(feats.get("schema") or 0) < 2:
                continue
            pop = float(feats["pop"])
            pnl = float(row["pnl"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (0.0 < pop < 1.0):
            continue
        pops.append(pop)
        outcomes.append(1.0 if pnl > 0 else 0.0)
    return pops, outcomes


# Last settings blob installed via sync — change detection so read-only
# consumers (the watcher) can call sync on every request for pennies.
_synced_state_raw: Optional[str] = None


async def sync_pop_calibrator_from_settings(db) -> bool:
    """Install the persisted outcome calibrator when it (re)appears or changes.

    Read-only consumers of ``predict_pop`` running outside the agent process
    (the watcher's analytics routes) call this before scoring so the POP they
    display is the POP the agent trades on. Returns True when a calibrator
    was (re)installed, False on no-change or any failure — never raises, and
    never clears an installed calibrator on a transient read error.
    """
    global _synced_state_raw
    from hermes.ml.pop_engine import set_pop_calibrator

    try:
        raw = await db.settings.get_setting(POP_CAL_STATE_KEY)
    except Exception:                                          # noqa: BLE001
        return False
    if not raw or raw == _synced_state_raw:
        return False
    try:
        state = json.loads(raw)
        calibrator = PlattCalibrator.from_dict(state["calibrator"])
    except Exception:                                          # noqa: BLE001
        logger.warning("pop_calibration settings blob unparseable; ignoring")
        return False
    set_pop_calibrator(calibrator)
    _synced_state_raw = raw
    logger.info("POP calibrator synced from settings (fitted_at=%s n=%s)",
                state.get("fitted_at"), state.get("n"))
    return True


async def fit_pop_calibrator(
    db,
    *,
    min_samples: int = MIN_SAMPLES,
    min_class: int = MIN_CLASS,
    max_samples: int = MAX_SAMPLES,
) -> Optional[Dict[str, Any]]:
    """Fit a Platt calibrator from the most recent closed trades.

    Returns ``{"calibrator", "n", "wins", "losses", "log_loss_raw",
    "log_loss_cal", "brier_raw", "brier_cal", "fitted_at"}`` when a fit is
    both possible and an improvement, else None (caller keeps whatever —
    including nothing — is currently installed).
    """
    trades = await db.trades.closed_trades_entry_features(limit=max_samples)
    pops, outcomes = extract_calibration_rows(trades)

    n = len(pops)
    wins = int(sum(outcomes))
    losses = n - wins
    if n < min_samples or wins < min_class or losses < min_class:
        logger.info("POP calibration deferred: n=%d wins=%d losses=%d "
                    "(need n>=%d and >=%d per class)",
                    n, wins, losses, min_samples, min_class)
        return None

    calibrator = PlattCalibrator.fit(pops, outcomes)
    ll_raw = log_loss(pops, outcomes)
    ll_cal = log_loss(calibrator.transform(pops).tolist(), outcomes)
    if not ll_cal <= ll_raw + 1e-9:
        logger.warning("POP calibration rejected: log-loss %.4f -> %.4f "
                       "did not improve on %d rows", ll_raw, ll_cal, n)
        return None

    return {
        "calibrator": calibrator,
        "n": n,
        "wins": wins,
        "losses": losses,
        "log_loss_raw": float(ll_raw),
        "log_loss_cal": float(ll_cal),
        "brier_raw": float(brier_score(pops, outcomes)),
        "brier_cal": float(brier_score(calibrator.transform(pops).tolist(), outcomes)),
        "fitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
