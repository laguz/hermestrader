"""
[Service-1: Hermes-Agent-Core]
Daily ATM implied-volatility snapshots for IV-rank conditioning.

IV history must accumulate unconditionally — one observation per symbol per
day from the pipeline heartbeat — not from inside ``is_ivr_gated``: gating
only records days the gate ran *and* passed, which both starves the history
while the gate is off (nothing to rank against when it's finally enabled)
and skews the recorded range toward high-IV days (selection bias in the
very min/max the rank is computed from).
"""
from __future__ import annotations

import logging
import statistics
from datetime import date, datetime
from typing import Iterable, Optional

logger = logging.getLogger("hermes.agent.iv_tracker")


async def fetch_current_atm_iv(broker, symbol: str, today: date) -> Optional[float]:
    """Mean IV across the ATM strike of the expiry nearest 30 DTE, or None
    whenever any input is unusable — never a fabricated value."""
    try:
        expirations = await broker.get_option_expirations(symbol)
        if not expirations:
            return None

        valid_expiries = []
        for exp in expirations:
            try:
                d = datetime.strptime(exp, "%Y-%m-%d").date()
                valid_expiries.append((d, exp))
            except (ValueError, TypeError):
                pass

        if not valid_expiries:
            return None

        best_expiry = min(valid_expiries, key=lambda x: abs((x[0] - today).days - 30))[1]

        chain = await broker.get_option_chains(symbol, best_expiry)
        if not chain:
            return None

        spot = await broker.last_price(symbol)
        if spot is None:
            quotes = await broker.get_quote(symbol)
            if quotes and len(quotes) > 0:
                spot = quotes[0].get("last")
            if spot is None:
                # Fallback to median strike in chain
                strikes = [o.get("strike") for o in chain if o.get("strike") is not None]
                if strikes:
                    spot = statistics.median(strikes)

        if spot is None:
            return None

        atm_strike = min(
            (o.get("strike") for o in chain if o.get("strike") is not None),
            key=lambda s: abs(s - spot),
            default=None
        )
        if atm_strike is None:
            return None

        ivs = []
        for o in chain:
            if o.get("strike") == atm_strike:
                greeks = o.get("greeks") or {}
                iv = greeks.get("mid_iv")
                if iv is None:
                    iv = greeks.get("smv_vol")
                if iv is not None:
                    try:
                        ivs.append(float(iv))
                    except (ValueError, TypeError):
                        pass

        if ivs:
            return float(statistics.mean(ivs))
        return None
    except Exception as exc:
        logger.debug("[IV TRACKER] Failed to fetch current ATM IV for %s: %s", symbol, exc)
        return None


async def snapshot_daily_iv(db, broker, symbols: Iterable[str], today: date) -> int:
    """Persist one ATM-IV observation per symbol for ``today``; returns the
    number saved. ``save_implied_vol`` upserts on (symbol, day), so re-running
    within the same day is idempotent. Per-symbol failures are logged and
    skipped — a bad chain for one symbol must not starve the rest."""
    saved = 0
    for symbol in symbols:
        try:
            iv = await fetch_current_atm_iv(broker, symbol, today)
            if iv is None:
                logger.debug("[IV TRACKER] No ATM IV for %s today; skipping.", symbol)
                continue
            await db.timeseries.save_implied_vol(symbol, iv)
            saved += 1
        except Exception as exc:
            logger.warning("[IV TRACKER] IV snapshot failed for %s: %s", symbol, exc)
    return saved
