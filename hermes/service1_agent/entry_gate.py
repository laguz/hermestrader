"""AI-entry gate — mechanical validation for overseer-originated trades.

The Hermes Overseer can *propose* brand-new trades from chart vision when
running in ``autonomous`` mode (see ``HermesOverseer.propose``). Those
proposals used to flow straight to the broker, bypassing every risk filter
the rule-based strategies enforce on their own entries (POP, delta band,
minimum credit, DTE window, and MoneyManager capacity).

This module re-derives those same gates against *live* market data and makes
every AI-originated entry clear them before it can be submitted. The AI widens
the funnel of ideas; the mechanical rules still decide what fills.

Design principle: **fail closed.** Any proposal we cannot fully validate —
unparseable legs, missing chain/quote data, an unsupported structure — is
rejected with a reason rather than passed through. An AI that bypasses the
gates is exactly the failure mode the gate exists to prevent.

Thresholds are read from ``system_settings`` (live-tunable, and adjustable by
the goal-aware tuner — see ``HermesOverseer.propose_parameter_adjustments``)
with a conservative static fallback, so an operator or the tuner can tighten
or loosen AI-entry stringency without a code change.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from hermes.utils import utc_now

from hermes.ml.pop_engine import augment_levels_with_pop

from .core import TradeAction
from .strategies._helpers import parse_occ

logger = logging.getLogger("hermes.agent.entry_gate")

# Conservative defaults (CS75-grade stringency). Each is overridable via the
# matching system_settings key so the tuner / operator can adjust live.
DEFAULTS: Dict[str, float] = {
    "ai_gate_min_pop": 0.75,        # short strike must sit beyond a ≥75% POP level
    "ai_gate_delta_min": 0.05,      # reject deep-OTM dust (no premium, no signal)
    "ai_gate_delta_max": 0.45,      # reject anything too close to the money
    "ai_gate_min_credit_pct": 0.12,  # net credit ≥ this fraction of spread width
    "ai_gate_min_dte": 5,
    "ai_gate_max_dte": 45,
}

# Bounds the tuner may move each gate threshold within. Centralised here so the
# gate and the parameter tuner share one source of truth (see TUNABLE_PARAMS in
# overseer.py, which references these).
BOUNDS: Dict[str, Tuple[float, float]] = {
    "ai_gate_min_pop": (0.60, 0.95),
    "ai_gate_delta_min": (0.01, 0.20),
    "ai_gate_delta_max": (0.20, 0.50),
    "ai_gate_min_credit_pct": (0.05, 0.40),
    "ai_gate_min_dte": (1, 30),
    "ai_gate_max_dte": (7, 90),
}


async def _setting_float(db, key: str) -> float:
    """Read a numeric setting, falling back to the conservative default."""
    default = DEFAULTS[key]
    try:
        raw = await db.settings.get_setting(key)
        if raw is None or str(raw).strip() == "":
            return float(default)
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _occ_strike(symbol: str) -> Optional[float]:
    """Extract the strike (decimal dollars) from an OCC symbol's 8-digit field."""
    from hermes.common import OCC_RE
    m = OCC_RE.match(symbol or "")
    if not m:
        return None
    return int(m.group(4)) / 1000.0


def _mid(opt: Dict[str, Any]) -> Optional[float]:
    try:
        bid = float(opt.get("bid"))
        ask = float(opt.get("ask"))
    except (TypeError, ValueError):
        return None
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2.0


async def gate_ai_action(
    action: TradeAction,
    *,
    broker,
    db,
    mm,
    multiplier: int = 100,
) -> Tuple[Optional[TradeAction], str]:
    """Validate one AI-proposed action against the mechanical entry gates.

    Returns ``(action, reason)`` where ``action`` is a normalised, capacity-
    scaled :class:`TradeAction` when every gate passes, or ``None`` when the
    proposal is rejected. ``reason`` always carries a human-readable summary
    for the operator log.

    Only short-premium *credit spreads* (one ``sell_to_open`` + one
    ``buy_to_open`` leg on the same side/expiry) are validatable today; any
    other structure is rejected (fail-closed) rather than waved through.
    """
    # --- structural: must be a recognisable two-leg credit spread -----------
    legs = action.legs or []
    short_legs = [l for l in legs if "sell_to_open" in (l.get("side") or "").lower()]
    long_legs = [l for l in legs if "buy_to_open" in (l.get("side") or "").lower()]
    if len(short_legs) != 1 or len(long_legs) != 1 or len(legs) != 2:
        return None, (
            f"unsupported structure (legs={len(legs)}, short={len(short_legs)}, "
            f"long={len(long_legs)}); only single credit spreads are gated"
        )

    short_sym = short_legs[0].get("option_symbol") or ""
    long_sym = long_legs[0].get("option_symbol") or ""
    short_occ = parse_occ(short_sym)
    long_occ = parse_occ(long_sym)
    if not short_occ or not long_occ:
        return None, f"unparseable OCC legs (short={short_sym!r} long={long_sym!r})"
    if short_occ["side"] != long_occ["side"]:
        return None, "short and long legs are different option types"
    if short_occ["expiry"] != long_occ["expiry"]:
        return None, "short and long legs have different expiries"

    side = short_occ["side"]                       # 'put' | 'call'
    expiry_date = short_occ["expiry"]
    expiry = expiry_date.strftime("%Y-%m-%d")
    short_strike = _occ_strike(short_sym)
    long_strike = _occ_strike(long_sym)
    if short_strike is None or long_strike is None:
        return None, "could not derive strikes from OCC symbols"
    width = round(abs(short_strike - long_strike), 2)
    if width <= 0:
        return None, "degenerate spread (zero width)"

    symbol = action.symbol

    # --- DTE window ---------------------------------------------------------
    min_dte = int(await _setting_float(db, "ai_gate_min_dte"))
    max_dte = int(await _setting_float(db, "ai_gate_max_dte"))
    now = getattr(broker, "current_date", None) or utc_now()
    today = now.date() if hasattr(now, "date") else now
    dte = (expiry_date - today).days
    if not (min_dte <= dte <= max_dte):
        return None, f"{symbol} {side}: DTE {dte} outside gate window {min_dte}-{max_dte}"

    # --- fetch the live chain once; locate the proposed legs ----------------
    chain = await broker.get_option_chains(symbol, expiry) or []
    if not chain:
        return None, f"{symbol}: no option chain for {expiry}"
    by_sym = {o.get("symbol"): o for o in chain}
    short_opt = by_sym.get(short_sym)
    long_opt = by_sym.get(long_sym)
    if not short_opt or not long_opt:
        return None, f"{symbol} {side}: proposed legs not present in live chain"

    # --- delta band on the short leg ---------------------------------------
    delta_min = await _setting_float(db, "ai_gate_delta_min")
    delta_max = await _setting_float(db, "ai_gate_delta_max")
    greeks = short_opt.get("greeks") or {}
    raw_delta = greeks.get("delta")
    if raw_delta is None:
        return None, f"{symbol} {side}: short leg has no delta in chain"
    delta = abs(float(raw_delta))
    if delta < delta_min or delta > delta_max:
        return None, (
            f"{symbol} {side}: short delta {delta:.3f} outside band "
            f"[{delta_min:.2f}, {delta_max:.2f}]"
        )

    # --- credit recomputed from live quotes (never trust the proposal) ------
    short_mid = _mid(short_opt)
    long_mid = _mid(long_opt)
    if short_mid is None or long_mid is None:
        return None, f"{symbol} {side}: stale/zero quote on a leg; cannot price"
    credit = round(short_mid - long_mid, 2)
    min_credit_pct = await _setting_float(db, "ai_gate_min_credit_pct")
    min_credit = round(width * min_credit_pct, 2)
    if credit < min_credit:
        return None, (
            f"{symbol} {side}: live credit ${credit:.2f} < min ${min_credit:.2f} "
            f"({min_credit_pct:.0%} of ${width:.2f} width)"
        )

    # --- POP: short strike must sit beyond a qualifying high-POP S/R level ---
    min_pop = await _setting_float(db, "ai_gate_min_pop")
    analysis = await broker.analyze_symbol(symbol, period="3m")
    if not analysis or "error" in analysis:
        return None, f"{symbol}: analysis unavailable for POP gate"
    xgb_pred = await db.decisions.latest_prediction(symbol) or {}
    analysis = augment_levels_with_pop(analysis, xgb_pred, period="3m")
    target_type = "support" if side == "put" else "resistance"
    qualifying = [
        lvl for lvl in analysis.get("key_levels", [])
        if lvl.get("type") == target_type and lvl.get("pop", 0.0) >= min_pop
    ]
    if not qualifying:
        best = max((lvl.get("pop", 0.0) for lvl in analysis.get("key_levels", [])
                    if lvl.get("type") == target_type), default=0.0)
        return None, (
            f"{symbol} {side}: no {target_type} level ≥{min_pop:.0%} POP "
            f"(best {best:.0%})"
        )
    # The short strike must be at least as protective as a qualifying level:
    # below support for puts, above resistance for calls. This blocks an AI
    # strike that sits closer to the money than the rules would ever pick.
    if side == "put":
        protected = any(short_strike <= lvl["price"] for lvl in qualifying)
    else:
        protected = any(short_strike >= lvl["price"] for lvl in qualifying)
    if not protected:
        return None, (
            f"{symbol} {side}: short strike {short_strike:.2f} closer to spot "
            f"than every ≥{min_pop:.0%} POP {target_type} level"
        )

    # --- capacity: scale through the shared MoneyManager --------------------
    requested = max(1, int(short_legs[0].get("quantity") or action.quantity or 1))
    requirement_per_lot = width * float(multiplier)
    lots = await mm.scale_quantity(
        requested_lots=requested,
        requirement_per_lot=requirement_per_lot,
        symbol=symbol,
        side=side,
        strategy_id=action.strategy_id,
        max_lots=requested,
        expiry=expiry,
    )
    if lots < 1:
        return None, f"{symbol} {side}: no capacity (MoneyManager scaled to 0 lots)"

    # --- passed: emit a normalised, capacity-scaled, honestly-priced action -
    normalised = TradeAction(
        strategy_id=action.strategy_id,
        symbol=symbol,
        order_class="multileg",
        legs=[
            {"option_symbol": short_sym, "side": "sell_to_open", "quantity": lots},
            {"option_symbol": long_sym, "side": "buy_to_open", "quantity": lots},
        ],
        price=credit,
        side="sell",
        quantity=1,
        order_type="credit",
        tag=action.tag or f"HERMES_{action.strategy_id}",
        strategy_params={
            **(action.strategy_params or {}),
            "side_type": side,
            "ai_gated": True,
        },
        expiry=expiry,
        width=width,
        ai_authored=True,
        ai_rationale=action.ai_rationale,
    )
    reason = (
        f"{symbol} {side}: PASSED gate — short={short_strike:.2f} long={long_strike:.2f} "
        f"Δ={delta:.3f} credit=${credit:.2f}/${min_credit:.2f} DTE={dte} lots={lots}"
    )
    return normalised, reason
