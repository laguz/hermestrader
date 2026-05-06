"""
[Service-1: Hermes-Agent-Core] — Concrete strategies.
Cascading priority is encoded in `PRIORITY` (1 highest).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .core import AbstractStrategy, TradeAction
from hermes.ml.pop_engine import calculate_strike_protection, generate_regime_pops, augment_levels_with_pop

logger = logging.getLogger("hermes.agent.strategies")

OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([PC])(\d{8})$")


def _parse_occ(symbol: str):
    m = OCC_RE.match(symbol or "")
    if not m:
        return None
    underlying, yymmdd, pc, _strike = m.groups()
    return {
        "underlying": underlying,
        "expiry": datetime.strptime(yymmdd, "%y%m%d").date(),
        "side": "put" if pc == "P" else "call",
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _nearest_strike(chain, option_type: str, target: float) -> Optional[Dict[str, Any]]:
    """Return the chain option whose strike is closest to `target`."""
    candidates = [o for o in chain if o.get("option_type") == option_type]
    if not candidates:
        return None
    return min(candidates, key=lambda o: abs(float(o["strike"]) - target))


# ---------------------------------------------------------------------------
# Priority 1 — CS75 (39-45 DTE entry; 25% / 20% credit-to-width by DTE band)
# ---------------------------------------------------------------------------
class CreditSpreads75(AbstractStrategy):
    PRIORITY = 1
    NAME = "CS75"

    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        width = float(self.config.get("cs75_width", 5.0))
        max_lots_global = int(self.config.get("cs75_max_lots", 1))
        target_lots_global = int(self.config.get("cs75_target_lots", 1))
        
        # Load detailed watchlist to get per-symbol target overrides
        detailed_wl = self.db.list_watchlist_detailed(self.strategy_id)
        symbols = list(watchlist)
        
        self._log(f"↻ scanning {len(symbols)} symbol(s) — global_target={target_lots_global} max={max_lots_global}")

        for sym_raw in symbols:
            try:
                # Support "SYMBOL:LOTS" format
                if ":" in sym_raw:
                    symbol, lots_str = sym_raw.split(":", 1)
                    symbol = symbol.strip()
                    try:
                        target_lots = int(lots_str)
                    except ValueError:
                        target_lots = target_lots_global
                else:
                    symbol = sym_raw
                    # Per-symbol overrides from DB
                    symbol_meta = detailed_wl.get(symbol, {})
                    target_lots = symbol_meta.get("target_lots") or target_lots_global
                
                max_lots = target_lots
                
                analysis = self.broker.analyze_symbol(symbol, period="6m")
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue
                
                # Augment analysis with centralized POP logic (6M regime for CS75)
                xgb_pred = self.db.latest_prediction(symbol) or {}
                analysis = augment_levels_with_pop(analysis, xgb_pred, period="6m")
                
                price = analysis["current_price"]

                # Mode A vs Mode B
                expiry = self.find_active_ic_expiry(symbol)
                mode_a = not expiry
                existing_sides: set = set()

                if mode_a:
                    expiry = self.find_expiry_in_dte_range(symbol, 39, 45, prefer="max")
                else:
                    dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                    if dte < 14 or dte > 45:
                        self._log(f"ℹ️ {symbol}: incomplete IC expiry {expiry} ({dte}DTE) outside 14-45 completion window; skip.")
                        continue
                    existing_sides = {leg.get("side", "").lower() for leg in self.db.open_legs(self.strategy_id, symbol) if leg.get("expiry") == expiry}

                if not expiry:
                    self._log(f"ℹ️ {symbol}: no expiry found in 39-45 DTE range; skip.")
                    continue

                # Required credit: 25% width for 30-45 DTE; 20% for 14-29 DTE
                dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                min_credit_pct = 0.25 if 30 <= dte <= 45 else 0.20
                min_credit = round(width * min_credit_pct, 2)
                mode_label = "A (new)" if mode_a else f"B (complete {sorted(existing_sides)})"
                self._log(
                    f"→ {symbol}: mode={mode_label} expiry={expiry} {dte}DTE "
                    f"price=${price:.2f} min_credit=${min_credit:.2f}"
                )

                def factory(side: str):
                    def _build(symbol, expiry, lots, width):
                        return self._build_spread_action(
                            symbol=symbol, expiry=expiry, side=side, lots=lots,
                            width=width, min_credit=min_credit, analysis=analysis,
                            current_price=price,
                        )
                    return _build

                planned = self.ic.plan(
                    strategy_id=self.strategy_id,
                    symbol=symbol, expiry=expiry,
                    target_lots=target_lots, width=width, max_lots=max_lots,
                    existing_sides=existing_sides,
                    put_action_factory=factory("put"),
                    call_action_factory=factory("call"),
                )
                actions.extend([a for a in planned if a is not None])
            except Exception as exc:                                  # noqa: BLE001
                self._log(f"❌ {symbol}: {exc}")
        return actions


    def _build_spread_action(self, *, symbol, expiry, side, lots, width,
                             min_credit, analysis, current_price) -> Optional[TradeAction]:
        chain = self.broker.get_option_chains(symbol, expiry) or []
        if not chain:
            self._log(f"{symbol} {side}: empty chain for {expiry}; skip.")
            return None
            
        opt_type = side
        options = [o for o in chain if o.get("option_type") == opt_type]
        
        # Selection Strategy: Use POP already calculated for Key Levels (Institutional Flow)
        # instead of looping through the entire option chain.
        best_strike = None
        best_pop_diff = 999.0
        max_level_pop = 0.0
        
        target_type = "support" if side == "put" else "resistance"
        levels = [l for l in analysis.get("key_levels", []) if l.get("type") == target_type]
        
        for level in levels:
            lvl_pop = level.get("pop", 0.0)
            if lvl_pop > max_level_pop:
                max_level_pop = lvl_pop
                
            if lvl_pop >= 0.75:
                diff = abs(lvl_pop - 0.75)
                if diff < best_pop_diff:
                    # Find the real strike closest to this key level
                    strike_opt = _nearest_strike(chain, opt_type, level["price"])
                    if not strike_opt: continue
                    
                    # Verify delta sanity on the real strike
                    greeks = strike_opt.get("greeks") or {}
                    delta = abs(float(greeks.get("delta", 0.0)))
                    if delta < 0.05 or delta > 0.40: continue
                    
                    best_pop_diff = diff
                    best_strike = strike_opt
                    
        if not best_strike:
            self._log(f"✗ {symbol} {side}: no >75% POP S/R level found in chain (Best Level POP: {max_level_pop:.1%}); skip.")
            return None
            
        short_leg = best_strike
        # Snap the long strike to the nearest chain strike below/above the short
        long_target = float(short_leg["strike"]) - width if side == "put" else float(short_leg["strike"]) + width
        long_leg = _nearest_strike(chain, opt_type, long_target)
        if not long_leg or long_leg["symbol"] == short_leg["symbol"]:
            self._log(
                f"✗ {symbol} {side}: no distinct long leg for short={short_leg['strike']:.2f} "
                f"long_target={long_target:.2f}; skip."
            )
            return None
        # Sanity-check direction: long must be further OTM than short
        sl_strike = float(short_leg["strike"])
        ll_strike = float(long_leg["strike"])
        if side == "put" and ll_strike >= sl_strike:
            self._log(f"✗ {symbol} {side}: long strike {ll_strike} ≥ short {sl_strike} (invalid put spread); skip.")
            return None
        if side == "call" and ll_strike <= sl_strike:
            self._log(f"✗ {symbol} {side}: long strike {ll_strike} ≤ short {sl_strike} (invalid call spread); skip.")
            return None
        actual_width = abs(sl_strike - ll_strike)
        self._log(
            f"→ {symbol} {side}: short={sl_strike:.2f} long={ll_strike:.2f} "
            f"width={actual_width:.2f}"
        )

        credit = self.short_credit(short_leg, long_leg)
        # Recompute min_credit against the actual chain width (snapped strikes may
        # differ from the requested `width` parameter).
        effective_min_credit = round(actual_width * (min_credit / width), 2) if width > 0 else min_credit
        if credit < effective_min_credit:
            self._log(
                f"✗ {symbol} {side}: credit ${credit:.2f} < min ${effective_min_credit:.2f} "
                f"(width={actual_width:.2f}); skip."
            )
            return None

        return TradeAction(
            strategy_id=self.strategy_id,
            symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
            ],
            price=credit, side="sell", quantity=1, order_type="credit",
            tag="HERMES_CS75",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side},
            dte=(datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days,
            expiry=expiry, width=width,
        )

    def manage_positions(self) -> List[TradeAction]:
        """TP @ 50% (DTE 21-45) or 75% (DTE<21); SL @ 2.5x; time exit ≤ 8 DTE."""
        actions: List[TradeAction] = []
        for trade in self.db.open_trades(self.strategy_id):
            short_leg, long_leg = trade["short_leg"], trade["long_leg"]
            entry_credit = float(trade["entry_credit"])
            info = _parse_occ(short_leg)
            if not info:
                continue
            dte = (info["expiry"] - self.today()).days
            quotes = self.broker.get_quote(f"{short_leg},{long_leg}") or []
            sq = next((q for q in quotes if q["symbol"] == short_leg), None)
            lq = next((q for q in quotes if q["symbol"] == long_leg), None)
            if not (sq and lq):
                continue
            debit = round(float(sq["ask"]) - float(lq["bid"]), 2)

            close_reason = None
            if 21 <= dte <= 45 and debit <= entry_credit * 0.50:
                close_reason = "TP-50"
            elif dte < 21 and debit <= entry_credit * 0.25:
                close_reason = "TP-75"
            elif debit >= entry_credit * 2.5:
                close_reason = "SL-2.5x"
            elif dte <= 8:
                close_reason = "TIME-EXIT"

            if close_reason:
                actions.append(self._close_action(trade, debit, close_reason))
        return actions

    def _close_action(self, trade, debit, reason) -> TradeAction:
        return TradeAction(
            strategy_id=self.strategy_id, symbol=trade["symbol"],
            order_class="multileg",
            legs=[
                {"option_symbol": trade["short_leg"], "side": "buy_to_close",  "quantity": int(trade["lots"])},
                {"option_symbol": trade["long_leg"],  "side": "sell_to_close", "quantity": int(trade["lots"])},
            ],
            price=round(debit * 1.05, 2), side="buy", quantity=1,
            order_type="debit", tag=f"HERMES_CS75_CLOSE_{reason}",
            strategy_params={"trade_id": trade["id"], "close_reason": reason},
        )


# ---------------------------------------------------------------------------
# Priority 2 — CS7 (7 DTE; min credit ≥ 12% width; TP 2% width; SL 3x credit)
# ---------------------------------------------------------------------------
class CreditSpreads7(AbstractStrategy):
    PRIORITY = 2
    NAME = "CS7"

    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        width = float(self.config.get("cs7_width", 1.0))
        max_lots_global = int(self.config.get("cs7_max_lots", 1))
        target_lots_global = int(self.config.get("cs7_target_lots", 1))
        min_credit = round(width * 0.12, 2)
        
        # Load detailed watchlist to get per-symbol target overrides
        detailed_wl = self.db.list_watchlist_detailed(self.strategy_id)
        symbols = list(watchlist)
        
        self._log(f"↻ scanning {len(symbols)} symbol(s) — global_target={target_lots_global} max={max_lots_global} min_credit=${min_credit:.2f}")

        for sym_raw in symbols:
            try:
                # Support "SYMBOL:LOTS" format
                if ":" in sym_raw:
                    symbol, lots_str = sym_raw.split(":", 1)
                    symbol = symbol.strip()
                    try:
                        target_lots = int(lots_str)
                    except ValueError:
                        target_lots = target_lots_global
                else:
                    symbol = sym_raw
                    # Per-symbol overrides from DB
                    symbol_meta = detailed_wl.get(symbol, {})
                    target_lots = symbol_meta.get("target_lots") or target_lots_global
                
                max_lots = target_lots
                
                analysis = self.broker.analyze_symbol(symbol, period="3m")
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue
                
                # Augment analysis with centralized POP logic (3M regime for CS7)
                xgb_pred = self.db.latest_prediction(symbol) or {}
                analysis = augment_levels_with_pop(analysis, xgb_pred, period="3m")
                
                price = analysis["current_price"]

                # Mode A vs Mode B Logic
                expiry = self.find_active_ic_expiry(symbol)
                mode_a = not expiry
                existing_sides: set = set()

                if mode_a:
                    # Initial Entry: Fixed 7 DTE
                    expiry = self.find_expiry_in_dte_range(symbol, 7, 7)
                    if not expiry:
                        self._log(f"ℹ️ {symbol}: no exact 7 DTE expiry found for new entry; skip.")
                        continue
                else:
                    # Completion (Mode B): Follow existing expiry if in 4-7 DTE window
                    dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                    if not (4 <= dte <= 7):
                        self._log(f"ℹ️ {symbol}: incomplete IC expiry {expiry} ({dte}DTE) outside 4-7 completion window; skip.")
                        continue
                    existing_sides = {leg.get("side", "").lower() for leg in self.db.open_legs(self.strategy_id, symbol) if leg.get("expiry") == expiry}

                self._log(f"→ {symbol}: {'MODE A' if mode_a else 'MODE B'} expiry={expiry} existing_sides={sorted(existing_sides)}")

                def factory(side: str):
                    def _b(symbol, expiry, lots, width):
                        return self._build_short_premium_spread(
                            symbol=symbol, expiry=expiry, side=side, lots=lots,
                            width=width, min_credit=min_credit, analysis=analysis,
                            current_price=price,
                        )
                    return _b

                planned = self.ic.plan(
                    strategy_id=self.strategy_id, symbol=symbol, expiry=expiry,
                    target_lots=target_lots, width=width, max_lots=max_lots,
                    existing_sides=existing_sides,
                    put_action_factory=factory("put"),
                    call_action_factory=factory("call"),
                )
                actions.extend([a for a in planned if a is not None])
            except Exception as exc:                              # noqa: BLE001
                self._log(f"❌ {symbol}: unexpected error — {exc}")
        return actions

    def _build_short_premium_spread(self, *, symbol, expiry, side, lots,
                                    width, min_credit, analysis, current_price) -> Optional[TradeAction]:
        from hermes.ml.pop_engine import calculate_strike_protection, generate_regime_pops
        chain = self.broker.get_option_chains(symbol, expiry) or []
        if not chain: return None
        
        opt_type = side
        # Selection Strategy: Use POP already calculated for Key Levels (Institutional Flow)
        # instead of looping through the entire option chain.
        best_strike = None
        best_pop_diff = 999.0
        max_level_pop = 0.0
        
        target_type = "support" if side == "put" else "resistance"
        levels = [l for l in analysis.get("key_levels", []) if l.get("type") == target_type]
        
        for level in levels:
            lvl_pop = level.get("pop", 0.0)
            if lvl_pop > max_level_pop:
                max_level_pop = lvl_pop
                
            if lvl_pop >= 0.75:
                diff = abs(lvl_pop - 0.75)
                if diff < best_pop_diff:
                    # Find the real strike closest to this key level
                    strike_opt = _nearest_strike(chain, opt_type, level["price"])
                    if not strike_opt: continue
                    
                    # Verify delta sanity on the real strike
                    greeks = strike_opt.get("greeks") or {}
                    delta = abs(float(greeks.get("delta", 0.0)))
                    # In CS7 (7DTE), we allow slightly higher deltas than CS75
                    if delta < 0.05 or delta > 0.45: continue
                    
                    best_pop_diff = diff
                    best_strike = strike_opt
                    
        if not best_strike:
            self._log(f"✗ {symbol} {side}: no >75% POP S/R level found in chain (Best Level POP: {max_level_pop:.1%}); skip.")
            return None
            
        short_leg = best_strike
        long_target = float(short_leg["strike"]) - width if side == "put" else float(short_leg["strike"]) + width
        long_leg = _nearest_strike(chain, opt_type, long_target)
        
        if not long_leg or long_leg["symbol"] == short_leg["symbol"]:
            self._log(
                f"✗ {symbol} {side}: no distinct long leg for short={short_leg['strike']:.2f} "
                f"long_target={long_target:.2f} (7DTE); skip."
            )
            return None

        credit = self.short_credit(short_leg, long_leg)
        if credit < min_credit:
            self._log(
                f"✗ {symbol} {side}: credit ${credit:.2f} < min ${min_credit:.2f} "
                f"(short={short_leg['strike']:.2f} long={long_leg['strike']:.2f}); skip."
            )
            return None

        self._log(
            f"→ {symbol} {side}: short={short_leg['strike']} long={long_leg['strike']} "
            f"credit=${credit:.2f} (7DTE)"
        )
        return TradeAction(
            strategy_id=self.strategy_id, symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
            ],
            price=credit, side="sell", quantity=1, order_type="credit",
            tag="HERMES_CS7",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side},
            expiry=expiry, width=width,
        )

    def manage_positions(self) -> List[TradeAction]:
        """TP @ debit ≤ 2% width; SL @ debit ≥ 3× entry credit."""
        actions: List[TradeAction] = []
        for trade in self.db.open_trades(self.strategy_id):
            entry_credit = float(trade["entry_credit"])
            width = float(trade.get("width", 5.0))
            quotes = self.broker.get_quote(f"{trade['short_leg']},{trade['long_leg']}") or []
            sq = next((q for q in quotes if q["symbol"] == trade["short_leg"]), None)
            lq = next((q for q in quotes if q["symbol"] == trade["long_leg"]), None)
            if not (sq and lq):
                continue
            debit = round(float(sq["ask"]) - float(lq["bid"]), 2)
            close_reason = None
            if debit <= width * 0.02:
                close_reason = "TP-2pctW"
            elif debit >= entry_credit * 3.0:
                close_reason = "SL-3x"
            if close_reason:
                actions.append(TradeAction(
                    strategy_id=self.strategy_id, symbol=trade["symbol"],
                    order_class="multileg",
                    legs=[
                        {"option_symbol": trade["short_leg"], "side": "buy_to_close",  "quantity": int(trade["lots"])},
                        {"option_symbol": trade["long_leg"],  "side": "sell_to_close", "quantity": int(trade["lots"])},
                    ],
                    price=round(debit * 1.05, 2), side="buy", quantity=1,
                    order_type="debit", tag=f"HERMES_CS7_CLOSE_{close_reason}",
                    strategy_params={"trade_id": trade["id"], "close_reason": close_reason},
                ))
        return actions


# ---------------------------------------------------------------------------
# Priority 3 — TastyTrade45 (16 Δ short, 30-60 DTE entry, 21 DTE hard exit)
# ---------------------------------------------------------------------------
class TastyTrade45(AbstractStrategy):
    PRIORITY = 3
    NAME = "TT45"

    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        width = float(self.config.get("tt45_width", 5.0))
        max_lots = int(self.config.get("tt45_max_lots", 5))
        target_lots = int(self.config.get("tt45_target_lots", 5))
        symbols = list(watchlist)
        self._log(f"↻ scanning {len(symbols)} symbol(s) — target={target_lots} max={max_lots} width={width} delta=0.16")

        for symbol in symbols:
            try:
                # Prioritize completing an existing Iron Condor
                expiry = self.find_active_ic_expiry(symbol)
                if not expiry:
                    expiry = self.find_expiry_in_dte_range(symbol, 30, 60, prefer="max")
                    
                if not expiry:
                    self._log(f"ℹ️ {symbol}: no expiry in 30-60 DTE range; skip.")
                    continue
                existing = {leg["side"] for leg in self.db.open_legs(self.strategy_id, symbol)
                            if leg.get("expiry") == expiry}
                dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                self._log(f"→ {symbol}: expiry={expiry} {dte}DTE existing_sides={sorted(existing)}")

                def factory(side: str):
                    def _b(symbol, expiry, lots, width):
                        chain = self.broker.get_option_chains(symbol, expiry) or []
                        short_leg = self.find_strike_by_delta(chain, side, 0.16, tolerance=0.05)
                        if not short_leg:
                            self._log(f"✗ {symbol} {side}: no strike near 0.16Δ (±0.05) in chain; skip.")
                            return None
                        long_strike = (float(short_leg["strike"]) - width
                                       if side == "put" else float(short_leg["strike"]) + width)
                        # Snap to nearest chain strike rather than requiring exact match
                        long_leg = min(
                            (o for o in chain if o.get("option_type") == side
                             and o["symbol"] != short_leg["symbol"]),
                            key=lambda o: abs(float(o["strike"]) - long_strike),
                            default=None,
                        )
                        if not long_leg:
                            self._log(f"✗ {symbol} {side}: no long leg near strike {long_strike:.2f}; skip.")
                            return None
                        # Direction check: long must be further OTM than short
                        sl_s = float(short_leg["strike"])
                        ll_s = float(long_leg["strike"])
                        if side == "put" and ll_s >= sl_s:
                            self._log(f"✗ {symbol} {side}: long {ll_s} ≥ short {sl_s} (invalid spread); skip.")
                            return None
                        if side == "call" and ll_s <= sl_s:
                            self._log(f"✗ {symbol} {side}: long {ll_s} ≤ short {sl_s} (invalid spread); skip.")
                            return None
                        credit = self.short_credit(short_leg, long_leg)
                        if credit <= 0:
                            self._log(
                                f"✗ {symbol} {side}: credit ${credit:.2f} ≤ 0 "
                                f"(short={sl_s:.0f} long={ll_s:.0f}); skip."
                            )
                            return None
                        return TradeAction(
                            strategy_id=self.strategy_id, symbol=symbol, order_class="multileg",
                            legs=[
                                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
                            ],
                            price=credit, side="sell", quantity=1, order_type="credit",
                            tag="HERMES_TT45",
                            strategy_params={"short_leg": short_leg["symbol"],
                                             "long_leg": long_leg["symbol"], "side_type": side},
                            expiry=expiry, width=width,
                        )
                    return _b

                planned = self.ic.plan(
                    strategy_id=self.strategy_id, symbol=symbol, expiry=expiry,
                    target_lots=target_lots, width=width, max_lots=max_lots,
                    existing_sides=existing,
                    put_action_factory=factory("put"),
                    call_action_factory=factory("call"),
                )
                actions.extend([a for a in planned if a is not None])
            except Exception as exc:                              # noqa: BLE001
                self._log(f"❌ {symbol}: unexpected error — {exc}")
        return actions

    def manage_positions(self) -> List[TradeAction]:
        """Hard exit at 21 DTE; neutralize challenged side (|Δ_short| > 0.30)."""
        actions: List[TradeAction] = []
        for trade in self.db.open_trades(self.strategy_id):
            info = _parse_occ(trade["short_leg"])
            if not info:
                continue
            dte = (info["expiry"] - self.today()).days
            short_delta = abs(self.broker.get_delta(trade["short_leg"]) or 0.0)

            close_reason = None
            if dte <= 21:
                close_reason = "HARD-21DTE"
            elif short_delta > 0.30:
                close_reason = "CHALLENGED-D30"
            if close_reason:
                actions.append(TradeAction(
                    strategy_id=self.strategy_id, symbol=trade["symbol"],
                    order_class="multileg",
                    legs=[
                        {"option_symbol": trade["short_leg"], "side": "buy_to_close",  "quantity": int(trade["lots"])},
                        {"option_symbol": trade["long_leg"],  "side": "sell_to_close", "quantity": int(trade["lots"])},
                    ],
                    price=None, side="buy", quantity=1, order_type="debit",
                    tag=f"HERMES_TT45_CLOSE_{close_reason}",
                    strategy_params={"trade_id": trade["id"], "close_reason": close_reason},
                ))
        return actions


# ---------------------------------------------------------------------------
# Priority 4 — Wheel (puts→assignment→calls; balance puts+calls to max_lots)
# ---------------------------------------------------------------------------
class WheelStrategy(AbstractStrategy):
    PRIORITY = 4
    NAME = "WHEEL"

    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        max_lots = int(self.config.get("wheel_max_lots", 5))
        symbols = list(watchlist)
        self._log(f"↻ scanning {len(symbols)} symbol(s) — max_lots={max_lots} delta=0.30")

        for symbol in symbols:
            shares = int(self.db.equity_position(symbol) or 0)
            shares_lots = shares // 100

            # side_aware_capacity = max_lots - (open_contracts + pending_orders)
            # This includes both pending_orders and pending_approvals so we
            # never exceed max_lots even when trades are awaiting execution.
            call_capacity = self.mm.side_aware_capacity(
                self.strategy_id, symbol, "call", max_lots)
            put_capacity = self.mm.side_aware_capacity(
                self.strategy_id, symbol, "put", max_lots)

            # calls committed = open + pending (what side_aware_capacity already subtracted)
            calls_committed = max_lots - call_capacity
            puts_committed  = max_lots - put_capacity

            self._log(
                f"→ {symbol}: shares={shares} ({shares_lots} lots) "
                f"calls_committed={calls_committed} puts_committed={puts_committed} "
                f"call_cap={call_capacity} put_cap={put_capacity}"
            )

            # ── Calls: cover shares first, bounded by share count and max_lots ──
            share_call_budget = max(0, min(shares_lots, max_lots) - calls_committed)
            wanted_calls = min(call_capacity, share_call_budget)
            if wanted_calls == 0 and call_capacity > 0:
                self._log(f"ℹ️ {symbol} CALL: no shares to cover (shares_lots={shares_lots}); skip calls.")
            elif wanted_calls == 0 and call_capacity == 0:
                self._log(f"ℹ️ {symbol} CALL: at capacity ({calls_committed}/{max_lots}); skip calls.")

            added_calls = 0
            for _ in range(wanted_calls):
                a = self._open_wheel_leg(symbol, "call")
                if a:
                    actions.append(a)
                    added_calls += 1

            # ── Puts: fill remaining capacity toward max_lots total ──
            total_calls = calls_committed + added_calls
            puts_budget = max(0, max_lots - total_calls - puts_committed)
            wanted_puts = min(put_capacity, puts_budget)
            if wanted_puts == 0:
                self._log(
                    f"ℹ️ {symbol} PUT: at capacity or budget exhausted "
                    f"(puts_committed={puts_committed} total_calls={total_calls} max={max_lots}); skip puts."
                )

            for _ in range(wanted_puts):
                a = self._open_wheel_leg(symbol, "put")
                if a:
                    actions.append(a)
        return actions

    def _open_wheel_leg(self, symbol, side) -> Optional[TradeAction]:
        expiry = self.find_expiry_in_dte_range(symbol, 30, 45, prefer="max")
        if not expiry:
            self._log(f"✗ {symbol} {side}: no expiry in 30-45 DTE range; skip.")
            return None
        chain = self.broker.get_option_chains(symbol, expiry) or []
        short = self.find_strike_by_delta(chain, side, 0.30, tolerance=0.05)
        if not short:
            self._log(f"✗ {symbol} {side}: no strike near 0.30Δ (±0.05) in chain for {expiry}; skip.")
            return None
        return TradeAction(
            strategy_id=self.strategy_id, symbol=symbol, order_class="option",
            legs=[{"option_symbol": short["symbol"], "side": "sell_to_open", "quantity": 1}],
            price=round((short["bid"] + short["ask"]) / 2, 2),
            side="sell", quantity=1, order_type="credit", tag="HERMES_WHEEL",
            strategy_params={"side_type": side, "short_leg": short["symbol"]},
            expiry=expiry,
        )

    def manage_positions(self) -> List[TradeAction]:
        """Roll ITM if DTE < 7 (rolls IGNORE max_lots)."""
        actions: List[TradeAction] = []
        for trade in self.db.open_trades(self.strategy_id):
            info = _parse_occ(trade["short_leg"])
            if not info:
                continue
            dte = (info["expiry"] - self.today()).days
            quote = self.broker.get_quote(trade["symbol"]) or {}
            spot = float(quote.get("last", 0))
            short_strike = float(trade.get("short_strike", 0))
            itm = (info["side"] == "put" and spot < short_strike) or \
                  (info["side"] == "call" and spot > short_strike)
            if dte < 7 and itm:
                actions.append(TradeAction(
                    strategy_id=self.strategy_id, symbol=trade["symbol"],
                    order_class="multileg",
                    legs=[
                        # buy back short, sell next-month equivalent
                        {"option_symbol": trade["short_leg"], "side": "buy_to_close",  "quantity": int(trade["lots"])},
                        # placeholder — broker.roll_to_next_month chooses the next short
                        {"option_symbol": self.broker.roll_to_next_month(trade["short_leg"]),
                         "side": "sell_to_open", "quantity": int(trade["lots"])},
                    ],
                    price=None, side="buy", quantity=1, order_type="market",
                    tag="HERMES_WHEEL_ROLL",
                    strategy_params={"trade_id": trade["id"], "ignore_max_lots": True},
                ))
        return actions
