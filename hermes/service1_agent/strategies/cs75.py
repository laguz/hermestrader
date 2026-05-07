"""CreditSpreads75 — priority-1 strategy.

Entry contract
--------------
- Mode A (no incomplete IC on this symbol):
    * Find an expiry in the 39–45 DTE window (prefer the latest).
    * Open both put and call spreads (Iron Condor).
- Mode B (already one side open):
    * Reuse the existing expiry if 14–45 DTE; otherwise skip.
    * Open the missing side only.

Selection
---------
Walk the analysis' key_levels (institutional-flow S/R, augmented with POP
by ``augment_levels_with_pop``). Pick the level whose POP is closest to
0.75 (i.e. ≥75% probability the short stays OTM). Verify the chain strike
nearest that level has ``0.05 ≤ |Δ| ≤ 0.40``.

Width is configurable (``cs75_width`` env, default 5). Required net credit
is 25% of the actual snapped width for 30–45 DTE entries, 20% for 14–29 DTE.

Management contract
-------------------
- TP @ 50% credit captured for 21–45 DTE
- TP @ 75% credit captured for <21 DTE
- SL @ 2.5× entry credit
- Hard time exit ≤ 8 DTE

Closing orders are tagged ``HERMES_CS75_CLOSE_<reason>`` so the broker-side
tag matcher knows they're not new entries.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional

from ..core import AbstractStrategy, TradeAction
from hermes.ml.pop_engine import augment_levels_with_pop

from ._helpers import nearest_strike, parse_occ


class CreditSpreads75(AbstractStrategy):
    PRIORITY = 1
    NAME = "CS75"

    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        width = float(self.config.get("cs75_width", 5.0))
        max_lots_global = int(self.config.get("cs75_max_lots", 1))
        target_lots_global = int(self.config.get("cs75_target_lots", 1))

        # Per-symbol target overrides live in strategy_watchlists.target_lots.
        detailed_wl = self.db.list_watchlist_detailed(self.strategy_id)
        symbols = list(watchlist)

        self._log(f"↻ scanning {len(symbols)} symbol(s) — global_target={target_lots_global} max={max_lots_global}")

        for sym_raw in symbols:
            try:
                # "SYMBOL:LOTS" entries (typed by the operator) override DB
                # metadata; bare "SYMBOL" entries fall back to the per-symbol
                # row in strategy_watchlists.
                if ":" in sym_raw:
                    symbol, lots_str = sym_raw.split(":", 1)
                    symbol = symbol.strip()
                    try:
                        target_lots = int(lots_str)
                    except ValueError:
                        target_lots = target_lots_global
                else:
                    symbol = sym_raw
                    symbol_meta = detailed_wl.get(symbol, {})
                    target_lots = symbol_meta.get("target_lots") or target_lots_global

                max_lots = target_lots

                analysis = self.broker.analyze_symbol(symbol, period="6m")
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue

                # Centralised POP: 6M regime for CS75 (longer-dated entries).
                xgb_pred = self.db.latest_prediction(symbol) or {}
                analysis = augment_levels_with_pop(analysis, xgb_pred, period="6m")

                price = analysis["current_price"]

                # Mode A vs Mode B (see module docstring).
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
                    existing_sides = {leg.get("side", "").lower()
                                      for leg in self.db.open_legs(self.strategy_id, symbol)
                                      if leg.get("expiry") == expiry}

                if not expiry:
                    self._log(f"ℹ️ {symbol}: no expiry found in 39-45 DTE range; skip.")
                    continue

                # Required credit varies with DTE band.
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
        # Use POP already calculated for key levels (institutional flow)
        # rather than scanning the entire chain — much cheaper, and POP is
        # the actual selection criterion anyway.
        best_strike = None
        best_pop_diff = 999.0
        max_level_pop = 0.0

        target_type = "support" if side == "put" else "resistance"
        levels = [lvl for lvl in analysis.get("key_levels", []) if lvl.get("type") == target_type]

        for level in levels:
            lvl_pop = level.get("pop", 0.0)
            if lvl_pop > max_level_pop:
                max_level_pop = lvl_pop

            if lvl_pop >= 0.75:
                diff = abs(lvl_pop - 0.75)
                if diff < best_pop_diff:
                    strike_opt = nearest_strike(chain, opt_type, level["price"])
                    if not strike_opt:
                        continue

                    # Delta sanity bound: avoid both deep OTM (no premium)
                    # and near-the-money (too much assignment risk).
                    greeks = strike_opt.get("greeks") or {}
                    delta = abs(float(greeks.get("delta", 0.0)))
                    if delta < 0.05 or delta > 0.40:
                        continue

                    best_pop_diff = diff
                    best_strike = strike_opt

        if not best_strike:
            self._log(f"✗ {symbol} {side}: no >75% POP S/R level found in chain (Best Level POP: {max_level_pop:.1%}); skip.")
            return None

        short_leg = best_strike
        # Snap the long strike to the nearest chain strike below/above the short.
        long_target = float(short_leg["strike"]) - width if side == "put" else float(short_leg["strike"]) + width
        long_leg = nearest_strike(chain, opt_type, long_target)
        if not long_leg or long_leg["symbol"] == short_leg["symbol"]:
            self._log(
                f"✗ {symbol} {side}: no distinct long leg for short={short_leg['strike']:.2f} "
                f"long_target={long_target:.2f}; skip."
            )
            return None
        sl_strike = float(short_leg["strike"])
        ll_strike = float(long_leg["strike"])
        # Direction sanity — long must be further OTM than short.
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
        # Re-scale min_credit against the actual width — chain strikes may
        # not match the requested ``width`` exactly.
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
        """TP @ 50% (DTE 21–45) or 75% (DTE<21); SL @ 2.5×; time exit ≤ 8 DTE."""
        actions: List[TradeAction] = []
        trades = self.db.open_trades(self.strategy_id)
        if not trades:
            return actions

        # Batch fetch quotes for all legs to eliminate N+1 API calls.
        symbols = set()
        for t in trades:
            symbols.add(t["short_leg"])
            symbols.add(t["long_leg"])

        raw_quotes = self.broker.get_quote(",".join(symbols)) or []
        quotes = {q["symbol"]: q for q in raw_quotes if "symbol" in q}

        for trade in trades:
            short_leg, long_leg = trade["short_leg"], trade["long_leg"]
            entry_credit = float(trade["entry_credit"])
            info = parse_occ(short_leg)
            if not info:
                continue
            dte = (info["expiry"] - self.today()).days

            sq = quotes.get(short_leg)
            lq = quotes.get(long_leg)
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
