"""CreditSpreads7 — priority-2 strategy (short-cycle spreads).

Entry contract
--------------
- Mode A (no incomplete IC on this symbol):
    * Find the exact 7 DTE expiry; otherwise skip.
    * Open both put and call spreads (Iron Condor).
- Mode B (already one side open):
    * Reuse the existing expiry only if 4–7 DTE remain.
    * Open the missing side only.

Selection
---------
Same key-level + POP heuristic as CS75, but with a slightly looser delta
window (``0.05 ≤ |Δ| ≤ 0.45``) — short-cycle gamma decay accelerates
fast, so a touch more delta is acceptable in exchange for more premium.

Width is configurable (``cs7_width``, default 1). Required net credit is a
fixed 12% of width — short-cycle entries get less expansion margin so the
threshold is lower than CS75's 25%/20%.

Management contract
-------------------
- TP @ debit ≤ 2% of width (very tight take-profit; collect quickly)
- SL @ debit ≥ 3× entry credit
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional

from ..core import AbstractStrategy, TradeAction
from hermes.ml.pop_engine import augment_levels_with_pop

from ._helpers import nearest_strike


class CreditSpreads7(AbstractStrategy):
    PRIORITY = 2
    NAME = "CS7"

    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        t = await self.load_tunables()
        width = t.cs7_width
        max_lots_global = int(self.config.get("cs7_max_lots", 1))
        target_lots_global = int(self.config.get("cs7_target_lots", 1))
        min_credit = round(width * t.cs7_min_credit_pct, 2)

        # DTE target — live-tunable via system_settings; fallback to 7.
        entry_dte = t.cs7_dte

        detailed_wl = await self.db.list_watchlist_detailed(self.strategy_id)
        symbols = list(watchlist)

        self._log(
            f"↻ scanning {len(symbols)} symbol(s) — global_target={target_lots_global} "
            f"max={max_lots_global} min_credit=${min_credit:.2f} dte={entry_dte}"
        )

        for sym_raw in symbols:
            try:
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

                # `max_lots_global` is the strategy-level hard cap (the
                # user-controlled `cs7_max_lots` setting). `target_lots` is
                # the per-entry desired size. Capacity must enforce the hard
                # cap; trim target down to it so a per-symbol override
                # never exceeds the strategy ceiling.
                max_lots = max_lots_global
                target_lots = min(target_lots, max_lots_global)

                # Cooldown check: prevent immediate re-entry if a trade was closed recently
                last_closed = await self.db.latest_closed_trade_time(self.strategy_id, symbol)
                if last_closed:
                    cooldown_seconds = int(self.config.get("reentry_cooldown_s", 1800))  # Default 30 mins
                    last_closed_naive = last_closed.replace(tzinfo=None) if last_closed.tzinfo else last_closed
                    now_naive = self.now().replace(tzinfo=None) if self.now().tzinfo else self.now()
                    time_since_close = (now_naive - last_closed_naive).total_seconds()
                    if time_since_close < cooldown_seconds:
                        self._log(f"ℹ️ {symbol}: closed recently ({time_since_close:.0f}s ago < {cooldown_seconds}s cooldown); skip entry.")
                        continue

                analysis = await self.broker.analyze_symbol(symbol, period="3m")
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue

                # 3M POP regime (short-cycle entries → shorter lookback).
                xgb_pred = await self.db.latest_prediction(symbol) or {}
                analysis = augment_levels_with_pop(analysis, xgb_pred, period="3m")

                price = analysis["current_price"]

                expiry = await self.find_active_ic_expiry(symbol)
                mode_a = not expiry
                existing_sides: set = set()

                if mode_a:
                    # New entry: target the configured DTE (default exact 7).
                    expiry = await self.find_expiry_in_dte_range(symbol, entry_dte, entry_dte)
                    if not expiry:
                        self._log(f"ℹ️ {symbol}: no exact {entry_dte} DTE expiry found for new entry; skip.")
                        continue
                else:
                    # Completion (Mode B): only complete if within the lower half of the DTE window.
                    dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                    completion_min = max(1, entry_dte - t.cs7_completion_window)
                    if not (completion_min <= dte <= entry_dte):
                        self._log(f"ℹ️ {symbol}: incomplete IC expiry {expiry} ({dte}DTE) outside {completion_min}-{entry_dte} completion window; skip.")
                        continue
                    existing_sides = {leg.get("side", "").lower()
                                      for leg in await self.db.open_legs(self.strategy_id, symbol)
                                      if leg.get("expiry") == expiry}

                self._log(f"→ {symbol}: {'MODE A' if mode_a else 'MODE B'} expiry={expiry} existing_sides={sorted(existing_sides)}")

                def factory(side: str):
                    async def _b(symbol, expiry, lots, width):
                        return await self._build_short_premium_spread(
                            symbol=symbol, expiry=expiry, side=side, lots=lots,
                            width=width, min_credit=min_credit, analysis=analysis,
                            current_price=price, t=t,
                        )
                    return _b

                planned = await self.ic.plan(
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

    async def _build_short_premium_spread(self, *, symbol, expiry, side, lots,
                                    width, min_credit, analysis, current_price, t) -> Optional[TradeAction]:
        chain = await self.broker.get_option_chains(symbol, expiry) or []
        if not chain:
            return None

        opt_type = side
        # Same POP-driven selection as CS75, slightly looser delta.
        best_strike = None
        best_pop_diff = 999.0
        max_level_pop = 0.0
        best_pop_val = None

        target_type = "support" if side == "put" else "resistance"
        levels = [lvl for lvl in analysis.get("key_levels", []) if lvl.get("type") == target_type]

        for level in levels:
            lvl_pop = level.get("pop", 0.0)
            if lvl_pop > max_level_pop:
                max_level_pop = lvl_pop

            if lvl_pop >= t.cs7_pop_target:
                diff = abs(lvl_pop - t.cs7_pop_target)
                if diff < best_pop_diff:
                    strike_opt = nearest_strike(chain, opt_type, level["price"])
                    if not strike_opt:
                        continue

                    greeks = strike_opt.get("greeks") or {}
                    delta = abs(float(greeks.get("delta", 0.0)))
                    # 7-DTE allows a slightly higher delta cap than CS75
                    # because gamma decays the position out of trouble fast.
                    if delta < t.cs7_short_delta_min or delta > t.cs7_short_delta_max:
                        continue

                    best_pop_diff = diff
                    best_strike = strike_opt
                    best_pop_val = lvl_pop

        if not best_strike:
            self._log(f"✗ {symbol} {side}: no >75% POP S/R level found in chain (Best Level POP: {max_level_pop:.1%}); skip.")
            return None

        short_leg = best_strike
        long_target = float(short_leg["strike"]) - width if side == "put" else float(short_leg["strike"]) + width
        long_leg = nearest_strike(chain, opt_type, long_target)

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
        if best_pop_val is None and short_leg:
            greeks = short_leg.get("greeks") or {}
            delta = abs(float(greeks.get("delta") or 0.0))
            best_pop_val = 1.0 - delta if delta > 0.0 else 0.75

        return TradeAction(
            strategy_id=self.strategy_id, symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
            ],
            price=credit, side="sell", quantity=1, order_type="credit",
            tag="HERMES_CS7",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side, "pop": best_pop_val,
                             "short_delta": abs(float((short_leg.get("greeks") or {}).get("delta") or 0.0))},
            expiry=expiry, width=width,
        )

    async def manage_positions(self) -> List[TradeAction]:
        """TP @ debit ≤ 2% of width; SL @ debit ≥ 3× entry credit."""
        actions: List[TradeAction] = []
        t = await self.load_tunables()
        # Configured width is the right fallback when a Trade row
        # somehow lacks one — the previous default of 5.0 came from
        # CS75 and silently inflated CS7's TP threshold 5×.
        cfg_width = t.cs7_width
        for trade in await self.db.open_trades(self.strategy_id):
            entry_credit = float(trade["entry_credit"])
            row_width = trade.get("width")
            width = float(row_width) if row_width is not None else cfg_width
            quotes = await self.broker.get_quote(f"{trade['short_leg']},{trade['long_leg']}") or []
            sq = next((q for q in quotes if q["symbol"] == trade["short_leg"]), None)
            lq = next((q for q in quotes if q["symbol"] == trade["long_leg"]), None)
            debit, blocked, reason = self.compute_close_debit(sq, lq, width)
            if blocked:
                # Skip this tick — quote feed will refresh and we'll
                # re-evaluate. Crucially, this prevents a stale-quote
                # SL-3x close on a $1-wide spread firing at $4+ debit
                # because the long-protection leg has bid=0.
                self._log(
                    f"⚠️ {trade['symbol']} {trade.get('side_type')}: "
                    f"close-debit blocked ({reason}); skip eval this tick"
                )
                continue
            close_reason = None
            if debit <= width * t.cs7_tp_pct_width:
                close_reason = "TP-2pctW"
            elif debit >= entry_credit * t.cs7_sl_mult:
                # Stop Loss width safety cap: don't close if already at/above max loss
                if debit < width:
                    close_reason = "SL-3x"
                else:
                    self._log(
                        f"ℹ️ {trade['symbol']} {trade.get('side_type')}: debit ${debit:.2f} "
                        f"is at/above width ${width:.2f} (max loss); skipping SL close."
                    )

            if close_reason:
                # Morning pricing guard: before 10:30 AM ET, do not allow closing if it is not in profit
                if self.is_morning_unreliable() and debit >= entry_credit:
                    self._log(
                        f"ℹ️ {trade['symbol']} {trade.get('side_type')}: close deferred (morning pricing unreliable, "
                        f"debit ${debit:.2f} >= entry credit ${entry_credit:.2f})"
                    )
                else:
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
