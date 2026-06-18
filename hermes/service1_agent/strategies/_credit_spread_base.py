"""Shared base for the POP-driven vertical-credit-spread strategies.

CS75 (39–45 DTE iron condors) and CS7 (~7 DTE short-cycle spreads) are the
same machine running at two tempos: walk the analysis' POP-augmented S/R
levels, pick the short strike nearest the POP target inside a delta band,
snap a long leg ``width`` away, and submit a credit spread — then manage the
open book on a take-profit / stop-loss / (optional) time-exit policy.

Everything that is *identical* between them lives here. Each concrete
strategy is reduced to a few class attributes plus the handful of hooks
where they genuinely diverge:

==========================  ===============  =================================
Knob                        CS75             CS7
==========================  ===============  =================================
``ANALYSIS_PERIOD``         ``"6m"``         ``"3m"``
``RESCALE_CREDIT_TO_WIDTH`` ``True``         ``False`` (flat min-credit)
``MANAGE_NEEDS_DTE``        ``True``         ``False`` (no time-based exit)
``_resolve_entry_expiry``   39–45, prefer max  exact ``cs7_dte``
``_completion_window``      14 → max         ``[dte - window, dte]``
``_min_credit``             far/near band    flat ``cs7_min_credit_pct``
``_close_reason``           TP-50/75, SL, time  TP-2%W, SL
``_forced_close_on_blocked`` time-exit        (inherited no-op)
==========================  ===============  =================================

Shared tunables (``width``, ``short_delta_min``/``max``, ``pop_target``,
``sl_mult``) are read through :meth:`_tun`, which prepends the subclass'
``KEY_PREFIX`` (``"cs75_"`` / ``"cs7_"``). Strategy-specific tunables are read
directly off the resolved ``Tunables`` inside the relevant hook.

Per the cross-strategy isolation contract, each subclass keeps its own
``strategy_id`` (``NAME``) and only ever reads/manages its own positions —
this base shares *code*, never state.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional, Tuple

from ..core import AbstractStrategy, TradeAction
from hermes.ml.pop_engine import augment_levels_with_pop

from ._helpers import nearest_strike, parse_occ


class CreditSpreadStrategy(AbstractStrategy):
    """POP-driven vertical credit-spread engine shared by CS75 and CS7."""

    # ── Per-strategy configuration (overridden on the subclass) ─────────────
    KEY_PREFIX: str = ""              # "cs75_" / "cs7_" — tunable & config namespace
    ANALYSIS_PERIOD: str = "6m"       # lookback regime for analysis + POP overlay
    RESCALE_CREDIT_TO_WIDTH: bool = False  # re-scale min credit to the snapped width
    MANAGE_NEEDS_DTE: bool = False    # skip a position we can't date (time-based exits)

    # ---- tunable access ----------------------------------------------------
    def _tun(self, t, suffix: str):
        """Read a shared tunable by suffix, e.g. ``_tun(t, "width")``."""
        return t[f"{self.KEY_PREFIX}{suffix}"]

    # =======================================================================
    # ENTRIES
    # =======================================================================
    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        t = await self.load_tunables()
        width = self._tun(t, "width")
        max_lots_global = int(self.config.get(f"{self.KEY_PREFIX}max_lots", 1))
        target_lots_global = int(self.config.get(f"{self.KEY_PREFIX}target_lots", 1))

        # Per-symbol target overrides live in strategy_watchlists.target_lots.
        detailed_wl = await self.db.watchlist.list_watchlist_detailed(self.strategy_id)
        symbols = list(watchlist)

        self._log(
            f"↻ scanning {len(symbols)} symbol(s) — global_target={target_lots_global} "
            f"max={max_lots_global} dte={self._dte_summary(t)}"
        )

        for sym_raw in symbols:
            try:
                symbol, target_lots = self._parse_symbol(sym_raw, detailed_wl, target_lots_global)

                # `max_lots_global` is the strategy hard cap; `target_lots` is
                # the per-entry desired size. Trim target so a watchlist
                # override never exceeds the cap.
                max_lots = max_lots_global
                target_lots = min(target_lots, max_lots_global)

                if await self._in_cooldown(symbol):
                    continue

                analysis = await self.broker.analyze_symbol(symbol, period=self.ANALYSIS_PERIOD)
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue

                # Centralised POP overlay on the institutional-flow S/R levels.
                xgb_pred = await self.db.decisions.latest_prediction(symbol) or {}
                analysis = augment_levels_with_pop(analysis, xgb_pred, period=self.ANALYSIS_PERIOD)

                price = analysis["current_price"]

                # Mode A (fresh IC) vs Mode B (complete an existing single side).
                expiry = await self.find_active_ic_expiry(symbol)
                mode_a = not expiry
                existing_sides: set = set()

                if mode_a:
                    expiry = await self._resolve_entry_expiry(symbol, t)
                    if not expiry:
                        continue                       # hook logged the reason
                else:
                    dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                    lo, hi = self._completion_window(t)
                    if not (lo <= dte <= hi):
                        self._log(f"ℹ️ {symbol}: incomplete IC expiry {expiry} ({dte}DTE) outside {lo}-{hi} completion window; skip.")
                        continue
                    existing_sides = {leg.get("side", "").lower()
                                      for leg in await self.db.trades.open_legs(self.strategy_id, symbol)
                                      if leg.get("expiry") == expiry}

                dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                min_credit = self._min_credit(dte, width, t)
                mode_label = "A (new)" if mode_a else f"B (complete {sorted(existing_sides)})"
                self._log(
                    f"→ {symbol}: mode={mode_label} expiry={expiry} {dte}DTE "
                    f"price=${price:.2f} min_credit=${min_credit:.2f}"
                )

                def factory(side: str):
                    async def _build(symbol, expiry, lots, width):
                        return await self._build_spread_action(
                            symbol=symbol, expiry=expiry, side=side, lots=lots,
                            width=width, min_credit=min_credit, analysis=analysis,
                            current_price=price, t=t,
                        )
                    return _build

                planned = await self.ic.plan(
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

    def _parse_symbol(self, sym_raw: str, detailed_wl: dict, target_lots_global: int) -> Tuple[str, int]:
        """Resolve ``"SYMBOL"`` / ``"SYMBOL:LOTS"`` into ``(symbol, target_lots)``.

        A ``"SYMBOL:LOTS"`` entry typed by the operator overrides DB metadata;
        a bare ``"SYMBOL"`` falls back to the per-symbol ``strategy_watchlists``
        row, then to the strategy-global target.
        """
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
        return symbol, target_lots

    async def _in_cooldown(self, symbol: str) -> bool:
        """True if a trade on ``symbol`` closed inside the re-entry cooldown."""
        last_closed = await self.db.trades.latest_closed_trade_time(self.strategy_id, symbol)
        if not last_closed:
            return False
        cooldown_seconds = int(self.config.get("reentry_cooldown_s", 1800))  # default 30 min
        last_closed_naive = last_closed.replace(tzinfo=None) if last_closed.tzinfo else last_closed
        now_naive = self.now().replace(tzinfo=None) if self.now().tzinfo else self.now()
        time_since_close = (now_naive - last_closed_naive).total_seconds()
        if time_since_close < cooldown_seconds:
            self._log(f"ℹ️ {symbol}: closed recently ({time_since_close:.0f}s ago < {cooldown_seconds}s cooldown); skip entry.")
            return True
        return False

    async def _build_spread_action(self, *, symbol, expiry, side, lots, width,
                                   min_credit, analysis, current_price, t) -> Optional[TradeAction]:
        chain = await self.broker.get_option_chains(symbol, expiry) or []
        if not chain:
            self._log(f"{symbol} {side}: empty chain for {expiry}; skip.")
            return None

        opt_type = side
        # Use the POP already computed for the key levels (institutional flow)
        # rather than scanning the whole chain — cheaper, and POP is the
        # selection criterion anyway. Pick the level whose POP is closest to
        # (and ≥) the target, whose nearest chain strike is in the delta band.
        best_strike = None
        best_pop_diff = 999.0
        max_level_pop = 0.0
        best_pop_val = None

        pop_target = self._tun(t, "pop_target")
        delta_min = self._tun(t, "short_delta_min")
        delta_max = self._tun(t, "short_delta_max")

        target_type = "support" if side == "put" else "resistance"
        levels = [lvl for lvl in analysis.get("key_levels", []) if lvl.get("type") == target_type]

        for level in levels:
            lvl_pop = level.get("pop", 0.0)
            if lvl_pop > max_level_pop:
                max_level_pop = lvl_pop

            if lvl_pop >= pop_target:
                diff = abs(lvl_pop - pop_target)
                if diff < best_pop_diff:
                    strike_opt = nearest_strike(chain, opt_type, level["price"])
                    if not strike_opt:
                        continue

                    # Delta sanity bound: avoid both deep OTM (no premium)
                    # and near-the-money (too much assignment risk).
                    greeks = strike_opt.get("greeks") or {}
                    delta = abs(float(greeks.get("delta", 0.0)))
                    if delta < delta_min or delta > delta_max:
                        continue

                    best_pop_diff = diff
                    best_strike = strike_opt
                    best_pop_val = lvl_pop

        if not best_strike:
            self._log(f"✗ {symbol} {side}: no ≥{pop_target:.0%} POP S/R level found in chain (Best Level POP: {max_level_pop:.1%}); skip.")
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
        # Optionally re-scale min_credit against the actual snapped width —
        # chain strikes may not match the requested ``width`` exactly.
        if self.RESCALE_CREDIT_TO_WIDTH and width > 0:
            effective_min_credit = round(actual_width * (min_credit / width), 2)
        else:
            effective_min_credit = min_credit
        if credit < effective_min_credit:
            self._log(
                f"✗ {symbol} {side}: credit ${credit:.2f} < min ${effective_min_credit:.2f} "
                f"(width={actual_width:.2f}); skip."
            )
            return None

        if best_pop_val is None and short_leg:
            greeks = short_leg.get("greeks") or {}
            delta = abs(float(greeks.get("delta") or 0.0))
            best_pop_val = 1.0 - delta if delta > 0.0 else 0.75

        return TradeAction(
            strategy_id=self.strategy_id,
            symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
            ],
            price=credit, side="sell", quantity=1, order_type="credit",
            tag=f"HERMES_{self.NAME}",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side, "pop": best_pop_val,
                             "short_delta": abs(float((short_leg.get("greeks") or {}).get("delta") or 0.0))},
            dte=(datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days,
            expiry=expiry, width=width,
        )

    # =======================================================================
    # MANAGEMENT
    # =======================================================================
    async def manage_positions(self) -> List[TradeAction]:
        """Drive each open trade through the subclass' close policy.

        Quotes for every leg are fetched in one batched call to avoid N+1
        broker round-trips; the per-strategy ``_close_reason`` hook decides
        whether (and why) to close.
        """
        actions: List[TradeAction] = []
        trades = await self.db.trades.open_trades(self.strategy_id)
        if not trades:
            return actions
        t = await self.load_tunables()
        cfg_width = self._tun(t, "width")

        # Batch fetch quotes for all legs to eliminate N+1 API calls.
        symbols = set()
        for tr in trades:
            symbols.add(tr["short_leg"])
            symbols.add(tr["long_leg"])
        raw_quotes = await self.broker.get_quote(",".join(symbols)) or []
        quotes = {q["symbol"]: q for q in raw_quotes if "symbol" in q}

        for trade in trades:
            short_leg, long_leg = trade["short_leg"], trade["long_leg"]
            entry_credit = float(trade["entry_credit"])
            row_width = trade.get("width")
            width = float(row_width) if row_width is not None else cfg_width

            info = parse_occ(short_leg)
            dte = (info["expiry"] - self.today()).days if info else None
            if dte is None and self.MANAGE_NEEDS_DTE:
                continue

            sq = quotes.get(short_leg)
            lq = quotes.get(long_leg)
            debit, blocked, reason = self.compute_close_debit(sq, lq, width)
            if blocked:
                forced = self._forced_close_on_blocked(trade, dte, width, reason, t)
                if forced is not None:
                    actions.append(forced)
                else:
                    self._log(
                        f"⚠️ {trade['symbol']} {trade.get('side_type')}: "
                        f"close-debit blocked ({reason}); skip eval this tick"
                    )
                continue

            close_reason = self._close_reason(trade, dte, debit, entry_credit, width, t)
            if close_reason:
                # Morning pricing guard: before 10:30 AM ET, don't close unless in profit.
                if self.is_morning_unreliable() and debit >= entry_credit:
                    self._log(
                        f"ℹ️ {trade['symbol']} {trade.get('side_type')}: close deferred (morning pricing unreliable, "
                        f"debit ${debit:.2f} >= entry credit ${entry_credit:.2f})"
                    )
                else:
                    actions.append(self._close_action(trade, debit, close_reason))
        return actions

    def _close_action(self, trade, debit, reason) -> TradeAction:
        # Cap the close limit at the spread width: a W-wide credit spread can
        # never be worth more than W to close, so never bid above it (a 5-wide
        # must not go out at 5.10). The 5% marketability buffer applies only up
        # to that ceiling. Matters most on the stale-quote TIME-EXIT path, which
        # passes a width-priced debit (width * 1.05 would otherwise exceed W).
        price = round(debit * 1.05, 2)
        width = trade.get("width")
        if width:
            price = min(price, round(float(width), 2))
        return TradeAction(
            strategy_id=self.strategy_id, symbol=trade["symbol"],
            order_class="multileg",
            legs=[
                {"option_symbol": trade["short_leg"], "side": "buy_to_close",  "quantity": int(trade["lots"])},
                {"option_symbol": trade["long_leg"],  "side": "sell_to_close", "quantity": int(trade["lots"])},
            ],
            price=price, side="buy", quantity=1,
            order_type="debit", tag=f"HERMES_{self.NAME}_CLOSE_{reason}",
            strategy_params={"trade_id": trade["id"], "close_reason": reason},
        )

    # =======================================================================
    # Hooks — the genuine differences between concrete strategies
    # =======================================================================
    def _dte_summary(self, t) -> str:
        """Human-readable DTE window for the scan log."""
        raise NotImplementedError

    async def _resolve_entry_expiry(self, symbol: str, t) -> Optional[str]:
        """Pick the Mode-A entry expiry (or log + return ``None`` to skip)."""
        raise NotImplementedError

    def _completion_window(self, t) -> Tuple[int, int]:
        """Inclusive ``(min_dte, max_dte)`` window for Mode-B completion."""
        raise NotImplementedError

    def _min_credit(self, dte: int, width: float, t) -> float:
        """Required net credit for an entry at this DTE / width."""
        raise NotImplementedError

    def _close_reason(self, trade, dte, debit, entry_credit, width, t) -> Optional[str]:
        """Return a close-reason tag suffix, or ``None`` to hold."""
        raise NotImplementedError

    def _forced_close_on_blocked(self, trade, dte, width, reason, t) -> Optional[TradeAction]:
        """Optional close to force even when the quote is stale/blocked.

        Default: never force — skip the trade this tick. CS75 overrides this
        to honour its hard time-exit at a width-priced debit.
        """
        return None
