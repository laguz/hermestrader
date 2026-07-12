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

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..core import AbstractStrategy, TradeAction
from hermes.ml.pop_engine import FeatureVector, augment_levels_with_pop, predict_pop

from ._helpers import nearest_strike, parse_occ

# Floating-point guard for the honest-POP entry gate: sigmoid(logit(p))
# round-trips a hair under p (0.75 → 0.7499999…), so an exactly-at-target
# strike must not be rejected on the last ulp.
_POP_GATE_EPS = 1e-9


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

                blackout_days = self._tun(t, "event_blackout_days")
                if await self.is_event_gated(symbol, blackout_days):
                    continue

                analysis = await self.broker.analyze_symbol(symbol, period=self.ANALYSIS_PERIOD)
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue

                # Centralised POP overlay on the institutional-flow S/R levels.
                # Prefer the in-process predictor (carries the calibrated
                # predicted_prob + quantile bands the DB row lacks); fall back
                # to the persisted row. Either way a stale prediction is
                # dropped to neutral rather than silently steering today's POP.
                xgb_pred = self._latest_xgb_pred(symbol)
                if xgb_pred is None:
                    xgb_pred = await self.db.decisions.latest_prediction(symbol) or {}
                xgb_pred = self._drop_stale_pred(xgb_pred)
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

                def factory(side: str, symbol=symbol, expiry=expiry, min_credit=min_credit, analysis=analysis, price=price, t=t):
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
            except Exception as exc:
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
            target_lots = symbol_meta.get("target_lots") if symbol_meta.get("target_lots") is not None else target_lots_global
        return symbol, target_lots

    def _latest_xgb_pred(self, symbol: str) -> Optional[Dict[str, Any]]:
        """In-process prediction from the agent's AsyncXGBPredictor, if wired.

        ``main.py`` stashes ``predictor.predict_latest`` in the shared config
        dict; the in-memory dict carries the calibrated ``predicted_prob`` and
        quantile bands that ``write_prediction`` never persists. Returns None
        when the hook is absent (tests, watcher) or has nothing for ``symbol``.
        """
        get_pred = (self.config or {}).get("xgb_predict_latest")
        if not callable(get_pred):
            return None
        try:
            pred = get_pred(symbol)
        except Exception:
            return None
        return dict(pred) if pred else None

    def _drop_stale_pred(self, pred: Dict[str, Any]) -> Dict[str, Any]:
        """Neutralise a prediction older than ``xgb_pred_max_age_s``.

        A row with no ``asof`` is kept as-is (pre-upgrade rows and test stubs
        never carried one). Stale → empty dict, which coerces to the neutral
        0.5 downstream instead of letting a days-old forecast tilt POP.
        """
        asof = pred.get("asof")
        if not asof:
            return pred
        if isinstance(asof, str):
            try:
                asof = datetime.fromisoformat(asof)
            except ValueError:
                return pred
        if asof.tzinfo is None:
            asof = asof.replace(tzinfo=timezone.utc)
        now = self.now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        max_age_s = int(self.config.get("xgb_pred_max_age_s", 86400))
        age_s = (now - asof).total_seconds()
        if age_s > max_age_s:
            self._log(
                f"ℹ️ XGB prediction stale ({age_s/3600:.1f}h old > "
                f"{max_age_s/3600:.1f}h max); using neutral POP inputs."
            )
            return {}
        return pred

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
        # Walk the S/R levels, snap each to its nearest chain strike, and gate
        # on an *honest* POP computed from the actual chain delta of that
        # strike — the market's own implied P(OTM) at the real candidate
        # expiry, absorbing IV skew and term structure for free. The level's
        # overlay POP (z-score delta estimate, hardcoded DTE) remains a
        # dashboard/ranking aid but no longer decides entries: gating on it
        # scored a 43Δ short as 76% "POP" in production and pulled strikes
        # closer than pop_target intends.
        #
        # Every candidate clearing the POP floor is fully priced (long leg
        # snapped, credit measured, min-credit enforced) and the winner is the
        # highest *expected value* under this strategy's own TP/SL policy —
        # POP-proximity always took the closest qualifying strike, which
        # loses whenever a farther strike carries disproportionate premium
        # (calibrated-POP × TP-profit beats a few extra cents of credit).
        dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days

        best: Optional[Dict[str, Any]] = None
        max_pop_seen = 0.0
        credit_reject: Optional[Tuple[float, float, float]] = None

        pop_target = self._tun(t, "pop_target")
        delta_min = self._tun(t, "short_delta_min")
        delta_max = self._tun(t, "short_delta_max")

        xgb_prob = float(analysis.get("xgb_prob", 0.5))
        current_vol = float(analysis.get("current_vol", 0.30))
        avg_vol = float(analysis.get("avg_vol", 0.25))

        target_type = "support" if side == "put" else "resistance"
        levels = [lvl for lvl in analysis.get("key_levels", []) if lvl.get("type") == target_type]

        for level in levels:
            strike_opt = nearest_strike(chain, opt_type, level["price"])
            if not strike_opt:
                continue

            # Delta sanity bound: avoid both deep OTM (no premium)
            # and near-the-money (too much assignment risk).
            greeks = strike_opt.get("greeks") or {}
            delta = abs(float(greeks.get("delta", 0.0)))
            if delta < delta_min or delta > delta_max:
                continue

            iv = greeks.get("mid_iv")
            if iv is None:
                iv = greeks.get("smv_vol")
            pop = predict_pop(FeatureVector(
                delta=delta,
                xgb_prob=xgb_prob,
                current_vol=current_vol,
                avg_vol=avg_vol,
                protection_score=float(level.get("protection", 1.0)),
                side=side,
                period=self.ANALYSIS_PERIOD.upper(),
                symbol=symbol,
                dte=float(dte),
                sigma=float(iv) if iv is not None else None,
            ))
            if pop > max_pop_seen:
                max_pop_seen = pop
            if pop < pop_target - _POP_GATE_EPS:
                continue

            # Snap the long strike to the nearest chain strike below/above
            # the short; long must be a distinct strike further OTM.
            sl_strike = float(strike_opt["strike"])
            long_target = sl_strike - width if side == "put" else sl_strike + width
            long_leg = nearest_strike(chain, opt_type, long_target)
            if not long_leg or long_leg["symbol"] == strike_opt["symbol"]:
                continue
            ll_strike = float(long_leg["strike"])
            if side == "put" and ll_strike >= sl_strike:
                continue
            if side == "call" and ll_strike <= sl_strike:
                continue
            actual_width = abs(sl_strike - ll_strike)

            credit = self.short_credit(strike_opt, long_leg)
            # Optionally re-scale min_credit against the actual snapped width —
            # chain strikes may not match the requested ``width`` exactly.
            if self.RESCALE_CREDIT_TO_WIDTH and width > 0:
                effective_min_credit = round(actual_width * (min_credit / width), 2)
            else:
                effective_min_credit = min_credit
            if credit < effective_min_credit:
                if credit_reject is None:
                    credit_reject = (credit, effective_min_credit, actual_width)
                continue

            ev = self._expected_value(pop=pop, credit=credit,
                                      width=actual_width, dte=dte, t=t)
            if best is None or ev > best["ev"]:
                best = {
                    "short": strike_opt, "long": long_leg, "pop": pop,
                    "delta": delta, "credit": credit,
                    "actual_width": actual_width, "ev": ev,
                }

        if best is None:
            if credit_reject is not None:
                credit, effective_min_credit, actual_width = credit_reject
                self._log(
                    f"✗ {symbol} {side}: credit ${credit:.2f} < min ${effective_min_credit:.2f} "
                    f"(width={actual_width:.2f}); skip."
                )
            else:
                self._log(f"✗ {symbol} {side}: no ≥{pop_target:.0%} POP S/R level found in chain (Best Level POP: {max_pop_seen:.1%}); skip.")
            return None

        short_leg, long_leg = best["short"], best["long"]
        self._log(
            f"→ {symbol} {side}: short={float(short_leg['strike']):.2f} "
            f"long={float(long_leg['strike']):.2f} width={best['actual_width']:.2f} "
            f"pop={best['pop']:.1%} credit=${best['credit']:.2f} ev=${best['ev']:.2f}"
        )

        return TradeAction(
            strategy_id=self.strategy_id,
            symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
            ],
            price=best["credit"], side="sell", quantity=1, order_type="credit",
            tag=f"HERMES_{self.NAME}",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side, "pop": best["pop"],
                             "short_delta": best["delta"],
                             "ev": round(best["ev"], 4)},
            dte=dte,
            expiry=expiry, width=width,
        )

    # ---- entry economics (EV under this strategy's management policy) ------
    def _tp_profit(self, credit: float, width: float, dte: int, t) -> float:
        """Per-share profit captured when this strategy's TP fires.

        Base default: half the credit (the canonical 50%-capture rule).
        Subclasses whose close policy differs must override so EV ranking
        prices their actual exit, not a generic one.
        """
        return 0.5 * credit

    def _sl_loss(self, credit: float, width: float, t) -> float:
        """Per-share loss realized when this strategy's SL fires.

        Close debit at the stop is ``sl_mult × credit`` → loss of
        ``(sl_mult − 1) × credit``, capped at the structural max loss
        (width − credit; the SL width-cap suppresses closes beyond it).
        """
        try:
            sl_mult = float(self._tun(t, "sl_mult"))
        except (KeyError, TypeError, ValueError):
            sl_mult = 2.5
        return min((sl_mult - 1.0) * credit, max(width - credit, 0.0))

    def _expected_value(self, *, pop: float, credit: float, width: float,
                        dte: int, t) -> float:
        """First-order EV per share under the management policy.

        ``pop`` is the calibrated P(win) — post-#156 it reflects realized
        outcomes under this very TP/SL policy, so pairing it with the
        policy's win/loss amounts prices the trade as it will actually be
        managed, not as if held to expiry.
        """
        win = self._tp_profit(credit, width, dte, t)
        loss = self._sl_loss(credit, width, t)
        return pop * win - (1.0 - pop) * loss

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
            mid_debit, exec_debit, blocked, reason = self.compute_close_debit(sq, lq, width)
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

            # SL/TP decisions use mid_debit (matches how entry credit is measured).
            # exec_debit (ask-bid) is used only for the order limit price.
            close_reason = self._close_reason(trade, dte, mid_debit, entry_credit, width, t)
            # Width safety cap: never SL-close when the execution cost is already
            # at or above max loss (width). No benefit paying full-width to exit.
            if (close_reason and "SL" in close_reason
                    and exec_debit is not None and exec_debit >= width):
                self._log(
                    f"ℹ️ {trade['symbol']} {trade.get('side_type')}: SL suppressed — "
                    f"exec_debit ${exec_debit:.2f} >= width ${width:.2f} (max loss)"
                )
                close_reason = None
            # No exec_debit recheck for TP: the close order's limit price below
            # is always capped at the TP target via max_price, regardless of
            # how wide exec_debit is — so a wide market can't make this
            # overpay. It just posts a resting limit at the real target price;
            # if the market never reaches it the order won't fill, and
            # TransactionManager.upsert_positions() re-arms the trade back to
            # OPEN once it notices the close order stopped resting. Requiring
            # exec_debit to independently clear the same threshold before
            # ever trying only delayed genuine TPs on structurally wide
            # (e.g. cheap/illiquid) option markets without buying any extra
            # price safety.
            if close_reason:
                # Morning pricing guard: before 10:30 AM ET, don't close unless in profit.
                if self.is_morning_unreliable() and mid_debit >= entry_credit:
                    self._log(
                        f"ℹ️ {trade['symbol']} {trade.get('side_type')}: close deferred (morning pricing unreliable, "
                        f"debit ${mid_debit:.2f} >= entry credit ${entry_credit:.2f})"
                    )
                else:
                    max_price = None
                    if "TP" in close_reason:
                        max_price = round(entry_credit - self._tp_profit(entry_credit, width, dte, t), 2)
                    actions.append(self._close_action(trade, exec_debit, close_reason, max_price=max_price))
        return actions

    def _close_action(self, trade, debit, reason, max_price=None) -> TradeAction:
        # Cap the close limit at the spread width: a W-wide credit spread can
        # never be worth more than W to close, so never bid above it (a 5-wide
        # must not go out at 5.10). The 5% marketability buffer applies only up
        # to that ceiling. Matters most on the stale-quote TIME-EXIT path, which
        # passes a width-priced debit (width * 1.05 would otherwise exceed W).
        price = round(debit * 1.05, 2)
        width = trade.get("width")
        if width:
            price = min(price, round(float(width), 2))
        if max_price is not None:
            price = min(price, round(float(max_price), 2))
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
