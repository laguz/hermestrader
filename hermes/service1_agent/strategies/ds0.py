"""DS0 — priority-6, 0 DTE mean-reversion debit spreads (docs/ds0_spec.md).

A fully rule-based reversion-toward-a-level trade on daily-expiry
underlyings (watchlist seeded with QQQ). Each morning a side qualifies when
a 3-month S/R level (a) has POP ≥ ``ds0_pop_target`` that it **holds** —
the same engine/number CS7 uses — and (b) sits inside today's expected
range, **session open ± Wilder ATR(``ds0_atr_period``)**: support in
``[open − ATR, open]``, resistance in ``[open, open + ATR]``. Both sides
are independent and may be open at once.

Direction pairing — INTENTIONAL, operator-specified 2026-07-10; do NOT
"correct" this in an audit: a qualified **support** arms a **put** debit
spread and a qualified **resistance** arms a **call** debit spread. The
spread points *toward* the level, and its fixed $0.10 day-limit only fills
once price has moved *away* from the level (that is what makes the spread
cheap) — the position bets the overextension reverts back toward the
strong level. There is deliberately **no price-proximity/touch trigger**:
the $0.10 limit itself is the trigger. This replaced the original
touch-fade design (support→call / resistance→put); see the spec's
revision note before flagging the pairing as inverted.

Everything after submission is price-bound:

- Entry: day-limit **buy** at ``ds0_open_price`` (default $0.10). Never
  repriced or chased; unfilled at end of day, the order dies.
- Exit: as soon as the fill is visible, a resting **sell** day-limit at
  ``ds0_close_price`` (default $0.40). No stop loss — the debit paid is the
  entire accepted risk.
- 3:01 PM ET sweep (``ds0_sweep_time``): anything marked at/above
  ``ds0_sweep_min`` (default $0.13) but shy of the target is closed at the
  live executable credit; anything below the floor rides to expiration as
  the accepted loss. (At/above the $0.40 target the resting TP should have
  filled; if it somehow hasn't, the sweep closing it only banks more.)
- 3:50 PM ET assignment guard (``ds0_assignment_guard``, default ON): a
  still-open spread whose near-money strike is ITM or within
  ``ds0_guard_band`` of spot is force-closed — QQQ options are
  American-style / physically settled, so expiring through the strikes is
  an assignment event, not just losing the debit. Clearly-OTM spreads are
  left to expire untouched.

One shot per side per symbol per day: any DS0 trade (OPEN/CLOSING/CLOSED)
for that (symbol, side, today-expiry) blocks re-entry, wins included.

Subclasses :class:`CreditSpreadStrategy` for its shared helpers
(``_parse_symbol`` / ``_latest_xgb_pred`` / ``_drop_stale_pred``) exactly as
HermesAlpha does, overriding both engine hooks; the credit base's POP-walk /
min-credit machinery is inverted for a debit structure and unused here.

Tag round-trips as ``HERMES_DS0`` / ``HERMES-DS0``; closes as
``HERMES_DS0_CLOSE_<reason>`` (CLAUDE.md safety rule #5).
"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from hermes.market_hours import ET as _ET
from hermes.ml.pop_engine import FeatureVector, augment_levels_with_pop, predict_pop

from ._credit_spread_base import CreditSpreadStrategy, TradeAction, _POP_GATE_EPS
from ._helpers import nearest_strike

# Broker order statuses that mean a close order is still working (mirrors
# _engine_pipeline.sync_positions' active-legs scan).
_ACTIVE_ORDER_STATUSES = {"open", "partially_filled", "pending", "accepted",
                          "calculated"}


def _parse_hhmm(raw: Any, default: time) -> time:
    try:
        return datetime.strptime(str(raw).strip(), "%H:%M").time()
    except (TypeError, ValueError):
        return default


class DebitSpreads0DTE(CreditSpreadStrategy):
    PRIORITY = 6
    NAME = "DS0"

    KEY_PREFIX = "ds0_"
    ANALYSIS_PERIOD = "3m"
    MANAGE_NEEDS_DTE = False

    # ── time helpers ─────────────────────────────────────────────────────────
    def _now_et(self) -> datetime:
        now = self.now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.astimezone(_ET)

    # ── quote helpers ────────────────────────────────────────────────────────
    @staticmethod
    def _mid(opt: Optional[Dict[str, Any]]) -> Optional[float]:
        """Bid/ask midpoint, or ``None`` when either side is missing/zero."""
        if not opt:
            return None
        try:
            bid = float(opt.get("bid") or 0)
            ask = float(opt.get("ask") or 0)
        except (TypeError, ValueError):
            return None
        if bid <= 0 or ask <= 0:
            return None
        return (bid + ask) / 2.0

    @staticmethod
    def _spread_close_value(long_quote, short_quote, width):
        """Sane credit-to-close for a long vertical (mirror of
        ``compute_close_debit``, roles inverted: we own the long leg and
        sold the short leg, so closing sells the spread for a credit).

        Returns ``(mid_credit, exec_credit, blocked, reason)``. ``mid_credit``
        (mid−mid) drives the sweep decision, matching how the entry debit is
        measured; ``exec_credit`` (long_bid − short_ask) is the worst-case
        executable credit used as the close limit so it actually fills.
        """
        if not (long_quote and short_quote):
            return None, None, True, "missing quote leg"
        try:
            lb = float(long_quote.get("bid") or 0)
            la = float(long_quote.get("ask") or 0)
            sb = float(short_quote.get("bid") or 0)
            sa = float(short_quote.get("ask") or 0)
            w = float(width or 0)
        except (TypeError, ValueError):
            return None, None, True, "quote parse error"
        if lb <= 0 or la <= 0 or sb <= 0 or sa <= 0:
            return None, None, True, f"stale quote: long={lb}/{la} short={sb}/{sa}"
        mid_credit = max(0.0, round(((lb + la) / 2) - ((sb + sa) / 2), 2))
        # A W-wide vertical can never be worth more than W; a mid beyond that
        # means a phantom/stale leg quote.
        if w > 0 and mid_credit > w * 1.10:
            return None, None, True, (
                f"phantom credit ${mid_credit:.2f} > width ${w:.2f} × 1.10"
            )
        exec_credit = max(0.0, round(lb - sa, 2))
        return mid_credit, exec_credit, False, ""

    async def _spot(self, symbol: str) -> Optional[float]:
        quotes = await self.broker.get_quote(symbol) or []
        if not quotes:
            return None
        q = quotes[0]
        raw = q.get("last") if q.get("last") is not None else q.get("close")
        try:
            spot = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            return None
        return spot if spot > 0 else None

    async def _today_open(self, symbol: str) -> Optional[float]:
        """Today's regular-session opening print, from the quote's ``open``.

        Fixed for the whole day — the open ± ATR range must not drift with
        spot intraday. ``None`` (pre-open, halted, stub without the field)
        means the symbol can't qualify and is skipped.
        """
        quotes = await self.broker.get_quote(symbol) or []
        if not quotes:
            return None
        try:
            raw = quotes[0].get("open")
            opn = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            return None
        return opn if opn > 0 else None

    async def _atr(self, symbol: str, period: int) -> Optional[float]:
        """Wilder ATR over the last ``period`` completed daily bars.

        True range includes overnight gaps (max of high−low, |high−prev
        close|, |low−prev close|); the seed is the simple mean of the first
        ``period`` TRs, then Wilder smoothing over the rest. Today's partial
        bar is excluded so the entry range stays anchored. ``None`` when
        history is too short/invalid — the symbol is skipped, never traded
        on a guessed range.
        """
        if period < 1:
            return None
        end = self._now_et().date()
        start = end - timedelta(days=period * 3 + 10)
        bars = await self.broker.get_history(
            symbol, start=start.isoformat(), end=end.isoformat()) or []
        rows: List[Tuple[str, float, float, float]] = []
        for b in bars:
            day = str(b.get("date", ""))[:10]
            if day >= end.isoformat():
                continue
            try:
                rows.append((day, float(b["high"]), float(b["low"]),
                             float(b["close"])))
            except (KeyError, TypeError, ValueError):
                continue
        rows.sort(key=lambda r: r[0])   # mock/history feeds vary in ordering
        if len(rows) < period + 1:
            return None
        trs: List[float] = []
        prev_close = rows[0][3]
        for _, high, low, close in rows[1:]:
            trs.append(max(high - low, abs(high - prev_close),
                           abs(low - prev_close)))
            prev_close = close
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return atr if atr > 0 else None

    # =======================================================================
    # ENTRIES — open±ATR-qualified reversion toward a strong S/R level
    # =======================================================================
    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        t = await self.load_tunables()

        now_et = self._now_et()
        cutoff = _parse_hhmm(t.ds0_entry_cutoff, time(14, 0))
        if now_et.time() >= cutoff:
            return actions

        open_price = float(t.ds0_open_price)
        pop_target = float(t.ds0_pop_target)
        width = float(t.ds0_width)
        atr_period = int(t.ds0_atr_period)
        ttl_s = int(t.ds0_approval_ttl_s)

        # The engine's _watchlist_for falls back to the global default list
        # when a strategy's own watchlist is empty. For DS0 that fallback is
        # a footgun (SPY/IWM in the global list are perfectly tradable 0DTE
        # symbols the operator never armed) — an empty own-watchlist means
        # idle, full stop.
        own_wl = await self.db.watchlist.list_watchlist(self.strategy_id)
        if not own_wl:
            self._log("ℹ️ DS0 watchlist is empty — idle (no global fallback).")
            return actions
        own_syms = {s.split(":", 1)[0].strip().upper() for s in own_wl}

        # DS0 is max-only sizing (like WHEEL): a single ds0_max_lots knob,
        # no separate target that can silently clamp it back down. Per-symbol
        # watchlist overrides (target_lots column or "SYMBOL:LOTS" inline
        # syntax) are authoritative when present.
        max_lots_global = int(self.config.get("ds0_max_lots", 1))
        detailed_wl = await self.db.watchlist.list_watchlist_detailed(self.strategy_id)
        symbols = [s for s in dict.fromkeys(watchlist)
                   if s.split(":", 1)[0].strip().upper() in own_syms]

        self._log(
            f"↻ scanning {len(symbols)} symbol(s) — 0DTE reversion, "
            f"open≤${open_price:.2f} pop≥{pop_target:.0%} range=open±ATR{atr_period}"
        )

        for sym_raw in symbols:
            try:
                symbol, target_lots = self._parse_symbol(sym_raw, detailed_wl, max_lots_global)
                if target_lots <= 0:
                    continue

                expiry = await self.find_expiry_in_dte_range(symbol, 0, 0)
                if not expiry:
                    self._log(f"ℹ️ {symbol}: no same-day expiration; skip.")
                    continue

                today_open = await self._today_open(symbol)
                if today_open is None:
                    self._log(f"⚠️ {symbol}: no session open on the quote; skip.")
                    continue
                atr = await self._atr(symbol, atr_period)
                if atr is None:
                    self._log(
                        f"⚠️ {symbol}: not enough daily bars for ATR{atr_period}; skip."
                    )
                    continue

                analysis = await self.broker.analyze_symbol(symbol, period=self.ANALYSIS_PERIOD)
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue
                xgb_pred = self._latest_xgb_pred(symbol)
                if xgb_pred is None:
                    xgb_pred = await self.db.decisions.latest_prediction(symbol) or {}
                xgb_pred = self._drop_stale_pred(xgb_pred)
                analysis = augment_levels_with_pop(analysis, xgb_pred, period=self.ANALYSIS_PERIOD)

                price = float(analysis["current_price"])
                chain = await self.broker.get_option_chains(symbol, expiry) or []
                if not chain:
                    self._log(f"⚠️ {symbol}: empty chain for {expiry}; skip.")
                    continue

                # Reversion pairing (intentional — see module docstring):
                # qualified support → put debit spread; resistance → call.
                for side, level_type in (("put", "support"), ("call", "resistance")):
                    action = await self._try_side(
                        symbol=symbol, side=side, level_type=level_type,
                        analysis=analysis, chain=chain, price=price,
                        expiry=expiry, width=width, open_price=open_price,
                        pop_target=pop_target, today_open=today_open,
                        atr=atr, lots=target_lots, ttl_s=ttl_s, now_et=now_et,
                    )
                    if action is not None:
                        actions.append(action)
            except Exception as exc:
                self._log(f"❌ {sym_raw}: {exc}")
        return actions

    async def _try_side(self, *, symbol: str, side: str, level_type: str,
                        analysis: Dict[str, Any], chain: List[Dict[str, Any]],
                        price: float, expiry: str, width: float,
                        open_price: float, pop_target: float,
                        today_open: float, atr: float, lots: int, ttl_s: int,
                        now_et: datetime) -> Optional[TradeAction]:
        if price <= 0:
            return None
        # A side qualifies only when the level sits inside today's expected
        # range anchored at the session open: support in [open − ATR, open],
        # resistance in [open, open + ATR]. Bounds inclusive. No proximity
        # trigger beyond this — the $0.10 day-limit is the trigger.
        lo, hi = ((today_open - atr, today_open) if level_type == "support"
                  else (today_open, today_open + atr))
        in_range = [lvl for lvl in analysis.get("key_levels", [])
                    if lvl.get("type") == level_type
                    and lvl.get("price") is not None
                    and lo <= float(lvl["price"]) <= hi]
        if not in_range:
            return None

        # One shot per side per symbol per day — a win, a loss and a resting
        # or queued entry all block alike.
        if await self.db.trades.count_trades_for_expiry(self.strategy_id, symbol, side, expiry):
            self._log(f"ℹ️ {symbol} {side}: already traded this side today; skip.")
            return None
        if await self.db.trades.count_pending_orders(self.strategy_id, symbol, side, expiry):
            self._log(f"ℹ️ {symbol} {side}: entry already pending/queued; skip.")
            return None

        # POP gate — the probability the level HOLDS, computed exactly as CS7
        # would for a credit spread at that level (resistance holding is the
        # call-credit view; support holding is the put-credit view). Levels
        # are tried nearest-to-open first; the first one passing wins.
        gate_side = "call" if level_type == "resistance" else "put"
        level: Optional[Dict[str, Any]] = None
        pop = 0.0
        delta = 0.0
        for cand in sorted(in_range,
                           key=lambda lvl: abs(float(lvl["price"]) - today_open)):
            gate_opt = nearest_strike(chain, gate_side, float(cand["price"]))
            if not gate_opt:
                continue
            greeks = gate_opt.get("greeks") or {}
            raw_delta = greeks.get("delta")
            if raw_delta is None:
                self._log(f"✗ {symbol} {side}: no delta at level strike; skip level.")
                continue
            cand_delta = abs(float(raw_delta))
            if cand_delta <= 0.0:
                continue
            iv = greeks.get("mid_iv")
            if iv is None:
                iv = greeks.get("smv_vol")
            cand_pop = predict_pop(FeatureVector(
                delta=cand_delta,
                xgb_prob=float(analysis.get("xgb_prob", 0.5)),
                current_vol=float(analysis.get("current_vol", 0.30)),
                avg_vol=float(analysis.get("avg_vol", 0.25)),
                protection_score=float(cand.get("protection", 1.0)),
                side=gate_side,
                period=self.ANALYSIS_PERIOD.upper(),
                symbol=symbol,
                dte=0.0,
                sigma=float(iv) if iv is not None else None,
            ))
            if cand_pop >= pop_target - _POP_GATE_EPS:
                level, pop, delta = cand, cand_pop, cand_delta
                break
            self._log(
                f"✗ {symbol} {side}: level {float(cand['price']):.2f} POP "
                f"{cand_pop:.1%} < {pop_target:.0%}; skip level."
            )
        if level is None:
            return None

        selected = self._select_debit_spread(chain, side, price, width, open_price)
        if selected is None:
            self._log(
                f"✗ {symbol} {side}: no OTM {width:g}-wide pair with mid debit "
                f"≤ ${open_price:.2f}; skip."
            )
            return None
        long_leg, short_leg, mid_debit = selected
        actual_width = abs(float(long_leg["strike"]) - float(short_leg["strike"]))

        valid_until = (now_et + timedelta(seconds=ttl_s)).isoformat() if ttl_s > 0 else None
        self._log(
            f"→ {symbol} {side}: revert toward {level_type} "
            f"{float(level['price']):.2f} (pop {pop:.1%}, range "
            f"{lo:.2f}–{hi:.2f}) long={float(long_leg['strike']):.2f} "
            f"short={float(short_leg['strike']):.2f} mid=${mid_debit:.2f} "
            f"limit=${open_price:.2f}"
        )
        sp: Dict[str, Any] = {
            "short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
            "side_type": side, "pop": pop, "short_delta": delta,
            "level": float(level["price"]), "today_open": today_open,
            "atr": atr,
        }
        if valid_until is not None:
            sp["valid_until"] = valid_until
        return TradeAction(
            strategy_id=self.strategy_id,
            symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": long_leg["symbol"], "side": "buy_to_open", "quantity": lots},
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
            ],
            price=open_price, side="buy", quantity=1, order_type="debit",
            tag=f"HERMES_{self.NAME}",
            strategy_params=sp,
            dte=0, expiry=expiry, width=actual_width,
        )

    def _select_debit_spread(self, chain: List[Dict[str, Any]], opt_type: str,
                             spot: float, width: float,
                             max_debit: float) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], float]]:
        """Closest-to-the-money OTM vertical whose mid debit is ≤ ``max_debit``.

        The long leg sits nearest the money in the bounce direction; the short
        leg is snapped ``width`` further out. Walking outward from spot, the
        first pair cheap enough wins — anything closer would cost more.
        """
        seen: set = set()
        otm: List[Dict[str, Any]] = []
        for o in chain:
            if o.get("option_type") != opt_type:
                continue
            try:
                k = float(o["strike"])
            except (KeyError, TypeError, ValueError):
                continue
            if opt_type == "put" and k >= spot:
                continue
            if opt_type == "call" and k <= spot:
                continue
            if k in seen:
                continue
            seen.add(k)
            otm.append(o)
        otm.sort(key=lambda o: abs(float(o["strike"]) - spot))

        for long_leg in otm:
            lk = float(long_leg["strike"])
            target = lk - width if opt_type == "put" else lk + width
            short_leg = nearest_strike(chain, opt_type, target)
            if not short_leg or short_leg["symbol"] == long_leg["symbol"]:
                continue
            sk = float(short_leg["strike"])
            if opt_type == "put" and sk >= lk:
                continue
            if opt_type == "call" and sk <= lk:
                continue
            lm = self._mid(long_leg)
            sm = self._mid(short_leg)
            if lm is None or sm is None:
                continue
            debit = round(lm - sm, 2)
            if debit <= 0 or debit > max_debit:
                continue
            return long_leg, short_leg, debit
        return None

    # =======================================================================
    # MANAGEMENT — resting TP on fill, 3 PM sweep, 3:50 assignment guard
    # =======================================================================
    async def manage_positions(self) -> List[TradeAction]:
        actions: List[TradeAction] = []
        trades_open = await self.db.trades.open_trades(self.strategy_id)
        trades_closing = await self.db.trades.closing_trades(self.strategy_id)
        if not trades_open and not trades_closing:
            return actions

        t = await self.load_tunables()
        sweep_time = _parse_hhmm(t.ds0_sweep_time, time(15, 1))
        guard_time = _parse_hhmm(t.ds0_guard_time, time(15, 50))
        guard_on = bool(int(t.ds0_assignment_guard))
        guard_band = float(t.ds0_guard_band)
        sweep_min = float(t.ds0_sweep_min)
        close_price = float(t.ds0_close_price)
        cfg_width = float(t.ds0_width)
        now_t = self._now_et().time()

        pos_syms = {p.get("symbol") for p in (await self.broker.get_positions() or [])}
        leg_syms: set = set()
        for tr in list(trades_open) + list(trades_closing):
            if tr.get("short_leg"):
                leg_syms.add(tr["short_leg"])
            if tr.get("long_leg"):
                leg_syms.add(tr["long_leg"])
        raw_quotes = (await self.broker.get_quote(",".join(leg_syms)) or []) if leg_syms else []
        quotes = {q["symbol"]: q for q in raw_quotes if "symbol" in q}
        spots: Dict[str, Optional[float]] = {}

        async def _danger(trade) -> bool:
            """Assignment-guard trigger: spot at/through the near-money strike
            (± ``guard_band``). The long strike is the closer of the pair, so
            danger there covers the whole spread."""
            strike = trade.get("long_strike") or trade.get("short_strike")
            if strike is None:
                return False
            sym = trade["symbol"]
            if sym not in spots:
                spots[sym] = await self._spot(sym)
            spot = spots[sym]
            if spot is None:
                return False
            if trade.get("side_type") == "put":
                return spot <= float(strike) * (1.0 + guard_band)
            return spot >= float(strike) * (1.0 - guard_band)

        for trade in trades_open:
            short_leg, long_leg = trade.get("short_leg"), trade.get("long_leg")
            if not short_leg or not long_leg:
                continue
            if short_leg not in pos_syms or long_leg not in pos_syms:
                continue                     # entry day-limit still resting
            width = float(trade["width"]) if trade.get("width") is not None else cfg_width

            if now_t < sweep_time:
                # Fill is visible and no close is resting (the trade would be
                # CLOSING otherwise) → park the take-profit immediately.
                self._log(
                    f"→ {trade['symbol']} {trade.get('side_type')}: entry filled — "
                    f"placing resting TP close at ${close_price:.2f}"
                )
                actions.append(self._close_spread_action(trade, close_price, "TP"))
                continue

            action = await self._sweep_decision(
                trade, quotes, width, now_t, sweep_time, guard_time,
                guard_on, sweep_min, _danger)
            if action is not None:
                actions.append(action)

        # CLOSING trades carry a resting TP; at/after the sweep the live order
        # must be cancelled before the replacement close goes out — the
        # executor honours ``replace_broker_order_id`` (cancel-or-abort).
        if now_t >= sweep_time and trades_closing:
            broker_orders = await self.broker.get_orders() or []
            for trade in trades_closing:
                short_leg, long_leg = trade.get("short_leg"), trade.get("long_leg")
                if not short_leg or not long_leg:
                    continue
                width = float(trade["width"]) if trade.get("width") is not None else cfg_width
                action = await self._sweep_decision(
                    trade, quotes, width, now_t, sweep_time, guard_time,
                    guard_on, sweep_min, _danger)
                if action is None:
                    continue
                oid = self._find_resting_close_order(broker_orders, short_leg, long_leg)
                if oid is None:
                    self._log(
                        f"ℹ️ {trade['symbol']} {trade.get('side_type')}: no resting "
                        f"close found (may have just filled); skip sweep this pass."
                    )
                    continue
                action.strategy_params["replace_broker_order_id"] = oid
                actions.append(action)
        return actions

    async def _sweep_decision(self, trade, quotes, width, now_t, sweep_time,
                              guard_time, guard_on, sweep_min,
                              danger_fn) -> Optional[TradeAction]:
        """Post-sweep close decision for one trade (OPEN or CLOSING).

        Guard first (assignment risk trumps the mark), then the sweep rule:
        mark at/above ``ds0_sweep_min`` → bank it; below the floor → ride to
        expiration as the accepted loss. No upper bound — at/above the $0.40
        target the resting TP should already have filled, and if it somehow
        hasn't, closing here only banks more than the target.
        """
        short_leg, long_leg = trade["short_leg"], trade["long_leg"]
        mid_credit, exec_credit, blocked, reason = self._spread_close_value(
            quotes.get(long_leg), quotes.get(short_leg), width)

        if guard_on and now_t >= guard_time and await danger_fn(trade):
            price = exec_credit if (not blocked and exec_credit) else 0.01
            self._log(
                f"→ {trade['symbol']} {trade.get('side_type')}: ASSIGN-GUARD — "
                f"spot at/through the strikes; closing at ${price:.2f}"
            )
            return self._close_spread_action(trade, price, "ASSIGN-GUARD")

        if blocked:
            self._log(
                f"⚠️ {trade['symbol']} {trade.get('side_type')}: sweep value "
                f"blocked ({reason}); skip eval this pass."
            )
            return None

        if mid_credit >= sweep_min:
            price = exec_credit if exec_credit and exec_credit > 0 else max(0.01, mid_credit)
            self._log(
                f"→ {trade['symbol']} {trade.get('side_type')}: SWEEP-3PM — mid "
                f"${mid_credit:.2f} ≥ floor ${sweep_min:.2f}; closing at ${price:.2f}"
            )
            return self._close_spread_action(trade, price, "SWEEP-3PM")
        return None

    @staticmethod
    def _find_resting_close_order(orders, short_leg: str, long_leg: str) -> Optional[str]:
        for o in orders or []:
            if str(o.get("status", "")).lower() not in _ACTIVE_ORDER_STATUSES:
                continue
            legs = o.get("leg") or []
            if isinstance(legs, dict):
                legs = [legs]
            leg_syms = {leg.get("option_symbol") for leg in legs}
            sides = [str(leg.get("side") or "").lower() for leg in legs]
            if leg_syms != {short_leg, long_leg}:
                continue
            if not sides or not all("to_close" in s for s in sides):
                continue
            oid = o.get("id")
            if oid is not None:
                return str(oid)
        return None

    def _close_spread_action(self, trade, credit: float, reason: str) -> TradeAction:
        """Sell-to-close the long vertical for ``credit`` (a debit spread's
        close is a credit order — the mirror of the base class' debit close)."""
        price = max(0.01, round(float(credit), 2))
        return TradeAction(
            strategy_id=self.strategy_id, symbol=trade["symbol"],
            order_class="multileg",
            legs=[
                {"option_symbol": trade["long_leg"], "side": "sell_to_close", "quantity": int(trade["lots"])},
                {"option_symbol": trade["short_leg"], "side": "buy_to_close", "quantity": int(trade["lots"])},
            ],
            price=price, side="sell", quantity=1,
            order_type="credit", tag=f"HERMES_{self.NAME}_CLOSE_{reason}",
            strategy_params={"trade_id": trade["id"], "close_reason": reason,
                             "side_type": trade.get("side_type")},
        )

    # ── unused base hooks (we override execute_entries / manage_positions) ────
    def _dte_summary(self, t) -> str:                                  # pragma: no cover
        return "0"

    async def _resolve_entry_expiry(self, symbol: str, t) -> Optional[str]:  # pragma: no cover
        return None

    def _completion_window(self, t) -> Tuple[int, int]:                # pragma: no cover
        return (0, 0)

    def _min_credit(self, dte: int, width: float, t) -> float:         # pragma: no cover
        return 0.0

    def _close_reason(self, trade, dte, debit, entry_credit, width, t) -> Optional[str]:  # pragma: no cover
        return None
