"""WheelStrategy — priority-4 strategy (CSP → assignment → covered call).

Entry contract
--------------
For each watchlist symbol, balance lot allocation across covered calls
and cash-secured puts up to ``wheel_max_lots``:

1. **Calls first** (cover existing equity). Open as many ``sell_to_open``
   short calls as the share count supports — one contract covers 100
   shares — bounded by remaining capacity.
2. **Puts second** (deploy unused capacity). Fill remaining capacity
   toward ``max_lots`` total with cash-secured puts.

Both legs target ``|Δ| ≈ 0.30`` (±``wheel_delta_tol``, default 0.05) on the
latest expiry within 30–45 DTE. Single-leg orders (``order_class='option'``);
pricing is the mid of bid/ask (defensive against ``None`` from illiquid
contracts).

POP overlay (hybrid)
--------------------
Delta stays the anchor — the wheel monetizes assignment, so it needs the
premium that ~0.30Δ provides, and pushing to the ≥0.75-POP strikes the
credit-spread strategies use would gut the credit and conflict with the
0.30Δ target. Instead we use the 6M POP surface (same engine as CS75) two
ways, both no-ops unless ``analyze_symbol`` returns data:

* **Tilt** — among the in-band candidate strikes, prefer the one with the
  highest S/R *protection score* (a short strike sitting just behind a
  support/resistance cluster), tie-broken by closeness to the target delta.
  We tilt on protection, not raw POP, because POP rises monotonically as
  delta falls — tilting on POP alone would just walk to the low-delta,
  low-premium edge of the band.
* **Gate** — skip the entry when the chosen strike's 6M POP falls below
  ``wheel_min_pop`` (default 0.50), i.e. the directional/vol regime is
  adverse enough that even the wheel shouldn't sell into it. Set 0 to
  disable.

When ``analyze_symbol`` is unavailable the selection degrades to the
original delta-only pick.

Management contract
-------------------
Roll any short ITM at <7 DTE to the next available month, same strike
and side. Rolls bypass ``max_lots`` because they're not new exposure.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional

from ..core import AbstractStrategy, TradeAction
from hermes.ml.pop_engine import (
    FeatureVector,
    calculate_strike_protection,
    coerce_xgb_prob,
    predict_pop,
)

from ._helpers import parse_occ


class WheelStrategy(AbstractStrategy):
    PRIORITY = 4
    NAME = "WHEEL"

    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        t = await self.load_tunables()
        max_lots = int(self.config.get("wheel_max_lots", 5))
        symbols = list(watchlist)
        self._log(f"↻ scanning {len(symbols)} symbol(s) — max_lots={max_lots} delta={t.wheel_delta:.2f}")

        for symbol in symbols:
            shares = int(await self.db.trades.equity_position(symbol) or 0)
            shares_lots = shares // 100

            # Pick the target expiry first so capacity is checked per
            # option chain (max_lots is per-expiry, not symbol-wide).
            expiry = await self.find_expiry_in_dte_range(symbol, t.wheel_min_dte, t.wheel_max_dte, prefer="max")
            if not expiry:
                self._log(f"✗ {symbol}: no expiry in {t.wheel_min_dte}-{t.wheel_max_dte} DTE range; skip.")
                continue

            # POP overlay inputs — fetched once per symbol (6M regime matches
            # the wheel's 30-45 DTE). Both stay None/neutral when analysis is
            # unavailable, in which case strike selection is delta-only.
            analysis, xgb_prob = await self._pop_inputs(symbol)

            # side_aware_capacity already subtracts open + pending + broker
            # orders for this chain, so capacity here is the actual headroom
            # remaining on the (symbol, side, expiry) bucket.
            call_capacity = await self.mm.side_aware_capacity(
                self.strategy_id, symbol, "call", max_lots, expiry=expiry)
            put_capacity = await self.mm.side_aware_capacity(
                self.strategy_id, symbol, "put", max_lots, expiry=expiry)

            # Derive committed = open + pending (what side_aware_capacity already subtracted).
            calls_committed = max_lots - call_capacity
            puts_committed = max_lots - put_capacity

            self._log(
                f"→ {symbol} exp={expiry}: shares={shares} ({shares_lots} lots) "
                f"calls_committed={calls_committed} puts_committed={puts_committed} "
                f"call_cap={call_capacity} put_cap={put_capacity}"
            )

            # ── Calls: cover existing shares first, bounded by share count + max_lots ──
            share_call_budget = max(0, min(shares_lots, max_lots) - calls_committed)
            wanted_calls = min(call_capacity, share_call_budget)
            if wanted_calls == 0 and call_capacity > 0:
                self._log(f"ℹ️ {symbol} CALL: no shares to cover (shares_lots={shares_lots}); skip calls.")
            elif wanted_calls == 0 and call_capacity == 0:
                self._log(f"ℹ️ {symbol} CALL: at capacity exp={expiry} ({calls_committed}/{max_lots}); skip calls.")

            added_calls = 0
            for _ in range(wanted_calls):
                a = await self._open_wheel_leg(symbol, "call", expiry,
                                               analysis=analysis, xgb_prob=xgb_prob, t=t)
                if not a:
                    # Nothing about the chain, BP, or strike selection changes
                    # between iterations within this tick — a miss here (no
                    # valid strike, no bid/ask, or insufficient BP) will miss
                    # identically on every remaining lot. Retrying just re-fetches
                    # the same option chain from the broker for no gain, and
                    # under strategy/symbol fan-out this real per-lot broker
                    # round-trip cost was enough to blow the reactive pipeline's
                    # processing budget.
                    break
                actions.append(a)
                added_calls += 1

            # ── Puts: fill remaining capacity toward max_lots total ──
            total_calls = calls_committed + added_calls
            puts_budget = max(0, max_lots - total_calls - puts_committed)
            wanted_puts = min(put_capacity, puts_budget)
            if wanted_puts == 0:
                self._log(
                    f"ℹ️ {symbol} PUT: at capacity or budget exhausted exp={expiry} "
                    f"(puts_committed={puts_committed} total_calls={total_calls} max={max_lots}); skip puts."
                )

            for _ in range(wanted_puts):
                a = await self._open_wheel_leg(symbol, "put", expiry,
                                               analysis=analysis, xgb_prob=xgb_prob, t=t)
                if not a:
                    # See the matching comment in the calls loop above.
                    break
                actions.append(a)
        return actions

    async def _pop_inputs(self, symbol: str):
        """Return ``(analysis, xgb_prob)`` for the POP overlay, or ``(None, None)``.

        ``analysis`` is the 6M ``analyze_symbol`` blob (current price/vol +
        S/R key levels); ``xgb_prob`` is the calibrated directional
        probability coerced from the latest stored prediction. Any failure
        degrades gracefully to delta-only selection.
        """
        try:
            analysis = await self.broker.analyze_symbol(symbol, period="6m")
        except Exception as exc:
            self._log(f"⚠️ {symbol}: 6M analysis failed ({exc}); delta-only strike selection.")
            return None, None
        if not analysis or "error" in analysis:
            self._log(f"ℹ️ {symbol}: no 6M analysis; delta-only strike selection.")
            return None, None
        xgb_pred = await self.db.decisions.latest_prediction(symbol) or {}
        current_vol = float(analysis.get("current_vol") if analysis.get("current_vol") is not None else 0.30)
        return analysis, coerce_xgb_prob(xgb_pred, current_vol)

    async def _open_wheel_leg(self, symbol: str, side: str, expiry: str,
                              *, analysis=None, xgb_prob=None, t=None) -> Optional[TradeAction]:
        if t is None:
            t = await self.load_tunables()
        chain = await self.broker.get_option_chains(symbol, expiry) or []
        try:
            dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
        except (TypeError, ValueError):
            dte = None
        short, pop = self._select_short_strike(symbol, side, chain, analysis, xgb_prob, t, dte=dte)
        if not short:
            return None
        # Defensive: illiquid contracts can return None for bid/ask.
        bid = float(short.get("bid") or 0.0)
        ask = float(short.get("ask") or 0.0)
        if bid <= 0 and ask <= 0:
            self._log(f"✗ {symbol} {side}: no bid/ask on {short.get('symbol')}; skip.")
            return None
        mid = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else round(max(bid, ask), 2)

        # Scale quantity and check buying power
        strike = float(short.get("strike") or 0.0)
        requirement_per_lot = strike * 100.0 if side == "put" else 0.0
        max_lots = int(self.config.get("wheel_max_lots", 5))
        lots = await self.mm.scale_quantity(
            requested_lots=1,
            requirement_per_lot=requirement_per_lot,
            symbol=symbol,
            side=side,
            strategy_id=self.strategy_id,
            max_lots=max_lots,
            expiry=expiry,
        )
        if lots < 1:
            return None

        params = {"side_type": side, "short_leg": short["symbol"]}
        if pop is not None:
            params["pop"] = round(pop, 4)
        return TradeAction(
            strategy_id=self.strategy_id, symbol=symbol, order_class="option",
            legs=[{"option_symbol": short["symbol"], "side": "sell_to_open", "quantity": 1}],
            price=mid,
            side="sell", quantity=1, order_type="credit", tag="HERMES_WHEEL",
            strategy_params=params,
            expiry=expiry,
        )

    def _select_short_strike(self, symbol, side, chain, analysis, xgb_prob, t, *, dte=None):
        """Pick the short strike for one wheel leg. Returns ``(option, pop)``.

        Delta is the anchor: gather chain strikes within ``wheel_delta_tol``
        of ``wheel_delta``. With no analysis we keep the original behaviour —
        the single strike nearest the target delta, ``pop=None``. With
        analysis we tilt toward the most S/R-protected in-band strike and
        gate on 6M POP (see the module docstring). ``t`` is the resolved
        :class:`Tunables` for this strategy.
        """
        target = t.wheel_delta
        tol = t.wheel_delta_tol

        candidates = []
        for o in chain:
            if o.get("option_type") != side:
                continue
            greeks = o.get("greeks") or {}
            raw = greeks.get("delta")
            if raw is None:
                continue
            d = abs(float(raw))
            if abs(d - target) <= tol:
                candidates.append((o, d))

        if not candidates:
            self._log(f"✗ {symbol} {side}: no strike near {target:.2f}Δ (±{tol:.2f}) in chain; skip.")
            return None, None

        # Delta-only fallback — preserves legacy behaviour when the POP
        # surface is unavailable.
        if analysis is None:
            opt, _ = min(candidates, key=lambda c: abs(c[1] - target))
            return opt, None

        current_price = float(analysis.get("current_price") if analysis.get("current_price") is not None else 0.0)
        current_vol = float(analysis.get("current_vol") if analysis.get("current_vol") is not None else 0.30)
        avg_vol = float(analysis.get("avg_vol") if analysis.get("avg_vol") is not None else 0.25)
        key_levels = analysis.get("key_levels") or []
        spread_type = "put_credit" if side == "put" else "call_credit"
        prob = 0.5 if xgb_prob is None else float(xgb_prob)

        # Tilt on protection score; tie-break toward the target delta.
        best_opt = best_delta = best_prot = None
        for o, d in candidates:
            prot = calculate_strike_protection(
                key_levels, current_price, float(o.get("strike") or 0.0), spread_type)
            if (best_opt is None
                    or prot > best_prot + 1e-9
                    or (abs(prot - best_prot) <= 1e-9 and abs(d - target) < abs(best_delta - target))):
                best_opt, best_delta, best_prot = o, d, prot

        best_greeks = best_opt.get("greeks") or {}
        iv = best_greeks.get("mid_iv")
        if iv is None:
            iv = best_greeks.get("smv_vol")
        pop = predict_pop(FeatureVector(
            delta=best_delta, xgb_prob=prob, current_vol=current_vol,
            avg_vol=avg_vol, protection_score=best_prot, side=side,
            period="6M", symbol=symbol,
            dte=float(dte) if dte is not None else None,
            sigma=float(iv) if iv is not None else None,
        ))

        min_pop = t.wheel_min_pop
        if min_pop > 0 and pop < min_pop:
            self._log(
                f"✗ {symbol} {side}: 6M POP {pop:.1%} < floor {min_pop:.0%} "
                f"(adverse regime, strike={best_opt.get('strike')}); skip."
            )
            return None, pop
        self._log(
            f"→ {symbol} {side}: strike={best_opt.get('strike')} Δ={best_delta:.2f} "
            f"POP={pop:.1%} prot={best_prot:.2f}"
        )
        return best_opt, pop

    async def manage_positions(self) -> List[TradeAction]:
        """Roll ITM at <7 DTE; detect put assignments and open covered calls."""
        actions: List[TradeAction] = []
        t = await self.load_tunables()

        # Fetch live broker positions once for assignment detection.
        try:
            live_positions = await self.broker.get_positions() or []
        except Exception as exc:
            self._log(f"⚠️ could not fetch positions for assignment check: {exc}")
            live_positions = []

        live_option_symbols = {
            str(p.get("symbol", "")).upper()
            for p in live_positions
            if p.get("asset_type", "").lower() == "option" or "option_symbol" in p
        }
        live_equity_lots: dict = {}
        for p in live_positions:
            sym = str(p.get("symbol", "")).upper()
            asset = str(p.get("asset_type") or p.get("type") or "").lower()
            if asset in ("stock", "equity", "") and not any(c.isdigit() for c in sym[-8:]):
                qty = int(float(p.get("quantity") or p.get("qty") or 0))
                live_equity_lots[sym] = live_equity_lots.get(sym, 0) + qty

        for trade in await self.db.trades.open_trades(self.strategy_id):
            info = parse_occ(trade["short_leg"])
            if not info:
                continue

            dte = (info["expiry"] - self.today()).days

            # ── Assignment detection (puts only) ──────────────────────────
            # If the short put is no longer at the broker and equity appeared,
            # the option was assigned.  We open a covered call to continue
            # the wheel cycle and let reconcile_orphans handle the put row.
            if info["side"] == "put" and trade["short_leg"] not in live_option_symbols:
                symbol = trade["symbol"].upper()
                equity_qty = live_equity_lots.get(symbol, 0)
                call_lots = equity_qty // 100
                if call_lots > 0:
                    self._log(
                        f"✓ {symbol}: put {trade['short_leg']} assigned — "
                        f"{equity_qty} shares detected; opening {call_lots} covered call(s)"
                    )
                    expiry = await self.find_expiry_in_dte_range(symbol, t.wheel_min_dte, t.wheel_max_dte, prefer="max")
                    if expiry:
                        call_action = await self._open_wheel_leg(symbol, "call", expiry, t=t)
                        if call_action:
                            call_action.quantity = call_lots
                            call_action.strategy_params = {
                                **(call_action.strategy_params or {}),
                                "triggered_by": "assignment",
                                "assigned_put": trade["short_leg"],
                            }
                            actions.append(call_action)
                    else:
                        self._log(f"⚠️ {symbol}: no 30-45 DTE expiry for post-assignment call; skip.")
                continue  # Skip ITM-roll check; put is gone

            # ── ITM roll at <7 DTE ────────────────────────────────────────
            quotes = await self.broker.get_quote(trade["symbol"]) or []
            quote = quotes[0] if quotes else {}
            spot = float(quote.get("last") or 0)
            short_strike = float(trade.get("short_strike") or 0)
            itm = ((info["side"] == "put" and spot < short_strike) or
                   (info["side"] == "call" and spot > short_strike))
            if dte < t.wheel_roll_dte and itm:
                actions.append(TradeAction(
                    strategy_id=self.strategy_id, symbol=trade["symbol"],
                    order_class="multileg",
                    legs=[
                        {"option_symbol": trade["short_leg"], "side": "buy_to_close",
                         "quantity": int(trade["lots"])},
                        {"option_symbol": await self.broker.roll_to_next_month(trade["short_leg"]),
                         "side": "sell_to_open", "quantity": int(trade["lots"])},
                    ],
                    price=None, side="buy", quantity=1, order_type="market",
                    tag="HERMES_WHEEL_ROLL",
                    strategy_params={"trade_id": trade["id"], "ignore_max_lots": True},
                ))
        return actions
