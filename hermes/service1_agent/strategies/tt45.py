"""TastyTrade45 — priority-3 strategy (delta-driven verticals).

Entry contract
--------------
- Pure delta selection (no key-level / POP screening): pick the short
  strike whose ``|Δ|`` is closest to 0.16 (±0.05 tolerance).
- Prefer to complete an existing incomplete IC; otherwise open both sides
  on the latest expiry within the 30–60 DTE window.
- Long leg snapped to the nearest chain strike at ``short_strike ± width``
  (no exact-match requirement).

Width is configurable (``tt45_width``, default 5).

Management contract
-------------------
- Hard exit at 21 DTE remaining (TastyTrade gamma-risk threshold).
- Neutralise the challenged side when the short's |Δ| > 0.30 (the trade
  has moved too far against us; defend by closing rather than rolling).
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List

from ..core import AbstractStrategy, TradeAction

from ._helpers import parse_occ


class TastyTrade45(AbstractStrategy):
    PRIORITY = 3
    NAME = "TT45"

    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        t = await self.load_tunables()
        width = t.tt45_width
        max_lots = int(self.config.get("tt45_max_lots", 5))
        target_lots = int(self.config.get("tt45_target_lots", 5))

        # DTE window and delta target — live-tunable via system_settings.
        entry_min_dte = t.tt45_min_dte
        entry_max_dte = t.tt45_max_dte
        entry_delta = t.tt45_delta
        delta_tol = t.tt45_delta_tol

        symbols = list(watchlist)
        self._log(
            f"↻ scanning {len(symbols)} symbol(s) — target={target_lots} max={max_lots} "
            f"width={width} delta={entry_delta} dte={entry_min_dte}-{entry_max_dte}"
        )

        for symbol in symbols:
            try:
                # Always prefer to complete an existing IC over opening a new one.
                expiry = await self.find_active_ic_expiry(symbol)
                if not expiry:
                    expiry = await self.find_expiry_in_dte_range(symbol, entry_min_dte, entry_max_dte, prefer="max")

                if not expiry:
                    self._log(f"ℹ️ {symbol}: no expiry in {entry_min_dte}-{entry_max_dte} DTE range; skip.")
                    continue
                existing = {leg["side"] for leg in await self.db.open_legs(self.strategy_id, symbol)
                            if leg.get("expiry") == expiry}
                dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                self._log(f"→ {symbol}: expiry={expiry} {dte}DTE existing_sides={sorted(existing)}")

                def factory(side: str):
                    async def _b(symbol, expiry, lots, width):
                        chain = await self.broker.get_option_chains(symbol, expiry) or []
                        short_leg = await self.find_strike_by_delta(chain, side, entry_delta, tolerance=delta_tol)
                        if not short_leg:
                            self._log(f"✗ {symbol} {side}: no strike near {entry_delta:.2f}Δ (±{delta_tol:.2f}) in chain; skip.")
                            return None
                        long_strike = (float(short_leg["strike"]) - width
                                       if side == "put" else float(short_leg["strike"]) + width)
                        # Snap to the nearest chain strike rather than requiring
                        # an exact-width match.
                        long_leg = min(
                            (o for o in chain if o.get("option_type") == side
                             and o["symbol"] != short_leg["symbol"]),
                            key=lambda o: abs(float(o["strike"]) - long_strike),
                            default=None,
                        )
                        if not long_leg:
                            self._log(f"✗ {symbol} {side}: no long leg near strike {long_strike:.2f}; skip.")
                            return None
                        # Direction sanity — long must be further OTM than short.
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
                        greeks = short_leg.get("greeks") or {}
                        delta_val = abs(float(greeks.get("delta") or entry_delta))
                        pop_val = 1.0 - delta_val

                        return TradeAction(
                            strategy_id=self.strategy_id, symbol=symbol, order_class="multileg",
                            legs=[
                                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
                            ],
                            price=credit, side="sell", quantity=1, order_type="credit",
                            tag="HERMES_TT45",
                            strategy_params={"short_leg": short_leg["symbol"],
                                             "long_leg": long_leg["symbol"], "side_type": side,
                                             "pop": pop_val, "short_delta": delta_val},
                            expiry=expiry, width=width,
                        )
                    return _b

                planned = await self.ic.plan(
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

    async def manage_positions(self) -> List[TradeAction]:
        """Hard exit at 21 DTE; neutralise challenged side (|Δ_short| > 0.30)."""
        actions: List[TradeAction] = []
        trades = await self.db.open_trades(self.strategy_id)
        if not trades:
            return []
        t = await self.load_tunables()

        # Optimization: batch fetch quotes for all short legs to avoid N+1 API calls
        # in the loop. get_delta(sym) internally calls get_quote(sym); we can
        # pull them all once.
        short_legs = list({t["short_leg"] for t in trades if t.get("short_leg")})
        deltas = {}
        if short_legs:
            quotes = await self.broker.get_quote(",".join(short_legs))
            for q in quotes:
                symbol = q.get("symbol")
                greeks = q.get("greeks") or {}
                delta = float(greeks.get("delta", 0.0) or 0.0)
                if symbol:
                    deltas[symbol] = delta

        for trade in trades:
            info = parse_occ(trade["short_leg"])
            if not info:
                continue
            dte = (info["expiry"] - self.today()).days
            short_delta = abs(deltas.get(trade["short_leg"], 0.0))

            close_reason = None
            if dte <= t.tt45_hard_exit_dte:
                close_reason = "HARD-21DTE"
            elif short_delta > t.tt45_challenged_delta:
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
