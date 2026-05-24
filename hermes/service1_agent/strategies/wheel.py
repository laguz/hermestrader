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

Both legs target ``|Δ| ≈ 0.30`` (±0.05) on the latest expiry within
30–45 DTE. Single-leg orders (``order_class='option'``); pricing is the
mid of bid/ask (defensive against ``None`` from illiquid contracts).

Management contract
-------------------
Roll any short ITM at <7 DTE to the next available month, same strike
and side. Rolls bypass ``max_lots`` because they're not new exposure.
"""
from __future__ import annotations

from typing import Iterable, List, Optional

from ..core import AbstractStrategy, TradeAction

from ._helpers import parse_occ


class WheelStrategy(AbstractStrategy):
    PRIORITY = 4
    NAME = "WHEEL"

    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        max_lots = int(self.config.get("wheel_max_lots", 5))
        symbols = list(watchlist)
        self._log(f"↻ scanning {len(symbols)} symbol(s) — max_lots={max_lots} delta=0.30")

        for symbol in symbols:
            shares = int(self.db.equity_position(symbol) or 0)
            shares_lots = shares // 100

            # Pick the target expiry first so capacity is checked per
            # option chain (max_lots is per-expiry, not symbol-wide).
            expiry = await self.find_expiry_in_dte_range(symbol, 30, 45, prefer="max")
            if not expiry:
                self._log(f"✗ {symbol}: no expiry in 30-45 DTE range; skip.")
                continue

            # side_aware_capacity already subtracts open + pending + broker
            # orders for this chain, so capacity here is the actual headroom
            # remaining on the (symbol, side, expiry) bucket.
            call_capacity = self.mm.side_aware_capacity(
                self.strategy_id, symbol, "call", max_lots, expiry=expiry)
            put_capacity = self.mm.side_aware_capacity(
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
                a = await self._open_wheel_leg(symbol, "call", expiry)
                if a:
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
                a = await self._open_wheel_leg(symbol, "put", expiry)
                if a:
                    actions.append(a)
        return actions

    async def _open_wheel_leg(self, symbol: str, side: str, expiry: str) -> Optional[TradeAction]:
        chain = await self.broker.get_option_chains(symbol, expiry) or []
        short = self.find_strike_by_delta(chain, side, 0.30, tolerance=0.05)
        if not short:
            self._log(f"✗ {symbol} {side}: no strike near 0.30Δ (±0.05) in chain for {expiry}; skip.")
            return None
        # Defensive: illiquid contracts can return None for bid/ask.
        bid = float(short.get("bid") or 0.0)
        ask = float(short.get("ask") or 0.0)
        if bid <= 0 and ask <= 0:
            self._log(f"✗ {symbol} {side}: no bid/ask on {short.get('symbol')}; skip.")
            return None
        mid = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else round(max(bid, ask), 2)
        return TradeAction(
            strategy_id=self.strategy_id, symbol=symbol, order_class="option",
            legs=[{"option_symbol": short["symbol"], "side": "sell_to_open", "quantity": 1}],
            price=mid,
            side="sell", quantity=1, order_type="credit", tag="HERMES_WHEEL",
            strategy_params={"side_type": side, "short_leg": short["symbol"]},
            expiry=expiry,
        )

    async def manage_positions(self) -> List[TradeAction]:
        """Roll ITM at <7 DTE; detect put assignments and open covered calls."""
        actions: List[TradeAction] = []

        # Fetch live broker positions once for assignment detection.
        try:
            live_positions = await self.broker.get_positions() or []
        except Exception as exc:                                   # noqa: BLE001
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

        for trade in self.db.open_trades(self.strategy_id):
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
                    expiry = await self.find_expiry_in_dte_range(symbol, 30, 45, prefer="max")
                    if expiry:
                        call_action = await self._open_wheel_leg(symbol, "call", expiry)
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
            if dte < 7 and itm:
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
