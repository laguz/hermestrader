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
# Priority 1 — CS75 (39-45 DTE entry; 25% / 20% credit-to-width by DTE band)
# ---------------------------------------------------------------------------
class CreditSpreads75(AbstractStrategy):
    PRIORITY = 1
    NAME = "CS75"

    def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        max_lots = int(self.config.get("cs75_max_lots", 10))
        target_lots = int(self.config.get("cs75_target_lots", 10))
        width = float(self.config.get("cs75_width", 5.0))

        for symbol in watchlist:
            try:
                analysis = self.broker.analyze_symbol(symbol, period="6m")
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ analysis missing for {symbol}; skip.")
                    continue
                price = analysis["current_price"]

                # Inspect existing CS75 sides for this symbol
                open_legs = self.db.open_legs(self.strategy_id, symbol)
                sides_by_expiry: Dict[str, set] = {}
                for leg in open_legs:
                    info = _parse_occ(leg["option_symbol"])
                    if not info:
                        continue
                    sides_by_expiry.setdefault(info["expiry"].isoformat(), set()).add(info["side"])

                # Mode A vs Mode B
                mode_a = not sides_by_expiry
                if mode_a:
                    expiry = self.find_expiry_in_dte_range(symbol, 39, 45, prefer="max")
                    existing_sides: set = set()
                else:
                    # Pick the latest existing expiry
                    expiry, existing_sides = sorted(sides_by_expiry.items())[-1]
                    dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                    if dte < 14 or dte > 45:
                        self._log(f"ℹ️ {symbol}: existing expiry {expiry} ({dte}DTE) outside completion window; skip.")
                        continue

                if not expiry:
                    self._log(f"ℹ️ {symbol}: no expiry in 39-45 DTE; skip.")
                    continue

                # Required credit: 25% width for 30-45 DTE; 20% for 14-29 DTE
                dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
                min_credit_pct = 0.25 if 30 <= dte <= 45 else 0.20
                min_credit = round(width * min_credit_pct, 2)

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
        # Pick a short strike with POP > 75% (S/R based) per the spec
        ep_key = "put_entry_points" if side == "put" else "call_entry_points"
        candidates = [ep for ep in analysis.get(ep_key, []) if ep.get("pop", 0) > 75]
        candidates = [
            ep for ep in candidates
            if (side == "put" and ep["price"] < current_price)
            or (side == "call" and ep["price"] > current_price)
        ]
        if not candidates:
            self._log(f"{symbol} {side}: no >75% POP S/R level; skip.")
            return None
        target = min(candidates, key=lambda ep: abs(ep["pop"] - 75))
        short_strike = target["price"]

        chain = self.broker.get_option_chains(symbol, expiry) or []
        opt_type = side
        short_leg = next(
            (o for o in chain if o["option_type"] == opt_type and abs(o["strike"] - short_strike) < 0.01),
            None,
        )
        long_strike = short_strike - width if side == "put" else short_strike + width
        long_leg = next(
            (o for o in chain if o["option_type"] == opt_type and abs(o["strike"] - long_strike) < 0.01),
            None,
        )
        if not (short_leg and long_leg):
            self._log(f"{symbol} {side}: missing legs at {short_strike}/{long_strike}; skip.")
            return None

        credit = self.short_credit(short_leg, long_leg)
        if credit < min_credit:
            self._log(f"{symbol} {side}: credit {credit} < min {min_credit}; skip.")
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
        width = float(self.config.get("cs7_width", 5.0))
        max_lots = int(self.config.get("cs7_max_lots", 10))
        target_lots = int(self.config.get("cs7_target_lots", 10))
        min_credit = round(width * 0.12, 2)

        for symbol in watchlist:
            expiry = self.find_expiry_in_dte_range(symbol, 5, 8, prefer="max")
            if not expiry:
                self._log(f"{symbol}: no 7-DTE expiry; skip.")
                continue
            existing = {leg["side"] for leg in self.db.open_legs(self.strategy_id, symbol)
                        if leg.get("expiry") == expiry}

            def factory(side: str):
                def _b(symbol, expiry, lots, width):
                    return self._build_short_premium_spread(
                        symbol=symbol, expiry=expiry, side=side, lots=lots,
                        width=width, min_credit=min_credit, target_delta=0.10,
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
        return actions

    def _build_short_premium_spread(self, *, symbol, expiry, side, lots,
                                    width, min_credit, target_delta) -> Optional[TradeAction]:
        chain = self.broker.get_option_chains(symbol, expiry) or []
        short_leg = self.find_strike_by_delta(chain, side, target_delta, tolerance=0.07)
        if not short_leg:
            return None
        long_strike = short_leg["strike"] - width if side == "put" else short_leg["strike"] + width
        long_leg = next(
            (o for o in chain if o["option_type"] == side and abs(o["strike"] - long_strike) < 0.01),
            None,
        )
        if not long_leg:
            return None
        credit = self.short_credit(short_leg, long_leg)
        if credit < min_credit:
            return None
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
        for symbol in watchlist:
            expiry = self.find_expiry_in_dte_range(symbol, 30, 60, prefer="max")
            if not expiry:
                continue
            existing = {leg["side"] for leg in self.db.open_legs(self.strategy_id, symbol)
                        if leg.get("expiry") == expiry}

            def factory(side: str):
                def _b(symbol, expiry, lots, width):
                    chain = self.broker.get_option_chains(symbol, expiry) or []
                    short_leg = self.find_strike_by_delta(chain, side, 0.16, tolerance=0.05)
                    if not short_leg:
                        return None
                    long_strike = (short_leg["strike"] - width
                                   if side == "put" else short_leg["strike"] + width)
                    long_leg = next(
                        (o for o in chain
                         if o["option_type"] == side and abs(o["strike"] - long_strike) < 0.01),
                        None,
                    )
                    if not long_leg:
                        return None
                    credit = self.short_credit(short_leg, long_leg)
                    if credit <= 0:
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
        for symbol in watchlist:
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

            # ── Calls: cover shares first, bounded by share count and max_lots ──
            # We can add at most call_capacity more, further capped by the
            # share-coverage constraint: total calls ≤ min(shares_lots, max_lots).
            share_call_budget = max(0, min(shares_lots, max_lots) - calls_committed)
            wanted_calls = min(call_capacity, share_call_budget)

            added_calls = 0
            for _ in range(wanted_calls):
                a = self._open_wheel_leg(symbol, "call")
                if a:
                    actions.append(a)
                    added_calls += 1

            # ── Puts: fill remaining capacity toward max_lots total ──
            # Total calls after this tick (committed + newly queued).
            total_calls = calls_committed + added_calls
            # Total puts must not push (calls + puts) beyond max_lots.
            puts_budget = max(0, max_lots - total_calls - puts_committed)
            wanted_puts = min(put_capacity, puts_budget)

            for _ in range(wanted_puts):
                a = self._open_wheel_leg(symbol, "put")
                if a:
                    actions.append(a)
        return actions

    def _open_wheel_leg(self, symbol, side) -> Optional[TradeAction]:
        expiry = self.find_expiry_in_dte_range(symbol, 30, 45, prefer="max")
        if not expiry:
            return None
        chain = self.broker.get_option_chains(symbol, expiry) or []
        short = self.find_strike_by_delta(chain, side, 0.30, tolerance=0.05)
        if not short:
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
