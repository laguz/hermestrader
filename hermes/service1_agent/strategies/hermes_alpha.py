"""HermesAlpha — Hermes's own self-directed strategy (priority 5).

Unlike the four rule-based strategies, HermesAlpha has no fixed entry
recipe. Each tick it asks the overseer (the LLM) to pick ONE credit-spread
setup from a bounded universe — symbol, side, short-leg delta, DTE, width
and size — then resolves that *intent* into real option legs against the
live chain, exactly the way the rule strategies do. The LLM never authors
raw legs or prices; this strategy does, and it clamps every numeric the LLM
returns to a hard safe range.

Position cap
------------
At most ``alpha_max_positions`` (default 10) open spreads at once. The
strategy proposes at most one new entry per tick and stands down once the
cap is reached.

Exits
-----
Primary, discretionary exits run through the overseer's ``propose_closes``
path — Hermes decides when to close each position by reading the live book.
``manage_positions`` here is a *backstop*: a hard stop-loss, a take-profit
floor, and a near-expiry close so a position can never run unbounded if the
LLM stays quiet.

Universe & enablement
---------------------
The universe spans the whole desk — the deduped union of every strategy's
watchlist (plus whatever the engine hands in). Hermes may pick any symbol any
strategy is watching, not just one list. With no watchlists anywhere the
strategy is inert — a safe default even when enabled. It needs an overseer
(LLM) wired; with none it simply stands down.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, List

from ..core import AbstractStrategy, TradeAction


def _clamp(value, lo, hi, *, cast=float, default):
    """Coerce ``value`` to ``cast`` and clamp to ``[lo, hi]``; ``default`` on junk."""
    try:
        v = cast(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


class HermesAlpha(AbstractStrategy):
    PRIORITY = 5
    NAME = "HermesAlpha"

    # Hard safety bounds for the LLM-chosen setup. The model may move these
    # knobs but can never push one outside its range — the same boundary
    # philosophy the overseer's parameter tuner uses.
    DELTA_MIN, DELTA_MAX = 0.05, 0.45
    DTE_MIN, DTE_MAX = 5, 45
    WIDTH_MIN, WIDTH_MAX = 1.0, 10.0

    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        if self.overseer is None:
            return []  # HermesAlpha is LLM-driven; nothing to do without one.

        max_positions = int(self.config.get("alpha_max_positions", 10))
        max_lots = int(self.config.get("alpha_max_lots", 1))
        delta_tol = float(self.config.get("alpha_delta_tol", 0.10))
        min_credit_pct = float(self.config.get("alpha_min_credit_pct", 0.12))

        open_trades = await self.db.open_trades(self.strategy_id)
        if len(open_trades) >= max_positions:
            self._log(f"⏸ position cap reached ({len(open_trades)}/{max_positions}); stand down.")
            return []

        # Universe = every symbol any strategy is watching (the whole desk),
        # unioned with whatever the engine handed us. HermesAlpha is free to
        # pick any of them, not just its own watchlist. Normalised to plain
        # symbols (strip any ':lots' suffix) and deduped.
        raw_syms: List[str] = list(watchlist)
        try:
            raw_syms += await self.db.all_watchlist_symbols()
        except Exception as exc:                                   # noqa: BLE001
            self._log(f"⚠️ all_watchlist_symbols failed ({exc}); engine watchlist only.")
        universe: List[str] = []
        for raw in raw_syms:
            sym = str(raw).split(":", 1)[0].strip().upper()
            if sym:
                universe.append(sym)
        universe = list(dict.fromkeys(universe))
        if not universe:
            self._log("ℹ️ empty universe; stand down.")
            return []

        open_summary = [
            {"symbol": t["symbol"], "side": t.get("side_type"),
             "expiry": str(t.get("expiry"))}
            for t in open_trades
        ]
        intent = await self.overseer.propose_alpha_setup(universe, open_summary)
        if not intent:
            self._log("ℹ️ overseer stood down — no setup this tick.")
            return []

        symbol = str(intent.get("symbol", "")).upper().strip()
        side = str(intent.get("side", "")).lower().strip()
        if side not in ("put", "call") or symbol not in universe:
            self._log(f"✗ invalid intent symbol={symbol!r} side={side!r}; skip.")
            return []

        # Don't stack a duplicate side on a symbol we already hold.
        for t in open_trades:
            if t["symbol"] == symbol and (t.get("side_type") or "").lower() == side:
                self._log(f"ℹ️ {symbol} {side}: already open; skip duplicate.")
                return []

        target_delta = _clamp(intent.get("target_delta"), self.DELTA_MIN, self.DELTA_MAX, default=0.16)
        dte = int(_clamp(intent.get("dte"), self.DTE_MIN, self.DTE_MAX, default=30))
        width = _clamp(intent.get("width"), self.WIDTH_MIN, self.WIDTH_MAX, default=1.0)
        lots = int(_clamp(intent.get("lots"), 1, max_lots, default=1))
        rationale = str(intent.get("rationale") or "HermesAlpha setup")

        self._log(
            f"→ {symbol} {side}: Δ={target_delta:.2f} dte≈{dte} width={width:.0f} "
            f"lots={lots} — {rationale}"
        )

        # Prefer the nearest expiry at/after the chosen DTE; allow a small
        # window so a symbol without exact-DTE listings still trades.
        expiry = await self.find_expiry_in_dte_range(symbol, max(1, dte - 3), dte + 7, prefer="min")
        if not expiry:
            self._log(f"✗ {symbol}: no expiry near {dte} DTE; skip.")
            return []

        chain = await self.broker.get_option_chains(symbol, expiry) or []
        if not chain:
            self._log(f"✗ {symbol}: empty chain for {expiry}; skip.")
            return []

        short_leg = self.find_strike_by_delta(chain, side, target_delta, tolerance=delta_tol)
        if not short_leg:
            self._log(f"✗ {symbol} {side}: no strike near {target_delta:.2f}Δ "
                      f"(±{delta_tol:.2f}); skip.")
            return []

        sl_strike = float(short_leg["strike"])
        long_target = sl_strike - width if side == "put" else sl_strike + width
        long_leg = min(
            (o for o in chain if o.get("option_type") == side and o["symbol"] != short_leg["symbol"]),
            key=lambda o: abs(float(o["strike"]) - long_target),
            default=None,
        )
        if not long_leg:
            self._log(f"✗ {symbol} {side}: no long leg near {long_target:.2f}; skip.")
            return []

        ll_strike = float(long_leg["strike"])
        # Direction sanity — the long leg must be further OTM than the short.
        if (side == "put" and ll_strike >= sl_strike) or (side == "call" and ll_strike <= sl_strike):
            self._log(f"✗ {symbol} {side}: long {ll_strike} not OTM of short {sl_strike}; skip.")
            return []

        credit = self.short_credit(short_leg, long_leg)
        actual_width = abs(sl_strike - ll_strike)
        min_credit = round(actual_width * min_credit_pct, 2)
        if credit < min_credit:
            self._log(
                f"✗ {symbol} {side}: credit ${credit:.2f} < min ${min_credit:.2f} "
                f"(short={sl_strike:.2f} long={ll_strike:.2f}); skip."
            )
            return []

        self._log(
            f"✓ {symbol} {side}: short={sl_strike:.2f} long={ll_strike:.2f} "
            f"credit=${credit:.2f} expiry={expiry}"
        )
        return [TradeAction(
            strategy_id=self.strategy_id, symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": lots},
            ],
            price=credit, side="sell", quantity=1, order_type="credit",
            tag="HERMES_HermesAlpha",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side},
            expiry=expiry, width=actual_width,
            ai_authored=True, ai_rationale=rationale,
        )]

    async def manage_positions(self) -> List[TradeAction]:
        """Bounded backstop — Hermes's primary exits run via ``propose_closes``.

        SL @ debit ≥ ``alpha_sl_mult``× entry credit; TP @ debit ≤
        ``alpha_tp_pct_width`` of width; and a near-expiry close at
        ``alpha_close_dte`` to sidestep pin/assignment risk. Matches the rule
        strategies: a blocked (stale/phantom) close-debit quote skips the
        position this tick rather than firing a panic-priced close.
        """
        actions: List[TradeAction] = []
        trades = await self.db.open_trades(self.strategy_id)
        if not trades:
            return []

        sl_mult = float(self.config.get("alpha_sl_mult", 3.0))
        tp_pct_width = float(self.config.get("alpha_tp_pct_width", 0.10))
        close_dte = int(self.config.get("alpha_close_dte", 1))
        today = self.today()

        for trade in trades:
            short = trade.get("short_leg")
            long_ = trade.get("long_leg")
            if not short or not long_:
                continue
            entry_credit = float(trade.get("entry_credit") or 0)
            width = float(trade["width"]) if trade.get("width") is not None else None

            quotes = await self.broker.get_quote(f"{short},{long_}") or []
            sq = next((q for q in quotes if q.get("symbol") == short), None)
            lq = next((q for q in quotes if q.get("symbol") == long_), None)
            debit, blocked, reason = self.compute_close_debit(sq, lq, width)
            if blocked:
                self._log(f"⚠️ {trade['symbol']}: close-debit blocked ({reason}); "
                          f"skip eval this tick.")
                continue

            exp = trade.get("expiry")
            dte = None
            if exp:
                try:
                    d = exp if hasattr(exp, "isoformat") else \
                        datetime.strptime(str(exp), "%Y-%m-%d").date()
                    dte = (d - today).days
                except Exception:                                  # noqa: BLE001
                    dte = None

            close_reason = None
            if dte is not None and dte <= close_dte:
                close_reason = f"EXPIRY-{dte}DTE"
            elif entry_credit > 0 and debit >= entry_credit * sl_mult:
                close_reason = "SL"
            elif width and debit <= width * tp_pct_width:
                close_reason = "TP"
            if close_reason is None:
                continue

            self._log(f"→ {trade['symbol']}: backstop close ({close_reason}) "
                      f"debit=${debit:.2f} entry=${entry_credit:.2f}")
            actions.append(TradeAction(
                strategy_id=self.strategy_id, symbol=trade["symbol"], order_class="multileg",
                legs=[
                    {"option_symbol": short, "side": "buy_to_close",  "quantity": int(trade["lots"])},
                    {"option_symbol": long_, "side": "sell_to_close", "quantity": int(trade["lots"])},
                ],
                price=round(debit * 1.05, 2), side="buy", quantity=1,
                order_type="debit", tag=f"HERMES_HermesAlpha_CLOSE_{close_reason}",
                strategy_params={"trade_id": trade["id"], "close_reason": close_reason,
                                 "side_type": trade.get("side_type")},
                expiry=str(exp) if exp else None, width=width,
            ))
        return actions
