"""HermesAlpha — priority-5, LLM-originated credit spreads.

The rule-free strategy. Unlike CS75/CS7 (which walk POP-augmented S/R levels),
HermesAlpha asks the **overseer** to originate an intent — *which* symbol, side,
delta band and expiry window — then resolves and prices that intent against the
live chain **deterministically** (the LLM names a structure; it never invents a
price). Exits are likewise LLM-chosen but the close debit is taken from the live
broker quote via :meth:`compute_close_debit`, so a hallucinated price can never
be sent.

Hard gates (all preserved from the cascading pipeline):

- **Autonomous-only origination.** ``execute_entries`` is a no-op unless the
  operator's autonomy is ``autonomous``; flipping back to enforcing/advisory
  disables origination instantly.
- Originated actions still flow through ``PortfolioRiskEngine`` (capacity /
  safety gateway / lot scaling) exactly like every other strategy's entries.
- The no-human-in-the-loop live path is gated separately in
  ``_engine_pipeline._execute_or_queue`` behind the default-OFF
  ``alpha_autonomous_live`` switch — this strategy only *originates*; routing
  stays in the engine.
- A deterministic exit backstop (max-loss / time-exit) bounds the LLM: it may
  close earlier, but it cannot defer a losing exit past the backstop.

Tag round-trips as ``HERMES_HERMESALPHA`` / ``HERMES-HERMESALPHA``; closes as
``HERMES_HERMESALPHA_CLOSE_<reason>`` (CLAUDE.md safety rule #5).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from ._credit_spread_base import CreditSpreadStrategy, TradeAction
from ._helpers import nearest_strike, parse_occ


class HermesAlpha(CreditSpreadStrategy):
    PRIORITY = 5
    NAME = "HERMESALPHA"

    KEY_PREFIX = "hermesalpha_"
    ANALYSIS_PERIOD = "3m"
    MANAGE_NEEDS_DTE = False

    # ── autonomy gate ───────────────────────────────────────────────────────
    def _is_autonomous(self) -> bool:
        """True only when the operator has put the overseer in autonomous mode.

        Origination depends on a present overseer *and* ``autonomy=='autonomous'``;
        any other state (no overseer, advisory, enforcing) leaves Alpha inert.
        """
        return (self.overseer is not None
                and str(getattr(self.overseer, "autonomy", "advisory")).lower()
                == "autonomous")

    # =======================================================================
    # ENTRIES — overseer originates the intent, the chain prices it
    # =======================================================================
    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        actions: List[TradeAction] = []
        if not self._is_autonomous():
            return actions

        width = int(self.config.get("hermesalpha_width", 5))
        target_lots = min(
            int(self.config.get("hermesalpha_target_lots", 1)),
            int(self.config.get("hermesalpha_max_lots", 1)),
        )
        symbols = list(dict.fromkeys(watchlist))
        self._log(f"↻ autonomous origination scan — {len(symbols)} symbol(s)")

        for symbol in symbols:
            try:
                if await self._in_cooldown(symbol):
                    continue
                analysis = await self.broker.analyze_symbol(symbol, period=self.ANALYSIS_PERIOD)
                if not analysis or "error" in analysis:
                    self._log(f"⚠️ {symbol}: analysis unavailable — {(analysis or {}).get('error','no data')}; skip.")
                    continue

                intent = await self.overseer.propose_intent(symbol, self._entry_context(symbol, analysis))
                if not intent:
                    continue
                action = await self._build_from_intent(symbol, intent, width, target_lots)
                if action is not None:
                    actions.append(action)
            except Exception as exc:
                self._log(f"❌ {symbol}: {exc}")
        return actions

    def _entry_context(self, symbol: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Compact, JSON-serialisable market context handed to the overseer."""
        return {
            "symbol": symbol,
            "current_price": analysis.get("current_price"),
            "period": self.ANALYSIS_PERIOD,
            "key_levels": analysis.get("key_levels", []),
            "trend": analysis.get("trend"),
            "iv_rank": analysis.get("iv_rank"),
        }

    async def _build_from_intent(self, symbol: str, intent: Dict[str, Any],
                                 default_width: int, target_lots: int) -> Optional[TradeAction]:
        """Resolve + price an overseer credit-spread intent against the live chain.

        The intent names the *structure*; every price here comes from the chain,
        and the entry is rejected unless the net credit clears the configured
        floor — the LLM cannot widen risk past the deterministic guards.
        """
        side = str(intent.get("side") or "").lower()
        if side not in ("put", "call"):
            self._log(f"✗ {symbol}: overseer intent has no valid side ({intent.get('side')!r}); skip.")
            return None
        target_delta = abs(float(intent.get("target_delta") or 0.16))
        dte_min = int(intent.get("dte_min") or 30)
        dte_max = int(intent.get("dte_max") or 45)
        width = int(intent.get("width") or default_width)

        expiry = await self.find_expiry_in_dte_range(symbol, dte_min, dte_max, prefer="max")
        if not expiry:
            self._log(f"ℹ️ {symbol}: no expiry in {dte_min}-{dte_max} DTE; skip.")
            return None

        chain = await self.broker.get_option_chains(symbol, expiry) or []
        if not chain:
            self._log(f"⚠️ {symbol} {side}: empty chain for {expiry}; skip.")
            return None

        short_leg = await self.find_strike_by_delta(chain, side, target_delta, tolerance=0.10)
        if not short_leg:
            self._log(f"✗ {symbol} {side}: no strike near Δ{target_delta:.2f}; skip.")
            return None

        sl_strike = float(short_leg["strike"])
        long_target = sl_strike - width if side == "put" else sl_strike + width
        long_leg = nearest_strike(chain, side, long_target)
        if not long_leg or long_leg["symbol"] == short_leg["symbol"]:
            self._log(f"✗ {symbol} {side}: no distinct long leg for short={sl_strike:.2f}; skip.")
            return None
        ll_strike = float(long_leg["strike"])
        if side == "put" and ll_strike >= sl_strike:
            self._log(f"✗ {symbol} put: long {ll_strike} ≥ short {sl_strike} (invalid); skip.")
            return None
        if side == "call" and ll_strike <= sl_strike:
            self._log(f"✗ {symbol} call: long {ll_strike} ≤ short {sl_strike} (invalid); skip.")
            return None

        actual_width = abs(sl_strike - ll_strike)
        credit = self.short_credit(short_leg, long_leg)
        min_credit_pct = float(self.config.get("hermesalpha_min_credit_pct", 0.20))
        min_credit = round(actual_width * min_credit_pct, 2)
        if credit < min_credit:
            self._log(
                f"✗ {symbol} {side}: credit ${credit:.2f} < min ${min_credit:.2f} "
                f"(width={actual_width:.2f}); skip."
            )
            return None

        short_delta = abs(float((short_leg.get("greeks") or {}).get("delta") or 0.0))
        pop = 1.0 - short_delta if short_delta > 0.0 else 1.0 - target_delta
        dte = (datetime.strptime(expiry, "%Y-%m-%d").date() - self.today()).days
        self._log(
            f"→ {symbol} {side}: AI intent short={sl_strike:.2f} long={ll_strike:.2f} "
            f"width={actual_width:.2f} credit=${credit:.2f} dte={dte} — {intent.get('rationale','')}"
        )

        action = TradeAction(
            strategy_id=self.strategy_id,
            symbol=symbol, order_class="multileg",
            legs=[
                {"option_symbol": short_leg["symbol"], "side": "sell_to_open", "quantity": target_lots},
                {"option_symbol": long_leg["symbol"],  "side": "buy_to_open",  "quantity": target_lots},
            ],
            price=credit, side="sell", quantity=1, order_type="credit",
            tag=f"HERMES_{self.NAME}",
            strategy_params={"short_leg": short_leg["symbol"], "long_leg": long_leg["symbol"],
                             "side_type": side, "pop": pop, "short_delta": short_delta},
            dte=dte, expiry=expiry, width=width,
        )
        # AI-authored: the overseer originated this, so the engine must not
        # re-review its own decision on the way to the broker.
        action.ai_authored = True
        action.ai_rationale = str(intent.get("rationale") or "autonomous origination")
        return action

    # =======================================================================
    # MANAGEMENT — overseer chooses the exit, the live quote validates it
    # =======================================================================
    async def manage_positions(self) -> List[TradeAction]:
        actions: List[TradeAction] = []
        trades = await self.db.trades.open_trades(self.strategy_id)
        if not trades:
            return actions

        cfg_width = int(self.config.get("hermesalpha_width", 5))
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
            width = float(row_width) if row_width is not None else float(cfg_width)
            info = parse_occ(short_leg)
            dte = (info["expiry"] - self.today()).days if info else None

            sq = quotes.get(short_leg)
            lq = quotes.get(long_leg)
            mid_debit, exec_debit, blocked, reason = self.compute_close_debit(sq, lq, width)

            # Deterministic backstop runs first — it bounds the LLM and also
            # covers the stale-quote case (force a width-priced time-exit).
            # mid_debit used for SL decision; exec_debit for limit price.
            backstop = self._exit_backstop(trade, dte, mid_debit, exec_debit, blocked, entry_credit, width)
            if backstop is not None:
                actions.append(backstop)
                continue
            if blocked:
                # No trustworthy live price → can't verify an LLM exit; hold.
                self._log(
                    f"⚠️ {trade['symbol']}: close-debit blocked ({reason}); "
                    f"hold (cannot verify exit price)"
                )
                continue

            decision = await self.overseer.decide_exit(
                trade, {"debit": mid_debit, "entry_credit": entry_credit, "dte": dte, "width": width})
            if str(decision.get("action") or "").lower() == "close":
                # Price from live exec_debit (ask-bid), never the LLM.
                self._log(
                    f"→ {trade['symbol']}: AI exit at live debit ${exec_debit:.2f} "
                    f"(mid ${mid_debit:.2f}) — {decision.get('rationale','')}"
                )
                actions.append(self._close_action(trade, exec_debit, "AI"))
        return actions

    def _exit_backstop(self, trade, dte: Optional[int], mid_debit, exec_debit,
                       blocked: bool, entry_credit: float, width: float) -> Optional[TradeAction]:
        """Hard exit the LLM cannot loosen: time-exit floor and max-loss stop.

        - At/under the time-exit DTE floor we force a TIME-EXIT (width-priced if
          the live quote is unusable) so a losing position can't ride to expiry.
        - A debit at/above the stop multiple forces an SL close while it is still
          below max loss (width).  mid_debit is used for the SL decision so that
          wide bid-ask spreads don't produce false triggers; exec_debit is used
          for the limit price so the close fills.
        """
        time_exit_dte = int(self.config.get("hermesalpha_time_exit_dte", 2))
        sl_mult = float(self.config.get("hermesalpha_sl_mult", 2.5))

        if dte is not None and dte <= time_exit_dte:
            price_debit = exec_debit if (not blocked and exec_debit is not None) else width
            return self._close_action(trade, price_debit, "TIME-EXIT")
        if not blocked and mid_debit is not None and entry_credit > 0:
            if mid_debit >= entry_credit * sl_mult and mid_debit < width:
                return self._close_action(trade, exec_debit, "SL")
        return None

    # ── unused base hooks (we override execute_entries / manage_positions) ────
    def _dte_summary(self, t) -> str:                                  # pragma: no cover
        return "AI"

    async def _resolve_entry_expiry(self, symbol: str, t):             # pragma: no cover
        return None

    def _completion_window(self, t):                                   # pragma: no cover
        return (0, 0)

    def _min_credit(self, dte: int, width: float, t) -> float:         # pragma: no cover
        return 0.0

    def _close_reason(self, trade, dte, debit, entry_credit, width, t): # pragma: no cover
        return None
