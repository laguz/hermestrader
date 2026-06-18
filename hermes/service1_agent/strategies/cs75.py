"""CreditSpreads75 — priority-1 strategy.

Entry contract
--------------
- Mode A (no incomplete IC on this symbol):
    * Find an expiry in the 39–45 DTE window (prefer the latest).
    * Open both put and call spreads (Iron Condor).
- Mode B (already one side open):
    * Reuse the existing expiry if 14–45 DTE; otherwise skip.
    * Open the missing side only.

Selection
---------
Walk the analysis' key_levels (institutional-flow S/R, augmented with POP
by ``augment_levels_with_pop``). Pick the level whose POP is closest to
0.75 (i.e. ≥75% probability the short stays OTM). Verify the chain strike
nearest that level has ``0.05 ≤ |Δ| ≤ 0.40``.

Width is configurable (``cs75_width`` env, default 5). Required net credit is
25% of the actual snapped width for 30–45 DTE entries, 20% for 14–29 DTE.

Management contract
-------------------
- TP @ 50% credit captured for 21–45 DTE
- TP @ 75% credit captured for <21 DTE
- SL @ 2.5× entry credit
- Hard time exit ≤ 8 DTE

The entry/management *machinery* lives in
:class:`~hermes.service1_agent.strategies._credit_spread_base.CreditSpreadStrategy`;
this file declares only what makes CS75 a 39–45 DTE iron condor. Closing
orders are tagged ``HERMES_CS75_CLOSE_<reason>`` by the shared close action.
"""
from __future__ import annotations

from typing import Optional, Tuple

from ._credit_spread_base import CreditSpreadStrategy, TradeAction


class CreditSpreads75(CreditSpreadStrategy):
    PRIORITY = 1
    NAME = "CS75"

    KEY_PREFIX = "cs75_"
    ANALYSIS_PERIOD = "6m"            # longer-dated entries → 6M regime
    RESCALE_CREDIT_TO_WIDTH = True    # re-scale min credit to the snapped width
    MANAGE_NEEDS_DTE = True           # TP bands + time-exit need a dated position

    def _dte_summary(self, t) -> str:
        return f"{t.cs75_min_dte}-{t.cs75_max_dte}"

    async def _resolve_entry_expiry(self, symbol: str, t) -> Optional[str]:
        expiry = await self.find_expiry_in_dte_range(
            symbol, t.cs75_min_dte, t.cs75_max_dte, prefer="max")
        if not expiry:
            self._log(f"ℹ️ {symbol}: no expiry found in {t.cs75_min_dte}-{t.cs75_max_dte} DTE range; skip.")
        return expiry

    def _completion_window(self, t) -> Tuple[int, int]:
        return t.cs75_completion_min_dte, t.cs75_max_dte

    def _min_credit(self, dte: int, width: float, t) -> float:
        min_credit_pct = (t.cs75_min_credit_pct_far if 30 <= dte <= 45
                          else t.cs75_min_credit_pct_near)
        return round(width * min_credit_pct, 2)

    def _close_reason(self, trade, dte, debit, entry_credit, width, t) -> Optional[str]:
        """TP @ 50% (DTE 21–45) or 75% (DTE<21); SL @ 2.5×; time exit ≤ 8 DTE."""
        if 21 <= dte <= 45 and debit <= entry_credit * t.cs75_tp_pct_far:
            return "TP-50"
        if dte < 21 and debit <= entry_credit * t.cs75_tp_pct_near:
            return "TP-75"
        if debit >= entry_credit * t.cs75_sl_mult:
            # Stop-loss width safety cap: don't close if already at/above max loss.
            if debit < width:
                return "SL-2.5x"
            self._log(
                f"ℹ️ {trade['symbol']} {trade.get('side_type')}: debit ${debit:.2f} "
                f"is at/above width ${width:.2f} (max loss); skipping SL close."
            )
            return None
        if dte <= t.cs75_time_exit_dte:
            return "TIME-EXIT"
        return None

    def _forced_close_on_blocked(self, trade, dte, width, reason, t) -> Optional[TradeAction]:
        # At ≤ time-exit DTE we cannot defer past the next quote refresh: force a
        # TIME-EXIT priced at a synthetic worst-case debit (= width) so the order
        # still goes in but priced defensively.
        if dte is not None and dte <= t.cs75_time_exit_dte:
            self._log(
                f"⚠️ {trade['symbol']}: close-debit blocked ({reason}) but DTE={dte} "
                f"≤ {t.cs75_time_exit_dte} — forcing TIME-EXIT at width-priced debit"
            )
            return self._close_action(trade, width, "TIME-EXIT")
        return None
