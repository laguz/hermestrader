"""DS02 — priority-7, 0 DTE implied-move iron condors (docs/ds02_spec.md).

Independent design (not a DS0 derivative): sells defined-risk verticals
whose strikes sit at the option chain's own live-implied move for the rest
of the session — the ATM straddle price (nearest-strike call mid + put
mid) — rather than at historical ATR or institutional S/R pivots. 0DTE
implied moves are priced in by the market and, on average, overstate what
actually happens by the close; selling right at that boundary (behind a
real delta/POP/credit gate) collects that gap. The signal is also
self-adjusting for free: the straddle price bleeds down through the day as
theta burns off, so a later entry automatically sits on a tighter range
than an earlier one — no session-open anchor needed.

Entry qualification, per side, on daily-expiry symbols from DS02's own
watchlist (empty watchlist = idle, never the global fallback):

- ET wall-clock inside ``[ds02_entry_start, ds02_entry_cutoff)`` (defaults
  10:00–13:30 — skips the open's unreliable quotes, stops early enough
  that the residual credit still compensates for the resting size).
- The ATM straddle price (``_implied_move``) sets a synthetic level each
  side: ``spot − ds02_move_mult × straddle`` (support) and
  ``spot + ds02_move_mult × straddle`` (resistance). These feed the same
  level-walk / honest-chain-delta-POP / EV-ranked strike selection every
  credit-spread strategy in this codebase shares
  (:class:`CreditSpreadStrategy`), gated on ``ds02_pop_target``,
  ``ds02_short_delta_min/max`` and ``ds02_min_credit_pct`` × width.
- Optional IV-rank floor (``ds02_min_ivr``, default 0 = off) via the
  shared ``is_ivr_gated`` — sell only when compensated more richly.
- Optional earnings/macro blackout (``ds02_event_blackout_days``,
  ``ds02_macro_blackout_days``) via the shared gates.
- One shot per side per symbol per day — win, loss, or resting entry all
  block alike.

Management is my own policy, not DS0's:

- TP at ``ds02_tp_pct`` (50%) of the entry credit captured — standard
  decay-harvesting, cuts the tail instead of holding a nearly-worthless
  spread into pin risk for a few extra cents.
- SL at ``ds02_sl_mult`` (2.5×) entry credit, base machinery, width-capped.
- Blanket EOD flatten at ``ds02_eod_close_time`` (15:45 ET): whatever is
  still open closes at the best executable price, no per-trade
  strike-proximity judgment call — a defined-risk premium-selling program
  has no business holding into assignment territory on American-style,
  physically-settled underlyings (QQQ/SPY).

Ships **default-disabled** (``DEFAULT_DISABLED_STRATEGIES`` in
``hermes/common.py``): on live, ``alpha_autonomous_live`` routes enabled
strategies' entries straight to the broker, so a new strategy must be
armed by the operator, never by a deploy.

Tag round-trips as ``HERMES_DS02`` / ``HERMES-DS02``; closes as
``HERMES_DS02_CLOSE_<reason>`` (CLAUDE.md safety rule #5).
"""
from __future__ import annotations

from datetime import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._credit_spread_base import CreditSpreadStrategy, TradeAction
from ._zero_dte import ZeroDTEMixin, _parse_hhmm


class CreditSpreads0DTE(ZeroDTEMixin, CreditSpreadStrategy):
    PRIORITY = 7
    NAME = "DS02"

    KEY_PREFIX = "ds02_"
    ANALYSIS_PERIOD = "3m"
    RESCALE_CREDIT_TO_WIDTH = False   # flat min-credit, like CS7
    MANAGE_NEEDS_DTE = False          # everything expires today anyway

    # =======================================================================
    # ENTRIES — time window + own-watchlist gate, then the shared engine
    # =======================================================================
    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        t = await self.load_tunables()
        now_t = self._now_et().time()
        start = _parse_hhmm(t.ds02_entry_start, time(10, 0))
        cutoff = _parse_hhmm(t.ds02_entry_cutoff, time(13, 30))
        if not (start <= now_t < cutoff):
            return []

        # Same footgun as DS0: the engine's _watchlist_for falls back to the
        # global default list when a strategy's own watchlist is empty. An
        # empty own-watchlist means idle, full stop.
        own_wl = await self.db.watchlist.list_watchlist(self.strategy_id)
        if not own_wl:
            self._log("ℹ️ DS02 watchlist is empty — idle (no global fallback).")
            return []
        own_syms = {s.split(":", 1)[0].strip().upper() for s in own_wl}
        symbols = [s for s in dict.fromkeys(watchlist)
                   if s.split(":", 1)[0].strip().upper() in own_syms]
        if not symbols:
            return []
        return await super().execute_entries(symbols)

    @staticmethod
    def _implied_move(chain: List[Dict[str, Any]], spot: float) -> Optional[float]:
        """ATM straddle price: nearest-strike call mid + nearest-strike put mid.

        The market's own estimate of how far the underlying still moves
        today. ``None`` when either leg's quote is missing/stale — the
        symbol is skipped rather than traded on a guessed range.
        """
        calls = [o for o in chain if o.get("option_type") == "call"]
        puts = [o for o in chain if o.get("option_type") == "put"]
        if not calls or not puts:
            return None
        atm_call = min(calls, key=lambda o: abs(float(o["strike"]) - spot))
        atm_put = min(puts, key=lambda o: abs(float(o["strike"]) - spot))
        call_mid = ZeroDTEMixin._mid(atm_call)
        put_mid = ZeroDTEMixin._mid(atm_put)
        if call_mid is None or put_mid is None:
            return None
        return call_mid + put_mid

    async def _build_spread_action(self, *, symbol, expiry, side, lots, width,
                                   min_credit, analysis, current_price, t) -> Optional[TradeAction]:
        # One shot per side per symbol per day — a win, a loss and a resting
        # or queued entry all block alike.
        if await self.db.trades.count_trades_for_expiry(self.strategy_id, symbol, side, expiry):
            self._log(f"ℹ️ {symbol} {side}: already traded this side today; skip.")
            return None
        if await self.db.trades.count_pending_orders(self.strategy_id, symbol, side, expiry):
            self._log(f"ℹ️ {symbol} {side}: entry already pending/queued; skip.")
            return None

        chain = await self.broker.get_option_chains(symbol, expiry) or []
        if not chain:
            self._log(f"{symbol} {side}: empty chain for {expiry}; skip.")
            return None
        move = self._implied_move(chain, current_price)
        if move is None:
            self._log(f"⚠️ {symbol} {side}: no ATM straddle quote; skip.")
            return None
        move *= float(t.ds02_move_mult)

        if side == "put":
            level = {"price": current_price - move, "type": "support", "strength": 1}
        else:
            level = {"price": current_price + move, "type": "resistance", "strength": 1}

        filtered = dict(analysis)
        filtered["key_levels"] = [level]
        return await super()._build_spread_action(
            symbol=symbol, expiry=expiry, side=side, lots=lots, width=width,
            min_credit=min_credit, analysis=filtered,
            current_price=current_price, t=t,
        )

    # =======================================================================
    # MANAGEMENT — base TP/SL machinery, then a blanket EOD flatten
    # =======================================================================
    async def manage_positions(self) -> List[TradeAction]:
        actions = await super().manage_positions()

        t = await self.load_tunables()
        close_time = _parse_hhmm(t.ds02_eod_close_time, time(15, 45))
        if self._now_et().time() < close_time:
            return actions

        handled_ids = {a.strategy_params.get("trade_id") for a in actions}
        trades = await self.db.trades.open_trades(self.strategy_id)
        remaining = [tr for tr in trades if tr["id"] not in handled_ids]
        if not remaining:
            return actions

        cfg_width = float(t.ds02_width)
        leg_syms = {tr[k] for tr in remaining for k in ("short_leg", "long_leg") if tr.get(k)}
        raw_quotes = (await self.broker.get_quote(",".join(leg_syms)) or []) if leg_syms else []
        quotes = {q["symbol"]: q for q in raw_quotes if "symbol" in q}

        for trade in remaining:
            width = float(trade["width"]) if trade.get("width") is not None else cfg_width
            _mid_debit, exec_debit, blocked, _reason = self.compute_close_debit(
                quotes.get(trade["short_leg"]), quotes.get(trade["long_leg"]), width)
            # A stale quote must not leave the position open past the close
            # window — fall back to a width-priced close (the structural
            # max, capped again inside _close_action).
            price = exec_debit if (not blocked and exec_debit is not None) else width
            self._log(
                f"→ {trade['symbol']} {trade.get('side_type')}: EOD-FLATTEN — "
                f"closing at ${price:.2f}"
            )
            actions.append(self._close_action(trade, price, "EOD-FLATTEN"))
        return actions

    # =======================================================================
    # Hooks
    # =======================================================================
    def _dte_summary(self, t) -> str:
        return "0"

    async def _resolve_entry_expiry(self, symbol: str, t) -> Optional[str]:
        expiry = await self.find_expiry_in_dte_range(symbol, 0, 0)
        if not expiry:
            self._log(f"ℹ️ {symbol}: no same-day expiration; skip.")
        return expiry

    def _completion_window(self, t) -> Tuple[int, int]:
        # Completing the second side is only ever a same-day affair.
        return (0, 0)

    def _min_credit(self, dte: int, width: float, t) -> float:
        return round(width * float(t.ds02_min_credit_pct), 2)

    def _tp_profit(self, credit: float, width: float, dte: int, t) -> float:
        return credit * float(t.ds02_tp_pct)

    def _close_reason(self, trade, dte, debit, entry_credit, width, t) -> Optional[str]:
        """TP @ debit ≤ (1 − ds02_tp_pct) × entry credit; SL @ ds02_sl_mult × credit."""
        if debit <= entry_credit * (1.0 - float(t.ds02_tp_pct)):
            return "TP"
        if debit >= entry_credit * float(t.ds02_sl_mult):
            if debit < width:
                return "SL"
            self._log(
                f"ℹ️ {trade['symbol']} {trade.get('side_type')}: debit ${debit:.2f} "
                f"is at/above width ${width:.2f} (max loss); skipping SL close."
            )
        return None
