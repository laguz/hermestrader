# DS02 — 0 DTE Implied-Move Iron Condors

*Spec v1 — 2026-07-17. Independent strategy design, not a DS0 derivative.
Designed by Claude at the operator's request ("create a 0DTE strategy; you
pick the parameters"); parameters below are the designer's defaults, all
operator-tunable. Ships **default-disabled**.*

## Thesis

Sell defined-risk verticals whose short strikes sit at the option chain's own
live-implied move for the rest of the session — the ATM straddle price
(nearest-strike call mid + nearest-strike put mid) — rather than at
historical ATR or institutional S/R pivots. 0DTE implied moves are priced in
by the market and, on average, overstate what actually happens by the close;
selling right at that boundary (behind a real delta/POP/credit gate) is what
collects that gap. The signal is also self-adjusting for free: the straddle
price bleeds down through the day as theta burns off, so an entry later in
the window automatically sits on a tighter range than an earlier one — no
session-open anchor is needed at all. No profitability guarantee is expressed
or implied; the edge claim is a documented, well-known tendency, not a proof.

Architecturally this reuses the same shared credit-spread engine every other
premium-selling strategy in this codebase already uses
(`CreditSpreadStrategy` — honest chain-delta POP, EV-ranked strike selection,
the iron-condor planner), the same way CS75/CS7/HermesAlpha do. Only the
signal feeding it (an implied-move straddle price, computed fresh per tick)
and the management policy below are DS02's own.

## Universe

DS02's **own** watchlist only (seeded `["QQQ"]`; empty = idle, never the
global fallback). Symbols without a same-day expiration skip safely.

## Entry (per side, both sides independent; both filled = iron condor)

A side qualifies when ALL of the following hold:

1. ET wall-clock in `[ds02_entry_start, ds02_entry_cutoff)` — default
   **10:00–13:30**. Before 10:00 the opening quotes/spreads are unreliable;
   after 13:30 the residual credit no longer compensates for the resting
   size.
2. The ATM straddle price sets a synthetic level: `spot − ds02_move_mult ×
   straddle` (support, arms the put side) or `spot + ds02_move_mult ×
   straddle` (resistance, arms the call side). Default `ds02_move_mult`
   **1.0** — sell right at the market's own 1× implied move.
3. That level feeds the shared credit-spread engine exactly like any other
   strategy's S/R level: snap to the nearest chain strike, then gate on
   - honest chain-delta POP ≥ `ds02_pop_target` (**0.80**),
   - |Δ| in `[ds02_short_delta_min, ds02_short_delta_max]` (**0.05–0.20**),
   - net credit ≥ `ds02_min_credit_pct × width` (**10% of $1 width**),
   - highest-EV candidate wins (EV priced against DS02's own TP/SL below).
4. Not event-gated: `ds02_macro_blackout_days` (**1**) sits out FOMC/CPI
   days — scheduled volatility is exactly what a 0DTE seller must avoid.
   Earnings blackout defaults **0** (index ETFs). Optional `ds02_min_ivr`
   floor (default **0**, off) to require richer premium before selling.
5. One shot per side per symbol per day: any DS02 trade (OPEN / CLOSING /
   CLOSED) or resting entry for that (symbol, side, today) blocks re-entry,
   wins included.

Order: sell-to-open vertical, `ds02_width` (**$1**) wide, marketable credit
limit at the measured credit, tagged `HERMES_DS02`. Sizing
`ds02_target_lots`/`ds02_max_lots` (**1/1**); max structural risk per lot ≈
$90 at the default credit floor.

## Management

- **Take-profit** at `ds02_tp_pct` (**50%**) of the entry credit captured —
  standard decay-harvesting: bank the bulk of the win and get out before a
  cheap spread's small remaining value is outweighed by gamma-tail risk.
- **Stop loss**: close when the mid debit reaches `ds02_sl_mult` (**2.5×**)
  the entry credit — base machinery, width-capped (never pay ≥ max loss to
  exit), morning-pricing guard inherited (no panic closes into unreliable
  pre-10:30 quotes, though DS02 never enters that early anyway).
- **Blanket EOD flatten** at `ds02_eod_close_time` (**15:45 ET**): whatever
  is still open (TP/SL never fired) closes at the best executable price —
  no per-trade strike-proximity judgment call. A defined-risk premium
  program has no business holding into assignment territory on
  American-style, physically-settled underlyings (QQQ/SPY); flattening
  everything is simpler and more predictable than trying to case-by-case
  guess which spreads are actually threatened.

## Safety posture

- **Default-disabled** via `DEFAULT_DISABLED_STRATEGIES` in
  `hermes/common.py`: with the setting row absent, the agent, control state
  and watcher all treat DS02 as OFF. On live, `alpha_autonomous_live` routes
  enabled strategies' entries straight to the broker — a new strategy must
  be armed by the operator from the C2 panel, never by a deploy.
- Everything else inherits the standard gates: `MoneyManager` sizing,
  `PortfolioRiskEngine`, the approval queue (when not in the autonomous
  carve-out), paper/live mode, `dry_run`, and the off-hours gate.

## Review checklist before arming on live

1. Enable on **paper** first (`strategy_ds02_enabled=true` via the C2
   toggle on the paper stack) and let it run through at least one FOMC/CPI
   week; check `trades` for TP/SL/EOD-flatten frequency and realized P&L
   against the entry credit.
2. Sanity-check a few live straddle-derived levels against the chain by
   hand — confirm the implied move looks sane relative to the day's actual
   range, especially on a low-liquidity or halted-then-reopened symbol.
3. Only then enable on live, starting at 1 lot.
