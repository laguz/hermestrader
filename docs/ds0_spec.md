# DS0 — QQQ 0 DTE Mean-Reversion Debit Spreads (Specification)

Status: **DRAFT — awaiting operator sign-off. No code exists yet.**
Author: designed interactively with the operator, 2026-07-10.

## Concept

A fully rule-based (no LLM involvement) contrarian fade at intraday
support/resistance on daily-expiry underlyings (watchlist-driven, seeded with
QQQ), expressed as same-day-expiry **debit** spreads with
bracket-style resting limit orders. Price touching the **upper bound**
(resistance) opens a **put debit spread** betting the bounce down; price
touching the **lower bound** (support) opens a **call debit spread** betting
the bounce up. Both sides are independent and may be open simultaneously on a
day that touches both bounds (a reverse-iron-condor-shaped book).

Everything after submission is price-bound: the entry is a day-limit at a
fixed maximum debit, the exit is a resting day-limit at a fixed credit placed
immediately on fill, plus one deterministic 3:00 PM ET sweep. There is **no
stop loss** — the debit paid is the entire accepted risk per side.

## Entry rules

1. **Universe**: operator-managed watchlist, same mechanics as CS7/CS75 —
   DS0 gets its own `strategy_watchlists` rows (managed from the watcher UI
   like the others), supports per-symbol `target_lots` overrides via
   `list_watchlist_detailed`, and the `"SYMBOL:LOTS"` inline syntax. Seeded
   with **QQQ**. Any watchlist symbol without a same-day expiration is
   skipped with a log line (in practice only daily-expiry underlyings — QQQ,
   SPY, IWM — will ever trade; everything else no-ops safely). All entry
   rules below apply per watchlist symbol; the per-side position limits and
   cooldowns in rule 7 are per symbol.
2. **Levels**: today's support/resistance bounds come from the existing
   `analyze_symbol(period="3m")` key levels with the standard POP overlay
   (`augment_levels_with_pop`, period `"3m"`) — the same stack CS7 uses.
3. **Triggers** (each side fully independent):
   - Price within `ds0_trigger_band` of **resistance** → candidate **put**
     debit spread.
   - Price within `ds0_trigger_band` of **support** → candidate **call**
     debit spread.
   - Entries ride the existing reactive S/R trigger path
     (`process_reactive_entries`) so touches between ticks are caught; the
     regular tick scan is the fallback.
4. **POP gate**: the POP engine's 3-month probability for the touched level
   must be **≥ `ds0_pop_target` (default 0.75)**. Mapping: fading resistance
   expresses the same directional view as a *call credit spread* at that
   resistance, so DS0 computes POP exactly as CS7 would for that level and
   side (chain delta of the strike at the level, `xgb_prob`, vols, protection
   score, period `"3M"`) and requires ≥ 0.75. Same engine, same number,
   reused unchanged.
5. **Structure**: 1-wide vertical (`ds0_width`, default 1 — operator default,
   see Open defaults). Strikes are the closest-to-the-money pair in the
   bounce direction whose net ask-side debit is ≤ `ds0_open_price`. At a
   $0.10 cap the spread is necessarily OTM, pointing toward the opposite
   bound.
6. **Entry order**: day-limit **buy at max $0.10 debit**
   (`ds0_open_price`, default 0.10). Never repriced, never chased; unfilled
   at end of day → the order dies.
7. **Position limits**: max **one open put spread and one open call spread
   per symbol per day** (per-side bookkeeping, same shape as CS75/CS7's
   per-side IC tracking). Cooldown is **per-side**: a closed or expired put
   side never blocks the call trigger on the same symbol. No intraday
   re-entry on the same side, even after a win — one shot per side per
   symbol per day.
8. **Sizing**: `ds0_target_lots` / `ds0_max_lots` (global defaults, per-
   symbol override via the watchlist as in rule 1), default **1 lot**
   (operator wants risk "low"). Worst-case day = 2 sides × $0.10 × 100 ×
   lots = **$20/lot-pair per watchlist symbol**, so total daily risk scales
   with watchlist size — keep the watchlist short. Entries still flow
   through `MoneyManager` and `PortfolioRiskEngine` like every other
   strategy.

## Exit rules

1. **On entry fill** (hook: existing `ORDER_FILL` reactive event; next tick
   as fallback): immediately place the closing day-limit **sell at $0.40**
   (`ds0_close_price`, default 0.40). It rests all day.
2. **3:00 PM ET sweep** (`ds0_sweep_time`, default 15:00 ET): for each open
   DS0 spread, take the live mark (mid, via the same batched-quote path the
   other strategies use):
   - **mark > $0.10 and < $0.40** → cancel the resting $0.40 order and close
     now with a marketable limit (banks the partial profit / avoids
     assignment exposure on anything with meaningful value).
   - **mark ≤ $0.10** → leave it; it rides to expiration and expires
     worthless as the accepted debit loss.
   - **mark ≥ $0.40** should be impossible (the resting limit would have
     filled); if it ever occurs (pathologically wide market), close it too —
     that only banks more than the target.
3. **No stop loss, no morning-pricing guard, no LLM exit.** The only exits
   are the $0.40 limit and the 3:00 PM sweep.

### Residual risk noted for sign-off

A spread marked ≤ $0.10 at 3:00 PM stays open, so a violent move in the last
hour could put price at/through its strikes at expiration → assignment risk
and an overnight QQQ share position (American-style, physical settlement).
Proposed default-ON safety tunable, **pending operator approval**:
`ds0_assignment_guard` — at 3:50 PM ET, force-close a still-open spread only
if its short strike is ITM or within `ds0_guard_band` of spot. Clearly-OTM
spreads still expire untouched. This is not a stop loss; it fires only in the
near-the-money tail case the 3:00 PM sweep can miss.

## Economics (stated for the record)

Risk $0.10 to net $0.30 → breakeven hit rate 25% (ignoring the 3:00 PM
partial-profit closes, which improve it). The 75% POP gate is the probability
the *level holds* — not directly the probability the spread reaches $0.40,
which additionally requires price to travel to the strikes. Realized hit rate
vs. the 25% breakeven is the key paper-trading metric; wire DS0 entries into
the prediction ledger so `backfill_prediction_outcomes` can measure it.

## Architecture fit

- New strategy class `DS0` in `hermes/service1_agent/strategies/ds0.py`,
  subclassing `AbstractStrategy` directly (not `CreditSpreadStrategy` — the
  credit base's POP-target strike walk, min-credit floors, and IC Mode A/B
  completion are all inverted for a directional debit structure). Reuse
  `nearest_strike` / `parse_occ` from `_helpers.py` and the batched-quote
  management pattern.
- **Priority 6**, after HermesAlpha — lowest claim on buying power until
  proven. Registered in the cascading order; the tick pipeline order itself
  is untouched (CLAUDE.md safety rule #3).
- **Tags**: `HERMES_DS0` opens, `HERMES_DS0_CLOSE_<reason>` closes; all
  matchers accept both `_` and `-` forms (safety rule #5). Close reasons:
  `TP` (the $0.40 fill is broker-side, recorded on reconcile), `SWEEP-3PM`,
  `ASSIGN-GUARD` (if approved).
- **Persistence**: opens record `entry_debit` (plumbing already exists in
  `trades.py` / `orm.py`; realized P&L uses `_compute_realized_pnl`'s debit
  branch). DS0 is the first strategy to *open* debit positions — regression
  tests must pin that `entry_debit`, not `entry_credit`, is recorded and
  that fill-price reconciliation handles the debit direction.
- **Approvals**: normal `approval_mode` flow — no autonomous carve-out in
  v1. An unapproved DS0 entry auto-expires after `ds0_approval_ttl`
  (default 15 min) so a stale trigger never executes hours later.
- **Known sharp edge**: DS0 would be the first caller passing `dte=0` into
  the POP engine's DTE-aware lognormal correction. Verify behavior at zero
  before wiring; the engine's linear no-dte path may be the correct route.

## Tunables (all in the `tunables.py` catalog from day one)

| Key | Default | Meaning |
|---|---|---|
| `ds0_enabled` | off | master switch |
| `ds0_open_price` | 0.10 | max entry debit (day-limit price) |
| `ds0_close_price` | 0.40 | resting close limit placed on fill |
| `ds0_pop_target` | 0.75 | min 3m POP for the touched level |
| `ds0_width` | 1 | vertical width (operator default, confirm) |
| `ds0_trigger_band` | TBD | proximity to a bound that arms a trigger |
| `ds0_target_lots` / `ds0_max_lots` | 1 / 1 | sizing (keep low) |
| `ds0_entry_cutoff` | 14:00 ET | no new entries after (operator default, confirm) |
| `ds0_sweep_time` | 15:00 ET | the partial-profit/flatten sweep |
| `ds0_approval_ttl` | 900 s | unapproved entry auto-expiry |
| `ds0_assignment_guard` | on | 3:50 PM near-the-money force-close (pending approval) |
| `ds0_guard_band` | TBD | short-strike-to-spot distance that trips the guard |

## Open defaults awaiting operator confirmation

1. **Width = 1** — assumed; wider at a $0.10 cap means far-OTM strikes that
   rarely see $0.40.
2. **Entry cutoff 14:00 ET** — assumed; with the 3:00 PM sweep, entries
   after 2 PM have almost no runway. No entry-start restriction (triggers
   armed from the open) unless the operator wants to skip the 9:30–10:00
   chop.
3. **`ds0_assignment_guard`** — recommended default-ON; operator may strike
   it.

## Testing posture (safety rule #1 — before any live tick)

Stub-broker tests: POP gate at/below/above 0.75; debit cap respected and
never repriced; close order placed exactly once on fill; both sides open
independently and simultaneously; per-side cooldown; one-shot-per-side-per-
day; 3:00 PM sweep matrix (≤0.10 hold / 0.10–0.40 close / ≥0.40 close);
sweep overrides nothing it shouldn't (no morning guard interaction); tag
round-trip both forms; `entry_debit` recorded and debit-branch P&L; approval
TTL expiry; `dte=0` POP engine behavior pinned. Paper instance for an
extended burn-in before the live checkout ever sees DS0.
