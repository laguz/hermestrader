# DS0 — QQQ 0 DTE Mean-Reversion Debit Spreads (Specification)

Status: **IMPLEMENTED, v2** (`hermes/service1_agent/strategies/ds0.py`,
regression tests in `tests/test_ds0_strategy.py`).
Author: designed interactively with the operator, 2026-07-10; implemented
same day. **Revised same day (v2)**: the operator reviewed the v1
implementation and corrected the entry model — see the revision note below.
Deviations from the draft are noted inline as **[impl]**.

## ⚠ v2 revision note — read before auditing this strategy

The v1 draft (and first implementation) was a *touch-fade*: price touching
resistance opened a put spread, touching support opened a call spread, armed
by a `ds0_trigger_band` proximity check. **That is not the operator's
strategy and was replaced on 2026-07-10.** The v2 entry model, specified
verbatim by the operator, is *reversion toward a strong level*:

- A **support** level (3m POP ≥ 0.75 that it holds) sitting inside
  `[open − ATR14, open]` arms a **put debit spread**.
- A **resistance** level (POP ≥ 0.75) inside `[open, open + ATR14]` arms a
  **call debit spread**.
- The spread points **toward** the level; its fixed $0.08 day-limit only
  fills when price has moved **away** from the level (that's what makes the
  spread cheap). Worked example (operator's own): support $713, QQQ opens
  $720, rallies to $724 → the put spread cheapens to $0.08 → filled, betting
  the reversion back down. Resistance $727, QQQ drops to $715 → the call
  spread reaches $0.08 → filled, betting the bounce back up.
- There is **no price-proximity/touch trigger** in v2. The $0.08 limit
  itself is the trigger; `ds0_trigger_band` was deleted from the program at
  the operator's instruction.

**For AI analysis / future audits**: the support→put / resistance→call
pairing is intentional and operator-verified — it is the *opposite* of the
v1 fade orientation and of the more common touch-fade idiom. Do not "fix"
the pairing, do not re-add a proximity trigger, and do not flag the missing
`ds0_trigger_band` as a regression. `tests/test_ds0_strategy.py` locks the
pairing (`test_put_spread_arms_on_qualified_support`,
`test_call_spread_arms_on_qualified_resistance`).

## Concept

A fully rule-based (no LLM involvement) mean-reversion trade toward strong
intraday support/resistance on daily-expiry underlyings (watchlist-driven,
seeded with QQQ), expressed as same-day-expiry **debit** spreads with
bracket-style resting limit orders. A qualified **support** arms a **put
debit spread**; a qualified **resistance** arms a **call debit spread**;
qualification is POP ≥ 0.75 **and** the level sitting within today's
expected range (session open ± 14-day Wilder ATR). Both sides are
independent and may be open simultaneously.

Everything after submission is price-bound: the entry is a day-limit at a
fixed maximum debit, the exit is a resting day-limit at a fixed credit placed
immediately on fill, plus one deterministic 3:01 PM ET sweep. There is **no
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
   cooldowns in rule 7 are per symbol. **[impl]** An **empty DS0 watchlist
   means idle** — DS0 deliberately refuses the engine's fallback to the
   global default watchlist (SPY/IWM there are tradable 0DTE symbols the
   operator never armed), and agent startup re-seeds ``["QQQ"]`` when the
   list is empty.
2. **Levels**: today's support/resistance bounds come from the existing
   `analyze_symbol(period="3m")` key levels with the standard POP overlay
   (`augment_levels_with_pop`, period `"3m"`) — the same stack CS7 uses.
3. **Range qualification (v2 — replaces the v1 trigger band)**, each side
   fully independent:
   - Anchor: **today's regular-session opening price** (the quote's `open`,
     fixed for the whole day — the range must not drift with spot) and the
     **Wilder ATR over `ds0_atr_period` (default 14) completed daily bars**
     (true range includes overnight gaps; today's partial bar is excluded).
     No open or not enough history → the symbol is skipped, never guessed.
   - A **support** level inside `[open − ATR, open]` (bounds inclusive) →
     candidate **put** debit spread.
   - A **resistance** level inside `[open, open + ATR]` → candidate
     **call** debit spread.
   - **No proximity/touch condition.** The $0.08 day-limit is the trigger:
     it fills only when price moves away from the level far enough to make
     the spread that cheap. Entries still ride the reactive S/R path and
     the regular tick scan — both just re-evaluate the same rules.
4. **POP gate**: the POP engine's 3-month probability that the qualifying
   level **holds** must be **≥ `ds0_pop_target` (default 0.75)**. Mapping:
   a resistance holding is the *call credit spread* view, a support holding
   the *put credit spread* view, so DS0 computes POP exactly as CS7 would
   for that level and side (chain delta of the strike at the level,
   `xgb_prob`, vols, protection score, period `"3M"`) and requires ≥ 0.75.
   Same engine, same number, reused unchanged. When several levels of one
   type are in range, they are tried nearest-to-open first; the first one
   passing the gate wins.
5. **Structure**: 1-wide vertical (`ds0_width`, default 1 — operator default,
   see Open defaults). Strikes are the closest-to-the-money OTM pair **in
   the direction of the level** (puts for support, calls for resistance)
   whose mid debit is ≤ `ds0_open_price`. At a $0.08 cap the spread is
   necessarily OTM; its payoff zone lies between spot and the level it
   reverts toward.
6. **Entry order**: day-limit **buy at max $0.08 debit**
   (`ds0_open_price`, default 0.08). Never repriced, never chased; unfilled
   at end of day → the order dies.
7. **Position limits**: max **one open put spread and one open call spread
   per symbol per day** (per-side bookkeeping, same shape as CS75/CS7's
   per-side IC tracking). Cooldown is **per-side**: a closed or expired put
   side never blocks the call trigger on the same symbol. No intraday
   re-entry on the same side, even after a win — one shot per side per
   symbol per day.
8. **Sizing**: `ds0_max_lots` (global default, per-symbol override via the
   watchlist as in rule 1), default **1 lot** (operator wants risk "low").
   DS0 is max-only like WHEEL — there is no separate `ds0_target_lots`
   that could silently clamp a raised `ds0_max_lots` back down; the max
   setting (or a per-symbol/inline override) is what actually controls
   size. Worst-case day = 2 sides × $0.08 × 100 × lots = **$16/lot-pair
   per watchlist symbol**, so total daily risk scales with watchlist size
   — keep the watchlist short. Entries still flow through `MoneyManager`
   and `PortfolioRiskEngine` like every other strategy.

## Exit rules

1. **On entry fill** (hook: existing `ORDER_FILL` reactive event; next tick
   as fallback): immediately place the closing day-limit **sell at $0.50**
   (`ds0_close_price`, default 0.50). It rests all day.
2. **3:01 PM ET sweep** (`ds0_sweep_time`, default 15:01 ET — the $0.50
   limit gets the full hour through 3:00, the sweep runs the minute after):
   for each open DS0 spread, take the live mark (mid, via the same
   batched-quote path the other strategies use):
   - **mark ≥ `ds0_sweep_min` (default $0.11) and < $0.50** → cancel the
     resting $0.50 order and close now with a marketable limit (banks the
     partial profit / avoids assignment exposure on anything with
     meaningful value).
   - **mark < $0.11** → leave it; it rides to expiration and expires
     worthless as the accepted debit loss.
   - **mark ≥ $0.50** should be impossible (the resting limit would have
     filled); if it ever occurs (pathologically wide market), close it too —
     that only banks more than the target.
3. **No stop loss, no morning-pricing guard, no LLM exit.** The only exits
   are the $0.50 limit and the 3:01 PM sweep.

### Residual risk noted for sign-off

A spread marked below $0.11 at 3:01 PM stays open, so a violent move in the last
hour could put price at/through its strikes at expiration → assignment risk
and an overnight QQQ share position (American-style, physical settlement).
Proposed default-ON safety tunable, **pending operator approval**:
`ds0_assignment_guard` — at 3:50 PM ET, force-close a still-open spread only
if its short strike is ITM or within `ds0_guard_band` of spot. Clearly-OTM
spreads still expire untouched. This is not a stop loss; it fires only in the
near-the-money tail case the 3:01 PM sweep can miss.

## Economics (stated for the record)

Risk $0.08 to net $0.42 → breakeven hit rate 16% (ignoring the 3:01 PM
partial-profit closes, which improve it). The 75% POP gate is the probability
the *level holds* — not directly the probability the spread reaches $0.50,
which additionally requires price to travel to the strikes. Realized hit rate
vs. the 25% breakeven is the key paper-trading metric; wire DS0 entries into
the prediction ledger so `backfill_prediction_outcomes` can measure it.

## Architecture fit

- New strategy class `DebitSpreads0DTE` in
  `hermes/service1_agent/strategies/ds0.py`. **[impl]** Subclasses
  `CreditSpreadStrategy` for its shared helpers (`_parse_symbol`,
  `_latest_xgb_pred`, `_drop_stale_pred`) exactly as HermesAlpha does,
  overriding both engine hooks — the credit base's POP-target strike walk,
  min-credit floors, and IC Mode A/B completion are unused (inverted for a
  directional debit structure). Reuses `nearest_strike` from `_helpers.py`
  and the batched-quote management pattern.
- **Priority 6**, after HermesAlpha — lowest claim on buying power until
  proven. Registered in the cascading order; the tick pipeline order itself
  is untouched (CLAUDE.md safety rule #3).
- **Tags**: `HERMES_DS0` opens, `HERMES_DS0_CLOSE_<reason>` closes; all
  matchers accept both `_` and `-` forms (safety rule #5). Close reasons:
  `TP` (the $0.50 fill is broker-side, recorded on reconcile), `SWEEP-3PM`,
  `ASSIGN-GUARD` (if approved).
- **Persistence**: opens record `entry_debit` (plumbing already exists in
  `trades.py` / `orm.py`; realized P&L uses `_compute_realized_pnl`'s debit
  branch). DS0 is the first strategy to *open* debit positions — regression
  tests must pin that `entry_debit`, not `entry_credit`, is recorded and
  that fill-price reconciliation handles the debit direction.
- **Approvals**: normal `approval_mode` flow — no autonomous carve-out in
  v1. An unapproved DS0 entry auto-expires after `ds0_approval_ttl`
  (default 15 min) so a stale trigger never executes hours later.
- **Known sharp edge — resolved [impl]**: `delta_implied_p_otm` already
  returns the linear `1-|delta|` path for `dte <= 0` (no √t singularity);
  DS0 passes `dte=0.0` and a regression test pins the behavior.
- **Order-replacement contract [impl]**: the 3 PM sweep on a CLOSING trade
  stamps `strategy_params["replace_broker_order_id"]`; both order sinks
  (`_execute_or_queue`, `_execute_approved_action`) cancel that resting
  close before placing the replacement and **abort if the cancel fails**
  (a failed cancel usually means the TP just filled — placing anyway would
  double-close).
- **Entry TTL [impl]**: DS0 stamps `strategy_params["valid_until"]`
  (now + `ds0_approval_ttl_s`); `_execute_approved_action` expires — never
  executes — an approval acted on past the stamp. Generic opt-in: actions
  without the stamp are unaffected.

## Tunables (all in the `tunables.py` catalog from day one)

| Key | Default | Meaning |
|---|---|---|
| `ds0_enabled` | off | master switch |
| `ds0_open_price` | 0.08 | max entry debit (day-limit price) |
| `ds0_close_price` | 0.50 | resting close limit placed on fill |
| `ds0_pop_target` | 0.75 | min 3m POP for the qualifying level |
| `ds0_width` | 1 | vertical width (operator default, confirm) |
| `ds0_atr_period` | 14 | completed daily bars in the Wilder ATR (v2) |
| `ds0_sweep_min` | 0.11 | sweep floor — marks below it ride to expiry (v2) |
| `ds0_max_lots` | 1 | sizing, max-only like WHEEL (keep low) |
| `ds0_entry_cutoff` | 14:00 ET | no new entries after (operator default, confirm) |
| `ds0_sweep_time` | 15:01 ET | the partial-profit/flatten sweep (v2: 15:01) |
| `ds0_approval_ttl` | 900 s | unapproved entry auto-expiry |
| `ds0_assignment_guard` | on | 3:50 PM near-the-money force-close (pending approval) |
| `ds0_guard_band` | TBD | short-strike-to-spot distance that trips the guard |

## Open defaults awaiting operator confirmation

1. **Width = 1** — assumed; wider at a $0.08 cap means far-OTM strikes that
   rarely see $0.50.
2. **Entry cutoff 14:00 ET** — assumed; with the 3:01 PM sweep, entries
   after 2 PM have almost no runway. No entry-start restriction (sides
   qualify from the open) unless the operator wants to skip the 9:30–10:00
   chop.
3. **`ds0_assignment_guard`** — recommended default-ON; operator may strike
   it.

## Testing posture (safety rule #1 — before any live tick)

Stub-broker tests: POP gate at/below/above 0.75; debit cap respected and
never repriced; close order placed exactly once on fill; both sides open
independently and simultaneously; per-side cooldown; one-shot-per-side-per-
day; 3:01 PM sweep matrix (<0.11 ride / 0.11–0.50 close / ≥0.50 close);
sweep overrides nothing it shouldn't (no morning guard interaction); tag
round-trip both forms; `entry_debit` recorded and debit-branch P&L; approval
TTL expiry; `dte=0` POP engine behavior pinned. Paper instance for an
extended burn-in before the live checkout ever sees DS0.
