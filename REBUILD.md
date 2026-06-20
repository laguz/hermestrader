# HermesTrader — Greenfield Rebuild Plan

> A hypothetical "if we built it again" design, written to answer one question:
> **what would we build first, and what would each later piece have to earn
> before it gets written?** This is a design doc, not a refactor mandate — the
> live system already contains all of this and most of it is intentional. The
> point is sequencing: same destination, but the dormant/diagnostic surface
> never gets written ahead of its use.
>
> Read [`ARCHITECTURE.md`](ARCHITECTURE.md) first — this assumes its vocabulary.

## Thesis

The current system is ~25k LOC across five strategies, a two-mode overseer, and
a 17-module ML subsystem (~4,300 LOC). The audit
([`ml-subsystem-decision-map`](.)) shows the decision-critical ML core is only
`pop_engine` + the XGB stack + `regime_weights` (~2,400 LOC); the other ~1,900
LOC is gated-off tuners, observability, or a dormant stacking layer. That's the
signature of **capability built ahead of use** — none of it unsafe, but a lot of
it written before a live P&L signal asked for it.

A rebuild keeps the safety-critical *spine* identical (it's the hard part and
it's right) and changes only the **order of construction**: ship the smallest
thing that can place a safe order, then admit each additional strategy / review
mode / ML module **only when it clears an explicit promotion gate.**

## What stays identical (do not re-litigate)

These are the load-bearing decisions. Rebuild them the same way, day one:

- **Single-writer invariant**, enforced by a source-scanning test
  ([`test_writer_ownership.py`](tests/test_writer_ownership.py)). Service-1 is
  the only writer; the watcher enqueues `operator_commands`.
- **Event sourcing** — `event_ledger` projecting the read models in one
  transaction, with replay-parity + crash-recovery guards
  ([`test_event_replay_parity.py`](tests/test_event_replay_parity.py)).
- **ORM as the single schema source**; `schema.sql` only the TimescaleDB addendum.
- **Order-sensitive tick pipeline** and the `dry_run` / `approval_mode` defaults.
- **Layering rule** ("never reach more than one layer up/down") and the
  **provider-agnostic LLM** seam.
- **The MoneyManager** — true BP, side-aware capacity, scaling. This is shared
  infrastructure, not a strategy; it ships in the core.

## Phase 0 — the minimal trading core

The smallest system that can place a real, safe options order and be operated.

| Component | Why it's in the core |
|-----------|----------------------|
| `CascadingEngine` spine (`core.py`) | The tick pipeline; one strategy is still a pipeline. |
| **CS75** (only strategy) | Highest priority (PRIORITY=1), the longest-DTE / most-vetted recipe. |
| `MoneyManager` | Capacity/sizing is not optional even for one strategy. |
| **overseer-single** (advisory → enforcing) | One LLM veto/modify call. *Strictly off the correctness path* — CS75's rules alone must produce a complete, safe decision; the overseer only vetoes or trims. |
| TradierBroker + MockBroker | Live + test broker. |
| Event store, ORM, repositories | The persistence spine above. |
| Service-2 watcher (read + `operator_commands`) | Approvals, paper/live toggle, P&L. |
| POP gate **chain-only** (`pop_engine` with `model=None`) | `pop_engine.py:125` already degrades to chain-only POP with no XGB. The gate ships; the ML model does not. |

Explicitly **not** in Phase 0: CS7, TT45, Wheel, HermesAlpha, the committee
overseer, XGB/`AsyncXGBPredictor`, `regime_weights`, the bandit/exit-policy
tuners, and all of Tier-3 ML.

## The promotion gate

Nothing below gets written until it clears **all** of:

1. **A demand signal** — a live-P&L or operator need that the current core
   *cannot* satisfy. ("It would be nice" is not a signal.)
2. **A measurable success criterion** stated *before* building, with a kill
   condition. (e.g. "Strategy X must beat CS75-only Sharpe over N paper weeks or
   it's reverted.")
3. **It degrades safely** — its absence/failure must not break correctness, the
   way chain-only POP survives a missing XGB model.
4. **A regression test lands with it** (the repo's "test before the bug" rule,
   applied to features).

If a candidate can't state #1 and #2 up front, that's the tell it's speculative
— defer it.

## Earned-promotion criteria, per deferred piece

Ordered roughly by how cheaply each earns its keep.

### Strategies

| Strategy | Promote when… |
|----------|---------------|
| **CS7** (7 DTE) | CS75 is filling reliably and there's a demonstrated short-DTE opportunity CS75's 39–45 DTE window misses. Cross-strategy position isolation is intentional ([`cross-strategy-isolation`](.)) — design it in from this point, not bolted on. |
| **TT45** (16Δ, 30–60 DTE) | A distinct vol/term-structure regime shows edge the credit-spread strategies don't capture. Shares `_credit_spread_base` machinery, so cost is low once CS7 exists. |
| **Wheel** | You actually intend to take assignment / run covered calls — it's a different lifecycle (put→assignment→call), not another spread. Don't build the assignment-handling code until that's the plan. |
| **HermesAlpha** | Last. It's the rule-free strategy: the overseer *originates* an intent. It depends on (a) a trusted overseer and (b) `autonomy=='autonomous'` being a mode you'll actually run. Until both are true it's a research toy in the live path. |

### Overseer

| Piece | Promote when… |
|-------|---------------|
| **overseer-committee** | Single-LLM review shows a measurable failure mode (systematic blind spot) that a Macro + Strategy + Risk-Officer split provably fixes. The mode split is intentional in the live system ([`overseer-mode-split`](.)) — but in a rebuild it earns its second mode, it doesn't start with it. Committee already falls back to single on failure; preserve that. |
| **proposers / governance / worker** collaborators | These are decompositions of a spine that grew. In a rebuild, keep `overseer.py` monolithic until it genuinely hurts, then split along the *same* context-object seam (`OverseerContext`). Don't pre-scaffold. |

### ML (sequenced straight from the audit tiers)

| Module(s) | Promote when… |
|-----------|---------------|
| **XGB stack** (`xgb_features`, `AsyncXGBPredictor`, predictor internals) | Chain-only POP is live and you can show that an XGB-*refined* POP would have changed real entry decisions for the better. It refines POP, it's never load-bearing — keep the `model is None` fallback as the contract. |
| **`regime_weights`** | XGB exists and regime-conditioning the POP lookup beats flat weighting on real data. |
| **`bandit` / `exit_policy`** (Tier 2, default-off tuners) | An operator wants automated knob/exit tuning *and* there's offline evidence it helps. They ship default-off — same as today. |
| **`drift` / `ledger`** (Tier 3, observability) | You're running enough ML in production to need monitoring. Pure read-side; safe to add anytime, but pointless before the models exist. |
| **`backtester` / `attribution`** (Tier 3, UI-unreferenced) | Only if the operator UI actually surfaces them. Today the shipped JS bundle references neither — in a rebuild, **don't write an endpoint with no consumer.** |
| **`meta_learner`** (dormant) | Only alongside a *scheduled* `nightly_calibrate` (cron/CI). Today it's inert because nothing writes its calibrator settings. A rebuild either wires the schedule or doesn't write the module — never ships it dormant. |

## What this buys

- **The ~1,900 LOC of Tier-2/Tier-3/dormant ML never exists** until something
  consumes it — no UI-less endpoints, no inert stacking layer.
- Every component in the tree has a recorded reason it's there (its promotion
  criterion) and a kill condition.
- The safety spine is unchanged, so none of the above trades away correctness,
  recoverability, or the single-writer guarantee.

## What this plan is *not*

A license to delete the live system down to CS75. Everything here exists today
for reasons, most of them intentional and recorded in memory. This is the
construction order you'd *wish* you'd followed — useful as a lens for "should
this next thing be built yet?", not as a teardown script.
