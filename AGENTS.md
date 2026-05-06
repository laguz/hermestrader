# Agent Instructions for HermesTrader

This file gives Google Jules and other autonomous coding agents the context
they need to make safe, on-style changes to this repository.

## Project at a glance

HermesTrader is a two-service Python trading system:

- **Service-1 (`hermes/service1_agent/`)** — the agent itself. Runs a
  `CascadingEngine` that ticks every `HERMES_TICK_INTERVAL` seconds and drives
  four credit-spread strategies (CS75, CS7, TT45, Wheel). Writes through a
  `MoneyManager` that enforces buying-power and side-aware capacity limits.
- **Service-2 (`hermes/service2_watcher/`)** — FastAPI C2 (command & control)
  panel for human oversight. Approves trades, edits the agent's "soul"
  doctrine, toggles paper/live mode.

Persistence: TimescaleDB via SQLAlchemy (`hermes/db/models.py`).
Broker: Tradier REST (`hermes/broker/tradier.py`). LLM overseer is
provider-agnostic (`hermes/llm/clients.py`).

## Ground rules

1. **This system places real options orders.** Treat every change to
   `core.py`, `strategies.py`, `tradier.py`, or `MoneyManager` as
   safety-critical. Add a regression test before fixing a bug.
2. **Never disable `dry_run` defaults or add a code path that can place a
   live order without `approval_mode` honoring the operator's setting.**
3. **Tag conventions matter.** Hermes tags broker orders `HERMES_<STRAT>`
   (e.g. `HERMES_CS75`). Tradier sanitises `_` → `-`, so the round-tripped
   form is `HERMES-<STRAT>`. Any new matcher must accept both.
4. **The strategy pipeline is order-sensitive.** `CascadingEngine.tick`
   syncs positions → broker orders → reconciles orphans → manages exits →
   then runs entries in priority order. Don't reorder.
5. **Don't introduce new dependencies casually.** The Dockerfile pins
   `requirements.txt`; both must stay in sync.

## Testing

```bash
pip install -r requirements.txt
pip install pytest ruff
pytest tests -q
```

CI runs the same on every PR (`.github/workflows/ci.yml`). Tests must not
require a live database — use the stub-broker / stub-DB pattern in
`tests/test_money_manager_sync.py`.

## Code style

- Python 3.11+ idioms; type-hinted public functions.
- Default to **no comments**. Add a comment only when the *why* is
  non-obvious (a workaround, a Tradier-specific quirk, a hidden invariant).
- Don't reformat unrelated code; a focused diff reviews faster.
- Follow the existing module structure — don't introduce new top-level
  packages without a clear reason.

## Commit and PR conventions

- Commit messages are imperative and start with a type prefix:
  `fix:`, `feat:`, `refactor:`, `test:`, `ci:`, `docs:`.
- One logical change per PR. The recent bug-fix PRs are good examples
  (see `git log --oneline`).
- Always include a **Test plan** section in the PR body.

## What to skip

- Don't touch `VERSION` unless explicitly asked.
- Don't edit `hermes/scratch/` — those are exploratory scripts.
- Don't migrate the schema (`hermes/db/migrate_*.py`) without operator
  sign-off; data lives in TimescaleDB and migrations are run by hand.

## Useful entry points for understanding the codebase

- `hermes/service1_agent/core.py` — `CascadingEngine`, `MoneyManager`,
  `IronCondorBuilder`, `AbstractStrategy`. Read this first.
- `hermes/service1_agent/strategies.py` — the four concrete strategies.
- `hermes/service1_agent/main.py` — tick loop + config reconciliation.
- `hermes/service2_watcher/api.py` — operator API surface.
