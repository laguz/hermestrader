# Contributing to HermesTrader

This guide is for both human contributors and autonomous coding agents
(Jules, Claude, etc.). The conventions here keep the codebase reviewable
without slowing anyone down.

## TL;DR

```bash
# Setup
pip install -r requirements.txt
pip install pytest ruff

# Verify your change
pytest tests -q
ruff check --select E9,F63,F7,F82 hermes tests

# Commit (imperative, type-prefixed)
git commit -m "fix: tag matcher accepts sanitised hyphen form"

# Push & open PR
git push -u origin <branch>
gh pr create --title "..." --body "..."
```

## Before you change anything

1. Read [`ARCHITECTURE.md`](ARCHITECTURE.md) to know which layer you're in.
2. Read [`AGENTS.md`](AGENTS.md) for the safety rules. They apply to humans
   too — this system places real options trades.
3. Search the issue tracker / PR list for prior work on the area.

## Safety rules (load-bearing)

These are mirrored from `AGENTS.md` because they catch people most often:

- **Tag conventions matter.** Hermes tags broker orders `HERMES_<STRAT>`.
  Tradier sanitises `_` → `-`, so the round-tripped form is
  `HERMES-<STRAT>`. Any matcher must accept both.
- **`CascadingEngine.tick` order is contractual.**
  `sync_positions` → `mm.sync_broker_orders` → `reconcile_orphans`
  → `process_management` → `process_entries` → `overseer.propose`.
  Don't reorder.
- **Strategy `PRIORITY` order is contractual.** CS75=1, CS7=2, TT45=3,
  WHEEL=4. Higher-priority strategies consume capacity first. If you add
  a strategy, pick a priority ≥5.
- **Never disable `dry_run` defaults**, and never add a code path that can
  place a live order without `approval_mode` honoring the operator's
  setting.

## Code style

- **Python 3.11+ idioms.** `from __future__ import annotations` everywhere
  there's typing.
- **Type-hinted public functions.** Internal helpers can skip hints if the
  types are obvious from one line of context.
- **Default to no comments.** Add a comment only when the *why* is
  non-obvious (a workaround, a Tradier-specific quirk, a hidden invariant).
  The git history captures *what* changed; comments should capture *why*
  the current shape exists.
- **Module docstrings.** Every module gets a one-paragraph `"""..."""`
  at the top describing what it owns and who calls it.
- **Don't reformat unrelated code.** Focused diffs review faster.
- **No new top-level packages without a clear reason.** The current layout
  is intentional (see `ARCHITECTURE.md`).
- **Timestamps.** Always tz-aware UTC for new code:
  `datetime.now(timezone.utc)`. `datetime.utcnow()` is deprecated.

## Tests

- Lives under `tests/`. One file per area (`test_money_manager_sync.py`,
  `test_market_hours.py`, etc.).
- **Tests must not require a live database.** Use the stub-broker /
  stub-DB pattern in `tests/test_money_manager_sync.py`.
- For tests that need helpers from `hermes/db/models.py` but not the full
  SQLAlchemy stack, import from `hermes/common.py` instead (e.g.
  `OCC_RE`). This keeps the test runnable without `psycopg`.
- Add a regression test before fixing a bug. Order matters: write the
  failing test first, watch it fail, then write the fix, watch it pass.
- Coverage target: 60%+ on touched files. Don't chase 100%.

## Commit messages

Imperative mood, type prefix, one logical change per commit:

- `fix:` — bug fix
- `feat:` — new feature or behaviour
- `refactor:` — code restructuring without behaviour change
- `test:` — test-only change
- `ci:` — CI / workflow change
- `docs:` — documentation only
- `chore:` — housekeeping (dep bumps, formatting)

Subject ≤72 chars. Body explains *why* and references the affected
behaviour. Example:

```
fix: accept sanitised hyphen tag in MoneyManager.sync_broker_orders

Tradier sanitises tags to [A-Za-z0-9-] before persisting, turning
`HERMES_CS75` into `HERMES-CS75`. The matcher was rejecting every
Hermes order on the broker side, so duplicate-entry guard never ran.
Accept both forms.
```

## Pull requests

- One logical change per PR. PRs over ~400 LOC are usually too big.
- PR body should have a **Summary** (what + why), **Changes** (file map),
  and **Test plan** (checklist of how to verify).
- Link any related PRs or issues.
- Wait for CI green before requesting review.

## Reviewing changes

When reviewing (or self-reviewing before merge):

- Does the diff match the PR title?
- Are the safety rules above respected?
- Are there tests for the change?
- Does the change introduce a new dependency? If yes, is it in
  `requirements.txt` *and* `Dockerfile`?
- Does it touch the broker, MoneyManager, or strategies? If yes,
  scrutinise extra carefully.

## Releasing

- `VERSION` is bumped manually before tagging a release.
- The Docker image takes the agent + watcher in one container; see
  `Dockerfile` and `docker-compose.yml`.
- Production deploys are operator-driven — no automation merges to `main`.

## Asking for help

- Architecture questions: re-read `ARCHITECTURE.md`. If still stuck, open
  a draft PR with your question in the description.
- Trading-system questions (why the strategies do what they do):
  the canonical reference is the operator's `soul.md` and the strategy
  docstrings — they explain entry/exit rules.
