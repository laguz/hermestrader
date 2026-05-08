---
name: promote_to_live
description: Gate the main → live merge on a paper-instance burn-in window. Opens a PR; never merges without operator approval.
triggers: [scheduled:daily-21:00-ET, manual]
authority: read-paper, propose-live
---

# Promote main → live

The `live` branch is what runs against real money. Code only earns
its way onto `live` after surviving an explicit burn-in period on
the paper instance. This skill measures the burn-in and, if it
passes, opens the promotion PR. **You never merge it.** Luis does.

## Pre-flight checks (abort the whole skill if any fails)

1. The `paper` instance is currently running and healthy.
2. The `live` instance is currently running and healthy. (We are not
   promoting *into* an outage.)
3. The US equity market is currently **closed**. Promotions during
   market hours are forbidden — even if Luis approves, refuse and
   make him override explicitly.
4. There is a positive commit delta: `git rev-list --count live..main`
   returns at least 1.

## Burn-in window

The candidate is whatever commit `main` currently points at. It must
satisfy ALL of:

- Has been the HEAD of `main` for **at least 7 calendar days**.
- The paper container has been running this commit (or a descendant)
  for **at least 5 trading sessions** with no unhandled exceptions
  in the watcher or agent logs.
- The paper instance has placed at least **20 simulated orders**
  during the window — a quiet week doesn't count as evidence of
  stability.
- Aggregate paper P&L over the window is **not catastrophically
  negative** (drawdown < 5% of starting paper buying power). A
  losing week is not a blocker, but a -15% week is.

You compute the burn-in metrics by querying the paper Postgres
directly (port 5433, db `hermes`, schema in
`hermes/db/schema.sql`). Useful tables: `logs`, `orders`,
`closed_trades`. Be aware that paper and live have **separate**
databases — never query the live DB to reason about paper.

## If burn-in passes

Open a PR from `main` into `live`:

```bash
cd ~/Git/hermestrader
git fetch --all
git checkout live && git pull
git merge --no-ff --no-commit main
# Don't commit yet — leave the merge staged so the PR description
# can include the burn-in report.
```

The PR description must include:

- Commit range being promoted (`git log live..main --oneline`).
- Burn-in window summary: dates, paper-session count, order count,
  paper P&L.
- Any warnings raised but not blockers (e.g. "agent logged 2
  retried HTTP 503s from Tradier — within tolerance").
- Suggested new tag name (`v{X.Y.Z}-live`) — bump from
  `cat VERSION`.

Notify Luis via the Telegram gateway with the PR link and a one-line
summary. **Stop here.** Do not merge.

## If burn-in fails

Notify Luis with the specific failed condition. Suggest a re-check
date (e.g. "burn-in window restarts; re-evaluate 2026-05-15"). Do
not open a PR — opening a PR for a failed burn-in trains everyone to
ignore your PRs.

## After Luis merges the PR

Once the merge lands on `live`:

1. Tag the merge commit: `git tag v{X.Y.Z}-live && git push --tags`.
2. Update `.env.live`'s `HERMES_TAG` line to the new tag (this is a
   manual edit on the live worktree — you propose the diff, Luis
   applies it).
3. Run the `maintain_service` skill against the **live** worktree
   with `--operator-approved-live=true` to perform the rebuild. This
   is the only path that grants you write authority on live.
