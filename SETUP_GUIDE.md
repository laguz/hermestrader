# HermesTrader — Dual-Instance Setup Guide

This repo runs **two** HermesTrader instances side-by-side from a single
codebase: a **paper** instance for development and a **live** instance
that only ever receives commits that have proven themselves in paper.

| Instance | Branch | Folder                          | Watcher port | DB port | Image tag |
|----------|--------|---------------------------------|--------------|---------|-----------|
| paper    | `main` | `~/Git/hermestrader`            | 8081         | 5433    | `latest`  |
| live     | `live` | `~/Git/hermestrader-live`       | 8082         | 5434    | `stable`  |

The two folders are git **worktrees** of the same repo — one `.git`
directory, two checked-out branches. There is no second clone, no
second remote, no manual code-syncing. Promotion is a PR from `main`
into `live`, gated by a paper burn-in window.

---

## One-time setup

### 1. Create the live worktree

From the existing repo root (this folder):

```bash
bash scripts/setup_worktree.sh
```

That script:

- Creates the `live` branch from current `main` (only if it doesn't
  already exist locally or on the remote).
- Adds `~/Git/hermestrader-live` as a worktree pointing at `live`.
- Copies `.env.paper.example` → `.env.paper` here, and
  `.env.live.example` → `.env.live` in the live worktree.
- Creates `~/.hermes-paper/` and `~/.hermes-live/` for persistent
  agent memory.

### 2. Fill in real broker credentials

Open both env files and replace the placeholders:

```bash
$EDITOR ~/Git/hermestrader/.env.paper       # paper-account Tradier keys
$EDITOR ~/Git/hermestrader-live/.env.live   # production-account Tradier keys
```

Both files are gitignored. Don't move them.

### 3. Start paper

```bash
cd ~/Git/hermestrader
docker compose --env-file .env.paper -p hermes-paper up -d --build
```

Verify:

```bash
curl -fsS http://localhost:8081/api/health
# {"ok":true,"service":"hermes-c2"}
curl -fsS http://localhost:8081/api/admin/instance
# {"instance":"hermes-paper","mode":"paper","version":"...","image_tag":"latest"}
```

### 4. Start live (only after paper is happy)

```bash
cd ~/Git/hermestrader-live
docker compose --env-file .env.live -p hermes-live up -d --build
```

Verify:

```bash
curl -fsS http://localhost:8082/api/health
curl -fsS http://localhost:8082/api/admin/instance
# {"instance":"hermes-live","mode":"live", ...}
```

You can now keep both dashboards open: <http://localhost:8081> for
paper, <http://localhost:8082> for live.

### 5. Start the host-side upgrade runner (paper only)

The `/api/admin/upgrade` endpoint queues an upgrade by writing a
marker file into `~/.hermes-paper/`. A small host-side script polls
that marker and runs the actual `git pull && docker compose build &&
up`. Keep it running in tmux / launchd / systemd:

```bash
bash ~/Git/hermestrader/scripts/upgrade_runner.sh paper
```

Do **not** start the live runner unless you've read
`.hermes/skills/promote_to_live.md` and understand the gating model.

---

## Linking the Hermes Gateway

Hermes (the control-plane agent) has authority over the **paper**
instance autonomously and a propose-only relationship with **live**.

### Gateway config

Hermes' `SOUL.md` lives at `.hermes/SOUL.md` in the repo and is
bind-mounted read-only into both containers. Skill files live at
`.hermes/skills/*.md`:

- `maintain_service.md` — paper-only routine ops
- `promote_to_live.md` — burn-in evaluator + PR opener
- `troubleshoot_build.md` — adaptive-thinking subroutine

To wire the Telegram gateway, drop a `~/.hermes/gateway.toml` (host
side, gitignored) per Hermes' standard:

```toml
[telegram]
bot_token = "..."
chat_id   = "..."
```

For the CLI gateway no config is required — invoke skills with:

```bash
hermes run maintain_service --instance paper
hermes run promote_to_live
```

### What Hermes is allowed to do, by instance

| Action                              | paper  | live                   |
|-------------------------------------|--------|------------------------|
| Read logs, query DB                 | yes    | yes                    |
| Pull updates, rebuild               | yes    | no — operator only     |
| Restart services                    | yes    | no — operator only     |
| Open promotion PR (main → live)     | yes    | n/a                    |
| Merge promotion PR                  | n/a    | no — operator only     |
| Place / cancel orders               | no     | no                     |

These boundaries are enforced two ways: by Hermes' `SOUL.md` (norms)
and by the env files (`HERMES_ADMIN_ALLOW_CIDRS` on the live container
should NOT include any CIDR Hermes lives on without the operator
explicitly adding it).

---

## The promotion workflow

```
                 ┌─ paper instance (port 8081) ──┐
   git push ─→  main ─→ Hermes maintains paper ─→ burn-in window
                                                    │
                                      passes? ──── yes
                                                    │
                                      Hermes opens PR: main → live
                                                    │
                                          Luis reviews & merges
                                                    │
                            tag v{X.Y.Z}-live  &  bump HERMES_TAG in .env.live
                                                    │
                                  Operator runs upgrade_runner.sh live
                                                    │
                                       live instance (port 8082)
```

Burn-in criteria (full detail in `promote_to_live.md`):

- Candidate has been HEAD of `main` for ≥ 7 calendar days.
- ≥ 5 trading sessions of clean paper logs.
- ≥ 20 simulated orders placed in the window.
- Paper drawdown < 5% over the window.

If any criterion fails, the PR is not opened — Hermes notifies you
with the specific failed condition.

### Changes to the POP gate

The only ML surface left is `hermes/ml/pop_engine.py` — a deterministic,
chain-only probability-of-profit gate (short-leg delta + S/R protection +
vol regime, with a static regime-weight fallback). It has **no trained
model**. Treat changes to it as safety-critical, live-bound code under the
repo's standing rule: add a regression test before the change, and
paper-validate before promoting to live.

The old XGB/Brier promotion gate — the seven-day `/api/ml/diagnostics`
Brier-parity proof, the `/api/ml/backtest` AUC check, the drift alarms, and
the `promote_to_live.md` automation — was **retired in the Phase-0 strip**
(see [`REBUILD.md`](REBUILD.md)) along with the predictor stack it
calibrated. There is no learned model to calibrate anymore, so that proof no
longer applies.

---

## Common operations

### Watch both instances at once

```bash
docker compose --env-file .env.paper -p hermes-paper logs -f &
docker compose --env-file ~/Git/hermestrader-live/.env.live -p hermes-live logs -f
```

### Pause the agent in one instance without stopping the watcher

```bash
curl -X POST http://localhost:8081/api/agent/pause   # paper
curl -X POST http://localhost:8082/api/agent/pause   # live
```

### Roll live back to a prior tag

```bash
cd ~/Git/hermestrader-live
git fetch --tags
git checkout v{X.Y.Z-1}-live   # the previous tag
# update .env.live HERMES_TAG to match
docker compose --env-file .env.live -p hermes-live up -d --build
```

### Tear everything down

```bash
docker compose --env-file ~/Git/hermestrader/.env.paper      -p hermes-paper down
docker compose --env-file ~/Git/hermestrader-live/.env.live  -p hermes-live  down
```

Volumes (`hermes_db_data`) are namespaced by project, so paper and live
each get their own — neither command touches the other's data.

---

## Troubleshooting

**Both containers fight for port 5432 inside Docker.** They don't —
the in-container port is fixed at 5432, and the *host* publish ports
(5433 paper, 5434 live) are the only ones that need to be unique.

**`docker compose ps` shows the wrong instance.** Compose decides
which project you're talking about from the `-p` flag (or
`COMPOSE_PROJECT_NAME` in the env file). Always pass `-p hermes-paper`
or `-p hermes-live` explicitly when scripting.

**Hermes upgraded paper and now imports are broken.** Open
`scripts/upgrade_runner.sh`'s log (it tees to `/tmp/hermes-upgrade-*.log`).
The `troubleshoot_build` skill walks the diagnostic tree; that's
your first stop before paging anyone.

**Live container won't start because `HERMES_TAG=stable` doesn't
exist yet.** Expected on first install — there's no stable tag until
the first promotion. Either tag the current main as
`stable` manually (`git tag stable && git push --tags`) or set
`HERMES_TAG=latest` in `.env.live` for the very first boot, then
follow the promotion workflow on the next iteration.
