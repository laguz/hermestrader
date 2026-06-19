# CLAUDE.md — HermesTrader

Guidance for Claude Code working in this repo. Keep this file short; the
detailed maps live in [`ARCHITECTURE.md`](ARCHITECTURE.md) (read first) and
[`AGENTS.md`](AGENTS.md) (conventions, MCP, entry points).

## What this is

A two-service options-trading system on a TimescaleDB backbone:

- **Service-1 — Hermes Agent** (`hermes/service1_agent/`): event-driven
  `CascadingEngine` that ticks five priority-ordered strategies
  (CS75 → CS7 → TT45 → Wheel → HermesAlpha), sized by `MoneyManager`, reviewed
  by the LLM `HermesOverseer`. **The only writer.** Places real broker orders.
- **Service-2 — Watcher** (`hermes/service2_watcher/`): read-only FastAPI
  operator panel (approvals, soul doctrine, paper/live toggle, P&L).

Persistence: TimescaleDB via SQLAlchemy (`hermes/db/`). Broker: Tradier REST
(`hermes/broker/tradier.py`). LLM overseer is provider-agnostic
(`hermes/llm/`).

## Safety rules (this code places real money orders)

1. Treat `core.py`, `strategies/`, `tradier.py`, and `MoneyManager` as
   safety-critical. **Add a regression test before fixing a bug** in them.
2. Never weaken `dry_run` defaults or add a path that places a live order
   without honoring the operator's `approval_mode` setting.
3. The tick pipeline is **order-sensitive**: sync positions → sync broker
   orders → reconcile orphans → manage exits → entries in priority order →
   overseer proposals. Don't reorder it.
4. **Single-writer invariant**: only Service-1 writes the event-sourced read
   models, ledger, and time series. Service-2 is read-only except the four
   deliberately-shared tables. `tests/test_writer_ownership.py` enforces this.
5. Order tags round-trip as both `HERMES_<STRAT>` and `HERMES-<STRAT>`
   (Tradier rewrites `_`→`-`). Any new matcher must accept both forms.

## Working style

- Python 3.11+, type-hinted public functions. Match the surrounding code's
  idioms, naming, and comment density.
- **Default to no comments**; add one only when the *why* is non-obvious (a
  workaround, a Tradier quirk, a hidden invariant).
- Keep diffs focused — don't reformat unrelated code or add top-level packages
  without a clear reason.
- The ORM (`hermes/db/orm.py`) is the single source of truth for schema;
  `schema.sql` is only the TimescaleDB addendum. Don't add a second catalog.

## Testing

```bash
pip install -r requirements.txt && pip install pytest ruff
pytest tests -q
ruff check --select E9,F63,F7,F82 hermes tests
```

Most tests use the stub-broker / stub-DB pattern and need no database
(`tests/test_money_manager_sync.py`). DB-backed tests provision a throwaway
Timescale DB and **skip** when no server is reachable — they never fail for
lack of a DB. CI runs the full suite against a Timescale service container.

## Commits & PRs

- Imperative messages with a type prefix: `fix:`, `feat:`, `refactor:`,
  `test:`, `ci:`, `docs:`. One logical change per PR; include a **Test plan**.
- Verify the current branch before committing — this repo is actively
  multi-branch and checkout can move between turns.

## Don't touch

- `VERSION` (unless asked), `hermes/scratch/` and `scratch/` (exploratory),
  and schema migrations without operator sign-off (data is live in Timescale,
  migrations run by hand).
