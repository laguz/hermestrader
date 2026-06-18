-- =====================================================================
-- [TimescaleDB-Schema] — TimescaleDB addendum (NOT the table catalog).
--
-- The ORM (hermes/db/orm.py) is the single source of truth for every
-- table and column. This file holds ONLY the Postgres/TimescaleDB layer
-- the ORM cannot express:
--   * the two raw `bars_*` price tables (written by the time-series paths,
--     never modelled as ORM classes),
--   * hypertable conversions for the time-partitioned tables,
--   * compression / retention policies,
--   * the `pnl_daily` roll-up view.
--
-- It is applied *after* the ORM tables exist (via `create_all` on SQLite,
-- and via the Alembic baseline's `metadata.create_all` on Postgres), so the
-- `create_hypertable` / `ALTER TABLE … SET (compress…)` statements below all
-- reference tables that are already present. It declares NO column structure
-- for ORM tables — `tests/test_schema_parity.py` enforces that and checks
-- every hypertable-backed ORM table has its `create_hypertable` line here.
--
-- All statements are idempotent so the baseline is safe to re-run.
-- =====================================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- Daily / intraday bars — raw price history for XGBoost features.
-- These are the only data tables NOT backed by an ORM class: they are
-- written and read by the time-series paths, not the declarative models,
-- so their DDL lives here rather than in hermes/db/orm.py.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bars_daily (
    ts          TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    vwap_close  NUMERIC(12,4),
    PRIMARY KEY (symbol, ts)
);
SELECT create_hypertable('bars_daily', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS bars_intraday (
    ts        TIMESTAMPTZ NOT NULL,
    symbol    TEXT NOT NULL,
    open      NUMERIC(12,4),
    high      NUMERIC(12,4),
    low       NUMERIC(12,4),
    close     NUMERIC(12,4),
    volume    BIGINT,
    PRIMARY KEY (symbol, ts)
);
SELECT create_hypertable('bars_intraday', 'ts', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '7 days');

-- ---------------------------------------------------------------------
-- Hypertable conversions for the ORM-owned, time-partitioned tables.
-- Each table is created (with its composite PK over the partitioning
-- column) by the ORM; this turns it into a TimescaleDB hypertable.
-- ---------------------------------------------------------------------
SELECT create_hypertable('trades',         'opened_at',    if_not_exists => TRUE);
SELECT create_hypertable('pending_orders', 'submitted_at', if_not_exists => TRUE);
SELECT create_hypertable('bot_logs',       'ts',           if_not_exists => TRUE);
SELECT create_hypertable('ai_decisions',   'ts',           if_not_exists => TRUE);
SELECT create_hypertable('predictions',    'ts',           if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- Compression / retention policies (TimescaleDB best-practice).
-- ---------------------------------------------------------------------
ALTER TABLE bot_logs       SET (timescaledb.compress, timescaledb.compress_segmentby='strategy_id');
ALTER TABLE bars_intraday  SET (timescaledb.compress, timescaledb.compress_segmentby='symbol');
ALTER TABLE predictions    SET (timescaledb.compress, timescaledb.compress_segmentby='symbol');
SELECT add_compression_policy('bot_logs',      INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('bars_intraday', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_compression_policy('predictions',   INTERVAL '30 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- Daily PnL roll-up for the C2 dashboard.
-- A plain view (not a continuous aggregate): continuous aggregates need
-- time_bucket() on the hypertable's primary time dimension, but `trades`
-- is partitioned by opened_at while PnL must be bucketed by closed_at.
-- The closed-trade volume is small enough that the unmaterialized view is
-- fine; if it ever isn't, switch to a refresh-on-demand materialized view.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW pnl_daily AS
SELECT date_trunc('day', closed_at)::timestamptz AS day,
       strategy_id,
       symbol,
       SUM(pnl) FILTER (WHERE status = 'CLOSED') AS realized_pnl,
       COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_trades
FROM trades
WHERE closed_at IS NOT NULL
GROUP BY 1, strategy_id, symbol;
