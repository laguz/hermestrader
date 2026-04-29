-- =====================================================================
-- [TimescaleDB-Schema]
-- HermesTrader unified store. Run on a fresh TimescaleDB instance.
-- =====================================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- Strategies registry
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id TEXT PRIMARY KEY,
    priority    INT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Per-strategy watchlists — managed from the Human Watcher UI
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_watchlists (
    strategy_id TEXT NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    symbol      TEXT NOT NULL,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, symbol)
);
CREATE INDEX IF NOT EXISTS idx_strategy_watchlists_sid ON strategy_watchlists(strategy_id);

-- ---------------------------------------------------------------------
-- Trades — one row per opened structure (single leg, vertical, IC, etc.)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    id            BIGSERIAL,
    strategy_id   TEXT NOT NULL REFERENCES strategies(strategy_id),
    symbol        TEXT NOT NULL,
    side_type     TEXT NOT NULL,            -- 'put' | 'call' | 'iron_condor' | 'wheel'
    short_leg     TEXT,
    long_leg      TEXT,
    short_strike  NUMERIC(10,4),
    long_strike   NUMERIC(10,4),
    width         NUMERIC(10,4),
    lots          INT NOT NULL,
    entry_credit  NUMERIC(10,4),
    entry_debit   NUMERIC(10,4),
    expiry        DATE,
    status        TEXT NOT NULL DEFAULT 'OPEN', -- OPEN/CLOSED/MANUAL_ORPHAN/PENDING
    pnl           NUMERIC(12,2),
    opened_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ,
    close_reason  TEXT,
    ai_authored   BOOLEAN NOT NULL DEFAULT FALSE,
    ai_rationale  TEXT,
    PRIMARY KEY (id, opened_at)
);
SELECT create_hypertable('trades', 'opened_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_trades_strategy_status
    ON trades(strategy_id, status, symbol);

-- ---------------------------------------------------------------------
-- Pending orders (deduped against side-aware sizing)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pending_orders (
    id            BIGSERIAL,
    strategy_id   TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL,
    quantity      INT  NOT NULL,
    payload       JSONB NOT NULL,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    status        TEXT NOT NULL DEFAULT 'PENDING',
    PRIMARY KEY (id, submitted_at)
);
SELECT create_hypertable('pending_orders', 'submitted_at', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- Bot logs (Hermes execution trail)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bot_logs (
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    strategy_id  TEXT NOT NULL,
    level        TEXT NOT NULL DEFAULT 'INFO',
    message      TEXT NOT NULL
);
SELECT create_hypertable('bot_logs', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_bot_logs_strategy ON bot_logs(strategy_id, ts DESC);

-- ---------------------------------------------------------------------
-- AI decisions (overseer audit trail)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_decisions (
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    strategy_id  TEXT,
    symbol       TEXT,
    autonomy     TEXT NOT NULL,            -- advisory|enforcing|autonomous
    decision     JSONB NOT NULL
);
SELECT create_hypertable('ai_decisions', 'ts', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- Daily / intraday bars (price history for XGBoost features)
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
-- ML predictions (XGBoost output stream)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predictions (
    ts                TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbol            TEXT NOT NULL,
    predicted_return  DOUBLE PRECISION,
    predicted_price   NUMERIC(12,4),
    spot              NUMERIC(12,4),
    model_tag         TEXT NOT NULL DEFAULT 'xgb-10feat-v1'
);
SELECT create_hypertable('predictions', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_predictions_symbol_ts ON predictions(symbol, ts DESC);

-- ---------------------------------------------------------------------
-- Compression / retention policies (TimescaleDB best-practice)
-- ---------------------------------------------------------------------
ALTER TABLE bot_logs       SET (timescaledb.compress, timescaledb.compress_segmentby='strategy_id');
ALTER TABLE bars_intraday  SET (timescaledb.compress, timescaledb.compress_segmentby='symbol');
ALTER TABLE predictions    SET (timescaledb.compress, timescaledb.compress_segmentby='symbol');
SELECT add_compression_policy('bot_logs',      INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('bars_intraday', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_compression_policy('predictions',   INTERVAL '30 days', if_not_exists => TRUE);

-- ---------------------------------------------------------------------
-- Continuous aggregate — Daily PnL roll-up for the Watcher dashboard
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS pnl_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', closed_at) AS day,
       strategy_id,
       symbol,
       SUM(pnl) FILTER (WHERE status = 'CLOSED') AS realized_pnl,
       COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_trades
FROM trades
WHERE closed_at IS NOT NULL
GROUP BY day, strategy_id, symbol;
