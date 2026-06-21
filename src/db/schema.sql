-- Bharat Ticker — database schema (portable: vanilla Postgres OR TimescaleDB).
-- Apply once:  psql "$DATABASE_URL" -f src/db/schema.sql
--
-- The base tables are plain Postgres and work on ANY Postgres (e.g. Northflank's
-- managed PG addon, Supabase, RDS). The TimescaleDB upgrades (hypertables,
-- compression, retention) are wrapped in a guarded block that DEGRADES to a
-- notice on vanilla Postgres — so the same file applies everywhere. Sub-minute
-- reads use plain-SQL bucketing (queries.py), so they do NOT require Timescale.

-- ── Raw ticks (full-fidelity feed) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ticks (
    time            TIMESTAMPTZ      NOT NULL,
    symbol          TEXT             NOT NULL,
    exchange        TEXT             NOT NULL DEFAULT 'NSE',
    ltp             DOUBLE PRECISION,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          BIGINT,
    value           DOUBLE PRECISION,
    vwap            DOUBLE PRECISION,
    total_buy_qty   BIGINT,
    total_sell_qty  BIGINT,
    source          TEXT,
    source_latency  DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS ticks_symbol_time ON ticks (symbol, exchange, time DESC);

-- ── Intraday 1-second candles (the sub-minute history store) ─────────────────
-- Base resolution is 1s. Coarser bars (10s/1m/…) are derived on read with plain
-- epoch-floor bucketing (portable). PK makes the per-session flush idempotent.
CREATE TABLE IF NOT EXISTS intraday_candles (
    time        TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    exchange    TEXT             NOT NULL DEFAULT 'NSE',
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, exchange, time)
);
CREATE INDEX IF NOT EXISTS intraday_symbol_time ON intraday_candles (symbol, exchange, time DESC);

-- ── EOD + instrument master (daily layer) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS eod_data (
    date          DATE             NOT NULL,
    symbol        TEXT             NOT NULL,
    exchange      TEXT             NOT NULL DEFAULT 'NSE',
    open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION,
    vwap DOUBLE PRECISION, volume BIGINT, value DOUBLE PRECISION,
    delivery_qty BIGINT, delivery_pct DOUBLE PRECISION, trades BIGINT,
    PRIMARY KEY (date, symbol, exchange)
);

CREATE TABLE IF NOT EXISTS instruments (
    symbol TEXT PRIMARY KEY, isin TEXT, name TEXT, exchange TEXT, series TEXT,
    sector TEXT, industry TEXT, lot_size INT, face_value DOUBLE PRECISION,
    listing_date DATE, is_active BOOLEAN DEFAULT TRUE, updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ── TimescaleDB upgrades (optional — skipped cleanly on vanilla Postgres) ────
-- Hypertables + compression + retention bound storage (1s data is GB-scale).
-- The whole block degrades to a NOTICE if the timescaledb extension is not
-- available, so a plain Postgres just keeps the regular tables above.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    PERFORM create_hypertable('ticks', 'time', if_not_exists => TRUE, migrate_data => TRUE);
    PERFORM create_hypertable('intraday_candles', 'time', if_not_exists => TRUE, migrate_data => TRUE);

    ALTER TABLE intraday_candles SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'symbol, exchange');
    PERFORM add_compression_policy('intraday_candles', INTERVAL '2 days');
    PERFORM add_retention_policy('intraday_candles', INTERVAL '60 days');  -- keep 1s ~60d

    ALTER TABLE ticks SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'symbol, exchange');
    PERFORM add_compression_policy('ticks', INTERVAL '2 days');
    PERFORM add_retention_policy('ticks', INTERVAL '14 days');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'TimescaleDB upgrades skipped — running on vanilla Postgres, plain tables in use (%).', SQLERRM;
END $$;
