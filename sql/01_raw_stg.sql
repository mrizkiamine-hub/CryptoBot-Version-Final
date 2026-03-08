-- ==========================================================
-- 01_raw_stg.sql
-- Create RAW + STAGING layers (replayable)
-- ==========================================================

CREATE SCHEMA IF NOT EXISTS cryptobot AUTHORIZATION daniel;

-- ------------------------------
-- RAW: store JSON snapshots (1 row = 1 file snapshot)
-- ------------------------------
CREATE TABLE IF NOT EXISTS cryptobot.raw_market_data (
    id            BIGSERIAL PRIMARY KEY,
    ingest_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_system TEXT        NOT NULL DEFAULT 'BINANCE',
    market_symbol VARCHAR(30) NOT NULL,
    interval_code VARCHAR(10) NOT NULL,
    payload       JSONB       NOT NULL
);

-- Ensure new column exists even if table was created earlier
ALTER TABLE cryptobot.raw_market_data
  ADD COLUMN IF NOT EXISTS source_file TEXT;

-- Backfill old rows once (so we can set NOT NULL)
UPDATE cryptobot.raw_market_data
SET source_file = CONCAT('legacy_', id)
WHERE source_file IS NULL;

ALTER TABLE cryptobot.raw_market_data
  ALTER COLUMN source_file SET NOT NULL;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_raw_market_data_ingest_ts
  ON cryptobot.raw_market_data (ingest_ts);

CREATE INDEX IF NOT EXISTS idx_raw_market_data_market_interval
  ON cryptobot.raw_market_data (market_symbol, interval_code);

-- Anti-duplicate (Option B): same file reloaded => ignored
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_market_data_file
  ON cryptobot.raw_market_data (source_system, market_symbol, interval_code, source_file);

-- ------------------------------
-- STAGING: normalized OHLCV rows
-- ------------------------------
CREATE TABLE IF NOT EXISTS cryptobot.stg_ohlcv (
    source_system TEXT        NOT NULL DEFAULT 'BINANCE',
    market_symbol VARCHAR(30) NOT NULL,
    interval_code VARCHAR(10) NOT NULL,
    open_time     TIMESTAMPTZ NOT NULL,
    close_time    TIMESTAMPTZ NOT NULL,
    open          NUMERIC,
    high          NUMERIC,
    low           NUMERIC,
    close         NUMERIC,
    volume_base   NUMERIC,
    quote_volume  NUMERIC,
    trade_count   INTEGER,
    ingest_ts     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_stg_ohlcv
  ON cryptobot.stg_ohlcv (market_symbol, interval_code, open_time);

CREATE INDEX IF NOT EXISTS idx_stg_ohlcv_key
  ON cryptobot.stg_ohlcv (market_symbol, interval_code, open_time);

CREATE INDEX IF NOT EXISTS idx_stg_ohlcv_ingest_ts
  ON cryptobot.stg_ohlcv (ingest_ts);
