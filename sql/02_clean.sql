-- ==========================================
-- 02_clean.sql
-- CLEAN layer : dedup + flags qualité
-- ==========================================

BEGIN;
CREATE SCHEMA IF NOT EXISTS cryptobot;

CREATE TABLE IF NOT EXISTS cryptobot.clean_ohlcv (
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

  dq_is_valid   BOOLEAN     NOT NULL DEFAULT TRUE,
  dq_notes      TEXT,

  ingest_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT uq_clean_ohlcv UNIQUE (market_symbol, interval_code, open_time)
);

CREATE INDEX IF NOT EXISTS idx_clean_ohlcv_key
  ON cryptobot.clean_ohlcv (market_symbol, interval_code, open_time);

COMMIT;
