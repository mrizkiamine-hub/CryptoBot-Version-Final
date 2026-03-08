-- 04_stg_to_clean.sql
-- STG -> CLEAN avec règles DQ simples, idempotent

BEGIN;

WITH base AS (
  SELECT
    source_system,
    market_symbol,
    interval_code,
    open_time,
    close_time,
    open, high, low, close,
    volume_base,
    quote_volume,
    trade_count,
    ingest_ts
  FROM cryptobot.stg_ohlcv
),
dq AS (
  SELECT
    b.*,
    CASE
      WHEN open_time IS NULL OR close_time IS NULL THEN false
      WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL THEN false
      WHEN high < low THEN false
      WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN false
      WHEN volume_base < 0 OR quote_volume < 0 THEN false
      ELSE true
    END AS dq_is_valid,
    CASE
      WHEN open_time IS NULL OR close_time IS NULL THEN 'missing_time'
      WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL THEN 'missing_price'
      WHEN high < low THEN 'high_lt_low'
      WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 'non_positive_price'
      WHEN volume_base < 0 OR quote_volume < 0 THEN 'negative_volume'
      ELSE NULL
    END AS dq_notes
  FROM base b
)
INSERT INTO cryptobot.clean_ohlcv (
  source_system,
  market_symbol,
  interval_code,
  open_time,
  close_time,
  open,
  high,
  low,
  close,
  volume_base,
  quote_volume,
  trade_count,
  dq_is_valid,
  dq_notes,
  ingest_ts
)
SELECT
  source_system,
  market_symbol,
  interval_code,
  open_time,
  close_time,
  open,
  high,
  low,
  close,
  volume_base,
  quote_volume,
  trade_count,
  dq_is_valid,
  dq_notes,
  ingest_ts
FROM dq d
WHERE NOT EXISTS (
  SELECT 1
  FROM cryptobot.clean_ohlcv c
  WHERE c.market_symbol = d.market_symbol
    AND c.interval_code = d.interval_code
    AND c.open_time     = d.open_time
);

COMMIT;

