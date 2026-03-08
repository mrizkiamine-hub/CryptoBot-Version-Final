-- ==========================================================
-- 06_clean_to_fact_market_price.sql
-- Load clean_ohlcv -> fact_market_price
-- ==========================================================

INSERT INTO cryptobot.fact_market_price (
  market_id,
  interval_code,
  open_time,
  close_time,
  open, high, low, close,
  volume_base,
  quote_volume,
  trade_count,
  ingest_ts,
  source_system
)
SELECT
  m.id AS market_id,
  c.interval_code,
  c.open_time,
  c.close_time,
  c.open, c.high, c.low, c.close,
  c.volume_base,
  c.quote_volume,
  c.trade_count,
  c.ingest_ts,
  c.source_system
FROM cryptobot.clean_ohlcv c
JOIN cryptobot.dim_market m
  ON m.symbol = c.market_symbol
 AND m.exchange = c.source_system
WHERE c.dq_is_valid = true
ON CONFLICT (market_id, interval_code, open_time)
DO NOTHING;
