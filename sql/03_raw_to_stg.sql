-- ==========================================================
-- 03_raw_to_stg.sql  (PATCHED FOR CHUNKS)
-- RAW (json snapshots) -> STG (1 row per kline)
-- Strategy: load ALL Binance snapshots (historical chunks)
-- Idempotent: ON CONFLICT DO NOTHING on (market_symbol, interval_code, open_time)
-- Supports payload formats:
--   - array: [ [open_time, open, high, low, close, volume, close_time, ...], ... ]
--   - object: { ..., "klines": [ [...], ... ] }
-- ==========================================================

INSERT INTO cryptobot.stg_ohlcv (
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
  ingest_ts
)
SELECT
  r.source_system,
  r.market_symbol,
  r.interval_code,

  to_timestamp((k->>0)::bigint / 1000.0) AT TIME ZONE 'UTC' AS open_time,
  to_timestamp((k->>6)::bigint / 1000.0) AT TIME ZONE 'UTC' AS close_time,

  (k->>1)::numeric AS open,
  (k->>2)::numeric AS high,
  (k->>3)::numeric AS low,
  (k->>4)::numeric AS close,

  (k->>5)::numeric  AS volume_base,
  (k->>7)::numeric  AS quote_volume,
  (k->>8)::integer  AS trade_count,

  r.ingest_ts
FROM cryptobot.raw_market_data r
CROSS JOIN LATERAL jsonb_array_elements(
  CASE
    WHEN jsonb_typeof(r.payload) = 'array' THEN r.payload
    ELSE r.payload->'klines'
  END
) AS k
WHERE r.source_system = 'BINANCE'
  AND r.market_symbol = 'BTCUSDT'
  AND r.interval_code = '1h'
ON CONFLICT (market_symbol, interval_code, open_time) DO NOTHING;