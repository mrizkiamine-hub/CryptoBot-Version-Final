-- ==========================================
-- 02_seed_cryptobot_enriched.sql
-- Seed enrichi : assets + intervals + markets
-- ==========================================

BEGIN;

-- ------------------------------
-- DIM ASSET (actifs)
-- ------------------------------
INSERT INTO cryptobot.dim_asset (symbol, name)
VALUES
  ('BTC','Bitcoin'),
  ('ETH','Ethereum'),
  ('USDT','Tether'),
  ('BNB','Binance Coin'),
  ('SOL','Solana'),
  ('XRP','Ripple'),
  ('ADA','Cardano'),
  ('DOGE','Dogecoin'),
  ('AVAX','Avalanche'),
  ('DOT','Polkadot'),
  ('LINK','Chainlink'),
  ('LTC','Litecoin'),
  ('MATIC','Polygon'),
  ('ATOM','Cosmos'),
  ('TRX','Tron'),
  ('USDC','USD Coin'),
  ('DAI','Dai')
ON CONFLICT (symbol) DO NOTHING;

-- ------------------------------
-- DIM INTERVAL (granularités)
-- ------------------------------
INSERT INTO cryptobot.dim_interval (interval_code, seconds)
VALUES
  ('15m',  900),
  ('1h',  3600),
  ('4h', 14400),
  ('1d', 86400)
ON CONFLICT (interval_code) DO NOTHING;

-- ------------------------------
-- DIM MARKET (paires)
-- ------------------------------
-- Stratégie simple : majoritairement XXXUSDT + quelques cross BTC/ETH
WITH a AS (
  SELECT id, symbol FROM cryptobot.dim_asset
)
INSERT INTO cryptobot.dim_market (symbol, base_asset_id, quote_asset_id, exchange)
VALUES
  -- vs USDT (les plus utiles)
  ('BTCUSDT', (SELECT id FROM a WHERE symbol='BTC'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('ETHUSDT', (SELECT id FROM a WHERE symbol='ETH'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('BNBUSDT', (SELECT id FROM a WHERE symbol='BNB'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('SOLUSDT', (SELECT id FROM a WHERE symbol='SOL'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('XRPUSDT', (SELECT id FROM a WHERE symbol='XRP'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('ADAUSDT', (SELECT id FROM a WHERE symbol='ADA'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('DOGEUSDT',(SELECT id FROM a WHERE symbol='DOGE'),(SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('AVAXUSDT',(SELECT id FROM a WHERE symbol='AVAX'),(SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('DOTUSDT', (SELECT id FROM a WHERE symbol='DOT'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('LINKUSDT',(SELECT id FROM a WHERE symbol='LINK'),(SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('LTCUSDT', (SELECT id FROM a WHERE symbol='LTC'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('MATICUSDT',(SELECT id FROM a WHERE symbol='MATIC'),(SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('ATOMUSDT',(SELECT id FROM a WHERE symbol='ATOM'),(SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('TRXUSDT', (SELECT id FROM a WHERE symbol='TRX'), (SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),
  ('USDCUSDT',(SELECT id FROM a WHERE symbol='USDC'),(SELECT id FROM a WHERE symbol='USDT'), 'BINANCE'),

  -- quelques cross (optionnel mais intéressant)
  ('ETHBTC',  (SELECT id FROM a WHERE symbol='ETH'), (SELECT id FROM a WHERE symbol='BTC'),  'BINANCE'),
  ('SOLBTC',  (SELECT id FROM a WHERE symbol='SOL'), (SELECT id FROM a WHERE symbol='BTC'),  'BINANCE'),
  ('BNBBTC',  (SELECT id FROM a WHERE symbol='BNB'), (SELECT id FROM a WHERE symbol='BTC'),  'BINANCE')
ON CONFLICT (symbol) DO NOTHING;

COMMIT;
