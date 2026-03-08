-- ==========================================================
-- 05_star_schema.sql
-- Star schema CryptoBot (DST-friendly)
-- Tables + contraintes + index (simple et lisible)
-- ==========================================================

-- 1) Schéma
CREATE SCHEMA IF NOT EXISTS cryptobot AUTHORIZATION daniel;

-- ==========================================================
-- 2) DIMENSIONS
-- ==========================================================

-- DIM: asset
CREATE TABLE IF NOT EXISTS cryptobot.dim_asset (
    id     SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name   VARCHAR(100)
);

-- DIM: interval
CREATE TABLE IF NOT EXISTS cryptobot.dim_interval (
    interval_code VARCHAR(10) PRIMARY KEY,
    seconds       INTEGER NOT NULL,
    CONSTRAINT chk_dim_interval_seconds_positive CHECK (seconds > 0)
);

-- DIM: market
CREATE TABLE IF NOT EXISTS cryptobot.dim_market (
    id             SERIAL PRIMARY KEY,
    symbol         VARCHAR(20) UNIQUE NOT NULL,  -- ex: BTCUSDT
    base_asset_id  INTEGER NOT NULL,
    quote_asset_id INTEGER NOT NULL,
    exchange       VARCHAR(50) NOT NULL,         -- ex: BINANCE

    CONSTRAINT fk_market_base_asset
      FOREIGN KEY (base_asset_id)
      REFERENCES cryptobot.dim_asset(id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT,

    CONSTRAINT fk_market_quote_asset
      FOREIGN KEY (quote_asset_id)
      REFERENCES cryptobot.dim_asset(id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT
);

-- ==========================================================
-- 3) FACT TABLES
-- ==========================================================

-- FACT: market price (OHLCV)
CREATE TABLE IF NOT EXISTS cryptobot.fact_market_price (
    id            SERIAL PRIMARY KEY,

    -- contexte
    market_id     INTEGER       NOT NULL,
    interval_code VARCHAR(10)   NOT NULL,
    open_time     TIMESTAMPTZ   NOT NULL,
    close_time    TIMESTAMPTZ   NOT NULL,

    -- mesures
    open          NUMERIC(18,8) NOT NULL,
    high          NUMERIC(18,8) NOT NULL,
    low           NUMERIC(18,8) NOT NULL,
    close         NUMERIC(18,8) NOT NULL,

    volume_base   NUMERIC(28,10),
    quote_volume  NUMERIC(28,10),
    trade_count   INTEGER,

    -- traçabilité
    ingest_ts     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    source_system VARCHAR(50)   NOT NULL DEFAULT 'BINANCE',

    -- FK
    CONSTRAINT fk_fact_market_price_market
      FOREIGN KEY (market_id)
      REFERENCES cryptobot.dim_market(id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT,

    CONSTRAINT fk_fact_market_price_interval
      FOREIGN KEY (interval_code)
      REFERENCES cryptobot.dim_interval(interval_code)
      ON UPDATE CASCADE
      ON DELETE RESTRICT,

    -- checks
    CONSTRAINT chk_fact_market_price_time_order
      CHECK (open_time < close_time),

    CONSTRAINT chk_fact_market_price_prices_non_negative
      CHECK (open >= 0 AND high >= 0 AND low >= 0 AND close >= 0),

    -- anti-doublons (clé business)
    CONSTRAINT uq_fact_market_price_kline
      UNIQUE (market_id, interval_code, open_time)
);

-- FACT: macro price
CREATE TABLE IF NOT EXISTS cryptobot.fact_macro_price (
    id         SERIAL PRIMARY KEY,
    asset_id   INTEGER       NOT NULL,
    date       DATE          NOT NULL,
    price_usd  NUMERIC(18,8),
    price_eur  NUMERIC(18,8),
    market_cap NUMERIC(28,2),

    CONSTRAINT fk_fact_macro_price_asset
      FOREIGN KEY (asset_id)
      REFERENCES cryptobot.dim_asset(id)
      ON UPDATE CASCADE
      ON DELETE RESTRICT,

    CONSTRAINT uq_fact_macro_price_asset_date
      UNIQUE (asset_id, date)
);

-- ==========================================================
-- 4) INDEXES
-- ==========================================================

CREATE INDEX IF NOT EXISTS ix_fact_market_price_market_interval_time
  ON cryptobot.fact_market_price (market_id, interval_code, open_time DESC);

CREATE INDEX IF NOT EXISTS ix_fact_market_price_open_time
  ON cryptobot.fact_market_price (open_time DESC);

CREATE INDEX IF NOT EXISTS ix_fact_macro_price_asset_date
  ON cryptobot.fact_macro_price (asset_id, date DESC);
