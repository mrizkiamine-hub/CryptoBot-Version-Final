-- ==========================================================
-- Schéma relationnel CryptoBot - STEP 2
-- ==========================================================
--  - Schéma : cryptobot
--  - Dimensions :
--      * dim_asset
--      * dim_market
--      * dim_interval
--  - Faits :
--      * fact_market_price  (prix & volumes par marché / intervalle)
--      * fact_macro_price   (prix macro par asset / jour)
-- ==========================================================

CREATE SCHEMA IF NOT EXISTS cryptobot AUTHORIZATION daniel;

-- DIMENSION : ASSET
CREATE TABLE IF NOT EXISTS cryptobot.dim_asset (
    id     SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name   VARCHAR(100)
);

-- DIMENSION : MARKET
CREATE TABLE IF NOT EXISTS cryptobot.dim_market (
    id             SERIAL PRIMARY KEY,
    symbol         VARCHAR(20) UNIQUE NOT NULL,   -- ex : BTCUSDT
    base_asset_id  INTEGER NOT NULL,
    quote_asset_id INTEGER NOT NULL,
    exchange       VARCHAR(50) NOT NULL           -- ex : BINANCE
);

ALTER TABLE cryptobot.dim_market
    ADD CONSTRAINT fk_market_base_asset
        FOREIGN KEY (base_asset_id)
        REFERENCES cryptobot.dim_asset(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT;

ALTER TABLE cryptobot.dim_market
    ADD CONSTRAINT fk_market_quote_asset
        FOREIGN KEY (quote_asset_id)
        REFERENCES cryptobot.dim_asset(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT;

-- DIMENSION : INTERVAL
CREATE TABLE IF NOT EXISTS cryptobot.dim_interval (
    interval_code VARCHAR(10) PRIMARY KEY,        -- ex : '1h', '4h'
    seconds       INTEGER NOT NULL
);

-- FACT : MARKET_PRICE (ex-OHLCV)
CREATE TABLE IF NOT EXISTS cryptobot.fact_market_price (
    id            SERIAL PRIMARY KEY,

    -- clés de contexte
    market_id     INTEGER       NOT NULL,         -- FK → dim_market
    interval_code VARCHAR(10)   NOT NULL,         -- FK → dim_interval
    open_time     TIMESTAMPTZ   NOT NULL,
    close_time    TIMESTAMPTZ   NOT NULL,

    -- prix
    open          NUMERIC(18,8) NOT NULL,
    high          NUMERIC(18,8) NOT NULL,
    low           NUMERIC(18,8) NOT NULL,
    close         NUMERIC(18,8) NOT NULL,

    -- volumes & activité
    volume_base   NUMERIC(28,10),
    quote_volume  NUMERIC(28,10),
    trade_count   INTEGER,

    -- métadonnées / traçabilité
    load_ts       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    source_system VARCHAR(50)   NOT NULL DEFAULT 'BINANCE'
);

ALTER TABLE cryptobot.fact_market_price
    ADD CONSTRAINT fk_fact_market_price_market
        FOREIGN KEY (market_id)
        REFERENCES cryptobot.dim_market(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT;

ALTER TABLE cryptobot.fact_market_price
    ADD CONSTRAINT fk_fact_market_price_interval
        FOREIGN KEY (interval_code)
        REFERENCES cryptobot.dim_interval(interval_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT;

-- FACT : MACRO_PRICE
CREATE TABLE IF NOT EXISTS cryptobot.fact_macro_price (
    id         SERIAL PRIMARY KEY,
    asset_id   INTEGER       NOT NULL,           -- FK → dim_asset
    date       DATE          NOT NULL,
    price_usd  NUMERIC(18,8),
    price_eur  NUMERIC(18,8),
    market_cap NUMERIC(28,2)
);

ALTER TABLE cryptobot.fact_macro_price
    ADD CONSTRAINT fk_fact_macro_price_asset
        FOREIGN KEY (asset_id)
        REFERENCES cryptobot.dim_asset(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT;

-- ==========================================================
-- Optimisations / contraintes
-- ==========================================================

-- dim_interval : cohérence
ALTER TABLE cryptobot.dim_interval
    ADD CONSTRAINT chk_dim_interval_seconds_positive
    CHECK (seconds > 0);

-- fact_market_price : cohérence temporelle + prix
ALTER TABLE cryptobot.fact_market_price
    ADD CONSTRAINT chk_fact_market_price_time_order
    CHECK (open_time < close_time);

ALTER TABLE cryptobot.fact_market_price
    ADD CONSTRAINT chk_fact_market_price_prices_non_negative
    CHECK (open >= 0 AND high >= 0 AND low >= 0 AND close >= 0);

-- Anti-doublons : une bougie par (market_id, interval_code, open_time)
ALTER TABLE cryptobot.fact_market_price
    ADD CONSTRAINT uq_fact_market_price_kline
    UNIQUE (market_id, interval_code, open_time);

-- Index : accélération des requêtes (filtre par marché/intervalle/temps)
CREATE INDEX IF NOT EXISTS ix_fact_market_price_market_interval_time
    ON cryptobot.fact_market_price (market_id, interval_code, open_time DESC);

CREATE INDEX IF NOT EXISTS ix_fact_market_price_open_time
    ON cryptobot.fact_market_price (open_time DESC);

-- fact_macro_price : une valeur par asset/date
ALTER TABLE cryptobot.fact_macro_price
    ADD CONSTRAINT uq_fact_macro_price_asset_date
    UNIQUE (asset_id, date);

CREATE INDEX IF NOT EXISTS ix_fact_macro_price_asset_date
    ON cryptobot.fact_macro_price (asset_id, date DESC);
