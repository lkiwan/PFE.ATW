-- ATW database schema
-- Runs automatically on first Postgres boot (empty data dir).

CREATE TABLE IF NOT EXISTS bourse_daily (
    seance          DATE        PRIMARY KEY,
    instrument      TEXT,
    ticker          TEXT,
    ouverture       NUMERIC,
    dernier_cours   NUMERIC,
    plus_haut       NUMERIC,
    plus_bas        NUMERIC,
    nb_titres       NUMERIC,
    volume          NUMERIC,
    nb_transactions INTEGER,
    capitalisation  NUMERIC
);

CREATE TABLE IF NOT EXISTS macro_morocco (
    date                   DATE PRIMARY KEY,
    frequency_tag          TEXT,
    gdp_growth_pct         NUMERIC,
    current_account_pct_gdp NUMERIC,
    public_debt_pct_gdp    NUMERIC,
    inflation_cpi_pct      NUMERIC,
    eur_mad                NUMERIC,
    usd_mad                NUMERIC,
    brent_usd              NUMERIC,
    wheat_usd              NUMERIC,
    gold_usd               NUMERIC,
    vix                    NUMERIC,
    sp500_close            NUMERIC,
    em_close               NUMERIC,
    us10y_yield            NUMERIC,
    masi_close             NUMERIC,
    gdp_ci                 NUMERIC,
    gdp_sn                 NUMERIC,
    gdp_cm                 NUMERIC,
    gdp_tn                 NUMERIC,
    macro_momentum         NUMERIC,
    fx_pressure_eur        NUMERIC,
    global_risk_flag       SMALLINT
);

CREATE TABLE IF NOT EXISTS news (
    id              BIGSERIAL PRIMARY KEY,
    date            TIMESTAMPTZ,
    ticker          TEXT,
    title           TEXT,
    source          TEXT,
    url             TEXT UNIQUE,
    full_content    TEXT,
    query_source    TEXT,
    signal_score    INTEGER,
    is_atw_core     SMALLINT,
    scraping_date   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_news_ticker_date ON news (ticker, date DESC);

CREATE TABLE IF NOT EXISTS fondamental_snapshot (
    id                BIGSERIAL PRIMARY KEY,
    symbol            TEXT NOT NULL,
    scrape_timestamp  TIMESTAMPTZ NOT NULL,
    payload           JSONB NOT NULL,
    UNIQUE (symbol, scrape_timestamp)
);

CREATE TABLE IF NOT EXISTS fondamental_yearly (
    symbol  TEXT    NOT NULL,
    year    INTEGER NOT NULL,
    metric  TEXT    NOT NULL,
    value   NUMERIC,
    PRIMARY KEY (symbol, year, metric)
);
