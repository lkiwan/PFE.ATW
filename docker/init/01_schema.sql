-- ATW database schema
-- Runs automatically on first Postgres boot (empty data dir).
CREATE TABLE IF NOT EXISTS bourse_daily (
    seance DATE PRIMARY KEY,
    instrument TEXT,
    ticker TEXT,
    ouverture NUMERIC,
    dernier_cours NUMERIC,
    plus_haut NUMERIC,
    plus_bas NUMERIC,
    nb_titres NUMERIC,
    volume NUMERIC,
    nb_transactions INTEGER,
    capitalisation NUMERIC
);
CREATE TABLE IF NOT EXISTS macro_morocco (
    date DATE PRIMARY KEY,
    frequency_tag TEXT,
    gdp_growth_pct NUMERIC,
    current_account_pct_gdp NUMERIC,
    public_debt_pct_gdp NUMERIC,
    inflation_cpi_pct NUMERIC,
    eur_mad NUMERIC,
    usd_mad NUMERIC,
    brent_usd NUMERIC,
    wheat_usd NUMERIC,
    gold_usd NUMERIC,
    vix NUMERIC,
    sp500_close NUMERIC,
    em_close NUMERIC,
    us10y_yield NUMERIC,
    masi_close NUMERIC,
    gdp_ci NUMERIC,
    gdp_sn NUMERIC,
    gdp_cm NUMERIC,
    gdp_tn NUMERIC,
    macro_momentum NUMERIC,
    fx_pressure_eur NUMERIC,
    global_risk_flag SMALLINT
);
CREATE TABLE IF NOT EXISTS news (
    id BIGSERIAL PRIMARY KEY,
    date TIMESTAMPTZ,
    ticker TEXT,
    title TEXT,
    source TEXT,
    url TEXT UNIQUE,
    full_content TEXT,
    query_source TEXT,
    signal_score INTEGER,
    is_atw_core SMALLINT,
    scraping_date TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_news_ticker_date ON news (ticker, date DESC);
CREATE TABLE IF NOT EXISTS fondamental_snapshot (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    scrape_timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (symbol, scrape_timestamp)
);
CREATE TABLE IF NOT EXISTS fondamental_yearly (
    symbol TEXT NOT NULL,
    year INTEGER NOT NULL,
    metric TEXT NOT NULL,
    value NUMERIC,
    PRIMARY KEY (symbol, year, metric)
);
CREATE TABLE IF NOT EXISTS bourse_intraday (
    snapshot_ts      TIMESTAMPTZ PRIMARY KEY,
    cotation_ts      TIMESTAMPTZ,
    ticker           TEXT NOT NULL DEFAULT 'ATW',
    market_status    TEXT,
    last_price       NUMERIC,
    open             NUMERIC,
    high             NUMERIC,
    low              NUMERIC,
    prev_close       NUMERIC,
    variation_pct    NUMERIC,
    shares_traded    NUMERIC,
    value_traded_mad NUMERIC,
    num_trades       INTEGER,
    market_cap       NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_intraday_ticker_ts
    ON bourse_intraday (ticker, snapshot_ts DESC);
CREATE TABLE IF NOT EXISTS technicals_snapshot (
    id           BIGSERIAL PRIMARY KEY,
    symbol       TEXT NOT NULL DEFAULT 'ATW',
    computed_at  TIMESTAMPTZ NOT NULL,
    as_of_date   DATE,
    payload      JSONB NOT NULL,
    UNIQUE (symbol, computed_at)
);
CREATE INDEX IF NOT EXISTS idx_tech_symbol_date
    ON technicals_snapshot (symbol, as_of_date DESC);

CREATE TABLE IF NOT EXISTS bourse_orderbook (
    snapshot_ts  TIMESTAMPTZ PRIMARY KEY,
    ticker       TEXT NOT NULL DEFAULT 'ATW',
    bid1_orders  NUMERIC,
    bid2_orders  NUMERIC,
    bid3_orders  NUMERIC,
    bid4_orders  NUMERIC,
    bid5_orders  NUMERIC,
    bid1_qty     NUMERIC,
    bid2_qty     NUMERIC,
    bid3_qty     NUMERIC,
    bid4_qty     NUMERIC,
    bid5_qty     NUMERIC,
    bid1_price   NUMERIC,
    bid2_price   NUMERIC,
    bid3_price   NUMERIC,
    bid4_price   NUMERIC,
    bid5_price   NUMERIC,
    ask1_price   NUMERIC,
    ask2_price   NUMERIC,
    ask3_price   NUMERIC,
    ask4_price   NUMERIC,
    ask5_price   NUMERIC,
    ask1_qty     NUMERIC,
    ask2_qty     NUMERIC,
    ask3_qty     NUMERIC,
    ask4_qty     NUMERIC,
    ask5_qty     NUMERIC,
    ask1_orders  NUMERIC,
    ask2_orders  NUMERIC,
    ask3_orders  NUMERIC,
    ask4_orders  NUMERIC,
    ask5_orders  NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_orderbook_ticker_ts
    ON bourse_orderbook (ticker, snapshot_ts DESC);
