# Whale Project (ATW Intelligence Suite)

Whale is an end-to-end intelligence pipeline for **Attijariwafa Bank (ATW)**: market data, macro context, multi-source news, valuation models, and AI synthesis agents.

## What this project includes

- **Market pipeline** (`scrapers/atw_realtime_scraper.py`): intraday snapshots + official EOD consolidation from Casablanca Bourse.
- **Macro pipeline** (`scrapers/atw_macro_collector.py`): Morocco macro + global risk factors (WB/IMF/yfinance/investing.com).
- **Fundamentals pipeline** (`scrapers/fondamental_scraper.py`): MarketScreener periodic fundamentals (monthly cadence).
- **News pipeline** (`news_crawler/ATW_*_news.py`): Boursenews, Medias24, L’Economiste, Aujourd’hui, MarketScreener, Google News.
- **Valuation engine** (`models/fundamental_models.py`): DCF, DDM, Graham, Relative, Monte Carlo.
- **AI agents**
  - `agents/agent_news.py`: live DDGS + Groq synthesis brief (terminal output).
  - `agents/agent_analyse.py`: holistic cited BUY/HOLD/SELL report from local data files.
- **Database layer** (`database/db.py`): unified PostgreSQL read/write adapter for all pipelines.
- **Automation scheduler** (`autorun/scheduler.py`): Windows-friendly cron replacement.

## Repository map

```text
PFE.01/
├── agents/                  # AI agents (news + analyse)
├── autorun/                 # long-running scheduler
├── database/                # AtwDatabase adapter
├── docker/
│   └── init/01_schema.sql   # PostgreSQL schema bootstrap
├── models/                  # valuation models
├── news_crawler/            # source-specific news collectors
├── scripts/                 # DB loading/backfill helpers
├── scrapers/                # market, macro, fundamentals collectors
├── data/                    # generated CSV/JSON artifacts
├── docker-compose.yml       # postgres + pgAdmin
├── .env.example
└── requirements.txt
```

## Prerequisites

- Python 3.10+
- PostgreSQL (or Docker)
- Internet access for external APIs/sources
- For fundamentals scraper: Chrome/Chromium + Selenium stack
- For agents: Groq API key

## Installation

1. Create and activate a virtual environment.
2. Install base dependencies:

```bash
pip install -r requirements.txt
```

3. Install extra dependencies used by optional/full pipeline parts:

```bash
pip install cloudscraper feedparser lxml selenium undetected-chromedriver webdriver-manager googlenewsdecoder
```

4. Create `.env` from `.env.example` and fill secrets/credentials.

## Environment variables

From `.env.example`:

- PostgreSQL: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`
- pgAdmin: `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD`
- Agents: `GROQ_API_KEY`, optional `GROQ_MODEL`
- Optional (if you extend agent tooling): `ANTHROPIC_API_KEY`, Google API/CSE keys

## Database setup (Docker)

```bash
docker compose up -d postgres pgadmin
```

- Postgres schema is auto-created by `docker/init/01_schema.sql`.
- pgAdmin runs on `http://localhost:5050`.

## Core workflows

### 1) Realtime market data

```bash
# default: one snapshot; auto-finalize after close; no raw intraday/orderbook write
python scrapers/atw_realtime_scraper.py

# explicit commands
python scrapers/atw_realtime_scraper.py snapshot --force
python scrapers/atw_realtime_scraper.py finalize --date 2026-04-28
```

Outputs/state:

- `data/ATW_bourse_casa_full.csv`
- `data/ATW_intraday.csv`
- `data/ATW_orderbook_YYYY-MM-DD.csv`
- `data/atw_realtime_state.json`
- DB tables: `bourse_daily`, `bourse_intraday`, `bourse_orderbook`, `technicals_snapshot`

### 2) Macro dataset

```bash
python scrapers/atw_macro_collector.py
python scrapers/atw_macro_collector.py --full-refresh --start-date 2010-01-01
```

Output: `data/ATW_macro_morocco.csv` (+ DB upsert to `macro_morocco`).

### 3) Monthly fundamentals

```bash
python scrapers/fondamental_scraper.py
python scrapers/fondamental_scraper.py --force
python scrapers/fondamental_scraper.py --headful --debug
```

Output: `data/ATW_fondamental.json` (+ DB upsert to `fondamental_snapshot` and `fondamental_yearly`).

### 4) News collection (source scripts)

```bash
python news_crawler/ATW_boursenews_news.py
python news_crawler/ATW_medias24_news.py
python news_crawler/ATW_leconomiste_news.py
python news_crawler/ATW_aujourdhui_news.py
python news_crawler/ATW_marketscreener_news.py
python news_crawler/ATW_googlenews_news.py
```

Common behavior:

- canonicalization + dedup
- noise filtering
- signal scoring (`signal_score` 0–100 + `is_atw_core`)
- merge/upsert to `data/ATW_news.csv`
- DB upsert to `news` table when run as script entrypoint
- crawler state in `data/scrapers/atw_news_state.json` (created on run)

### 5) Valuation models

```bash
python -m models.fundamental_models --model graham
python -m models.fundamental_models --model all
```

Behavior:

- Loads ATW market + fundamentals from `data/` (with fallback logic).
- Always writes **all** model outputs to `data/models_result.json`:
  `dcf`, `ddm`, `graham`, `relative`, `monte_carlo`.
- `--model` controls console print, not output file coverage.

### 6) AI agents

#### News AI brief (live web search)

```bash
python agents/agent_news.py
python agents/agent_news.py --query "Only ATW earnings updates"
python agents/agent_news.py --raw --per-query 6
```

- Uses DDGS + Groq (`GROQ_API_KEY` required).
- Terminal output only (no DB/file writes in this agent).

#### Holistic analyse AI report (file-based synthesis)

```bash
python agents/agent_analyse.py
python agents/agent_analyse.py --raw
python agents/agent_analyse.py --lookback-days 7 --min-news-score 80
python agents/agent_analyse.py --evidence-only
```

- Reads:
  - `data/ATW_bourse_casa_full.csv`
  - `data/ATW_macro_morocco.csv`
  - `data/ATW_news.csv`
  - `data/ATW_fondamental.json`
  - `data/models_result.json`
- Writes append-only prediction log: `data/prediction_history.csv` (unless `--no-history`).

## Automation

Run the built-in scheduler:

```bash
python autorun/scheduler.py
python autorun/scheduler.py --status
python autorun/scheduler.py --once NEWS
python autorun/scheduler.py --once REALTIME
python autorun/scheduler.py --once MONTHLY
```

Configured cadences:

- **NEWS**: hourly (runs all `news_crawler/ATW_*_news.py`)
- **REALTIME**: every 15 minutes during market window
- **MONTHLY**: day 1 at 02:00 (fundamentals + macro)

Logs: `autorun/autorun.log`

## Utility scripts

```bash
python scripts/load_data.py
python scripts/backfill_realtime_to_db.py
```

- `load_data.py`: pushes existing CSV/JSON in `data/` to Postgres.
- `backfill_realtime_to_db.py`: migrates `data/atw_realtime_state.json` history into DB.

## PostgreSQL schema (created automatically)

- `bourse_daily`
- `macro_morocco`
- `news`
- `fondamental_snapshot`
- `fondamental_yearly`
- `bourse_intraday`
- `bourse_orderbook`
- `technicals_snapshot`

See full DDL in `docker/init/01_schema.sql`.

## Main output files in `data/`

- `ATW_bourse_casa_full.csv`
- `ATW_intraday.csv`
- `ATW_orderbook_YYYY-MM-DD.csv`
- `ATW_macro_morocco.csv`
- `ATW_fondamental.json`
- `ATW_news.csv`
- `models_result.json`
- `prediction_history.csv`
- `atw_realtime_state.json`

## Detailed documentation tasks (execute step by step)

Each task will fully explain one part with:

- purpose and business meaning
- exact inputs/outputs
- internal workflow (step-by-step)
- CLI flags and examples
- DB writes/reads
- edge cases, fallbacks, and failure modes
- generated files and field-by-field meaning

1. **Task 1 — Architecture deep dive:** Explain every folder/file role and how data flows across modules.
2. **Task 2 — Realtime scraper deep dive:** Full breakdown of snapshot/finalize logic, market status, fallback chain, and technicals.
3. **Task 3 — Macro collector deep dive:** Source-by-source explanation (WB/IMF/YF/Investing), sanity bands, incremental mode, and feature engineering.
4. **Task 4 — Fundamentals scraper deep dive:** Selenium flow, pages parsed, metric mapping, normalization, and monthly guard behavior.
5. **Task 5 — News crawlers deep dive:** Per-source strategy, dedup/canonicalization, noise filtering, signal scoring formula, and CSV merge logic.
6. **Task 6 — Database layer deep dive:** `AtwDatabase` lifecycle, all save/get methods, upsert rules, and table-level behavior.
7. **Task 7 — Valuation engine deep dive:** Loader merge strategy + each model (DCF/DDM/Graham/Relative/Monte Carlo) with formulas and bank-aware fallbacks.
8. **Task 8 — Agent News deep dive:** DDGS search flow, schema contract, prompt constraints, and output interpretation.
9. **Task 9 — Agent Analyse deep dive:** Evidence block format, deterministic predictions, verdict rules, and history tracking.
10. **Task 10 — Automation deep dive:** Scheduler cadence, time windows, timeout model, `--once` modes, and operational logging.
11. **Task 11 — Scripts and operations deep dive:** `load_data.py`, `backfill_realtime_to_db.py`, and safe recovery workflows.
12. **Task 12 — Full project reference pass:** Build a final "all information" consolidated reference in this file with cross-links and command playbooks.

## Task 1 — Architecture deep dive

### 1) System mission and boundary

Whale is a **single-ticker intelligence platform** focused on ATW.  
It does 4 things end-to-end:

1. **Collect** market/macro/news/fundamental data from multiple external providers.
2. **Normalize and store** data into files (`data/`) and PostgreSQL tables.
3. **Model** fair value with 5 valuation methods.
4. **Synthesize** actionable insights with two AI agents (news brief + holistic analysis).

Project boundary: this repository is data/analysis infrastructure, not a trading execution system.

### 2) Architecture layers

| Layer         | Main folders/files                                | Responsibility                                | Produces                                    |
| ------------- | ------------------------------------------------- | --------------------------------------------- | ------------------------------------------- |
| Ingestion     | `scrapers/`, `news_crawler/`                      | Pull raw/structured data from APIs/web pages  | CSV/JSON in `data/`, DB upserts             |
| Storage       | `database/db.py`, `docker/init/01_schema.sql`     | Unified persistence interface and schema      | Postgres tables + idempotent writes         |
| Modeling      | `models/fundamental_models.py`                    | Load merged inputs + run valuation models     | `data/models_result.json`                   |
| Intelligence  | `agents/agent_news.py`, `agents/agent_analyse.py` | AI synthesis for news and holistic verdicts   | Terminal reports + `prediction_history.csv` |
| Orchestration | `autorun/scheduler.py`, `scripts/*.py`            | Scheduling, backfill, and operational loading | Automated runs + maintenance flows          |

### 3) Folder-by-folder architecture map

#### `scrapers/` (core numeric pipelines)

- `atw_realtime_scraper.py`
  - Intraday snapshots from Medias24.
  - EOD finalization from Casablanca Bourse API (with fallback chain).
  - Maintains `data/atw_realtime_state.json`.
  - Writes market/technicals to DB when available.
- `atw_macro_collector.py`
  - Builds `ATW_macro_morocco.csv` from WB/IMF/YF/Investing.
  - Applies sanity guards + incremental update logic.
  - Upserts to `macro_morocco`.
- `fondamental_scraper.py`
  - Selenium/BeautifulSoup scraper for MarketScreener financial pages.
  - Monthly-oriented periodic fundamentals generation.
  - Writes `ATW_fondamental.json` + upserts snapshot/yearly DB tables.

#### `news_crawler/` (source-specific text pipelines)

- `ATW_boursenews_news.py`
- `ATW_medias24_news.py`
- `ATW_leconomiste_news.py`
- `ATW_aujourdhui_news.py`
- `ATW_marketscreener_news.py`
- `ATW_googlenews_news.py`

Each crawler:

- extracts source-specific links/articles,
- canonicalizes + deduplicates URLs,
- applies noise filtering,
- computes `signal_score` and `is_atw_core`,
- merges into `data/ATW_news.csv`,
- can upsert into `news` table in standalone mode.

#### `database/` (persistence abstraction)

- `db.py` exposes `AtwDatabase` with:
  - connection lifecycle management,
  - save/upsert methods for all major datasets,
  - retrieval methods for downstream consumers.
- `__init__.py` exports `AtwDatabase` for consistent imports.

#### `models/` (valuation computation)

- `fundamental_models.py`:
  - loads merged market + fundamentals input with fallback strategy,
  - runs: `dcf`, `ddm`, `graham`, `relative`, `monte_carlo`,
  - always writes all model outputs to `data/models_result.json`.

#### `agents/` (AI synthesis)

- `agent_news.py`:
  - live DDGS searches,
  - structured Groq synthesis into a news brief schema,
  - terminal output only.
- `agent_analyse.py`:
  - reads local file outputs from the data/model pipelines,
  - builds citation-based evidence block,
  - computes deterministic prediction numbers + LLM narrative synthesis,
  - appends `data/prediction_history.csv`.

#### `autorun/` (automation layer)

- `scheduler.py` is a long-running scheduler with 3 buckets:
  - NEWS (hourly),
  - REALTIME (15 min during market window),
  - MONTHLY (day 1, 02:00).
- `autorun.log` stores runtime logs.

#### `scripts/` (operations and migration)

- `load_data.py`: load current `data/` artifacts into DB.
- `backfill_realtime_to_db.py`: replay realtime state history into DB.

#### Infrastructure/config files

- `docker-compose.yml`: PostgreSQL + pgAdmin services.
- `docker/init/01_schema.sql`: DB schema bootstrap (tables/indexes).
- `.env` / `.env.example`: runtime configuration and credentials.
- `requirements.txt`: Python dependency baseline.

### 4) Data contracts between components

#### File contracts in `data/`

- `ATW_bourse_casa_full.csv` → market truth for models + analyse agent.
- `ATW_macro_morocco.csv` → macro context for analyse agent.
- `ATW_news.csv` → scored/canonicalized news corpus.
- `ATW_fondamental.json` → periodic financial fundamentals.
- `models_result.json` → multi-model valuation output.
- `prediction_history.csv` → historical analysis snapshots.
- `atw_realtime_state.json` + intraday/orderbook CSVs → operational realtime state.

#### Database contracts (high-level)

- `bourse_daily`, `bourse_intraday`, `bourse_orderbook`, `technicals_snapshot` (market/technicals)
- `macro_morocco` (macro context)
- `news` (scored article records)
- `fondamental_snapshot`, `fondamental_yearly` (fundamentals)

Design pattern: **idempotent insert/update behavior** to safely support repeated scheduled runs.

### 5) End-to-end flow (how everything connects)

1. Realtime scraper updates market files + DB market tables.
2. Macro collector updates macro file + DB macro table.
3. Fundamentals scraper updates fundamentals JSON + DB fundamentals tables.
4. News crawlers update news CSV + DB news table.
5. Valuation engine reads market + fundamentals, writes `models_result.json`.
6. `agent_analyse` reads all final artifacts and produces a cited verdict.
7. Scheduler orchestrates repeated execution windows.

This forms a **closed intelligence loop**: data collection → normalization/storage → valuation → AI interpretation.

### 6) Control plane vs data plane

- **Data plane**: scrapers/crawlers/models/agents processing data artifacts.
- **Control plane**: scheduler, scripts, Docker services, env config.

This separation makes operations safer:

- you can rerun ingestion without changing modeling logic,
- rerun modeling/agents on existing artifacts,
- backfill DB independently of scraping.

### 7) Reliability architecture patterns already used

- Multi-source fallbacks (especially realtime finalize and macro sources).
- Dedup/canonicalization for noisy web/news sources.
- Sanity bands to reject corrupted upstream numeric feeds.
- Stateful resumability (`atw_realtime_state.json`, news crawler state).
- Append-only history for analysis audit trail (`prediction_history.csv`).
- DB writes wrapped as upsert/conflict-safe operations.

### 8) Architecture summary

Whale is built as a **modular pipeline architecture** with clear responsibilities:

- ingestion modules gather domain-specific data,
- storage layer standardizes persistence,
- valuation layer converts data to fair-value signals,
- agent layer converts signals to human-ready intelligence,
- scheduler layer turns all of it into repeatable operations.

Task 1 status: complete in this document.  
Next task to execute on your command: **Task 2 — Realtime scraper deep dive**.

## Task 2 — Realtime scraper deep dive (`scrapers/atw_realtime_scraper.py`)

### 1) Purpose in the architecture

This module is the **market heartbeat** of Whale.  
It is responsible for:

1. Capturing intraday ATW snapshots (price, O/H/L, variation, volume, trades, cap, orderbook).
2. Producing one reliable end-of-day row in `ATW_bourse_casa_full.csv`.
3. Computing and storing technical indicators.
4. Persisting market/technicals into PostgreSQL when DB is available.

It is designed for repeated scheduled execution and safe reruns.

### 2) External data sources and identifiers

#### Intraday source (primary for snapshot)

- **Medias24 JSON API**
  - Root: `https://medias24.com/content/api`
  - Methods used:
    - `getStockInfo`
    - `getTransactions`
    - `getBidAsk`

#### Official EOD source (primary for finalize)

- **Casablanca Bourse JSONAPI**
  - URL: `https://www.casablanca-bourse.com/api/proxy/fr/api/bourse_data/instrument_history`
  - Uses `cloudscraper` session for anti-bot compatibility.

#### Hardwired ATW identifiers

- `ATW_ISIN = MA0000012445`
- `ATW_INSTRUMENT_ID = 511`

### 3) CLI contract and run modes

#### Default run (no subcommand)

```bash
python scrapers/atw_realtime_scraper.py
```

- Executes `snapshot` behavior.
- Uses `no_save_raw=True` internally (so it skips writing intraday/orderbook CSV on this path).
- After market close, it auto-attempts `finalize`.

#### Explicit subcommands

```bash
python scrapers/atw_realtime_scraper.py snapshot [--force]
python scrapers/atw_realtime_scraper.py finalize [--date YYYY-MM-DD] [--force]
```

- `snapshot --force` bypasses debounce/stall checks and allows technical recomputation.
- `finalize --force` bypasses idempotency guard and allows rebuild/append logic.

### 4) Files, schemas, and state

#### Main files

- `data/ATW_bourse_casa_full.csv` (daily EOD master file)
- `data/ATW_intraday.csv` (intraday time series)
- `data/ATW_orderbook_YYYY-MM-DD.csv` (level-5 bid/ask snapshots)
- `data/atw_realtime_state.json` (operational state)

#### EOD schema (`FULL_EOD_FIELDS`)

- `Séance`
- `Instrument`
- `Ticker`
- `Ouverture`
- `Dernier Cours`
- `+haut du jour`
- `+bas du jour`
- `Nombre de titres échangés`
- `Volume des échanges`
- `Nombre de transactions`
- `Capitalisation`

#### Intraday schema (`INTRADAY_FIELDS`)

- `timestamp`, `cotation`, `market_status`
- `last_price`, `open`, `high`, `low`, `prev_close`, `variation_pct`
- `shares_traded`, `value_traded_mad`, `num_trades`, `market_cap`

#### State keys used

- `last_snapshot_ts`
- `last_snapshot_cotation`
- `last_snapshot_last_price`, `last_snapshot_open`, `last_snapshot_high`, `last_snapshot_low`
- `last_snapshot_prev_close`, `last_snapshot_variation_pct`
- `last_snapshot_shares_traded`, `last_snapshot_value_traded_mad`
- `last_snapshot_num_trades`, `last_snapshot_market_cap`
- `snapshot_history` (newest first)
- `technicals` (latest computed technical payload)
- `finalized_days` (idempotency memory for EOD finalization)

### 5) Snapshot pipeline (step-by-step)

1. Load current Casablanca-local time (`UTC+1` in script constant) and load state.
2. Apply **debounce**: if last snapshot is under `60s`, skip (`return 0`).
3. Apply **stall replay logic**:
   - if market is `CLOSED` and same-day snapshot already exists,
   - replay cached last snapshot values instead of calling network.
4. If not replay path:
   - call Medias24 methods (`getStockInfo`, `getTransactions`, `getBidAsk`),
   - build `Snapshot` + `OrderBook`.
5. If parsed `last_price` is missing, fail fast (`return 2`).
6. Optionally write raw files (`ATW_intraday.csv`, orderbook daily file).
7. Update state with latest snapshot keys.
8. Compute technicals only when:
   - market status is `OPEN`, or
   - `--force` was used.
9. Persist snapshot (+ optional technicals) into DB using `AtwDatabase`.
10. If after market close, trigger auto-finalize check.

### 6) Market-status and timing logic

- `PRE_OPEN`: 09:00–09:29
- `OPEN`: 09:30–15:29
- `CLOSED`: outside those windows
- Auto-finalize threshold: after `15:30`.

This gives deterministic behavior for scheduler-driven runs.

### 7) Finalize pipeline and fallback chain

`finalize` creates exactly one EOD row for a day, using strict source priority:

1. **Casablanca Bourse official API** (`source=casablanca_bourse_daily_api`)
2. **Intraday aggregation fallback** from `ATW_intraday.csv` (`source=medias24_intraday_csv`)
3. **State snapshot fallback** from `atw_realtime_state.json` (`source=medias24_state_snapshot`)

If all fail, finalize returns code `1` ("no data available").

#### Idempotency behavior

- If day is already in `finalized_days` and row exists in EOD CSV, it exits cleanly.
- If state says finalized but row is missing, script rebuilds to repair inconsistency.
- If row already exists in CSV, it marks finalized and skips duplicate append.

### 8) Intraday-to-EOD aggregation fallback details

When official API is unavailable:

- Filter `ATW_intraday.csv` rows by target day.
- Build EOD fields from intraday rows:
  - open = first valid intraday open
  - close = last row `last_price` (required)
  - high/low = extrema over valid intraday highs/lows
  - shares/volume/trades/cap = from last intraday row cumulative values
- If `last_price` missing in final row, fallback fails.

### 9) Technical indicator engine (`compute_technicals`)

#### Input assembly

- Loads all historical daily files `ATW_bourse_casa_*.csv` + master `ATW_bourse_casa_full.csv`.
- Normalizes to canonical columns: `Date`, `Open`, `High`, `Low`, `Close`, `Volume`.
- Deduplicates by date and sorts chronologically.

#### Indicators computed

- Moving averages: `SMA_20`, `SMA_50`, `SMA_200`, `EMA_12`, `EMA_26`
- `RSI(14)`
- `MACD(12,26,9)` (+ histogram)
- Bollinger bands `(20,2)` + `%B` + bandwidth
- Stochastic oscillator `%K/%D`
- `ATR_14` + `ATR_14_pct`
- `VWAP_20d` (rolling approximation from daily data)
- Support/resistance (20d)
- Returns (`1d`, `5d`, `20d`, `60d`)
- `realized_vol_20d_ann`
- 52-week high/low + distance metrics
- `OBV` + slope
- `ADX` + `+DI`/`-DI`
- `MFI_14`
- Derived trend and qualitative signal labels.

#### Output contract

Returns a JSON-like dict stored in state and optionally persisted in DB table `technicals_snapshot`.

### 10) Database writes from this module

#### During snapshot

- `db.save_intraday([row])` → `bourse_intraday`
- `db.save_orderbook([row], ticker=TICKER)` (top 5 bid/ask levels when available) → `bourse_orderbook`
- `db.save_technicals(technicals_payload, symbol=TICKER)` (if technicals computed) → `technicals_snapshot`

#### During finalize

- Converts final EOD row to DataFrame with official column names.
- `db.save_bourse(dataframe_with_eod_columns)` → `bourse_daily`

DB failures are non-fatal in this module (warning logged, run continues).

### 11) Return codes and failure behavior

#### `snapshot`

- `0` success/skip (including debounce replay paths)
- `2` network/API parsing failure (HTTP error, request failure, invalid payload)

#### `finalize`

- `0` success/already finalized
- `1` no source had enough data to build EOD

This makes scheduler behavior predictable and easy to monitor.

### 12) Reliability and safety mechanisms

- Debounce protection (`60s`) against duplicate high-frequency calls.
- Closed-session stall replay to keep continuity without unnecessary network calls.
- Multi-level finalize fallback chain.
- Header migration logic in `_append_row` if CSV schema evolves.
- Atomic state writes using temp file + `os.replace`.
- Finalization memory (`finalized_days`) for idempotency.

### 13) Relationship with scheduler (`autorun/scheduler.py`)

- Scheduler calls:
  - `python scrapers/atw_realtime_scraper.py snapshot`
- This scraper itself performs:
  - capture + optional technicals
  - post-close auto-finalize attempt

So one scheduler bucket can drive both intraday and daily closure workflows.

### 14) Operational caveats

- Casablanca API path depends on `cloudscraper`; missing dependency disables official finalize source.
- If Medias24 changes payload keys (e.g., `cours`, `ouverture`), snapshot can fail with parse error.
- Technicals need enough historical rows (`len >= 30`) or return an insufficient-history error payload.

### 15) Task 2 completion note

Task 2 status: complete in this document.

## Task 3 — Macro collector deep dive (`scrapers/atw_macro_collector.py`)

### 1) Purpose in the architecture

This module builds the macro context dataset used by:

- the analysis agent (`agents/agent_analyse.py`)
- the PostgreSQL macro table (`macro_morocco`)
- downstream decision logic that depends on inflation, FX, market regime, and global risk.

Its responsibility is to convert mixed-frequency external series into one daily, forward-filled, validated table.

### 2) Output contract

Default output path:

- `data/ATW_macro_morocco.csv`

Output columns are fixed by `OUTPUT_COLUMNS` in this exact order:

1. `date`
2. `frequency_tag`
3. `gdp_growth_pct`
4. `current_account_pct_gdp`
5. `public_debt_pct_gdp`
6. `inflation_cpi_pct`
7. `eur_mad`
8. `usd_mad`
9. `brent_usd`
10. `wheat_usd`
11. `gold_usd`
12. `vix`
13. `sp500_close`
14. `em_close`
15. `us10y_yield`
16. `masi_close`
17. `gdp_ci`
18. `gdp_sn`
19. `gdp_cm`
20. `gdp_tn`
21. `macro_momentum`
22. `fx_pressure_eur`
23. `global_risk_flag`

### 3) External sources and exact series mapping

#### World Bank (Morocco)

- `NY.GDP.MKTP.KD.ZG` → `gdp_growth_pct`
- `BN.CAB.XOKA.GD.ZS` → `current_account_pct_gdp`
- `GC.DOD.TOTL.GD.ZS` → `public_debt_pct_gdp_wb` (fallback source)
- `FP.CPI.TOTL.ZG` → `inflation_cpi_pct_wb`

#### World Bank (regional ATW footprint proxy)

- Côte d’Ivoire `NY.GDP.MKTP.KD.ZG` → `gdp_ci`
- Senegal `NY.GDP.MKTP.KD.ZG` → `gdp_sn`
- Cameroon `NY.GDP.MKTP.KD.ZG` → `gdp_cm`
- Tunisia `NY.GDP.MKTP.KD.ZG` → `gdp_tn`

#### IMF DataMapper

- `PCPIPCH` (MAR) → `inflation_cpi_pct_imf`
- `GGXWDG_NGDP` (MAR) → `public_debt_pct_gdp_imf`

#### yfinance candidates

- `eur_mad`: `EURMAD=X`
- `usd_mad`: `USDMAD=X`
- `brent_usd`: `BZ=F`
- `wheat_usd`: `WEAT`
- `gold_usd`: `GC=F`
- `vix`: `^VIX`
- `sp500_close`: `^GSPC`
- `em_close`: `IEMG`, fallback `EEM`
- `us10y_yield`: `^TNX`

#### investing.com

- MASI pair id `13228` → `masi_close`

### 4) Network and security handling

- Forces certificate bundle paths with `certifi`:
  - `REQUESTS_CA_BUNDLE`
  - `CURL_CA_BUNDLE`
  - `SSL_CERT_FILE`
- Uses `cloudscraper` for investing.com MASI endpoint because plain requests can be blocked.

### 5) Data acquisition behavior

Each source fetch is isolated. If one source fails:

- error is logged,
- that series becomes an empty `pd.Series`,
- pipeline continues with remaining sources.

This avoids total run failure from single-source outage.

### 6) Incremental versus full-refresh logic

#### Full refresh

- Triggered by `--full-refresh`.
- Uses provided `--start-date` directly.
- Rebuilds output from scratch.

#### Incremental mode (default)

- If output already exists and has valid dates:
  - reads latest existing date,
  - sets new start date to `latest_date - 30 days`.
- 30-day lookback is intentional to recompute rolling and percent-change derived features correctly.
- Merges new frame with existing file and deduplicates by `date` (keep last row).

### 7) Temporal normalization strategy

Different sources have annual, daily, and mixed date formats.  
Normalization process:

1. Parse each source index into datetime.
2. Deduplicate and sort source index.
3. Build global daily index from start to end.
4. Reindex each series on union index.
5. Forward-fill.
6. Reindex to final daily index.

This prevents losing annual macro points during incremental slices.

### 8) Sanity-band design and validation

#### Band definitions

- `masi_close`: 5000 to 50000
- `inflation_cpi_pct`: -5 to 25
- `public_debt_pct_gdp`: 20 to 150
- `gdp_growth_pct`: -25 to 25
- `current_account_pct_gdp`: -25 to 25
- `brent_usd`: 5 to 300
- `gold_usd`: 200 to 15000
- `eur_mad`: 5 to 20
- `usd_mad`: 5 to 20
- `vix`: 5 to 100

#### Two-stage enforcement

1. **Source-level rejection** with `_passes_sanity`:
   - if any non-null point violates band, whole source series is discarded.
2. **Row-level rejection** with `validate_frame`:
   - for populated cells, if value is outside band, row is dropped.

### 9) Precedence rules for disputed indicators

#### Inflation

- Primary: World Bank (`inflation_cpi_pct_wb`)
- Secondary fallback: IMF (`inflation_cpi_pct_imf`)
- Both pass through sanity checks before combine.

#### Public debt

- Primary: IMF general government debt (`public_debt_pct_gdp_imf`)
- Secondary fallback: World Bank central government debt (`public_debt_pct_gdp_wb`)
- Both pass through sanity checks before combine.

### 10) Derived feature engineering

After base series merge:

- `frequency_tag = "daily_ffill"`
- `macro_momentum = gdp_growth_pct.diff(4)`
- `fx_pressure_eur = eur_mad.pct_change(20)`
- `global_risk_flag = 1 if vix > 25 else 0` (nullable if VIX missing)

### 11) Sparse data and column pruning

#### Sparse row cleanup

- Drops rows where all four core daily indicators are missing:
  - `eur_mad`
  - `usd_mad`
  - `masi_close`
  - `vix`

#### Sparse column cleanup

- Controlled by `--max-missing-ratio` (0.0 to 1.0).
- Columns above threshold are dropped.
- `date` and `frequency_tag` are always preserved.

### 12) CLI interface

```bash
python scrapers/atw_macro_collector.py
python scrapers/atw_macro_collector.py --full-refresh --start-date 2010-01-01
python scrapers/atw_macro_collector.py --end-date 2026-04-28
python scrapers/atw_macro_collector.py --max-missing-ratio 0.4
python scrapers/atw_macro_collector.py --log-level DEBUG
```

Arguments:

- `--out`: output path (default `data/ATW_macro_morocco.csv`)
- `--start-date`: start date for full mode
- `--end-date`: optional end date
- `--max-missing-ratio`: column pruning threshold
- `--full-refresh`: force rebuild
- `--log-level`: `DEBUG`, `INFO`, `WARNING`, `ERROR`

Validation rule:

- if `--max-missing-ratio` is not within `[0.0, 1.0]`, raises `ValueError`.

### 13) Database write behavior

After final CSV generation:

- imports `AtwDatabase`
- executes `db.save_macro(final_df)`
- upserts rows into `macro_morocco` with conflict-safe behavior.

If DB fails, file output still succeeds and warning is logged.

### 14) Return behavior and failure modes

- Success return code: `0`
- Hard failure cases:
  - malformed argument range for `--max-missing-ratio`
  - unhandled runtime exception outside guarded fetch blocks
- Soft failure cases:
  - source outage, malformed source payload, missing cloudscraper, transient API errors
  - these are logged and pipeline continues with available series.

### 15) Role in end-to-end intelligence quality

This collector is the macro truth layer for Whale.  
Its value comes from:

- strict source precedence rules,
- corruption guards for known problematic feeds,
- deterministic daily normalization,
- operational resilience under partial data outage.

### 16) Task 3 completion note

Task 3 status: complete in this document.

## Task 4 — Fundamentals scraper deep dive (`scrapers/fondamental_scraper.py`)

### 1) Purpose in the architecture

This module is Whale’s **periodic fundamentals ingestion engine** for ATW.  
Its main job is to extract multi-year accounting, valuation, and profitability series from MarketScreener pages and save them in a model-ready JSON contract.

Primary output used by the rest of the system:

- `data/ATW_fondamental.json`

Primary downstream consumers:

- `models/fundamental_models.py`
- `agents/agent_analyse.py`
- PostgreSQL tables `fondamental_snapshot` and `fondamental_yearly`

### 2) Core constants and project scope

Hardcoded project identity and paths:

- `ATW_SYMBOL = "ATW"`
- `ATW_NAME = "ATTIJARIWAFA BANK"`
- `ATW_URL_CODE = "ATTIJARIWAFA-BANK-SA-41148801"`
- `BASE_URL = "https://www.marketscreener.com/quote/stock"`
- `ATW_FUNDAMENTAL_JSON = data/ATW_fondamental.json`
- `ATW_OUTPUT_CSV = data/ATW_fondamental.csv` (helper output path exists in module)
- `ATW_MERGED_JSON = data/ATW_merged.json` (helper output path exists in module)
- `ATW_MODEL_INPUTS_JSON = data/ATW_model_inputs.json` (helper output path exists in module)

Timing/performance constants:

- `FAST_PAGE_LOAD_TIMEOUT = 30`
- `FAST_DOM_WAIT_TIMEOUT = 8`

### 3) Dependency model

Required runtime libraries:

- Selenium stack (`selenium`, `webdriver-manager`)
- HTML parser (`beautifulsoup4`, `lxml`)
- stealth browser driver (`undetected-chromedriver`) when available

Fallback behavior:

- if `undetected-chromedriver` is unavailable, module falls back to plain Selenium Chrome driver and warns about reduced stealth reliability.

### 4) Data model contract (`StockData`)

`StockData` is the canonical in-memory structure. It contains:

- scalar market fields:
  - `price`, `market_cap`, `volume`, `high_52w`, `low_52w`
  - `pe_ratio`, `dividend_yield`, `price_to_book`
- historical financial series:
  - `hist_revenue`, `hist_net_income`, `hist_eps`, `hist_ebitda`
  - `hist_fcf`, `hist_ocf`, `hist_capex`
  - `hist_debt`, `hist_cash`, `hist_equity`
- margin and returns series:
  - `hist_net_margin`, `hist_ebit_margin`, `hist_ebitda_margin`, `hist_gross_margin`
  - `hist_roe`, `hist_roce`
- valuation history series:
  - `hist_ev_ebitda`
  - `pe_ratio_hist`, `pbr_hist`, `ev_revenue_hist`, `ev_ebit_hist`
  - `capitalization_hist`, `hist_ebit`, `fcf_yield_hist`
- shareholder return series:
  - `hist_dividend_per_share`, `hist_eps_growth`
- metadata:
  - `symbol`, `scrape_timestamp`, `scrape_warnings`

### 5) Number and text parsing layer

Key parser behavior:

- `parse_number`:
  - accepts localized formats and suffixes (`K`, `M`, `B`, `T`)
  - handles decimal and thousands separators from multiple formats
  - rejects malformed or suspiciously long numeric blobs
- `parse_percent`:
  - converts percent-like strings to float percentage values
- KV extraction helpers (`extract_kv_pairs`, `find_in_kv`, `find_all_in_kv`):
  - extract label-value widgets from rendered DOM
  - used for robust scalar extraction on dynamic pages

### 6) SeleniumScraper runtime behavior

`SeleniumScraper` handles browser lifecycle and anti-bot resilience:

1. Picks user-agent (randomized when not provided).
2. Creates isolated temporary profile path per run.
3. Starts undetected-chromedriver when available:
   - eager page load strategy
   - image loading disabled
   - language and window sizing set
4. If stealth driver unavailable, starts plain Selenium Chrome fallback.
5. Applies page load timeout based on fast/slow mode.

Important internals:

- `_wait_and_get_soup` waits for `<body>`, then delays for JavaScript-rendered widgets.
- `_maybe_dump_html` writes rendered HTML when debug mode is enabled.

### 7) Generic year-table parser (`_parse_year_tables`)

This helper is the module’s most important extraction primitive:

- scans all tables,
- identifies historical year columns (`20xx`) up to current year,
- ignores estimate/forecast rows,
- maps row labels to target dictionaries via regex maps,
- supports growth rows linked to previous primary metric context.

This single helper powers finances, ratios, cashflow, and valuation page extraction.

### 8) Page-by-page scraping flow

#### `scrape_main_page`

Extracts scalar fields from quote page:

- price
- market cap
- P/E
- dividend yield
- P/B
- 52-week high and low
- volume

Strategy:

- first try DOM key-value extraction,
- then regex fallback on flattened page text for missing fields.

#### `scrape_finances_page`

Extracts historical core metrics from `/finances/` tables, then calls income-statement override for higher-quality revenue and DPS.

Captured targets include:

- net income
- EPS
- EBITDA
- free cash flow
- operating cash flow
- capex
- debt
- cash
- equity
- EV/EBITDA history

#### `_override_income_statement_metrics`

Loads `/finances-income-statement/` and redefines critical values:

- revenue source preference:
  1. Revenues before provision for loan losses
  2. net banking income / produit net bancaire
  3. total revenues
  4. generic revenues / net sales
- recalculates net margin from selected denominator
- captures DPS from income statement rows

This is central for bank-appropriate revenue handling.

#### `scrape_balance_sheet_page`

Loads `/finances-balance-sheet/` and fills missing equity years required for stable ROE recomputation.

#### `scrape_ratios_page`

Loads `/finances-ratios/` as source of truth for:

- gross margin
- EBIT margin
- EBITDA margin
- ROE
- ROCE

Ratios are cleared first, then repopulated to avoid stale mixed-source contamination.

#### `scrape_cashflow_page`

Loads `/finances-cash-flow-statement/` and fills missing:

- operating cash flow
- free cash flow
- capex
- cash
- equity

Uses temporary dictionaries, normalizes units to millions, then merges only absent years into main payload.

#### `scrape_valuation_page`

Loads `/valuation/` and extracts:

- P/B fallback from KV or table
- DPS fallback when not previously captured
- EV/EBITDA history fallback
- company valuation history table:
  - `pe_ratio_hist`
  - `pbr_hist`
  - `ev_revenue_hist`
  - `ev_ebit_hist`
  - `capitalization_hist`
  - `hist_ebit`

Cleans spurious zero placeholders for non-applicable multiples and normalizes capitalization/EBIT units.

### 9) Rate-limit and bot challenge handling

Detection:

- checks title/body markers such as:
  - "just a moment"
  - "verify you are human"
  - "too many requests"
  - "captcha"

Recovery flow:

1. clear cookies
2. clear local and session storage
3. clear browser cookies/cache through CDP
4. wait randomized cooldown (`10` to `20` seconds)
5. retry page once

If still blocked after retry:

- records warning in `scrape_warnings`
- stops remaining page pipeline for that run.

### 10) Post-processing pipeline

After page scraping:

1. `_keep_reported_years_only`
   - removes non-year keys
   - removes future years beyond current year
2. `_compute_derived_series`
   - fills missing `hist_fcf = hist_ocf - abs(hist_capex)` when possible
   - computes `fcf_yield_hist = hist_fcf / capitalization_hist`
   - derives `hist_revenue = capitalization_hist / ev_revenue_hist` when revenue missing
3. `_recompute_roe_average_equity`
   - recomputes ROE using average equity between year `t` and `t-1`
4. `data.validate()`
   - applies sanity checks including suspicious P/E handling and EPS growth fallback logic

### 11) Monthly guard behavior

Function `_already_scraped_this_month`:

- loads existing `ATW_fondamental.json`,
- reads `scrape_timestamp`,
- compares year and month with current UTC month.

If already scraped and `--force` not passed:

- run exits early with informational log,
- no new scrape is executed.

### 12) Output behavior

#### Primary saved artifact

- `_save_atw_fondamental_json(stock_data, ATW_FUNDAMENTAL_JSON)`

JSON contains:

- scalar valuation and price fields
- all historical series dictionaries
- scrape warnings
- `data_source` metadata block

Before writing:

- `_prune_empty_values` removes empty keys and empty collections.

#### Database write after save

- imports `AtwDatabase`
- executes `db.save_fondamental(payload)`
- writes:
  - one snapshot row to `fondamental_snapshot`
  - year-metric rows to `fondamental_yearly`

#### Additional helper outputs present in module

- `_save_atw_fondamental_csv`
- `_save_merged_json`
- `_save_model_inputs_json`

These helpers are defined and available in module but not invoked by the current `main()` path.

### 13) CLI interface

```bash
python scrapers/fondamental_scraper.py
python scrapers/fondamental_scraper.py --force
python scrapers/fondamental_scraper.py --headful
python scrapers/fondamental_scraper.py --debug
python scrapers/fondamental_scraper.py --slow
```

Arguments:

- `--headful`: open visible browser window
- `--debug`: dump rendered HTML and key-value traces under debug folder
- `--slow`: disable fast mode timings
- `--force`: bypass monthly guard and run scrape again

### 14) Operational caveats

- MarketScreener layout or label changes can break regex-label mappings.
- Browser automation stability depends on local Chrome compatibility.
- Rate-limit events can truncate scrape coverage on a run.
- Financial units can vary between pages; module compensates through normalization, but unexpected format shifts require mapping updates.

### 15) Task 4 completion note

Task 4 status: complete in this document.

## Task 5 — News crawlers deep dive (`news_crawler/ATW_*_news.py`)

### 1) Purpose in the architecture

The `news_crawler` layer is Whale’s multi-source narrative ingestion system for ATW.  
Each source file runs as an independent scraper and produces article rows in a shared schema, then merges into:

- `data/ATW_news.csv`

It is built to:

- collect source-specific ATW news,
- extract usable article text,
- remove noise and duplicates,
- score relevance (`signal_score`),
- persist rows for downstream agent analysis.

### 2) Source inventory and current files

Implemented source modules:

- `ATW_aujourdhui_news.py`
- `ATW_boursenews_news.py`
- `ATW_marketscreener_news.py`
- `ATW_leconomiste_news.py`
- `ATW_medias24_news.py`
- `ATW_googlenews_news.py`

All six files are executable standalone scripts and are also discovered by scheduler pattern `ATW_*_news.py`.

### 3) Shared architecture pattern inside each file

Each file embeds a shared helper layer named in comments as injected common logic.  
That shared layer provides:

1. HTTP fetch utilities and redirect handling.
2. Date normalization and French month parsing.
3. URL canonicalization and URL key generation.
4. ATW mention detection and domain noise filtering.
5. Signal scoring and ATW-core tagging.
6. Deduplication.
7. CSV merge and full rewrite persistence.
8. Optional state file load/save helpers.
9. Article body extraction using `trafilatura`.

Then each module adds source-specific scraping logic near the bottom of the file.

### 4) Canonical article row schema

Merged CSV rows use these fields:

- `date`
- `ticker`
- `title`
- `source`
- `url`
- `full_content`
- `query_source`
- `signal_score`
- `is_atw_core`
- `scraping_date`

Ticker default:

- `TICKER = "ATW"`

Output target default in all source files:

- `DEFAULT_OUT = data/ATW_news.csv`

State file path constant:

- `STATE_FILE = data/scrapers/atw_news_state.json`

### 5) Date parsing and extraction model

Date parsing is multi-layered:

1. direct ISO date strings and timestamps,
2. `datetime.fromisoformat`,
3. explicit formats like `%Y-%m-%d %H:%M:%S`,
4. RFC-style dates via `email.utils.parsedate_to_datetime`,
5. French textual month parser (`janvier` to `décembre`, with accented and non-accented forms).

Article page date extraction order:

1. meta tags (`article:published_time`, `datePublished`, `pubdate`, related tags),
2. `<time>` elements,
3. JSON-LD `datePublished` values.

### 6) Content extraction model

`fetch_article_body(url)` behavior:

1. requests HTML with browser-like headers.
2. extracts clean body text through `trafilatura.extract`:
   - `include_comments=False`
   - `include_tables=False`
   - `favor_recall=False`
3. extracts publication date from same HTML.
4. returns tuple `(body_text, extracted_date)`.

If extraction fails, function returns empty strings and logs debug-level failure.

### 7) URL normalization and deduplication model

Canonical URL logic:

- strips `www.`,
- normalizes repeated slashes,
- removes trailing slash from non-root paths,
- removes tracking parameters (`utm_*`, `fbclid`, `gclid`, `igshid`, `mkt_tok`, `mc_cid`, `mc_eid`, `oc`, `ved`, `usg`),
- unwraps nested destination parameters (`url`, `u`, `target`, `dest`, `destination`).

Deduplication strategy:

1. Rank rows to keep highest-quality variants first:
   - non-Google-News redirect URL first,
   - row with `full_content` first,
   - row with `date` first.
2. Drop duplicates by:
   - canonical URL key,
   - date plus normalized title key,
   - normalized title key fallback.
3. Apply extra guard:
   - raw Google News redirect links are deprioritized and rejected when title already exists.

### 8) Noise filter and relevance scoring model

Noise detection removes rows when any condition is true:

- source or URL contains blocked noise substrings (`bebee`, `instagram`, `facebook.com`),
- article mentions `focus pme`,
- Egypt-specific context is detected without Morocco context.

`signal_score` formula outputs `0` to `100`:

- base `10`
- `+20` if ATW mentioned in title or snippet or full content
- `+15` if ATW in title
- `+18` per title core-finance hit up to `3`
- `+8` per additional core-finance hit in full text up to `4`
- `+6` when `query_source` starts with `direct:`
- `-8` per passing-context hit up to `3` (forum, event, sponsor style)
- `-40` for Egypt-specific context without Morocco context

`is_atw_core` becomes `1` when ATW is present and article has strong core-finance signal.

### 9) CSV merge semantics

`merge_and_save_to_csv(new_items)` pipeline:

1. Load existing CSV rows by canonical URL key.
2. For each incoming row:
   - keep prior `full_content` if new row lacks it.
   - keep prior `date` if new row lacks it.
3. Merge map by canonical URL.
4. Run:
   - noise filter,
   - deduplication,
   - signal metadata enrichment.
5. Sort by descending date.
6. Rewrite full CSV atomically.

Return value:

- total number of rows after merge.

### 10) State file model

The helper state API defines fields:

- `seen_urls`
- `per_source_last_seen`
- `failed_body_urls`
- `gnews_resolved`
- `last_full_run_ts`

In current source scripts:

- state helpers exist in all files,
- only Google News body-enrichment helper actively checkpoints `failed_body_urls` and `gnews_resolved`,
- main `scrape()` flows currently rely mostly on `known_url_keys` short-circuit checks and CSV merge memory, not on state-driven incremental cursors.

### 11) Source-specific deep dive — Aujourd'hui (`ATW_aujourdhui_news.py`)

Collection method:

- WordPress REST endpoint:
  - `https://aujourdhui.ma/wp-json/wp/v2/posts`
  - query `search=Attijariwafa+bank`
  - paginated with `per_page=50` and `page=N`

Key behavior:

1. uses fast HTML stripping helper (`fast_strip_tags`) for title and excerpt text.
2. pulls full content directly from REST `content.rendered` then strips to plain text.
3. requires ATW mention in title plus excerpt or title plus first section of full text.
4. supports early stop when top URL key already known.
5. sets `query_source = direct:aujourdhui_search`.
6. source label: `Aujourd'hui`.

CLI:

- `--show`
- `--max-pages` (default `1`)
- `--no-save`

### 12) Source-specific deep dive — Boursenews (`ATW_boursenews_news.py`)

Collection method:

- fixed page:
  - `https://boursenews.ma/action/attijariwafa-bank`

Key behavior:

1. scans all `<a>` links containing `/article/`.
2. requires title length threshold.
3. extracts date near link by walking parent blocks and checking:
   - `<time>`
   - date-like class elements
   - regex on nearby text (`YYYY-MM-DD` and `DD/MM/YYYY`)
4. extracts short nearby snippet from surrounding `<p>`.
5. enriches each row body via `fetch_article_body`.
6. sets `query_source = direct:boursenews_stock`.
7. source label: `Boursenews`.

CLI:

- `--show`
- `--no-save`

### 13) Source-specific deep dive — MarketScreener (`ATW_marketscreener_news.py`)

Collection method:

- fixed ATW news page:
  - `https://www.marketscreener.com/quote/stock/ATTIJARIWAFA-BANK-SA-41148801/news/`

Key behavior:

1. matches news links using regex:
   - `/news/<slug>-<numeric_id_or_hex_suffix>`
2. extracts date from nearest row context using:
   - `data-utc-date` attributes,
   - `<time>` values,
   - regex fallback on row text.
3. enriches all rows with body extraction.
4. sets `query_source = direct:marketscreener_atw_news`.
5. source label: `MarketScreener`.

CLI:

- `--show`
- `--no-save`

### 14) Source-specific deep dive — L'Economiste (`ATW_leconomiste_news.py`)

Collection method has fallback chain:

1. WordPress REST search:
   - `https://www.leconomiste.com/wp-json/wp/v2/posts`
   - search term `attijariwafa`
2. structured HTML parsing of site search results.
3. loose HTML link fallback to avoid zero-output under layout changes.

Key behavior:

1. URL acceptance excludes search, tag, and category pages.
2. extracts date from nearby node using `<time>`, class-based date blocks, French textual patterns, and numeric regex fallback.
3. extracts snippet from excerpt or teaser-like blocks.
4. enriches each row with article body.
5. query sources:
   - `direct:leconomiste_wp`
   - `direct:leconomiste_search`
6. source label: `L'Economiste`.

CLI:

- `--show`
- `--no-save`

### 15) Source-specific deep dive — Medias24 (`ATW_medias24_news.py`)

Current implemented collection method:

- Le Boursier ATW page:
  - `https://medias24.com/leboursier/fiche-action?action=attijariwafa-bank&valeur=actualites`

Key behavior:

1. collects links matching strict Medias24 article-date URL pattern:
   - `https://medias24.com/YYYY/MM/DD/<slug>/`
2. infers fallback title from URL slug when needed.
3. derives fallback date from URL date segments when body date unavailable.
4. enriches each row with article body extraction.
5. sets `query_source = direct:medias24_leboursier`.
6. source label: `Medias24`.

Important implementation note:

- script docstring describes a WordPress REST tag-based path, but current executable logic uses Le Boursier HTML links.

CLI:

- `--show`
- `--max-pages` (argument exists, passed into `scrape`, currently not used by link extraction loop)
- `--no-save`

### 16) Source-specific deep dive — Google News (`ATW_googlenews_news.py`)

Collection method:

- Google News RSS search queries across locales.

Configured queries:

1. `"Attijariwafa bank"` with `hl=fr`, `gl=MA`, `ceid=MA:fr`
2. `"Attijariwafa"` with `hl=fr`, `gl=MA`, `ceid=MA:fr`
3. `"Attijariwafa bank"` with `hl=en`, `gl=US`, `ceid=US:en`

Key behavior:

1. parses feed entries with `feedparser`.
2. host-level blocklist removes many domains not relevant for actionable market intelligence.
3. whitelist allows `ir.attijariwafabank.com` and `attijaricib.com`.
4. row source is publisher name from feed entry source field when present, otherwise link hostname fallback.
5. sets `query_source = google_news:<ceid>`.

Advanced helper present:

- `enrich_with_bodies()` supports redirect resolution for Google News wrapper URLs, per-run cache reuse, failed URL memory, and periodic state checkpoints.

Current standalone execution path:

- main path calls `scrape()` then `merge_and_save_to_csv`.
- standalone path does not call `enrich_with_bodies()` directly.

CLI:

- `--show`

### 17) Standalone execution and DB save behavior

Every source file supports standalone execution using `python news_crawler/<file>.py`.  
Typical flow:

1. scrape source items,
2. print sampled rows,
3. merge into shared CSV,
4. attempt database write through `AtwDatabase.save_news(items)` inside `try` block.

Practical difference:

- most sources have `--no-save`; Google News script does not expose `--no-save`.

### 18) Scheduler integration

`autorun/scheduler.py` news bucket behavior:

1. discovers scripts via `news_crawler/ATW_*_news.py`,
2. runs each script as subprocess every hour,
3. assigns per-source timeout (`30` minutes),
4. keeps execution independent so one source failure does not stop others.

This means news ingestion resilience comes from source isolation plus merge-level deduplication.

### 19) Failure and resilience characteristics

Hard and soft failure handling pattern:

- HTTP and parse failures usually return empty batches and continue.
- extraction exceptions in body/date helpers are downgraded to debug warnings and empty fallback values.
- DB write failure in standalone runs does not block CSV persistence.
- dedup and merge prevent duplicate growth from repeated schedule executions.

### 20) Task 5 completion note

Task 5 status: complete in this document.

## Task 6 — Database layer deep dive (`database/db.py`, `docker/init/01_schema.sql`)

### 1) Purpose in system architecture

`database/db.py` defines `AtwDatabase`, the single shared PostgreSQL adapter used by scrapers, news crawlers, and backfill scripts.  
It centralizes:

- connection lifecycle,
- write/upsert semantics,
- read/query helpers for downstream analysis.

This keeps ingestion code focused on extraction logic while database behavior stays consistent.

### 2) Technology stack and runtime dependencies

Database layer dependencies:

- `SQLAlchemy` for engine, transactions, table reflection, and SQL text execution.
- `psycopg2` through SQLAlchemy URL `postgresql+psycopg2://...`.
- `pandas` for dataframe input and dataframe output from query methods.
- `python-dotenv` to load `.env` credentials.

### 3) Connection contract and environment variables

`AtwDatabase.connect()` loads `.env` and requires:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- optional `POSTGRES_HOST` default `localhost`
- optional `POSTGRES_PORT` default `5432`

Engine URL format:

- `postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}`

Context-manager usage:

- `with AtwDatabase() as db:` automatically connects and disposes.

### 4) Schema bootstrap and container wiring

Schema file:

- `docker/init/01_schema.sql`

Container boot wiring:

- `docker-compose.yml` mounts `./docker/init` into `/docker-entrypoint-initdb.d`.
- On first Postgres initialization, schema SQL is executed automatically.

Primary database service:

- container `atw_postgres`
- image `postgres:16-alpine`
- external mapped port `${POSTGRES_PORT:-5432}`

### 5) Full table catalog and constraints

#### `bourse_daily`

Columns:

- `seance DATE PRIMARY KEY`
- `instrument TEXT`
- `ticker TEXT`
- `ouverture NUMERIC`
- `dernier_cours NUMERIC`
- `plus_haut NUMERIC`
- `plus_bas NUMERIC`
- `nb_titres NUMERIC`
- `volume NUMERIC`
- `nb_transactions INTEGER`
- `capitalisation NUMERIC`

#### `macro_morocco`

Columns:

- `date DATE PRIMARY KEY`
- `frequency_tag TEXT`
- `gdp_growth_pct NUMERIC`
- `current_account_pct_gdp NUMERIC`
- `public_debt_pct_gdp NUMERIC`
- `inflation_cpi_pct NUMERIC`
- `eur_mad NUMERIC`
- `usd_mad NUMERIC`
- `brent_usd NUMERIC`
- `wheat_usd NUMERIC`
- `gold_usd NUMERIC`
- `vix NUMERIC`
- `sp500_close NUMERIC`
- `em_close NUMERIC`
- `us10y_yield NUMERIC`
- `masi_close NUMERIC`
- `gdp_ci NUMERIC`
- `gdp_sn NUMERIC`
- `gdp_cm NUMERIC`
- `gdp_tn NUMERIC`
- `macro_momentum NUMERIC`
- `fx_pressure_eur NUMERIC`
- `global_risk_flag SMALLINT`

#### `news`

Columns:

- `id BIGSERIAL PRIMARY KEY`
- `date TIMESTAMPTZ`
- `ticker TEXT`
- `title TEXT`
- `source TEXT`
- `url TEXT UNIQUE`
- `full_content TEXT`
- `query_source TEXT`
- `signal_score INTEGER`
- `is_atw_core SMALLINT`
- `scraping_date TIMESTAMPTZ`

Indexes:

- `idx_news_ticker_date` on `(ticker, date DESC)`

#### `fondamental_snapshot`

Columns:

- `id BIGSERIAL PRIMARY KEY`
- `symbol TEXT NOT NULL`
- `scrape_timestamp TIMESTAMPTZ NOT NULL`
- `payload JSONB NOT NULL`

Constraint:

- `UNIQUE (symbol, scrape_timestamp)`

#### `fondamental_yearly`

Columns:

- `symbol TEXT NOT NULL`
- `year INTEGER NOT NULL`
- `metric TEXT NOT NULL`
- `value NUMERIC`

Primary key:

- `(symbol, year, metric)`

#### `bourse_intraday`

Columns:

- `snapshot_ts TIMESTAMPTZ PRIMARY KEY`
- `cotation_ts TIMESTAMPTZ`
- `ticker TEXT NOT NULL DEFAULT 'ATW'`
- `market_status TEXT`
- `last_price NUMERIC`
- `open NUMERIC`
- `high NUMERIC`
- `low NUMERIC`
- `prev_close NUMERIC`
- `variation_pct NUMERIC`
- `shares_traded NUMERIC`
- `value_traded_mad NUMERIC`
- `num_trades INTEGER`
- `market_cap NUMERIC`

Indexes:

- `idx_intraday_ticker_ts` on `(ticker, snapshot_ts DESC)`

#### `technicals_snapshot`

Columns:

- `id BIGSERIAL PRIMARY KEY`
- `symbol TEXT NOT NULL DEFAULT 'ATW'`
- `computed_at TIMESTAMPTZ NOT NULL`
- `as_of_date DATE`
- `payload JSONB NOT NULL`

Constraint and index:

- `UNIQUE (symbol, computed_at)`
- `idx_tech_symbol_date` on `(symbol, as_of_date DESC)`

### 6) `AtwDatabase` lifecycle internals

Core lifecycle methods:

1. `connect()` creates engine only once.
2. `close()` disposes engine and clears handle.
3. `__enter__` and `__exit__` provide context-manager safety.
4. `_require()` lazily connects when a save or get method is called directly.

Behavior:

- all methods either reuse existing engine or create one on first call.

### 7) Generic upsert helper `_upsert_ignore`

Shared helper signature:

- `_upsert_ignore(table: str, df: pd.DataFrame, conflict_cols: list[str]) -> int`

Pipeline:

1. returns `0` for empty dataframe.
2. converts `NaN` to `None`.
3. converts dataframe to records list.
4. reflects target table columns from live database metadata.
5. drops unknown input columns.
6. executes `INSERT ... ON CONFLICT DO NOTHING` using PostgreSQL dialect `insert`.

Return value:

- returns `len(records)` submitted, not the exact number newly inserted after conflict skipping.

### 8) `save_bourse` write behavior

`save_bourse(df)`:

1. renames market columns using `BOURSE_RENAME`:
   - `Séance` to `seance`
   - `Instrument` to `instrument`
   - `Ticker` to `ticker`
   - `Ouverture` to `ouverture`
   - `Dernier Cours` to `dernier_cours`
   - `+haut du jour` to `plus_haut`
   - `+bas du jour` to `plus_bas`
   - `Nombre de titres échangés` to `nb_titres`
   - `Volume des échanges` to `volume`
   - `Nombre de transactions` to `nb_transactions`
   - `Capitalisation` to `capitalisation`
2. parses `seance` to date and drops invalid dates.
3. calls `_upsert_ignore("bourse_daily", ..., ["seance"])`.

### 9) `save_macro` write behavior

`save_macro(df)`:

1. copies dataframe.
2. parses `date` to date and drops invalid dates.
3. calls `_upsert_ignore("macro_morocco", ..., ["date"])`.

### 10) `save_news` write behavior

`save_news(rows)` accepts either:

- iterable of dictionaries,
- pandas dataframe.

Normalization:

1. drops rows without URL.
2. ensures all `NEWS_COLUMNS` keys exist.
3. normalizes empty `full_content` to `NULL`.
4. fills missing `scraping_date` with current UTC timestamp.

Conflict rule on `url`:

1. new URL inserts full row.
2. existing URL updates only when existing `full_content` is empty and incoming `full_content` is non-empty.
3. when existing row already has non-empty `full_content`, update is skipped.

Updated fields during eligible enrichment:

- `full_content`
- `scraping_date`
- `title` only if existing title is empty
- `date` only if existing date is null

Return tuple:

- `(inserted, enriched)`

Computation:

1. counts total rows and rows with non-empty content before write.
2. executes upsert.
3. recounts after write.
4. derives:
   - `inserted = after_total - before_total`
   - `enriched = (after_with_content - before_with_content) - inserted`, clamped to minimum zero.

### 11) `save_fondamental` dual-write behavior

`save_fondamental(doc)` writes to two tables:

1. `fondamental_snapshot`:
   - stores full document as JSONB with `(symbol, scrape_timestamp)` uniqueness.
2. `fondamental_yearly`:
   - expands every metric mapping `{year: value}` into normalized rows.
   - upserts on `(symbol, year, metric)` and updates value on conflict.

Return tuple:

- `(1, number_of_yearly_rows)` when document is present,
- `(0, 0)` when empty.

### 12) `save_intraday` behavior

`save_intraday(snapshots, ticker="ATW")` accepts dataframe or iterable records.

Normalization:

1. extracts timestamp from `snapshot_ts` or fallback `timestamp`.
2. skips records without timestamp.
3. fills canonical row with allowed `INTRADAY_COLUMNS`.
4. assigns ticker fallback when missing.

Insert semantics:

- `INSERT INTO bourse_intraday ... ON CONFLICT (snapshot_ts) DO NOTHING`

Return value:

- exact inserted count computed as `after_count - before_count`.

### 13) `save_technicals` behavior

`save_technicals(doc, symbol="ATW", computed_at=None)`:

1. returns `0` when payload empty.
2. chooses `computed_at` argument or current UTC timestamp.
3. takes `as_of_date` from payload key if present.
4. inserts JSONB row into `technicals_snapshot`.
5. ignores duplicates on `(symbol, computed_at)`.

Return value:

- rowcount from insert execution, typically `1` for new row and `0` for duplicate.

### 14) Read/query method catalog

`AtwDatabase` read methods:

1. `get_bourse(start=None, end=None)`:
   - reads `bourse_daily`, optional date filters, ordered by `seance`.
2. `get_macro(start=None, end=None)`:
   - reads `macro_morocco`, optional date filters, ordered by `date`.
3. `get_news(ticker="ATW", limit=100, only_with_content=False)`:
   - reads `news`, optionally requires non-empty body, ordered by newest date.
4. `get_fondamental_latest(symbol="ATW")`:
   - returns latest `payload` JSON from `fondamental_snapshot`.
5. `get_fondamental_yearly(symbol="ATW")`:
   - returns normalized yearly metric table for symbol.
6. `get_intraday(ticker="ATW", start=None, end=None, limit=None)`:
   - reads `bourse_intraday`, newest first, optional window and limit.
7. `get_technicals_latest(symbol="ATW")`:
   - returns most recent technicals JSON payload.
8. `get_technicals_history(symbol="ATW", limit=50)`:
   - returns historical technicals rows with payload.

Return types:

- pandas dataframe for table-style methods,
- dictionary for latest JSON payload methods.

### 15) Transaction model and error propagation

All writes run in `engine.begin()` transaction blocks.  
If one statement in the block fails:

- the transaction is rolled back by SQLAlchemy context behavior,
- exception propagates to caller unless caller catches it.

No silent fallback is implemented in `AtwDatabase` itself.

### 16) Cross-module write integration map

Writers in project modules:

1. `scrapers/atw_realtime_scraper.py`:
   - `save_intraday`
   - `save_technicals`
   - `save_bourse` during finalize.
2. `scrapers/atw_macro_collector.py`:
   - `save_macro`.
3. `scrapers/fondamental_scraper.py`:
   - `save_fondamental`.
4. all `news_crawler/ATW_*_news.py`:
   - `save_news`.
5. `scripts/backfill_realtime_to_db.py`:
   - `save_intraday`
   - `save_technicals`.

### 17) Supporting scripts and overlap

`scripts/load_data.py` provides a parallel direct-loading route without `AtwDatabase` wrapper:

1. builds engine from same env variables.
2. upserts:
   - `bourse_daily`
   - `macro_morocco`
   - `news`
   - `fondamental_snapshot`
   - `fondamental_yearly`
3. intended for historical seed load from files in `data/`.

`scripts/backfill_realtime_to_db.py` uses `AtwDatabase` directly for one-off migration from `data/atw_realtime_state.json`.

### 18) Idempotency and dedup guarantees

Idempotency is enforced through primary keys and unique constraints:

- `bourse_daily` by `seance`
- `macro_morocco` by `date`
- `news` by `url`
- `fondamental_snapshot` by `(symbol, scrape_timestamp)`
- `fondamental_yearly` by `(symbol, year, metric)`
- `bourse_intraday` by `snapshot_ts`
- `technicals_snapshot` by `(symbol, computed_at)`

Result:

- repeated runs do not duplicate key-identical rows.

### 19) Operational caveats and implementation details

Important practical notes:

1. `save_news` only enriches rows where previous body is empty; it does not overwrite existing non-empty `full_content`.
2. `_upsert_ignore` reports submitted row count, not exact inserted row count after conflict skipping.
3. `get_news` filter clause allows passing `ticker=None` to remove ticker restriction because SQL condition is `(ticker = :ticker OR :ticker IS NULL)`.
4. schema initialization SQL runs only on first container initialization when Postgres data directory is empty.
5. `.env.example` uses `POSTGRES_PORT=5433`, while compose mapping defaults to `5432` when variable missing; runtime behavior follows actual `.env`.

### 20) Task 6 completion note

Task 6 status: complete in this document.

## Task 7 — Valuation engine deep dive (`models/fundamental_models.py`)

### 1) Purpose in the architecture

This module is Whale’s valuation computation core.  
It transforms merged market/fundamental inputs into per-share intrinsic values using five independent methods:

1. `dcf`
2. `ddm`
3. `graham`
4. `relative`
5. `monte_carlo`

It is designed so each model can fail gracefully (insufficient data) without breaking the full output payload.

### 2) Output contract

Each model returns a `ValuationResult` object with:

- `model_name`
- `intrinsic_value`
- `intrinsic_value_low`
- `intrinsic_value_high`
- `upside_pct`
- `confidence`
- `methodology`
- `details` (model-specific diagnostics)

CLI output file:

- `data/models_result.json`

JSON shape:

- top-level object with keys: `dcf`, `ddm`, `graham`, `relative`, `monte_carlo`
- each key stores a serialized `ValuationResult`.

### 3) Core constants and assumptions

Global constants in module:

- `RISK_FREE_RATE = 3.5%`
- `EQUITY_RISK_PREMIUM = 6.5%`
- `CORPORATE_TAX_RATE = 31%`
- `TERMINAL_GROWTH_RATE = 2.5%`
- `STOCK_BETA = 0.90`
- `NUM_SHARES = 215,140,839`
- `COST_OF_EQUITY = Rf + beta * ERP`

Sector anchors (`SECTOR_BENCHMARKS`) are used mainly by Relative Valuation blends.

Unit convention inside models:

- enterprise/equity values are mostly handled in **millions MAD**
- per-share outputs are converted via: `(equity_value_m * 1_000_000) / NUM_SHARES`.

### 4) Loader merge strategy (`load_stock_data`)

`load_stock_data()` always starts from the latest Bourse Casa market row and then merges fundamental layers.

Input files considered:

1. Required:
   - `data/ATW_bourse_casa_full.csv` (or `<ticker>_bourse_casa_full.csv`)
2. Primary fundamentals mode:
   - `data/ATW_fondamental.json`
   - else `data/historical/ATW_merged.json`
3. Legacy fallback mode:
   - `data/historical/ATW_marketscreener_v3.json`
   - `data/ATW_fondamental.csv`
4. Legacy model-input overlay (only when primary merged JSON is absent):
   - `data/ATW_model_inputs.json`

Merge behavior:

1. map raw scraper payload into canonical sections (`price_performance`, `financials`, `valuation`);
2. recursive merge with `_deep_merge` (later source overrides earlier values);
3. hard override from Bourse row for:
   - `current_price`
   - `price_performance.last_price`
   - `valuation.market_cap` (converted from MAD to millions MAD).

If the Bourse file is missing or `Dernier Cours` cannot be parsed, loader raises hard errors.

### 5) Canonical in-memory data schema used by models

Models consume a nested dictionary with these root sections:

- `identity`
- `current_price`
- `price_performance`
- `financials`
- `valuation`
- `consensus`

All helper getters in `BaseValuationModel` read through this schema, including:

- `_get_financial(field, year)`
- `_get_valuation(field)`
- `_get_hist_values(section, field, years=None)`
- `_compute_upside(fair_value)`.

### 6) DCF model (`DCFModel`) — formulas and fallbacks

Methodology: two-stage DCF + Gordon terminal value.

Core flow:

1. compute WACC  
   `WACC = we*Ke + wd*Kd*(1-tax)`
2. build FCF projections;
3. extend to 5 years if needed;
4. compute terminal value:
   - `TV = FCF_t * (1+g)/(WACC-g)` when `WACC > g`
   - fallback `TV = FCF_t * 20` when `WACC <= g`
5. discount projected FCF + terminal value;
6. convert EV to equity: `EV - net_debt + cash`;
7. convert to per-share intrinsic value.

FCF sourcing logic:

1. preferred: positive forecast `financials.free_cash_flow` years `>= 2026`;
2. fallback: approximate FCF from earnings and capex:
   - use `ebitda` if available, else `ebit`
   - `FCF ~= earnings*(1-tax) - abs(capex)` for years `2025/2024/2023`
3. last resort: latest positive historical FCF.

Scenario banding:

- low case uses `WACC + 1%`, `g - 0.5%`
- high case uses `WACC - 1%`, `g + 0.5%`.

Confidence rule:

- `min(80, 40 + len(fcf_projections)*8)`.

### 7) DDM model (`DDMModel`) — formulas and fallbacks

Methodology: three-stage dividend discount model.

Flow:

1. Stage 1 explicit dividends from `valuation.dividend_per_share_hist` forecast years `>= 2026`.
2. If no forecast series, fallback to latest positive historical DPS as one-point base.
3. Compute stage-1 growth as average YoY growth (or terminal growth fallback).
4. Build stage-2 transition dividends (5 years) by linearly fading growth toward terminal growth.
5. Terminal value:
   - `TV = D_(t+1)/(Ke-g)` when `Ke > g`
   - fallback `TV = D_(t+1) * 30` when `Ke <= g`
6. Discount dividends + terminal value to present.

Sensitivity band:

- low/high recomputed with `Ke + 1%` and `Ke - 1%`.

Confidence rule:

- `min(75, 35 + len(stage1_dividends)*10)`.

### 8) Graham model (`GrahamModel`) — formulas and guardrails

Implements three Graham-style references:

1. **Graham Number**:  
   `sqrt(22.5 * EPS * BVPS)`
2. **Graham Growth Formula**:  
   `EPS * (8.5 + 2g) * 4.4 / Y`
   - `g` estimated from realized history and capped at `10%`
   - `Y` uses `max(risk_free_rate*100, 3.0)`
3. **NCAV per share** (optional):  
   `(total_assets - total_liabilities) * 1_000_000 / NUM_SHARES`

Bank-aware / robustness behavior:

- uses latest **reported** year cutoff based on `shareholders_equity` to avoid mixing forward EPS forecasts into Graham growth estimates;
- BVPS fallback uses shareholders’ equity per share if direct BVPS series is missing.

Final selection:

1. if both Graham Number and Growth are available, fair value = their average;
2. else use the available one;
3. low/high come from min/max across positive computed references.

### 9) Relative valuation model (`RelativeValuationModel`)

Composite built from available implied values, with weights:

- `pe`: 0.25
- `ev_ebitda`: 0.35
- `pb`: 0.15
- `ev_revenue`: 0.10
- `fcf_yield`: 0.15

Only available components are included; weights are re-normalized over present signals.

#### PE block

- historical median `pe_ratio_hist` (2021–2025)
- blended multiple = `0.6*historical + 0.4*sector_benchmark`
- fair value = blended PE × latest EPS.

#### EV/EBITDA block (with bank fallback)

Primary path:

- use `ev_ebitda_hist` median + sector blend × latest EBITDA.

Fallback path (bank-compatible):

- if EBITDA path unavailable, switch to `ev_ebit_hist` × latest EBIT.

Both paths then convert EV to equity using `-net_debt + cash`, then to per-share.

#### P/B block

- historical `pbr_hist` median + sector blend
- fair value = blended PB × BVPS
- BVPS fallback from shareholders’ equity per share.

#### EV/Revenue block

- blended multiple from `ev_revenue_hist` + sector `ev_sales`
- fair value = blended EV/Revenue × `net_sales(2025)` then EV-to-equity conversion.

#### FCF yield block

- compares current FCF yield to historical median FCF yield:
  - `fair_value = current_price * (current_yield / historical_median_yield)`.

Confidence rule:

- `min(80, 30 + number_of_available_components*10)`.

### 10) Monte Carlo model (`MonteCarloModel`)

Methodology: stochastic DCF with `10,000` simulations and deterministic seed (`42`).

Base parameter extraction:

- base revenue from latest available `net_sales` (prefer 2025 -> 2021 order)
- base margin from median of recent margins (`ebitda_margin` -> `ebit_margin` -> `net_margin`)
- base growth from revenue CAGR (capped to 1%..10%, default 3%)
- capex ratio from recent capex/sales (default 15%)
- net debt and cash from financials.

Simulation draws:

- revenue growth ~ Normal(base_growth, 2.5%)
- margin ~ Normal(base_margin, 4%)
- WACC ~ Uniform(6.5%, 9.5%)
- terminal growth ~ Uniform(1.5%, 3.5%)
- capex ratio ~ Normal(base_capex, 2%) then clipped.

Bank-aware mode:

- if margin type is `net`, module switches to bank mode:
  - margin clipped to `5%..45%`,
  - capex forced to zero,
  - FCF approximation uses `revenue * net_margin` (already post-tax).

Corporate mode:

- uses `FCF = EBITDA*(1-tax) - capex`.

Post-processing:

1. discard invalid tails (`<=0` or `>=5000`);
2. report median as intrinsic value;
3. report P10/P90 as low/high;
4. include probability fair value > current price;
5. confidence derived from coefficient of variation:
   - `max(30, min(75, 75 - cv*100))`.

### 11) Bank-aware fallback summary across all models

Key banking-specific adaptations in this file:

1. DCF uses EBIT fallback when EBITDA-based path is unavailable.
2. Relative valuation switches EV/EBITDA to EV/EBIT when needed.
3. Monte Carlo enters bank mode when net margin is the active margin basis.
4. Graham growth estimation limits optimism (realized years + growth cap).

These guardrails reduce overvaluation risk from non-bank corporate assumptions.

### 12) CLI interface

Examples:

```bash
python models/fundamental_models.py
python models/fundamental_models.py --model all
python models/fundamental_models.py --model dcf
python models/fundamental_models.py --ticker ATW
python models/fundamental_models.py --data-dir data
```

Flags:

- `--model`: `graham | dcf | ddm | relative | monte_carlo | all` (default `graham`)
- `--ticker`: input prefix (default `ATW`)
- `--data-dir`: custom data directory (default `<project_root>/data`).

Important runtime behavior:

- the script computes **all five models every run**, then:
  - prints only selected model unless `--model all`,
  - still writes full five-model payload to `models_result.json`.

### 13) Failure behavior and reliability profile

Hard failures (exceptions):

- missing/invalid primary market CSV
- invalid JSON object structure in input files.

Soft failures (model-level graceful degradation):

- each model can return intrinsic value `0` with confidence `0` and explicit methodology note when required data is missing.

Design implication:

- end-to-end valuation run is resilient even if one or more model blocks are data-starved.

### 14) Task 7 completion note

Task 7 status: complete in this document.

## Task 8 — Agent News deep dive (`agents/agent_news.py`)

### 1) Purpose in the architecture

`agent_news.py` is Whale’s **live news synthesis** script.  
It does two stages in one run:

1. pull fresh web hits via DuckDuckGo search (`ddgs`),
2. synthesize a structured ATW brief via Groq LLM into a strict Pydantic schema.

It is intentionally standalone and ephemeral:

- terminal output only,
- no database writes,
- no file writes.

### 2) Input and output contract

#### Inputs

1. environment:
   - `.env` loaded from project root
   - `GROQ_API_KEY` required
   - optional `GROQ_MODEL`
2. runtime search inputs:
   - default 5 ATW/macro/global query angles from `ATW_SEARCH_HINTS`,
   - or one custom query via `--query`
3. optional depth control:
   - `--per-query` result cap per DDG query.

#### Outputs

Two mutually exclusive terminal formats:

1. formatted brief (`_print_brief`) by default,
2. raw JSON (`--raw`) from `NewsBrief.model_dump_json`.

Exit codes:

- `0` success
- `2` missing `GROQ_API_KEY`
- `3` LLM output not parseable as `NewsBrief`.

### 3) Pydantic schema contract (strict structure)

#### `NewsItem` fields

- `date`: publication date string (`YYYY-MM-DD`) when known, else today
- `title`
- `source`
- `url`
- `summary` (1 sentence expected by prompt)
- `category` enum:
  - `EARNINGS`, `DIVIDEND`, `ANALYST`, `REGULATORY`,
  - `MA`, `MACRO`, `AFRICA`, `GEOPOLITICS`, `COMMODITY`, `OTHER`
- `signal_score` integer `0..100`
- `is_atw_core` boolean
- `bucket` enum: `HIGH`, `MEDIUM`, `CONTEXT`, `NOISE`.

#### `NewsBrief` fields

- `as_of_date`
- `items: list[NewsItem]`
- `sector_pulse`
- `sentiment_verdict` (`POSITIVE|NEUTRAL|NEGATIVE`)
- `sentiment_reasoning`.

Practical effect:

- schema validation enforces output shape and value ranges before display.

### 4) Search stage flow (`run_searches`)

`run_searches(queries, per_query=4)` behavior:

1. opens `DDGS()` context;
2. loops each query and calls `ddg.text(query, max_results=per_query)`;
3. catches per-query exceptions and continues (non-fatal);
4. normalizes URL from `href` fallback to `url`;
5. deduplicates by exact URL string with `seen_urls` set;
6. stores compact hit objects:
   - `query`, `title`, `url`, `snippet`.

Search-stage resilience:

- one failed query does not fail the whole run.

### 5) Prompt packaging for synthesis (`_format_hits_for_llm`)

Prompt payload includes:

1. explicit `Today: YYYY-MM-DD`,
2. total hit count,
3. numbered hit blocks with query/title/url/snippet.

Normalization applied before prompt:

- snippets are newline-stripped,
- snippets are truncated to 300 chars,
- empty-hit case generates explicit instruction to return `items=[]`.

This makes the LLM call deterministic in format and bounded in prompt size.

### 6) Synthesis agent configuration (`_build_synth_agent`)

LLM setup:

- provider/model: `Groq(id=GROQ_MODEL or "openai/gpt-oss-120b")`
- `max_tokens=4096`
- `temperature=0.2`
- output schema: `NewsBrief`
- instructions: `SYNTH_INSTRUCTIONS` list.

Important architecture choice:

- all web search is done outside the LLM, then passed as static context,
- no model-side tool calling is used.

### 7) Prompt constraints that govern behavior

`SYNTH_INSTRUCTIONS` enforces:

1. use only provided hits (no invented links/dates/sources),
2. fill all `NewsItem` fields,
3. score impact using the 0–100 rubric,
4. derive bucket from score thresholds,
5. exclude `NOISE` from returned `items`,
6. keep summaries to one sentence,
7. cap output to at most 8 items,
8. always fill `sector_pulse`, verdict, and reasoning,
9. include global topics only if plausibly linked to Morocco/ATW.

### 8) Terminal rendering and output interpretation

Formatted mode prints:

1. header: `ATW NEWS BRIEF — <as_of_date>`
2. grouped sections by bucket:
   - `HIGH` (🔴)
   - `MEDIUM` (🟡)
   - `CONTEXT` (🟢)
3. each item with source/title/summary/category/score/core/url
4. `SECTOR PULSE`
5. final verdict and reasoning.

Interpretation semantics:

- `signal_score >= 75`: likely price-moving
- `60..74`: meaningful but less immediate
- `30..59`: contextual backdrop
- `<30`: noise (filtered from `items` by instruction).

### 9) CLI interface

Supported commands:

```bash
python agents/agent_news.py
python agents/agent_news.py --raw
python agents/agent_news.py --per-query 6
python agents/agent_news.py --query "Only ATW Q1 2026 earnings"
```

Flags:

- `--query`: replace default multi-angle search list with one custom query
- `--raw`: print raw JSON instead of formatted grouped brief
- `--per-query`: maximum DDG results per query (default `4`).

### 10) Failure modes and caveats

1. Missing `GROQ_API_KEY` hard-stops the script with exit `2`.
2. If model output cannot be parsed into `NewsBrief`, script prints raw content and exits `3`.
3. DDG query failures are logged but non-fatal; run may complete with fewer hits.
4. URL dedup is exact-string only (no advanced canonicalization), so near-duplicate URL variants can still pass through.
5. Date assignment in news items is model-driven from snippets/today context, not a deterministic parser.

### 11) Relationship with the rest of Whale

`agent_news.py` is complementary to crawler-based historical pipelines:

- crawlers in `news_crawler/` build persistent dataset history (`ATW_news.csv` + DB flow),
- `agent_news.py` provides an on-demand, live, quick intelligence snapshot.

So it is a **real-time brief layer**, not a storage pipeline.

### 12) Task 8 completion note

Task 8 status: complete in this document.

## Task 9 — Agent Analyse deep dive (`agents/agent_analyse.py`)

### 1) Purpose in the architecture

`agent_analyse.py` is Whale’s **holistic decision agent** for ATW.  
It consolidates five local data domains:

1. market (`ATW_bourse_casa_full.csv`)
2. macro (`ATW_macro_morocco.csv`)
3. news (`ATW_news.csv`)
4. fundamentals (`ATW_fondamental.json`)
5. model valuations (`models_result.json`)

Then it produces a cited structured verdict (`BUY/HOLD/SELL`) plus two horizon predictions:

- short-term trading plan (4 weeks)
- medium-term investment plan (12 months).

### 2) Input and output contract

#### Inputs

Local files under `data/`:

- `ATW_bourse_casa_full.csv`
- `ATW_macro_morocco.csv`
- `ATW_news.csv`
- `ATW_fondamental.json`
- `models_result.json`

Environment:

- `.env` at project root
- required `GROQ_API_KEY`
- optional `GROQ_MODEL` (default `openai/gpt-oss-120b`).

#### Outputs

Primary terminal output:

- formatted analysis report (default), or
- raw `ATWAnalysis` JSON (`--raw`).

Secondary persistent output (default enabled):

- append row to `data/prediction_history.csv` (disable with `--no-history`).

Exit codes:

- `0` success
- `2` missing `GROQ_API_KEY`
- `3` LLM synthesis/schema failure.

### 3) Strict response schema (`ATWAnalysis`)

Pydantic response tree:

1. top-level: `as_of_date`, `last_close_mad`, fair-value range, `upside_pct`, `findings`, `risks`, `verdict`, `conviction`, `verdict_reasoning`
2. `findings[]`: dimensioned entries (`MARKET|MACRO|NEWS|FUNDAMENTAL|VALUATION`) with polarity and mandatory evidence citations
3. `trading_prediction`: numeric short-term plan + confidence + thesis
4. `investment_prediction`: valuation-based target/recommendation + confidence + thesis.

Citation enforcement:

- every finding requires `evidence` with at least one `source_ref` that must match a bracketed ID emitted in the evidence block.

### 4) Source loaders and normalization

#### Market loader

`load_market_snapshot()`:

- parses/sorts `Séance`,
- computes returns (`1m/3m/6m/52w`) from close series,
- computes 14-day Wilder ATR,
- tracks 52w high/low and 4w high/low.

#### Macro loader

`load_macro_snapshot()`:

- parses daily macro series,
- applies defensive scrub for implausible CPI/debt outliers:
  - `inflation_cpi_pct > 50` or `public_debt_pct_gdp > 200` -> nullified
- computes 90-day deltas for EUR/MAD, Brent, MASI.

#### News loader

`load_news_window()`:

- parses date as UTC,
- filters by lookback window and minimum signal score,
- sorts by `signal_score` descending,
- caps rows (`DEFAULT_NEWS_CAP=5`),
- truncates article content summary to fixed length.

Default filters:

- lookback `14` days
- minimum score `50`.

#### Fundamentals loader

`load_fundamentals()`:

- reads merged JSON snapshot,
- keeps only recent year slices (last 2 years) for key metrics:
  revenue, net income, EPS, equity, cash, FCF, net margin, ROE, PE, DPS.

#### Valuations loader

`load_valuations()`:

- reads `models_result.json`,
- maps model keys to tags:
  - `dcf -> DCF`
  - `ddm -> DDM`
  - `graham -> GRAHAM`
  - `relative -> RELATIVE`
  - `monte_carlo -> MC`
- builds estimate objects with intrinsic value/range/upside/confidence/methodology.

### 5) Deterministic prediction engine (non-LLM)

Both prediction blocks are computed in Python before synthesis and inserted into evidence as `[PRED-*]`.

#### Trading prediction (4-week ATR framework)

`compute_trading_prediction()`:

1. entry zone: `[last - ATR, last]` bounded by 4-week low
2. stop loss: lower entry minus `2 * ATR`
3. target: `last + 1.5 * ATR * sqrt(horizon_days)` capped by 52w high
4. expected return and risk/reward from target/stop geometry
5. confidence from ATR ratio + count of high-score news (`>=75`).

#### Investment prediction (12-month valuation framework)

`compute_investment_prediction()`:

1. target = confidence-weighted midpoint of model intrinsic values
2. low/high from min/max model bounds
3. upside from target vs last close
4. dividend yield from latest DPS
5. expected total return using reinvested TSR form:
   - `TSR = capital_gain + (1 + capital_gain) * dividend_yield`
6. recommendation thresholds:
   - `ACHAT` if upside `> +15%`
   - `VENDRE` if upside `< -10%`
   - `CONSERVER` otherwise.

Important trust model in main flow:

- numeric prediction fields are always overwritten by deterministic Python values after LLM response; the LLM contributes thesis text only.

### 6) Strategy filter layer (present but optional/not applied in main path)

Filter helpers exist for tactical veto logic:

1. bear regime (`3m return < -5%`)
2. top-buying risk (`1m return > 8%` and too close to 52w high)
3. cooldown after repeated stops.

Orchestrator function:

- `apply_strategy_filters(...)` (first-hit-wins).

Current status in this script:

- filters are defined but not called in `main()`, so they are available for integration but inactive in default execution path.

### 7) Evidence block architecture (stable ID contract)

`compose_evidence_block()` builds one deterministic prompt section with:

1. MARKET `[MKT-*]`
2. MACRO `[MACRO-*]`
3. NEWS `[NEWS-*]`
4. FUNDAMENTAL `[FUND-*]`
5. VALUATION `[VAL-*]`
6. PREDICTIONS `[PRED-*]`

Guardrails embedded in block header:

- cite only provided IDs,
- never invent IDs/values,
- copy prediction numbers verbatim from `[PRED-*]`.

This is the core mechanism that makes citations auditable and reproducible.

### 8) LLM synthesis rules and verdict logic

Synthesis model:

- Groq agent, `temperature=0.2`, `max_tokens=4096`, output schema `ATWAnalysis`.

Instruction highlights:

1. use only evidence-block facts
2. provide findings across all available dimensions
3. compute upside from fair-value midpoint formula
4. strict verdict policy:
   - `BUY` if upside `> +15` and macro/news net polarity not bearish
   - `SELL` if upside `< -10` or fundamentals clearly deteriorate
   - `HOLD` otherwise
5. conviction rules based on dimension coverage + polarity alignment
6. risks must be concrete and sourced from NEWS/MACRO context
7. trading thesis must be in French and cite `[MKT-*]/[NEWS-*]/[PRED-TRADE-*]`
8. investment thesis must be in French and cite `[VAL-*]/[FUND-*]/[PRED-INV-*]`.

### 9) Formatter and operator-facing output

`print_analysis()` renders:

- headline valuation snapshot (last close, fair-value range, upside),
- findings grouped by dimension with polarity icons and citation IDs,
- risks section,
- trading block (entry/target/stop/ATR/RR/confidence + thesis),
- investment block (target range/upside/dividend/TSR/recommendation + thesis),
- final verdict and conviction.

Display conventions:

- verdict emoji mapping: BUY/HOLD/SELL
- recommendation emoji mapping: ACHAT/CONSERVER/VENDRE.

### 10) History logging behavior

`save_prediction_history()` appends one row per run to `prediction_history.csv` with:

- timestamp + as-of date
- valuation range and upside
- verdict and conviction
- full trading metrics
- full investment metrics.

Write mode is append-only with header on first write.

### 11) CLI interface

Supported commands:

```bash
python agents/agent_analyse.py
python agents/agent_analyse.py --raw
python agents/agent_analyse.py --asof 2026-04-25 --lookback-days 7
python agents/agent_analyse.py --evidence-only
python agents/agent_analyse.py --min-news-score 60
python agents/agent_analyse.py --no-history
```

Flags:

- `--asof`: override today anchor (`YYYY-MM-DD`)
- `--lookback-days`: news window
- `--min-news-score`: score threshold for included news
- `--raw`: print raw JSON
- `--evidence-only`: print assembled evidence and skip LLM
- `--no-history`: skip CSV history append.

### 12) Reliability profile and caveats

1. LLM output type is strictly checked; non-schema output triggers explicit synthesis error.
2. Numeric predictions remain deterministic even when LLM narratives vary.
3. News influence depends on recency/score filters; overly strict thresholds can leave sparse NEWS findings.
4. Missing or malformed source files will fail earlier in loader stage (no silent fallback in this script).

### 13) Task 9 completion note

Task 9 status: complete in this document.

## Task 10 — Automation deep dive (`autorun/scheduler.py`)

### 1) Purpose in the architecture

`autorun/scheduler.py` is Whale’s built-in cron replacement (especially for Windows).  
It runs as one long-lived Python process that:

1. wakes up every fixed tick (`30s`),
2. checks which job buckets are due,
3. executes each bucket as subprocess commands with per-job timeout control.

It orchestrates collection freshness without external scheduler dependencies.

### 2) Buckets and command mapping

Three buckets are defined:

1. `NEWS` (interval):
   - dynamic command list from `news_crawler/ATW_*_news.py`
   - one subprocess per source script
2. `REALTIME` (interval):
   - `python scrapers/atw_realtime_scraper.py snapshot`
3. `MONTHLY` (monthly trigger):
   - `python scrapers/fondamental_scraper.py`
   - `python scrapers/atw_macro_collector.py`.

Command lists are built at fire time, not hard-cached at startup.

### 3) Cadence model

Configured cadences:

- `NEWS_INTERVAL = 1 hour`
- `REALTIME_INTERVAL = 15 minutes`
- `MONTHLY = day 1 at 02:00`.

Scheduler scanning cadence:

- loop sleep `TICK_SECONDS = 30`.

Initial `next_fire` behavior:

1. `NEWS`: immediate at startup (`next_fire=now`)
2. `REALTIME`: immediate only if market-open window is active; otherwise parked to next opening slot
3. `MONTHLY`: next calendar monthly trigger.

### 4) Market-time window logic for REALTIME bucket

REALTIME is gated by a trading-time window:

- open hour: `09:00` (inclusive)
- close hour: `16:00` (exclusive)
- weekends skipped (`Sat/Sun`).

Functions:

- `is_market_open(now)`: weekday + hour gate
- `next_market_open(after)`: next valid weekday at 09:00

Runtime behavior:

- if REALTIME fire occurs outside market window, scheduler does not run snapshot; it logs a “market closed” park message and reschedules to the next opening slot.

### 5) Monthly trigger semantics

`next_monthly(after)` computes the next strict trigger point at:

- day `1`
- hour `02`
- minute `00`.

If current month’s target is already passed, it rolls to next month (including year rollover from December to January).

### 6) Timeout model and failure codes

Per-command timeout caps:

- news source script: `30 min`
- realtime snapshot: `5 min`
- monthly scraper: `60 min`.

Execution wrapper (`run_command`) behavior:

1. runs subprocess with project root as CWD,
2. returns actual process exit code on completion,
3. returns `-1` on timeout,
4. returns `-2` on unexpected execution crash.

Bucket behavior:

- `run_bucket()` executes commands sequentially and always continues to next command even if one fails.

### 7) Rescheduling logic

After a bucket fires:

1. interval jobs (`NEWS`, `REALTIME`): `next_fire = fired_at + interval`
2. monthly job: `next_fire = next_monthly(fired_at)`.

This ensures fixed cadence progression from actual execution time rather than original planned timestamp.

### 8) `--once` operational modes

CLI supports one-shot execution:

```bash
python autorun/scheduler.py --once NEWS
python autorun/scheduler.py --once REALTIME
python autorun/scheduler.py --once MONTHLY
```

Behavior:

- builds jobs once,
- selects the requested bucket,
- executes that bucket immediately,
- exits without entering scheduler loop.

### 9) `--status` mode

Status command:

```bash
python autorun/scheduler.py --status
```

Prints current schedule plan and exits:

- bucket label
- schedule kind (`interval`/`monthly`)
- computed `next_fire` timestamp.

Useful for quick operational checks without running jobs.

### 10) Logging and observability model

Logger setup (`setup_logging`):

- dual sinks:
  - file: `autorun/autorun.log`
  - stdout stream
- UTF-8 encoding
- timestamped format: `YYYY-MM-DD HH:MM:SS [LEVEL] message`.

Runtime log events include:

1. scheduler startup + initial next-fire table
2. per-bucket start/end
3. per-command start + exit code + elapsed seconds
4. timeout/crash events
5. REALTIME parked messages when market is closed
6. next-fire update after each run
7. clean stop notice on `KeyboardInterrupt`.

### 11) Operational behavior notes

1. NEWS bucket command discovery is pattern-driven (`ATW_*_news.py`), so adding/removing source scripts changes scheduler coverage automatically.
2. Subprocess output is not captured (`capture_output=False`), so child logs print directly to console/log stream context.
3. Bucket execution is serial, not parallel; long-running jobs can delay subsequent due checks.
4. No persistent state file is used; schedule state is in-memory and recalculated at process startup.

### 12) CLI interface summary

Supported modes:

```bash
python autorun/scheduler.py
python autorun/scheduler.py --once NEWS
python autorun/scheduler.py --once REALTIME
python autorun/scheduler.py --once MONTHLY
python autorun/scheduler.py --status
```

- default mode: continuous scheduler loop
- one-shot mode: run one bucket once
- status mode: inspect next fire times and exit.

### 13) Task 10 completion note

Task 10 status: complete in this document.

## Task 11 — Scripts and operations deep dive (`scripts/load_data.py`, `scripts/backfill_realtime_to_db.py`)

### 1) Purpose in the architecture

The `scripts/` folder contains operational recovery/load helpers that complement the normal pipeline runners.

Current scripts:

1. `load_data.py` — bulk-load persisted files in `data/` into PostgreSQL.
2. `backfill_realtime_to_db.py` — replay realtime state history (`atw_realtime_state.json`) into intraday/technicals DB tables.

They are designed for bootstrap, migration, and recovery operations rather than continuous scheduling.

### 2) `load_data.py` — scope and behavior

`load_data.py` is a direct SQLAlchemy loader that writes five data domains from disk:

1. market daily CSV -> `bourse_daily`
2. macro CSV -> `macro_morocco`
3. news CSV -> `news`
4. orderbook CSV files (`ATW_orderbook_*.csv`) -> `bourse_orderbook`
5. fundamentals JSON -> `fondamental_snapshot` + `fondamental_yearly`.

Input root:

- `data/` under project root.

### 3) `load_data.py` connection and environment contract

Engine builder (`build_engine`) loads `.env` then requires:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`

Optional defaults:

- `POSTGRES_HOST` default `localhost`
- `POSTGRES_PORT` default `5432`.

No custom error wrapper exists: missing required env keys fail immediately via `KeyError`.

### 4) `load_data.py` table-write logic and conflict policy

Generic helper `upsert_df(...)`:

1. normalizes `NaN -> None`
2. reflects destination table metadata
3. executes PostgreSQL `INSERT ... ON CONFLICT DO NOTHING`
4. reports submitted row count.

Per-table conflict keys:

- `bourse_daily`: conflict on `seance`
- `macro_morocco`: conflict on `date`
- `news`: conflict on `url`.

Important nuance:

- this script’s news load uses **do-nothing on URL conflict** only; it does not perform content enrichment updates like `AtwDatabase.save_news`.

### 5) `load_data.py` fundamentals dual-write

`load_fondamental()` reads `ATW_fondamental.json` and writes:

1. snapshot row to `fondamental_snapshot`:
   - key `(symbol, scrape_timestamp)` with `DO NOTHING`
2. expanded year-metric rows to `fondamental_yearly`:
   - iterates over every JSON field that is a dict
   - keeps entries where key parses to integer year
   - upsert key `(symbol, year, metric)` with `DO UPDATE SET value = EXCLUDED.value`.

Operational effect:

- snapshot is append-ish/idempotent by timestamp
- yearly table is current-value correcting on repeated loads.

### 6) `load_data.py` missing-file and end-of-run behavior

For each source file:

- if file missing -> prints `skip ... not found` and continues.

After loads, script prints row counts for:

- `bourse_daily`
- `macro_morocco`
- `news`
- `fondamental_snapshot`
- `fondamental_yearly`.

No CLI flags are implemented; execution is one-mode:

```bash
python scripts/load_data.py
```

### 7) `backfill_realtime_to_db.py` — scope and behavior

This script is a one-off migration utility for realtime state file replay.

Input:

- `data/atw_realtime_state.json`

Writes:

1. `snapshot_history[]` -> `bourse_intraday`
2. `technicals{}` -> `technicals_snapshot` with `computed_at = last_snapshot_ts`.

Ticker is fixed to `ATW`.

### 8) `backfill_realtime_to_db.py` write path

Processing steps:

1. validate state file existence (exit `1` if missing)
2. parse JSON
3. map each history snapshot to canonical intraday row fields
4. open `AtwDatabase` context manager
5. call:
   - `db.save_intraday(rows, ticker="ATW")`
   - `db.save_technicals(technicals, symbol="ATW", computed_at=last_snapshot_ts)` when payload exists
6. print inserted counts / no-payload message
7. exit `0` on success.

Because `AtwDatabase` uses conflict-safe inserts on key constraints, this script is safe to re-run.

### 9) Idempotency and dedup guarantees for both scripts

`load_data.py` relies on table keys:

- `bourse_daily.seance`
- `macro_morocco.date`
- `news.url`
- `fondamental_snapshot(symbol, scrape_timestamp)`
- `fondamental_yearly(symbol, year, metric)`.

`backfill_realtime_to_db.py` relies on:

- `bourse_intraday.snapshot_ts`
- `technicals_snapshot(symbol, computed_at)`.

Result:

- repeated operational runs do not duplicate key-identical rows.

### 10) Operational caveats

1. `load_data.py` assumes destination schema already exists; it does not create tables.
2. `load_data.py` uses direct SQLAlchemy reflection and will fail if file columns no longer match schema expectations.
3. `backfill_realtime_to_db.py` depends on historical state file structure (`snapshot_history`, `technicals`, `last_snapshot_ts`).
4. Neither script has granular partial-rollback orchestration across all domains; failures stop current execution path.

### 11) Safe recovery workflows (recommended runbooks)

#### A) Fresh DB bootstrap from existing artifacts

1. Ensure Postgres/schema is up.
2. Run:
   - `python scripts/load_data.py`
   - `python scripts/backfill_realtime_to_db.py`
3. Resume scheduler/pipelines.

Why both:

- `load_data.py` covers daily/macro/news/orderbook/fundamentals.
- `backfill_realtime_to_db.py` covers intraday + technicals from realtime state.

#### B) Rebuild intraday history after DB outage

1. Keep `data/atw_realtime_state.json` intact.
2. Run only:
   - `python scripts/backfill_realtime_to_db.py`

This replays missed intraday snapshots/technicals without touching other domains.

#### C) Re-sync core datasets from file truth

When DB drifts but `data/` files are trusted:

1. run `python scripts/load_data.py`
2. verify printed table counts
3. rerun source pipelines as needed for newest deltas.

### 12) CLI summary

```bash
python scripts/load_data.py
python scripts/backfill_realtime_to_db.py
```

No arguments/flags are currently supported in either script.

### 13) Task 11 completion note

Task 11 status: complete in this document.

## Task 12 — Full project reference pass (consolidated handbook)

### 1) Goal of this final pass

This section is the consolidated operator/developer reference for running Whale end-to-end, recovering safely, and navigating deep details quickly.

### 2) Fast navigation (cross-links to deep dives)

1. [Task 1 — Architecture deep dive](#task-1--architecture-deep-dive)
2. [Task 2 — Realtime scraper deep dive](#task-2--realtime-scraper-deep-dive-scrapersatw_realtime_scraperpy)
3. [Task 3 — Macro collector deep dive](#task-3--macro-collector-deep-dive-scrapersatw_macro_collectorpy)
4. [Task 4 — Fundamentals scraper deep dive](#task-4--fundamentals-scraper-deep-dive-scrapersfondamental_scraperpy)
5. [Task 5 — News crawlers deep dive](#task-5--news-crawlers-deep-dive-news_crawleratw_newspy)
6. [Task 6 — Database layer deep dive](#task-6--database-layer-deep-dive-databasedbpy-dockerinit01_schemasql)
7. [Task 7 — Valuation engine deep dive](#task-7--valuation-engine-deep-dive-modelsfundamental_modelspy)
8. [Task 8 — Agent News deep dive](#task-8--agent-news-deep-dive-agentsagent_newspy)
9. [Task 9 — Agent Analyse deep dive](#task-9--agent-analyse-deep-dive-agentsagent_analysepy)
10. [Task 10 — Automation deep dive](#task-10--automation-deep-dive-autorunschedulerpy)
11. [Task 11 — Scripts and operations deep dive](#task-11--scripts-and-operations-deep-dive-scriptsload_datapy-scriptsbackfill_realtime_to_dbpy)

### 3) Canonical end-to-end data flow (single view)

| Pipeline | Primary command(s) | Main file output(s) | Main DB table(s) | Main downstream consumer |
| --- | --- | --- | --- | --- |
| Realtime market | `python scrapers/atw_realtime_scraper.py snapshot` | `ATW_intraday.csv`, `ATW_orderbook_YYYY-MM-DD.csv`, `atw_realtime_state.json`, `ATW_bourse_casa_full.csv` | `bourse_intraday`, `bourse_orderbook`, `bourse_daily`, `technicals_snapshot` | Valuation + Analyse agent |
| Macro | `python scrapers/atw_macro_collector.py` | `ATW_macro_morocco.csv` | `macro_morocco` | Analyse agent |
| Fundamentals | `python scrapers/fondamental_scraper.py` | `ATW_fondamental.json` | `fondamental_snapshot`, `fondamental_yearly` | Valuation + Analyse agent |
| News crawlers | `python news_crawler/ATW_*_news.py` | `ATW_news.csv` | `news` | Analyse agent (+ News AI context baseline) |
| Valuation | `python -m models.fundamental_models --model all` | `models_result.json` | (none direct here) | Analyse agent |
| News AI | `python agents/agent_news.py` | terminal-only | none | Human operator (live pulse) |
| Analyse AI | `python agents/agent_analyse.py` | terminal report + `prediction_history.csv` | none (CSV history file) | Decision support |

### 4) Command playbook — first-time setup

```bash
pip install -r requirements.txt
pip install cloudscraper feedparser lxml selenium undetected-chromedriver webdriver-manager googlenewsdecoder
docker compose up -d postgres pgadmin
```

Then create `.env` from `.env.example` and fill:

- Postgres credentials/host/port/db
- `GROQ_API_KEY`
- optional `GROQ_MODEL`.

### 5) Command playbook — manual full refresh (deterministic sequence)

Use this run order when you want a coherent same-session snapshot:

```bash
python scrapers/atw_realtime_scraper.py snapshot --force
python scrapers/atw_macro_collector.py
python scrapers/fondamental_scraper.py --force
python news_crawler/ATW_boursenews_news.py
python news_crawler/ATW_medias24_news.py
python news_crawler/ATW_leconomiste_news.py
python news_crawler/ATW_aujourdhui_news.py
python news_crawler/ATW_marketscreener_news.py
python news_crawler/ATW_googlenews_news.py
python -m models.fundamental_models --model all
python agents/agent_analyse.py
```

Optional live pulse during day:

```bash
python agents/agent_news.py --per-query 6
```

### 6) Command playbook — autonomous scheduled operations

```bash
python autorun/scheduler.py
```

Operational controls:

```bash
python autorun/scheduler.py --status
python autorun/scheduler.py --once NEWS
python autorun/scheduler.py --once REALTIME
python autorun/scheduler.py --once MONTHLY
```

Log path:

- `autorun/autorun.log`.

### 7) Command playbook — DB bootstrap/recovery from file truth

Bootstrap or reconcile DB from current `data/` artifacts:

```bash
python scripts/load_data.py
python scripts/backfill_realtime_to_db.py
```

Use only realtime replay after intraday-specific outage:

```bash
python scripts/backfill_realtime_to_db.py
```

### 8) Command playbook — evidence and analysis diagnostics

Get deterministic evidence block without LLM call:

```bash
python agents/agent_analyse.py --evidence-only
```

Tighten or loosen news influence:

```bash
python agents/agent_analyse.py --lookback-days 7 --min-news-score 80
python agents/agent_analyse.py --lookback-days 21 --min-news-score 40
```

Raw machine-readable output:

```bash
python agents/agent_analyse.py --raw
python agents/agent_news.py --raw
```

### 9) Reliability and safety principles (project-wide)

1. Idempotent writes by key constraints (daily/macro/news/fundamentals/intraday/technicals).
2. Multi-source fallbacks for critical market/fundamental paths.
3. Deterministic numeric prediction core in `agent_analyse.py`; LLM narrative cannot overwrite core numbers.
4. Scheduler uses explicit per-command timeouts and continues across bucket command failures.
5. Recovery scripts allow replay from persisted files without destructive DB resets.

### 10) Quick incident runbook

1. **Scheduler stopped**: restart `python autorun/scheduler.py`; inspect `autorun/autorun.log`.
2. **DB drift/missing rows**: run `python scripts/load_data.py` and verify printed counts.
3. **Intraday gap**: run `python scripts/backfill_realtime_to_db.py`.
4. **Analyse output questionable**: inspect `--evidence-only` output and rerun with adjusted news filters.
5. **Valuation mismatch**: rerun `python -m models.fundamental_models --model all` then `python agents/agent_analyse.py`.

### 11) Final reference scope statement

At this point, `details.md` contains:

1. architecture mapping,
2. every major module deep dive,
3. storage semantics and idempotency model,
4. AI agent schema and evidence logic,
5. automation scheduler internals,
6. operations/recovery playbooks,
7. this consolidated cross-linked runbook.

### 12) Task 12 completion note

Task 12 status: complete in this document.  
All planned documentation tasks in this file are now complete.

---

Built for PFE (Projet de Fin d'Études), focused on ATW and Moroccan market intelligence.
