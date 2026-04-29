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

---

Built for PFE (Projet de Fin d'Études), focused on ATW and Moroccan market intelligence.
