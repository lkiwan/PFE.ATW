# 🐋 WHALE PLAN — ATW Agent Intelligence Suite

> **Project**: PFE.01 · Attijariwafa Bank (ATW) AI Agents  
> **Date**: 2026-04-23  
> **Status**: v1.0 — Production-Ready Blueprint

---

## 0. What is the Whale Plan?

The **Whale Plan** is the master architecture document that governs how the two
AI agents — `agent_analyse` and `agent_news` — work independently and together
to produce institutional-grade market intelligence about ATW automatically.

The metaphor: a **whale** sees the full ocean (macro + news + technicals +
fundamentals) before surfacing with a verdict. No single data point decides
anything; the whole picture does.

---

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      EXTERNAL WORLD                                 │
│   Casablanca Bourse · BAM · Reuters · Medias24 · Boursenews · etc.  │
└────────────────┬────────────────────────────┬───────────────────────┘
                 │  (scrapers — existing)      │  (live web — agno)
                 ▼                             ▼
┌──────────────────────────┐     ┌─────────────────────────────────┐
│    PostgreSQL Database   │     │         AGENT_NEWS              │
│  ┌─────────────────────┐ │◄────│  Brain #2: News Intelligence    │
│  │ bourse_daily        │ │     │                                 │
│  │ macro_morocco       │ │     │  Tools:                         │
│  │ news                │ │────►│  • GoogleSearchTools (agno)     │
│  │ fondamental_yearly  │ │     │  • classify_news_event()        │
│  │ fondamental_snapshot│ │     │  • save_articles_to_db()        │
│  └─────────────────────┘ │     │  • get_recent_db_headlines()    │
└──────────┬───────────────┘     └──────────────┬──────────────────┘
           │                                    │
           │  reads all tables                  │  saves enriched news
           ▼                                    │
┌─────────────────────────────────────────────────────────────────────┐
│                        AGENT_ANALYSE                                │
│  Brain #1: Quantitative Engine                                      │
│                                                                     │
│  Tools:                                                             │
│  • get_price_history()          → OHLCV from bourse_daily           │
│  • compute_technical_indicators()→ SMA/EMA/RSI/MACD/BB/ATR         │
│  • get_macro_context()          → macro_morocco latest              │
│  • get_fundamentals()           → fondamental_yearly / snapshot     │
│  • get_news_sentiment()         → news signal_score aggregation     │
│  • generate_analytics_report()  → full JSON synthesis               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
                   ┌───────────────────────┐
                   │    FINAL OUTPUT       │
                   │  • Markdown report    │
                   │  • JSON data blob     │
                   │  • BUY/HOLD/SELL      │
                   └───────────────────────┘
```

---

## 2. Agent Profiles

### 2.1 `agent_analyse` — The Quant

| Property        | Value                                          |
| --------------- | ---------------------------------------------- |
| **File**        | `agents/agent_analyse.py`                      |
| **Model**       | Claude Sonnet (claude-sonnet-4-20250514)       |
| **Persona**     | ATW-QUANT — Senior Quantitative Equity Analyst |
| **Data source** | PostgreSQL (all 5 tables)                      |
| **Output**      | Structured Markdown report + JSON              |
| **Trigger**     | After market close (15:45 WAT) or on demand    |

**Analytics layers it covers:**

| Layer           | Indicators                                                                              |
| --------------- | --------------------------------------------------------------------------------------- |
| **Technical**   | SMA-20/50, EMA-20, RSI-14, MACD(12,26,9), Bollinger Bands(20,2), ATR-14, Momentum Score |
| **Fundamental** | P/E, P/B, Dividend Yield, ROE, EPS trend, Book Value/share                              |
| **Macro**       | Inflation, EUR/MAD + USD/MAD, Brent, Gold, VIX, MASI, US10Y, BAM monetary cycle         |
| **Sentiment**   | Avg signal score, core article ratio, top-5 headlines                                   |
| **Synthesis**   | BUY / HOLD / SELL signal + price target range + 3 risks + 3 catalysts                   |

---

### 2.2 `agent_news` — The Journalist

| Property        | Value                                                  |
| --------------- | ------------------------------------------------------ |
| **File**        | `agents/agent_news.py`                                 |
| **Model**       | Claude Sonnet (claude-sonnet-4-20250514)               |
| **Persona**     | ATW-NEWS — Financial News Intelligence Officer         |
| **Data source** | agno GoogleSearchTools (live web) + PostgreSQL (dedup) |
| **Output**      | Intelligence Brief (Markdown) + saves to DB            |
| **Trigger**     | Every 4–6 hours (intraday sweeps)                      |

**Search coverage:**

| Query Category    | What It Hunts                                       |
| ----------------- | --------------------------------------------------- |
| Earnings          | Net income, PNB, quarterly/annual results           |
| Dividends         | DPS announcements, ex-dividend dates                |
| Analyst Notes     | Price targets, rating changes, broker reports       |
| Regulatory        | BAM circulars, Basel III/IV updates, capital ratios |
| M&A               | Acquisitions, partnerships, subsidiary news         |
| Macro Impact      | Dirham moves, inflation, BAM rate decisions         |
| Africa Operations | CBAO, Wafa Assurance, sub-Saharan expansion         |

**Scoring system:**

```
signal_score = 0.5 (base)
  + 0.10 per HIGH_SIGNAL keyword match   (earnings, dividend, rating, etc.)
  - 0.15 per LOW_SIGNAL keyword match    (sport, CSR, cultural events)

 → score < 0.30  : DISCARDED (noise)
 → score 0.30-0.59: CONTEXT article
 → score 0.60-0.74: MEDIUM-IMPACT
 → score ≥ 0.75  : HIGH-IMPACT ⚡ (price-sensitive)
```

---

## 3. Data Flow: Step by Step

### Morning Run (08:00 WAT)

```
1. agent_news runs
   ├─ checks DB for last 10 headlines (dedup baseline)
   ├─ fires 4+ Google searches for overnight news
   ├─ classifies each article (EARNINGS/DIVIDEND/ANALYST/etc.)
   ├─ saves qualifying articles to PostgreSQL news table
   └─ produces intelligence brief → printed / logged

2. (Optional) agent_analyse runs with morning context
   └─ uses updated news sentiment + previous day close
```

### Post-Close Run (16:00 WAT — after Casablanca close at 15:30)

```
1. Scrapers run first (existing pipeline):
   ├─ atw_realtime_scraper.py     → bourse_daily updated
   ├─ atw_macro_collector.py      → macro_morocco updated
   └─ news_crawlers/*.py          → news table enriched

2. agent_news runs for a final news sweep

3. agent_analyse runs — full report with fresh data:
   ├─ reads updated bourse_daily (today's OHLCV)
   ├─ computes all technical indicators
   ├─ reads latest macro snapshot
   ├─ reads fresh news sentiment scores
   ├─ loads fundamentals
   └─ synthesises BUY/HOLD/SELL with price target
```

---

## 4. Installation & Setup

### 4.1 Install agno

```bash
pip install agno
```

### 4.2 Add API keys to `.env`

```dotenv
# Existing keys
POSTGRES_USER=atw
POSTGRES_PASSWORD=your_password
POSTGRES_DB=atw
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# New keys required by agents
ANTHROPIC_API_KEY=sk-ant-...          # for Claude model
GOOGLE_API_KEY=AIza...                # for GoogleSearchTools (agno)
GOOGLE_CSE_ID=...                     # Custom Search Engine ID
```

> **Alternative search**: Replace `GoogleSearchTools()` with `DuckDuckGoTools()`
> in `agent_news.py` — no API key required, but lower quality results.
>
> ```python
> from agno.tools.duckduckgo import DuckDuckGoTools
> # then in build_news_agent():
> tools=[ DuckDuckGoTools(), ... ]
> ```

### 4.3 Update `requirements.txt`

Add to the existing file:

```
agno>=1.0.0
anthropic>=0.40.0
```

---

## 5. Running the Agents

### Run `agent_news` (live news sweep)

```bash
# Default: search → classify → save to DB → produce brief
python agents/agent_news.py

# Custom query
python agents/agent_news.py --query "Find latest ATW earnings and analyst ratings"

# Dry-run (no DB writes, just brief)
python agents/agent_news.py --no-db
```

### Run `agent_analyse` (full market analytics)

```bash
# Default: full report on ATW
python agents/agent_analyse.py

# Custom analysis question
python agents/agent_analyse.py --query "Is ATW oversold? What are the key support levels?"

# Save JSON report to disk
python agents/agent_analyse.py --save data/analysis_report.json
```

### Run both in sequence (recommended daily workflow)

```bash
# 1. Refresh news first
python agents/agent_news.py

# 2. Then run full analytics (which reads the freshly saved news)
python agents/agent_analyse.py --save data/analysis_$(date +%Y%m%d).json
```

---

## 6. Scheduled Automation (cron)

Add to crontab (`crontab -e`):

```cron
# ── ATW Agent Suite ──────────────────────────────────────────────
# News sweep: every 4 hours during market hours
0 8,12,16 * * 1-5  cd /path/to/project && python agents/agent_news.py >> logs/news.log 2>&1

# Post-close full analytics (after scrapers have run)
15 16 * * 1-5  cd /path/to/project && python agents/agent_analyse.py --save data/analysis_$(date +\%Y\%m\%d).json >> logs/analyse.log 2>&1
```

---

## 7. Output Examples

### `agent_news` Intelligence Brief

```
═══════════════════════════════════════════════════════════════════
📰 ATW NEWS INTELLIGENCE BRIEF — 2026-04-23
═══════════════════════════════════════════════════════════════════

🔴 HIGH-IMPACT (signal ≥ 0.75)
  ⚡ [Reuters] Attijariwafa Bank Q1 2026: net profit up 12% YoY to MAD 2.1bn
     → Category: EARNINGS | Score: 0.90 | Source: reuters.com

🟡 MEDIUM-IMPACT (signal 0.50–0.74)
  • [Médias24] Attijariwafa raises ATW price target to MAD 590 — CFG Bank
     → Category: ANALYST | Score: 0.65

🟢 LOW-IMPACT / CONTEXT
  • [L'Économiste] BAM holds key rate at 2.75%, signals cautious stance
     → Category: REGULATORY | Score: 0.45

🧭 SECTOR PULSE
  • MAD held stable vs EUR at 10.82 — no FX headwind for ATW international ops.
  • MASI +0.4% today: broad-based banking sector recovery.
  • BAM unchanged rate confirms flat NIM environment for H1 2026.

📊 SENTIMENT VERDICT: POSITIVE
   Reasoning: Strong Q1 earnings beat + analyst upgrade offset muted macro.
```

### `agent_analyse` Analytics Report

```
═══════════════════════════════════════════════════════════════════
ATW-QUANT MARKET ANALYTICS REPORT — 2026-04-23
═══════════════════════════════════════════════════════════════════

📈 TECHNICAL ANALYSIS
  Close: 562.00 MAD | SMA-20: 551.2 | SMA-50: 538.7 (ABOVE BOTH ✓)
  RSI-14: 61.4 (Neutral-Bullish) | MACD: +2.1 (above signal ✓)
  Bollinger: [527.4 — 574.8] → price in upper half
  ATR-14: 8.3 MAD | Momentum Score: 75/100
  → Trend: BULLISH

📊 FUNDAMENTAL ANALYSIS
  P/E: 11.2x | P/B: 1.4x | Dividend Yield: 3.8%
  ROE: 16.1% (above sector 15% ✓)
  → Fundamentally attractive at current levels

🌍 MACRO ENVIRONMENT
  Inflation: 2.1% (contained) | EUR/MAD: 10.82 (stable)
  Brent: $78.4 | VIX: 14.2 (low fear)
  MASI: +0.4% | US10Y: 4.15%
  → Macro Verdict: RISK-ON (mild)

📰 NEWS SENTIMENT
  Avg Signal Score: 0.68 | 85% core ATW articles
  → Sentiment Proxy: POSITIVE

══════════════════════════════════════════════════════════════
🎯 FINAL VERDICT: BUY / ACCUMULATE
   Price Target Range: 580–600 MAD (technical resistance)
   Current Price: 562 MAD → Upside: +3.2% to +6.8%

⚠️  TOP 3 RISKS
  1. BAM rate cut delay → NIM compression
  2. EUR/MAD depreciation > 2% → FX translation loss
  3. RSI approaching 70 → short-term pullback risk

🚀 TOP 3 CATALYSTS
  1. Q1 2026 earnings beat → analyst upgrades incoming
  2. MASI bull run continuation → sector re-rating
  3. Africa ops growth (CBAO, Wafa Assurance) → revenue diversification
══════════════════════════════════════════════════════════════
```

---

## 8. Extending the Agents

### Add a new analytics tool to `agent_analyse`

```python
# In agents/agent_analyse.py

def get_peer_comparison(symbol: str = "ATW") -> dict:
    """Compare ATW P/E and P/B vs BCP, CIH, CDM peers."""
    # ... your logic
    pass

# Then add to build_analyse_agent():
tools=[..., get_peer_comparison]
```

### Add a new news source to `agent_news`

```python
# In ATW_SEARCH_QUERIES dict:
ATW_SEARCH_QUERIES["investor_relations"] = \
    '"Attijariwafa Bank" site:ir.attijariwafabank.com'
```

### Swap to a different LLM

```python
# In either agent file, replace:
model=Claude(id="claude-sonnet-4-20250514")

# With:
from agno.models.openai import OpenAIChat
model=OpenAIChat(id="gpt-4o")
```

---

## 9. File Map

```
project_root/
├── agents/
│   ├── __init__.py          ← package entry point
│   ├── agent_analyse.py     ← Brain #1: Quant Engine
│   ├── agent_news.py        ← Brain #2: News Intelligence
│   └── WHALE_PLAN.md        ← THIS FILE
│
├── database/
│   ├── __init__.py          ← exposes AtwDatabase
│   └── db.py                ← PostgreSQL ORM layer
│
├── models/
│   └── fundamental_models.py← DCF · DDM · Graham · Relative · MonteCarlo
│
├── scrapers/                ← existing data collectors
├── news_crawler/            ← existing news scrapers
├── docker/init/01_schema.sql← DB schema
├── docker-compose.yml       ← PostgreSQL + pgAdmin
├── .env                     ← secrets (never commit)
└── requirements.txt         ← add: agno · anthropic
```

---

## 10. Key Design Decisions

| Decision                               | Rationale                                                                                                      |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| **Separate agents** (not one monolith) | Each agent has a single responsibility → easier to debug, schedule, and extend independently                   |
| **agent_news runs first**              | Ensures DB has fresh news before agent_analyse computes sentiment                                              |
| **agno GoogleSearch over scraper**     | Scrapers cover specific known sites; Google catches any source that mentions ATW including international press |
| **Signal scoring in Python (not LLM)** | Fast, deterministic, cheap — LLM is used for synthesis not tagging                                             |
| **PostgreSQL as shared state**         | Both agents read/write the same DB → no inter-agent messaging bus needed at this scale                         |
| **Claude Sonnet not Opus**             | Balanced cost/performance for daily automation; Opus reserved for deep research questions                      |

---

_Whale Plan v1.0 — Built for PFE.01 · Attijariwafa Bank Intelligence Suite_
