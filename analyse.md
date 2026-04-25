# `agent_analyse.py` — design & sources

This document explains what the holistic ATW analyse agent does, how each
section is wired, and **where every numerical threshold and rule comes from**
(prior code, data files, project memory, or new design decisions made for this
agent).

The agent itself lives in a single file: `agents/agent_analyse.py`.

---

## 1. Purpose

`agent_analyse.py` produces a single, defensible **BUY / HOLD / SELL verdict**
on Attijariwafa Bank (ATW, Casablanca Bourse) by fusing five data sources that
are already collected by the project's scrapers. Every claim in the output
must cite a stable bracketed ID that points back to a specific value, row, or
article in the input files — so the verdict is reproducible and auditable.

It is a sibling of `agents/agent_news.py`. `agent_news.py` runs _live web
searches_; `agent_analyse.py` reads _files on disk_. Together they cover live
news intake and post-collection synthesis.

---

## 2. How to run

```bash
python agents/agent_analyse.py                       # default 14-day news window, formatted output
python agents/agent_analyse.py --raw                 # dump raw ATWAnalysis JSON
python agents/agent_analyse.py --asof 2026-04-25     # override "today"
python agents/agent_analyse.py --lookback-days 7     # tighter news window
python agents/agent_analyse.py --min-news-score 80   # only highest-conviction news
python agents/agent_analyse.py --evidence-only       # print prompt only, skip LLM
```

Requires `GROQ_API_KEY` in `.env` (already present in the project).
Optional `GROQ_MODEL` override (default: `openai/gpt-oss-120b`).

---

## 3. File structure — one file, banner-divided sections

Per project preference (`memory/feedback_modular_structure.md`), the agent is
organized as a single `.py` file with `# === SECTION ===` banners. Each
section can be edited without touching the others.

| Section                       | Lines   | Responsibility                                             |
| ----------------------------- | ------- | ---------------------------------------------------------- |
| CONFIG                        | 42–75   | paths, defaults, env loading, sanity-band thresholds       |
| SCHEMA                        | 78–112  | Pydantic models (`Evidence`, `Finding`, `ATWAnalysis`)     |
| SOURCE LOADERS / market       | 119–157 | `load_market_snapshot`                                     |
| SOURCE LOADERS / macro        | 160–224 | `load_macro_snapshot` (+ sanity-band scrubbing)            |
| SOURCE LOADERS / news         | 227–276 | `load_news_window`                                         |
| SOURCE LOADERS / fundamentals | 279–322 | `load_fundamentals`                                        |
| SOURCE LOADERS / valuations   | 325–375 | `load_valuations`                                          |
| EVIDENCE BLOCK                | 378–493 | `compose_evidence_block` — prompt assembly with stable IDs |
| LLM                           | 496–539 | `INSTRUCTIONS`, `build_synth_agent`, `synthesize`          |
| FORMATTER                     | 542–584 | `print_analysis` — terminal pretty-print                   |
| CLI / MAIN                    | 587–end | argparse + thin orchestrator, zero business logic          |

---

## 4. Data sources (read-only inputs)

All inputs live under `data/`. The agent never writes to disk and never
re-runs scrapers.

| Tag   | Path                            | Produced by                        | What the loader extracts                                                                              |
| ----- | ------------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------- |
| MKT   | `data/ATW_bourse_casa_full.csv` | `scrapers/atw_realtime_scraper.py` | last close, market cap, 1M/3M/6M/52w returns, 52w hi/lo                                               |
| MACRO | `data/ATW_macro_morocco.csv`    | `scrapers/atw_macro_collector.py`  | last row of GDP/CPI/debt/CA/EUR-USD-MAD/Brent/MASI/VIX + 90-day deltas                                |
| NEWS  | `data/ATW_news.csv`             | `news_crawler/ATW_*.py`            | rows in window, signal-score-filtered, capped, summary-truncated                                      |
| FUND  | `data/ATW_fondamental.json`     | `scrapers/fondamental_scraper.py`  | last 3 years of revenue / NI / EPS / equity / cash / FCF / margin / ROE / P/E / DPS, plus current P/B |
| VAL   | `data/models_result.json`       | `models/fundamental_models.py`     | DCF, DDM, Graham, Relative, Monte Carlo fair-value estimates                                          |

Per `memory/feedback_agent_architecture.md`: the agent must **not** modify any
scraper, model, or news-crawler code. It is a pure consumer.

---

## 5. Citation system — closed-set evidence IDs

The agent's "with sources" guarantee comes from a closed set of bracketed IDs
emitted by `compose_evidence_block` (lines 459–493). The LLM is instructed to
cite _only_ IDs that appear in this block, and the schema enforces
`min_length=1` on `Finding.evidence`, so the model cannot produce a
finding without a citation.

| ID family             | Examples                                         | Source                                                             |
| --------------------- | ------------------------------------------------ | ------------------------------------------------------------------ |
| `[MKT-*]`             | `[MKT-CLOSE]`, `[MKT-RET-3M]`, `[MKT-52W-HI]`    | computed from `Dernier Cours` column of `ATW_bourse_casa_full.csv` |
| `[MACRO-*]`           | `[MACRO-CPI]`, `[MACRO-EURMAD]`, `[MACRO-BRENT]` | last valid row of `ATW_macro_morocco.csv`                          |
| `[NEWS-N]`            | `[NEWS-1]`, `[NEWS-2]`, …                        | numbered per filtered row of `ATW_news.csv`                        |
| `[FUND-{TAG}-{year}]` | `[FUND-EPS-2024]`, `[FUND-ROE-2024]`             | year-keyed dicts in `ATW_fondamental.json`                         |
| `[VAL-*]`             | `[VAL-DCF]`, `[VAL-RANGE]`                       | top-level keys in `models_result.json`                             |

The numbering convention `[NEWS-N]` is borrowed from
`agent_news.py:_format_hits_for_llm` (lines 130–144), which already uses
`[1]`, `[2]`, … to label DDG search hits.

---

## 6. Scoring rules and thresholds — with sources

This is the part that benefits most from explicit sourcing because almost
every threshold has a _reason_ tied to a prior file or a memory note.

### 6.1 News-inclusion rule

> Only news rows from the last **N** days, with `signal_score >= threshold`,
> sorted by score, capped at 8 items, summary truncated to 140 characters.

- **`signal_score` semantics** are defined upstream in
  `agent_news.py:47–55` and `agent_news.py:104–117`:
  - 75+ = price-moving (earnings, guidance, M&A)
  - 60–74 = meaningful (analyst notes, regulatory)
  - 30–59 = contextual (macro, sector, global)
  - <30 = noise
    News crawlers (`news_crawler/ATW_*.py`) populate this column directly.
- **`DEFAULT_MIN_NEWS_SCORE = 65`** (line 60) — chosen to capture
  _meaningful and above_, slightly stricter than the news brief's 60 cutoff
  to reduce prompt size. The flag `--min-news-score` overrides it.
- **`DEFAULT_LOOKBACK_DAYS = 14`** (line 59) — two weeks, matches a
  reasonable post-earnings news window. Override via `--lookback-days`.
- **`DEFAULT_NEWS_CAP = 8`** (line 61) and
  **`DEFAULT_NEWS_SUMMARY_CHARS = 140`** (line 62) — these were tightened
  _after the first run hit Groq's free-tier 8 000 TPM limit_. The first
  attempt sent 8 826 tokens with cap=12 and summary=300 chars. Cap=8 +
  summary=140 brings the prompt comfortably under 8 000 tokens. Both are in
  CONFIG so they are easy to relax if you upgrade to Dev Tier.
  Source: Groq error response observed during smoke-testing — `Limit 8000,
Requested 8826`.

### 6.2 Macro sanity bands (defensive scrub)

> Drop rows where `inflation_cpi_pct > 50` or `public_debt_pct_gdp > 200`
> before reading the latest valid value.

- **`MACRO_CPI_MAX = 50.0`** and **`MACRO_DEBT_GDP_MAX = 200.0`**
  (lines 64–65) and the scrub function `_scrub_macro_band` (lines 181–187).
- **Source:** `memory/project_imf_datamapper_morocco_broken.md` — the IMF
  DataMapper API returned ~100× inflated values for Morocco's PCPIPCH and
  GGXWDG_NGDP. The V2 macro collector (`scrapers/atw_macro_collector.py`)
  already discards these via sanity bands and prefers the World Bank source,
  but this agent re-applies the same bands as a belt-and-suspenders so a
  stale row from before the V2 fix can never feed the LLM.

### 6.3 Verdict logic

> BUY if upside > +15% AND macro+news polarity is not net bearish.
> SELL if upside < −10% OR fundamentals are deteriorating.
> HOLD otherwise.

Encoded in the LLM `INSTRUCTIONS` (lines 504–514).

- The thresholds **+15% / −10%** are a **new design decision**, not borrowed
  from any existing file. They are deliberately asymmetric:
  - +15% upside is a meaningful margin of safety after model uncertainty (the
    `[VAL-RANGE]` spread in `models_result.json` is wide — DDM gives 277,
    DCF gives 1005, MC gives 1101, so noise alone can push the midpoint
    around).
  - −10% downside triggers SELL faster because a confirmed downside signal
    - deteriorating fundamentals is a stronger signal than upside alone.
- The "fundamentals deteriorating" trigger is checked against EPS, ROE, and
  net margin trends from `ATW_fondamental.json` (`hist_eps`, `hist_roe`,
  `hist_net_margin`).
- The "macro+news polarity is not net bearish" gate prevents BUY calls when
  the model itself flags bearish findings, even if the valuation midpoint
  looks attractive.

### 6.4 Conviction logic

> HIGH if all 5 dimensions present and aligned.
> MEDIUM if mixed polarities or only 3–4 dimensions.
> LOW if any source is missing or findings contradict each other.

Encoded in `INSTRUCTIONS` (lines 515–518). Also a new design decision. The
intent is to make the conviction visible to the user without forcing the
model into a binary signal.

### 6.5 Risks rule

> 3–5 concrete bullets drawn from NEWS or MACRO findings — no generic
> boilerplate.

`INSTRUCTIONS` line 519. The "no boilerplate" clause is there because early
prompts produced things like "market volatility" and "macro uncertainty"
without any anchor — adding the rule pushed the model to pull specific
items like "Managem overtook ATW in market cap [NEWS-2]".

---

## 7. LLM choice and output schema

- **Framework: agno + Groq.** Mirrors `agent_news.py:121–127` (line for
  line — same `Agent(model=Groq(id=..., max_tokens=4096, temperature=0.2),
output_schema=..., instructions=...)` shape). Keeping both agents on the
  same stack means a model swap or framework change touches both files in
  the same way.
- **Default model: `openai/gpt-oss-120b`** (line 63), same default as
  `agent_news.py:122`. Override with `GROQ_MODEL` in `.env`.
- **Schema enforcement.** `Finding.evidence: list[Evidence] =
Field(min_length=1)` (line 99) is the **structural** guarantee that every
  claim has a citation. Pydantic rejects the response if any `Finding`
  lacks evidence — there is no way for the LLM to emit a "free-form" claim.
- **Validation.** `synthesize` (line 533) checks `isinstance(content,
ATWAnalysis)`; on failure it raises `SynthesisError` and the CLI exits
  with code 3 — same pattern as `agent_news.py:191–194`.
- **Missing key.** `load_env` (line 71) raises `MissingEnvError` if
  `GROQ_API_KEY` is absent, and the CLI exits with code 2 — same pattern
  as `agent_news.py:176–178`.

---

## 8. References

### From the existing codebase

| File                            | Lines          | Borrowed for                                                                                                                                                                      |
| ------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `agents/agent_news.py`          | 21–26          | Windows UTF-8 stdout/stderr reconfig                                                                                                                                              |
| `agents/agent_news.py`          | 47–55, 104–117 | News `signal_score` 0-100 scale and bucket semantics                                                                                                                              |
| `agents/agent_news.py`          | 121–127        | `Agent(...)` + Groq + output_schema build pattern                                                                                                                                 |
| `agents/agent_news.py`          | 130–144        | Numbered `[NEWS-N]` evidence ID convention                                                                                                                                        |
| `agents/agent_news.py`          | 149–162        | Banner + grouped-block formatter style                                                                                                                                            |
| `agents/agent_news.py`          | 166            | `dotenv.load_dotenv(PROJECT_ROOT / ".env")`                                                                                                                                       |
| `agents/agent_news.py`          | 176–178        | `GROQ_API_KEY` missing → exit 2                                                                                                                                                   |
| `agents/agent_news.py`          | 191–194        | LLM type-check → exit 3                                                                                                                                                           |
| `data/ATW_bourse_casa_full.csv` | header row     | `Séance`, `Dernier Cours`, `Capitalisation` column names                                                                                                                          |
| `data/ATW_macro_morocco.csv`    | header row     | `gdp_growth_pct`, `inflation_cpi_pct`, `eur_mad`, `usd_mad`, `brent_usd`, `masi_close`, `vix`, `macro_momentum`, `global_risk_flag`, `public_debt_pct_gdp`                        |
| `data/ATW_news.csv`             | header row     | `date`, `signal_score`, `is_atw_core`, `full_content`                                                                                                                             |
| `data/ATW_fondamental.json`     | top-level keys | `hist_revenue`, `hist_net_income`, `hist_eps`, `hist_equity`, `hist_cash`, `hist_fcf`, `hist_net_margin`, `hist_roe`, `pe_ratio_hist`, `hist_dividend_per_share`, `price_to_book` |
| `data/models_result.json`       | top-level keys | `dcf`, `ddm`, `graham`, `relative`, `monte_carlo` and their `intrinsic_value{,_low,_high}`, `upside_pct`, `confidence`, `methodology` fields                                      |

### From project memory

| Memory note                                       | What it dictated                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------ |
| `memory/feedback_agent_architecture.md`           | Agent lives in `agents/`; never modifies scraper/model/news-crawler code |
| `memory/feedback_modular_structure.md`            | Single `.py` with banner-divided sections, NOT a package                 |
| `memory/project_imf_datamapper_morocco_broken.md` | Defensive macro sanity bands (CPI ≤ 50, debt/GDP ≤ 200)                  |

### New design decisions (no prior source)

| Rule                                                     | Where                                        | Rationale                                                  |
| -------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------------- |
| BUY threshold = +15% upside                              | `INSTRUCTIONS` line 510                      | Margin of safety against `[VAL-RANGE]` spread              |
| SELL threshold = −10% upside                             | `INSTRUCTIONS` line 511                      | Asymmetric — pair with fundamentals deterioration          |
| Fair-value range = min/max across all `[VAL-*]` low/high | `_val_block` line 491, instructions line 506 | Captures full model disagreement instead of cherry-picking |
| News cap = 8, summary = 140 chars                        | `config.py` lines 61–62                      | Empirically fits Groq's 8 000 TPM free-tier limit          |

---

## 9. Verification recipes

1. **End-to-end run** — `python agents/agent_analyse.py` should print a
   header, up to five dimension blocks, risks, and a final verdict block
   with no Python tracebacks.
2. **Citation coverage** — `python agents/agent_analyse.py --raw` then
   inspect every `findings[].evidence[].source_ref`; each value must be a
   bracketed ID that _also_ appears verbatim in the
   `--evidence-only` output.
3. **Numeric sanity** — pick one printed claim like
   `EPS rose from 44.18 in 2024 to 51.45 in 2026 [FUND-EPS-2024]
[FUND-EPS-2026]` and confirm those values match the corresponding keys
   in `data/ATW_fondamental.json` (`hist_eps.2024 == 44.18`,
   `hist_eps.2026 == 51.45`).
4. **Missing key guard** — temporarily unset `GROQ_API_KEY`; the CLI must
   exit with code 2 and print `ERROR: GROQ_API_KEY not set in .env`.
5. **News-window flag** — `python agents/agent_analyse.py --lookback-days 3`
   should produce a noticeably shorter NEWS block than the default 14-day
   run.
6. **Macro sanity scrub** — temporarily insert a row into
   `ATW_macro_morocco.csv` with `inflation_cpi_pct = 999`; `--evidence-only`
   should still report a sane CPI value (the bad row is replaced with `pd.NA`
   and the loader falls back to the previous valid one).
