# `agent_analyse.py` — methodology & sources

Holistic ATW analyse agent: BUY/HOLD/SELL verdict + **trading prediction (4 weeks)** + **investment prediction (12 months, BKGR convention)**, every number cited to a stable bracketed ID. Single file under `agents/`, banner-divided sections. This document supersedes `analyse.md`.

---

## 1. What the agent produces

For each run on Attijariwafa Bank (ATW, Casablanca Bourse):

1. **Findings** across 5 dimensions — MARKET, MACRO, NEWS, FUNDAMENTAL, VALUATION — each with at least one citation.
2. **Risks** — 3 to 5 concrete risk bullets drawn from NEWS / MACRO findings.
3. **Verdict** — BUY / HOLD / SELL with conviction (LOW / MEDIUM / HIGH).
4. **Trading prediction** (new) — 4-week ATR-based entry zone, target, stop-loss, risk/reward, French thesis.
5. **Investment prediction** (new) — 12-month BKGR-style cours cible, upside, dividend yield, total shareholder return (TSR), recommendation in French (ACHAT / CONSERVER / VENDRE), French thesis.

Numeric prediction values are computed **deterministically in Python** (no LLM hallucination possible). The LLM only writes the _thesis_ text justifying them.

---

## 2. How to run

```bash
python agents/agent_analyse.py                       # default 14-day news window
python agents/agent_analyse.py --raw                 # raw ATWAnalysis JSON
python agents/agent_analyse.py --asof 2026-04-25     # override "today"
python agents/agent_analyse.py --lookback-days 7
python agents/agent_analyse.py --min-news-score 80
python agents/agent_analyse.py --evidence-only       # skip LLM, see prompt + predictions
```

Requires `GROQ_API_KEY` in `.env`. Optional `GROQ_MODEL` override (default: `openai/gpt-oss-120b`).

---

## 3. File structure — single file, banner sections

| Section        | Responsibility                                                                          |
| -------------- | --------------------------------------------------------------------------------------- |
| CONFIG         | paths, defaults, sanity bands, **prediction constants**                                 |
| SCHEMA         | Pydantic models — `Finding`, `TradingPrediction`, `InvestmentPrediction`, `ATWAnalysis` |
| SOURCE LOADERS | one loader per data file (market, macro, news, fundamentals, valuations)                |
| PREDICTIONS    | **deterministic Python** for trading + investment predictions (no LLM)                  |
| EVIDENCE BLOCK | assembles all data + predictions into one prompt with `[*]` IDs                         |
| LLM            | `INSTRUCTIONS`, agent builder, `synthesize`                                             |
| FORMATTER      | terminal pretty-print, French-labeled prediction blocks                                 |
| CLI / MAIN     | thin orchestrator: load → compute predictions → LLM → overwrite numerics → print        |

---

## 4. Architecture: why "Python computes, LLM narrates"

LLMs are unreliable at exact arithmetic — particularly on price targets, stop-losses, and total returns where a decimal slip changes the recommendation. So:

1. **Python** computes ATR, target prices, stop-losses, dividend yield, TSR, recommendation, and confidence.
2. These deterministic values are injected into the evidence block as `[PRED-TRADE-*]` and `[PRED-INV-*]` IDs.
3. The LLM is instructed to produce a French _thesis_ (2–5 sentences) explaining and citing them.
4. After the LLM responds, `main()` **overwrites** the LLM's numeric prediction fields with the Python values, keeping only the LLM's thesis text. Hallucinated numbers cannot survive.

This mirrors regulated equity research practice where the **methodology is auditable** and the analyst's narrative justifies the model output, not the other way around.

---

## 5. Why split trading vs investment predictions

FINRA Rule 2241 explicitly recognizes two distinct research products: **"investor research"** (long-term) and **"trading research"** (short-term), and allows them to produce _different ratings_ on the same stock. ([FINRA.org][finra])

Empirically, the two horizons are driven by different signals:

> "Earnings forecasts are the main driver of target prices [over long horizons], while target prices at short-term horizons rely far more on sentiment and factors unrelated to firm fundamentals." — Da & Schaumburg, _JBFA_. ([nd.edu][da-schaumburg])

So the agent emits:

- A **trading block** driven by technicals (ATR, support/resistance) + news momentum
- An **investment block** driven by valuation models + fundamentals + dividend policy

[finra]: https://www.finra.org/rules-guidance/rulebooks/finra-rules/2241
[da-schaumburg]: https://www3.nd.edu/~zda/JBFA.pdf

---

## 6. Methodology — formulas with sources

### 6.1 Trading prediction (4 weeks, ATR-driven)

**Horizon = 4 weeks (20 trading days).** Analyst target accuracy decays sharply after a few weeks, so even formal 12-month price targets work better as a "short-term investment guide". ([ScienceDirect][sd-targets])

**Average True Range (Wilder, 14-day).** True Range = `max(H−L, |H−Cprev|, |L−Cprev|)`; ATR = mean of last 14 TR values. Standard short-term volatility benchmark. ([Optimus Futures][optimus])

**Entry zone.**

```
entry_low  = max(last_close − 1×ATR, low_4w)
entry_high = last_close
```

The entry band sits between recent support (4-week low) and the current close, in line with classical chartist convention used by Boursenews on the MASI. ([Boursenews — Indice MASI][bn-masi])

**Stop-loss (k×ATR).**

```
stop = max(entry_low − k×ATR, 0)         with k = 2.0 (default)
```

ATR multiples in the 1.5–3.0 range are the textbook stop-loss method for technical traders. ([LuxAlgo][lux], [TradersPost][tp-atr])

**Target price.** Daily ATR scaled to a 4-week horizon by `√n_days` (volatility-time scaling), then capped at the 52-week high to respect realistic resistance:

```
target = min(last_close + m×ATR×√20, high_52w)         with m = 1.5
```

TradersPost recommends keeping the profit target inside the daily ATR band; we extend that idea to 4 weeks via the `√t` rule. ([TradersPost][tp-atr])

**Expected return** = `(target − last_close) / last_close × 100`.

**Risk/reward** = `(target − last_close) / (last_close − stop)`.

**Confidence**:

- HIGH: ATR/last_close < 1.5% AND ≥2 high-score (≥75) news items in window
- MEDIUM: ATR/last_close < 2.5%
- LOW: otherwise

The signal-score thresholds mirror those defined in `agent_news.py` (75+ = price-moving).

[sd-targets]: https://www.sciencedirect.com/science/article/abs/pii/S1059056024000960
[optimus]: https://optimusfutures.com/blog/average-true-range-indicator/
[bn-masi]: https://boursenews.ma/article/analyse-technique/indice-masi-que-dit-l-analyse-technique
[lux]: https://www.luxalgo.com/blog/5-atr-stop-loss-strategies-for-risk-control/
[tp-atr]: https://blog.traderspost.io/article/atr-trading-strategies-guide

### 6.2 Investment prediction (12 months, BKGR convention)

**Horizon = 12 months.** Standard analyst convention; explicitly used by BKGR (BMCE Capital Global Research) in their published ATW notes. ([BKGR via Boursenews][bkgr-26pct], [BKGR via La Vie éco][bkgr-900], [BKGR via L'Economiste][bkgr-800])

**Cours cible — confidence-weighted across `[VAL-*]` models.**

```
target = Σ(intrinsic_value_i × confidence_i) / Σ(confidence_i)
target_low  = min over all VAL-* low values
target_high = max over all VAL-* high values
```

The `models_result.json` already carries five regulator-accepted methods — DCF, DDM (Gordon-Shapiro), Graham, Relative (multiples), Monte Carlo — covering the **three valuation families** the AMMC formally accepts on the Casablanca exchange: DCF, comparable multiples, and patrimonial. ([AMMC — Bourse des Valeurs de Casablanca][ammc])

The CFA curriculum identifies these same three cash-flow streams (dividends, free cash flow, residual income) as the valid intrinsic-value approaches. ([CFA Institute — DDM][cfa-ddm], [CFA Institute — FCF][cfa-fcf])

Why **confidence-weighted** rather than equal-weight? `models_result.json` exposes a per-model `confidence` score. Equal-weighting would let DDM=277 (confidence 45) drag the midpoint as much as DCF=1005 (confidence 80). The weighted midpoint at 872 MAD lines up cleanly with BKGR's published targets of 800–900 MAD on ATW.

**Upside** = `(target − last_close) / last_close × 100`.

**Dividend yield** = `last DPS / last_close × 100`, using the most recent year in `[FUND-DPS-*]`. BKGR's 2025 ATW note projects DPS = 23 MAD (2025) and 24 MAD (2026), so this metric is part of the standard Moroccan equity-research workflow. ([BKGR via Boursenews — DPS forecast][bkgr-26pct])

**Total Shareholder Return (Morgan Stanley reinvested form).**

```
TSR = CGY + (1 + CGY) × DY                    where CGY = upside/100, DY = dividend yield (decimal)
```

The cross term `(1 + CGY) × DY` accounts for dividend reinvestment over the period, more accurate than the naïve `CGY + DY` for multi-period horizons. ([Morgan Stanley — Total Shareholder Return][ms-tsr], [Wall Street Prep — HPR][wsp-hpr])

**Recommendation** (French — BKGR/Moroccan-broker convention):

- ACHAT if upside > +15%
- VENDRE if upside < −10%
- CONSERVER otherwise

The +15% / −10% asymmetric thresholds are explained in §6.3 of `analyse.md` (margin of safety against valuation spread).

**Confidence**:

- HIGH: model spread < 30% of target AND average model confidence ≥ 70
- MEDIUM: model spread < 60% AND average confidence ≥ 50
- LOW: otherwise

The current `models_result.json` triggers LOW because DDM=277 vs Graham=1081 implies a spread > 60% — this is _correct_: it tells the user the underlying models disagree.

[bkgr-26pct]: https://boursenews.ma/article/marches/BKGR-recommande-attijariwafa-bank-a-l-achat-avec-un-potentiel-de-hausse-de-26-%25
[bkgr-900]: https://www.lavieeco.com/argent/attijariwafa-bank-bkgr-recommande-le-titre-a-lachat-pour-un-cours-cible-de-900-dh
[bkgr-800]: https://www.leconomiste.com/flash-infos/bkgr-releve-le-cours-cible-d-attijariwafa-bank-800-dh
[ammc]: https://www.ammc.ma/en/node/197
[cfa-ddm]: https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/discounted-dividend-valuation
[cfa-fcf]: https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/free-cash-flow-valuation
[ms-tsr]: https://www.morganstanley.com/im/publication/insights/articles/article_totalshareholderreturns.pdf
[wsp-hpr]: https://www.wallstreetprep.com/knowledge/holding-period-return-hpr/

### 6.3 Why French labels on the prediction blocks

Moroccan equity research, including BKGR notes carried by Boursenews, La Vie éco, L'Economiste, and Le Matin, is published in French and uses a fixed vocabulary: **"cours cible"**, **"potentiel d'appréciation"**, **"rendement de dividende"**, **"ACHAT / CONSERVER / VENDRE"**. ([Le Matin — BMCE Capital ATW][lematin-bmce])

Producing predictions in this vocabulary makes the agent output directly usable in any Moroccan-investor context. The rest of the agent's output (findings, risks, verdict) stays in English to match the existing schema.

[lematin-bmce]: https://lematin.ma/economie/bmce-capital-revalorise-le-titre-attijariwafa-bank-a-900-dh/301637

---

## 7. New evidence IDs

The predictions surface as their own ID family in the evidence block, so the LLM can cite them in its thesis text:

| ID                    | Meaning                             |
| --------------------- | ----------------------------------- |
| `[MKT-ATR]`           | 14-day Wilder ATR (MAD)             |
| `[MKT-4W-HI]`         | 4-week high (entry-zone reference)  |
| `[MKT-4W-LO]`         | 4-week low (entry-zone reference)   |
| `[PRED-TRADE-ENTRY]`  | trading entry zone (low–high MAD)   |
| `[PRED-TRADE-TARGET]` | 4-week target (MAD)                 |
| `[PRED-TRADE-STOP]`   | stop-loss (MAD)                     |
| `[PRED-TRADE-RET]`    | expected return % over 4 weeks      |
| `[PRED-TRADE-RR]`     | risk/reward ratio                   |
| `[PRED-TRADE-CONF]`   | LOW / MEDIUM / HIGH                 |
| `[PRED-INV-TARGET]`   | 12-month cours cible (MAD)          |
| `[PRED-INV-UPSIDE]`   | upside %                            |
| `[PRED-INV-DY]`       | dividend yield %                    |
| `[PRED-INV-TSR]`      | expected total shareholder return % |
| `[PRED-INV-RECO]`     | ACHAT / CONSERVER / VENDRE          |
| `[PRED-INV-CONF]`     | LOW / MEDIUM / HIGH                 |

---

## 7b. Token budget tuning (Groq free tier, 8000 TPM)

Adding the predictions block + ATR fields + new INSTRUCTIONS pushed the prompt over Groq's free-tier 8000 TPM limit. The original `analyse.md` had already tuned `DEFAULT_NEWS_CAP=8` / `DEFAULT_NEWS_SUMMARY_CHARS=140` to fit within 8000 TPM **before** predictions were added. After the addition, the following further trims were necessary:

| Setting                      | Before                   | After             | Why                               |
| ---------------------------- | ------------------------ | ----------------- | --------------------------------- |
| `DEFAULT_NEWS_CAP`           | 8                        | 5                 | predictions block adds ~150 tok   |
| `DEFAULT_NEWS_SUMMARY_CHARS` | 140                      | 90                | further compression               |
| `_last_n_years` default      | 3                        | 2                 | drops ~10 fund-block lines        |
| News block format            | 4-line                   | 1-line + drop URL | LLM doesn't need URL for analysis |
| `[VAL-*]` line               | full methodology trailer | numeric only      | methodology not needed for thesis |

End-to-end measurement: prompt now ≈ 7800 tokens, comfortably under 8000. Upgrading to Groq Dev Tier would let us restore higher news caps and full fundamentals depth.

---

## 8. Verification recipes

1. **End-to-end run** — `python agents/agent_analyse.py` should print findings, risks, a TRADING block, an INVESTISSEMENT block, and the verdict — no Python tracebacks.
2. **Numeric integrity** — `--raw` output: confirm `trading_prediction.target_price_mad` matches the `[PRED-TRADE-TARGET]` value in `--evidence-only`. They must be identical (the post-LLM overwrite guarantees this).
3. **ATR sanity** — pick 5 recent rows in `data/ATW_bourse_casa_full.csv`, compute TR by hand, average → should match `[MKT-ATR]` in `--evidence-only` to within rounding.
4. **TSR sanity** — verify `expected_total_return_pct ≈ upside_pct + (1 + upside_pct/100) × dividend_yield_pct`.
5. **Recommendation thresholds** — temporarily set `last_close` such that upside crosses +15% / −10% and confirm ACHAT / CONSERVER / VENDRE flips correctly.
6. **BKGR cross-check** — `cours_cible_mad` in the current run is **872 MAD**, which sits between BKGR's published targets of **800 MAD** and **900 MAD** on ATW. This is a sanity check, not a constraint.

---

## 9. Sources — full list

### Moroccan / Casablanca-specific (primary references)

- **BKGR (BMCE Capital Global Research) ATW notes:**
  - [BKGR recommande Attijariwafa Bank à l'achat avec un potentiel de hausse de 26% — Boursenews](https://boursenews.ma/article/marches/BKGR-recommande-attijariwafa-bank-a-l-achat-avec-un-potentiel-de-hausse-de-26-%25)
  - [BKGR recommande le titre à l'achat pour un cours cible de 900 DH — La Vie éco](https://www.lavieeco.com/argent/attijariwafa-bank-bkgr-recommande-le-titre-a-lachat-pour-un-cours-cible-de-900-dh)
  - [BKGR relève le cours cible d'Attijariwafa Bank à 800 DH — L'Economiste](https://www.leconomiste.com/flash-infos/bkgr-releve-le-cours-cible-d-attijariwafa-bank-800-dh)
  - [BMCE Capital revalorise Attijariwafa Bank à 900 DH — Le Matin](https://lematin.ma/economie/bmce-capital-revalorise-le-titre-attijariwafa-bank-a-900-dh/301637)
  - [Bourse de Casablanca: les actions sur lesquelles miser en 2026 (BMCE Capital) — Le Matin](https://lematin.ma/economie/bourse-de-casablanca-les-actions-sur-lesquelles-miser-en-2026-bkgr/324205)
- **Casablanca Stock Exchange & AMMC (regulator):**
  - [AMMC — Bourse des Valeurs de Casablanca](https://www.ammc.ma/en/node/197)
  - [Casablanca Stock Exchange — Analyses & recherches](https://www.casablanca-bourse.com/fr/analyses-recherches)
  - [Casablanca Stock Exchange — fiche instrument ATW](https://www.casablanca-bourse.com/fr/live-market/instruments/ATW)
- **Casabourse / Moroccan technical analysis:**
  - [Casabourse — page Attijariwafa bank](https://casabourse.ma/entreprise/attijariwafa-bank/)
  - [Boursenews — Indice MASI : Que dit l'analyse technique](https://boursenews.ma/article/analyse-technique/indice-masi-que-dit-l-analyse-technique)
  - [Boursenews — Flash Momentum scoring technique MASI20](https://boursenews.ma/article/graphiques-et-analyse-technique/Flash-Momentum-15-septembre-2025)
- **Academic (Moroccan):**
  - [Analyse technique ou Analyse fondamentale — African Scientific Journal](https://africanscientificjournal.com/index.php/AfricanScientificJournal/article/download/79/78/81)

### International methodology references

- **Regulation & analyst practice:**
  - [FINRA Rule 2241 — Research Analysts and Research Reports](https://www.finra.org/rules-guidance/rulebooks/finra-rules/2241)
  - [TipRanks — Price Target meaning (12–18 month convention)](https://www.tipranks.com/glossary/p/price-target)
  - [Da & Schaumburg, _JBFA_ — What Drives Target Price Forecasts and Their Investment Value](https://www3.nd.edu/~zda/JBFA.pdf)
  - [Multi-dimensional assessment of analyst target price accuracy — ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S1059056024000960)
- **Valuation theory (CFA Institute):**
  - [Discounted Dividend Valuation — CFA Institute](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/discounted-dividend-valuation)
  - [Free Cash Flow Valuation — CFA Institute](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/free-cash-flow-valuation)
- **Total return / TSR formulas:**
  - [Morgan Stanley — Total Shareholder Return](https://www.morganstanley.com/im/publication/insights/articles/article_totalshareholderreturns.pdf)
  - [Wall Street Prep — Holding Period Return (HPR)](https://www.wallstreetprep.com/knowledge/holding-period-return-hpr/)
- **ATR / technical analysis:**
  - [TradersPost — ATR Trading Strategies Guide](https://blog.traderspost.io/article/atr-trading-strategies-guide)
  - [LuxAlgo — 5 ATR Stop-Loss Strategies for Risk Control](https://www.luxalgo.com/blog/5-atr-stop-loss-strategies-for-risk-control/)
  - [Optimus Futures — Average True Range Indicator](https://optimusfutures.com/blog/average-true-range-indicator/)

---

## 10. Project memory references

| Memory note                                       | What it dictated                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------ |
| `memory/feedback_agent_architecture.md`           | Agent lives in `agents/`; never modifies scraper/model/news-crawler code |
| `memory/feedback_modular_structure.md`            | Single `.py` with banner-divided sections, NOT a package                 |
| `memory/project_imf_datamapper_morocco_broken.md` | Defensive macro sanity bands (CPI ≤ 50, debt/GDP ≤ 200)                  |
