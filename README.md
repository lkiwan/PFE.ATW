# Attijariwafa Bank (ATW) Data Collection Pipeline

This project is a comprehensive data collection and processing pipeline for financial and economic data related to Attijariwafa Bank (ATW) and the Moroccan market.

## Features
- **News Scraping**: Automated crawlers for major Moroccan news outlets (Boursenews, Médias24, L'Economiste, Aujourd'hui le Maroc, Google News).
- **Macro-economic Data**: Integration with World Bank, IMF, and Yahoo Finance to track GDP, inflation, and FX rates (EUR/MAD, USD/MAD).
- **Market Data**: Real-time and historical data collection from the Casablanca Stock Exchange (Bourse Casa).
- **Incremental Updates**: Optimized scrapers that only fetch new data since the last successful run.

## Project Structure
- `news_crawler/`: Scrapers for various news sources.
- `scrapers/`: Core data collection scripts for macro and financial indicators.
- `data/`: Collected datasets in CSV format (News, Macro, Market data).

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the collectors:
   ```bash
   python scrapers/atw_macro_collector.py
   ```

## Run valuation models from `data/`
Fundamental valuation models are now merged in:
- `models/fundamental_models.py`
- The file is standalone (no dependency on `utils/financial_constants.py`)

They read inputs from the `data` folder:
- Required daily market source: `data/ATW_bourse_casa_full.csv`
- Required periodic fundamentals source: `data/ATW_fondamental.json`
- Optional legacy/fallback files: `data/historical/ATW_merged.json`, `data/ATW_fondamental.csv`, `data/ATW_model_inputs.json`, `data/historical/ATW_marketscreener_v3.json`
- Output file (always written): `data/models_result.json`

Example:
```bash
python -m models.fundamental_models --model graham
python -m models.fundamental_models --model all
```

Behavior:
- The loader merges available ATW files automatically before running calculations.
- `--model graham` (or any single model) prints only that model in terminal.
- `data/models_result.json` always contains all models: `dcf`, `ddm`, `graham`, `relative`, `monte_carlo`.

Minimum inputs by model:
- `DDM`: `hist_dividend_per_share`
- `Graham`: `hist_eps` + `hist_equity` (or book value per share)
- `DCF`: `hist_fcf` **or** (`hist_ebitda` + `hist_capex`)
- `Monte Carlo`: `hist_revenue` (mapped as `net_sales`)
- `Relative`: historical multiples (`pe_ratio_hist`, `ev_ebitda_hist`, `pbr_hist`, `ev_revenue_hist`, `fcf_yield_hist`) plus supporting financial fields

## Minimal 2-file workflow (recommended)
1. Update daily market file:
   ```bash
   python scrapers/atw_realtime_scraper.py
   ```
2. Update periodic fundamentals monthly:
   ```bash
   python scrapers/fondamental_scraper.py
   ```
   The scraper skips if this month is already saved. Use `--force` to re-run:
   ```bash
   python scrapers/fondamental_scraper.py --force
   ```
3. Run valuation:
   ```bash
   python -m models.fundamental_models --model all
   ```

---
*Created as part of PFE (Projet de Fin d'Études).*
