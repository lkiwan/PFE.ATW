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

---
*Created as part of PFE (Projet de Fin d'Études).*
