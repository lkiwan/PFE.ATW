"""
ATW macroeconomic and market-context collector.

Builds a daily macro dataset for ATW (Attijariwafa Bank) by combining:
- World Bank indicators (REST API)
- IMF DataMapper indicators (REST API)
- yfinance daily market series

Output:
    data/ATW_macro_morocco.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import certifi

try:
    import yfinance as yf
except ImportError as exc:
    # ... handle missing dependency ...
    raise RuntimeError("Missing dependency: yfinance") from exc

logger = logging.getLogger("atw_macro_collector")

# ... rest of constants ...

_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = _ROOT / "data"
DEFAULT_OUTPUT = DATA_DIR / "ATW_macro_morocco.csv"
DEFAULT_START = "2010-01-01"

# Force valid CA bundle (Windows env may point to broken PostgreSQL cert path).
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["CURL_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()


OUTPUT_COLUMNS = [
    "date",
    "frequency_tag",
    "gdp_growth_pct",
    "current_account_pct_gdp",
    "public_debt_pct_gdp",
    "inflation_cpi_pct",
    "eur_mad",
    "usd_mad",
    "brent_usd",
    "wheat_usd",
    "gold_usd",
    "vix",
    "sp500_close",
    "em_close",
    "us10y_yield",
    "masi_close",
    "gdp_ci",
    "gdp_sn",
    "gdp_cm",
    "gdp_tn",
    "macro_momentum",
    "fx_pressure_eur",
    "global_risk_flag",
]


PHASE1_COLUMNS = [
    "inflation_cpi_pct",
    "eur_mad",
    "usd_mad",
    "brent_usd",
    "vix",
    "em_close",
    "masi_close",
]


WORLD_BANK_MA = {
    "gdp_growth_pct": "NY.GDP.MKTP.KD.ZG",
    "current_account_pct_gdp": "BN.CAB.XOKA.GD.ZS",
    # GC.DOD.TOTL.GD.ZS is CENTRAL government debt (~51% for MA) — kept as WB fallback only.
    # Primary source for public_debt_pct_gdp is IMF GGXWDG_NGDP (general government gross debt, ~67-70%).
    "public_debt_pct_gdp_wb": "GC.DOD.TOTL.GD.ZS",
    "inflation_cpi_pct_wb": "FP.CPI.TOTL.ZG",
}


WORLD_BANK_REGIONAL = {
    "gdp_ci": ("CI", "NY.GDP.MKTP.KD.ZG"),
    "gdp_sn": ("SN", "NY.GDP.MKTP.KD.ZG"),
    "gdp_cm": ("CM", "NY.GDP.MKTP.KD.ZG"),
    "gdp_tn": ("TN", "NY.GDP.MKTP.KD.ZG"),
}


YF_CANDIDATES = {
    "eur_mad": ["EURMAD=X"],
    "usd_mad": ["USDMAD=X"],
    "brent_usd": ["BZ=F"],
    # NOTE: "WEAT" is the Teucrium Wheat Fund ETF (USD/share, ~$20-25 range),
    # NOT CME wheat futures. Futures would be ZW=F in cents/bushel. The column
    # name wheat_usd refers to the ETF share price in USD.
    "wheat_usd": ["WEAT"],
    "gold_usd": ["GC=F"],
    "vix": ["^VIX"],
    "sp500_close": ["^GSPC"],
    # IEMG (iShares Core MSCI EM) tracks same index as EEM but cleaner yfinance
    # bars; EEM retained as fallback in case IEMG data is unavailable.
    "em_close": ["IEMG", "EEM"],
    "us10y_yield": ["^TNX"],
    "masi_close": ["MASI"],
}


def _to_datetime_index(series: pd.Series) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype=float)
    s = series.copy()
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s[~s.index.isna()]
    s = s[~s.index.duplicated(keep="last")]
    s = s.sort_index()
    return pd.to_numeric(s, errors="coerce")


def _parse_year_or_date(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}", text):
        return pd.Timestamp(f"{text}-12-31")
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt)


def fetch_world_bank_indicator(country_iso2: str, indicator: str, mrv: int = 20) -> pd.Series:
    url = f"https://api.worldbank.org/v2/country/{country_iso2}/indicator/{indicator}"
    resp = requests.get(url, params={"format": "json", "mrv": mrv}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(f"Unexpected World Bank payload for {country_iso2}:{indicator}")
    rows = payload[1] or []
    values: dict[pd.Timestamp, float] = {}
    for row in rows:
        dt = _parse_year_or_date(row.get("date"))
        val = row.get("value")
        if dt is None or val is None:
            continue
        values[dt] = float(val)
    return _to_datetime_index(pd.Series(values))


def _extract_year_map(node: Any) -> dict[str, Any] | None:
    if isinstance(node, dict):
        keys = list(node.keys())
        if keys and all(re.fullmatch(r"\d{4}", str(k)) for k in keys):
            return node
        for val in node.values():
            found = _extract_year_map(val)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _extract_year_map(item)
            if found is not None:
                return found
    return None


def fetch_imf_datamapper_series(indicator: str, country_iso3: str = "MAR") -> pd.Series:
    url = f"https://www.imf.org/external/datamapper/api/v1/{indicator}/{country_iso3}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    year_map = _extract_year_map(payload)
    if not year_map:
        raise ValueError(f"Unexpected IMF payload for {indicator}/{country_iso3}")
    values: dict[pd.Timestamp, float] = {}
    for y, v in year_map.items():
        dt = _parse_year_or_date(y)
        if dt is None or v is None:
            continue
        values[dt] = float(v)
    return _to_datetime_index(pd.Series(values))


def get_last_date(path: Path) -> pd.Timestamp | None:
    """Safely extracts the last valid date from the output CSV."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        # Read only the last few rows to find a date
        df = pd.read_csv(path, usecols=["date"]).tail(5)
        if df.empty:
            return None
        return pd.to_datetime(df["date"]).max()
    except Exception:
        return None


def fetch_yf_close(
    ticker: str,
    period: str | None = "10y",
    start: str | None = None,
    end: str | None = None,
    interval: str = "1d",
) -> pd.Series:
    """Fetches Close prices from yfinance using either period or start/end window."""
    # Force explicit exclusive end = tomorrow, so today's close is always included
    # and yfinance cannot silently return a stale forward-filled bar.
    if end is None:
        end = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    df = yf.download(
        tickers=ticker,
        period=None if start else period,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if isinstance(df.columns, pd.MultiIndex):
        if "Close" not in df.columns.get_level_values(0):
            return pd.Series(dtype=float)
        close = df["Close"].iloc[:, 0]
    else:
        if "Close" not in df.columns:
            return pd.Series(dtype=float)
        close = df["Close"]
    close.index = pd.to_datetime(close.index, errors="coerce").tz_localize(None)
    return _to_datetime_index(close)


def fetch_first_available_yf(candidates: list[str]) -> tuple[pd.Series, str | None]:
    for ticker in candidates:
        try:
            s = fetch_yf_close(ticker)
        except (requests.RequestException, OSError, ValueError) as exc:
            logger.warning("yfinance %s failed: %s", ticker, exc)
            continue
        if not s.empty:
            return s, ticker
    return pd.Series(dtype=float), None


def collect_series(start_date: str | None = None) -> dict[str, pd.Series]:
    series_map: dict[str, pd.Series] = {}
    yf_period = "10y" if not start_date else None

    # World Bank (Morocco)
    for output_col, indicator in WORLD_BANK_MA.items():
        try:
            series_map[output_col] = fetch_world_bank_indicator("MA", indicator, mrv=20)
            logger.info("WB MA %s -> %s (%d pts)", indicator, output_col, len(series_map[output_col]))
        except (requests.RequestException, ValueError, OSError) as exc:
            logger.warning("WB MA %s failed: %s", indicator, exc)
            series_map[output_col] = pd.Series(dtype=float)

    # World Bank (regional ATW footprint)
    for output_col, (country, indicator) in WORLD_BANK_REGIONAL.items():
        try:
            series_map[output_col] = fetch_world_bank_indicator(country, indicator, mrv=20)
            logger.info("WB %s %s -> %s (%d pts)", country, indicator, output_col, len(series_map[output_col]))
        except (requests.RequestException, ValueError, OSError) as exc:
            logger.warning("WB %s %s failed: %s", country, indicator, exc)
            series_map[output_col] = pd.Series(dtype=float)

    # IMF
    try:
        series_map["inflation_cpi_pct_imf"] = fetch_imf_datamapper_series("PCPIPCH", "MAR")
        logger.info("IMF PCPIPCH -> inflation_cpi_pct_imf (%d pts)", len(series_map["inflation_cpi_pct_imf"]))
    except (requests.RequestException, ValueError, OSError) as exc:
        logger.warning("IMF PCPIPCH failed: %s", exc)
        series_map["inflation_cpi_pct_imf"] = pd.Series(dtype=float)

    # IMF general government gross debt (% of GDP) — matches the ~67-70% figure
    # for Morocco, unlike WB's central-government-only GC.DOD.TOTL.GD.ZS.
    try:
        series_map["public_debt_pct_gdp_imf"] = fetch_imf_datamapper_series("GGXWDG_NGDP", "MAR")
        logger.info("IMF GGXWDG_NGDP -> public_debt_pct_gdp_imf (%d pts)", len(series_map["public_debt_pct_gdp_imf"]))
    except (requests.RequestException, ValueError, OSError) as exc:
        logger.warning("IMF GGXWDG_NGDP failed: %s", exc)
        series_map["public_debt_pct_gdp_imf"] = pd.Series(dtype=float)

    # yfinance
    for output_col, candidates in YF_CANDIDATES.items():
        for ticker in candidates:
            try:
                series = fetch_yf_close(ticker, period=yf_period, start=start_date)
                if not series.empty:
                    logger.info("yfinance %s -> %s (%d pts)", ticker, output_col, len(series))
                    series_map[output_col] = series
                    break
            except Exception as exc:
                logger.warning("yfinance %s failed: %s", ticker, exc)
        else:
            logger.warning("yfinance failed all candidates for %s", output_col)
            series_map[output_col] = pd.Series(dtype=float)

    return series_map


def _to_daily_ffill(series: pd.Series, full_index: pd.DatetimeIndex) -> pd.Series:
    s = _to_datetime_index(series)
    if s.empty:
        return pd.Series(index=full_index, dtype=float)
    s_daily = s.resample("D").ffill()
    s_daily = s_daily.reindex(full_index).ffill()
    return s_daily


def _prune_sparse_columns(
    df: pd.DataFrame,
    max_missing_ratio: float,
    preserve: set[str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    preserve_set = preserve or set()
    dropped: list[str] = []
    keep: list[str] = []

    for col in df.columns:
        if col in preserve_set:
            keep.append(col)
            continue
        missing_ratio = float(df[col].isna().mean())
        if missing_ratio > max_missing_ratio:
            dropped.append(col)
            continue
        keep.append(col)

    return df[keep].copy(), dropped


def build_daily_frame(
    series_map: dict[str, pd.Series],
    start_date: str,
    end_date: str | None,
    max_missing_ratio: float,
) -> pd.DataFrame:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) if end_date else pd.Timestamp(date.today())
    full_index = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame(index=full_index)

    direct_cols = [
        "gdp_growth_pct",
        "current_account_pct_gdp",
        "eur_mad",
        "usd_mad",
        "brent_usd",
        "wheat_usd",
        "gold_usd",
        "vix",
        "sp500_close",
        "em_close",
        "us10y_yield",
        "masi_close",
        "gdp_ci",
        "gdp_sn",
        "gdp_cm",
        "gdp_tn",
    ]
    for col in direct_cols:
        df[col] = _to_daily_ffill(series_map.get(col, pd.Series(dtype=float)), full_index)

    # Inflation precedence: World Bank -> IMF
    infl_wb = _to_daily_ffill(series_map.get("inflation_cpi_pct_wb", pd.Series(dtype=float)), full_index)
    infl_imf = _to_daily_ffill(series_map.get("inflation_cpi_pct_imf", pd.Series(dtype=float)), full_index)
    df["inflation_cpi_pct"] = infl_wb.combine_first(infl_imf)

    # Public debt precedence: IMF general government gross debt -> WB central gov't debt.
    # IMF GGXWDG_NGDP is the correct indicator (~67-70% for Morocco); WB GC.DOD.TOTL.GD.ZS
    # covers central government only and underestimates by ~15-20 pp.
    debt_imf = _to_daily_ffill(series_map.get("public_debt_pct_gdp_imf", pd.Series(dtype=float)), full_index)
    debt_wb = _to_daily_ffill(series_map.get("public_debt_pct_gdp_wb", pd.Series(dtype=float)), full_index)
    df["public_debt_pct_gdp"] = debt_imf.combine_first(debt_wb)

    # Required metadata + derived features
    df["frequency_tag"] = "daily_ffill"

    if df["gdp_growth_pct"].notna().any():
        df["macro_momentum"] = df["gdp_growth_pct"].diff(4)
    else:
        df["macro_momentum"] = pd.Series(index=full_index, dtype=float)

    if df["eur_mad"].notna().any():
        df["fx_pressure_eur"] = df["eur_mad"].pct_change(20)
    else:
        df["fx_pressure_eur"] = pd.Series(index=full_index, dtype=float)

    risk_flag = np.where(df["vix"].notna(), (df["vix"] > 25).astype(int), pd.NA)
    df["global_risk_flag"] = pd.Series(risk_flag, index=full_index, dtype="Int64")

    out = df.reset_index().rename(columns={"index": "date"})
    out["date"] = out["date"].dt.date.astype(str)

    # --- Data Cleaning (per USER request): Remove rows with missing core daily data ---
    # We drop rows where crucial Yahoo Finance columns are NaN to remove sparse historical rows.
    # This ensures the dataset effectively starts from ~2016-04-20 when daily tracking begins.
    clean_subset = ["eur_mad", "usd_mad", "masi_close", "vix"]
    existing_subset = [c for c in clean_subset if c in out.columns]
    if existing_subset:
        initial_len = len(out)
        out = out.dropna(subset=existing_subset, how="all")
        if len(out) < initial_len:
            logger.info("Removed %d sparse historical rows (missing core daily indicators).", initial_len - len(out))

    out = out[OUTPUT_COLUMNS]
    out, dropped = _prune_sparse_columns(
        out,
        max_missing_ratio=max_missing_ratio,
        preserve={"date", "frequency_tag"},
    )
    if dropped:
        logger.info("Dropped sparse columns (missing ratio > %.2f): %s", max_missing_ratio, ",".join(dropped))
    return out


def write_output(df: pd.DataFrame, out_path: Path, full_refresh: bool) -> pd.DataFrame:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    target_columns = list(df.columns)

    if out_path.exists() and not full_refresh:
        try:
            existing = pd.read_csv(out_path)
            if existing.empty:
                combined = df.copy()
            else:
                combined = pd.concat([existing, df], ignore_index=True, sort=False)
        except pd.errors.EmptyDataError:
            logger.warning("Existing file was empty. Starting fresh.")
            combined = df.copy()
        except Exception as exc:
            logger.warning("Could not read existing file: %s. Starting fresh.", exc)
            combined = df.copy()
        
        if "date" in combined.columns:
            combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
            combined = combined.dropna(subset=["date"])
            combined = combined.sort_values("date")
            combined = combined.drop_duplicates(subset=["date"], keep="last")
            combined["date"] = combined["date"].dt.date.astype(str)
        for col in target_columns:
            if col not in combined.columns:
                combined[col] = np.nan
        combined = combined[target_columns]
    else:
        combined = df.copy()

    combined.to_csv(out_path, index=False)
    return combined


def log_summary(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("Output dataframe is empty.")
        return
    logger.info("Rows: %d | Date range: %s -> %s", len(df), df["date"].iloc[0], df["date"].iloc[-1])
    for col in PHASE1_COLUMNS:
        non_null = int(df[col].notna().sum()) if col in df.columns else 0
        logger.info("Phase-1 coverage %-24s : %d non-null", col, non_null)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect ATW macro/market context dataset.")
    p.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path.")
    p.add_argument("--start-date", type=str, default=DEFAULT_START, help="Start date YYYY-MM-DD.")
    p.add_argument("--end-date", type=str, default=None, help="End date YYYY-MM-DD (default: today).")
    p.add_argument(
        "--max-missing-ratio",
        type=float,
        default=1.0,
        help="Drop feature columns with missing ratio above this threshold (0.0 to 1.0). Default 1.0 keeps all columns.",
    )
    p.add_argument("--full-refresh", action="store_true", help="Rewrite output file from scratch.")
    p.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    if not 0.0 <= args.max_missing_ratio <= 1.0:
        raise ValueError("--max-missing-ratio must be between 0.0 and 1.0")

    # Determine window for collection
    start_date = args.start_date
    is_incremental = False

    if not args.full_refresh:
        last_date = get_last_date(args.out)
        if last_date:
            # Lookback 30 days to ensure windowed indicators (fx_pressure, momentum) are correct
            lookback = last_date - timedelta(days=30)
            start_date = lookback.strftime("%Y-%m-%d")
            is_incremental = True
            logger.info("Incremental update: resuming from %s (lookback to %s)", last_date.date(), start_date)
    
    if not is_incremental:
        logger.info("Full refresh/Initial run: scraping from %s", start_date)

    logger.info("Collecting ATW macro series...")
    series_map = collect_series(start_date=start_date if is_incremental else None)

    logger.info("Building daily merged dataset...")
    daily_df = build_daily_frame(
        series_map,
        start_date=start_date,
        end_date=args.end_date,
        max_missing_ratio=args.max_missing_ratio,
    )

    logger.info("Writing output to %s", args.out)
    final_df = write_output(daily_df, args.out, full_refresh=args.full_refresh)

    log_summary(final_df)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
