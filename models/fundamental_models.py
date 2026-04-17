"""Unified fundamental valuation models and data loader."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Type

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Inlined constants so this file runs without requiring utils/financial_constants.py.
RISK_FREE_RATE = 0.035
EQUITY_RISK_PREMIUM = 0.065
CORPORATE_TAX_RATE = 0.31
TERMINAL_GROWTH_RATE = 0.025
STOCK_BETA = 0.90
IAM_BETA = STOCK_BETA
NUM_SHARES = 215_140_839
COST_OF_EQUITY = RISK_FREE_RATE + STOCK_BETA * EQUITY_RISK_PREMIUM
SECTOR_BENCHMARKS = {
    "pe_ratio": 13.0,
    "ev_ebitda": 6.5,
    "ev_sales": 3.0,
    "price_to_book": 1.8,
    "dividend_yield": 4.5,
    "roe": 15.0,
    "roa": 1.2,
    "net_margin": 25.0,
    "ebitda_margin": 40.0,
    "operating_margin": 35.0,
    "debt_to_equity": 1.0,
    "current_ratio": 1.0,
}


@dataclass
class ValuationResult:
    """Output of a single valuation model."""

    model_name: str
    intrinsic_value: float
    intrinsic_value_low: Optional[float] = None
    intrinsic_value_high: Optional[float] = None
    upside_pct: float = 0.0
    confidence: float = 0.0
    methodology: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


class BaseValuationModel(ABC):
    """Base class all valuation models inherit from."""

    def __init__(self, stock_data: dict, constants: dict):
        self.data = stock_data
        self.constants = constants

    @abstractmethod
    def calculate(self) -> ValuationResult:
        pass

    def _current_price(self) -> float:
        return self.data.get("current_price") or self.data["price_performance"]["last_price"]

    def _get_financial(self, field: str, year: Optional[str] = None) -> Optional[float]:
        fin = self.data.get("financials", {})
        field_data = fin.get(field)
        if field_data is None:
            return None
        if isinstance(field_data, dict):
            if year:
                return field_data.get(year)
            for y in sorted(field_data.keys(), reverse=True):
                if field_data[y] is not None:
                    return field_data[y]
            return None
        return field_data

    def _get_valuation(self, field: str) -> Optional[float]:
        return self.data.get("valuation", {}).get(field)

    def _get_hist_values(self, section: str, field: str, years: Optional[List[str]] = None) -> Dict[str, float]:
        data = self.data.get(section, {}).get(field, {})
        if not isinstance(data, dict):
            return {}
        if years:
            return {y: v for y, v in data.items() if y in years and v is not None}
        return {y: v for y, v in data.items() if v is not None}

    def _compute_upside(self, fair_value: float) -> float:
        price = self._current_price()
        if price and price > 0:
            return ((fair_value - price) / price) * 100
        return 0.0


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _deep_merge(base: Dict[str, Any], updates: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _load_latest_market_row(csv_path: Path) -> Dict[str, str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing market data file: {csv_path}")

    rows: List[Dict[str, str]] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("Dernier Cours") or "").strip():
                rows.append(row)

    if not rows:
        raise ValueError(f"No usable rows in {csv_path}")
    return rows[-1]


def _load_model_inputs(inputs_path: Path) -> Dict[str, Any]:
    if not inputs_path.exists():
        return {}
    with open(inputs_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"{inputs_path} must contain a JSON object")
    return payload


def _load_json_object(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _load_latest_csv_row(path: Path, key_field: str) -> Dict[str, str]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get(key_field) or "").strip():
                rows.append(row)
    if not rows:
        raise ValueError(f"No usable rows in {path}")
    return rows[-1]


def _to_number_series(raw: Any) -> Dict[str, float]:
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        raw = json.loads(raw)
    if not isinstance(raw, Mapping):
        return {}

    out: Dict[str, float] = {}
    for year, value in raw.items():
        num = _to_float(value)
        if num is not None and math.isfinite(num):
            out[str(year)] = num
    return out


def _market_cap_to_millions(value: Any) -> Optional[float]:
    cap = _to_float(value)
    if cap is None:
        return None
    return cap / 1_000_000 if cap > 1_000_000 else cap


def _map_scraper_payload_to_model_inputs(raw: Mapping[str, Any], *, csv_row: bool) -> Dict[str, Any]:
    def hist(field: str) -> Dict[str, float]:
        key = f"{field}_json" if csv_row else field
        return _to_number_series(raw.get(key))

    mapped: Dict[str, Any] = {
        "price_performance": {
            "last_price": _to_float(raw.get("price")),
            "high_52w": _to_float(raw.get("high_52w")),
            "low_52w": _to_float(raw.get("low_52w")),
            "volume": _to_float(raw.get("volume")),
        },
        "financials": {
            "net_sales": hist("hist_revenue"),
            "revenues": hist("hist_revenue"),
            "net_income": hist("hist_net_income"),
            "eps": hist("hist_eps"),
            "ebit": hist("hist_ebit"),
            "ebitda": hist("hist_ebitda"),
            "free_cash_flow": hist("hist_fcf"),
            "operating_cash_flow": hist("hist_ocf"),
            "capex": hist("hist_capex"),
            "total_debt": hist("hist_debt"),
            "cash_and_equivalents": hist("hist_cash"),
            "shareholders_equity": hist("hist_equity"),
            "net_margin": hist("hist_net_margin"),
            "ebit_margin": hist("hist_ebit_margin"),
            "ebitda_margin": hist("hist_ebitda_margin"),
            "gross_margin": hist("hist_gross_margin"),
            "roe": hist("hist_roe"),
            "roce": hist("hist_roce"),
            "dividend_per_share": hist("hist_dividend_per_share"),
        },
        "valuation": {
            "market_cap": _market_cap_to_millions(raw.get("market_cap")),
            "pe_ratio": _to_float(raw.get("pe_ratio")),
            "price_to_book": _to_float(raw.get("price_to_book")),
            "dividend_yield": _to_float(raw.get("dividend_yield")),
            "ev_ebitda_hist": hist("hist_ev_ebitda"),
            "pe_ratio_hist": hist("pe_ratio_hist"),
            "pbr_hist": hist("pbr_hist"),
            "ev_revenue_hist": hist("ev_revenue_hist"),
            "ev_ebit_hist": hist("ev_ebit_hist"),
            "capitalization_hist": hist("capitalization_hist"),
            "fcf_yield_hist": hist("fcf_yield_hist"),
            "dividend_per_share_hist": hist("hist_dividend_per_share"),
            "eps_growth_hist": hist("hist_eps_growth"),
        },
    }
    return mapped


def _load_scraper_merged_inputs(data_dir: Path, ticker: str) -> Dict[str, Any]:
    fundamentals_json_path = data_dir / f"{ticker}_fondamental.json"
    if fundamentals_json_path.exists():
        raw_json = _load_json_object(fundamentals_json_path)
        return _map_scraper_payload_to_model_inputs(raw_json, csv_row=False)

    merged_json_path = data_dir / "historical" / f"{ticker}_merged.json"
    if merged_json_path.exists():
        raw_json = _load_json_object(merged_json_path)
        return _map_scraper_payload_to_model_inputs(raw_json, csv_row=False)

    # Legacy fallback path (for pre-simplification datasets).
    merged: Dict[str, Any] = {}
    raw_json_path = data_dir / "historical" / f"{ticker}_marketscreener_v3.json"
    if raw_json_path.exists():
        raw_json = _load_json_object(raw_json_path)
        _deep_merge(merged, _map_scraper_payload_to_model_inputs(raw_json, csv_row=False))

    fond_csv_path = data_dir / f"{ticker}_fondamental.csv"
    if fond_csv_path.exists():
        row = _load_latest_csv_row(fond_csv_path, key_field="symbol")
        _deep_merge(merged, _map_scraper_payload_to_model_inputs(row, csv_row=True))

    return merged


def load_stock_data(data_dir: Optional[str] = None, ticker: str = "ATW") -> Dict[str, Any]:
    """Build merged stock_data from files under data/ before valuation."""

    project_root = Path(__file__).resolve().parent.parent
    base_data_dir = Path(data_dir) if data_dir else (project_root / "data")
    csv_path = base_data_dir / f"{ticker}_bourse_casa_full.csv"
    inputs_path = base_data_dir / f"{ticker}_model_inputs.json"
    fundamentals_json_path = base_data_dir / f"{ticker}_fondamental.json"
    merged_json_path = base_data_dir / "historical" / f"{ticker}_merged.json"

    market_row = _load_latest_market_row(csv_path)
    current_price = _to_float(market_row.get("Dernier Cours"))
    if current_price is None:
        raise ValueError(f"Could not parse 'Dernier Cours' from {csv_path}")

    market_cap_mad = _to_float(market_row.get("Capitalisation"))
    scraper_merged_inputs = _load_scraper_merged_inputs(base_data_dir, ticker)
    has_primary_fundamental_source = fundamentals_json_path.exists() or merged_json_path.exists()
    model_inputs = {} if has_primary_fundamental_source else _load_model_inputs(inputs_path)

    stock_data: Dict[str, Any] = {
        "identity": {
            "ticker": ticker,
            "full_name": "Attijariwafa Bank",
            "exchange": "Casablanca Stock Exchange",
            "currency": "MAD",
        },
        "current_price": current_price,
        "price_performance": {"last_price": current_price},
        "financials": {},
        "valuation": {},
        "consensus": {},
    }

    # Primary mode: merged periodic JSON + Bourse Casa market row.
    # Legacy mode: older files are merged only if the primary merged JSON is missing.
    _deep_merge(stock_data, scraper_merged_inputs)
    _deep_merge(stock_data, model_inputs)

    # Bourse Casa is the source of truth for market price and market cap.
    stock_data["current_price"] = current_price
    price_perf = stock_data.setdefault("price_performance", {})
    if not isinstance(price_perf, dict):
        stock_data["price_performance"] = {"last_price": current_price}
    else:
        price_perf["last_price"] = current_price

    valuation = stock_data.setdefault("valuation", {})
    if not isinstance(valuation, dict):
        stock_data["valuation"] = {}
        valuation = stock_data["valuation"]
    if market_cap_mad is not None:
        valuation["market_cap"] = market_cap_mad / 1_000_000

    return stock_data


class DCFModel(BaseValuationModel):
    """Two-stage DCF with terminal value via Gordon Growth Model."""

    def calculate(self) -> ValuationResult:
        wacc = self._compute_wacc()
        fcf_projections = self._get_fcf_projections()
        if not fcf_projections:
            return ValuationResult(
                model_name="DCF",
                intrinsic_value=0,
                confidence=0,
                methodology="DCF — insufficient FCF data",
            )

        fcf_projections = self._extend_projections(fcf_projections, years_total=5)
        terminal_fcf = fcf_projections[-1]
        terminal_value = self._terminal_value(terminal_fcf, wacc, TERMINAL_GROWTH_RATE)
        enterprise_value = self._discount_cashflows(fcf_projections, terminal_value, wacc)

        net_debt = self._get_net_debt()
        cash = self._get_cash()
        equity_value = enterprise_value - net_debt + cash
        per_share = (equity_value * 1_000_000) / NUM_SHARES

        low = self._run_scenario(
            fcf_projections,
            wacc + 0.01,
            TERMINAL_GROWTH_RATE - 0.005,
            net_debt,
            cash,
        )
        high = self._run_scenario(
            fcf_projections,
            wacc - 0.01,
            TERMINAL_GROWTH_RATE + 0.005,
            net_debt,
            cash,
        )

        upside = self._compute_upside(per_share)
        confidence = min(80, 40 + len(fcf_projections) * 8)

        return ValuationResult(
            model_name="DCF",
            intrinsic_value=round(per_share, 2),
            intrinsic_value_low=round(low, 2),
            intrinsic_value_high=round(high, 2),
            upside_pct=round(upside, 1),
            confidence=confidence,
            methodology="Two-stage DCF with Gordon Growth terminal value",
            details={
                "wacc": round(wacc * 100, 2),
                "terminal_growth": round(TERMINAL_GROWTH_RATE * 100, 2),
                "enterprise_value_m": round(enterprise_value, 0),
                "net_debt_m": round(net_debt, 0),
                "fcf_projections": [round(f, 0) for f in fcf_projections],
            },
        )

    def _compute_wacc(self) -> float:
        cost_of_equity = RISK_FREE_RATE + IAM_BETA * EQUITY_RISK_PREMIUM

        market_cap = self._get_valuation("market_cap") or 83_954
        total_debt = self._get_financial("total_debt", "2025") or 19_603
        total_capital = market_cap + total_debt

        cost_of_debt = RISK_FREE_RATE + 0.015

        weight_equity = market_cap / total_capital
        weight_debt = total_debt / total_capital

        return weight_equity * cost_of_equity + weight_debt * cost_of_debt * (1 - CORPORATE_TAX_RATE)

    def _get_fcf_projections(self) -> List[float]:
        fcf = self._get_hist_values("financials", "free_cash_flow")
        forecast_years = sorted(y for y in fcf if int(y) >= 2026)
        values = [fcf[y] for y in forecast_years if fcf[y] is not None and fcf[y] > 0]
        if values:
            return values

        # Use EBITDA when available, otherwise EBIT — banks report EBIT only.
        ebitda = self._get_hist_values("financials", "ebitda")
        ebit = self._get_hist_values("financials", "ebit")
        earnings_series = ebitda or ebit
        capex = self._get_hist_values("financials", "capex")
        for year in ["2025", "2024", "2023"]:
            eb = earnings_series.get(year)
            cx = capex.get(year)
            if eb and cx:
                fcf_approx = eb * (1 - CORPORATE_TAX_RATE) - abs(cx)
                if fcf_approx > 0:
                    return [fcf_approx]

        # Last-resort fallback: most recent historical FCF if we have any.
        for year in sorted(fcf.keys(), reverse=True):
            val = fcf[year]
            if val is not None and val > 0:
                return [val]
        return []

    def _extend_projections(self, fcf: List[float], years_total: int = 5) -> List[float]:
        while len(fcf) < years_total:
            remaining = years_total - len(fcf)
            decay_rate = TERMINAL_GROWTH_RATE + 0.02 * (remaining / years_total)
            fcf.append(fcf[-1] * (1 + decay_rate))
        return fcf[:years_total]

    def _terminal_value(self, fcf_terminal: float, wacc: float, growth: float) -> float:
        if wacc <= growth:
            return fcf_terminal * 20
        return fcf_terminal * (1 + growth) / (wacc - growth)

    def _discount_cashflows(self, fcfs: List[float], terminal_value: float, wacc: float) -> float:
        pv_fcf = sum(fcf / (1 + wacc) ** (i + 1) for i, fcf in enumerate(fcfs))
        pv_terminal = terminal_value / (1 + wacc) ** len(fcfs)
        return pv_fcf + pv_terminal

    def _get_net_debt(self) -> float:
        nd = self._get_financial("net_debt", "2025")
        if nd and nd > 100:
            return nd
        debt = self._get_financial("total_debt", "2025") or 0
        return debt - self._get_cash()

    def _get_cash(self) -> float:
        return self._get_financial("cash_and_equivalents", "2025") or 0

    def _run_scenario(self, fcf: List[float], wacc: float, growth: float, net_debt: float, cash: float) -> float:
        tv = self._terminal_value(fcf[-1], wacc, growth)
        ev = self._discount_cashflows(fcf, tv, wacc)
        equity = ev - net_debt + cash
        return (equity * 1_000_000) / NUM_SHARES


class DDMModel(BaseValuationModel):
    """Three-stage Dividend Discount Model."""

    def calculate(self) -> ValuationResult:
        cost_of_equity = COST_OF_EQUITY
        stage1_divs = self._get_stage1_dividends()
        if not stage1_divs:
            return ValuationResult(
                model_name="DDM",
                intrinsic_value=0,
                confidence=0,
                methodology="DDM — insufficient dividend data",
            )

        stage1_growth = self._compute_div_growth(stage1_divs)
        stage2_divs = self._project_stage2(stage1_divs[-1], stage1_growth, years=5)

        terminal_div = stage2_divs[-1] * (1 + TERMINAL_GROWTH_RATE)
        if cost_of_equity <= TERMINAL_GROWTH_RATE:
            terminal_value = terminal_div * 30
        else:
            terminal_value = terminal_div / (cost_of_equity - TERMINAL_GROWTH_RATE)

        all_divs = stage1_divs + stage2_divs
        total_years = len(all_divs)
        pv_dividends = sum(d / (1 + cost_of_equity) ** (i + 1) for i, d in enumerate(all_divs))
        pv_terminal = terminal_value / (1 + cost_of_equity) ** total_years
        fair_value = pv_dividends + pv_terminal

        low = self._run_scenario(all_divs, cost_of_equity + 0.01)
        high = self._run_scenario(all_divs, cost_of_equity - 0.01)

        upside = self._compute_upside(fair_value)
        confidence = min(75, 35 + len(stage1_divs) * 10)

        return ValuationResult(
            model_name="DDM",
            intrinsic_value=round(fair_value, 2),
            intrinsic_value_low=round(low, 2),
            intrinsic_value_high=round(high, 2),
            upside_pct=round(upside, 1),
            confidence=confidence,
            methodology="Three-stage DDM (explicit + transition + terminal)",
            details={
                "cost_of_equity_pct": round(cost_of_equity * 100, 2),
                "stage1_dividends": [round(d, 2) for d in stage1_divs],
                "stage1_growth_pct": round(stage1_growth * 100, 2),
                "terminal_growth_pct": round(TERMINAL_GROWTH_RATE * 100, 2),
                "pv_dividends": round(pv_dividends, 2),
                "pv_terminal": round(pv_terminal, 2),
            },
        )

    def _get_stage1_dividends(self) -> List[float]:
        dps_hist = self._get_hist_values("valuation", "dividend_per_share_hist")
        forecast_years = sorted(y for y in dps_hist if int(y) >= 2026)
        divs = [dps_hist[y] for y in forecast_years if dps_hist[y] is not None and dps_hist[y] > 0]
        if divs:
            return divs

        for year in sorted(dps_hist.keys(), reverse=True):
            if dps_hist[year] and dps_hist[year] > 0:
                return [dps_hist[year]]
        return []

    def _compute_div_growth(self, divs: List[float]) -> float:
        if len(divs) < 2:
            return TERMINAL_GROWTH_RATE
        growth_rates = []
        for i in range(1, len(divs)):
            if divs[i - 1] > 0:
                growth_rates.append((divs[i] - divs[i - 1]) / divs[i - 1])
        if growth_rates:
            return sum(growth_rates) / len(growth_rates)
        return TERMINAL_GROWTH_RATE

    def _project_stage2(self, last_div: float, initial_growth: float, years: int = 5) -> List[float]:
        divs = []
        for i in range(years):
            weight = (i + 1) / years
            growth = initial_growth * (1 - weight) + TERMINAL_GROWTH_RATE * weight
            last_div = last_div * (1 + growth)
            divs.append(last_div)
        return divs

    def _run_scenario(self, all_divs: List[float], cost_of_equity: float) -> float:
        pv = sum(d / (1 + cost_of_equity) ** (i + 1) for i, d in enumerate(all_divs))
        terminal_div = all_divs[-1] * (1 + TERMINAL_GROWTH_RATE)
        if cost_of_equity <= TERMINAL_GROWTH_RATE:
            tv = terminal_div * 30
        else:
            tv = terminal_div / (cost_of_equity - TERMINAL_GROWTH_RATE)
        pv_tv = tv / (1 + cost_of_equity) ** len(all_divs)
        return pv + pv_tv


class GrahamModel(BaseValuationModel):
    """Graham Number + Graham Growth Formula + NCAV."""

    def calculate(self) -> ValuationResult:
        eps = self._get_eps()
        bvps = self._get_book_value_per_share()
        results: Dict[str, Any] = {}

        graham_number = None
        if eps and eps > 0 and bvps and bvps > 0:
            graham_number = math.sqrt(22.5 * eps * bvps)
            results["graham_number"] = round(graham_number, 2)

        graham_growth = None
        growth_rate = self._estimate_growth_rate()
        bond_yield = max(RISK_FREE_RATE * 100, 3.0)
        if eps and eps > 0 and growth_rate is not None:
            graham_growth = eps * (8.5 + 2 * growth_rate) * 4.4 / bond_yield
            results["graham_growth_formula"] = round(graham_growth, 2)
            results["assumed_growth_rate"] = round(growth_rate, 2)
            results["bond_yield_used"] = round(bond_yield, 2)

        ncav = self._compute_ncav()
        if ncav is not None:
            results["ncav_per_share"] = round(ncav, 2)

        if graham_growth and graham_growth > 0:
            primary = graham_growth
        elif graham_number and graham_number > 0:
            primary = graham_number
        else:
            return ValuationResult(
                model_name="Graham",
                intrinsic_value=0,
                confidence=0,
                methodology="Graham — insufficient data (need EPS and BVPS)",
            )

        low_val = min(v for v in [graham_number, graham_growth, ncav] if v and v > 0)
        high_val = max(v for v in [graham_number, graham_growth, ncav] if v and v > 0)
        upside = self._compute_upside(primary)
        confidence = 70 if (eps and bvps) else 40

        return ValuationResult(
            model_name="Graham",
            intrinsic_value=round(primary, 2),
            intrinsic_value_low=round(low_val, 2),
            intrinsic_value_high=round(high_val, 2),
            upside_pct=round(upside, 1),
            confidence=confidence,
            methodology="Graham Number + Growth Formula (The Intelligent Investor)",
            details=results,
        )

    def _get_eps(self) -> Optional[float]:
        eps_data = self._get_hist_values("financials", "eps")
        if eps_data:
            return eps_data[max(eps_data.keys())]
        eps_hist = self._get_hist_values("valuation", "eps_hist")
        if eps_hist:
            return eps_hist[max(eps_hist.keys())]
        return None

    def _get_book_value_per_share(self) -> Optional[float]:
        bvps = self._get_hist_values("financials", "book_value_per_share")
        if bvps:
            return bvps[max(bvps.keys())]

        equity = self._get_financial("shareholders_equity", "2025")
        if equity:
            return (equity * 1_000_000) / NUM_SHARES
        return None

    def _estimate_growth_rate(self) -> Optional[float]:
        eps_data = self._get_hist_values("financials", "eps")
        if len(eps_data) >= 2:
            years = sorted(eps_data.keys())
            first_val = eps_data[years[0]]
            last_val = eps_data[years[-1]]
            n_years = int(years[-1]) - int(years[0])
            if first_val and first_val > 0 and last_val and n_years > 0:
                cagr = (last_val / first_val) ** (1 / n_years) - 1
                return cagr * 100

        sales = self._get_hist_values("financials", "net_sales")
        if len(sales) >= 2:
            years = sorted(sales.keys())
            first_val = sales[years[0]]
            last_val = sales[years[-1]]
            n_years = int(years[-1]) - int(years[0])
            if first_val and first_val > 0 and n_years > 0:
                return ((last_val / first_val) ** (1 / n_years) - 1) * 100

        return 3.0

    def _compute_ncav(self) -> Optional[float]:
        total_assets = self._get_financial("total_assets", "2025")
        total_liabilities = self._get_financial("total_liabilities", "2025")
        if total_assets and total_liabilities:
            return (total_assets - total_liabilities) * 1_000_000 / NUM_SHARES
        return None


class RelativeValuationModel(BaseValuationModel):
    """Multiples-based relative valuation."""

    MULTIPLE_WEIGHTS = {
        "pe": 0.25,
        "ev_ebitda": 0.35,
        "pb": 0.15,
        "ev_revenue": 0.10,
        "fcf_yield": 0.15,
    }

    def calculate(self) -> ValuationResult:
        implied_values: Dict[str, float] = {}
        details: Dict[str, Dict[str, Any]] = {}

        pe_fv = self._pe_fair_value()
        if pe_fv:
            implied_values["pe"] = pe_fv["value"]
            details["pe"] = pe_fv

        ev_ebitda_fv = self._ev_ebitda_fair_value()
        if ev_ebitda_fv:
            implied_values["ev_ebitda"] = ev_ebitda_fv["value"]
            details["ev_ebitda"] = ev_ebitda_fv

        pb_fv = self._pb_fair_value()
        if pb_fv:
            implied_values["pb"] = pb_fv["value"]
            details["pb"] = pb_fv

        ev_rev_fv = self._ev_revenue_fair_value()
        if ev_rev_fv:
            implied_values["ev_revenue"] = ev_rev_fv["value"]
            details["ev_revenue"] = ev_rev_fv

        fcf_fv = self._fcf_yield_fair_value()
        if fcf_fv:
            implied_values["fcf_yield"] = fcf_fv["value"]
            details["fcf_yield"] = fcf_fv

        if not implied_values:
            return ValuationResult(
                model_name="Relative Valuation",
                intrinsic_value=0,
                confidence=0,
                methodology="Relative — insufficient multiples data",
            )

        total_weight = sum(self.MULTIPLE_WEIGHTS.get(k, 0) for k in implied_values)
        composite = sum(implied_values[k] * self.MULTIPLE_WEIGHTS.get(k, 0) for k in implied_values) / total_weight

        all_values = list(implied_values.values())
        low_val = min(all_values)
        high_val = max(all_values)
        upside = self._compute_upside(composite)
        confidence = min(80, 30 + len(implied_values) * 10)

        return ValuationResult(
            model_name="Relative Valuation",
            intrinsic_value=round(composite, 2),
            intrinsic_value_low=round(low_val, 2),
            intrinsic_value_high=round(high_val, 2),
            upside_pct=round(upside, 1),
            confidence=confidence,
            methodology="Weighted composite of historical and sector multiples",
            details=details,
        )

    def _pe_fair_value(self) -> Optional[Dict[str, Any]]:
        pe_hist = self._get_hist_values("valuation", "pe_ratio_hist", years=["2021", "2022", "2023", "2024", "2025"])
        if not pe_hist:
            return None

        hist_median = statistics.median(pe_hist.values())
        sector_pe = SECTOR_BENCHMARKS["pe_ratio"]
        blended_pe = hist_median * 0.6 + sector_pe * 0.4

        eps = self._get_latest_eps()
        if not eps or eps <= 0:
            return None

        fair_value = blended_pe * eps
        return {
            "value": round(fair_value, 2),
            "historical_median_pe": round(hist_median, 1),
            "sector_pe": sector_pe,
            "blended_pe": round(blended_pe, 1),
            "eps_used": round(eps, 2),
        }

    def _ev_ebitda_fair_value(self) -> Optional[Dict[str, Any]]:
        years = ["2021", "2022", "2023", "2024", "2025"]
        ev_ebitda_hist = self._get_hist_values("valuation", "ev_ebitda_hist", years=years)
        ebitda = self._get_financial("ebitda", "2025")

        # Primary path: EV/EBITDA × latest EBITDA.
        if ev_ebitda_hist and ebitda and ebitda > 0:
            hist_median = statistics.median(ev_ebitda_hist.values())
            sector_ev = SECTOR_BENCHMARKS["ev_ebitda"]
            blended = hist_median * 0.6 + sector_ev * 0.4
            net_debt = self._get_financial("net_debt", "2025") or 0
            cash = self._get_financial("cash_and_equivalents", "2025") or 0
            implied_ev = blended * ebitda
            equity_value = implied_ev - net_debt + cash
            per_share = (equity_value * 1_000_000) / NUM_SHARES
            return {
                "value": round(per_share, 2),
                "historical_median": round(hist_median, 2),
                "sector_benchmark": sector_ev,
                "blended_multiple": round(blended, 2),
                "ebitda_used": round(ebitda, 0),
                "multiple_used": "EV/EBITDA",
            }

        # Bank fallback: EV/EBIT × latest EBIT.
        ev_ebit_hist = self._get_hist_values("valuation", "ev_ebit_hist", years=years)
        ebit = self._get_financial("ebit", "2025")
        if not ev_ebit_hist or not ebit or ebit <= 0:
            return None

        hist_median = statistics.median(ev_ebit_hist.values())
        sector_ev = SECTOR_BENCHMARKS["ev_ebitda"]
        blended = hist_median * 0.6 + sector_ev * 0.4
        net_debt = self._get_financial("net_debt", "2025") or 0
        cash = self._get_financial("cash_and_equivalents", "2025") or 0
        implied_ev = blended * ebit
        equity_value = implied_ev - net_debt + cash
        per_share = (equity_value * 1_000_000) / NUM_SHARES
        return {
            "value": round(per_share, 2),
            "historical_median": round(hist_median, 2),
            "sector_benchmark": sector_ev,
            "blended_multiple": round(blended, 2),
            "ebit_used": round(ebit, 0),
            "multiple_used": "EV/EBIT",
        }

    def _pb_fair_value(self) -> Optional[Dict[str, Any]]:
        pb_hist = self._get_hist_values("valuation", "pbr_hist", years=["2021", "2022", "2023", "2024", "2025"])
        if not pb_hist:
            return None

        hist_median = statistics.median(pb_hist.values())
        sector_pb = SECTOR_BENCHMARKS["price_to_book"]
        blended = hist_median * 0.6 + sector_pb * 0.4

        bvps = self._get_book_value_per_share()
        if not bvps or bvps <= 0:
            return None

        fair_value = blended * bvps
        return {
            "value": round(fair_value, 2),
            "historical_median_pb": round(hist_median, 2),
            "sector_pb": sector_pb,
            "bvps_used": round(bvps, 2),
        }

    def _ev_revenue_fair_value(self) -> Optional[Dict[str, Any]]:
        ev_rev_hist = self._get_hist_values(
            "valuation",
            "ev_revenue_hist",
            years=["2021", "2022", "2023", "2024", "2025"],
        )
        if not ev_rev_hist:
            return None

        hist_median = statistics.median(ev_rev_hist.values())
        sector_ev_rev = SECTOR_BENCHMARKS["ev_sales"]
        blended = hist_median * 0.6 + sector_ev_rev * 0.4

        revenue = self._get_financial("net_sales", "2025")
        net_debt = self._get_financial("net_debt", "2025") or 0
        cash = self._get_financial("cash_and_equivalents", "2025") or 0
        if not revenue or revenue <= 0:
            return None

        implied_ev = blended * revenue
        equity_value = implied_ev - net_debt + cash
        per_share = (equity_value * 1_000_000) / NUM_SHARES
        return {
            "value": round(per_share, 2),
            "historical_median": round(hist_median, 2),
            "blended_multiple": round(blended, 2),
        }

    def _fcf_yield_fair_value(self) -> Optional[Dict[str, Any]]:
        fcf_yield_hist = self._get_hist_values(
            "valuation",
            "fcf_yield_hist",
            years=["2021", "2022", "2023", "2024", "2025"],
        )
        if not fcf_yield_hist:
            return None

        hist_median = statistics.median(fcf_yield_hist.values())
        if hist_median <= 0:
            return None

        price = self._current_price()
        current_yield = self._get_valuation("fcf_yield_hist")
        if isinstance(current_yield, dict):
            current_yield = current_yield.get("2025")
        if current_yield and current_yield > 0:
            fair_value = price * (current_yield / hist_median)
            return {
                "value": round(fair_value, 2),
                "current_yield": round(current_yield, 2),
                "historical_median_yield": round(hist_median, 2),
            }
        return None

    def _get_latest_eps(self) -> Optional[float]:
        eps = self._get_hist_values("financials", "eps")
        if eps:
            return eps[max(eps.keys())]
        return None

    def _get_book_value_per_share(self) -> Optional[float]:
        bvps = self._get_hist_values("financials", "book_value_per_share")
        if bvps:
            return bvps[max(bvps.keys())]

        equity = self._get_financial("shareholders_equity", "2025")
        if equity:
            return (equity * 1_000_000) / NUM_SHARES
        return None


class MonteCarloModel(BaseValuationModel):
    """Monte Carlo DCF simulation."""

    N_SIMULATIONS = 10_000
    FORECAST_YEARS = 5

    def calculate(self) -> ValuationResult:
        base_revenue = self._get_base_revenue()
        base_ebitda_margin = self._get_base_margin()
        base_capex_ratio = self._get_capex_ratio()
        net_debt = self._get_net_debt()
        cash = self._get_cash()

        if not base_revenue or not base_ebitda_margin:
            return ValuationResult(
                model_name="Monte Carlo",
                intrinsic_value=0,
                confidence=0,
                methodology="Monte Carlo — insufficient base data",
            )

        np.random.seed(42)
        rev_growth = np.random.normal(0.015, 0.020, self.N_SIMULATIONS)
        margin = np.random.normal(base_ebitda_margin / 100, 0.05, self.N_SIMULATIONS)
        margin = np.clip(margin, 0.15, 0.70)
        wacc = np.random.uniform(0.065, 0.095, self.N_SIMULATIONS)
        terminal_g = np.random.uniform(0.015, 0.035, self.N_SIMULATIONS)
        capex_pct = np.random.normal(base_capex_ratio, 0.02, self.N_SIMULATIONS)
        capex_pct = np.clip(capex_pct, 0.05, 0.30)

        fair_values = np.zeros(self.N_SIMULATIONS)
        for i in range(self.N_SIMULATIONS):
            fair_values[i] = self._simulate_dcf(
                base_revenue=base_revenue,
                rev_growth=rev_growth[i],
                ebitda_margin=margin[i],
                capex_pct=capex_pct[i],
                wacc=wacc[i],
                terminal_g=terminal_g[i],
                net_debt=net_debt,
                cash=cash,
            )

        fair_values = fair_values[(fair_values > 0) & (fair_values < 1000)]
        if len(fair_values) == 0:
            return ValuationResult(
                model_name="Monte Carlo",
                intrinsic_value=0,
                confidence=0,
                methodology="Monte Carlo — all simulations produced invalid results",
            )

        median_value = float(np.median(fair_values))
        p10 = float(np.percentile(fair_values, 10))
        p90 = float(np.percentile(fair_values, 90))
        mean_value = float(np.mean(fair_values))
        price = self._current_price()
        prob_above_price = float(np.mean(fair_values > price)) * 100
        upside = self._compute_upside(median_value)
        cv = float(np.std(fair_values) / np.mean(fair_values))
        confidence = max(30, min(75, 75 - cv * 100))

        return ValuationResult(
            model_name="Monte Carlo",
            intrinsic_value=round(median_value, 2),
            intrinsic_value_low=round(p10, 2),
            intrinsic_value_high=round(p90, 2),
            upside_pct=round(upside, 1),
            confidence=round(confidence, 0),
            methodology=f"Monte Carlo DCF ({self.N_SIMULATIONS:,} simulations)",
            details={
                "median": round(median_value, 2),
                "mean": round(mean_value, 2),
                "p10_bear": round(p10, 2),
                "p25": round(float(np.percentile(fair_values, 25)), 2),
                "p75": round(float(np.percentile(fair_values, 75)), 2),
                "p90_bull": round(p90, 2),
                "prob_above_current_price_pct": round(prob_above_price, 1),
                "valid_simulations": len(fair_values),
                "base_revenue_m": round(base_revenue, 0),
                "base_ebitda_margin_pct": round(base_ebitda_margin, 1),
            },
        )

    def _simulate_dcf(
        self,
        base_revenue: float,
        rev_growth: float,
        ebitda_margin: float,
        capex_pct: float,
        wacc: float,
        terminal_g: float,
        net_debt: float,
        cash: float,
    ) -> float:
        revenue = base_revenue
        fcfs: List[float] = []
        for _ in range(self.FORECAST_YEARS):
            revenue *= 1 + rev_growth
            ebitda = revenue * ebitda_margin
            capex = revenue * capex_pct
            fcf = ebitda * (1 - CORPORATE_TAX_RATE) - capex
            fcfs.append(fcf)

        terminal_fcf = fcfs[-1]
        if wacc <= terminal_g:
            tv = terminal_fcf * 20
        else:
            tv = terminal_fcf * (1 + terminal_g) / (wacc - terminal_g)

        pv_fcf = sum(f / (1 + wacc) ** (i + 1) for i, f in enumerate(fcfs))
        pv_tv = tv / (1 + wacc) ** self.FORECAST_YEARS

        ev = pv_fcf + pv_tv
        equity = ev - net_debt + cash
        return (equity * 1_000_000) / NUM_SHARES

    def _get_base_revenue(self) -> float:
        sales = self._get_hist_values("financials", "net_sales")
        if not sales:
            return 0
        for year in ["2025", "2024", "2023", "2022", "2021"]:
            if year in sales and sales[year]:
                return sales[year]
        # Last resort: take the most recent year available.
        latest = max(sales.keys())
        return sales.get(latest) or 0

    def _get_base_margin(self) -> float:
        # EBITDA margin first; fall back to EBIT margin for banks where EBITDA
        # isn't reported.
        for margin_field in ("ebitda_margin", "ebit_margin"):
            margins = self._get_hist_values("financials", margin_field)
            for year in ["2025", "2024", "2023"]:
                if year in margins and margins[year]:
                    return margins[year]
        return 45.0

    def _get_capex_ratio(self) -> float:
        capex = self._get_hist_values("financials", "capex")
        sales = self._get_hist_values("financials", "net_sales")
        ratios = []
        for year in ["2023", "2024", "2025"]:
            c_val = capex.get(year)
            s_val = sales.get(year)
            if c_val and s_val and s_val > 0:
                ratio = abs(c_val) / s_val
                if 0.01 < ratio < 0.50:
                    ratios.append(ratio)
        if ratios:
            return sum(ratios) / len(ratios)
        return 0.15

    def _get_net_debt(self) -> float:
        nd = self._get_financial("net_debt", "2025")
        if nd and nd > 100:
            return nd
        debt = self._get_financial("total_debt", "2025") or 0
        return debt - self._get_cash()

    def _get_cash(self) -> float:
        return self._get_financial("cash_and_equivalents", "2025") or 0


MODEL_REGISTRY: Dict[str, Type[BaseValuationModel]] = {
    "dcf": DCFModel,
    "ddm": DDMModel,
    "graham": GrahamModel,
    "relative": RelativeValuationModel,
    "monte_carlo": MonteCarloModel,
}

__all__ = [
    "ValuationResult",
    "DCFModel",
    "DDMModel",
    "GrahamModel",
    "RelativeValuationModel",
    "MonteCarloModel",
    "load_stock_data",
    "run_model",
    "MODEL_REGISTRY",
]


def run_model(model_key: str, stock_data: Dict[str, Any]) -> ValuationResult:
    model_cls = MODEL_REGISTRY[model_key]
    return model_cls(stock_data, {}).calculate()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run fundamental valuation models from data/.")
    parser.add_argument(
        "--model",
        choices=["graham", "dcf", "ddm", "relative", "monte_carlo", "all"],
        default="graham",
        help="Model to run (default: graham).",
    )
    parser.add_argument("--ticker", default="ATW", help="Ticker prefix for input files (default: ATW).")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Path to data directory (default: <project_root>\\data).",
    )
    args = parser.parse_args()

    output_dir = Path(args.data_dir) if args.data_dir else Path(__file__).resolve().parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "models_result.json"

    stock_data = load_stock_data(data_dir=args.data_dir, ticker=args.ticker)
    model_keys = ["dcf", "ddm", "graham", "relative", "monte_carlo"]
    all_results: Dict[str, Any] = {}
    for key in model_keys:
        all_results[key] = asdict(run_model(key, stock_data))

    if args.model == "all":
        for key in model_keys:
            payload = all_results[key]
            print(f"\n=== {payload['model_name']} ===")
            print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(all_results[args.model], indent=2, ensure_ascii=False))

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
