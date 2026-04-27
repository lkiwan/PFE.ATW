"""ATW-ANALYSE — holistic ATW verdict from already-collected data, with cited evidence.

Reads market, macro, news, fundamentals, and valuation files, assembles them into
one prompt block of stable bracketed IDs, and asks Groq for a structured BUY/HOLD/SELL
verdict where every claim must cite an ID from the block. Terminal output only.

  python agents/agent_analyse.py
  python agents/agent_analyse.py --raw
  python agents/agent_analyse.py --asof 2026-04-25 --lookback-days 7
  python agents/agent_analyse.py --evidence-only        # skip the LLM call

The file is organized in clearly-separated sections — config, schema, one loader per
data source, evidence assembly, LLM wiring, formatter, and CLI. Each section can be
modified independently.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from agno.agent import Agent
from agno.models.groq import Groq


# =============================================================================
# CONFIG — paths, defaults, env loading
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

MARKET_CSV = DATA_DIR / "ATW_bourse_casa_full.csv"
MACRO_CSV = DATA_DIR / "ATW_macro_morocco.csv"
NEWS_CSV = DATA_DIR / "ATW_news.csv"
FUND_JSON = DATA_DIR / "ATW_fondamental.json"
VAL_JSON = DATA_DIR / "models_result.json"

DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_MIN_NEWS_SCORE = 50
DEFAULT_NEWS_CAP = 5
DEFAULT_NEWS_SUMMARY_CHARS = 90
DEFAULT_GROQ_MODEL = "openai/gpt-oss-120b"

# Per project_imf_datamapper_morocco_broken: defensive belt-and-suspenders skip.
MACRO_CPI_MAX = 50.0
MACRO_DEBT_GDP_MAX = 200.0

# --- Predictions (see agent_analyse.md §6 for sources) ----------------------
ATR_WINDOW = 14
TRADING_HORIZON_WEEKS = 4
TRADING_HORIZON_DAYS = TRADING_HORIZON_WEEKS * 5
ATR_STOP_K = 2.0
ATR_TARGET_M = 1.5
INVESTMENT_HORIZON_MONTHS = 12
RECO_BUY_THRESHOLD = 15.0
RECO_SELL_THRESHOLD = -10.0


class MissingEnvError(RuntimeError):
    pass


def load_env() -> dict[str, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    key = os.getenv("GROQ_API_KEY")
    if not key:
        raise MissingEnvError("GROQ_API_KEY not set in .env")
    return {"groq_key": key, "groq_model": os.getenv("GROQ_MODEL", DEFAULT_GROQ_MODEL)}


# =============================================================================
# SCHEMA — Pydantic models for the LLM output
# =============================================================================

SourceFile = Literal["market", "macro", "news", "fundamentals", "valuations"]
Dimension = Literal["MARKET", "MACRO", "NEWS", "FUNDAMENTAL", "VALUATION"]
Polarity = Literal["BULLISH", "BEARISH", "NEUTRAL"]
Verdict = Literal["BUY", "HOLD", "SELL"]
Conviction = Literal["LOW", "MEDIUM", "HIGH"]
Recommendation = Literal["ACHAT", "CONSERVER", "VENDRE"]


class Evidence(BaseModel):
    claim: str = Field(description="Short factual statement drawn from the evidence block.")
    source_ref: str = Field(description="One of the bracketed IDs emitted in the evidence block.")
    source_file: SourceFile


class Finding(BaseModel):
    dimension: Dimension
    statement: str = Field(description="One-sentence finding for this dimension.")
    polarity: Polarity
    evidence: list[Evidence] = Field(min_length=1, description="At least one citation is required.")


class TradingPrediction(BaseModel):
    horizon_weeks: int
    entry_zone_low_mad: float
    entry_zone_high_mad: float
    target_price_mad: float
    stop_loss_mad: float
    expected_return_pct: float
    risk_reward_ratio: float
    atr_mad: float
    confidence: Conviction
    thesis: str = Field(description="2-3 sentences citing [MKT-*], [NEWS-*], or [PRED-TRADE-*] IDs (French).")


class InvestmentPrediction(BaseModel):
    horizon_months: int
    target_price_mad: float
    target_price_low_mad: float
    target_price_high_mad: float
    upside_pct: float
    dividend_yield_pct: float
    expected_total_return_pct: float
    recommendation: Recommendation
    confidence: Conviction
    thesis: str = Field(description="3-5 sentences citing [VAL-*], [FUND-*], [MACRO-*], or [PRED-INV-*] IDs (French).")


class ATWAnalysis(BaseModel):
    as_of_date: str
    last_close_mad: float
    fair_value_low_mad: float
    fair_value_high_mad: float
    upside_pct: float
    findings: list[Finding]
    risks: list[str]
    verdict: Verdict
    conviction: Conviction
    verdict_reasoning: str
    trading_prediction: TradingPrediction
    investment_prediction: InvestmentPrediction


# =============================================================================
# SOURCE LOADERS — one dataclass + one function per data file
# =============================================================================

# --- market ------------------------------------------------------------------

@dataclass
class MarketSnapshot:
    as_of: str
    last_close: float
    market_cap: float
    return_1m_pct: float | None
    return_3m_pct: float | None
    return_6m_pct: float | None
    return_52w_pct: float | None
    high_52w: float
    low_52w: float
    atr_14d: float
    high_4w: float
    low_4w: float


def _pct_change(series: pd.Series, window: int) -> float | None:
    if len(series) <= window:
        return None
    last, past = series.iloc[-1], series.iloc[-1 - window]
    if past == 0 or pd.isna(past):
        return None
    return float((last - past) / past * 100)


def _wilder_atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> float:
    """Wilder True Range averaged over `window` periods. Standard ATR formula."""
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return float(tr.tail(window).mean())


def load_market_snapshot(path: Path) -> MarketSnapshot:
    df = pd.read_csv(path, parse_dates=["Séance"]).sort_values("Séance").reset_index(drop=True)
    close = df["Dernier Cours"]
    high = df["+haut du jour"]
    low = df["+bas du jour"]
    last_252 = close.tail(252)
    last_20 = close.tail(20)
    return MarketSnapshot(
        as_of=df["Séance"].iloc[-1].date().isoformat(),
        last_close=float(close.iloc[-1]),
        market_cap=float(df["Capitalisation"].iloc[-1]),
        return_1m_pct=_pct_change(close, 21),
        return_3m_pct=_pct_change(close, 63),
        return_6m_pct=_pct_change(close, 126),
        return_52w_pct=_pct_change(close, 252),
        high_52w=float(last_252.max()),
        low_52w=float(last_252.min()),
        atr_14d=_wilder_atr(high, low, close, ATR_WINDOW),
        high_4w=float(last_20.max()),
        low_4w=float(last_20.min()),
    )


# --- macro -------------------------------------------------------------------

@dataclass
class MacroSnapshot:
    as_of: str
    gdp_growth_pct: float | None
    inflation_cpi_pct: float | None
    public_debt_pct_gdp: float | None
    current_account_pct_gdp: float | None
    eur_mad: float | None
    usd_mad: float | None
    brent_usd: float | None
    masi_close: float | None
    vix: float | None
    macro_momentum: float | None
    global_risk_flag: float | None
    eur_mad_90d_delta_pct: float | None
    brent_90d_delta_pct: float | None
    masi_90d_delta_pct: float | None


def _scrub_macro_band(df: pd.DataFrame) -> pd.DataFrame:
    cpi, debt = df["inflation_cpi_pct"], df["public_debt_pct_gdp"]
    bad = ((cpi > MACRO_CPI_MAX) & cpi.notna()) | ((debt > MACRO_DEBT_GDP_MAX) & debt.notna())
    if bad.any():
        df = df.copy()
        df.loc[bad, ["inflation_cpi_pct", "public_debt_pct_gdp"]] = pd.NA
    return df


def _macro_delta_pct(series: pd.Series, window: int) -> float | None:
    s = series.dropna()
    if len(s) <= window:
        return None
    last, past = s.iloc[-1], s.iloc[-1 - window]
    if past == 0 or pd.isna(past):
        return None
    return float((last - past) / past * 100)


def _last_valid(series: pd.Series) -> float | None:
    s = series.dropna()
    return None if s.empty else float(s.iloc[-1])


def load_macro_snapshot(path: Path) -> MacroSnapshot:
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    df = _scrub_macro_band(df)
    return MacroSnapshot(
        as_of=df["date"].iloc[-1].date().isoformat(),
        gdp_growth_pct=_last_valid(df["gdp_growth_pct"]),
        inflation_cpi_pct=_last_valid(df["inflation_cpi_pct"]),
        public_debt_pct_gdp=_last_valid(df["public_debt_pct_gdp"]),
        current_account_pct_gdp=_last_valid(df["current_account_pct_gdp"]),
        eur_mad=_last_valid(df["eur_mad"]),
        usd_mad=_last_valid(df["usd_mad"]),
        brent_usd=_last_valid(df["brent_usd"]),
        masi_close=_last_valid(df["masi_close"]),
        vix=_last_valid(df["vix"]),
        macro_momentum=_last_valid(df["macro_momentum"]),
        global_risk_flag=_last_valid(df["global_risk_flag"]),
        eur_mad_90d_delta_pct=_macro_delta_pct(df["eur_mad"], 90),
        brent_90d_delta_pct=_macro_delta_pct(df["brent_usd"], 90),
        masi_90d_delta_pct=_macro_delta_pct(df["masi_close"], 90),
    )


# --- news --------------------------------------------------------------------

@dataclass
class NewsRow:
    date: str
    title: str
    source: str
    url: str
    signal_score: int
    is_atw_core: bool
    summary: str


def _truncate(text: str, limit: int) -> str:
    text = (text or "").replace("\n", " ").strip()
    return text if len(text) <= limit else text[:limit] + "…"


def load_news_window(
    path: Path,
    lookback_days: int,
    min_score: int,
    cap: int = DEFAULT_NEWS_CAP,
    summary_chars: int = DEFAULT_NEWS_SUMMARY_CHARS,
    asof: datetime | None = None,
) -> list[NewsRow]:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"])

    anchor = asof or datetime.now(tz=timezone.utc)
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=timezone.utc)
    cutoff = anchor - timedelta(days=lookback_days)

    df = df[(df["date"] >= cutoff) & (df["signal_score"] >= min_score)]
    df = df.sort_values("signal_score", ascending=False).head(cap)

    return [
        NewsRow(
            date=r["date"].date().isoformat(),
            title=str(r.get("title", "")),
            source=str(r.get("source", "")),
            url=str(r.get("url", "")),
            signal_score=int(r.get("signal_score", 0)),
            is_atw_core=bool(r.get("is_atw_core", False)),
            summary=_truncate(str(r.get("full_content", "")), summary_chars),
        )
        for _, r in df.iterrows()
    ]


# --- fundamentals ------------------------------------------------------------

YearMap = dict[str, float]


@dataclass
class Fundamentals:
    as_of: str
    price_to_book: float | None
    revenue: YearMap = field(default_factory=dict)
    net_income: YearMap = field(default_factory=dict)
    eps: YearMap = field(default_factory=dict)
    equity: YearMap = field(default_factory=dict)
    cash: YearMap = field(default_factory=dict)
    fcf: YearMap = field(default_factory=dict)
    net_margin: YearMap = field(default_factory=dict)
    roe: YearMap = field(default_factory=dict)
    pe_ratio: YearMap = field(default_factory=dict)
    dividend_per_share: YearMap = field(default_factory=dict)


def _last_n_years(d: dict | None, n: int = 2) -> YearMap:
    if not d:
        return {}
    keys = sorted(d.keys())[-n:]
    return {k: float(d[k]) for k in keys if d[k] is not None}


def load_fundamentals(path: Path) -> Fundamentals:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return Fundamentals(
        as_of=str(raw.get("scrape_timestamp", "")),
        price_to_book=raw.get("price_to_book"),
        revenue=_last_n_years(raw.get("hist_revenue")),
        net_income=_last_n_years(raw.get("hist_net_income")),
        eps=_last_n_years(raw.get("hist_eps")),
        equity=_last_n_years(raw.get("hist_equity")),
        cash=_last_n_years(raw.get("hist_cash")),
        fcf=_last_n_years(raw.get("hist_fcf")),
        net_margin=_last_n_years(raw.get("hist_net_margin")),
        roe=_last_n_years(raw.get("hist_roe")),
        pe_ratio=_last_n_years(raw.get("pe_ratio_hist")),
        dividend_per_share=_last_n_years(raw.get("hist_dividend_per_share")),
    )


# --- valuations --------------------------------------------------------------

@dataclass
class ModelEstimate:
    name: str
    intrinsic_value: float
    intrinsic_low: float
    intrinsic_high: float
    upside_pct: float
    confidence: float
    methodology: str


@dataclass
class Valuations:
    estimates: dict[str, ModelEstimate]

    def fair_value_range(self) -> tuple[float, float]:
        if not self.estimates:
            return (0.0, 0.0)
        lows = [e.intrinsic_low for e in self.estimates.values()]
        highs = [e.intrinsic_high for e in self.estimates.values()]
        return (min(lows), max(highs))


_VAL_KEY_TO_TAG = {
    "dcf": "DCF",
    "ddm": "DDM",
    "graham": "GRAHAM",
    "relative": "RELATIVE",
    "monte_carlo": "MC",
}


def load_valuations(path: Path) -> Valuations:
    raw = json.loads(path.read_text(encoding="utf-8"))
    estimates: dict[str, ModelEstimate] = {}
    for key, tag in _VAL_KEY_TO_TAG.items():
        block = raw.get(key)
        if not block:
            continue
        estimates[tag] = ModelEstimate(
            name=block.get("model_name", tag),
            intrinsic_value=float(block.get("intrinsic_value", 0.0)),
            intrinsic_low=float(block.get("intrinsic_value_low", 0.0)),
            intrinsic_high=float(block.get("intrinsic_value_high", 0.0)),
            upside_pct=float(block.get("upside_pct", 0.0)),
            confidence=float(block.get("confidence", 0.0)),
            methodology=str(block.get("methodology", "")),
        )
    return Valuations(estimates=estimates)


# =============================================================================
# PREDICTIONS — deterministic Python (no LLM); see agent_analyse.md §6
# =============================================================================

# --- trading (4-week, ATR-driven, MASI/Boursenews convention) ---------------

def _trading_confidence(atr: float, last_close: float, news_count_high_score: int) -> Conviction:
    """Low ATR ratio + multiple high-score news items → higher confidence."""
    if last_close <= 0:
        return "LOW"
    atr_ratio = atr / last_close
    if atr_ratio < 0.015 and news_count_high_score >= 2:
        return "HIGH"
    if atr_ratio < 0.025:
        return "MEDIUM"
    return "LOW"


def compute_trading_prediction(market: MarketSnapshot, news: list[NewsRow]) -> TradingPrediction:
    """ATR-based 4-week target, stop-loss, and entry zone.

    Target scales daily ATR by sqrt(horizon_days) — standard volatility-time
    scaling. Capped by 52-week high to respect realistic resistance.
    """
    last = market.last_close
    atr = market.atr_14d

    # Entry zone — recent pullback band: between 4-week low and current close.
    entry_low = max(last - atr, market.low_4w)
    entry_high = last

    # Stop-loss — k × ATR below the lower entry (Wilder/LuxAlgo convention).
    stop = max(entry_low - ATR_STOP_K * atr, 0.0)

    # Target — m × ATR × sqrt(horizon_days), capped by 52w high.
    horizon_scale = (TRADING_HORIZON_DAYS) ** 0.5
    raw_target = last + ATR_TARGET_M * atr * horizon_scale
    target = min(raw_target, market.high_52w)

    expected_return_pct = (target - last) / last * 100 if last > 0 else 0.0
    risk = max(last - stop, 1e-9)
    reward = max(target - last, 0.0)
    rr = reward / risk

    high_score_news = sum(1 for n in news if n.signal_score >= 75)
    confidence = _trading_confidence(atr, last, high_score_news)

    return TradingPrediction(
        horizon_weeks=TRADING_HORIZON_WEEKS,
        entry_zone_low_mad=round(entry_low, 2),
        entry_zone_high_mad=round(entry_high, 2),
        target_price_mad=round(target, 2),
        stop_loss_mad=round(stop, 2),
        expected_return_pct=round(expected_return_pct, 2),
        risk_reward_ratio=round(rr, 2),
        atr_mad=round(atr, 2),
        confidence=confidence,
        thesis="",  # filled by LLM after evidence injection
    )


# --- investment (12-month, BKGR convention, valuation-anchored) -------------

def _confidence_weighted_midpoint(val: Valuations) -> tuple[float, float, float]:
    """Confidence-weighted target with bear/bull bounds across [VAL-*] models."""
    if not val.estimates:
        return (0.0, 0.0, 0.0)
    total_conf = sum(e.confidence for e in val.estimates.values())
    if total_conf == 0:
        mids = [e.intrinsic_value for e in val.estimates.values()]
        target = sum(mids) / len(mids)
    else:
        target = sum(e.intrinsic_value * e.confidence for e in val.estimates.values()) / total_conf
    lows = [e.intrinsic_low for e in val.estimates.values() if e.intrinsic_low > 0]
    highs = [e.intrinsic_high for e in val.estimates.values() if e.intrinsic_high > 0]
    return (target, min(lows) if lows else target, max(highs) if highs else target)


def _classify_recommendation(upside_pct: float) -> Recommendation:
    if upside_pct > RECO_BUY_THRESHOLD:
        return "ACHAT"
    if upside_pct < RECO_SELL_THRESHOLD:
        return "VENDRE"
    return "CONSERVER"


def _investment_confidence(val: Valuations, target: float) -> Conviction:
    """Tight model spread + high model confidence → higher conviction."""
    if not val.estimates or target <= 0:
        return "LOW"
    mids = [e.intrinsic_value for e in val.estimates.values()]
    spread_ratio = (max(mids) - min(mids)) / target
    avg_conf = sum(e.confidence for e in val.estimates.values()) / len(val.estimates)
    if spread_ratio < 0.30 and avg_conf >= 70:
        return "HIGH"
    if spread_ratio < 0.60 and avg_conf >= 50:
        return "MEDIUM"
    return "LOW"


def _last_dps(fund: Fundamentals) -> float | None:
    if not fund.dividend_per_share:
        return None
    last_year = max(fund.dividend_per_share.keys())
    return fund.dividend_per_share[last_year]


def compute_investment_prediction(
    market: MarketSnapshot,
    val: Valuations,
    fund: Fundamentals,
) -> InvestmentPrediction:
    """12-month BKGR-style: confidence-weighted target across [VAL-*] models,
    TSR = upside + (1+upside) × dividend_yield (Morgan Stanley reinvested form).
    """
    target, low, high = _confidence_weighted_midpoint(val)
    last = market.last_close
    upside_pct = (target - last) / last * 100 if last > 0 else 0.0

    dps = _last_dps(fund) or 0.0
    div_yield = dps / last if last > 0 else 0.0  # decimal
    cgy = upside_pct / 100.0
    tsr = cgy + (1 + cgy) * div_yield  # Morgan Stanley TSR

    return InvestmentPrediction(
        horizon_months=INVESTMENT_HORIZON_MONTHS,
        target_price_mad=round(target, 2),
        target_price_low_mad=round(low, 2),
        target_price_high_mad=round(high, 2),
        upside_pct=round(upside_pct, 2),
        dividend_yield_pct=round(div_yield * 100, 2),
        expected_total_return_pct=round(tsr * 100, 2),
        recommendation=_classify_recommendation(upside_pct),
        confidence=_investment_confidence(val, target),
        thesis="",  # filled by LLM after evidence injection
    )


# =============================================================================
# STRATEGY FILTERS — context-aware vetoes informed by backtest loss analysis
# =============================================================================
#
# Tunable thresholds. Calibrated from backtest breakdown buckets where
# loss_rate >> baseline. See agents/analyze_losses.py.
FILTER_BEAR_3M_THRESHOLD = -5.0
FILTER_TOP_1M_THRESHOLD = 8.0
FILTER_TOP_NEAR_HIGH_PCT = 3.0
FILTER_COOLDOWN_RECENT_STOPS = 2
FILTER_COOLDOWN_BARS = 5


def filter_bear_regime(snap: MarketSnapshot) -> str | None:
    r3m = snap.return_3m_pct
    if r3m is not None and r3m < FILTER_BEAR_3M_THRESHOLD:
        return f"BEAR_REGIME (return_3m_pct={r3m:.2f}%)"
    return None


def filter_top_buying(snap: MarketSnapshot) -> str | None:
    r1m = snap.return_1m_pct
    if r1m is None or snap.high_52w <= 0 or snap.last_close <= 0:
        return None
    pct_below_high = (snap.high_52w - snap.last_close) / snap.high_52w * 100
    if r1m > FILTER_TOP_1M_THRESHOLD and pct_below_high < FILTER_TOP_NEAR_HIGH_PCT:
        return f"TOP_BUYING (return_1m_pct={r1m:.2f}%, %below52w={pct_below_high:.2f}%)"
    return None


def filter_cooldown(recent_outcomes: list[str] | None) -> str | None:
    """Pause when the last N resolved signals were all stops."""
    if not recent_outcomes:
        return None
    last_n = recent_outcomes[-FILTER_COOLDOWN_RECENT_STOPS:]
    if len(last_n) < FILTER_COOLDOWN_RECENT_STOPS:
        return None
    if all(o == "STOP" for o in last_n):
        return f"COOLDOWN (last {FILTER_COOLDOWN_RECENT_STOPS} signals stopped)"
    return None


def apply_strategy_filters(
    snap: MarketSnapshot,
    recent_outcomes: list[str] | None = None,
    enabled: tuple[str, ...] = ("bear", "top", "cooldown"),
) -> str | None:
    """First-hit-wins filter chain. Returns a skip reason or None."""
    if "bear" in enabled:
        r = filter_bear_regime(snap)
        if r:
            return r
    if "top" in enabled:
        r = filter_top_buying(snap)
        if r:
            return r
    if "cooldown" in enabled:
        r = filter_cooldown(recent_outcomes)
        if r:
            return r
    return None


# =============================================================================
# EVIDENCE BLOCK — assemble all loaders into one prompt with stable IDs
# =============================================================================

def _fmt(v: float | None, fmt: str = "{:.2f}") -> str:
    return fmt.format(v) if v is not None else "n/a"


def _market_block(m: MarketSnapshot) -> list[str]:
    return [
        "## MARKET (source_file=market)",
        f"  [MKT-AS-OF]   as_of_date = {m.as_of}",
        f"  [MKT-CLOSE]   last_close_mad = {_fmt(m.last_close)}",
        f"  [MKT-MCAP]    market_cap = {_fmt(m.market_cap, '{:.0f}')}",
        f"  [MKT-RET-1M]  return_1m_pct = {_fmt(m.return_1m_pct)}",
        f"  [MKT-RET-3M]  return_3m_pct = {_fmt(m.return_3m_pct)}",
        f"  [MKT-RET-6M]  return_6m_pct = {_fmt(m.return_6m_pct)}",
        f"  [MKT-RET-52W] return_52w_pct = {_fmt(m.return_52w_pct)}",
        f"  [MKT-52W-HI]  high_52w = {_fmt(m.high_52w)}",
        f"  [MKT-52W-LO]  low_52w = {_fmt(m.low_52w)}",
        f"  [MKT-4W-HI]   high_4w = {_fmt(m.high_4w)}",
        f"  [MKT-4W-LO]   low_4w = {_fmt(m.low_4w)}",
        f"  [MKT-ATR]     atr_14d_mad = {_fmt(m.atr_14d)}",
    ]


def _predictions_block(t: TradingPrediction, i: InvestmentPrediction) -> list[str]:
    return [
        "",
        "## PREDICTIONS (computed deterministically, source_file=market+valuations)",
        f"  [PRED-TRADE-HORIZON] trading_horizon_weeks = {t.horizon_weeks}",
        f"  [PRED-TRADE-ENTRY]   entry_zone = {t.entry_zone_low_mad:.2f}–{t.entry_zone_high_mad:.2f} MAD",
        f"  [PRED-TRADE-TARGET]  trading_target_mad = {t.target_price_mad:.2f}",
        f"  [PRED-TRADE-STOP]    stop_loss_mad = {t.stop_loss_mad:.2f} (k={ATR_STOP_K})",
        f"  [PRED-TRADE-RET]     trading_expected_return_pct = {t.expected_return_pct:.2f}",
        f"  [PRED-TRADE-RR]      risk_reward_ratio = {t.risk_reward_ratio:.2f}",
        f"  [PRED-TRADE-CONF]    trading_confidence = {t.confidence}",
        f"  [PRED-INV-HORIZON]   investment_horizon_months = {i.horizon_months}",
        f"  [PRED-INV-TARGET]    cours_cible_mad = {i.target_price_mad:.2f} (low={i.target_price_low_mad:.2f}, high={i.target_price_high_mad:.2f})",
        f"  [PRED-INV-UPSIDE]    upside_pct = {i.upside_pct:.2f}",
        f"  [PRED-INV-DY]        dividend_yield_pct = {i.dividend_yield_pct:.2f}",
        f"  [PRED-INV-TSR]       expected_total_return_pct = {i.expected_total_return_pct:.2f}",
        f"  [PRED-INV-RECO]      recommendation = {i.recommendation}",
        f"  [PRED-INV-CONF]      investment_confidence = {i.confidence}",
    ]


def _macro_block(mc: MacroSnapshot) -> list[str]:
    return [
        "",
        "## MACRO (source_file=macro)",
        f"  [MACRO-AS-OF]    as_of_date = {mc.as_of}",
        f"  [MACRO-GDP]      gdp_growth_pct = {_fmt(mc.gdp_growth_pct)}",
        f"  [MACRO-CPI]      inflation_cpi_pct = {_fmt(mc.inflation_cpi_pct)}",
        f"  [MACRO-DEBT]     public_debt_pct_gdp = {_fmt(mc.public_debt_pct_gdp)}",
        f"  [MACRO-CA]       current_account_pct_gdp = {_fmt(mc.current_account_pct_gdp)}",
        f"  [MACRO-EURMAD]   eur_mad = {_fmt(mc.eur_mad, '{:.4f}')} (90d Δ {_fmt(mc.eur_mad_90d_delta_pct)}%)",
        f"  [MACRO-USDMAD]   usd_mad = {_fmt(mc.usd_mad, '{:.4f}')}",
        f"  [MACRO-BRENT]    brent_usd = {_fmt(mc.brent_usd)} (90d Δ {_fmt(mc.brent_90d_delta_pct)}%)",
        f"  [MACRO-MASI]     masi_close = {_fmt(mc.masi_close)} (90d Δ {_fmt(mc.masi_90d_delta_pct)}%)",
        f"  [MACRO-VIX]      vix = {_fmt(mc.vix)}",
        f"  [MACRO-MOMENTUM] macro_momentum = {_fmt(mc.macro_momentum)}",
        f"  [MACRO-RISK]     global_risk_flag = {_fmt(mc.global_risk_flag)}",
    ]


def _news_block(news: list[NewsRow]) -> list[str]:
    if not news:
        return ["", "## NEWS (source_file=news)", "  (no news rows in window)"]
    lines = ["", "## NEWS (source_file=news)"]
    for i, n in enumerate(news, 1):
        core = "core" if n.is_atw_core else "ctx"
        lines.append(
            f"  [NEWS-{i}] {n.date} s={n.signal_score} {core}: {n.title} — {n.summary}"
        )
    return lines


def _fund_block(f: Fundamentals) -> list[str]:
    lines = ["", "## FUNDAMENTAL (source_file=fundamentals)"]
    lines.append(f"  [FUND-PB] price_to_book = {_fmt(f.price_to_book)}")
    series_map = (
        ("REV", f.revenue, "{:.0f}"),
        ("NI", f.net_income, "{:.0f}"),
        ("EPS", f.eps, "{:.2f}"),
        ("EQ", f.equity, "{:.0f}"),
        ("CASH", f.cash, "{:.0f}"),
        ("FCF", f.fcf, "{:.0f}"),
        ("MARGIN", f.net_margin, "{:.2f}"),
        ("ROE", f.roe, "{:.2f}"),
        ("PE", f.pe_ratio, "{:.2f}"),
        ("DPS", f.dividend_per_share, "{:.2f}"),
    )
    for tag, series, fmt in series_map:
        for year, value in series.items():
            lines.append(f"  [FUND-{tag}-{year}] {tag.lower()}_{year} = {fmt.format(value)}")
    return lines


def _val_block(v: Valuations) -> list[str]:
    lines = ["", "## VALUATION (source_file=valuations)"]
    if not v.estimates:
        lines.append("  (no valuation models present)")
        return lines
    for tag, est in v.estimates.items():
        lines.append(
            f"  [VAL-{tag}] fair={est.intrinsic_value:.0f} "
            f"(low={est.intrinsic_low:.0f}, high={est.intrinsic_high:.0f}, "
            f"upside={est.upside_pct:.1f}%, conf={est.confidence:.0f})"
        )
    low, high = v.fair_value_range()
    lines.append(f"  [VAL-RANGE] fair_value_low_mad={low:.2f} fair_value_high_mad={high:.2f}")
    return lines


def compose_evidence_block(
    market: MarketSnapshot,
    macro: MacroSnapshot,
    news: list[NewsRow],
    fund: Fundamentals,
    val: Valuations,
    trading: TradingPrediction,
    investment: InvestmentPrediction,
    today: str,
) -> str:
    head = [
        f"# ATW EVIDENCE BLOCK — today={today}",
        "Cite ONLY the bracketed IDs below. Never invent IDs, numbers, or URLs.",
        "Every Finding.evidence[].source_ref MUST be one of the IDs in this block.",
        "Numbers in the [PRED-*] block are pre-computed — copy them verbatim into",
        "the trading_prediction / investment_prediction fields, do NOT recompute.",
        "",
    ]
    body: list[str] = []
    body += _market_block(market)
    body += _macro_block(macro)
    body += _news_block(news)
    body += _fund_block(fund)
    body += _val_block(val)
    body += _predictions_block(trading, investment)
    return "\n".join(head + body)


# =============================================================================
# LLM — instructions, agent builder, synthesis call
# =============================================================================

INSTRUCTIONS: list[str] = [
    "You are ATW-ANALYSE — synthesize a holistic verdict on Attijariwafa Bank (ticker ATW, Casablanca Bourse) from the EVIDENCE BLOCK provided.",
    "Use ONLY values, dates, sources, and headlines that appear in the EVIDENCE BLOCK. Never invent numbers, URLs, or IDs.",
    "Every Finding MUST include at least one Evidence entry whose source_ref EXACTLY matches a bracketed ID from the EVIDENCE BLOCK (e.g. [MKT-RET-3M], [FUND-EPS-2024], [NEWS-2]).",
    "Produce findings across all 5 dimensions when data is present: MARKET, MACRO, NEWS, FUNDAMENTAL, VALUATION. If a dimension has no usable data, omit it (don't fabricate).",
    "Set fair_value_low_mad and fair_value_high_mad from the [VAL-RANGE] entry if present, otherwise the min/max across [VAL-*] low/high values.",
    "Compute upside_pct = (((fair_value_low_mad + fair_value_high_mad) / 2) - last_close_mad) / last_close_mad * 100. Round to 1 decimal.",
    "Verdict logic — apply strictly:",
    "  - BUY  if upside_pct > +15 AND net polarity across MACRO+NEWS findings is not bearish.",
    "  - SELL if upside_pct < -10 OR fundamentals show a clear deterioration (declining EPS, ROE, or net margin).",
    "  - HOLD otherwise.",
    "Conviction logic:",
    "  - HIGH   if all 5 dimensions have ≥1 finding AND polarities are aligned.",
    "  - MEDIUM if mixed polarities or 3-4 dimensions covered.",
    "  - LOW    if any source is missing or findings contradict each other.",
    "Risks: 3-5 concrete bullets drawn from NEWS or MACRO findings — no generic boilerplate (avoid 'market volatility', 'macro uncertainty' alone).",
    "verdict_reasoning: 3-5 sentences tying specific findings to the verdict; cite IDs inline like ([MKT-RET-3M], [VAL-DCF]).",
    "Each Finding.statement is ONE sentence. Polarity must reflect the ATW investor's perspective (BULLISH = supportive of share price).",
    "Predictions: copy [PRED-*] numbers VERBATIM into trading_prediction/investment_prediction fields (framework overwrites them anyway).",
    "trading_prediction.thesis: 2 sentences FRENCH, cite [MKT-*]/[NEWS-*]/[PRED-TRADE-*]. Use 'zone d'entrée', 'objectif', 'stop'.",
    "investment_prediction.thesis: 3 sentences FRENCH, cite [VAL-*]/[FUND-*]/[PRED-INV-*]. Use 'cours cible', 'potentiel', 'ACHAT/CONSERVER/VENDRE'.",
]


class SynthesisError(RuntimeError):
    pass


def build_synth_agent(groq_model: str) -> Agent:
    return Agent(
        model=Groq(id=groq_model, max_tokens=4096, temperature=0.2),
        output_schema=ATWAnalysis,
        instructions=INSTRUCTIONS,
    )


def synthesize(agent: Agent, evidence_block: str, today: str) -> ATWAnalysis:
    prompt = f"Today: {today}\n\n{evidence_block}"
    resp = agent.run(prompt)
    content = resp.content
    if not isinstance(content, ATWAnalysis):
        raise SynthesisError(f"LLM did not return an ATWAnalysis. Got: {type(content).__name__}\n{content}")
    return content


# =============================================================================
# FORMATTER — terminal pretty-print
# =============================================================================

_POLARITY_MARK = {"BULLISH": "📈", "BEARISH": "📉", "NEUTRAL": "➖"}
_DIM_ORDER = ("MARKET", "MACRO", "NEWS", "FUNDAMENTAL", "VALUATION")
_VERDICT_MARK = {"BUY": "🟢", "HOLD": "🟡", "SELL": "🔴"}
_RECO_MARK = {"ACHAT": "🟢", "CONSERVER": "🟡", "VENDRE": "🔴"}


def _format_citations(f: Finding) -> str:
    return " ".join(e.source_ref for e in f.evidence)


def print_analysis(a: ATWAnalysis) -> None:
    midpoint = (a.fair_value_low_mad + a.fair_value_high_mad) / 2
    print(f"\n═══ ATW ANALYSIS — {a.as_of_date} ═══\n")
    print(f"  Last close      : {a.last_close_mad:.2f} MAD")
    print(f"  Fair value range: {a.fair_value_low_mad:.2f} – {a.fair_value_high_mad:.2f} MAD (mid {midpoint:.2f})")
    print(f"  Implied upside  : {a.upside_pct:+.1f}%\n")

    by_dim: dict[str, list[Finding]] = {d: [] for d in _DIM_ORDER}
    for f in a.findings:
        by_dim.setdefault(f.dimension, []).append(f)

    for dim in _DIM_ORDER:
        items = by_dim.get(dim, [])
        if not items:
            continue
        print(f"── {dim} ──")
        for f in items:
            mark = _POLARITY_MARK.get(f.polarity, "·")
            print(f"  {mark} {f.statement}  {_format_citations(f)}")
        print()

    if a.risks:
        print("⚠️  RISKS")
        for r in a.risks:
            print(f"  • {r}")
        print()

    t = a.trading_prediction
    print(f"📊 TRADING ({t.horizon_weeks} semaines)")
    print(f"   Zone d'entrée   : {t.entry_zone_low_mad:.2f} – {t.entry_zone_high_mad:.2f} MAD")
    print(f"   Objectif        : {t.target_price_mad:.2f} MAD  (rendement attendu {t.expected_return_pct:+.2f}%)")
    print(f"   Stop de prot.   : {t.stop_loss_mad:.2f} MAD  (ATR×{ATR_STOP_K} = {t.atr_mad:.2f} MAD)")
    print(f"   Risk/reward     : {t.risk_reward_ratio:.2f}    Confiance : {t.confidence}")
    print(f"   Thèse : {t.thesis}\n")

    i = a.investment_prediction
    rmark = _RECO_MARK.get(i.recommendation, "·")
    print(f"💼 INVESTISSEMENT ({i.horizon_months} mois — méthodologie BKGR)")
    print(f"   Cours cible     : {i.target_price_mad:.2f} MAD  (range {i.target_price_low_mad:.2f}–{i.target_price_high_mad:.2f})")
    print(f"   Potentiel cours : {i.upside_pct:+.2f}%")
    print(f"   Rendement div.  : {i.dividend_yield_pct:.2f}%")
    print(f"   TSR attendu     : {i.expected_total_return_pct:+.2f}%  (formule Morgan Stanley)")
    print(f"   {rmark} Recommandation : {i.recommendation}    Confiance : {i.confidence}")
    print(f"   Thèse : {i.thesis}\n")

    mark = _VERDICT_MARK.get(a.verdict, "·")
    print(f"{mark} VERDICT: {a.verdict}  (conviction: {a.conviction})")
    print(f"   {a.verdict_reasoning}\n")


# =============================================================================
# HISTORY — append-only prediction log for performance tracking
# =============================================================================

HISTORY_CSV = DATA_DIR / "prediction_history.csv"


def save_prediction_history(analysis: ATWAnalysis, path: Path = HISTORY_CSV) -> None:
    """Append the latest analysis to a CSV history for performance tracking."""
    t = analysis.trading_prediction
    i = analysis.investment_prediction
    new_row = {
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "as_of_date": analysis.as_of_date,
        "last_close_mad": analysis.last_close_mad,
        "fair_value_low_mad": analysis.fair_value_low_mad,
        "fair_value_high_mad": analysis.fair_value_high_mad,
        "upside_pct": analysis.upside_pct,
        "verdict": analysis.verdict,
        "conviction": analysis.conviction,
        "trading_horizon_weeks": t.horizon_weeks,
        "trading_entry_low_mad": t.entry_zone_low_mad,
        "trading_entry_high_mad": t.entry_zone_high_mad,
        "trading_target_mad": t.target_price_mad,
        "trading_stop_loss_mad": t.stop_loss_mad,
        "trading_expected_return_pct": t.expected_return_pct,
        "trading_risk_reward_ratio": t.risk_reward_ratio,
        "trading_atr_mad": t.atr_mad,
        "trading_confidence": t.confidence,
        "investment_horizon_months": i.horizon_months,
        "investment_target_mad": i.target_price_mad,
        "investment_target_low_mad": i.target_price_low_mad,
        "investment_target_high_mad": i.target_price_high_mad,
        "investment_upside_pct": i.upside_pct,
        "investment_dividend_yield_pct": i.dividend_yield_pct,
        "investment_total_return_pct": i.expected_total_return_pct,
        "investment_recommendation": i.recommendation,
        "investment_confidence": i.confidence,
    }
    df_new = pd.DataFrame([new_row])
    write_header = not path.exists()
    df_new.to_csv(path, mode="a", header=write_header, index=False, encoding="utf-8")


# =============================================================================
# CLI / MAIN — thin orchestrator, no business logic
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ATW holistic analyse agent — cited multi-source verdict")
    p.add_argument("--asof", type=str, default=None, help="Override 'today' as YYYY-MM-DD.")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                   help=f"News lookback window in days (default {DEFAULT_LOOKBACK_DAYS}).")
    p.add_argument("--min-news-score", type=int, default=DEFAULT_MIN_NEWS_SCORE,
                   help=f"Minimum signal_score for news inclusion (default {DEFAULT_MIN_NEWS_SCORE}).")
    p.add_argument("--raw", action="store_true",
                   help="Print raw ATWAnalysis JSON instead of the formatted report.")
    p.add_argument("--evidence-only", action="store_true",
                   help="Print the assembled evidence block and exit (no LLM call).")
    p.add_argument("--no-history", action="store_true",
                   help="Skip appending the result to prediction_history.csv.")
    return p.parse_args()


def _today_iso(asof: str | None) -> str:
    return asof or datetime.now(tz=timezone.utc).date().isoformat()


def main() -> int:
    args = _parse_args()

    asof_dt = None
    if args.asof:
        asof_dt = datetime.fromisoformat(args.asof).replace(tzinfo=timezone.utc)

    print(f"[load] reading data from {DATA_DIR}", flush=True)
    market_snap = load_market_snapshot(MARKET_CSV)
    macro_snap = load_macro_snapshot(MACRO_CSV)
    news_rows = load_news_window(
        NEWS_CSV,
        lookback_days=args.lookback_days,
        min_score=args.min_news_score,
        cap=DEFAULT_NEWS_CAP,
        summary_chars=DEFAULT_NEWS_SUMMARY_CHARS,
        asof=asof_dt,
    )
    fund_snap = load_fundamentals(FUND_JSON)
    val_snap = load_valuations(VAL_JSON)

    trading_pred = compute_trading_prediction(market_snap, news_rows)
    investment_pred = compute_investment_prediction(market_snap, val_snap, fund_snap)

    today = _today_iso(args.asof)
    block = compose_evidence_block(
        market_snap, macro_snap, news_rows, fund_snap, val_snap,
        trading_pred, investment_pred, today,
    )

    if args.evidence_only:
        print(block)
        return 0

    try:
        env = load_env()
    except MissingEnvError as e:
        print(f"ERROR: {e}", flush=True)
        return 2

    print(f"[llm] sending evidence to Groq model {env['groq_model']!r}…", flush=True)
    agent = build_synth_agent(env["groq_model"])
    try:
        analysis = synthesize(agent, block, today=today)
    except SynthesisError as e:
        print(f"ERROR: {e}", flush=True)
        return 3

    # Trust Python over the LLM for numeric predictions — keep only its thesis.
    analysis.trading_prediction = trading_pred.model_copy(
        update={"thesis": analysis.trading_prediction.thesis}
    )
    analysis.investment_prediction = investment_pred.model_copy(
        update={"thesis": analysis.investment_prediction.thesis}
    )

    if not args.no_history:
        save_prediction_history(analysis)
        print(f"[history] appended to {HISTORY_CSV}", flush=True)

    if args.raw:
        print(analysis.model_dump_json(indent=2))
    else:
        print_analysis(analysis)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
