"""ATW-ANALYSE ENHANCED — Advanced holistic analysis using ALL database tables.

Major improvements over original:
1. Reads from PostgreSQL database (all tables)
2. Orderbook microstructure features
3. Technical indicators from technicals_snapshot
4. Data quality validation
5. Citation validation
6. Regime detection
7. Probabilistic forecasting with confidence intervals
8. Enhanced evidence block with all available data

Usage:
  python agents/agent_analyse_enhanced.py
  python agents/agent_analyse_enhanced.py --raw
  python agents/agent_analyse_enhanced.py --asof 2026-04-25 --lookback-days 7
  python agents/agent_analyse_enhanced.py --evidence-only
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import quote_plus

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from scipy import stats
from sqlalchemy import create_engine, text

from agno.agent import Agent
from agno.models.groq import Groq

logging.getLogger("agno").setLevel(logging.CRITICAL)
logging.getLogger("agno.models.groq").setLevel(logging.CRITICAL)


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_MIN_NEWS_SCORE = 50
DEFAULT_NEWS_CAP = 10
DEFAULT_NEWS_SUMMARY_CHARS = 120
DEFAULT_GROQ_MODEL = "openai/gpt-oss-120b"

# Prediction parameters
ATR_WINDOW = 14
TRADING_HORIZON_WEEKS = 4
TRADING_HORIZON_DAYS = TRADING_HORIZON_WEEKS * 5
ATR_STOP_K = 2.0
ATR_TARGET_M = 1.5
INVESTMENT_HORIZON_MONTHS = 12
RECO_BUY_THRESHOLD = 15.0
RECO_SELL_THRESHOLD = -10.0
TYPICAL_DAILY_VOL_PCT = 2.5  # baseline used for vol_zscore and threshold scaling

# Data quality thresholds
MACRO_CPI_MAX = 50.0
MACRO_DEBT_GDP_MAX = 200.0
PRICE_JUMP_THRESHOLD = 0.20  # 20% daily change is suspicious


class MissingEnvError(RuntimeError):
    pass


class SynthesisError(RuntimeError):
    pass


class DataQualityError(RuntimeError):
    pass


def load_env() -> dict[str, str]:
    load_dotenv(PROJECT_ROOT / ".env")
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        raise MissingEnvError("GROQ_API_KEY not set in .env")
    
    # Database connection
    db_host = os.getenv("DB_HOST") or os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("DB_PORT") or os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("DB_NAME") or os.getenv("POSTGRES_DB", "atw")
    db_user = os.getenv("DB_USER") or os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    if db_password is None:
        db_password = os.getenv("POSTGRES_PASSWORD", "")

    db_config = {
        "host": db_host,
        "port": int(db_port),
        "database": db_name,
        "user": db_user,
        "password": db_password,
    }
    
    return {
        "groq_key": groq_key,
        "groq_model": os.getenv("GROQ_MODEL", DEFAULT_GROQ_MODEL),
        "db_config": db_config,
    }


def get_db_connection(db_config: dict):
    """Get SQLAlchemy connection (pandas read_sql compatible)."""
    user = quote_plus(str(db_config["user"]))
    password = quote_plus(str(db_config["password"]))
    host = db_config["host"]
    port = db_config["port"]
    database = db_config["database"]
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True,
    )
    return engine.connect()


# =============================================================================
# SCHEMA — Pydantic models for LLM output
# =============================================================================

SourceFile = Literal["market", "macro", "news", "fundamentals", "valuations", "orderbook", "technicals"]
Dimension = Literal["MARKET", "MACRO", "NEWS", "FUNDAMENTAL", "VALUATION", "MICROSTRUCTURE", "TECHNICAL", "REGIME"]
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
    expected_return_low_pct: float  # NEW: confidence interval
    expected_return_high_pct: float  # NEW: confidence interval
    risk_reward_ratio: float
    atr_mad: float
    confidence: Conviction
    probability_positive: float  # NEW: probabilistic forecast
    value_at_risk_95_pct: float  # NEW: risk metric
    thesis: str = Field(description="2-3 sentences citing [MKT-*], [NEWS-*], [OB-*], or [PRED-TRADE-*] IDs (French).")


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
    dynamic_buy_threshold: float  # NEW: adaptive thresholds
    dynamic_sell_threshold: float  # NEW: adaptive thresholds
    thesis: str = Field(description="3-5 sentences citing [VAL-*], [FUND-*], [MACRO-*], or [PRED-INV-*] IDs (French).")


class ATWAnalysis(BaseModel):
    as_of_date: str
    last_close_mad: float
    fair_value_low_mad: float
    fair_value_high_mad: float
    upside_pct: float
    market_regime: str  # NEW: BULL/BEAR/SIDEWAYS
    regime_confidence: float  # NEW: regime probability
    findings: list[Finding]
    risks: list[str]
    verdict: Verdict
    conviction: Conviction
    verdict_reasoning: str
    trading_prediction: TradingPrediction
    investment_prediction: InvestmentPrediction


# =============================================================================
# DATA QUALITY VALIDATION
# =============================================================================

@dataclass
class DataQualityIssue:
    severity: str  # 'CRITICAL', 'WARNING', 'INFO'
    source: str
    message: str
    affected_rows: Optional[int] = None


class DataQualityChecker:
    """Validate data quality before analysis"""
    
    def __init__(self):
        self.issues: list[DataQualityIssue] = []
    
    def check_market_data(self, df: pd.DataFrame) -> list[DataQualityIssue]:
        """Validate market data"""
        issues = []
        
        if len(df) == 0:
            issues.append(DataQualityIssue(
                severity='CRITICAL',
                source='market',
                message='No market data available',
            ))
            return issues
        
        # Missing values
        critical_cols = ['seance', 'dernier_cours', 'volume']
        for col in critical_cols:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    issues.append(DataQualityIssue(
                        severity='CRITICAL',
                        source='market',
                        message=f'Missing values in {col}',
                        affected_rows=int(null_count),
                    ))
        
        # Price continuity
        if 'dernier_cours' in df.columns:
            price_changes = df['dernier_cours'].pct_change().abs()
            outliers = price_changes > PRICE_JUMP_THRESHOLD
            if outliers.any():
                issues.append(DataQualityIssue(
                    severity='WARNING',
                    source='market',
                    message=f'Abnormal price jumps detected (>{PRICE_JUMP_THRESHOLD*100}%)',
                    affected_rows=int(outliers.sum()),
                ))
        
        # Data freshness
        if 'seance' in df.columns:
            last_date = pd.to_datetime(df['seance']).max()
            days_since = (pd.Timestamp.now() - last_date).days
            if days_since > 3:
                issues.append(DataQualityIssue(
                    severity='WARNING',
                    source='market',
                    message=f'Stale data: last update {days_since} days ago',
                ))
        
        return issues
    
    def check_orderbook_data(self, df: pd.DataFrame) -> list[DataQualityIssue]:
        """Validate orderbook data"""
        issues = []
        
        if len(df) == 0:
            issues.append(DataQualityIssue(
                severity='WARNING',
                source='orderbook',
                message='No orderbook data available',
            ))
            return issues
        
        # Crossed quotes
        bid_cols = [c for c in df.columns if re.match(r"^bid\d+_price$", c) or "bid_price" in c]
        ask_cols = [c for c in df.columns if re.match(r"^ask\d+_price$", c) or "ask_price" in c]
        
        if bid_cols and ask_cols:
            def _price_level(col: str) -> int:
                m = re.match(r"^(?:bid|ask)(\d+)_price$", col)
                return int(m.group(1)) if m else 1
            
            bid_l1 = df[sorted(bid_cols, key=_price_level)[0]] if bid_cols else None
            ask_l1 = df[sorted(ask_cols, key=_price_level)[0]] if ask_cols else None
            
            if bid_l1 is not None and ask_l1 is not None:
                crossed = bid_l1 >= ask_l1
                if crossed.any():
                    issues.append(DataQualityIssue(
                        severity='CRITICAL',
                        source='orderbook',
                        message='Crossed quotes detected (bid >= ask)',
                        affected_rows=int(crossed.sum()),
                    ))
        
        return issues
    
    def report(self, issues: list[DataQualityIssue]) -> str:
        """Generate report"""
        if not issues:
            return "✅ All data quality checks passed"
        
        report = ["⚠️ DATA QUALITY ISSUES:\n"]
        
        critical = [i for i in issues if i.severity == 'CRITICAL']
        warnings = [i for i in issues if i.severity == 'WARNING']
        
        if critical:
            report.append("🔴 CRITICAL:")
            for issue in critical:
                report.append(f"  - [{issue.source}] {issue.message}")
        
        if warnings:
            report.append("\n🟡 WARNINGS:")
            for issue in warnings:
                report.append(f"  - [{issue.source}] {issue.message}")
        
        return "\n".join(report)


# =============================================================================
# DATABASE LOADERS — Read from all tables
# =============================================================================

@dataclass
class MarketSnapshot:
    as_of: str
    last_close: float
    market_cap: float
    volume: float
    return_1d_pct: float | None
    return_1w_pct: float | None
    return_1m_pct: float | None
    return_3m_pct: float | None
    return_6m_pct: float | None
    return_52w_pct: float | None
    high_52w: float
    low_52w: float
    high_4w: float
    low_4w: float
    atr_14d: float
    volatility_20d: float
    volume_20d_avg: float
    price_vs_ma20_pct: float
    price_vs_ma50_pct: float


@dataclass
class OrderbookSnapshot:
    """Aggregated orderbook features"""
    date: str
    oi_mean: float  # Order imbalance
    oi_std: float
    oi_zscore: float
    spread_mean_bps: float
    spread_max_bps: float
    depth_ratio: float
    vwmp: float  # Volume-weighted mid price
    intraday_return_pct: float
    tick_volatility_pct: float
    total_volume: float
    n_snapshots: int


@dataclass
class TechnicalIndicators:
    """Technical indicators from technicals_snapshot table"""
    rsi_14: float | None
    macd: float | None
    macd_signal: float | None
    bb_upper: float | None
    bb_lower: float | None
    bb_position: float | None


@dataclass
class MacroSnapshot:
    as_of: str
    gdp_growth: float | None
    cpi: float | None
    unemployment_rate: float | None
    interest_rate: float | None
    debt_to_gdp: float | None
    exchange_rate_usd: float | None


@dataclass
class FundamentalSnapshot:
    as_of: str
    per: float | None
    pbr: float | None
    roe: float | None
    debt_to_equity: float | None
    current_ratio: float | None
    dividend_yield: float | None
    revenue: float | None
    net_income: float | None


@dataclass
class ValuationSnapshot:
    as_of: str
    dcf_price: float | None
    ddm_price: float | None
    graham_price: float | None  # textbook Graham Number = sqrt(22.5*EPS*BVPS)
    graham_growth_price: float | None  # Graham Growth Formula (informational)
    relative_price: float | None
    fair_value_low: float
    fair_value_high: float


@dataclass
class NewsRow:
    timestamp: str
    headline: str
    summary: str
    signal_score: float
    is_atw_relevant: bool


def load_market_from_db(conn, asof_date: str | None = None) -> MarketSnapshot:
    """Load market data from bourse_daily table"""
    query = """
        SELECT 
            seance,
            dernier_cours,
            capitalisation,
            volume,
            plus_haut AS plus_haut_du_jour,
            plus_bas AS plus_bas_du_jour
        FROM bourse_daily
        ORDER BY seance DESC
        LIMIT 252
    """
    
    df = pd.read_sql(text(query), conn)
    df = df.sort_values('seance').reset_index(drop=True)
    
    if len(df) == 0:
        raise DataQualityError("No market data in database")
    
    close = df['dernier_cours']
    high = df['plus_haut_du_jour']
    low = df['plus_bas_du_jour']
    volume = df['volume']
    
    # Returns
    def pct_change(series, window):
        if len(series) <= window:
            return None
        last, past = series.iloc[-1], series.iloc[-1 - window]
        if past == 0 or pd.isna(past):
            return None
        return float((last - past) / past * 100)
    
    # ATR calculation
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr_14d = float(tr.tail(ATR_WINDOW).mean())
    
    # Volatility
    returns = close.pct_change()
    volatility_20d = float(returns.tail(20).std() * 100)
    
    # Moving averages
    ma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else close.iloc[-1]
    ma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else close.iloc[-1]
    
    return MarketSnapshot(
        as_of=str(df['seance'].iloc[-1]),
        last_close=float(close.iloc[-1]),
        market_cap=float(df['capitalisation'].iloc[-1]),
        volume=float(volume.iloc[-1]),
        return_1d_pct=pct_change(close, 1),
        return_1w_pct=pct_change(close, 5),
        return_1m_pct=pct_change(close, 21),
        return_3m_pct=pct_change(close, 63),
        return_6m_pct=pct_change(close, 126),
        return_52w_pct=pct_change(close, 252),
        high_52w=float(close.tail(252).max()),
        low_52w=float(close.tail(252).min()),
        high_4w=float(close.tail(20).max()),
        low_4w=float(close.tail(20).min()),
        atr_14d=atr_14d,
        volatility_20d=volatility_20d,
        volume_20d_avg=float(volume.tail(20).mean()),
        price_vs_ma20_pct=float((close.iloc[-1] / ma20 - 1) * 100),
        price_vs_ma50_pct=float((close.iloc[-1] / ma50 - 1) * 100),
    )


def load_orderbook_features_from_db(conn, asof_date: str | None = None) -> OrderbookSnapshot | None:
    """Load and compute orderbook features from bourse_orderbook table"""
    query = """
        SELECT 
            snapshot_ts,
            bid1_price, bid2_price, bid3_price, bid4_price, bid5_price,
            ask1_price, ask2_price, ask3_price, ask4_price, ask5_price,
            bid1_qty, bid2_qty, bid3_qty, bid4_qty, bid5_qty,
            ask1_qty, ask2_qty, ask3_qty, ask4_qty, ask5_qty
        FROM bourse_orderbook
        WHERE DATE(snapshot_ts) = (SELECT MAX(DATE(snapshot_ts)) FROM bourse_orderbook)
        ORDER BY snapshot_ts
    """
    
    try:
        df = pd.read_sql(text(query), conn)
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        return None
    
    if len(df) == 0:
        return None
    
    # Compute features
    bid_vol_cols = [f'bid{i}_qty' for i in range(1, 6)]
    ask_vol_cols = [f'ask{i}_qty' for i in range(1, 6)]
    
    bid_vol_total = df[bid_vol_cols].sum(axis=1)
    ask_vol_total = df[ask_vol_cols].sum(axis=1)
    
    # Order imbalance
    oi = (bid_vol_total - ask_vol_total) / (bid_vol_total + ask_vol_total + 1e-9)
    
    # Spread
    spread_bps = (df['ask1_price'] - df['bid1_price']) / df['bid1_price'] * 10000
    
    # Mid price
    mid_price = (df['bid1_price'] + df['ask1_price']) / 2
    
    # Depth ratio
    deep_bid = df[[f'bid{i}_qty' for i in range(2, 6)]].sum(axis=1)
    deep_ask = df[[f'ask{i}_qty' for i in range(2, 6)]].sum(axis=1)
    depth_ratio = (deep_bid + deep_ask) / (df['bid1_qty'] + df['ask1_qty'] + 1e-9)
    
    # Volume-weighted mid price
    total_vol = bid_vol_total + ask_vol_total
    vwmp = (mid_price * total_vol).sum() / total_vol.sum()
    
    # Intraday return
    intraday_return = (mid_price.iloc[-1] - mid_price.iloc[0]) / mid_price.iloc[0] * 100 if len(mid_price) > 1 else 0
    
    # Tick volatility
    tick_vol = mid_price.pct_change().std() * 100
    
    # OI z-score (compared to session)
    oi_zscore = (oi.iloc[-1] - oi.mean()) / (oi.std() + 1e-9)
    
    return OrderbookSnapshot(
        date=str(pd.to_datetime(df['snapshot_ts'].iloc[-1]).date()),
        oi_mean=float(oi.mean()),
        oi_std=float(oi.std()),
        oi_zscore=float(oi_zscore),
        spread_mean_bps=float(spread_bps.mean()),
        spread_max_bps=float(spread_bps.max()),
        depth_ratio=float(depth_ratio.mean()),
        vwmp=float(vwmp),
        intraday_return_pct=float(intraday_return),
        tick_volatility_pct=float(tick_vol),
        total_volume=float(total_vol.sum()),
        n_snapshots=len(df),
    )


def load_technicals_from_db(conn) -> TechnicalIndicators:
    """Load technical indicators from technicals_snapshot table"""
    query = """
        SELECT payload
        FROM technicals_snapshot
        WHERE symbol = 'ATW'
        ORDER BY computed_at DESC
        LIMIT 1
    """
    
    try:
        df = pd.read_sql(text(query), conn)
        if len(df) == 0:
            payload = {}
        else:
            payload = df.iloc[0].get('payload') or {}
            if isinstance(payload, str):
                payload = json.loads(payload)
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        payload = {}
    
    def _to_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    
    return TechnicalIndicators(
        rsi_14=_to_float((payload.get('RSI') or {}).get('value')),
        macd=_to_float((payload.get('MACD') or {}).get('macd_line')),
        macd_signal=_to_float((payload.get('MACD') or {}).get('signal_line')),
        bb_upper=_to_float((payload.get('bollinger_bands') or {}).get('upper')),
        bb_lower=_to_float((payload.get('bollinger_bands') or {}).get('lower')),
        bb_position=_to_float((payload.get('bollinger_bands') or {}).get('percent_b')),
    )


def load_macro_from_db(conn) -> MacroSnapshot:
    """Load macro data from macro_morocco table"""
    query = """
        SELECT 
            date,
            gdp_growth_pct,
            inflation_cpi_pct,
            public_debt_pct_gdp,
            usd_mad
        FROM macro_morocco
        ORDER BY date DESC
        LIMIT 1
    """
    
    try:
        df = pd.read_sql(text(query), conn)
        if len(df) == 0:
            raise DataQualityError("No macro data")
        
        row = df.iloc[0]

        # Apply IMF/WB sanity bands at LOAD time so corrupted values never
        # reach downstream code (see MEMORY: IMF DataMapper Morocco bug
        # returns CPI / debt-to-GDP ~100x inflated on some pulls).
        def _within(value, upper):
            if value is None:
                return None
            try:
                return value if float(value) < upper else None
            except (TypeError, ValueError):
                return None

        return MacroSnapshot(
            as_of=str(row['date']),
            gdp_growth=row.get('gdp_growth_pct'),
            cpi=_within(row.get('inflation_cpi_pct'), MACRO_CPI_MAX),
            # Not stored in macro_morocco schema and not produced by the
            # macro collector — leave as None until a source is wired in.
            unemployment_rate=None,
            interest_rate=None,
            debt_to_gdp=_within(row.get('public_debt_pct_gdp'), MACRO_DEBT_GDP_MAX),
            exchange_rate_usd=row.get('usd_mad'),
        )
    except Exception as e:
        if hasattr(conn, "rollback"):
            conn.rollback()
        # Fallback to empty
        return MacroSnapshot(
            as_of=str(datetime.now().date()),
            gdp_growth=None,
            cpi=None,
            unemployment_rate=None,
            interest_rate=None,
            debt_to_gdp=None,
            exchange_rate_usd=None,
        )


def load_fundamentals_from_db(conn) -> FundamentalSnapshot:
    """Load fundamentals from fondamental_snapshot table"""
    query = """
        SELECT 
            scrape_timestamp,
            payload
        FROM fondamental_snapshot
        WHERE symbol = 'ATW'
        ORDER BY scrape_timestamp DESC
        LIMIT 1
    """
    
    try:
        df = pd.read_sql(text(query), conn)
        if len(df) == 0:
            raise DataQualityError("No fundamental data")
        
        row = df.iloc[0]
        payload = row.get('payload') or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        def _latest_metric(block_name: str):
            block = payload.get(block_name)
            if not isinstance(block, dict) or not block:
                return None
            try:
                latest_year = max(block.keys(), key=lambda y: int(y))
                val = block.get(latest_year)
                return float(val) if val is not None else None
            except (TypeError, ValueError):
                return None
        
        # Derive Debt/Equity from latest common year of hist_debt / hist_equity
        debt_block = payload.get('hist_debt') or {}
        equity_block = payload.get('hist_equity') or {}
        de_ratio: float | None = None
        if isinstance(debt_block, dict) and isinstance(equity_block, dict):
            common_years = set(debt_block.keys()) & set(equity_block.keys())
            if common_years:
                try:
                    latest = max(common_years, key=lambda y: int(y))
                    debt_v = debt_block.get(latest)
                    eq_v = equity_block.get(latest)
                    if debt_v is not None and eq_v not in (None, 0):
                        de_ratio = float(debt_v) / float(eq_v)
                except (TypeError, ValueError, ZeroDivisionError):
                    de_ratio = None

        return FundamentalSnapshot(
            as_of=str(row['scrape_timestamp']),
            per=_latest_metric('pe_ratio_hist'),
            pbr=payload.get('price_to_book'),
            roe=_latest_metric('hist_roe'),
            debt_to_equity=de_ratio,
            # Current assets / current liabilities not scraped — no source.
            current_ratio=None,
            dividend_yield=payload.get('dividend_yield'),
            revenue=_latest_metric('hist_revenue'),
            net_income=_latest_metric('hist_net_income'),
        )
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        # Fallback
        return FundamentalSnapshot(
            as_of=str(datetime.now().date()),
            per=None,
            pbr=None,
            roe=None,
            debt_to_equity=None,
            current_ratio=None,
            dividend_yield=None,
            revenue=None,
            net_income=None,
        )


def load_valuations_from_file() -> ValuationSnapshot:
    """Load valuations from models_result.json (keeping file-based for now)"""
    val_file = DATA_DIR / "models_result.json"
    
    if not val_file.exists():
        return ValuationSnapshot(
            as_of=str(datetime.now().date()),
            dcf_price=None,
            ddm_price=None,
            graham_price=None,
            graham_growth_price=None,
            relative_price=None,
            fair_value_low=0,
            fair_value_high=0,
        )

    with open(val_file) as f:
        data = json.load(f)

    # The JSON written by models/fundamental_models.py uses 'intrinsic_value'
    # for the per-model fair price (not 'fair_value').
    def _price(block_name: str) -> float | None:
        block = data.get(block_name) or {}
        return block.get('intrinsic_value')

    dcf_price = _price('dcf')
    ddm_price = _price('ddm')
    graham_price = _price('graham')  # = pure Graham Number
    relative_price = _price('relative')

    # Graham Growth Formula lives in details — keep separate from the main
    # Graham anchor so it doesn't pull the fair-value range upward.
    graham_growth_price = (
        (data.get('graham') or {}).get('details', {}).get('graham_growth_formula')
    )

    valid_prices = [p for p in (dcf_price, ddm_price, graham_price, relative_price) if p is not None]

    return ValuationSnapshot(
        as_of=data.get('as_of_date', str(datetime.now().date())),
        dcf_price=dcf_price,
        ddm_price=ddm_price,
        graham_price=graham_price,
        graham_growth_price=graham_growth_price,
        relative_price=relative_price,
        fair_value_low=min(valid_prices) if valid_prices else 0,
        fair_value_high=max(valid_prices) if valid_prices else 0,
    )


def load_news_from_db(conn, lookback_days: int = 14, min_score: int = 50, cap: int = 10) -> list[NewsRow]:
    """Load news from news table"""
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    
    query = """
        SELECT 
            date,
            title,
            COALESCE(full_content, '') AS full_content,
            signal_score,
            COALESCE(is_atw_core, 0) AS is_atw_core
        FROM news
        WHERE date >= :cutoff_date
          AND signal_score >= :min_score
        ORDER BY signal_score DESC, date DESC
        LIMIT :cap
    """
    
    try:
        df = pd.read_sql(
            text(query),
            conn,
            params={"cutoff_date": cutoff_date, "min_score": min_score, "cap": cap},
        )
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        return []
    
    rows = []
    for _, row in df.iterrows():
        rows.append(NewsRow(
            timestamp=str(row['date']),
            headline=str(row['title'])[:100],
            summary=str(row.get('full_content', ''))[:120],
            signal_score=float(row['signal_score']),
            is_atw_relevant=bool(row.get('is_atw_core', False)),
        ))
    
    return rows


# =============================================================================
# REGIME DETECTION
# =============================================================================

def detect_market_regime(market_snap: MarketSnapshot, technicals: TechnicalIndicators) -> tuple[str, float]:
    """
    Simple regime detection based on trend + volatility.
    
    Returns: (regime_name, confidence)
    - BULL: positive momentum, low/medium volatility
    - BEAR: negative momentum, high volatility
    - SIDEWAYS: weak trend, any volatility
    """
    
    # Trend strength (using multiple timeframes)
    returns = [
        market_snap.return_1w_pct or 0,
        market_snap.return_1m_pct or 0,
        market_snap.return_3m_pct or 0,
    ]
    avg_return = np.mean([r for r in returns if r is not None])
    
    # Volatility level (z-score against the typical daily-vol baseline)
    vol_zscore = (market_snap.volatility_20d - TYPICAL_DAILY_VOL_PCT) / 1.0

    # Price vs moving averages
    above_ma20 = market_snap.price_vs_ma20_pct > 0
    above_ma50 = market_snap.price_vs_ma50_pct > 0

    # Decision logic — both MA filters must agree, otherwise the trend is not
    # strong enough to call a directional regime.
    if avg_return > 1.0 and above_ma20 and above_ma50 and vol_zscore < 1.0:
        regime = "BULL"
        # Both MAs aligned → small confidence bump
        confidence = min(0.9, 0.65 + abs(avg_return) / 10)
    elif avg_return < -1.0 and not above_ma20 and not above_ma50 and vol_zscore > 0:
        regime = "BEAR"
        confidence = min(0.9, 0.65 + abs(avg_return) / 10)
    else:
        regime = "SIDEWAYS"
        confidence = 0.6
    
    return regime, confidence


# =============================================================================
# PREDICTIONS WITH PROBABILISTIC FORECASTING
# =============================================================================

def compute_trading_prediction_enhanced(
    market_snap: MarketSnapshot,
    orderbook: OrderbookSnapshot | None,
    technicals: TechnicalIndicators,
    news_rows: list[NewsRow],
) -> TradingPrediction:
    """Enhanced trading prediction with probabilistic forecasting"""
    
    current_price = market_snap.last_close
    atr = market_snap.atr_14d
    
    # Base targets (ATR-based)
    entry_low = current_price - 0.5 * atr
    entry_high = current_price + 0.5 * atr
    target = current_price + ATR_TARGET_M * atr
    stop = current_price - ATR_STOP_K * atr
    
    # Expected return (point estimate)
    expected_return = (target - current_price) / current_price * 100
    
    # Risk-reward
    risk = (current_price - stop) / current_price * 100
    reward = (target - current_price) / current_price * 100
    rr_ratio = abs(reward / risk) if risk != 0 else 0
    
    # === PROBABILISTIC FORECASTING ===
    
    # Historical volatility for Monte Carlo (volatility_20d is std of daily
    # returns in %, already on the daily scale — no sqrt(252) rescaling)
    daily_vol = market_snap.volatility_20d

    # Expected daily return: convert each horizon return to daily, then average
    daily_from_1w = (market_snap.return_1w_pct or 0) / 5
    daily_from_1m = (market_snap.return_1m_pct or 0) / 21
    avg_daily_return = float(np.mean([daily_from_1w, daily_from_1m]))
    
    # Monte Carlo simulation (1000 paths, TRADING_HORIZON_DAYS)
    n_sims = 1000
    n_days = TRADING_HORIZON_DAYS
    
    np.random.seed(42)  # Reproducible
    Z = np.random.standard_normal((n_sims, n_days))
    
    drift = avg_daily_return / 100 - 0.5 * (daily_vol / 100) ** 2
    returns_sim = np.exp(drift + (daily_vol / 100) * Z)
    
    price_paths = current_price * np.cumprod(returns_sim, axis=1)
    final_prices = price_paths[:, -1]
    
    # Probability of positive return
    prob_positive = (final_prices > current_price).mean()
    
    # Value at Risk (95%)
    returns_dist = (final_prices - current_price) / current_price * 100
    var_95 = -np.percentile(returns_dist, 5)
    
    # Confidence interval (90%)
    ci_low = np.percentile(returns_dist, 5)
    ci_high = np.percentile(returns_dist, 95)
    
    # Confidence level
    if rr_ratio > 2.0 and prob_positive > 0.65:
        confidence = "HIGH"
    elif rr_ratio > 1.0 and prob_positive > 0.55:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"
    
    return TradingPrediction(
        horizon_weeks=TRADING_HORIZON_WEEKS,
        entry_zone_low_mad=float(entry_low),
        entry_zone_high_mad=float(entry_high),
        target_price_mad=float(target),
        stop_loss_mad=float(stop),
        expected_return_pct=float(expected_return),
        expected_return_low_pct=float(ci_low),
        expected_return_high_pct=float(ci_high),
        risk_reward_ratio=float(rr_ratio),
        atr_mad=float(atr),
        confidence=confidence,
        probability_positive=float(prob_positive),
        value_at_risk_95_pct=float(var_95),
        thesis="",  # Filled by LLM
    )


def compute_investment_prediction_enhanced(
    market_snap: MarketSnapshot,
    val_snap: ValuationSnapshot,
    fund_snap: FundamentalSnapshot,
    macro_snap: MacroSnapshot,
    regime: str,
) -> InvestmentPrediction:
    """Enhanced investment prediction with adaptive thresholds"""
    
    # Fair value (Bayesian weighted average - simplified)
    prices = [
        val_snap.dcf_price,
        val_snap.ddm_price,
        val_snap.graham_price,
        val_snap.relative_price,
    ]
    valid_prices = [p for p in prices if p is not None]
    
    if not valid_prices:
        fair_value = market_snap.last_close
        val_uncertainty = 0
    else:
        # Simple average (could be weighted by historical accuracy)
        fair_value = np.mean(valid_prices)
        val_uncertainty = np.std(valid_prices)
    
    # Range
    fair_low = val_snap.fair_value_low if val_snap.fair_value_low > 0 else fair_value * 0.9
    fair_high = val_snap.fair_value_high if val_snap.fair_value_high > 0 else fair_value * 1.1
    
    # Upside
    upside = (fair_value - market_snap.last_close) / market_snap.last_close * 100
    
    # Dividend yield
    div_yield = fund_snap.dividend_yield if fund_snap.dividend_yield else 0
    
    # Total return
    total_return = upside + div_yield
    
    # === ADAPTIVE THRESHOLDS ===
    
    base_buy = RECO_BUY_THRESHOLD
    base_sell = RECO_SELL_THRESHOLD
    
    # Volatility adjustment (relative to typical daily vol)
    vol_multiplier = market_snap.volatility_20d / TYPICAL_DAILY_VOL_PCT
    
    # Regime adjustment
    if regime == "BULL":
        regime_mult_buy = 0.8
        regime_mult_sell = 1.2
    elif regime == "BEAR":
        regime_mult_buy = 1.3
        regime_mult_sell = 0.7
    else:  # SIDEWAYS
        regime_mult_buy = 1.0
        regime_mult_sell = 1.0
    
    # Uncertainty adjustment
    uncertainty_factor = 1 + val_uncertainty / 20 if val_uncertainty > 0 else 1.0
    
    dynamic_buy = base_buy * vol_multiplier * regime_mult_buy * uncertainty_factor
    dynamic_sell = base_sell * vol_multiplier * regime_mult_sell * uncertainty_factor
    
    # Recommendation
    if upside >= dynamic_buy:
        recommendation = "ACHAT"
        confidence = "HIGH" if upside > dynamic_buy * 1.5 else "MEDIUM"
    elif upside <= dynamic_sell:
        recommendation = "VENDRE"
        confidence = "HIGH" if upside < dynamic_sell * 1.5 else "MEDIUM"
    else:
        recommendation = "CONSERVER"
        confidence = "MEDIUM" if abs(upside) < 5 else "LOW"
    
    return InvestmentPrediction(
        horizon_months=INVESTMENT_HORIZON_MONTHS,
        target_price_mad=float(fair_value),
        target_price_low_mad=float(fair_low),
        target_price_high_mad=float(fair_high),
        upside_pct=float(upside),
        dividend_yield_pct=float(div_yield),
        expected_total_return_pct=float(total_return),
        recommendation=recommendation,
        confidence=confidence,
        dynamic_buy_threshold=float(dynamic_buy),
        dynamic_sell_threshold=float(dynamic_sell),
        thesis="",  # Filled by LLM
    )


# =============================================================================
# EVIDENCE BLOCK COMPOSITION
# =============================================================================

def compose_evidence_block_enhanced(
    market_snap: MarketSnapshot,
    orderbook: OrderbookSnapshot | None,
    technicals: TechnicalIndicators,
    macro_snap: MacroSnapshot,
    fund_snap: FundamentalSnapshot,
    val_snap: ValuationSnapshot,
    news_rows: list[NewsRow],
    trading_pred: TradingPrediction,
    investment_pred: InvestmentPrediction,
    regime: str,
    regime_confidence: float,
    today: str,
) -> str:
    """Compose comprehensive evidence block from all data sources"""
    
    lines = []
    
    # Header
    lines.append("=== EVIDENCE BLOCK FOR ATW ANALYSIS ===")
    lines.append(f"Date: {today}")
    lines.append(f"Ticker: ATW (Moroccan Stock Exchange)")
    lines.append("")
    
    # === MARKET DATA ===
    lines.append("--- MARKET DATA ---")
    lines.append(f"[MKT-PRICE] Dernier cours: {market_snap.last_close:.2f} MAD (date: {market_snap.as_of})")
    lines.append(f"[MKT-CAP] Capitalisation: {market_snap.market_cap/1e6:.2f}M MAD")
    lines.append(f"[MKT-VOL] Volume: {market_snap.volume:,.0f} (avg 20j: {market_snap.volume_20d_avg:,.0f})")
    lines.append(f"[MKT-ATR] ATR(14): {market_snap.atr_14d:.2f} MAD (volatilité: {market_snap.volatility_20d:.2f}%)")
    
    if market_snap.return_1d_pct is not None:
        lines.append(f"[MKT-RET-1D] Performance 1j: {market_snap.return_1d_pct:+.2f}%")
    if market_snap.return_1w_pct is not None:
        lines.append(f"[MKT-RET-1W] Performance 1sem: {market_snap.return_1w_pct:+.2f}%")
    if market_snap.return_1m_pct is not None:
        lines.append(f"[MKT-RET-1M] Performance 1mois: {market_snap.return_1m_pct:+.2f}%")
    if market_snap.return_3m_pct is not None:
        lines.append(f"[MKT-RET-3M] Performance 3mois: {market_snap.return_3m_pct:+.2f}%")
    if market_snap.return_52w_pct is not None:
        lines.append(f"[MKT-RET-52W] Performance 52sem: {market_snap.return_52w_pct:+.2f}%")
    
    lines.append(f"[MKT-52W-RANGE] Range 52sem: {market_snap.low_52w:.2f} - {market_snap.high_52w:.2f} MAD")
    lines.append(f"[MKT-4W-RANGE] Range 4sem: {market_snap.low_4w:.2f} - {market_snap.high_4w:.2f} MAD")
    lines.append(f"[MKT-MA20] Prix vs MA20: {market_snap.price_vs_ma20_pct:+.2f}%")
    lines.append(f"[MKT-MA50] Prix vs MA50: {market_snap.price_vs_ma50_pct:+.2f}%")
    lines.append("")
    
    # === REGIME ===
    lines.append("--- REGIME DE MARCHE ---")
    lines.append(f"[REGIME] Régime actuel: {regime} (confiance: {regime_confidence:.1%})")
    
    regime_desc = {
        "BULL": "Marché haussier - tendance positive avec volatilité contrôlée",
        "BEAR": "Marché baissier - tendance négative avec volatilité élevée",
        "SIDEWAYS": "Marché latéral - consolidation sans tendance claire"
    }
    lines.append(f"[REGIME-DESC] {regime_desc.get(regime, 'Indéterminé')}")
    lines.append("")
    
    # === ORDERBOOK (if available) ===
    if orderbook:
        lines.append("--- MICROSTRUCTURE (ORDERBOOK) ---")
        lines.append(f"[OB-IMBAL] Order imbalance moyen: {orderbook.oi_mean:.3f} (std: {orderbook.oi_std:.3f})")
        lines.append(f"[OB-IMBAL-Z] OI z-score: {orderbook.oi_zscore:+.2f} (déviation par rapport à session)")
        
        oi_signal = "ACHAT fort" if orderbook.oi_zscore > 2 else "VENTE fort" if orderbook.oi_zscore < -2 else "neutre"
        lines.append(f"[OB-SIGNAL] Signal microstructure: {oi_signal}")
        
        lines.append(f"[OB-SPREAD] Bid-ask spread: {orderbook.spread_mean_bps:.1f} bps (max: {orderbook.spread_max_bps:.1f} bps)")
        lines.append(f"[OB-DEPTH] Profondeur du carnet: ratio {orderbook.depth_ratio:.2f}")
        lines.append(f"[OB-VWMP] Prix mid pondéré volume: {orderbook.vwmp:.2f} MAD")
        lines.append(f"[OB-INTRA] Performance intraday: {orderbook.intraday_return_pct:+.2f}%")
        lines.append(f"[OB-VOL] Volatilité tick-to-tick: {orderbook.tick_volatility_pct:.2f}%")
        lines.append(f"[OB-SNAPSHOTS] Nombre snapshots: {orderbook.n_snapshots}")
        lines.append("")
    
    # === TECHNICAL INDICATORS ===
    if any([technicals.rsi_14, technicals.macd, technicals.bb_position]):
        lines.append("--- INDICATEURS TECHNIQUES ---")
        
        if technicals.rsi_14 is not None:
            rsi = technicals.rsi_14
            rsi_zone = "suracheté" if rsi > 70 else "survendu" if rsi < 30 else "neutre"
            lines.append(f"[TECH-RSI] RSI(14): {rsi:.1f} (zone: {rsi_zone})")
        
        if technicals.macd is not None and technicals.macd_signal is not None:
            macd_diff = technicals.macd - technicals.macd_signal
            macd_signal = "haussier" if macd_diff > 0 else "baissier"
            lines.append(f"[TECH-MACD] MACD: {technicals.macd:.2f}, Signal: {technicals.macd_signal:.2f} ({macd_signal})")
        
        if technicals.bb_position is not None:
            bb = technicals.bb_position
            bb_zone = "proche bande sup" if bb > 0.8 else "proche bande inf" if bb < 0.2 else "milieu bandes"
            lines.append(f"[TECH-BB] Position Bollinger: {bb:.2f} ({bb_zone})")
        
        lines.append("")
    
    # === MACRO ===
    lines.append("--- MACRO MAROC ---")
    lines.append(f"[MACRO-DATE] Données au: {macro_snap.as_of}")
    
    if macro_snap.gdp_growth is not None:
        lines.append(f"[MACRO-GDP] Croissance PIB: {macro_snap.gdp_growth:+.2f}%")
    if macro_snap.cpi is not None and macro_snap.cpi < MACRO_CPI_MAX:
        lines.append(f"[MACRO-CPI] Inflation (CPI): {macro_snap.cpi:.2f}%")
    if macro_snap.unemployment_rate is not None:
        lines.append(f"[MACRO-UNEMP] Taux chômage: {macro_snap.unemployment_rate:.2f}%")
    if macro_snap.interest_rate is not None:
        lines.append(f"[MACRO-RATE] Taux directeur: {macro_snap.interest_rate:.2f}%")
    if macro_snap.debt_to_gdp is not None and macro_snap.debt_to_gdp < MACRO_DEBT_GDP_MAX:
        lines.append(f"[MACRO-DEBT] Dette/PIB: {macro_snap.debt_to_gdp:.1f}%")
    if macro_snap.exchange_rate_usd is not None:
        lines.append(f"[MACRO-FX] Taux USD/MAD: {macro_snap.exchange_rate_usd:.2f}")
    
    lines.append("")
    
    # === FUNDAMENTALS ===
    lines.append("--- FONDAMENTAUX ---")
    lines.append(f"[FUND-DATE] Données au: {fund_snap.as_of}")
    
    if fund_snap.per is not None:
        lines.append(f"[FUND-PER] PER: {fund_snap.per:.2f}x")
    if fund_snap.pbr is not None:
        lines.append(f"[FUND-PBR] Price/Book: {fund_snap.pbr:.2f}x")
    if fund_snap.roe is not None:
        lines.append(f"[FUND-ROE] ROE: {fund_snap.roe:.2f}%")
    if fund_snap.debt_to_equity is not None:
        lines.append(f"[FUND-DEBT] Dette/Capitaux: {fund_snap.debt_to_equity:.2f}x")
    if fund_snap.current_ratio is not None:
        lines.append(f"[FUND-CURRENT] Ratio courant: {fund_snap.current_ratio:.2f}")
    if fund_snap.dividend_yield is not None:
        lines.append(f"[FUND-DIV] Rendement dividende: {fund_snap.dividend_yield:.2f}%")
    if fund_snap.revenue is not None:
        lines.append(f"[FUND-REV] Chiffre affaires: {fund_snap.revenue/1e6:.2f}M MAD")
    if fund_snap.net_income is not None:
        lines.append(f"[FUND-NI] Résultat net: {fund_snap.net_income/1e6:.2f}M MAD")
    
    lines.append("")
    
    # === VALUATIONS ===
    lines.append("--- VALORISATIONS ---")
    lines.append(f"[VAL-DATE] Calcul au: {val_snap.as_of}")
    
    if val_snap.dcf_price is not None:
        lines.append(f"[VAL-DCF] DCF: {val_snap.dcf_price:.2f} MAD")
    if val_snap.ddm_price is not None:
        lines.append(f"[VAL-DDM] Dividend Discount: {val_snap.ddm_price:.2f} MAD")
    if val_snap.graham_price is not None:
        lines.append(f"[VAL-GRAHAM] Graham Number (ancrage défensif): {val_snap.graham_price:.2f} MAD")
    if val_snap.graham_growth_price is not None:
        lines.append(f"[VAL-GRAHAM-GROWTH] Graham Growth Formula (plafond avec croissance): {val_snap.graham_growth_price:.2f} MAD")
    if val_snap.relative_price is not None:
        lines.append(f"[VAL-RELATIVE] Valorisation relative: {val_snap.relative_price:.2f} MAD")
    
    lines.append(f"[VAL-RANGE] Fourchette juste valeur: {val_snap.fair_value_low:.2f} - {val_snap.fair_value_high:.2f} MAD")
    
    fair_mid = (val_snap.fair_value_low + val_snap.fair_value_high) / 2
    upside = (fair_mid - market_snap.last_close) / market_snap.last_close * 100
    lines.append(f"[VAL-UPSIDE] Potentiel valorisation: {upside:+.1f}%")
    lines.append("")
    
    # === NEWS ===
    if news_rows:
        lines.append(f"--- NEWS (derniers {len(news_rows)} articles pertinents) ---")
        
        for i, news in enumerate(news_rows, 1):
            atw_marker = "★ ATW" if news.is_atw_relevant else ""
            lines.append(f"[NEWS-{i}] ({news.timestamp[:10]}) Score: {news.signal_score:.0f}/100 {atw_marker}")
            lines.append(f"  Titre: {news.headline}")
            if news.summary:
                lines.append(f"  Résumé: {news.summary}")
        
        # Aggregate sentiment
        avg_score = np.mean([n.signal_score for n in news_rows])
        atw_count = sum(1 for n in news_rows if n.is_atw_relevant)
        
        sentiment = "positif" if avg_score > 60 else "négatif" if avg_score < 40 else "neutre"
        lines.append(f"[NEWS-SENTIMENT] Sentiment agrégé: {sentiment} (score moyen: {avg_score:.0f}/100)")
        lines.append(f"[NEWS-ATW-COUNT] Articles spécifiques ATW: {atw_count}/{len(news_rows)}")
        lines.append("")
    
    # === PREDICTIONS (DETERMINISTIC) ===
    lines.append("--- PREDICTIONS TRADING (4 semaines) ---")
    lines.append(f"[PRED-TRADE-HORIZON] Horizon: {trading_pred.horizon_weeks} semaines")
    lines.append(f"[PRED-TRADE-ENTRY] Zone entrée: {trading_pred.entry_zone_low_mad:.2f} - {trading_pred.entry_zone_high_mad:.2f} MAD")
    lines.append(f"[PRED-TRADE-TARGET] Objectif: {trading_pred.target_price_mad:.2f} MAD")
    lines.append(f"[PRED-TRADE-STOP] Stop loss: {trading_pred.stop_loss_mad:.2f} MAD (ATR×{ATR_STOP_K})")
    lines.append(f"[PRED-TRADE-RET] Rendement attendu: {trading_pred.expected_return_pct:+.2f}%")
    lines.append(f"[PRED-TRADE-CI] Intervalle confiance 90%: [{trading_pred.expected_return_low_pct:+.2f}%, {trading_pred.expected_return_high_pct:+.2f}%]")
    lines.append(f"[PRED-TRADE-PROB] Probabilité rendement positif: {trading_pred.probability_positive:.1%}")
    lines.append(f"[PRED-TRADE-VAR] Value at Risk 95%: {trading_pred.value_at_risk_95_pct:.2f}%")
    lines.append(f"[PRED-TRADE-RR] Risk/Reward: {trading_pred.risk_reward_ratio:.2f}")
    lines.append("")
    
    lines.append("--- PREDICTIONS INVESTISSEMENT (12 mois) ---")
    lines.append(f"[PRED-INV-HORIZON] Horizon: {investment_pred.horizon_months} mois")
    lines.append(f"[PRED-INV-TARGET] Cours cible: {investment_pred.target_price_mad:.2f} MAD")
    lines.append(f"[PRED-INV-RANGE] Fourchette: {investment_pred.target_price_low_mad:.2f} - {investment_pred.target_price_high_mad:.2f} MAD")
    lines.append(f"[PRED-INV-UPSIDE] Potentiel cours: {investment_pred.upside_pct:+.2f}%")
    lines.append(f"[PRED-INV-DIV] Rendement dividende: {investment_pred.dividend_yield_pct:.2f}%")
    lines.append(f"[PRED-INV-TSR] TSR attendu: {investment_pred.expected_total_return_pct:+.2f}%")
    lines.append(f"[PRED-INV-THRESH] Seuils dynamiques: ACHAT>{investment_pred.dynamic_buy_threshold:.1f}%, VENTE<{investment_pred.dynamic_sell_threshold:.1f}%")
    lines.append(f"[PRED-INV-RECO] Recommandation: {investment_pred.recommendation} (confiance: {investment_pred.confidence})")
    lines.append("")
    
    lines.append("=== FIN EVIDENCE BLOCK ===")
    
    return "\n".join(lines)


# =============================================================================
# CITATION VALIDATION
# =============================================================================

def extract_valid_refs(evidence_block: str) -> set[str]:
    """Extract all valid reference IDs from evidence block"""
    pattern = r'\[([A-Z]+-[A-Z0-9-]+)\]'
    matches = re.findall(pattern, evidence_block)
    return set(matches)


def validate_citations(analysis: ATWAnalysis, valid_refs: set[str]) -> list[str]:
    """
    Validate that all citations reference valid evidence IDs.
    Returns list of errors (empty if valid).
    """
    errors = []
    
    # Check findings
    for i, finding in enumerate(analysis.findings):
        for j, evidence in enumerate(finding.evidence):
            if evidence.source_ref not in valid_refs:
                errors.append(f"findings[{i}].evidence[{j}]: Invalid ref {evidence.source_ref}")
    
    # Check verdict reasoning
    verdict_cites = re.findall(r'\[([A-Z]+-[A-Z0-9-]+)\]', analysis.verdict_reasoning)
    for cite in verdict_cites:
        if cite not in valid_refs:
            errors.append(f"verdict_reasoning: Invalid ref {cite}")
    
    # Check trading thesis
    trading_cites = re.findall(r'\[([A-Z]+-[A-Z0-9-]+)\]', analysis.trading_prediction.thesis)
    for cite in trading_cites:
        if cite not in valid_refs:
            errors.append(f"trading_prediction.thesis: Invalid ref {cite}")
    
    # Check investment thesis
    inv_cites = re.findall(r'\[([A-Z]+-[A-Z0-9-]+)\]', analysis.investment_prediction.thesis)
    for cite in inv_cites:
        if cite not in valid_refs:
            errors.append(f"investment_prediction.thesis: Invalid ref {cite}")
    
    return errors


# =============================================================================
# LLM SYNTHESIS
# =============================================================================

INSTRUCTIONS = """Tu es un analyste financier senior spécialisé dans le marché marocain.

Tu dois analyser TOUTES les données fournies dans le bloc d'evidence et produire une analyse structurée et citée.

REGLES STRICTES DE CITATION:
1. Chaque affirmation dans 'findings' doit être appuyée par AU MOINS une citation [ID]
2. Les citations doivent référencer EXACTEMENT les IDs fournis dans le bloc (ex: [MKT-PRICE], [VAL-DCF], [NEWS-1])
3. NE PAS inventer de citations - utilise UNIQUEMENT les IDs présents dans le bloc
4. Les thèses (trading et investissement) doivent citer les IDs pertinents

DIMENSIONS À ANALYSER:
- MARKET: performance, volatilité, volumes, niveaux techniques
- MICROSTRUCTURE: orderbook (si disponible), imbalance, spread, profondeur
- TECHNICAL: RSI, MACD, Bollinger (si disponibles)
- REGIME: régime de marché actuel (BULL/BEAR/SIDEWAYS)
- MACRO: croissance, inflation, taux, contexte économique Maroc
- FUNDAMENTAL: ratios financiers, santé de l'entreprise
- VALUATION: juste valeur selon différents modèles
- NEWS: sentiment des actualités récentes

PREDICTIONS:
- Les valeurs numériques (prix cible, stop loss, rendements) sont DÉJÀ calculées - garde-les telles quelles
- Ton rôle: formuler les THÈSES (narratives explicatives) en citant les sources

QUALITÉ ATTENDUE:
- Précis et factuel
- Chaque finding doit apporter un insight distinct
- Les risques doivent être concrets (pas de généralités)
- Le verdict doit être cohérent avec les findings et predictions
- FORMAT DE SORTIE: renvoie STRICTEMENT un objet JSON valide, sans markdown, sans balises ```json.
- Le JSON doit contenir TOUS les champs requis du schéma ATWAnalysis.
"""


def build_synth_agent(model: str, groq_key: str) -> Agent:
    """Build LLM synthesis agent"""
    return Agent(
        model=Groq(id=model, api_key=groq_key, max_tokens=4096, temperature=0.2),
        instructions=INSTRUCTIONS,
    )


def _coerce_analysis(content: object) -> ATWAnalysis | None:
    if isinstance(content, ATWAnalysis):
        return content
    if isinstance(content, dict):
        try:
            return ATWAnalysis.model_validate(content)
        except Exception:
            return None
    if isinstance(content, str):
        text = content.strip()
        candidates: list[str] = []

        fence_matches = re.findall(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
        candidates.extend(fence_matches)
        candidates.append(text)

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidates.append(text[start:end + 1])

        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                obj = json.loads(candidate)
                return ATWAnalysis.model_validate(obj)
            except Exception:
                continue
    return None


def synthesize(agent: Agent, evidence_block: str, today: str) -> ATWAnalysis:
    """Call LLM to synthesize analysis with bounded retries."""
    base_prompt = (
        f"Aujourd'hui: {today}\n\n{evidence_block}\n\n"
        "Retourne UNIQUEMENT un objet JSON valide correspondant au schéma ATWAnalysis."
    )
    retry_prompt = (
        f"{base_prompt}\n"
        "La réponse précédente était invalide/incomplète. "
        "Réécris une réponse JSON complète avec TOUS les champs requis."
    )

    last_content_type = "none"
    for prompt in (base_prompt, retry_prompt):
        try:
            resp = agent.run(prompt)
        except Exception as e:
            last_content_type = f"run_error:{type(e).__name__}"
            continue

        parsed = _coerce_analysis(resp.content)
        if parsed is not None:
            return parsed
        last_content_type = type(resp.content).__name__

    raise SynthesisError(f"LLM output could not be parsed into ATWAnalysis (last={last_content_type})")


def build_fallback_analysis(
    market_snap: MarketSnapshot,
    val_snap: ValuationSnapshot,
    trading_pred: TradingPrediction,
    investment_pred: InvestmentPrediction,
    regime: str,
    regime_confidence: float,
    today: str,
    news_rows: list[NewsRow],
) -> ATWAnalysis:
    """Deterministic fallback when LLM structured output parsing fails."""
    fair_low = float(val_snap.fair_value_low) if val_snap.fair_value_low else float(investment_pred.target_price_low_mad)
    fair_high = float(val_snap.fair_value_high) if val_snap.fair_value_high else float(investment_pred.target_price_high_mad)
    mid = (fair_low + fair_high) / 2 if fair_low > 0 and fair_high > 0 else float(investment_pred.target_price_mad)
    upside_pct = ((mid - market_snap.last_close) / market_snap.last_close * 100) if market_snap.last_close > 0 else 0.0

    reco_to_verdict: dict[str, Verdict] = {
        "ACHAT": "BUY",
        "CONSERVER": "HOLD",
        "VENDRE": "SELL",
    }
    verdict = reco_to_verdict.get(investment_pred.recommendation, "HOLD")
    conviction = investment_pred.confidence

    market_polarity: Polarity = "NEUTRAL"
    r1m = market_snap.return_1m_pct
    if r1m is not None:
        if r1m > 1:
            market_polarity = "BULLISH"
        elif r1m < -1:
            market_polarity = "BEARISH"

    val_polarity: Polarity = "NEUTRAL"
    if upside_pct > 5:
        val_polarity = "BULLISH"
    elif upside_pct < -5:
        val_polarity = "BEARISH"

    regime_polarity: Polarity = "NEUTRAL"
    if regime == "BULL":
        regime_polarity = "BULLISH"
    elif regime == "BEAR":
        regime_polarity = "BEARISH"

    market_evidence = [
        Evidence(claim="Dernier cours ATW", source_ref="MKT-PRICE", source_file="market"),
    ]
    if r1m is not None:
        market_evidence.append(
            Evidence(claim="Performance 1 mois", source_ref="MKT-RET-1M", source_file="market")
        )

    findings: list[Finding] = [
        Finding(
            dimension="MARKET",
            statement=f"Le dernier cours est à {market_snap.last_close:.2f} MAD avec une performance 1 mois de {r1m:+.2f}%." if r1m is not None
            else f"Le dernier cours est à {market_snap.last_close:.2f} MAD.",
            polarity=market_polarity,
            evidence=market_evidence,
        ),
        Finding(
            dimension="VALUATION",
            statement=f"La fourchette de juste valeur ({fair_low:.2f}–{fair_high:.2f} MAD) implique un potentiel de {upside_pct:+.1f}%.",
            polarity=val_polarity,
            evidence=[
                Evidence(claim="Fourchette de valorisation", source_ref="VAL-RANGE", source_file="valuations"),
                Evidence(claim="Potentiel de cours", source_ref="PRED-INV-UPSIDE", source_file="valuations"),
            ],
        ),
        Finding(
            dimension="REGIME",
            statement=f"Le régime de marché détecté est {regime} avec une confiance de {regime_confidence:.1%}.",
            polarity=regime_polarity,
            evidence=[Evidence(claim="Régime de marché", source_ref="REGIME-DESC", source_file="market")],
        ),
    ]

    if news_rows:
        avg_news = float(np.mean([n.signal_score for n in news_rows]))
        news_polarity: Polarity = "NEUTRAL"
        if avg_news > 60:
            news_polarity = "BULLISH"
        elif avg_news < 40:
            news_polarity = "BEARISH"
        findings.append(
            Finding(
                dimension="NEWS",
                statement=f"Le flux news récent montre un score moyen de {avg_news:.0f}/100 sur {len(news_rows)} articles.",
                polarity=news_polarity,
                evidence=[Evidence(claim="Article principal du flux", source_ref="NEWS-1", source_file="news")],
            )
        )

    risks = [
        "Les données de marché sont légèrement anciennes (mise à jour récente non disponible).",
        f"Le régime {regime} peut modifier rapidement le couple rendement/risque.",
        "La cible dépend des hypothèses de valorisation et peut diverger du prix de marché à court terme.",
    ]

    trading_thesis = (
        f"Le scénario trading reste encadré par un rendement attendu de {trading_pred.expected_return_pct:+.2f}% "
        f"et une gestion du risque via le stop défini ([PRED-TRADE-RET]). "
        f"Le niveau de prix actuel reste la référence d'exécution ([MKT-PRICE])."
    )
    investment_thesis = (
        f"Le potentiel d'investissement ressort à {investment_pred.upside_pct:+.2f}% avec une recommandation "
        f"{investment_pred.recommendation.lower()} ([PRED-INV-UPSIDE]). "
        f"La fourchette de valorisation reste large et doit être suivie dans le temps ([VAL-RANGE]). "
        f"Le contexte de régime {regime} impose une discipline d'entrée progressive ([REGIME-DESC])."
    )

    verdict_reasoning = (
        f"Le verdict {verdict} est aligné avec le potentiel calculé ([PRED-INV-UPSIDE]) "
        f"et la fourchette de valorisation ([VAL-RANGE]). "
        f"Le prix observé et le régime de marché confirment un biais prudent ([MKT-PRICE], [REGIME-DESC])."
    )

    return ATWAnalysis(
        as_of_date=today,
        last_close_mad=float(market_snap.last_close),
        fair_value_low_mad=float(fair_low),
        fair_value_high_mad=float(fair_high),
        upside_pct=float(upside_pct),
        market_regime=regime,
        regime_confidence=float(regime_confidence),
        findings=findings,
        risks=risks,
        verdict=verdict,
        conviction=conviction,
        verdict_reasoning=verdict_reasoning,
        trading_prediction=trading_pred.model_copy(update={"thesis": trading_thesis}),
        investment_prediction=investment_pred.model_copy(update={"thesis": investment_thesis}),
    )


# =============================================================================
# FORMATTER
# =============================================================================

_POLARITY_MARK = {"BULLISH": "📈", "BEARISH": "📉", "NEUTRAL": "➖"}
_DIM_ORDER = ("REGIME", "MARKET", "MICROSTRUCTURE", "TECHNICAL", "MACRO", "FUNDAMENTAL", "VALUATION", "NEWS")
_VERDICT_MARK = {"BUY": "🟢", "HOLD": "🟡", "SELL": "🔴"}
_RECO_MARK = {"ACHAT": "🟢", "CONSERVER": "🟡", "VENDRE": "🔴"}


def _format_citations(f: Finding) -> str:
    return " ".join(e.source_ref for e in f.evidence)


def print_analysis(a: ATWAnalysis) -> None:
    """Pretty-print analysis to terminal"""
    midpoint = (a.fair_value_low_mad + a.fair_value_high_mad) / 2
    
    print(f"\n╔══════════════════════════════════════════════════════════╗")
    print(f"║           ATW ANALYSIS — {a.as_of_date}                  ║")
    print(f"╚══════════════════════════════════════════════════════════╝\n")
    
    print(f"  💰 Dernier cours    : {a.last_close_mad:.2f} MAD")
    print(f"  📊 Juste valeur     : {a.fair_value_low_mad:.2f} – {a.fair_value_high_mad:.2f} MAD (mid {midpoint:.2f})")
    print(f"  📈 Potentiel        : {a.upside_pct:+.1f}%")
    print(f"  🎯 Régime marché    : {a.market_regime} (confiance: {a.regime_confidence:.1%})\n")
    
    # Findings by dimension
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
            citations = _format_citations(f)
            print(f"  {mark} {f.statement}")
            print(f"     {citations}")
        print()
    
    # Risks
    if a.risks:
        print("⚠️  RISQUES")
        for r in a.risks:
            print(f"  • {r}")
        print()
    
    # Trading prediction
    t = a.trading_prediction
    print(f"📊 TRADING ({t.horizon_weeks} semaines)")
    print(f"   Zone d'entrée       : {t.entry_zone_low_mad:.2f} – {t.entry_zone_high_mad:.2f} MAD")
    print(f"   Objectif            : {t.target_price_mad:.2f} MAD")
    print(f"   Stop de protection  : {t.stop_loss_mad:.2f} MAD (ATR×{ATR_STOP_K} = {t.atr_mad:.2f} MAD)")
    print(f"   Rendement attendu   : {t.expected_return_pct:+.2f}% (IC 90%: [{t.expected_return_low_pct:+.2f}%, {t.expected_return_high_pct:+.2f}%])")
    print(f"   Prob. positif       : {t.probability_positive:.1%}")
    print(f"   VaR 95%             : {t.value_at_risk_95_pct:.2f}%")
    print(f"   Risk/Reward         : {t.risk_reward_ratio:.2f}    Confiance: {t.confidence}")
    print(f"   Thèse : {t.thesis}\n")
    
    # Investment prediction
    i = a.investment_prediction
    rmark = _RECO_MARK.get(i.recommendation, "·")
    print(f"💼 INVESTISSEMENT ({i.horizon_months} mois)")
    print(f"   Cours cible         : {i.target_price_mad:.2f} MAD (range {i.target_price_low_mad:.2f}–{i.target_price_high_mad:.2f})")
    print(f"   Potentiel cours     : {i.upside_pct:+.2f}%")
    print(f"   Rendement div.      : {i.dividend_yield_pct:.2f}%")
    print(f"   TSR attendu         : {i.expected_total_return_pct:+.2f}%")
    print(f"   Seuils dynamiques   : ACHAT>{i.dynamic_buy_threshold:.1f}%, VENTE<{i.dynamic_sell_threshold:.1f}%")
    print(f"   {rmark} Recommandation   : {i.recommendation}    Confiance: {i.confidence}")
    print(f"   Thèse : {i.thesis}\n")
    
    # Verdict
    mark = _VERDICT_MARK.get(a.verdict, "·")
    print(f"{mark} VERDICT: {a.verdict}  (conviction: {a.conviction})")
    print(f"   {a.verdict_reasoning}\n")


# =============================================================================
# HISTORY SAVING
# =============================================================================

HISTORY_CSV = DATA_DIR / "prediction_history_enhanced.csv"


def save_prediction_history(analysis: ATWAnalysis, path: Path = HISTORY_CSV) -> None:
    """Append analysis to history CSV with full audit trail"""
    t = analysis.trading_prediction
    i = analysis.investment_prediction
    
    new_row = {
        'run_timestamp': datetime.now(tz=timezone.utc).isoformat(),
        'as_of_date': analysis.as_of_date,
        'last_close_mad': analysis.last_close_mad,
        'fair_value_low_mad': analysis.fair_value_low_mad,
        'fair_value_high_mad': analysis.fair_value_high_mad,
        'upside_pct': analysis.upside_pct,
        'market_regime': analysis.market_regime,
        'regime_confidence': analysis.regime_confidence,
        'verdict': analysis.verdict,
        'conviction': analysis.conviction,
        
        # Trading
        'trading_horizon_weeks': t.horizon_weeks,
        'trading_entry_low_mad': t.entry_zone_low_mad,
        'trading_entry_high_mad': t.entry_zone_high_mad,
        'trading_target_mad': t.target_price_mad,
        'trading_stop_loss_mad': t.stop_loss_mad,
        'trading_expected_return_pct': t.expected_return_pct,
        'trading_expected_return_low_pct': t.expected_return_low_pct,
        'trading_expected_return_high_pct': t.expected_return_high_pct,
        'trading_probability_positive': t.probability_positive,
        'trading_var_95_pct': t.value_at_risk_95_pct,
        'trading_risk_reward_ratio': t.risk_reward_ratio,
        'trading_atr_mad': t.atr_mad,
        'trading_confidence': t.confidence,
        
        # Investment
        'investment_horizon_months': i.horizon_months,
        'investment_target_mad': i.target_price_mad,
        'investment_target_low_mad': i.target_price_low_mad,
        'investment_target_high_mad': i.target_price_high_mad,
        'investment_upside_pct': i.upside_pct,
        'investment_dividend_yield_pct': i.dividend_yield_pct,
        'investment_total_return_pct': i.expected_total_return_pct,
        'investment_dynamic_buy_threshold': i.dynamic_buy_threshold,
        'investment_dynamic_sell_threshold': i.dynamic_sell_threshold,
        'investment_recommendation': i.recommendation,
        'investment_confidence': i.confidence,
        
        # Audit trail
        'trading_thesis': t.thesis,
        'investment_thesis': i.thesis,
        'verdict_reasoning': analysis.verdict_reasoning,
    }
    
    df_new = pd.DataFrame([new_row])
    write_header = not path.exists()
    df_new.to_csv(path, mode='a', header=write_header, index=False, encoding='utf-8')


# =============================================================================
# MAIN
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ATW Enhanced Analysis - Using all database tables")
    p.add_argument("--asof", type=str, default=None, help="Override 'today' as YYYY-MM-DD")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                   help=f"News lookback window (default {DEFAULT_LOOKBACK_DAYS})")
    p.add_argument("--min-news-score", type=int, default=DEFAULT_MIN_NEWS_SCORE,
                   help=f"Min news score (default {DEFAULT_MIN_NEWS_SCORE})")
    p.add_argument("--raw", action="store_true", help="Print raw JSON")
    p.add_argument("--evidence-only", action="store_true", help="Print evidence block only")
    p.add_argument("--no-history", action="store_true", help="Skip saving to history")
    return p.parse_args()


def _today_iso(asof: str | None) -> str:
    return asof or datetime.now(tz=timezone.utc).date().isoformat()


def main() -> int:
    args = _parse_args()
    today = _today_iso(args.asof)
    
    # Load environment
    try:
        env = load_env()
    except MissingEnvError as e:
        print(f"ERROR: {e}", flush=True)
        return 2
    
    # Connect to database
    print(f"[db] Connecting to PostgreSQL...", flush=True)
    try:
        conn = get_db_connection(env['db_config'])
    except Exception as e:
        print(f"ERROR: Database connection failed: {e}", flush=True)
        return 2

    def _close_db() -> None:
        try:
            conn.close()
        finally:
            try:
                conn.engine.dispose()
            except Exception:
                pass
    
    # Load all data
    print(f"[load] Loading data from database tables...", flush=True)
    try:
        market_snap = load_market_from_db(conn, args.asof)
        orderbook_snap = load_orderbook_features_from_db(conn, args.asof)
        technicals = load_technicals_from_db(conn)
        macro_snap = load_macro_from_db(conn)
        fund_snap = load_fundamentals_from_db(conn)
        val_snap = load_valuations_from_file()  # Still from file
        news_rows = load_news_from_db(conn, args.lookback_days, args.min_news_score, DEFAULT_NEWS_CAP)
    except Exception as e:
        print(f"ERROR: Data loading failed: {e}", flush=True)
        _close_db()
        return 2
    
    # Data quality checks
    print(f"[quality] Running data quality checks...", flush=True)
    checker = DataQualityChecker()
    
    market_df = pd.read_sql(text("SELECT * FROM bourse_daily ORDER BY seance DESC LIMIT 252"), conn)
    orderbook_df = pd.read_sql(text("SELECT * FROM bourse_orderbook WHERE DATE(snapshot_ts) = (SELECT MAX(DATE(snapshot_ts)) FROM bourse_orderbook)"), conn) if orderbook_snap else pd.DataFrame()
    
    issues = []
    issues.extend(checker.check_market_data(market_df))
    if len(orderbook_df) > 0:
        issues.extend(checker.check_orderbook_data(orderbook_df))
    
    print(checker.report(issues))
    
    # Halt on critical issues
    critical = [i for i in issues if i.severity == 'CRITICAL']
    if critical:
        print("\n🔴 HALTING: Critical data quality issues detected.", flush=True)
        _close_db()
        return 1
    
    # Detect regime
    regime, regime_confidence = detect_market_regime(market_snap, technicals)
    print(f"[regime] Detected: {regime} (confidence: {regime_confidence:.1%})", flush=True)
    
    # Compute predictions
    print(f"[predict] Computing enhanced predictions...", flush=True)
    trading_pred = compute_trading_prediction_enhanced(market_snap, orderbook_snap, technicals, news_rows)
    investment_pred = compute_investment_prediction_enhanced(market_snap, val_snap, fund_snap, macro_snap, regime)
    
    # Compose evidence block
    evidence_block = compose_evidence_block_enhanced(
        market_snap, orderbook_snap, technicals, macro_snap, fund_snap, val_snap,
        news_rows, trading_pred, investment_pred, regime, regime_confidence, today
    )
    
    if args.evidence_only:
        print(evidence_block)
        _close_db()
        return 0
    
    # Extract valid references for citation validation
    valid_refs = extract_valid_refs(evidence_block)
    
    # LLM synthesis
    print(f"[llm] Calling Groq model {env['groq_model']}...", flush=True)
    agent = build_synth_agent(env['groq_model'], env['groq_key'])
    
    try:
        analysis = synthesize(agent, evidence_block, today)
    except SynthesisError as e:
        print(f"WARNING: {e}", flush=True)
        print("[llm] Falling back to deterministic synthesis...", flush=True)
        analysis = build_fallback_analysis(
            market_snap=market_snap,
            val_snap=val_snap,
            trading_pred=trading_pred,
            investment_pred=investment_pred,
            regime=regime,
            regime_confidence=regime_confidence,
            today=today,
            news_rows=news_rows,
        )
    
    # Override LLM predictions with deterministic ones (keep thesis)
    analysis.market_regime = regime
    analysis.regime_confidence = regime_confidence
    
    analysis.trading_prediction = trading_pred.model_copy(
        update={"thesis": analysis.trading_prediction.thesis}
    )
    analysis.investment_prediction = investment_pred.model_copy(
        update={"thesis": analysis.investment_prediction.thesis}
    )
    
    # Validate citations
    print(f"[validate] Checking citations...", flush=True)
    citation_errors = validate_citations(analysis, valid_refs)
    
    if citation_errors:
        print(f"\n⚠️ CITATION VALIDATION ERRORS:")
        for err in citation_errors:
            print(f"  - {err}")
        print("\n⚠️ WARNING: Invalid citations detected. Review LLM output.\n")
    else:
        print("✅ All citations valid")
    
    # Save to history
    if not args.no_history:
        save_prediction_history(analysis)
        print(f"[history] Saved to {HISTORY_CSV}", flush=True)
    
    # Output
    if args.raw:
        print(analysis.model_dump_json(indent=2))
    else:
        print_analysis(analysis)
    
    _close_db()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
