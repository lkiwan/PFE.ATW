"""ATW-LOSS-ANALYSER — break down losing trades by market context.

Reads `data/backtest_trading_results.csv`, splits trades into winners and
losers, and groups by feature buckets (confidence, ATR ratio, momentum,
position in 52w range, news context, time period). For each bucket prints
the loss rate vs. the global baseline, so you can spot regimes where the
strategy underperforms.

  python agents/analyze_losses.py
  python agents/analyze_losses.py --in data/backtest_trading_results.csv
  python agents/analyze_losses.py --losers-only       # show only losing rows
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IN = PROJECT_ROOT / "data" / "backtest_trading_results.csv"


# =============================================================================
# CLASSIFICATION — what counts as a loser
# =============================================================================

def classify(df: pd.DataFrame) -> pd.DataFrame:
    """Add `is_loser`, `is_winner`, `regime` columns."""
    df = df.copy()
    df = df[df["outcome"] != "INCOMPLETE"]
    df["is_loser"] = df["realized_return_pct"] < 0
    df["is_winner"] = df["realized_return_pct"] > 0

    # ATR ratio regime
    df["atr_regime"] = pd.cut(
        df["atr_ratio_pct"],
        bins=[-0.01, 1.5, 2.5, 100],
        labels=["LOW_VOL (<1.5%)", "MID_VOL (1.5–2.5%)", "HIGH_VOL (>2.5%)"],
    )

    # 3-month momentum regime
    df["momentum_regime"] = pd.cut(
        df["return_3m_pct"],
        bins=[-100, -5, 5, 100],
        labels=["BEAR (<-5%)", "FLAT (-5..+5%)", "BULL (>+5%)"],
    )

    # Position in 52w range (proxied by % below 52w high)
    df["range_position"] = pd.cut(
        df["pct_below_52w_high"],
        bins=[-0.01, 5, 15, 100],
        labels=["NEAR_HIGH (<5%)", "MID_RANGE (5–15%)", "FAR_FROM_HIGH (>15%)"],
    )

    # News context
    df["news_regime"] = pd.cut(
        df["news_count"],
        bins=[-1, 0, 2, 100],
        labels=["NO_NEWS (0)", "FEW_NEWS (1–2)", "MANY_NEWS (≥3)"],
    )

    # Risk/reward ratio bucket
    df["rr_bucket"] = pd.cut(
        df["risk_reward_ratio"],
        bins=[-0.01, 1.0, 2.0, 100],
        labels=["RR<1", "RR 1–2", "RR>2"],
    )

    # Time bucket (year-quarter)
    df["asof_dt"] = pd.to_datetime(df["asof"])
    df["year"] = df["asof_dt"].dt.year
    df["quarter"] = df["asof_dt"].dt.year.astype(str) + "-Q" + df["asof_dt"].dt.quarter.astype(str)

    # Macro regimes (only present if backtest CSV has the columns)
    if "brent_90d_delta_pct" in df.columns:
        df["brent_regime"] = pd.cut(
            df["brent_90d_delta_pct"],
            bins=[-100, -8, 8, 200],
            labels=["BRENT_FALLING (<-8%)", "BRENT_FLAT (-8..+8%)", "BRENT_RISING (>+8%)"],
        )
    if "eur_mad_90d_delta_pct" in df.columns:
        df["mad_regime"] = pd.cut(
            df["eur_mad_90d_delta_pct"],
            bins=[-100, -2, 2, 100],
            labels=["MAD_STRONGER (EUR<-2%)", "MAD_FLAT (-2..+2%)", "MAD_WEAKER (EUR>+2%)"],
        )
    if "masi_90d_delta_pct" in df.columns:
        df["masi_regime"] = pd.cut(
            df["masi_90d_delta_pct"],
            bins=[-100, -3, 3, 100],
            labels=["MASI_FALLING (<-3%)", "MASI_FLAT (-3..+3%)", "MASI_RISING (>+3%)"],
        )
    if "vix" in df.columns:
        df["vix_regime"] = pd.cut(
            df["vix"],
            bins=[0, 15, 22, 100],
            labels=["VIX_LOW (<15)", "VIX_NORMAL (15-22)", "VIX_SPIKE (>22)"],
        )
    if "macro_momentum" in df.columns:
        df["macro_momentum_bucket"] = pd.cut(
            df["macro_momentum"],
            bins=[-100, -0.5, 0.5, 100],
            labels=["MOM_NEG (<-0.5)", "MOM_FLAT (-0.5..+0.5)", "MOM_POS (>+0.5)"],
        )

    return df


# =============================================================================
# BREAKDOWN — group + summarise
# =============================================================================

def breakdown(df: pd.DataFrame, by: str) -> pd.DataFrame:
    """For each group: count, loss rate, avg return, avg max drawdown."""
    grouped = df.groupby(by, observed=True).agg(
        n=("outcome", "size"),
        n_losers=("is_loser", "sum"),
        n_stops=("outcome", lambda s: (s == "STOP").sum()),
        loss_rate_pct=("is_loser", lambda s: round(s.mean() * 100, 2)),
        avg_return_pct=("realized_return_pct", lambda s: round(s.mean(), 2)),
        avg_max_dd_pct=("max_drawdown_pct", lambda s: round(s.mean(), 2)),
    )
    return grouped.sort_values("loss_rate_pct", ascending=False)


def print_breakdown(df: pd.DataFrame, by: str, title: str, baseline_loss_rate: float) -> None:
    print(f"── {title} ──")
    table = breakdown(df, by)
    print(table.to_string())
    # Highlight buckets that materially exceed baseline
    flagged = table[table["loss_rate_pct"] > baseline_loss_rate + 5]
    if not flagged.empty:
        print(f"\n  ⚠️  Bucket(s) with loss rate > baseline ({baseline_loss_rate:.2f}%) + 5pp:")
        for label, row in flagged.iterrows():
            print(f"     • {label}: loss_rate={row['loss_rate_pct']:.2f}% (n={int(row['n'])})")
    print()


# =============================================================================
# CORRELATION — feature vs realized return
# =============================================================================

def correlation_report(df: pd.DataFrame) -> None:
    print("── Pearson correlation: feature → realized_return_pct ──")
    features = [
        "expected_return_pct",
        "risk_reward_ratio",
        "atr_mad",
        "atr_ratio_pct",
        "return_1m_pct",
        "return_3m_pct",
        "return_6m_pct",
        "pct_below_52w_high",
        "news_count",
        "avg_news_score",
        "max_drawdown_pct",
        "max_runup_pct",
    ]
    target = df["realized_return_pct"]
    rows = []
    for f in features:
        if f not in df.columns:
            continue
        s = df[f]
        if s.notna().sum() < 5:
            continue
        corr = s.corr(target)
        if pd.isna(corr):
            continue
        rows.append((f, round(corr, 3), int(s.notna().sum())))
    rows.sort(key=lambda r: abs(r[1]), reverse=True)
    print(f"  {'feature':<25} {'corr':>8} {'n':>6}")
    for f, c, n in rows:
        marker = "  ←" if abs(c) >= 0.20 else ""
        print(f"  {f:<25} {c:>8.3f} {n:>6}{marker}")
    print()


# =============================================================================
# MAIN
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyse losing trades from a backtest CSV.")
    p.add_argument("--in", dest="in_path", type=str, default=str(DEFAULT_IN),
                   help=f"Backtest CSV path (default {DEFAULT_IN}).")
    p.add_argument("--losers-only", action="store_true",
                   help="Print the rows themselves (filtered to losers) instead of the breakdown.")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    in_path = Path(args.in_path)
    if not in_path.exists():
        print(f"ERROR: not found: {in_path}", flush=True)
        return 2

    df = pd.read_csv(in_path)
    df = classify(df)

    n = len(df)
    if n == 0:
        print("No complete rows to analyse.")
        return 0

    if args.losers_only:
        losers = df[df["is_loser"]].sort_values("realized_return_pct")
        cols = [
            "asof", "outcome", "bars_to_resolution", "realized_return_pct",
            "confidence", "atr_ratio_pct", "return_3m_pct",
            "pct_below_52w_high", "news_count",
        ]
        print(losers[cols].to_string(index=False))
        print(f"\nTotal losers: {len(losers)} / {n}")
        return 0

    # Header
    baseline_loss_rate = df["is_loser"].mean() * 100
    avg_return = df["realized_return_pct"].mean()
    print("\n═══ ATW LOSS ANALYSIS ═══\n")
    print(f"  Source            : {in_path}")
    print(f"  Total trades      : {n}")
    print(f"  Loss baseline     : {baseline_loss_rate:.2f}% ({int(df['is_loser'].sum())} losers)")
    print(f"  Avg return        : {avg_return:+.2f}%")
    print()

    # Breakdowns
    print_breakdown(df, "confidence",      "By CONFIDENCE",      baseline_loss_rate)
    print_breakdown(df, "atr_regime",      "By ATR REGIME (volatility)", baseline_loss_rate)
    print_breakdown(df, "momentum_regime", "By MOMENTUM (3-month return)", baseline_loss_rate)
    print_breakdown(df, "range_position",  "By RANGE POSITION (vs 52w high)", baseline_loss_rate)
    print_breakdown(df, "news_regime",     "By NEWS CONTEXT", baseline_loss_rate)
    print_breakdown(df, "rr_bucket",       "By RISK/REWARD", baseline_loss_rate)
    print_breakdown(df, "year",            "By YEAR", baseline_loss_rate)
    print_breakdown(df, "quarter",         "By QUARTER", baseline_loss_rate)

    correlation_report(df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
