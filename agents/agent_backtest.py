"""ATW-BACKTEST — historical replay of the 4-week trading prediction.

Walks every trading day in a date range, rebuilds the MarketSnapshot from data
ONLY up to that date (no lookahead), runs `compute_trading_prediction()`, then
looks 20 trading days forward to resolve the outcome (target hit, stop hit,
expired). Logs each row to CSV and prints aggregate metrics.

  python agents/agent_backtest.py
  python agents/agent_backtest.py --start 2025-09-01 --end 2025-10-01
  python agents/agent_backtest.py --append --quiet

Investment (12-month) backtest is intentionally out of scope: we have only
current fundamentals/valuations, no historical snapshots, so a 12-month replay
would leak future information.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from agent_analyse import (
    DATA_DIR,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_NEWS_SCORE,
    DEFAULT_NEWS_CAP,
    DEFAULT_NEWS_SUMMARY_CHARS,
    FILTER_COOLDOWN_BARS,
    MACRO_CSV,
    MARKET_CSV,
    NEWS_CSV,
    ATR_WINDOW,
    TRADING_HORIZON_DAYS,
    MarketSnapshot,
    NewsRow,
    _last_valid,
    _macro_delta_pct,
    _pct_change,
    _scrub_macro_band,
    _truncate,
    _wilder_atr,
    apply_strategy_filters,
    compute_trading_prediction,
)


# =============================================================================
# CONFIG
# =============================================================================

DEFAULT_OUT_CSV = DATA_DIR / "backtest_trading_results.csv"
DEFAULT_SIM_OUT_CSV = DATA_DIR / "backtest_simulation_trades.csv"
DEFAULT_INITIAL_CAPITAL_MAD = 10_000.0
# Need at least 4 weeks of warmup so the 4w high/low and ATR are meaningful.
MIN_WARMUP_BARS = max(ATR_WINDOW + 1, 20)


# =============================================================================
# HISTORICAL SLICING — no-lookahead snapshots
# =============================================================================

def slice_market_at(df: pd.DataFrame, asof: pd.Timestamp) -> MarketSnapshot | None:
    """MarketSnapshot using only rows with Séance <= asof."""
    sub = df[df["Séance"] <= asof]
    if len(sub) < MIN_WARMUP_BARS:
        return None
    close = sub["Dernier Cours"]
    high = sub["+haut du jour"]
    low = sub["+bas du jour"]
    last_252 = close.tail(252)
    last_20 = close.tail(20)
    return MarketSnapshot(
        as_of=sub["Séance"].iloc[-1].date().isoformat(),
        last_close=float(close.iloc[-1]),
        market_cap=float(sub["Capitalisation"].iloc[-1]),
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


_MACRO_FIELDS = (
    "brent_usd", "eur_mad", "usd_mad", "masi_close",
    "vix", "macro_momentum", "global_risk_flag",
)
_MACRO_DELTA_FIELDS = ("brent_usd", "eur_mad", "masi_close")


def slice_macro_at(df: pd.DataFrame, asof: pd.Timestamp) -> dict:
    """Macro values + 90d deltas using only rows with date <= asof."""
    sub = df[df["date"] <= asof]
    out: dict = {f: None for f in _MACRO_FIELDS}
    out.update({f"{f}_90d_delta_pct": None for f in _MACRO_DELTA_FIELDS})
    if sub.empty:
        return out
    for f in _MACRO_FIELDS:
        if f in sub.columns:
            out[f] = _last_valid(sub[f])
    for f in _MACRO_DELTA_FIELDS:
        if f in sub.columns:
            out[f"{f}_90d_delta_pct"] = _macro_delta_pct(sub[f], 90)
    return out


def slice_news_at(
    df: pd.DataFrame,
    asof: pd.Timestamp,
    lookback_days: int,
    min_score: int,
    cap: int = DEFAULT_NEWS_CAP,
    summary_chars: int = DEFAULT_NEWS_SUMMARY_CHARS,
) -> list[NewsRow]:
    cutoff = asof - pd.Timedelta(days=lookback_days)
    sub = df[(df["date"] >= cutoff) & (df["date"] <= asof) & (df["signal_score"] >= min_score)]
    sub = sub.sort_values("signal_score", ascending=False).head(cap)
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
        for _, r in sub.iterrows()
    ]


# =============================================================================
# OUTCOME RESOLUTION — first-touch wins, stop wins ties
# =============================================================================

@dataclass
class BacktestOutcome:
    outcome: str  # TARGET | STOP | EXPIRED | INCOMPLETE
    bars_to_resolution: int  # 1..horizon, or -1 if INCOMPLETE
    realized_return_pct: float | None
    max_runup_pct: float | None
    max_drawdown_pct: float | None


def resolve_outcome(
    future_df: pd.DataFrame,
    last_close: float,
    target: float,
    stop: float,
    horizon_days: int = TRADING_HORIZON_DAYS,
) -> BacktestOutcome:
    if future_df.empty or len(future_df) < horizon_days:
        return BacktestOutcome("INCOMPLETE", -1, None, None, None)

    window = future_df.head(horizon_days)
    highs = window["+haut du jour"].astype(float)
    lows = window["+bas du jour"].astype(float)
    closes = window["Dernier Cours"].astype(float)

    if last_close <= 0:
        return BacktestOutcome("INCOMPLETE", -1, None, None, None)

    max_runup = (highs.max() - last_close) / last_close * 100
    max_drawdown = (lows.min() - last_close) / last_close * 100

    for i in range(horizon_days):
        h = float(highs.iloc[i])
        l = float(lows.iloc[i])
        # Same-day touch: stop wins (conservative).
        if l <= stop:
            ret = (stop - last_close) / last_close * 100
            return BacktestOutcome("STOP", i + 1, round(ret, 2), round(max_runup, 2), round(max_drawdown, 2))
        if h >= target:
            ret = (target - last_close) / last_close * 100
            return BacktestOutcome("TARGET", i + 1, round(ret, 2), round(max_runup, 2), round(max_drawdown, 2))

    final_close = float(closes.iloc[-1])
    ret = (final_close - last_close) / last_close * 100
    return BacktestOutcome("EXPIRED", horizon_days, round(ret, 2), round(max_runup, 2), round(max_drawdown, 2))


# =============================================================================
# WALK-FORWARD LOOP
# =============================================================================

@dataclass
class BacktestRow:
    asof: str
    last_close: float
    entry_low: float
    entry_high: float
    target: float
    stop: float
    expected_return_pct: float
    risk_reward_ratio: float
    atr_mad: float
    confidence: str
    # Market context at entry — used by analyze_losses.py
    return_1m_pct: float | None
    return_3m_pct: float | None
    return_6m_pct: float | None
    pct_below_52w_high: float | None
    atr_ratio_pct: float
    news_count: int
    avg_news_score: float
    # Macro context at entry (no lookahead — sliced to date <= asof)
    brent_usd: float | None
    brent_90d_delta_pct: float | None
    eur_mad: float | None
    eur_mad_90d_delta_pct: float | None
    usd_mad: float | None
    masi_close: float | None
    masi_90d_delta_pct: float | None
    vix: float | None
    macro_momentum: float | None
    global_risk_flag: float | None
    # Outcome
    outcome: str
    bars_to_resolution: int
    realized_return_pct: float | None
    max_runup_pct: float | None
    max_drawdown_pct: float | None
    skip_reason: str | None


def _load_market(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["Séance"]).sort_values("Séance").reset_index(drop=True)


def _load_macro(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    return _scrub_macro_band(df)


def _load_news(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    # Strip tz so we can compare with naive Séance from market CSV.
    df["date"] = df["date"].dt.tz_convert(None)
    return df


def run_backtest(
    market_df: pd.DataFrame,
    news_df: pd.DataFrame,
    macro_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    lookback_days: int,
    min_news_score: int,
    enabled_filters: tuple[str, ...] = (),
) -> list[BacktestRow]:
    rows: list[BacktestRow] = []
    market_df = market_df.sort_values("Séance").reset_index(drop=True)

    # Resolved-outcome history is the input to filter_cooldown.
    # We only feed it outcomes from signals whose end_idx < current_idx.
    pending: list[tuple[int, str]] = []  # (end_idx, outcome) for resolved-but-not-yet-flushed
    resolved_history: list[str] = []
    cooldown_until_idx = -1

    for idx, market_row in market_df.iterrows():
        asof = market_row["Séance"]
        if asof < start or asof > end:
            continue

        # Flush pending outcomes that have now resolved (end_idx < idx).
        still_pending: list[tuple[int, str]] = []
        for end_idx, out in pending:
            if end_idx < idx:
                resolved_history.append(out)
            else:
                still_pending.append((end_idx, out))
        pending = still_pending

        snap = slice_market_at(market_df, asof)
        if snap is None:
            continue

        news = slice_news_at(news_df, asof, lookback_days, min_news_score)
        pred = compute_trading_prediction(snap, news)

        pct_below_52w_high = (
            round((snap.high_52w - snap.last_close) / snap.high_52w * 100, 2)
            if snap.high_52w > 0 else None
        )
        atr_ratio_pct = (
            round(snap.atr_14d / snap.last_close * 100, 3)
            if snap.last_close > 0 else 0.0
        )
        avg_news_score = (
            round(sum(n.signal_score for n in news) / len(news), 2)
            if news else 0.0
        )
        macro = slice_macro_at(macro_df, asof)

        # ---- FILTER GATE -------------------------------------------------
        skip_reason: str | None = None
        if enabled_filters:
            # Sticky cooldown: once triggered, keep firing for FILTER_COOLDOWN_BARS.
            if "cooldown" in enabled_filters and idx <= cooldown_until_idx:
                skip_reason = f"COOLDOWN (active until idx {cooldown_until_idx})"
            else:
                skip_reason = apply_strategy_filters(
                    snap, recent_outcomes=resolved_history, enabled=enabled_filters
                )
                if skip_reason and skip_reason.startswith("COOLDOWN"):
                    cooldown_until_idx = idx + FILTER_COOLDOWN_BARS

        if skip_reason is not None:
            rows.append(BacktestRow(
                asof=asof.date().isoformat(),
                last_close=snap.last_close,
                entry_low=pred.entry_zone_low_mad,
                entry_high=pred.entry_zone_high_mad,
                target=pred.target_price_mad,
                stop=pred.stop_loss_mad,
                expected_return_pct=pred.expected_return_pct,
                risk_reward_ratio=pred.risk_reward_ratio,
                atr_mad=pred.atr_mad,
                confidence=pred.confidence,
                return_1m_pct=round(snap.return_1m_pct, 2) if snap.return_1m_pct is not None else None,
                return_3m_pct=round(snap.return_3m_pct, 2) if snap.return_3m_pct is not None else None,
                return_6m_pct=round(snap.return_6m_pct, 2) if snap.return_6m_pct is not None else None,
                pct_below_52w_high=pct_below_52w_high,
                atr_ratio_pct=atr_ratio_pct,
                news_count=len(news),
                avg_news_score=avg_news_score,
                brent_usd=macro["brent_usd"],
                brent_90d_delta_pct=macro["brent_usd_90d_delta_pct"],
                eur_mad=macro["eur_mad"],
                eur_mad_90d_delta_pct=macro["eur_mad_90d_delta_pct"],
                usd_mad=macro["usd_mad"],
                masi_close=macro["masi_close"],
                masi_90d_delta_pct=macro["masi_close_90d_delta_pct"],
                vix=macro["vix"],
                macro_momentum=macro["macro_momentum"],
                global_risk_flag=macro["global_risk_flag"],
                outcome="SKIPPED",
                bars_to_resolution=0,
                realized_return_pct=None,
                max_runup_pct=None,
                max_drawdown_pct=None,
                skip_reason=skip_reason,
            ))
            continue
        # ------------------------------------------------------------------

        future = market_df[market_df["Séance"] > asof]
        outcome = resolve_outcome(future, snap.last_close, pred.target_price_mad, pred.stop_loss_mad)

        # Track for the cooldown filter (only resolved trades count).
        if outcome.outcome != "INCOMPLETE":
            end_idx = idx + max(outcome.bars_to_resolution, 1)
            pending.append((end_idx, outcome.outcome))

        rows.append(BacktestRow(
            asof=asof.date().isoformat(),
            last_close=snap.last_close,
            entry_low=pred.entry_zone_low_mad,
            entry_high=pred.entry_zone_high_mad,
            target=pred.target_price_mad,
            stop=pred.stop_loss_mad,
            expected_return_pct=pred.expected_return_pct,
            risk_reward_ratio=pred.risk_reward_ratio,
            atr_mad=pred.atr_mad,
            confidence=pred.confidence,
            return_1m_pct=round(snap.return_1m_pct, 2) if snap.return_1m_pct is not None else None,
            return_3m_pct=round(snap.return_3m_pct, 2) if snap.return_3m_pct is not None else None,
            return_6m_pct=round(snap.return_6m_pct, 2) if snap.return_6m_pct is not None else None,
            pct_below_52w_high=pct_below_52w_high,
            atr_ratio_pct=atr_ratio_pct,
            news_count=len(news),
            avg_news_score=avg_news_score,
            brent_usd=macro["brent_usd"],
            brent_90d_delta_pct=macro["brent_usd_90d_delta_pct"],
            eur_mad=macro["eur_mad"],
            eur_mad_90d_delta_pct=macro["eur_mad_90d_delta_pct"],
            usd_mad=macro["usd_mad"],
            masi_close=macro["masi_close"],
            masi_90d_delta_pct=macro["masi_close_90d_delta_pct"],
            vix=macro["vix"],
            macro_momentum=macro["macro_momentum"],
            global_risk_flag=macro["global_risk_flag"],
            outcome=outcome.outcome,
            bars_to_resolution=outcome.bars_to_resolution,
            realized_return_pct=outcome.realized_return_pct,
            max_runup_pct=outcome.max_runup_pct,
            max_drawdown_pct=outcome.max_drawdown_pct,
            skip_reason=None,
        ))

    return rows


# =============================================================================
# METRICS
# =============================================================================

def _safe_avg(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 2) if xs else None


def aggregate(rows: list[BacktestRow]) -> dict:
    skipped = [r for r in rows if r.outcome == "SKIPPED"]
    complete = [r for r in rows if r.outcome not in ("INCOMPLETE", "SKIPPED")]
    n = len(complete)
    skip_breakdown: dict[str, int] = {}
    for r in skipped:
        if not r.skip_reason:
            continue
        # Group by leading tag (e.g., "BEAR_REGIME (...)" → "BEAR_REGIME").
        tag = r.skip_reason.split(" ")[0]
        skip_breakdown[tag] = skip_breakdown.get(tag, 0) + 1

    if n == 0:
        return {
            "n_total": len(rows),
            "n_complete": 0,
            "n_incomplete": sum(1 for r in rows if r.outcome == "INCOMPLETE"),
            "n_skipped": len(skipped),
            "skip_breakdown": skip_breakdown,
        }

    targets = [r for r in complete if r.outcome == "TARGET"]
    stops = [r for r in complete if r.outcome == "STOP"]
    expired = [r for r in complete if r.outcome == "EXPIRED"]

    returns = [r.realized_return_pct for r in complete if r.realized_return_pct is not None]
    winners = [x for x in returns if x > 0]
    losers = [x for x in returns if x <= 0]

    win_rate = len(winners) / n * 100 if n else 0.0
    loss_rate = len(losers) / n * 100 if n else 0.0
    avg_winner = _safe_avg(winners) or 0.0
    avg_loser = _safe_avg(losers) or 0.0
    expectancy = (win_rate / 100 * avg_winner) + (loss_rate / 100 * avg_loser)

    n_incomplete = sum(1 for r in rows if r.outcome == "INCOMPLETE")
    out = {
        "n_total": len(rows),
        "n_complete": n,
        "n_incomplete": n_incomplete,
        "n_skipped": len(skipped),
        "skip_breakdown": skip_breakdown,
        "n_target_hit": len(targets),
        "n_stop_hit": len(stops),
        "n_expired": len(expired),
        "target_hit_rate_pct": round(len(targets) / n * 100, 2),
        "stop_hit_rate_pct": round(len(stops) / n * 100, 2),
        "expired_rate_pct": round(len(expired) / n * 100, 2),
        "avg_realized_return_pct": _safe_avg(returns),
        "median_realized_return_pct": round(pd.Series(returns).median(), 2) if returns else None,
        "avg_bars_to_target": _safe_avg([r.bars_to_resolution for r in targets]),
        "avg_bars_to_stop": _safe_avg([r.bars_to_resolution for r in stops]),
        "win_rate_pct": round(win_rate, 2),
        "avg_winner_pct": avg_winner,
        "avg_loser_pct": avg_loser,
        "expectancy_pct": round(expectancy, 2),
    }

    by_conf: dict[str, dict] = {}
    for level in ("LOW", "MEDIUM", "HIGH"):
        sub = [r for r in complete if r.confidence == level]
        if not sub:
            continue
        sub_targets = sum(1 for r in sub if r.outcome == "TARGET")
        sub_stops = sum(1 for r in sub if r.outcome == "STOP")
        sub_returns = [r.realized_return_pct for r in sub if r.realized_return_pct is not None]
        by_conf[level] = {
            "n": len(sub),
            "target_rate_pct": round(sub_targets / len(sub) * 100, 2),
            "stop_rate_pct": round(sub_stops / len(sub) * 100, 2),
            "avg_return_pct": _safe_avg(sub_returns),
        }
    out["by_confidence"] = by_conf
    return out


# =============================================================================
# SIMULATION — sequential all-in portfolio walk (one trade at a time)
# =============================================================================

@dataclass
class SimulationTrade:
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    outcome: str
    bars_held: int
    return_pct: float
    confidence: str
    capital_before: float
    capital_after: float


@dataclass
class SimulationResult:
    initial_capital: float
    final_capital: float
    total_return_pct: float
    n_trades: int
    n_wins: int
    n_losses: int
    win_rate_pct: float
    peak_capital: float
    max_drawdown_pct: float
    trades: list[SimulationTrade]


def simulate_portfolio(
    rows: list[BacktestRow],
    market_df: pd.DataFrame,
    initial_capital: float,
) -> SimulationResult:
    """One-position-at-a-time, all-in sequential simulation.

    Entry on the signal close (`last_close`); exit at target / stop / expiry close.
    While a trade is open, all signals on intervening dates are skipped — you
    cannot stack overlapping positions on the same instrument.
    """
    market_dates = list(market_df["Séance"].dt.date.map(lambda d: d.isoformat()))
    date_index = {d: i for i, d in enumerate(market_dates)}

    completed = sorted(
        [r for r in rows if r.outcome not in ("INCOMPLETE", "SKIPPED")],
        key=lambda r: r.asof,
    )

    capital = initial_capital
    peak = initial_capital
    max_dd = 0.0
    trades: list[SimulationTrade] = []
    blocked_until_idx = -1

    for r in completed:
        idx = date_index.get(r.asof, -1)
        if idx < 0 or idx <= blocked_until_idx:
            continue
        if r.realized_return_pct is None or r.bars_to_resolution <= 0:
            continue

        entry_price = r.last_close
        ret_frac = r.realized_return_pct / 100.0
        capital_before = capital
        capital_after = capital_before * (1 + ret_frac)

        exit_idx = min(idx + r.bars_to_resolution, len(market_dates) - 1)
        exit_date = market_dates[exit_idx]

        if r.outcome == "TARGET":
            exit_price = r.target
        elif r.outcome == "STOP":
            exit_price = r.stop
        else:  # EXPIRED
            exit_price = entry_price * (1 + ret_frac)

        trades.append(SimulationTrade(
            entry_date=r.asof,
            entry_price=round(entry_price, 2),
            exit_date=exit_date,
            exit_price=round(exit_price, 2),
            outcome=r.outcome,
            bars_held=r.bars_to_resolution,
            return_pct=r.realized_return_pct,
            confidence=r.confidence,
            capital_before=round(capital_before, 2),
            capital_after=round(capital_after, 2),
        ))

        capital = capital_after
        peak = max(peak, capital)
        if peak > 0:
            dd = (capital - peak) / peak * 100
            max_dd = min(max_dd, dd)

        blocked_until_idx = exit_idx

    final = capital
    n = len(trades)
    wins = sum(1 for t in trades if t.return_pct > 0)
    losses = sum(1 for t in trades if t.return_pct <= 0)
    total_return_pct = (final - initial_capital) / initial_capital * 100 if initial_capital > 0 else 0.0

    return SimulationResult(
        initial_capital=round(initial_capital, 2),
        final_capital=round(final, 2),
        total_return_pct=round(total_return_pct, 2),
        n_trades=n,
        n_wins=wins,
        n_losses=losses,
        win_rate_pct=round(wins / n * 100, 2) if n else 0.0,
        peak_capital=round(peak, 2),
        max_drawdown_pct=round(max_dd, 2),
        trades=trades,
    )


# =============================================================================
# OUTPUT — CSV write, terminal print
# =============================================================================

def write_csv(rows: list[BacktestRow], path: Path, append: bool) -> None:
    df = pd.DataFrame([asdict(r) for r in rows])
    if append and path.exists():
        df.to_csv(path, mode="a", header=False, index=False, encoding="utf-8")
    else:
        df.to_csv(path, mode="w", header=True, index=False, encoding="utf-8")


def write_simulation_csv(sim: SimulationResult, path: Path) -> None:
    df = pd.DataFrame([asdict(t) for t in sim.trades])
    df.to_csv(path, mode="w", header=True, index=False, encoding="utf-8")


def _fmt_pct(v: float | None) -> str:
    return f"{v:+.2f}%" if v is not None else "n/a"


def print_summary(metrics: dict, out_path: Path) -> None:
    print("\n═══ ATW BACKTEST — TRADING (4 semaines) ═══\n")
    n = metrics.get("n_complete", 0)
    if n == 0:
        print("  (no complete backtest rows — all dates incomplete)")
        print(f"  n_total={metrics.get('n_total', 0)}  incomplete={metrics.get('n_incomplete', 0)}\n")
        return

    n_skipped = metrics.get("n_skipped", 0)
    print(f"  Period rows           : {metrics['n_total']} (complete {n}, skipped {n_skipped}, incomplete {metrics['n_incomplete']})")
    skip_breakdown = metrics.get("skip_breakdown") or {}
    if skip_breakdown:
        parts = ", ".join(f"{tag}={cnt}" for tag, cnt in sorted(skip_breakdown.items()))
        print(f"  Skip reasons          : {parts}")
    print(f"  Target hit            : {metrics['n_target_hit']} ({metrics['target_hit_rate_pct']:.2f}%)")
    print(f"  Stop hit              : {metrics['n_stop_hit']} ({metrics['stop_hit_rate_pct']:.2f}%)")
    print(f"  Expired (no touch)    : {metrics['n_expired']} ({metrics['expired_rate_pct']:.2f}%)")
    print()
    print(f"  Win rate              : {metrics['win_rate_pct']:.2f}%")
    print(f"  Avg winner            : {_fmt_pct(metrics['avg_winner_pct'])}")
    print(f"  Avg loser             : {_fmt_pct(metrics['avg_loser_pct'])}")
    print(f"  Expectancy / trade    : {_fmt_pct(metrics['expectancy_pct'])}")
    print(f"  Avg realized return   : {_fmt_pct(metrics['avg_realized_return_pct'])}   (median {_fmt_pct(metrics['median_realized_return_pct'])})")
    print()
    avg_target = metrics.get("avg_bars_to_target")
    avg_stop = metrics.get("avg_bars_to_stop")
    print(f"  Avg bars to target    : {avg_target if avg_target is not None else 'n/a'}")
    print(f"  Avg bars to stop      : {avg_stop if avg_stop is not None else 'n/a'}")
    print()

    by_conf = metrics.get("by_confidence") or {}
    if by_conf:
        print("  Confidence calibration:")
        for level in ("HIGH", "MEDIUM", "LOW"):
            d = by_conf.get(level)
            if not d:
                continue
            print(
                f"    {level:6}  n={d['n']:4}  target={d['target_rate_pct']:5.2f}%  "
                f"stop={d['stop_rate_pct']:5.2f}%  avg_ret={_fmt_pct(d['avg_return_pct'])}"
            )
    print()
    print(f"  Per-row CSV: {out_path}\n")


def print_simulation(sim: SimulationResult, out_path: Path) -> None:
    print("═══ PORTFOLIO SIMULATION — sequential, all-in, 1 position max ═══\n")
    print(f"  Initial capital   : {sim.initial_capital:>10,.2f} MAD")
    print(f"  Final capital     : {sim.final_capital:>10,.2f} MAD")
    print(f"  Total return      : {sim.total_return_pct:+.2f}%")
    print(f"  Peak capital      : {sim.peak_capital:>10,.2f} MAD")
    print(f"  Max drawdown      : {sim.max_drawdown_pct:.2f}%")
    print()
    print(f"  Trades executed   : {sim.n_trades}")
    print(f"  Wins / Losses     : {sim.n_wins} / {sim.n_losses}  (win rate {sim.win_rate_pct:.2f}%)")
    if sim.trades:
        first = sim.trades[0]
        last = sim.trades[-1]
        print(f"  First trade       : {first.entry_date} → {first.exit_date}  ({first.outcome})")
        print(f"  Last trade        : {last.entry_date} → {last.exit_date}  ({last.outcome})")
    print()
    print(f"  Trade ledger CSV  : {out_path}\n")


# =============================================================================
# CLI / MAIN
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ATW backtest — replay 4-week trading predictions across history.")
    p.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (default: first eligible after warmup).")
    p.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: last date with full 20-bar future).")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                   help=f"News lookback window (default {DEFAULT_LOOKBACK_DAYS}).")
    p.add_argument("--min-news-score", type=int, default=DEFAULT_MIN_NEWS_SCORE,
                   help=f"Minimum news signal_score (default {DEFAULT_MIN_NEWS_SCORE}).")
    p.add_argument("--out", type=str, default=str(DEFAULT_OUT_CSV),
                   help=f"Output CSV path (default {DEFAULT_OUT_CSV}).")
    p.add_argument("--append", action="store_true", help="Append to existing CSV instead of overwrite.")
    p.add_argument("--quiet", action="store_true", help="Skip terminal summary block.")
    p.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL_MAD,
                   help=f"Starting capital in MAD for the portfolio simulation (default {DEFAULT_INITIAL_CAPITAL_MAD:.0f}).")
    p.add_argument("--simulation-out", type=str, default=str(DEFAULT_SIM_OUT_CSV),
                   help=f"Trade ledger CSV (default {DEFAULT_SIM_OUT_CSV}).")
    p.add_argument("--no-simulation", action="store_true",
                   help="Skip the portfolio simulation step.")
    p.add_argument("--filters", type=str, default="",
                   help="Comma-separated filter names to enable: bear,top,cooldown. "
                        "Use 'none' or '' (default) for the baseline strategy. "
                        "Use 'all' for bear,top,cooldown.")
    return p.parse_args()


def _parse_filters(arg: str) -> tuple[str, ...]:
    s = (arg or "").strip().lower()
    if s in ("", "none"):
        return ()
    if s == "all":
        return ("bear", "top", "cooldown")
    valid = {"bear", "top", "cooldown"}
    parts = tuple(p.strip() for p in s.split(",") if p.strip())
    bad = [p for p in parts if p not in valid]
    if bad:
        raise SystemExit(f"unknown filter(s): {', '.join(bad)}; valid: {sorted(valid)}")
    return parts


def _resolve_window(
    market_df: pd.DataFrame, start_arg: str | None, end_arg: str | None
) -> tuple[pd.Timestamp, pd.Timestamp]:
    n = len(market_df)
    first_idx = min(MIN_WARMUP_BARS, n - 1)
    last_idx = max(0, n - 1 - TRADING_HORIZON_DAYS)
    first_eligible = market_df["Séance"].iloc[first_idx]
    last_eligible = market_df["Séance"].iloc[last_idx]
    start = pd.Timestamp(start_arg) if start_arg else first_eligible
    end = pd.Timestamp(end_arg) if end_arg else last_eligible
    return start, end


def main() -> int:
    args = _parse_args()

    print(f"[load] reading {MARKET_CSV}", flush=True)
    market_df = _load_market(MARKET_CSV)
    print(f"[load] reading {NEWS_CSV}", flush=True)
    news_df = _load_news(NEWS_CSV)
    print(f"[load] reading {MACRO_CSV}", flush=True)
    macro_df = _load_macro(MACRO_CSV)

    start, end = _resolve_window(market_df, args.start, args.end)
    enabled_filters = _parse_filters(args.filters)
    filter_label = ",".join(enabled_filters) if enabled_filters else "none"
    print(f"[run]  backtest window: {start.date()} -> {end.date()}  filters={filter_label}", flush=True)

    rows = run_backtest(
        market_df, news_df, macro_df, start, end,
        args.lookback_days, args.min_news_score,
        enabled_filters=enabled_filters,
    )
    print(f"[run]  produced {len(rows)} rows", flush=True)

    out_path = Path(args.out)
    write_csv(rows, out_path, append=args.append)
    print(f"[save] wrote {out_path}", flush=True)

    if not args.quiet:
        metrics = aggregate(rows)
        print_summary(metrics, out_path)

    if not args.no_simulation:
        sim = simulate_portfolio(rows, market_df, args.initial_capital)
        sim_path = Path(args.simulation_out)
        write_simulation_csv(sim, sim_path)
        print(f"[save] wrote {sim_path}", flush=True)
        if not args.quiet:
            print_simulation(sim, sim_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
