"""One-off backfill: push existing atw_realtime_state.json to Postgres.

- snapshot_history[]  -> bourse_intraday  (dedup by snapshot_ts)
- technicals{}        -> technicals_snapshot (one row, computed_at = last_snapshot_ts)

Idempotent: safe to re-run.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from database import AtwDatabase

STATE_FILE = ROOT / "data" / "atw_realtime_state.json"
TICKER = "ATW"


def main() -> int:
    if not STATE_FILE.exists():
        print(f"Missing state file: {STATE_FILE}")
        return 1

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    history = state.get("snapshot_history", [])
    rows = []
    for s in history:
        rows.append({
            "snapshot_ts": s.get("timestamp"),
            "cotation_ts": s.get("cotation"),
            "ticker": TICKER,
            "market_status": s.get("market_status"),
            "last_price": s.get("last_price"),
            "open": s.get("open"),
            "high": s.get("high"),
            "low": s.get("low"),
            "prev_close": s.get("prev_close"),
            "variation_pct": s.get("variation_pct"),
            "shares_traded": s.get("shares_traded"),
            "value_traded_mad": s.get("value_traded_mad"),
            "num_trades": s.get("num_trades"),
            "market_cap": s.get("market_cap"),
        })

    technicals = state.get("technicals")
    computed_at = state.get("last_snapshot_ts")

    with AtwDatabase() as db:
        n_intraday = db.save_intraday(rows, ticker=TICKER)
        print(f"bourse_intraday: {n_intraday} new rows (of {len(rows)} in history)")
        if technicals and computed_at:
            n_tech = db.save_technicals(technicals, symbol=TICKER, computed_at=computed_at)
            print(f"technicals_snapshot: {n_tech} new row (computed_at={computed_at})")
        else:
            print("technicals_snapshot: no payload to backfill")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
