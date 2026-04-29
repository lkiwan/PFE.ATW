"""Load CSV/JSON files from data/ into the Postgres instance defined in .env."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

BOURSE_RENAME = {
    "Séance": "seance",
    "Instrument": "instrument",
    "Ticker": "ticker",
    "Ouverture": "ouverture",
    "Dernier Cours": "dernier_cours",
    "+haut du jour": "plus_haut",
    "+bas du jour": "plus_bas",
    "Nombre de titres échangés": "nb_titres",
    "Volume des échanges": "volume",
    "Nombre de transactions": "nb_transactions",
    "Capitalisation": "capitalisation",
}


def build_engine():
    load_dotenv(ROOT / ".env")
    user = os.environ["POSTGRES_USER"]
    pw = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")


def ensure_runtime_tables(engine):
    """Create newer tables on existing DBs that predate schema additions."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS bourse_orderbook (
                    snapshot_ts  TIMESTAMPTZ PRIMARY KEY,
                    ticker       TEXT NOT NULL DEFAULT 'ATW',
                    bid1_orders  NUMERIC,
                    bid2_orders  NUMERIC,
                    bid3_orders  NUMERIC,
                    bid4_orders  NUMERIC,
                    bid5_orders  NUMERIC,
                    bid1_qty     NUMERIC,
                    bid2_qty     NUMERIC,
                    bid3_qty     NUMERIC,
                    bid4_qty     NUMERIC,
                    bid5_qty     NUMERIC,
                    bid1_price   NUMERIC,
                    bid2_price   NUMERIC,
                    bid3_price   NUMERIC,
                    bid4_price   NUMERIC,
                    bid5_price   NUMERIC,
                    ask1_price   NUMERIC,
                    ask2_price   NUMERIC,
                    ask3_price   NUMERIC,
                    ask4_price   NUMERIC,
                    ask5_price   NUMERIC,
                    ask1_qty     NUMERIC,
                    ask2_qty     NUMERIC,
                    ask3_qty     NUMERIC,
                    ask4_qty     NUMERIC,
                    ask5_qty     NUMERIC,
                    ask1_orders  NUMERIC,
                    ask2_orders  NUMERIC,
                    ask3_orders  NUMERIC,
                    ask4_orders  NUMERIC,
                    ask5_orders  NUMERIC
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_orderbook_ticker_ts
                ON bourse_orderbook (ticker, snapshot_ts DESC)
                """
            )
        )


def upsert_df(engine, table: str, df: pd.DataFrame, conflict_cols: list[str]):
    if df.empty:
        print(f"  {table}: no rows")
        return
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        from sqlalchemy import MetaData, Table
        md = MetaData()
        tbl = Table(table, md, autoload_with=conn)
        stmt = insert(tbl).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
        conn.execute(stmt)
    print(f"  {table}: {len(records)} rows submitted (conflicts skipped)")


def load_bourse(engine):
    path = DATA / "ATW_bourse_casa_full.csv"
    if not path.exists():
        print(f"  skip bourse_daily — {path.name} not found")
        return
    df = pd.read_csv(path).rename(columns=BOURSE_RENAME)
    df["seance"] = pd.to_datetime(df["seance"]).dt.date
    upsert_df(engine, "bourse_daily", df, ["seance"])


def load_macro(engine):
    path = DATA / "ATW_macro_morocco.csv"
    if not path.exists():
        print(f"  skip macro_morocco — {path.name} not found")
        return
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    upsert_df(engine, "macro_morocco", df, ["date"])


def load_news(engine):
    path = DATA / "ATW_news.csv"
    if not path.exists():
        print(f"  skip news — {path.name} not found")
        return
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["scraping_date"] = pd.to_datetime(df["scraping_date"], errors="coerce", utc=True)
    df = df.dropna(subset=["url"])
    upsert_df(engine, "news", df, ["url"])


def load_orderbook(engine):
    paths = sorted(DATA.glob("ATW_orderbook_*.csv"))
    if not paths:
        print("  skip bourse_orderbook — no ATW_orderbook_*.csv files found")
        return

    frames = []
    for path in paths:
        df = pd.read_csv(path)
        if "timestamp" not in df.columns:
            print(f"  skip bourse_orderbook file — missing timestamp column: {path.name}")
            continue
        df = df.copy()
        df["snapshot_ts"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.drop(columns=["timestamp"])
        df["ticker"] = "ATW"
        df = df.dropna(subset=["snapshot_ts"])
        frames.append(df)

    if not frames:
        print("  skip bourse_orderbook — no usable rows")
        return

    merged = pd.concat(frames, ignore_index=True)
    cols = (
        ["snapshot_ts", "ticker"]
        + [f"bid{i}_orders" for i in range(1, 6)]
        + [f"bid{i}_qty" for i in range(1, 6)]
        + [f"bid{i}_price" for i in range(1, 6)]
        + [f"ask{i}_price" for i in range(1, 6)]
        + [f"ask{i}_qty" for i in range(1, 6)]
        + [f"ask{i}_orders" for i in range(1, 6)]
    )
    for c in cols:
        if c not in merged.columns:
            merged[c] = None
    merged = merged[cols]
    upsert_df(engine, "bourse_orderbook", merged, ["snapshot_ts"])


def load_fondamental(engine):
    path = DATA / "ATW_fondamental.json"
    if not path.exists():
        print(f"  skip fondamental — {path.name} not found")
        return
    doc = json.loads(path.read_text(encoding="utf-8"))
    symbol = doc.get("symbol", "ATW")
    ts = doc.get("scrape_timestamp")

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO fondamental_snapshot (symbol, scrape_timestamp, payload)
                VALUES (:symbol, :ts, CAST(:payload AS JSONB))
                ON CONFLICT (symbol, scrape_timestamp) DO NOTHING
                """
            ),
            {"symbol": symbol, "ts": ts, "payload": json.dumps(doc)},
        )

        yearly_rows = []
        for metric, mapping in doc.items():
            if not isinstance(mapping, dict):
                continue
            for year_str, value in mapping.items():
                try:
                    year = int(year_str)
                except (TypeError, ValueError):
                    continue
                yearly_rows.append(
                    {"symbol": symbol, "year": year, "metric": metric, "value": value}
                )

        if yearly_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO fondamental_yearly (symbol, year, metric, value)
                    VALUES (:symbol, :year, :metric, :value)
                    ON CONFLICT (symbol, year, metric) DO UPDATE
                    SET value = EXCLUDED.value
                    """
                ),
                yearly_rows,
            )
    print(f"  fondamental_snapshot: 1 row; fondamental_yearly: {len(yearly_rows)} rows")


def main():
    engine = build_engine()
    ensure_runtime_tables(engine)
    print("Loading data into Postgres...")
    load_bourse(engine)
    load_macro(engine)
    load_news(engine)
    load_orderbook(engine)
    load_fondamental(engine)

    with engine.connect() as conn:
        for t in (
            "bourse_daily",
            "macro_morocco",
            "news",
            "bourse_orderbook",
            "fondamental_snapshot",
            "fondamental_yearly",
        ):
            n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"  {t}: {n} rows in DB")


if __name__ == "__main__":
    main()
