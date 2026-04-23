"""Single entry-point for all Postgres reads/writes used by scrapers + news crawlers."""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

_ROOT = Path(__file__).resolve().parents[1]

NEWS_COLUMNS = [
    "date", "ticker", "title", "source", "url", "full_content",
    "query_source", "signal_score", "is_atw_core", "scraping_date",
]

INTRADAY_COLUMNS = [
    "snapshot_ts", "cotation_ts", "ticker", "market_status", "last_price",
    "open", "high", "low", "prev_close", "variation_pct",
    "shares_traded", "value_traded_mad", "num_trades", "market_cap",
]

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


class AtwDatabase:
    def __init__(self, env_path: Path | None = None):
        self.env_path = env_path or (_ROOT / ".env")
        self.engine: Engine | None = None

    # ---------- lifecycle ----------
    def connect(self) -> None:
        if self.engine is not None:
            return
        load_dotenv(self.env_path)
        user = os.environ["POSTGRES_USER"]
        pw = os.environ["POSTGRES_PASSWORD"]
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ["POSTGRES_DB"]
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        )

    def close(self) -> None:
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _require(self) -> Engine:
        if self.engine is None:
            self.connect()
        assert self.engine is not None
        return self.engine

    # ---------- generic upsert helper ----------
    def _upsert_ignore(self, table: str, df: pd.DataFrame, conflict_cols: list[str]) -> int:
        if df is None or df.empty:
            return 0
        df = df.where(pd.notnull(df), None)
        records = df.to_dict(orient="records")
        engine = self._require()
        with engine.begin() as conn:
            md = MetaData()
            tbl = Table(table, md, autoload_with=conn)
            allowed = {c.name for c in tbl.columns}
            clean = [{k: v for k, v in r.items() if k in allowed} for r in records]
            stmt = insert(tbl).values(clean).on_conflict_do_nothing(
                index_elements=conflict_cols
            )
            conn.execute(stmt)
        return len(records)

    # ---------- SAVE ----------
    def save_bourse(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        df = df.rename(columns=BOURSE_RENAME).copy()
        if "seance" in df.columns:
            df["seance"] = pd.to_datetime(df["seance"], errors="coerce").dt.date
            df = df.dropna(subset=["seance"])
        return self._upsert_ignore("bourse_daily", df, ["seance"])

    def save_macro(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        df = df.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            df = df.dropna(subset=["date"])
        return self._upsert_ignore("macro_morocco", df, ["date"])

    def save_news(self, rows: Iterable[dict] | pd.DataFrame) -> tuple[int, int]:
        """Returns (inserted, enriched).

        - New url  -> INSERT including full_content if present.
        - Existing url with empty full_content and incoming non-empty -> UPDATE full_content.
        - Existing url already populated -> no-op.
        """
        if isinstance(rows, pd.DataFrame):
            records = rows.where(pd.notnull(rows), None).to_dict(orient="records")
        else:
            records = [dict(r) for r in rows]
        records = [r for r in records if r.get("url")]
        if not records:
            return (0, 0)

        for r in records:
            for k in NEWS_COLUMNS:
                r.setdefault(k, None)
            # normalize empty strings in full_content so SQL "= ''" check works uniformly
            if r.get("full_content") in ("", None):
                r["full_content"] = None
            # scraping_date default = now (UTC)
            if not r.get("scraping_date"):
                r["scraping_date"] = datetime.utcnow().isoformat()

        engine = self._require()
        with engine.begin() as conn:
            before_cnt = conn.execute(text("SELECT COUNT(*) FROM news")).scalar() or 0
            before_with_content = conn.execute(
                text("SELECT COUNT(*) FROM news WHERE full_content IS NOT NULL AND full_content <> ''")
            ).scalar() or 0

            conn.execute(
                text(
                    """
                    INSERT INTO news (
                        date, ticker, title, source, url, full_content,
                        query_source, signal_score, is_atw_core, scraping_date
                    ) VALUES (
                        :date, :ticker, :title, :source, :url, :full_content,
                        :query_source, :signal_score, :is_atw_core, :scraping_date
                    )
                    ON CONFLICT (url) DO UPDATE
                    SET full_content  = EXCLUDED.full_content,
                        scraping_date = EXCLUDED.scraping_date,
                        title         = COALESCE(NULLIF(news.title, ''), EXCLUDED.title),
                        date          = COALESCE(news.date, EXCLUDED.date)
                    WHERE (news.full_content IS NULL OR news.full_content = '')
                      AND EXCLUDED.full_content IS NOT NULL
                      AND EXCLUDED.full_content <> ''
                    """
                ),
                records,
            )

            after_cnt = conn.execute(text("SELECT COUNT(*) FROM news")).scalar() or 0
            after_with_content = conn.execute(
                text("SELECT COUNT(*) FROM news WHERE full_content IS NOT NULL AND full_content <> ''")
            ).scalar() or 0

        inserted = after_cnt - before_cnt
        enriched = (after_with_content - before_with_content) - inserted
        return (inserted, max(enriched, 0))

    def save_fondamental(self, doc: dict) -> tuple[int, int]:
        if not doc:
            return (0, 0)
        symbol = doc.get("symbol", "ATW")
        ts = doc.get("scrape_timestamp")
        engine = self._require()
        yearly_rows: list[dict] = []
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
        return (1, len(yearly_rows))

    def save_intraday(self, snapshots: Iterable[dict], ticker: str = "ATW") -> int:
        """Upsert intraday snapshots. Keyed on snapshot_ts; duplicates ignored."""
        if snapshots is None:
            return 0
        if isinstance(snapshots, pd.DataFrame):
            records = snapshots.where(pd.notnull(snapshots), None).to_dict(orient="records")
        else:
            records = [dict(s) for s in snapshots]
        if not records:
            return 0

        clean: list[dict] = []
        for r in records:
            ts = r.get("snapshot_ts") or r.get("timestamp")
            if not ts:
                continue
            row = {k: None for k in INTRADAY_COLUMNS}
            row["snapshot_ts"] = ts
            row["cotation_ts"] = r.get("cotation_ts") or r.get("cotation")
            row["ticker"] = r.get("ticker") or ticker
            row["market_status"] = r.get("market_status")
            for k in (
                "last_price", "open", "high", "low", "prev_close", "variation_pct",
                "shares_traded", "value_traded_mad", "num_trades", "market_cap",
            ):
                row[k] = r.get(k)
            clean.append(row)
        if not clean:
            return 0

        engine = self._require()
        with engine.begin() as conn:
            before = conn.execute(text("SELECT COUNT(*) FROM bourse_intraday")).scalar() or 0
            conn.execute(
                text(
                    """
                    INSERT INTO bourse_intraday (
                        snapshot_ts, cotation_ts, ticker, market_status, last_price,
                        open, high, low, prev_close, variation_pct,
                        shares_traded, value_traded_mad, num_trades, market_cap
                    ) VALUES (
                        :snapshot_ts, :cotation_ts, :ticker, :market_status, :last_price,
                        :open, :high, :low, :prev_close, :variation_pct,
                        :shares_traded, :value_traded_mad, :num_trades, :market_cap
                    )
                    ON CONFLICT (snapshot_ts) DO NOTHING
                    """
                ),
                clean,
            )
            after = conn.execute(text("SELECT COUNT(*) FROM bourse_intraday")).scalar() or 0
        return after - before

    def save_technicals(self, doc: dict, symbol: str = "ATW",
                        computed_at: str | datetime | None = None) -> int:
        """Append a technicals computation as JSONB. Returns 1 if inserted, 0 if duplicate."""
        if not doc:
            return 0
        ts = computed_at or datetime.utcnow().isoformat()
        as_of = doc.get("as_of_date")
        engine = self._require()
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO technicals_snapshot (symbol, computed_at, as_of_date, payload)
                    VALUES (:symbol, :ts, :as_of, CAST(:payload AS JSONB))
                    ON CONFLICT (symbol, computed_at) DO NOTHING
                    """
                ),
                {
                    "symbol": symbol,
                    "ts": ts,
                    "as_of": as_of,
                    "payload": json.dumps(doc, default=str),
                },
            )
        return result.rowcount or 0

    # ---------- RETRIEVE ----------
    def get_bourse(self, start: date | None = None, end: date | None = None) -> pd.DataFrame:
        engine = self._require()
        q = "SELECT * FROM bourse_daily"
        clauses, params = [], {}
        if start:
            clauses.append("seance >= :start"); params["start"] = start
        if end:
            clauses.append("seance <= :end"); params["end"] = end
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY seance"
        return pd.read_sql(text(q), engine, params=params)

    def get_macro(self, start: date | None = None, end: date | None = None) -> pd.DataFrame:
        engine = self._require()
        q = "SELECT * FROM macro_morocco"
        clauses, params = [], {}
        if start:
            clauses.append("date >= :start"); params["start"] = start
        if end:
            clauses.append("date <= :end"); params["end"] = end
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY date"
        return pd.read_sql(text(q), engine, params=params)

    def get_news(self, ticker: str = "ATW", limit: int = 100, only_with_content: bool = False) -> pd.DataFrame:
        engine = self._require()
        clauses = ["(ticker = :ticker OR :ticker IS NULL)"]
        params: dict[str, Any] = {"ticker": ticker, "limit": limit}
        if only_with_content:
            clauses.append("full_content IS NOT NULL AND full_content <> ''")
        q = (
            "SELECT * FROM news WHERE " + " AND ".join(clauses)
            + " ORDER BY date DESC NULLS LAST LIMIT :limit"
        )
        return pd.read_sql(text(q), engine, params=params)

    def get_fondamental_latest(self, symbol: str = "ATW") -> dict:
        engine = self._require()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT payload FROM fondamental_snapshot
                    WHERE symbol = :s
                    ORDER BY scrape_timestamp DESC
                    LIMIT 1
                    """
                ),
                {"s": symbol},
            ).fetchone()
        return dict(row[0]) if row else {}

    def get_fondamental_yearly(self, symbol: str = "ATW") -> pd.DataFrame:
        engine = self._require()
        return pd.read_sql(
            text(
                "SELECT symbol, year, metric, value FROM fondamental_yearly "
                "WHERE symbol = :s ORDER BY year, metric"
            ),
            engine,
            params={"s": symbol},
        )

    def get_intraday(self, ticker: str = "ATW",
                     start: datetime | None = None,
                     end: datetime | None = None,
                     limit: int | None = None) -> pd.DataFrame:
        engine = self._require()
        clauses = ["ticker = :ticker"]
        params: dict[str, Any] = {"ticker": ticker}
        if start:
            clauses.append("snapshot_ts >= :start"); params["start"] = start
        if end:
            clauses.append("snapshot_ts <= :end"); params["end"] = end
        q = "SELECT * FROM bourse_intraday WHERE " + " AND ".join(clauses)
        q += " ORDER BY snapshot_ts DESC"
        if limit:
            q += " LIMIT :limit"; params["limit"] = limit
        return pd.read_sql(text(q), engine, params=params)

    def get_technicals_latest(self, symbol: str = "ATW") -> dict:
        engine = self._require()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT payload FROM technicals_snapshot
                    WHERE symbol = :s
                    ORDER BY computed_at DESC
                    LIMIT 1
                    """
                ),
                {"s": symbol},
            ).fetchone()
        return dict(row[0]) if row else {}

    def get_technicals_history(self, symbol: str = "ATW",
                               limit: int = 50) -> pd.DataFrame:
        engine = self._require()
        return pd.read_sql(
            text(
                """
                SELECT id, symbol, computed_at, as_of_date, payload
                FROM technicals_snapshot
                WHERE symbol = :s
                ORDER BY computed_at DESC
                LIMIT :limit
                """
            ),
            engine,
            params={"s": symbol, "limit": limit},
        )
