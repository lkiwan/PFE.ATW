"""
ATW Realtime Scraper
====================
Intraday snapshot + end-of-day consolidation for Attijariwafa Bank (ATW).

Two data sources, each used for what it does best:

  **Medias24 JSON API** (live intraday — primary for `snapshot`)
    getStockInfo       live price, open/high/low, prev close, cumulative
                       shares & MAD volume, market cap, cotation timestamp,
                       variation %
    getTransactions    per-trade tick list → count yields num_trades
    getBidAsk          10-level order book

  **Casablanca Bourse JSONAPI** (official EOD — primary for `finalize`)
    instrument_history  official closing price, OHLCV, capitalisation, trades
                        (only published after session close)

ATW ISIN: MA0000012445
ATW Casablanca Bourse instrument_id: 511

Subcommands
-----------
  (no cmd)   Runs default flow without writing intraday/orderbook CSVs:
             `snapshot` only, and after market close it auto-runs `finalize`
             so data/ATW_bourse_casa_full.csv gets the daily EOD row.

  snapshot   Pull one real-time snapshot from Medias24; append rows to
             data/historical/ATW_bourse_casa_{YYYY-MM-DD}.csv
             (plus optional raw intraday/orderbook CSVs).

  finalize   Consolidate today's session → one EOD row.
             Casablanca Bourse official data is saved to:
               data/historical/ATW_bourse_casa_full.csv
             Fallback (Medias24-derived) rows are saved to:
               data/historical/ATW_bourse_casa_{YYYY-MM-DD}.csv

The caller handles scheduling (cron / systemd / loop). This script does NOT
loop. Expected external cadence ≤15 min during trading hours.

State file: data/atw_realtime_state.json
  - last_snapshot_ts, last_snapshot_* (cumulative counters for stall detection)
  - finalized_days[] → finalize() is idempotent
  - debounce: refuses a new snapshot < 60 s after the last one
  - stall detection: session closed + no change in cumulative counters → no
    network call, write cached values to keep the timeseries contiguous

No Selenium, no Chrome. Pure requests / cloudscraper.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import certifi

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

import numpy as np
import pandas as pd
import requests

try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

# --- Paths & constants -------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = _ROOT / "data"
STATE_DIR = _ROOT / "data" 

STATE_FILE = STATE_DIR / "atw_realtime_state.json"
EOD_CSV = DATA_DIR / "ATW_bourse_casa_full.csv"
INTRADAY_CSV = DATA_DIR / "ATW_intraday.csv"


TICKER = "ATW"
ATW_ISIN = "MA0000012445"
ATW_INSTRUMENT_ID = "511"
CASA_TZ = timezone(timedelta(hours=1))  # Africa/Casablanca (permanent UTC+1)

# Medias24 API (live intraday)
MEDIAS24_API_ROOT = "https://medias24.com/content/api"
MEDIAS24_REFERER = "https://medias24.com/leboursier/fiche-action?action=attijariwafa-bank"

# Casablanca Bourse JSONAPI (official EOD)
BOURSE_API_URL = "https://www.casablanca-bourse.com/api/proxy/fr/api/bourse_data/instrument_history"
BOURSE_REFERER = "https://www.casablanca-bourse.com/fr/live-market/instruments/ATW"

# Official daily schema required in ATW_bourse_casa_full.csv.
FULL_EOD_FIELDS = [
    "Séance", "Instrument", "Ticker",
    "Ouverture", "Dernier Cours", "+haut du jour", "+bas du jour",
    "Nombre de titres échangés", "Volume des échanges",
    "Nombre de transactions", "Capitalisation",
]

INTRADAY_FIELDS = [
    "timestamp", "cotation", "market_status",
    "last_price", "open", "high", "low", "prev_close",
    "variation_pct", "shares_traded", "value_traded_mad",
    "num_trades", "market_cap",
]

ORDERBOOK_FIELDS = (
    ["timestamp"]
    + [f"bid{i}_orders" for i in range(1, 6)]
    + [f"bid{i}_qty"    for i in range(1, 6)]
    + [f"bid{i}_price"  for i in range(1, 6)]
    + [f"ask{i}_price"  for i in range(1, 6)]
    + [f"ask{i}_qty"    for i in range(1, 6)]
    + [f"ask{i}_orders" for i in range(1, 6)]
)

SNAPSHOT_DEBOUNCE_SECONDS = 60
REQUEST_TIMEOUT = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("atw_realtime")


# --- Sessions ----------------------------------------------------------------

def _build_medias24_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Referer": MEDIAS24_REFERER,
    })
    return s


def _build_bourse_session():
    if not HAS_CLOUDSCRAPER:
        raise RuntimeError("cloudscraper is required: pip install cloudscraper")
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    s.headers.update({
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "Referer": BOURSE_REFERER,
    })
    return s


# --- Parsing helpers ---------------------------------------------------------

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "" or v == "-":
        return None
    try:
        return float(str(v).replace(",", "."))
    except ValueError:
        return None


def _to_int(v: Any) -> Optional[int]:
    f = _to_float(v)
    return int(f) if f is not None else None


def _parse_cotation(raw: Optional[str]) -> Optional[datetime]:
    """Medias24 returns cotation as 'DD/MM/YYYY X HH:MM' where X is a mojibake 'à'."""
    if not raw:
        return None
    import re
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})\D+(\d{1,2}):(\d{2})", raw)
    if not m:
        return None
    d, mo, y, hh, mm = m.groups()
    try:
        return datetime(int(y), int(mo), int(d), int(hh), int(mm), tzinfo=CASA_TZ)
    except ValueError:
        return None


def _classify_market_status(now_casa: datetime) -> str:
    t = now_casa.time()
    hhmm = t.hour * 60 + t.minute
    if 9 * 60 <= hhmm < 9 * 60 + 30:
        return "PRE_OPEN"
    if 9 * 60 + 30 <= hhmm < 15 * 60 + 30:
        return "OPEN"
    return "CLOSED"


def _is_after_market_close(now_casa: datetime) -> bool:
    t = now_casa.time()
    return (t.hour * 60 + t.minute) >= (15 * 60 + 30)


# --- Data models -------------------------------------------------------------

@dataclass
class Snapshot:
    timestamp: str                     # when WE captured it
    cotation: str                      # exchange-reported last-update time
    market_status: str
    last_price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_close: Optional[float] = None
    variation_pct: Optional[float] = None
    shares_traded: Optional[int] = None
    value_traded_mad: Optional[float] = None
    num_trades: Optional[int] = None
    market_cap: Optional[float] = None


@dataclass
class OrderBook:
    timestamp: str
    bids: List[Dict[str, float]] = field(default_factory=list)
    asks: List[Dict[str, float]] = field(default_factory=list)


# --- Fetchers: Medias24 (live intraday) -------------------------------------

def _medias24_api_get(session: requests.Session, method: str, **params) -> Any:
    params["method"] = method
    params.setdefault("ISIN", ATW_ISIN)
    params.setdefault("format", "json")
    params.setdefault("t", int(time.time() * 1000))
    r = session.get(MEDIAS24_API_ROOT, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    if not isinstance(payload, dict) or "result" not in payload:
        raise RuntimeError(f"{method}: unexpected response shape: {str(payload)[:200]}")
    return payload["result"]


def fetch_stock_info(session: requests.Session) -> Dict[str, Any]:
    return _medias24_api_get(session, "getStockInfo")


def fetch_transactions(session: requests.Session) -> List[Dict[str, Any]]:
    res = _medias24_api_get(session, "getTransactions")
    return res if isinstance(res, list) else []


def fetch_bid_ask(session: requests.Session) -> List[Dict[str, Any]]:
    res = _medias24_api_get(session, "getBidAsk")
    if isinstance(res, dict):
        return res.get("orderBook", []) or []
    return []


# --- Fetchers: Casablanca Bourse JSONAPI (official EOD) ---------------------

def fetch_bourse_eod(session, day: str) -> Optional[Dict[str, Any]]:
    """Fetch official EOD data for a specific date from Casablanca Bourse."""
    params = {
        "fields[instrument_history]": (
            "symbol,created,openingPrice,coursCourant,highPrice,lowPrice,"
            "cumulTitresEchanges,cumulVolumeEchange,totalTrades,"
            "capitalisation,closingPrice,staticReferencePrice"
        ),
        "include": "symbol",
        "sort[date-seance][path]": "created",
        "sort[date-seance][direction]": "DESC",
        "filter[filter-historique-instrument-emetteur][condition][path]": "symbol.meta.drupal_internal__target_id",
        "filter[filter-historique-instrument-emetteur][condition][operator]": "=",
        "filter[filter-historique-instrument-emetteur][condition][value]": ATW_INSTRUMENT_ID,
        "filter[instrument-history-class][condition][path]": "symbol.codeClasse.field_code",
        "filter[instrument-history-class][condition][value]": "1",
        "filter[instrument-history-class][condition][operator]": "=",
        "filter[published]": "1",
        "filter[filter-date-start-vh-select][condition][path]": "field_seance_date",
        "filter[filter-date-start-vh-select][condition][operator]": "=",
        "filter[filter-date-start-vh-select][condition][value]": day,
        "page[offset]": "0",
        "page[limit]": "1",
    }
    r = session.get(BOURSE_API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("data", [])
    if not items:
        return None
    return items[0].get("attributes", {})


# --- Builders ---------------------------------------------------------------

def build_snapshot(info: Dict[str, Any], transactions: List[Dict[str, Any]],
                   now_casa: datetime) -> Snapshot:
    cotation_raw = info.get("cotation") or ""
    cotation_dt = _parse_cotation(cotation_raw)
    cotation_out = cotation_dt.isoformat(timespec="minutes") if cotation_dt else cotation_raw

    return Snapshot(
        timestamp=now_casa.isoformat(timespec="seconds"),
        cotation=cotation_out,
        market_status=_classify_market_status(now_casa),
        last_price=_to_float(info.get("cours")),
        open=_to_float(info.get("ouverture")),
        high=_to_float(info.get("max")),
        low=_to_float(info.get("min")),
        prev_close=_to_float(info.get("cloture")),
        variation_pct=_to_float(info.get("variation")),
        shares_traded=_to_int(info.get("volumeTitre")),
        value_traded_mad=_to_float(info.get("volume")),
        num_trades=len(transactions) if transactions else None,
        market_cap=_to_float(info.get("capitalisation")),
    )


def build_orderbook(raw_levels: List[Dict[str, Any]], now_casa: datetime) -> OrderBook:
    ob = OrderBook(timestamp=now_casa.isoformat(timespec="seconds"))
    for lvl in raw_levels:
        bid = {
            "price": _to_float(lvl.get("bidValue")),
            "qty": _to_float(lvl.get("bidQte")),
            "orders": _to_float(lvl.get("bidOrder")),
        }
        ask = {
            "price": _to_float(lvl.get("askValue")),
            "qty": _to_float(lvl.get("askQte")),
            "orders": _to_float(lvl.get("askOrder")),
        }
        if bid["price"] and bid["qty"]:
            ob.bids.append(bid)
        if ask["price"] and ask["qty"]:
            ob.asks.append(ask)
    return ob


# --- State -------------------------------------------------------------------

def _load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        logger.warning("State corrupt — starting fresh.")
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


# --- CSV writers -------------------------------------------------------------

def _append_row(path: Path, fields: List[str], row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row_out = {k: row.get(k, "") for k in fields}
    is_new = not path.exists() or path.stat().st_size == 0
    if is_new:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerow(row_out)
        return

    # Migrate file header if schema changed (e.g., added metadata columns).
    with open(path, "r", encoding="utf-8-sig", newline="") as rf:
        reader = csv.DictReader(rf)
        existing_fields = reader.fieldnames or []
        if existing_fields != fields:
            existing_rows = list(reader)
            tmp = path.with_suffix(path.suffix + ".tmp")
            with open(tmp, "w", encoding="utf-8-sig", newline="") as wf:
                w = csv.DictWriter(wf, fieldnames=fields)
                w.writeheader()
                for r in existing_rows:
                    w.writerow({k: r.get(k, "") for k in fields})
                w.writerow(row_out)
            os.replace(tmp, path)
            return

    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writerow(row_out)


def _csv_has_day(path: Path, day: str) -> bool:
    if not path.exists():
        return False
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return any((r.get("Séance") == day) for r in csv.DictReader(f))


def write_intraday(snap: Snapshot) -> Path:
    _append_row(INTRADAY_CSV, INTRADAY_FIELDS, asdict(snap))
    return INTRADAY_CSV


def write_orderbook(ob: OrderBook, day: str) -> Path:
    path = DATA_DIR / f"ATW_orderbook_{day}.csv"
    row: Dict[str, Any] = {"timestamp": ob.timestamp}
    for i in range(1, 6):
        b = ob.bids[i - 1] if len(ob.bids) >= i else {}
        a = ob.asks[i - 1] if len(ob.asks) >= i else {}
        row[f"bid{i}_orders"] = b.get("orders", "")
        row[f"bid{i}_qty"]    = b.get("qty", "")
        row[f"bid{i}_price"]  = b.get("price", "")
        row[f"ask{i}_price"]  = a.get("price", "")
        row[f"ask{i}_qty"]    = a.get("qty", "")
        row[f"ask{i}_orders"] = a.get("orders", "")
    _append_row(path, ORDERBOOK_FIELDS, row)
    return path


def _merge_technicals_into_state(
    state: Dict[str, Any],
    technicals: Dict[str, Any],
) -> None:
    """Merge computed technicals flat into the state dict (no separate file).
       Pops and re-inserts to ensure it appears at the end of the JSON.
    """
    state.pop("technicals", None)
    state["technicals"] = technicals


# --- Subcommands -------------------------------------------------------------

def cmd_snapshot(args) -> int:
    now_casa = datetime.now(CASA_TZ)
    day = now_casa.date().isoformat()
    state = _load_state()
    save_raw = not getattr(args, "no_save_raw", False)

    # Debounce
    last_ts = state.get("last_snapshot_ts")
    if last_ts and not args.force:
        try:
            prev = datetime.fromisoformat(last_ts)
            delta = (now_casa - prev).total_seconds()
            if delta < SNAPSHOT_DEBOUNCE_SECONDS:
                logger.info("cooldown: last snapshot %.0fs ago (< %ds)", delta, SNAPSHOT_DEBOUNCE_SECONDS)
                return 0
        except ValueError:
            pass

    # Stall: session closed + already captured today → replay cached snapshot
    market = _classify_market_status(now_casa)
    if market == "CLOSED" and last_ts and not args.force:
        try:
            prev = datetime.fromisoformat(last_ts)
            same_day = prev.date() == now_casa.date()
            if same_day:
                logger.info("stall: session closed + already captured today -> replay cached snapshot")
                cached = Snapshot(
                    timestamp=now_casa.isoformat(timespec="seconds"),
                    cotation=state.get("last_snapshot_cotation", ""),
                    market_status="CLOSED",
                    last_price=state.get("last_snapshot_last_price"),
                    open=state.get("last_snapshot_open"),
                    high=state.get("last_snapshot_high"),
                    low=state.get("last_snapshot_low"),
                    prev_close=state.get("last_snapshot_prev_close"),
                    variation_pct=state.get("last_snapshot_variation_pct"),
                    shares_traded=state.get("last_snapshot_shares_traded"),
                    value_traded_mad=state.get("last_snapshot_value_traded_mad"),
                    num_trades=state.get("last_snapshot_num_trades"),
                    market_cap=state.get("last_snapshot_market_cap"),
                )
                if save_raw:
                    write_intraday(cached)
                _log_summary(cached)
                # Update state with cached snapshot values
                state["last_snapshot_ts"] = cached.timestamp
                state["last_snapshot_cotation"] = cached.cotation
                for k in ("last_price", "open", "high", "low", "prev_close",
                          "variation_pct", "shares_traded", "value_traded_mad",
                          "num_trades", "market_cap"):
                    state[f"last_snapshot_{k}"] = getattr(cached, k)

                if args.force or cached.market_status == "OPEN":
                    technicals = compute_technicals(TICKER)
                    _merge_technicals_into_state(state, technicals)
                    logger.info("Technicals merged into state")
                else:
                    logger.info("Market closed — skipping technical merge for %s", day)

                _save_state(state)
                _auto_finalize_if_needed(now_casa)
                return 0
        except ValueError:
            pass

    session = _build_medias24_session()
    try:
        info = fetch_stock_info(session)
        transactions = fetch_transactions(session)
        ob_raw = fetch_bid_ask(session)
    except requests.HTTPError as e:
        logger.error("HTTP error from Medias24 API: %s", e)
        return 2
    except requests.RequestException as e:
        logger.error("Network error: %s", e)
        return 2

    snap = build_snapshot(info, transactions, now_casa)
    ob = build_orderbook(ob_raw, now_casa)

    if snap.last_price is None:
        logger.error("Parsed snapshot has no last_price — API payload changed? Raw: %s", info)
        return 2

    if save_raw:
        write_intraday(snap)
        write_orderbook(ob, day)
    _log_summary(snap, ob)
    # Update state keys first (so they appear first in JSON)
    state["last_snapshot_ts"] = snap.timestamp
    state["last_snapshot_cotation"] = snap.cotation
    for k in ("last_price", "open", "high", "low", "prev_close",
              "variation_pct", "shares_traded", "value_traded_mad",
              "num_trades", "market_cap"):
        state[f"last_snapshot_{k}"] = getattr(snap, k)

    if args.force or snap.market_status == "OPEN":
        technicals = compute_technicals(TICKER)
        _merge_technicals_into_state(state, technicals)
        logger.info("Technicals merged into state")
    else:
        logger.info("Market closed — skipping technical merge for %s", day)

    _save_state(state)
    _auto_finalize_if_needed(now_casa)
    return 0


def _auto_finalize_if_needed(now_casa: datetime) -> None:
    if not _is_after_market_close(now_casa):
        return
    day = now_casa.date().isoformat()
    rc = cmd_finalize(argparse.Namespace(date=day, force=False))
    if rc == 0:
        logger.info("Auto-finalize check completed for %s", day)
    elif rc == 1:
        logger.info("Auto-finalize deferred for %s (no data available yet)", day)
    else:
        logger.warning("Auto-finalize failed for %s (code=%s)", day, rc)


def cmd_finalize(args) -> int:
    """Consolidate session → one EOD row.

    Strategy: try Casablanca Bourse official API first (authoritative closing
    price). If the day isn't published yet, fall back to the intraday CSV.
    """
    now_casa = datetime.now(CASA_TZ)
    day = args.date or now_casa.date().isoformat()
    state = _load_state()
    finalized = set(state.get("finalized_days", []))

    if day in finalized and not args.force:
        if _csv_has_day(EOD_CSV, day):
            logger.info("already finalized: %s", day)
            return 0
        logger.warning("finalized state exists for %s but output row is missing — rebuilding", day)

    # --- Try Casablanca Bourse official data first ---
    eod = _try_bourse_finalize(day)

    # --- Fallback: aggregate from intraday CSV ---
    if eod is None:
        eod = _try_intraday_finalize(day)

    # --- Final fallback: latest in-memory/state snapshot ---
    if eod is None:
        eod = _try_state_finalize(day, state)

    if eod is None:
        logger.warning("no data available to finalize %s", day)
        return 1

    if not eod.get("scraped_at"):
        eod["scraped_at"] = now_casa.isoformat(timespec="seconds")
    if "cotation" not in eod:
        eod["cotation"] = state.get("last_snapshot_cotation", "")
    if not eod.get("source"):
        eod["source"] = "unknown"

    if not args.force and _csv_has_day(EOD_CSV, day):
        logger.info("EOD row for %s already in %s — marking finalized, skip append", day, EOD_CSV.name)
        finalized.add(day)
        state["finalized_days"] = sorted(finalized)
        _save_state(state)
        return 0

    _append_row(EOD_CSV, FULL_EOD_FIELDS, eod)
    logger.info(
        "Finalized ATW %s @ %s (%s): O=%s C=%s H=%s L=%s Vshares=%s Trades=%s -> %s",
        day, eod["scraped_at"], eod["source"],
        eod["Ouverture"], eod["Dernier Cours"],
        eod["+haut du jour"], eod["+bas du jour"],
        eod["Nombre de titres échangés"], eod["Nombre de transactions"],
        EOD_CSV.name,
    )

    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from database import AtwDatabase
        import pandas as _pd
        with AtwDatabase() as db:
            n = db.save_bourse(_pd.DataFrame([{k: eod.get(k) for k in (
                "Séance", "Instrument", "Ticker", "Ouverture", "Dernier Cours",
                "+haut du jour", "+bas du jour",
                "Nombre de titres échangés", "Volume des échanges",
                "Nombre de transactions", "Capitalisation",
            )}]))
            logger.info("DB: %d bourse row submitted.", n)
    except Exception as e:
        logger.warning("DB save skipped: %s", e)

    finalized.add(day)
    state["finalized_days"] = sorted(finalized)
    _save_state(state)
    return 0


def _try_bourse_finalize(day: str) -> Optional[Dict[str, str]]:
    """Try to get official EOD data from Casablanca Bourse JSONAPI."""
    try:
        session = _build_bourse_session()
        attrs = fetch_bourse_eod(session, day)
        if attrs is None:
            logger.info("Bourse API: no data for %s yet", day)
            return None

        eod = {
            "Séance": day,
            "Instrument": TICKER,
            "Ticker": TICKER,
            "Ouverture": _to_float(attrs.get("openingPrice")) or "",
            "Dernier Cours": _to_float(attrs.get("closingPrice")) or _to_float(attrs.get("coursCourant")) or "",
            "+haut du jour": _to_float(attrs.get("highPrice")) or "",
            "+bas du jour": _to_float(attrs.get("lowPrice")) or "",
            "Nombre de titres échangés": _to_int(attrs.get("cumulTitresEchanges")) or "0",
            "Volume des échanges": _to_float(attrs.get("cumulVolumeEchange")) or "0",
            "Nombre de transactions": _to_int(attrs.get("totalTrades")) or "0",
            "Capitalisation": _to_float(attrs.get("capitalisation")) or "",
            "scraped_at": datetime.now(CASA_TZ).isoformat(timespec="seconds"),
            "cotation": "",
            "source": "casablanca_bourse_daily_api",
        }
        logger.info("Finalize source: Casablanca Bourse official API")
        return eod
    except Exception as e:
        logger.warning("Bourse API finalize failed: %s — falling back to intraday CSV", e)
        return None


def _try_intraday_finalize(day: str) -> Optional[Dict[str, str]]:
    """Fall back to aggregating from the intraday CSV (single file, filter by day)."""
    if not INTRADAY_CSV.exists():
        logger.warning("no intraday file (%s)", INTRADAY_CSV)
        return None

    rows: List[Dict[str, str]] = []
    with open(INTRADAY_CSV, "r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ts = r.get("timestamp") or ""
            if ts.startswith(day):
                rows.append(r)
    if not rows:
        logger.warning("intraday file has no rows for %s", day)
        return None

    def _f(v: str) -> Optional[float]:
        if v is None or v == "":
            return None
        try:
            return float(v)
        except ValueError:
            return None

    opens = [x for x in (_f(r.get("open")) for r in rows) if x is not None and x > 0]
    highs = [x for x in (_f(r.get("high")) for r in rows) if x is not None and x > 0]
    lows  = [x for x in (_f(r.get("low"))  for r in rows) if x is not None and x > 0]

    last_row = rows[-1]
    last_price = _f(last_row.get("last_price"))
    if last_price is None:
        logger.error("last_price missing in final intraday row of %s", day)
        return None

    logger.info("Finalize source: intraday CSV aggregation")
    return {
        "Séance": day,
        "Instrument": TICKER,
        "Ticker": TICKER,
        "Ouverture": opens[0] if opens else "",
        "Dernier Cours": last_price,
        "+haut du jour": max(highs) if highs else "",
        "+bas du jour": min(lows) if lows else "",
        "Nombre de titres échangés": last_row.get("shares_traded", "") or "0",
        "Volume des échanges": last_row.get("value_traded_mad", "") or "0",
        "Nombre de transactions": last_row.get("num_trades", "") or "0",
        "Capitalisation": last_row.get("market_cap", "") or "",
        "scraped_at": last_row.get("timestamp", "") or datetime.now(CASA_TZ).isoformat(timespec="seconds"),
        "cotation": last_row.get("cotation", "") or "",
        "source": "medias24_intraday_csv",
    }


def _try_state_finalize(day: str, state: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Final fallback: build daily row from latest saved snapshot state."""
    last_ts = state.get("last_snapshot_ts")
    if not last_ts:
        logger.warning("state finalize: no last snapshot in state")
        return None
    try:
        snap_dt = datetime.fromisoformat(str(last_ts))
    except ValueError:
        logger.warning("state finalize: invalid last_snapshot_ts=%s", last_ts)
        return None
    if snap_dt.date().isoformat() != day:
        logger.warning("state finalize: last snapshot day (%s) != requested day (%s)", snap_dt.date(), day)
        return None

    last_price = _to_float(state.get("last_snapshot_last_price"))
    if last_price is None:
        logger.warning("state finalize: missing last_snapshot_last_price")
        return None

    logger.info("Finalize source: state snapshot fallback")
    return {
        "Séance": day,
        "Instrument": TICKER,
        "Ticker": TICKER,
        "Ouverture": _to_float(state.get("last_snapshot_open")) or "",
        "Dernier Cours": last_price,
        "+haut du jour": _to_float(state.get("last_snapshot_high")) or "",
        "+bas du jour": _to_float(state.get("last_snapshot_low")) or "",
        "Nombre de titres échangés": _to_int(state.get("last_snapshot_shares_traded")) or "0",
        "Volume des échanges": _to_float(state.get("last_snapshot_value_traded_mad")) or "0",
        "Nombre de transactions": _to_int(state.get("last_snapshot_num_trades")) or "0",
        "Capitalisation": _to_float(state.get("last_snapshot_market_cap")) or "",
        "scraped_at": str(last_ts),
        "cotation": state.get("last_snapshot_cotation", ""),
        "source": "medias24_state_snapshot",
    }


# --- Technical Analysis ------------------------------------------------------

def _read_csv_any_encoding(path: Path) -> Optional[pd.DataFrame]:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return None


def _load_eod_dataframe() -> Optional[pd.DataFrame]:
    """Load EOD CSV history into a clean DataFrame with columns:
    Date, Open, High, Low, Close, Volume.
    """
    daily_paths = sorted(
        p for p in DATA_DIR.glob("ATW_bourse_casa_*.csv")
        if p.name != EOD_CSV.name
    )
    frames: List[pd.DataFrame] = []
    for path in daily_paths:
        df_part = _read_csv_any_encoding(path)
        if df_part is not None and not df_part.empty:
            frames.append(df_part)

    if EOD_CSV.exists():
        df_full = _read_csv_any_encoding(EOD_CSV)
        if df_full is not None and not df_full.empty:
            frames.append(df_full)

    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True, sort=False)

    col_map = {}
    used_targets = set()
    for col in df.columns:
        cl = col.strip().lower()
        target = None
        if "ance" in cl or cl in ("date", "séance", "seance"):
            target = "Date"
        elif cl in ("ouverture",):
            target = "Open"
        elif "haut" in cl:
            target = "High"
        elif "bas" in cl:
            target = "Low"
        elif "dernier" in cl or "closing" in cl:
            target = "Close"
        elif "titres" in cl:
            target = "Volume"
        if target and target not in used_targets:
            col_map[col] = target
            used_targets.add(target)
    df = df.rename(columns=col_map)
    for c in ("Open", "High", "Low", "Close", "Volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = (
            df.dropna(subset=["Date"])
            .sort_values("Date")
            .drop_duplicates(subset=["Date"], keep="last")
            .reset_index(drop=True)
        )
    return df


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = series.ewm(span=fast, min_periods=fast).mean()
    ema_slow = series.ewm(span=slow, min_periods=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, min_periods=signal).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                k_period: int = 14, d_period: int = 3):
    lowest_low = low.rolling(k_period, min_periods=k_period).min()
    highest_high = high.rolling(k_period, min_periods=k_period).max()
    denom = (highest_high - lowest_low).replace(0, np.nan)
    k = 100 * (close - lowest_low) / denom
    d = k.rolling(d_period, min_periods=1).mean()
    return k, d


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0))
    return (direction * volume.fillna(0)).cumsum()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14):
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=high.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=high.index)
    h_l = high - low
    h_pc = (high - close.shift(1)).abs()
    l_pc = (low - close.shift(1)).abs()
    tr = pd.concat([h_l, h_pc, l_pc], axis=1).max(axis=1)
    # Wilder smoothing via EMA with alpha = 1/period
    atr = tr.ewm(alpha=1 / period, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, min_periods=period).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, min_periods=period).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / period, min_periods=period).mean()
    return adx, plus_di, minus_di


def _mfi(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 14) -> pd.Series:
    typical = (high + low + close) / 3
    raw_flow = typical * volume
    direction = np.sign(typical.diff().fillna(0))
    pos_flow = raw_flow.where(direction > 0, 0.0).rolling(period, min_periods=period).sum()
    neg_flow = raw_flow.where(direction < 0, 0.0).rolling(period, min_periods=period).sum()
    ratio = pos_flow / neg_flow.replace(0, np.nan)
    return 100 - (100 / (1 + ratio))


def compute_technicals(symbol: str = TICKER) -> Dict[str, Any]:
    """Compute technical indicators from the EOD CSV.

    Returns a dict ready to be injected into the AI agent context.
    Called by agents/tools.py — no network calls, pure computation.
    """
    df = _load_eod_dataframe()
    if df is None or len(df) < 30:
        return {"error": "Insufficient price history for technical analysis"}

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"] if "Volume" in df.columns else pd.Series(dtype=float)
    last_close = float(close.iloc[-1])
    last_date = str(df["Date"].iloc[-1].date()) if "Date" in df.columns else ""

    # --- Moving Averages ---
    sma_20 = close.rolling(20, min_periods=20).mean()
    sma_50 = close.rolling(50, min_periods=20).mean()
    sma_200 = close.rolling(200, min_periods=50).mean()
    ema_12 = close.ewm(span=12, min_periods=12).mean()
    ema_26 = close.ewm(span=26, min_periods=26).mean()

    # --- RSI ---
    rsi_14 = _rsi(close, 14)

    # --- MACD ---
    macd_line, macd_signal, macd_hist = _macd(close)

    # --- Bollinger Bands ---
    bb_mid = sma_20
    bb_std = close.rolling(20, min_periods=20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    # --- Stochastic ---
    stoch_k, stoch_d = _stochastic(high, low, close)

    # --- ATR (14) ---
    h_l = high - low
    h_pc = (high - close.shift(1)).abs()
    l_pc = (low - close.shift(1)).abs()
    tr = pd.concat([h_l, h_pc, l_pc], axis=1).max(axis=1)
    atr_14 = tr.rolling(14, min_periods=5).mean()

    # --- VWAP (cumulative intraday approx from daily data) ---
    vwap = None
    if len(volume) > 0 and volume.sum() > 0:
        typical_price = (high + low + close) / 3
        cum_tp_vol = (typical_price * volume).rolling(20, min_periods=1).sum()
        cum_vol = volume.rolling(20, min_periods=1).sum().replace(0, np.nan)
        vwap_series = cum_tp_vol / cum_vol
        vwap = round(float(vwap_series.iloc[-1]), 2) if pd.notna(vwap_series.iloc[-1]) else None

    # --- Support / Resistance (20-day low/high) ---
    support_20 = float(low.tail(20).min()) if len(low) >= 20 else None
    resistance_20 = float(high.tail(20).max()) if len(high) >= 20 else None

    # --- Returns & realized volatility (log returns, annualized at 252d) ---
    log_ret = np.log(close / close.shift(1))
    def _ret(n: int):
        if len(close) <= n:
            return None
        prev = close.iloc[-1 - n]
        if not pd.notna(prev) or prev == 0:
            return None
        return float(close.iloc[-1] / prev - 1.0)
    ret_1d = _ret(1)
    ret_5d = _ret(5)
    ret_20d = _ret(20)
    ret_60d = _ret(60)
    rv_20 = log_ret.rolling(20, min_periods=20).std(ddof=0)
    realized_vol_20d_ann = float(rv_20.iloc[-1] * np.sqrt(252)) if pd.notna(rv_20.iloc[-1]) else None

    # --- 52-week high/low (252 trading days) ---
    hi_52w_series = high.rolling(252, min_periods=60).max()
    lo_52w_series = low.rolling(252, min_periods=60).min()
    hi_52w = float(hi_52w_series.iloc[-1]) if pd.notna(hi_52w_series.iloc[-1]) else None
    lo_52w = float(lo_52w_series.iloc[-1]) if pd.notna(lo_52w_series.iloc[-1]) else None
    pct_from_52w_high = (last_close / hi_52w - 1.0) if hi_52w else None
    pct_from_52w_low = (last_close / lo_52w - 1.0) if lo_52w else None

    # --- Bollinger %B & bandwidth ---
    bb_u_val = bb_upper.iloc[-1]
    bb_l_val = bb_lower.iloc[-1]
    bb_m_val = bb_mid.iloc[-1]
    bb_percent_b = None
    bb_bandwidth = None
    if pd.notna(bb_u_val) and pd.notna(bb_l_val) and (bb_u_val - bb_l_val) != 0:
        bb_percent_b = float((last_close - bb_l_val) / (bb_u_val - bb_l_val))
    if pd.notna(bb_u_val) and pd.notna(bb_l_val) and pd.notna(bb_m_val) and bb_m_val != 0:
        bb_bandwidth = float((bb_u_val - bb_l_val) / bb_m_val)

    # --- ATR% (position sizing) ---
    atr_val = atr_14.iloc[-1] if pd.notna(atr_14.iloc[-1]) else None
    atr_pct = float(atr_val / last_close) if atr_val and last_close else None

    # --- OBV ---
    obv_val = None
    obv_slope = None
    if len(volume) > 0 and volume.sum() > 0:
        obv_series = _obv(close, volume)
        obv_val = float(obv_series.iloc[-1]) if pd.notna(obv_series.iloc[-1]) else None
        if len(obv_series) > 20 and pd.notna(obv_series.iloc[-21]):
            obv_slope = "RISING" if obv_series.iloc[-1] > obv_series.iloc[-21] else "FALLING"

    # --- ADX ---
    adx_series, pdi_series, mdi_series = _adx(high, low, close, 14)
    adx_val = float(adx_series.iloc[-1]) if pd.notna(adx_series.iloc[-1]) else None
    pdi_val = float(pdi_series.iloc[-1]) if pd.notna(pdi_series.iloc[-1]) else None
    mdi_val = float(mdi_series.iloc[-1]) if pd.notna(mdi_series.iloc[-1]) else None

    def _adx_signal():
        if adx_val is None:
            return "NO_DATA"
        if adx_val >= 25:
            return "STRONG_TREND"
        if adx_val < 20:
            return "NO_TREND"
        return "WEAK_TREND"

    # --- MFI (volume-weighted RSI) ---
    mfi_val = None
    if len(volume) > 0 and volume.sum() > 0:
        mfi_series = _mfi(high, low, close, volume, 14)
        if pd.notna(mfi_series.iloc[-1]):
            mfi_val = float(mfi_series.iloc[-1])

    def _mfi_signal():
        if mfi_val is None:
            return "NO_DATA"
        if mfi_val > 80:
            return "OVERBOUGHT"
        if mfi_val < 20:
            return "OVERSOLD"
        return "NEUTRAL"

    # --- Trend classification ---
    def _trend():
        sma50_val = sma_50.iloc[-1] if pd.notna(sma_50.iloc[-1]) else None
        sma200_val = sma_200.iloc[-1] if pd.notna(sma_200.iloc[-1]) else None
        if sma50_val and sma200_val:
            if last_close > sma50_val > sma200_val:
                return "STRONG_UPTREND"
            if last_close > sma50_val:
                return "UPTREND"
            if last_close < sma50_val < sma200_val:
                return "STRONG_DOWNTREND"
            if last_close < sma50_val:
                return "DOWNTREND"
        elif sma50_val:
            return "UPTREND" if last_close > sma50_val else "DOWNTREND"
        return "NEUTRAL"

    # --- Signal interpretations ---
    rsi_val = float(rsi_14.iloc[-1]) if pd.notna(rsi_14.iloc[-1]) else None
    macd_val = float(macd_line.iloc[-1]) if pd.notna(macd_line.iloc[-1]) else None
    macd_sig = float(macd_signal.iloc[-1]) if pd.notna(macd_signal.iloc[-1]) else None
    macd_h = float(macd_hist.iloc[-1]) if pd.notna(macd_hist.iloc[-1]) else None
    stoch_k_val = float(stoch_k.iloc[-1]) if pd.notna(stoch_k.iloc[-1]) else None
    stoch_d_val = float(stoch_d.iloc[-1]) if pd.notna(stoch_d.iloc[-1]) else None

    def _rsi_signal():
        if rsi_val is None:
            return "NO_DATA"
        if rsi_val > 70:
            return "OVERBOUGHT"
        if rsi_val < 30:
            return "OVERSOLD"
        return "NEUTRAL"

    def _macd_signal():
        if macd_val is None or macd_sig is None:
            return "NO_DATA"
        if macd_val > macd_sig:
            return "BULLISH"
        return "BEARISH"

    def _stoch_signal():
        if stoch_k_val is None:
            return "NO_DATA"
        if stoch_k_val > 80:
            return "OVERBOUGHT"
        if stoch_k_val < 20:
            return "OVERSOLD"
        return "NEUTRAL"

    def _bb_signal():
        bb_u = bb_upper.iloc[-1] if pd.notna(bb_upper.iloc[-1]) else None
        bb_l = bb_lower.iloc[-1] if pd.notna(bb_lower.iloc[-1]) else None
        if bb_u is None or bb_l is None:
            return "NO_DATA"
        if last_close >= bb_u:
            return "OVERBOUGHT"
        if last_close <= bb_l:
            return "OVERSOLD"
        return "NEUTRAL"

    def _safe_round(val, decimals=2):
        return round(float(val), decimals) if val is not None and pd.notna(val) else None

    return {
        "as_of_date": last_date,
        "last_close": last_close,
        "trend": _trend(),
        "moving_averages": {
            "SMA_20": _safe_round(sma_20.iloc[-1]),
            "SMA_50": _safe_round(sma_50.iloc[-1]),
            "SMA_200": _safe_round(sma_200.iloc[-1]),
            "EMA_12": _safe_round(ema_12.iloc[-1]),
            "EMA_26": _safe_round(ema_26.iloc[-1]),
            "price_vs_SMA50": "ABOVE" if last_close > (sma_50.iloc[-1] or 0) else "BELOW",
            "price_vs_SMA200": "ABOVE" if last_close > (sma_200.iloc[-1] or 0) else "BELOW",
        },
        "RSI": {
            "value": _safe_round(rsi_val),
            "signal": _rsi_signal(),
        },
        "MACD": {
            "macd_line": _safe_round(macd_val),
            "signal_line": _safe_round(macd_sig),
            "histogram": _safe_round(macd_h),
            "signal": _macd_signal(),
        },
        "bollinger_bands": {
            "upper": _safe_round(bb_upper.iloc[-1]),
            "middle": _safe_round(bb_mid.iloc[-1]),
            "lower": _safe_round(bb_lower.iloc[-1]),
            "percent_b": _safe_round(bb_percent_b, 3),
            "bandwidth": _safe_round(bb_bandwidth, 4),
            "signal": _bb_signal(),
        },
        "stochastic": {
            "K": _safe_round(stoch_k_val),
            "D": _safe_round(stoch_d_val),
            "signal": _stoch_signal(),
        },
        "ATR_14": _safe_round(atr_14.iloc[-1]),
        "ATR_14_pct": _safe_round(atr_pct, 4),
        "VWAP_20d": vwap,
        "support_resistance": {
            "support_20d": _safe_round(support_20),
            "resistance_20d": _safe_round(resistance_20),
        },
        "returns": {
            "ret_1d": _safe_round(ret_1d, 4),
            "ret_5d": _safe_round(ret_5d, 4),
            "ret_20d": _safe_round(ret_20d, 4),
            "ret_60d": _safe_round(ret_60d, 4),
        },
        "realized_vol_20d_ann": _safe_round(realized_vol_20d_ann, 4),
        "fifty_two_week": {
            "high": _safe_round(hi_52w),
            "low": _safe_round(lo_52w),
            "pct_from_high": _safe_round(pct_from_52w_high, 4),
            "pct_from_low": _safe_round(pct_from_52w_low, 4),
        },
        "OBV": {
            "value": _safe_round(obv_val, 0) if obv_val is not None else None,
            "slope_20d": obv_slope,
        },
        "ADX": {
            "value": _safe_round(adx_val),
            "plus_DI": _safe_round(pdi_val),
            "minus_DI": _safe_round(mdi_val),
            "signal": _adx_signal(),
        },
        "MFI_14": {
            "value": _safe_round(mfi_val),
            "signal": _mfi_signal(),
        },
    }


# --- Logging -----------------------------------------------------------------

def _log_summary(snap: Snapshot, ob: Optional[OrderBook] = None) -> None:
    price = f"{snap.last_price:.2f}" if snap.last_price is not None else "?"
    var = f"{snap.variation_pct:+.2f}%" if snap.variation_pct is not None else "?"
    vol = f"{(snap.shares_traded or 0):,}"
    tr = f"{(snap.num_trades or 0):,}"
    spread = ""
    if ob and ob.bids and ob.asks:
        try:
            s = ob.asks[0]["price"] - ob.bids[0]["price"]
            spread = f" spread={s:.2f}"
        except (KeyError, TypeError):
            pass
    logger.info("%s ATW %s (%s) vol=%s trades=%s%s [%s]",
                snap.timestamp, price, var, vol, tr, spread, snap.market_status)


# --- CLI ---------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(
        description="ATW realtime scraper — Medias24 live + Casablanca Bourse EOD."
    )
    sub = p.add_subparsers(dest="cmd", required=False)

    sp = sub.add_parser("snapshot", help="Take one real-time snapshot (Medias24).")
    sp.add_argument("--force", action="store_true", help="Bypass debounce / stall detection.")
    sp.set_defaults(func=cmd_snapshot)

    fp = sub.add_parser("finalize", help="Consolidate session -> append EOD row.")
    fp.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today).")
    fp.add_argument("--force", action="store_true", help="Bypass idempotency guard.")
    fp.set_defaults(func=cmd_finalize)

    args = p.parse_args()
    if not hasattr(args, "func"):
        logger.info("No cmd provided — running snapshot (auto-finalize after close)")
        return cmd_snapshot(argparse.Namespace(force=False, no_save_raw=True))
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
