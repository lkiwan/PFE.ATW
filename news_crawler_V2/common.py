"""Shared helpers for all ATW news sources.

Per-source scrapers live in their own files (ATW_*_news.py) and each export
a single `scrape(known_url_keys=None) -> list[dict]` function. This module
owns everything that is NOT source-specific:

- Fetch / parse / canonicalize / blocklist helpers
- Signal scoring + noise filtering + dedup
- State persistence + CSV save + existing-CSV load
- Body enrichment (trafilatura + gnewsdecoder), parallelized
- Shared CLI entrypoint `run_cli(scrape_fn, source_label)`

No database writes. CSV is the only sink.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

import certifi

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["CURL_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

import requests
from bs4 import BeautifulSoup

# --- Paths & constants -------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = _ROOT / "data" / "ATW_news.csv"
STATE_FILE = _ROOT / "data" / "scrapers" / "atw_news_state.json"
TICKER = "ATW"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 20
POLITE_DELAY = 1.0
BODY_FETCH_WORKERS = 5

logger = logging.getLogger("atw_news")


def configure_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


# --- Blocklists --------------------------------------------------------------

ATW_TOKEN_RE = re.compile(
    r"\b(attijariwafa|attijari\s*wafa|\bATW\b)",
    flags=re.IGNORECASE,
)

NOISE_SOURCE_SUBSTRINGS = ("bebee", "instagram", "facebook.com")

FOCUS_PME_RE = re.compile(r"\bfocus\s*pme\b", flags=re.IGNORECASE)
EGYPT_KEYWORD_RE = re.compile(
    r"\b(egypt|egypte|égypte|cairo|le\s+caire|alexandrie|alexandria|egx|attijariwafa\s+bank\s+egypt)\b",
    flags=re.IGNORECASE,
)
MOROCCO_CONTEXT_RE = re.compile(
    r"\b(maroc|morocco|casablanca|bourse de casablanca|masi|ammc|bank al[-\s]?maghrib|bam)\b",
    flags=re.IGNORECASE,
)
ATW_CORE_SIGNAL_RE = re.compile(
    r"\b("
    r"résultats?|resultats?|earnings|rnpg|pnb|bénéfices?|benefices?|profits?|net income|"
    r"chiffre d'affaires|revenus?|croissance|guidance|outlook|"
    r"dividendes?|dividend|"
    r"strat[ée]gie|plan strat[ée]gique|transformation|acquisition|fusion|cession|"
    r"rating|notation|recommandation|cours cible|objectif de cours|upgrade|downgrade|surpond[ée]rer|"
    r"valorisation|capitalisation|bourse"
    r")\b",
    flags=re.IGNORECASE,
)
ATW_PASSING_RE = re.compile(
    r"\b(forum|salon|webinaire|événement|evenement|event|sponsor|sponsoring|campagne)\b",
    flags=re.IGNORECASE,
)


# --- HTTP --------------------------------------------------------------------

def fetch(url: str, timeout: int = REQUEST_TIMEOUT, retries: int = 1) -> Optional[str]:
    for attempt in range(retries + 1):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
                timeout=timeout,
            )
            if resp.status_code == 200 and resp.text:
                return resp.text
            logger.warning("HTTP %s for %s", resp.status_code, url)
            return None
        except requests.RequestException as exc:
            if attempt < retries:
                logger.info("Retry %d for %s (%s)", attempt + 1, url, exc)
                continue
            logger.warning("Fetch failed for %s: %s", url, exc)
    return None


def resolve_final_url(url: str) -> str:
    """Resolve a Google-News `rss/articles/CBMi...` URL to the real publisher
    URL via googlenewsdecoder. For non-Google-News URLs, follow standard HTTP
    redirects. Returns the original URL on failure.
    """
    if "news.google.com/rss/articles/" in url:
        try:
            from googlenewsdecoder import gnewsdecoder
            result = gnewsdecoder(url, interval=1)
            if isinstance(result, dict) and result.get("status") and result.get("decoded_url"):
                return result["decoded_url"]
        except Exception as exc:
            logger.debug("gnewsdecoder failed for %s: %s", url[:80], exc)
        return url

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        return resp.url or url
    except requests.RequestException:
        return url


# --- Date parsing ------------------------------------------------------------

_FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def parse_date(value) -> str:
    if not value:
        return ""
    if isinstance(value, time.struct_time):
        return datetime(*value[:6], tzinfo=timezone.utc).isoformat()
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return ""
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                continue
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            pass
    return ""


def parse_french_date(s: str) -> str:
    if not s:
        return ""
    m = re.search(
        r"(\d{1,2})\s+(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)\s+(\d{4})",
        s,
        re.IGNORECASE,
    )
    if not m:
        return ""
    day, month_name, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
    month = _FRENCH_MONTHS.get(month_name)
    if not month:
        return ""
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def extract_article_date(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    for attrs in (
        {"property": "article:published_time"},
        {"property": "og:article:published_time"},
        {"name": "article:published_time"},
        {"itemprop": "datePublished"},
        {"name": "date"},
        {"name": "pubdate"},
        {"name": "publish-date"},
    ):
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            parsed = parse_date(tag["content"]) or parse_french_date(tag["content"])
            if parsed:
                return parsed

    for t in soup.find_all("time"):
        candidate = t.get("datetime") or t.get_text(strip=True)
        parsed = parse_date(candidate) or parse_french_date(candidate)
        if parsed:
            return parsed

    for s in soup.find_all("script", type="application/ld+json"):
        txt = s.string or s.get_text() or ""
        for m in re.finditer(r'"datePublished"\s*:\s*"([^"]+)"', txt):
            parsed = parse_date(m.group(1)) or parse_french_date(m.group(1))
            if parsed:
                return parsed

    return ""


def fetch_article_body(url: str) -> tuple[str, str]:
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code != 200 or not resp.text:
            return "", ""
        import trafilatura
        text = trafilatura.extract(
            resp.text,
            url=resp.url,
            include_comments=False,
            include_tables=False,
            favor_recall=False,
        )
        date_str = extract_article_date(resp.text)
        return (text or "").strip(), date_str
    except Exception as exc:
        logger.debug("Body extract failed for %s: %s", url, exc)
        return "", ""


def fetch_article_date_only(url: str) -> str:
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code != 200 or not resp.text:
            return ""
        return extract_article_date(resp.text)
    except Exception as exc:
        logger.debug("Date extract failed for %s: %s", url, exc)
        return ""


def enrich_bodies(items: list[dict], workers: int = BODY_FETCH_WORKERS) -> int:
    """Fetch article bodies in parallel and mutate items in place.

    Returns the count of items that received new full_content. Items that
    already have full_content are skipped. One slow/stalled article cannot
    block the others beyond its own REQUEST_TIMEOUT.
    """
    to_fetch = [it for it in items if not it.get("full_content")]
    if not to_fetch:
        return 0
    t0 = time.time()
    enriched = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_item = {pool.submit(fetch_article_body, it["url"]): it for it in to_fetch}
        done = 0
        for fut in as_completed(future_to_item):
            it = future_to_item[fut]
            done += 1
            try:
                body, body_date = fut.result()
            except Exception as exc:
                logger.debug("Body fetch raised for %s: %s", it.get("url"), exc)
                body, body_date = "", ""
            if body:
                it["full_content"] = body
                enriched += 1
            if not it.get("date") and body_date:
                it["date"] = body_date
            logger.info("  [%d/%d] %s", done, len(to_fetch), it.get("url", ""))
    logger.info("  -> body fetch took %.1fs", time.time() - t0)
    return enriched


# --- URL / host helpers ------------------------------------------------------

def mentions_atw(*fields: str) -> bool:
    for f in fields:
        if f and ATW_TOKEN_RE.search(f):
            return True
    return False


def canonical_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except ValueError:
        return raw.split("?")[0].rstrip("/").lower()

    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]

    path = re.sub(r"/+", "/", parsed.path or "")
    if path != "/":
        path = path.rstrip("/")

    query_params = parse_qs(parsed.query, keep_blank_values=False)

    for key in ("url", "u", "target", "dest", "destination"):
        values = query_params.get(key) or query_params.get(key.upper())
        if values:
            nested = unquote(values[0]).strip()
            if nested.startswith(("http://", "https://")):
                return canonical_url(nested)

    kept_items: list[tuple[str, str]] = []
    for key, values in query_params.items():
        lk = key.lower()
        if lk.startswith("utm_") or lk in {
            "oc", "ved", "usg", "fbclid", "gclid", "igshid", "mkt_tok", "mc_cid", "mc_eid",
        }:
            continue
        for value in values:
            kept_items.append((lk, value))
    kept_items.sort()
    query = "&".join(f"{k}={v}" if v else k for k, v in kept_items)

    c = f"{host}{path}"
    if query:
        c = f"{c}?{query}"
    return c.lower()


def url_key(url: str) -> str:
    return canonical_url(url)


def abs_url(base: str, href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    return base.rstrip("/") + "/" + href.lstrip("/")


def normalize_title(title: str) -> str:
    t = re.sub(r"\s+", " ", title or "").strip().lower()
    t = re.sub(
        r"\s(?:-|–|—|\|)\s(?:medias24|l['’]?economiste|boursenews|infom[ée]diaire|facebook\.com|instagram\.com|bebee\.com)$",
        "",
        t,
        flags=re.IGNORECASE,
    )
    t = re.sub(r"[^\w\sàâäéèêëïîôöùûüç%-]", " ", t, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", t).strip()


# --- Signal / noise / filter -------------------------------------------------

def is_egypt_specific(*fields: str) -> bool:
    text = " ".join(f for f in fields if f)
    if not text:
        return False
    if not EGYPT_KEYWORD_RE.search(text):
        return False
    return not MOROCCO_CONTEXT_RE.search(text)


def is_noise_article(article: dict) -> bool:
    source = (article.get("source") or "").lower()
    url = (article.get("url") or "").lower()
    title = article.get("title") or ""
    snippet = article.get("snippet") or ""
    text = f"{title} {snippet} {source} {url}"

    if any(sub in source for sub in NOISE_SOURCE_SUBSTRINGS):
        return True
    if "bebee" in url or "instagram.com" in url:
        return True
    if FOCUS_PME_RE.search(text):
        return True
    if is_egypt_specific(title, snippet, source, url):
        return True
    return False


def compute_signal_fields(article: dict) -> tuple[int, int]:
    title = article.get("title") or ""
    snippet = article.get("snippet") or ""
    full_content = article.get("full_content") or ""
    query_source = (article.get("query_source") or "").lower()

    text_all = f"{title} {snippet} {full_content}"
    atw_title = mentions_atw(title)
    atw_any = mentions_atw(title, snippet, full_content)

    core_title_hits = len(ATW_CORE_SIGNAL_RE.findall(title))
    core_all_hits = len(ATW_CORE_SIGNAL_RE.findall(text_all))
    passing_hits = len(ATW_PASSING_RE.findall(text_all))

    score = 10
    if atw_any:
        score += 20
    if atw_title:
        score += 15
    score += min(core_title_hits, 3) * 18
    score += min(max(core_all_hits - core_title_hits, 0), 4) * 8
    if query_source.startswith("direct:"):
        score += 6
    score -= min(passing_hits, 3) * 8
    if is_egypt_specific(title, snippet, full_content):
        score -= 40

    score = max(0, min(100, score))
    is_core = int(atw_any and (core_title_hits > 0 or core_all_hits >= 2))
    return score, is_core


def deduplicate(articles: Iterable[dict]) -> list[dict]:
    ranked = sorted(
        list(articles),
        key=lambda a: (
            "news.google.com/rss/articles/" not in (a.get("url", "").lower()),
            bool(a.get("full_content")),
            bool(a.get("date")),
        ),
        reverse=True,
    )
    seen_urls: set[str] = set()
    seen_date_titles: set[str] = set()
    seen_titles: set[str] = set()
    out: list[dict] = []
    for a in ranked:
        url_raw = a.get("url") or ""
        uk = canonical_url(url_raw)
        title_key = normalize_title(a.get("title", ""))
        if not uk or not title_key:
            continue

        date_key = (parse_date(a.get("date")) or "")[:10]
        date_title_key = f"{date_key}|{title_key}" if date_key else ""
        is_gnews = "news.google.com/rss/articles/" in url_raw.lower()

        if uk in seen_urls:
            continue
        if date_title_key and date_title_key in seen_date_titles:
            continue
        if not date_title_key and title_key in seen_titles:
            continue
        if is_gnews and title_key in seen_titles:
            continue

        seen_urls.add(uk)
        seen_titles.add(title_key)
        if date_title_key:
            seen_date_titles.add(date_title_key)
        out.append(a)
    return out


def filter_noise_articles(articles: Iterable[dict]) -> list[dict]:
    return [a for a in articles if not is_noise_article(a)]


def add_signal_metadata(articles: Iterable[dict]) -> list[dict]:
    out: list[dict] = []
    scraping_time = datetime.now(timezone.utc).isoformat()
    for article in articles:
        row = dict(article)
        row.setdefault("ticker", TICKER)
        score, is_core = compute_signal_fields(row)
        row["signal_score"] = score
        row["is_atw_core"] = is_core
        row.setdefault("scraping_date", scraping_time)
        out.append(row)
    return out


def filter_since(articles: list[dict], since_iso: Optional[str]) -> list[dict]:
    if not since_iso:
        return articles
    cutoff = datetime.fromisoformat(since_iso).replace(tzinfo=timezone.utc)
    kept = []
    for a in articles:
        date_str = a.get("date")
        if not date_str:
            kept.append(a)
            continue
        try:
            d = datetime.fromisoformat(date_str)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            if d >= cutoff:
                kept.append(a)
        except ValueError:
            kept.append(a)
    return kept


# --- State -------------------------------------------------------------------

def empty_state() -> dict:
    return {
        "seen_urls": {},
        "per_source_last_seen": {},
        "failed_body_urls": [],
        "gnews_resolved": {},
        "last_full_run_ts": None,
    }


def load_state() -> dict:
    if not STATE_FILE.exists():
        return empty_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
        s.setdefault("seen_urls", {})
        s.setdefault("per_source_last_seen", {})
        s.setdefault("failed_body_urls", [])
        s.setdefault("gnews_resolved", {})
        s.setdefault("last_full_run_ts", None)
        return s
    except (json.JSONDecodeError, OSError):
        logger.warning("News state corrupt — starting fresh.")
        return empty_state()


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


# --- CSV ---------------------------------------------------------------------

CSV_FIELDS = [
    "date", "ticker", "title", "source", "url", "full_content",
    "query_source", "signal_score", "is_atw_core", "scraping_date",
]


def _flatten(value) -> str:
    s = "" if value is None else str(value)
    return re.sub(r"\s*\n+\s*", " ", s).strip()


def save_csv(articles: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for a in articles:
            w.writerow({k: _flatten(a.get(k, "")) for k in CSV_FIELDS})


def load_existing_csv(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            k = url_key(row.get("url", ""))
            if k:
                out[k] = row
    return out


def load_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def merge_and_save_to_csv(new_items: list[dict], out_path: Path = DEFAULT_OUT) -> int:
    existing = load_existing_csv(out_path)
    merged: dict[str, dict] = dict(existing)
    for a in new_items:
        a.setdefault("ticker", TICKER)
        k = url_key(a.get("url", ""))
        if not k:
            continue
        prior = merged.get(k)
        if prior:
            if prior.get("full_content") and not a.get("full_content"):
                a["full_content"] = prior["full_content"]
            if prior.get("date") and not a.get("date"):
                a["date"] = prior["date"]
        merged[k] = a
    final = add_signal_metadata(deduplicate(filter_noise_articles(merged.values())))
    final.sort(key=lambda r: r.get("date") or "", reverse=True)
    save_csv(final, out_path)
    return len(final)


# --- Shared CLI --------------------------------------------------------------

def run_cli(scrape_fn: Callable[..., list[dict]], source_label: str) -> None:
    """Standard argparse entrypoint used by every ATW_*_news.py module."""
    parser = argparse.ArgumentParser(description=f"Standalone run: {source_label} ATW")
    parser.add_argument("--show", type=int, default=10, help="How many items to print")
    parser.add_argument("--no-save", action="store_true", help="Print only; don't write CSV")
    args = parser.parse_args()

    configure_logging()
    items = scrape_fn()
    print(f"\n=== {len(items)} items scraped ===")
    for a in items[: args.show]:
        print(f"  {(a.get('date') or '')[:10]:10}  {a.get('title','')[:90]}")
        print(f"     {a.get('url','')}")
    if args.no_save:
        return

    total = merge_and_save_to_csv(items, DEFAULT_OUT)
    print(f"\nCSV saved: {DEFAULT_OUT} ({total} total rows)")
    try:
        sys.path.insert(0, str(_ROOT))
        from database import AtwDatabase
        with AtwDatabase() as db:
            ins, enr = db.save_news(items)
            print(f"DB: +{ins} new, {enr} enriched with full_content")
    except Exception as exc:
        print(f"DB save skipped: {exc}")
