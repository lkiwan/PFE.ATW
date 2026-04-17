"""Aujourd'hui le Maroc — Attijariwafa bank articles via WordPress REST.

The HTML search page became Cloudflare-protected (2026-04). The site's
WP REST endpoint `/wp-json/wp/v2/posts?search=...` is not CF-gated and
returns structured JSON with title/date/link/excerpt/full content.
"""
from __future__ import annotations


# --- BEGIN INJECTED COMMON.PY ---
"""Shared helpers for all ATW news sources.

Per-source scrapers live in their own files (ATW_*_news.py) and each export
a single `scrape(known_url_keys=None) -> list[dict]` function. This module
owns everything that is NOT source-specific:

- Fetch / parse / canonicalize / blocklist helpers
- Signal scoring + noise filtering + dedup
- State persistence + CSV save + existing-CSV load
- Body enrichment (trafilatura + gnewsdecoder)

No database writes. CSV is the only sink.
"""



import csv
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

import certifi

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["CURL_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

import requests
import html
from bs4 import BeautifulSoup

TAG_RE = re.compile(r'<[^>]+>')

def fast_strip_tags(text: str) -> str:
    """Strip HTML tags using regex and unescape entities. 100x faster than BeautifulSoup."""
    if not text:
        return ""
    return html.unescape(TAG_RE.sub('', text)).strip()

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

logger = logging.getLogger("atw_news")


def configure_logging(level: int = logging.INFO) -> None:
    """Idempotent root-logger setup for standalone source runs and orchestrator."""
    root = logging.getLogger()
    if root.handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


# --- Blocklists --------------------------------------------------------------

BLOCKED_HOST_SUBSTRINGS = (
    "attijariwafa", "attijari.com", "daralmoukawil.com",
    "facebook.com", "instagram.com", "twitter.com", "threads.net",
    "tiktok.com", "youtube.com", "youtu.be", "linkedin.com", "pinterest.",
    "reddit.com", "bebee.com",
    "waze.com", "openstreetmap", "foursquare", "yelp.",
    "remitly.com", "wise.com", "wewire.com", "transferwise.com",
    "worldremit.com", "xoom.com", "moneygram.com", "westernunion.com",
    "paysend.com",
    "rekrute.com", "emploi.ma", "anapec.org", "bayt.com", "indeed.com",
    "glassdoor.com", "welcometothejungle.com", "jobzyn.com", "monster.com",
    "bghit-nekhdem", "drh.ma",
    "apps.apple.com", "play.google.com",
    "lbankalik.ma",
    "remittanceprices.worldbank.org",
    "xe.com", "qonto.com", "globaldata.com", "euroquity.com", "viguier.com",
    "wikipedia.org", "fsma.be",
    "greenclimate.fund", "eib.org", "hps-worldwide.com",
    "royalairmaroc.com", "airarabia.com",
    "prnewswire.com", "businesswire.com",
)

BLOCKED_HOSTPATH_SUBSTRINGS = ("x.com/", "google.com/maps")

WHITELISTED_HOST_SUFFIXES = ("ir.attijariwafabank.com", "attijaricib.com")

BLOCKED_HOSTS: set[str] = set()

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


# --- URL / host helpers ------------------------------------------------------

def mentions_atw(*fields: str) -> bool:
    for f in fields:
        if f and ATW_TOKEN_RE.search(f):
            return True
    return False


def host_blocked(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if not host:
            return False
        if any(host == w or host.endswith("." + w) or host == w
               for w in WHITELISTED_HOST_SUFFIXES):
            return False
        if host in BLOCKED_HOSTS:
            return True
        if any(sub in host for sub in BLOCKED_HOST_SUBSTRINGS):
            return True
        path = (parsed.path or "").lower()
        if "/" in path[1:]:
            first_seg = path[:path.index("/", 1) + 1]
        else:
            first_seg = path + "/"
        hostpath = f"{host}{first_seg}"
        return any(sub in hostpath for sub in BLOCKED_HOSTPATH_SUBSTRINGS)
    except Exception:
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
    """Merge new items into the existing CSV and save. Used by standalone
    per-source runs so each source appends to the same `data/ATW_news.csv`.

    Behavior:
    - Load existing rows keyed by `url_key`.
    - For each new item, fill in full_content/date from prior row when missing.
    - Apply noise filter + dedup + signal metadata.
    - Sort by date desc.
    - Write CSV atomically (save_csv does full rewrite).

    Returns the total row count after merge.
    """
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


# --- Body enrichment ---------------------------------------------------------

def enrich_with_bodies(
    articles: list[dict],
    limit: Optional[int] = None,
    existing: Optional[dict[str, dict]] = None,
    failed_urls: Optional[set[str]] = None,
    gnews_cache: Optional[dict[str, str]] = None,
    state: Optional[dict] = None,
    save_every: int = 20,
) -> list[dict]:
    total = len(articles)
    existing = existing or {}
    failed_urls = failed_urls or set()
    gnews_cache = gnews_cache if gnews_cache is not None else {}
    logger.info(
        "Enriching %d articles (resolve Google News + fetch body)%s",
        total,
        f", body limit={limit}" if limit is not None else "",
    )
    kept: list[dict] = []
    fetched = 0
    reused = 0
    skipped_failed = 0
    for idx, a in enumerate(articles, 1):
        url = a.get("url", "")
        if not url:
            kept.append(a)
            continue

        if "news.google.com" in url:
            cached = gnews_cache.get(url)
            if cached:
                final_url = cached
            else:
                final_url = resolve_final_url(url)
                if final_url and "news.google.com" not in final_url:
                    gnews_cache[url] = final_url
        else:
            final_url = url
        if "news.google.com" in final_url:
            logger.debug("Dropped unresolved Google News URL: %s", final_url)
            continue
        if host_blocked(final_url):
            logger.debug("Dropped after redirect (blocked host): %s", final_url)
            continue
        a["url"] = final_url

        key = url_key(final_url)
        prior = existing.get(key)
        if prior and prior.get("full_content"):
            a["full_content"] = prior["full_content"]
            if not a.get("date"):
                cached_date = (prior.get("date") or "").strip()
                if cached_date:
                    a["date"] = cached_date
                else:
                    page_date = fetch_article_date_only(final_url)
                    if page_date:
                        a["date"] = page_date
                    time.sleep(POLITE_DELAY)
            reused += 1
        elif key in failed_urls:
            skipped_failed += 1
        elif limit is None or fetched < limit:
            body, page_date = fetch_article_body(final_url)
            if body:
                a["full_content"] = body
                if not a.get("date") and page_date:
                    a["date"] = page_date
                existing[key] = dict(a)
                fetched += 1
            else:
                failed_urls.add(key)
            time.sleep(POLITE_DELAY)

        kept.append(a)
        if idx % 10 == 0 or idx == total:
            logger.info(
                "  progress: %d/%d processed, %d fetched, %d reused, %d skipped-failed",
                idx, total, fetched, reused, skipped_failed,
            )

        if state is not None and idx % save_every == 0:
            state["failed_body_urls"] = sorted(failed_urls)
            state["gnews_resolved"] = gnews_cache
            try:
                save_state(state)
            except OSError as exc:
                logger.warning("State checkpoint failed: %s", exc)

    logger.info(
        "Enriched %d/%d articles (fetched %d new, reused %d cached, skipped %d known-failed)",
        fetched + reused, len(kept), fetched, reused, skipped_failed,
    )
    return kept

# --- END INJECTED COMMON.PY ---


import time
from typing import Optional

import requests
from bs4 import BeautifulSoup




def scrape(
    max_pages: int = 1,
    known_url_keys: Optional[set[str]] = None,
) -> list[dict]:
    endpoint = "https://aujourdhui.ma/wp-json/wp/v2/posts"
    per_page = 50
    logger.info("Direct scrape: Aujourd'hui WP REST (up to %d pages)", max_pages)
    items: list[dict] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        url = (
            f"{endpoint}?search=Attijariwafa+bank"
            f"&per_page={per_page}&page={page}&orderby=date&order=desc"
        )
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=45,
            )
            if resp.status_code == 400:
                break
            if resp.status_code != 200:
                logger.warning("HTTP %s for %s", resp.status_code, url)
                break
            posts = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Aujourd'hui REST fetch failed: %s", exc)
            break
        if not isinstance(posts, list) or not posts:
            break
        page_new = 0
        for post in posts:
            link = (post.get("link") or "").strip()
            title_html = (post.get("title") or {}).get("rendered", "") or ""
            title = fast_strip_tags(title_html)
            if not link or not title:
                continue
            if link in seen:
                continue
            seen.add(link)
            excerpt_html = (post.get("excerpt") or {}).get("rendered", "") or ""
            content_html = (post.get("content") or {}).get("rendered", "") or ""
            excerpt = fast_strip_tags(excerpt_html)
            full_content = BeautifulSoup(content_html, "lxml").get_text(" ", strip=True) if content_html else ""
            if not mentions_atw(title, excerpt) and not mentions_atw(title, full_content[:2000]):
                continue
            if known_url_keys is not None and page_new == 0:
                if url_key(link) in known_url_keys:
                    if page == 1:
                        logger.info("  -> source unchanged (top item known), skipped")
                        return []
                    logger.info("  page %d: top item known, stopping pagination", page)
                    return items
            date_raw = post.get("date_gmt") or post.get("date") or ""
            items.append({
                "date": parse_date(date_raw),
                "title": title,
                "source": "Aujourd'hui",
                "url": link,
                "snippet": excerpt,
                "full_content": full_content,
                "query_source": "direct:aujourdhui_search",
            })
            page_new += 1
        logger.info("  page %d: %d new items (of %d returned)", page, page_new, len(posts))
        if len(posts) < per_page:
            break
        time.sleep(POLITE_DELAY)
    logger.info("  -> %d items total", len(items))
    return items


if __name__ == "__main__":
    import argparse
    

    parser = argparse.ArgumentParser(description="Standalone run: Aujourd'hui ATW")
    parser.add_argument("--show", type=int, default=10, help="How many items to print")
    parser.add_argument("--max-pages", type=int, default=1, help="WP REST pages to fetch")
    parser.add_argument("--no-save", action="store_true", help="Print only; don't write CSV")
    args = parser.parse_args()

    configure_logging()
    items = scrape(max_pages=args.max_pages)
    print(f"\n=== {len(items)} items scraped ===")
    for a in items[: args.show]:
        print(f"  {(a.get('date') or '')[:10]:10}  {a.get('title','')[:90]}")
        print(f"     {a.get('url','')}")
    if not args.no_save:
        total = merge_and_save_to_csv(items, DEFAULT_OUT)
        print(f"\nCSV saved: {DEFAULT_OUT} ({total} total rows)")
