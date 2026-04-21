"""L'Economiste search — direct article URLs for Attijariwafa.

Strategy (in order):
1. Try WordPress REST API (`/wp-json/wp/v2/posts?search=...`) — clean JSON.
2. Fall back to structured HTML parsing of the search results page,
   paginated via `?s=attijariwafa&page=N` or `&paged=N`.
3. Fall back to loose-link scrape (current legacy behavior) so the source
   never returns zero if the theme changes.

Search is already keyword-filtered server-side, so we drop the redundant
`mentions_atw` filter.
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


# --- END INJECTED COMMON.PY ---


import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag


LE_BASE = "https://www.leconomiste.com"
LE_SEARCH = f"{LE_BASE}/?s=attijariwafa"
LE_WP_REST = f"{LE_BASE}/wp-json/wp/v2/posts"

_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_SLASH_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


def _extract_date_near(node: Optional[Tag]) -> str:
    """Pull a date out of a result block."""
    if node is None:
        return ""
    time_tag = node.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
        parsed = parse_date(dt) or parse_french_date(dt)
        if parsed:
            return parsed[:10]
    date_el = node.find(attrs={"class": re.compile(r"date", re.IGNORECASE)})
    if date_el:
        txt = date_el.get_text(" ", strip=True)
        parsed = parse_date(txt) or parse_french_date(txt)
        if parsed:
            return parsed[:10]
    block_text = node.get_text(" ", strip=True)
    parsed = parse_french_date(block_text)
    if parsed:
        return parsed
    iso_m = _ISO_DATE_RE.search(block_text)
    if iso_m:
        return iso_m.group(1)
    slash_m = _SLASH_DATE_RE.search(block_text)
    if slash_m:
        d, m, y = slash_m.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"
    return ""


def _extract_snippet_near(node: Optional[Tag]) -> str:
    if node is None:
        return ""
    for sel in (
        {"class": re.compile(r"(excerpt|summary|field-content|teaser)", re.IGNORECASE)},
        None,
    ):
        if sel is not None:
            el = node.find(attrs=sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt and len(txt) >= 30:
                    return txt[:400]
    for p in node.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt and len(txt) >= 30:
            return txt[:400]
    return ""


def _is_article_url(href: str) -> bool:
    if "leconomiste.com/" not in href:
        return False
    if any(seg in href for seg in ("/search/", "/?s=", "/tag/", "/tags/", "/category/", "/categories/")):
        return False
    return True


def _try_wp_rest(max_pages: int = 3) -> list[dict]:
    items: list[dict] = []
    per_page = 50
    for page in range(1, max_pages + 1):
        try:
            r = requests.get(
                LE_WP_REST,
                params={
                    "search": "attijariwafa",
                    "per_page": per_page,
                    "page": page,
                    "orderby": "date",
                    "order": "desc",
                    "_fields": "id,date,link,title,excerpt",
                },
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            logger.info("  WP REST failed: %s", exc)
            return []
        if r.status_code == 400:
            break
        if r.status_code != 200:
            logger.info("  WP REST page %d: HTTP %s (falling back to HTML)", page, r.status_code)
            return []
        try:
            batch = r.json()
        except ValueError:
            return []
        if not isinstance(batch, list) or not batch:
            break
        for p in batch:
            link = (p.get("link") or "").strip()
            title_html = (p.get("title") or {}).get("rendered", "") or ""
            excerpt_html = (p.get("excerpt") or {}).get("rendered", "") or ""
            title = BeautifulSoup(title_html, "html.parser").get_text(" ", strip=True)
            excerpt = BeautifulSoup(excerpt_html, "html.parser").get_text(" ", strip=True)
            if not link or not title:
                continue
            items.append({
                "date": parse_date(p.get("date", "")),
                "title": title,
                "source": "L'Economiste",
                "url": link,
                "snippet": excerpt,
                "full_content": "",
                "query_source": "direct:leconomiste_wp",
            })
        if len(batch) < per_page:
            break
        time.sleep(POLITE_DELAY)
    return items


def _parse_structured_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    seen: set[str] = set()
    containers = soup.find_all(
        ["article", "div", "li"],
        class_=re.compile(r"(views-row|search-result|post|article|result|entry|teaser)", re.IGNORECASE),
    )
    for block in containers:
        heading_link = block.find(["h1", "h2", "h3", "h4"])
        link_tag = heading_link.find("a", href=True) if heading_link else block.find("a", href=True)
        if not link_tag:
            continue
        href = link_tag["href"]
        if not _is_article_url(href):
            continue
        title = link_tag.get_text(" ", strip=True)
        if not title or len(title) < 15:
            continue
        link = href if href.startswith("http") else f"{LE_BASE}{href if href.startswith('/') else '/' + href}"
        if link in seen:
            continue
        seen.add(link)
        date = _extract_date_near(block)
        if not date:
            continue
        items.append({
            "date": date,
            "title": title,
            "source": "L'Economiste",
            "url": link,
            "snippet": _extract_snippet_near(block),
            "full_content": "",
            "query_source": "direct:leconomiste_search",
        })
    return items


def _parse_loose_html(html: str) -> list[dict]:
    """Legacy fallback: iterate all <a> tags."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not _is_article_url(href):
            continue
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 15:
            continue
        link = href if href.startswith("http") else f"{LE_BASE}{href if href.startswith('/') else '/' + href}"
        if link in seen:
            continue
        seen.add(link)
        date = _extract_date_near(a.parent)
        if not date:
            continue
        items.append({
            "date": date,
            "title": title,
            "source": "L'Economiste",
            "url": link,
            "snippet": "",
            "full_content": "",
            "query_source": "direct:leconomiste_search",
        })
    return items


def _scrape_html(max_pages: int = 3) -> list[dict]:
    items: list[dict] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        url = LE_SEARCH if page == 1 else f"{LE_SEARCH}&paged={page}"
        html = fetch(url)
        if not html:
            break
        batch = _parse_structured_html(html)
        if not batch and page == 1:
            batch = _parse_loose_html(html)
        new_count = 0
        for it in batch:
            if it["url"] in seen:
                continue
            seen.add(it["url"])
            items.append(it)
            new_count += 1
        logger.info("  page %d: %d new items", page, new_count)
        if new_count == 0:
            break
        time.sleep(POLITE_DELAY)
    return items


def scrape(known_url_keys: Optional[set[str]] = None) -> list[dict]:
    logger.info("Direct scrape: L'Economiste search (%s)", LE_SEARCH)
    items = _try_wp_rest()
    if not items:
        items = _scrape_html()
    if known_url_keys is not None and items:
        if url_key(items[0]["url"]) in known_url_keys:
            logger.info("  -> source unchanged (top item known), skipped")
            return []
    logger.info("  -> %d items", len(items))

    enriched = 0
    for it in items:
        if it.get("full_content"):
            continue
        body, body_date = fetch_article_body(it["url"])
        if body:
            it["full_content"] = body
            enriched += 1
        if not it.get("date") and body_date:
            it["date"] = body_date
        time.sleep(POLITE_DELAY)
    logger.info("  -> enriched full_content for %d/%d items", enriched, len(items))
    return items


if __name__ == "__main__":
    import argparse
    

    parser = argparse.ArgumentParser(description="Standalone run: L'Economiste ATW")
    parser.add_argument("--show", type=int, default=10, help="How many items to print")
    parser.add_argument("--no-save", action="store_true", help="Print only; don't write CSV")
    args = parser.parse_args()

    configure_logging()
    items = scrape()
    print(f"\n=== {len(items)} items scraped ===")
    for a in items[: args.show]:
        print(f"  {(a.get('date') or '')[:10]:10}  {a.get('title','')[:90]}")
        print(f"     {a.get('url','')}")
    if not args.no_save:
        total = merge_and_save_to_csv(items, DEFAULT_OUT)
        print(f"\nCSV saved: {DEFAULT_OUT} ({total} total rows)")
        try:
            import sys as _sys
            _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from database import AtwDatabase
            with AtwDatabase() as _db:
                ins, enr = _db.save_news(items)
                print(f"DB: +{ins} new, {enr} enriched with full_content")
        except Exception as _e:
            print(f"DB save skipped: {_e}")
