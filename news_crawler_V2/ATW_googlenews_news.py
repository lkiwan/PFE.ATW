"""Google News RSS — Attijariwafa queries (multi-locale)."""
from __future__ import annotations

import time
from typing import Optional
from urllib.parse import quote_plus, urlparse

import feedparser
from bs4 import BeautifulSoup

from news_crawler_V2.common import (
    POLITE_DELAY,
    fetch,
    logger,
    mentions_atw,
    parse_date,
    run_cli,
)

GOOGLE_NEWS_QUERIES = [
    ('"Attijariwafa bank" -site:attijariwafa.com -site:attijariwafabank.com', "fr", "MA", "MA:fr"),
    ('"Attijariwafa" -site:attijariwafa.com -site:attijariwafabank.com', "fr", "MA", "MA:fr"),
    ('"Attijariwafa bank" -site:attijariwafa.com -site:attijariwafabank.com', "en", "US", "US:en"),
]

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


def _host_blocked(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if not host:
            return False
        if any(host == w or host.endswith("." + w) for w in WHITELISTED_HOST_SUFFIXES):
            return False
        if any(sub in host for sub in BLOCKED_HOST_SUBSTRINGS):
            return True
        path = (parsed.path or "").lower()
        first_seg = path[:path.index("/", 1) + 1] if "/" in path[1:] else path + "/"
        return any(sub in f"{host}{first_seg}" for sub in BLOCKED_HOSTPATH_SUBSTRINGS)
    except Exception:
        return False


def _fetch_one(query: str, hl: str, gl: str, ceid: str) -> list[dict]:
    url = (
        "https://news.google.com/rss/search"
        f"?q={quote_plus(query)}&hl={hl}&gl={gl}&ceid={ceid}"
    )
    logger.info("Google News RSS: %s [%s]", query, ceid)
    content = fetch(url)
    if not content:
        return []
    parsed = feedparser.parse(content)
    items: list[dict] = []
    for entry in parsed.entries:
        title = entry.get("title") or ""
        link = entry.get("link") or ""
        if _host_blocked(link):
            continue
        if not mentions_atw(title, entry.get("summary", "")):
            continue
        source_field = entry.get("source", {})
        source = (
            source_field.get("title")
            if isinstance(source_field, dict)
            else (urlparse(link).hostname or "Google News")
        )
        items.append({
            "date": parse_date(entry.get("published_parsed") or entry.get("published")),
            "title": title.strip(),
            "source": source,
            "url": link,
            "snippet": BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(" ", strip=True)[:400],
            "full_content": "",
            "query_source": f"google_news:{ceid}",
        })
    logger.info("  -> %d ATW-matching items", len(items))
    return items


def scrape(known_url_keys: Optional[set[str]] = None) -> list[dict]:
    items: list[dict] = []
    for query, hl, gl, ceid in GOOGLE_NEWS_QUERIES:
        items.extend(_fetch_one(query, hl, gl, ceid))
        time.sleep(POLITE_DELAY)
    return items


if __name__ == "__main__":
    run_cli(scrape, "Google News")
