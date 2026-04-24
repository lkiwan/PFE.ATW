"""MarketScreener quote page — ATW news rows."""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup, Tag

from news_crawler_V2.common import (
    enrich_bodies,
    fetch,
    logger,
    parse_date,
    parse_french_date,
    run_cli,
    url_key,
)

MS_BASE = "https://www.marketscreener.com"
MS_PAGE = f"{MS_BASE}/quote/stock/ATTIJARIWAFA-BANK-SA-41148801/news/"

_NEWS_LINK_RE = re.compile(r"/news/.*-(?:\d{6,}|[a-f0-9]{15,})", re.IGNORECASE)
_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_SLASH_DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")


def _extract_row_date(anchor: Tag) -> str:
    row = anchor.find_parent("tr") or anchor.find_parent("div") or anchor.parent
    if row is None:
        return ""
    for el in row.find_all(attrs={"data-utc-date": True}):
        parsed = parse_date(el.get("data-utc-date"))
        if parsed:
            return parsed[:10]
    time_tag = row.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
        parsed = parse_date(dt)
        if parsed:
            return parsed[:10]
    row_text = row.get_text(" ", strip=True)
    iso_m = _ISO_DATE_RE.search(row_text)
    if iso_m:
        return iso_m.group(1)
    slash_m = _SLASH_DATE_RE.search(row_text)
    if slash_m:
        d, m, y = slash_m.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"
    return parse_french_date(row_text) or ""


def scrape(known_url_keys: Optional[set[str]] = None) -> list[dict]:
    logger.info("Direct scrape: MarketScreener ATW news (%s)", MS_PAGE)
    page_html = fetch(MS_PAGE)
    if not page_html:
        return []
    soup = BeautifulSoup(page_html, "html.parser")

    items: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not _NEWS_LINK_RE.search(href):
            continue
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 15:
            continue
        link = href if href.startswith("http") else f"{MS_BASE}{href}"
        if link in seen:
            continue
        seen.add(link)
        if known_url_keys is not None and not items and url_key(link) in known_url_keys:
            logger.info("  -> source unchanged (top item known), skipped")
            return []
        items.append({
            "date": _extract_row_date(a),
            "title": title,
            "source": "MarketScreener",
            "url": link,
            "snippet": "",
            "full_content": "",
            "query_source": "direct:marketscreener_atw_news",
        })

    logger.info("  -> %d items", len(items))
    enriched = enrich_bodies(items)
    logger.info("  -> enriched full_content for %d/%d items", enriched, len(items))
    return items


if __name__ == "__main__":
    run_cli(scrape, "MarketScreener")
