"""Boursenews stock page — ATW ticker news."""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup, Tag

from news_crawler_V2.common import (
    abs_url,
    enrich_bodies,
    fetch,
    logger,
    parse_date,
    parse_french_date,
    run_cli,
    url_key,
)

BOURSENEWS_BASE = "https://boursenews.ma"
BOURSENEWS_PAGE = f"{BOURSENEWS_BASE}/action/attijariwafa-bank"

_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_SLASH_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


def _extract_date_near(anchor: Tag) -> str:
    node: Optional[Tag] = anchor
    for _ in range(5):
        if node is None:
            break
        time_tag = node.find("time") if hasattr(node, "find") else None
        if time_tag:
            dt_attr = time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
            parsed = parse_date(dt_attr) or parse_french_date(dt_attr)
            if parsed:
                return parsed[:10]
        date_span = (
            node.find(attrs={"class": re.compile(r"date", re.IGNORECASE)})
            if hasattr(node, "find") else None
        )
        if date_span:
            txt = date_span.get_text(" ", strip=True)
            parsed = parse_date(txt) or parse_french_date(txt)
            if parsed:
                return parsed[:10]
        block_text = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
        if block_text:
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
        node = node.parent if hasattr(node, "parent") else None
    return ""


def _extract_snippet_near(anchor: Tag) -> str:
    node = anchor.parent
    for _ in range(3):
        if node is None:
            break
        p = node.find("p") if hasattr(node, "find") else None
        if p:
            txt = p.get_text(" ", strip=True)
            if txt and len(txt) >= 30:
                return txt[:400]
        node = node.parent if hasattr(node, "parent") else None
    return ""


def scrape(known_url_keys: Optional[set[str]] = None) -> list[dict]:
    logger.info("Direct scrape: Boursenews stock (%s)", BOURSENEWS_PAGE)
    page_html = fetch(BOURSENEWS_PAGE)
    if not page_html:
        return []
    soup = BeautifulSoup(page_html, "html.parser")

    items: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/article/" not in href:
            continue
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 15:
            continue
        link = abs_url(BOURSENEWS_BASE, href)
        if link in seen:
            continue
        seen.add(link)
        if known_url_keys is not None and not items and url_key(link) in known_url_keys:
            logger.info("  -> source unchanged (top item known), skipped")
            return []
        date = _extract_date_near(a)
        if not date:
            continue
        items.append({
            "date": date,
            "title": title,
            "source": "Boursenews",
            "url": link,
            "snippet": _extract_snippet_near(a),
            "full_content": "",
            "query_source": "direct:boursenews_stock",
        })

    logger.info("  -> %d items", len(items))
    enriched = enrich_bodies(items)
    logger.info("  -> enriched full_content for %d/%d items", enriched, len(items))
    return items


if __name__ == "__main__":
    run_cli(scrape, "Boursenews")
