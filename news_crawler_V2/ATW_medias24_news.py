"""Medias24 Le Boursier — ATW fiche-action news feed."""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

from news_crawler_V2.common import (
    enrich_bodies,
    fetch,
    logger,
    run_cli,
    url_key,
)

LEBOURSIER_ATW_URL = (
    "https://medias24.com/leboursier/fiche-action"
    "?action=attijariwafa-bank&valeur=actualites"
)

_ARTICLE_URL_RE = re.compile(r"^https?://medias24\.com/\d{4}/\d{2}/\d{2}/[^/?#]+/?$")
_URL_DATE_RE = re.compile(r"medias24\.com/(\d{4})/(\d{2})/(\d{2})/")


def _title_from_slug(url: str) -> str:
    m = re.search(r"/\d{4}/\d{2}/\d{2}/([^/?#]+)", url)
    if not m:
        return ""
    slug = m.group(1).rstrip("-").replace("-", " ").strip()
    return slug[:1].upper() + slug[1:] if slug else ""


def _url_date(url: str) -> str:
    m = _URL_DATE_RE.search(url)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""


def scrape(known_url_keys: Optional[set[str]] = None) -> list[dict]:
    logger.info("Direct scrape: Medias24 Le Boursier (%s)", LEBOURSIER_ATW_URL)
    html = fetch(LEBOURSIER_ATW_URL)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")

    links: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not _ARTICLE_URL_RE.match(href) or href in seen:
            continue
        seen.add(href)
        links.append(href)

    logger.info("  -> %d article links found", len(links))

    if known_url_keys is not None and links and url_key(links[0]) in known_url_keys:
        logger.info("  -> source unchanged (top item known), skipped")
        return []

    items: list[dict] = [
        {
            "date": _url_date(link),
            "title": _title_from_slug(link),
            "source": "Medias24",
            "url": link,
            "snippet": "",
            "full_content": "",
            "query_source": "direct:medias24_leboursier",
        }
        for link in links
    ]

    enriched = enrich_bodies(items)
    logger.info("  -> enriched full_content for %d/%d items", enriched, len(items))
    return items


if __name__ == "__main__":
    run_cli(scrape, "Medias24")
