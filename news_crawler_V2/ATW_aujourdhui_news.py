"""Aujourd'hui Le Maroc — WP REST search for Attijariwafa."""
from __future__ import annotations

import html
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from news_crawler_V2.common import (
    POLITE_DELAY,
    USER_AGENT,
    logger,
    mentions_atw,
    parse_date,
    run_cli,
    url_key,
)

ENDPOINT = "https://aujourdhui.ma/wp-json/wp/v2/posts"

_TAG_RE = re.compile(r"<[^>]+>")


def _fast_strip_tags(text: str) -> str:
    if not text:
        return ""
    return html.unescape(_TAG_RE.sub("", text)).strip()


def scrape(
    max_pages: int = 1,
    known_url_keys: Optional[set[str]] = None,
) -> list[dict]:
    logger.info("Direct scrape: Aujourd'hui WP REST (up to %d pages)", max_pages)
    per_page = 50
    items: list[dict] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        url = (
            f"{ENDPOINT}?search=Attijariwafa+bank"
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
            title = _fast_strip_tags((post.get("title") or {}).get("rendered", "") or "")
            if not link or not title or link in seen:
                continue
            seen.add(link)

            excerpt = _fast_strip_tags((post.get("excerpt") or {}).get("rendered", "") or "")
            content_html = (post.get("content") or {}).get("rendered", "") or ""
            full_content = (
                BeautifulSoup(content_html, "lxml").get_text(" ", strip=True) if content_html else ""
            )
            if not mentions_atw(title, excerpt) and not mentions_atw(title, full_content[:2000]):
                continue

            if known_url_keys is not None and page_new == 0 and url_key(link) in known_url_keys:
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
    run_cli(scrape, "Aujourd'hui")
