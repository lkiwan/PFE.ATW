"""L'Economiste search — direct article URLs for Attijariwafa.

Strategy (in order):
1. Try WordPress REST API (`/wp-json/wp/v2/posts?search=...`) — clean JSON.
2. Fall back to structured HTML parsing of the search results page,
   paginated via `?s=attijariwafa&paged=N`.
3. Fall back to loose-link scrape so the source never returns zero if
   the theme changes.
"""
from __future__ import annotations

import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag

from news_crawler_V2.common import (
    POLITE_DELAY,
    REQUEST_TIMEOUT,
    USER_AGENT,
    enrich_bodies,
    fetch,
    logger,
    parse_date,
    parse_french_date,
    run_cli,
    url_key,
)

LE_BASE = "https://www.leconomiste.com"
LE_SEARCH = f"{LE_BASE}/?s=attijariwafa"
LE_WP_REST = f"{LE_BASE}/wp-json/wp/v2/posts"

_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_SLASH_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


def _extract_date_near(node: Optional[Tag]) -> str:
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
    el = node.find(attrs={"class": re.compile(r"(excerpt|summary|field-content|teaser)", re.IGNORECASE)})
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
        heading = block.find(["h1", "h2", "h3", "h4"])
        link_tag = heading.find("a", href=True) if heading else block.find("a", href=True)
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
    items = _try_wp_rest() or _scrape_html()
    if known_url_keys is not None and items and url_key(items[0]["url"]) in known_url_keys:
        logger.info("  -> source unchanged (top item known), skipped")
        return []
    logger.info("  -> %d items", len(items))
    enriched = enrich_bodies(items)
    logger.info("  -> enriched full_content for %d/%d items", enriched, len(items))
    return items


if __name__ == "__main__":
    run_cli(scrape, "L'Economiste")
