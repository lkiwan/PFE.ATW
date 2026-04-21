"""
MarketScreener Scraper V3 - With Selenium for JavaScript Rendering
===================================================================
Uses Selenium to wait for JavaScript-rendered content (Market Cap, P/E, etc.)
Then uses BeautifulSoup for fast table parsing (historical data).

Installation:
    pip install undetected-chromedriver selenium webdriver-manager

Usage:
    python scrapers/fondamental_scraper.py
"""

import csv
import re
import time
import logging
import json
import copy
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field, asdict
import argparse
import random

try:
    import undetected_chromedriver as uc
    HAS_UC = True
except ImportError:
    HAS_UC = False

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from bs4 import BeautifulSoup, Tag
    HAS_DEPENDENCIES = True
except ImportError as e:
    HAS_DEPENDENCIES = False
    print("Missing dependencies. Install with:")
    print("pip install undetected-chromedriver selenium beautifulsoup4 lxml")
    print(f"\nError: {e}")
    exit(1)

# =============================================================================
# Configuration
# =============================================================================
_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = _ROOT / "data" 
DATA_DIR.mkdir(parents=True, exist_ok=True)
ATW_OUTPUT_CSV = _ROOT / "data" / "ATW_fondamental.csv"
ATW_FUNDAMENTAL_JSON = _ROOT / "data" / "ATW_fondamental.json"
ATW_SYMBOL = "ATW"
ATW_NAME = "ATTIJARIWAFA BANK"
ATW_URL_CODE = "ATTIJARIWAFA-BANK-SA-41148801"
ATW_MERGED_JSON = DATA_DIR / f"{ATW_SYMBOL}_merged.json"
ATW_MODEL_INPUTS_JSON = _ROOT / "data" / f"{ATW_SYMBOL}_model_inputs.json"
FAST_PAGE_LOAD_TIMEOUT = 30
FAST_DOM_WAIT_TIMEOUT = 8

BASE_URL = "https://www.marketscreener.com/quote/stock"

# Used by SeleniumScraper to rotate User-Agent strings between sessions.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# Data Model
# =============================================================================

@dataclass
class StockData:
    symbol: str
    scrape_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # Price & Market Data
    price: Optional[float] = None
    market_cap: Optional[float] = None
    volume: Optional[int] = None
    high_52w: Optional[float] = None
    low_52w: Optional[float] = None
    
    # Valuation Ratios
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    price_to_book: Optional[float] = None

    # Historical Data
    hist_revenue: Dict[str, float] = field(default_factory=dict)
    hist_net_income: Dict[str, float] = field(default_factory=dict)
    hist_eps: Dict[str, float] = field(default_factory=dict)
    hist_ebitda: Dict[str, float] = field(default_factory=dict)
    hist_fcf: Dict[str, float] = field(default_factory=dict)
    hist_ocf: Dict[str, float] = field(default_factory=dict)
    hist_capex: Dict[str, float] = field(default_factory=dict)

    # Balance sheet (8-year history)
    hist_debt: Dict[str, float] = field(default_factory=dict)
    hist_cash: Dict[str, float] = field(default_factory=dict)
    hist_equity: Dict[str, float] = field(default_factory=dict)

    # Margins (8-year history, % values)
    hist_net_margin: Dict[str, float] = field(default_factory=dict)
    # MarketScreener labels this as "EBIT Margin" — renamed from hist_operating_margin
    # for accuracy (EBIT ≠ operating income for all companies, especially banks).
    hist_ebit_margin: Dict[str, float] = field(default_factory=dict)
    hist_ebitda_margin: Dict[str, float] = field(default_factory=dict)
    hist_gross_margin: Dict[str, float] = field(default_factory=dict)

    # Returns (8-year history, % values)
    hist_roe: Dict[str, float] = field(default_factory=dict)
    hist_roce: Dict[str, float] = field(default_factory=dict)

    # Valuation multiple history
    hist_ev_ebitda: Dict[str, float] = field(default_factory=dict)

    # Historical valuation multiples (from /valuation/ page "Company Valuation" table).
    # Needed for banks where EBITDA-based multiples are unavailable and the relative
    # model has to fall back to P/E, P/BV, EV/Revenue, and EV/EBIT.
    pe_ratio_hist: Dict[str, float] = field(default_factory=dict)
    pbr_hist: Dict[str, float] = field(default_factory=dict)
    ev_revenue_hist: Dict[str, float] = field(default_factory=dict)
    ev_ebit_hist: Dict[str, float] = field(default_factory=dict)

    # Historical absolute metrics (Million MAD) from the valuation page.
    capitalization_hist: Dict[str, float] = field(default_factory=dict)
    hist_ebit: Dict[str, float] = field(default_factory=dict)

    # Computed from hist_fcf / capitalization_hist, filled in post-scrape.
    fcf_yield_hist: Dict[str, float] = field(default_factory=dict)

    # Dividend per share (8-year history, MAD)
    hist_dividend_per_share: Dict[str, float] = field(default_factory=dict)

    # EPS growth %: scraped directly from MS's "EPS change" row when available,
    # falls back to YoY computation from hist_eps for any missing years.
    hist_eps_growth: Dict[str, float] = field(default_factory=dict)

    scrape_warnings: List[str] = field(default_factory=list)
    
    def validate(self) -> None:
        """Validate scraped data."""
        if self.pe_ratio and self.pe_ratio > 300:
            self.scrape_warnings.append(f"Suspicious P/E: {self.pe_ratio}")
            self.pe_ratio = None

        # EPS growth %. Prefer MarketScreener's own "EPS change" row (scraped
        # directly in scrape_finances_page) since it uses adjusted/diluted EPS
        # that may differ from a naive YoY computation. For any year that MS
        # didn't report, fall back to computing from hist_eps.
        years = sorted(self.hist_eps.keys())
        for i in range(1, len(years)):
            prev_y, curr_y = years[i - 1], years[i]
            if curr_y in self.hist_eps_growth:
                continue  # MS-reported value already present — keep it
            prev = self.hist_eps.get(prev_y)
            curr = self.hist_eps.get(curr_y)
            if prev is None or curr is None:
                continue
            if abs(prev) <= 0.01 or abs(prev) >= 1000 or abs(curr) >= 1000:
                continue
            growth = (curr - prev) / abs(prev) * 100
            if -500 < growth < 500:
                self.hist_eps_growth[curr_y] = round(growth, 2)

# =============================================================================
# Parsing Utilities
# =============================================================================

def parse_number(text: Optional[str]) -> Optional[float]:
    """
    Parse a numeric value with K/M/B/T suffixes, supporting both English
    (thousand=',', decimal='.') and French (thousand=' '/'.', decimal=',')
    formats. Returns None for inputs that don't look like a single sane number.
    """
    if not text:
        return None

    text = str(text).strip().upper()

    # Detect K/M/B/T multiplier even when followed by a currency code,
    # e.g. "83.5B MAD", "92,52 M €". The suffix must immediately follow a
    # digit and be a standalone token (not part of a word).
    mult = 1.0
    suffix_match = re.search(r'(?<=\d)\s*([KMBT])\b', text)
    if suffix_match:
        mult = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}[suffix_match.group(1)]
        text = text[:suffix_match.start()] + text[suffix_match.end():]

    # Strip currency symbols and other unit text. Keep digits, signs, separators.
    text = re.sub(r'[^\-0-9,.\s]', '', text).strip()
    if not text:
        return None

    # Defensive cap: real stock-page values never have more than ~16 digits.
    # If the text is much longer it almost certainly comes from a wide cell
    # that bundled multiple numbers together — refuse to guess.
    digit_count = sum(1 for c in text if c.isdigit())
    if digit_count == 0 or digit_count > 16:
        return None

    has_comma = ',' in text
    has_dot = '.' in text
    compact = text.replace(' ', '')

    if has_comma and has_dot:
        # Whichever separator appears LAST is the decimal point.
        if compact.rfind(',') > compact.rfind('.'):
            cleaned = compact.replace('.', '').replace(',', '.')
        else:
            cleaned = compact.replace(',', '')
    elif has_comma:
        # Pure comma. Distinguish thousand-grouping from decimal.
        if re.fullmatch(r'-?\d{1,3}(?:,\d{3})+', compact):
            cleaned = compact.replace(',', '')
        elif re.fullmatch(r'-?\d+,\d{1,3}', compact):
            cleaned = compact.replace(',', '.')
        else:
            cleaned = compact.replace(',', '')
    else:
        # Only dots (or none). Could be thousand separators or decimal.
        # Require at least 2 dot-groups (e.g. "1.234.567") to treat as
        # thousand separation. A single group like "6.185" is ambiguous
        # but almost always a decimal on MarketScreener.
        if re.fullmatch(r'-?\d{1,3}(?:\.\d{3}){2,}', compact):
            cleaned = compact.replace('.', '')
        else:
            cleaned = compact

    try:
        return float(cleaned) * mult
    except (ValueError, AttributeError):
        return None


def parse_percent(text: Optional[str]) -> Optional[float]:
    """Parse percentage values like '4.47%' or '4,47 %' -> 4.47."""
    if not text:
        return None
    cleaned = re.sub(r'[^\-0-9,.]', '', str(text)).replace(',', '.')
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


# =============================================================================
# DOM extraction helpers (key/value pairs from MarketScreener tables)
# =============================================================================

# Hard caps so a wide cell with concatenated junk can't poison parsing.
_MAX_LABEL_LEN = 60
_MAX_VALUE_LEN = 30


def _is_sane_kv(label: str, value: str) -> bool:
    if not label or not value or label == value:
        return False
    if len(label) > _MAX_LABEL_LEN or len(value) > _MAX_VALUE_LEN:
        return False
    # The value cell should look "atomic": at most one inner whitespace gap,
    # and no more than ~16 digits. Concatenated multi-number cells are noise.
    digits = sum(1 for c in value if c.isdigit())
    if digits > 16:
        return False
    return True


def extract_kv_pairs(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """
    Walk the DOM and collect every (label, value) pair where the label and
    value live in adjacent cells of a table row, or in a <dt>/<dd> pair.

    Returned as a list (not dict) so duplicate labels are preserved — useful
    when MS shows the same metric under multiple year columns.
    """
    pairs: List[Tuple[str, str]] = []

    # Tables: pair every cell with the cell immediately to its right.
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue
            for i in range(len(cells) - 1):
                label = cells[i].get_text(' ', strip=True)
                value = cells[i + 1].get_text(' ', strip=True)
                if _is_sane_kv(label, value):
                    pairs.append((label, value))

    # <dl> definition lists.
    for dl in soup.find_all('dl'):
        terms = dl.find_all('dt')
        defs = dl.find_all('dd')
        for t, d in zip(terms, defs):
            label = t.get_text(' ', strip=True)
            value = d.get_text(' ', strip=True)
            if _is_sane_kv(label, value):
                pairs.append((label, value))

    # MarketScreener also uses sibling <span>/<div> patterns inside cards,
    # e.g. <span class="c-table__field-name">Cap.</span><span ...>92.52B</span>
    for span in soup.find_all(['span', 'div']):
        cls = ' '.join(span.get('class') or [])
        if not cls:
            continue
        if 'field-name' in cls or 'label' in cls or 'title' in cls.lower():
            label = span.get_text(' ', strip=True)
            if not label or len(label) > _MAX_LABEL_LEN:
                continue
            sib = span.find_next_sibling(['span', 'div', 'td'])
            if sib:
                value = sib.get_text(' ', strip=True)
                if _is_sane_kv(label, value):
                    pairs.append((label, value))

    return pairs


def find_in_kv(pairs: List[Tuple[str, str]], label_patterns: List[str]) -> Optional[str]:
    """Return the first value whose label matches any of the given regex patterns."""
    compiled = [re.compile(p, re.IGNORECASE) for p in label_patterns]
    for label, value in pairs:
        for rx in compiled:
            if rx.search(label):
                return value
    return None


def find_all_in_kv(pairs: List[Tuple[str, str]], label_patterns: List[str]) -> List[str]:
    """Return ALL values whose label matches any of the given regex patterns,
    in original order. Useful when the first match is a scale/axis label and
    a later match is the real value."""
    compiled = [re.compile(p, re.IGNORECASE) for p in label_patterns]
    out: List[str] = []
    for label, value in pairs:
        for rx in compiled:
            if rx.search(label):
                out.append(value)
                break
    return out


# =============================================================================
# Selenium Scraper
# =============================================================================

class SeleniumScraper:
    def __init__(
        self,
        headless: bool = True,
        debug: bool = False,
        user_agent: Optional[str] = None,
        worker_id: int = 0,
        fast_mode: bool = True,
    ):
        """Initialize Selenium driver (uses undetected-chromedriver when available)."""
        self.debug = debug
        self.fast_mode = fast_mode
        ua = user_agent or random.choice(USER_AGENTS)

        # Ensure scraper sessions don't collide when creating Chrome profiles.
        import tempfile
        import os as _os
        base_tmp = Path(tempfile.gettempdir()) / f"marketscreener_uc_{worker_id}_{int(time.time())}"
        base_tmp.mkdir(parents=True, exist_ok=True)

        if HAS_UC:
            # -------------------------------------------------------
            # undetected-chromedriver: patches navigator.webdriver,
            # removes automation markers from the Chrome binary, and
            # bypasses Cloudflare/JS bot-detection used by MS.
            #
            # IMPORTANT for Windows / Chrome 112+:
            #   - Do NOT pass --headless=new as a flag — it crashes the
            #     renderer on Windows. Let UC manage headless itself via
            #     the headless= constructor parameter.
            #   - --no-sandbox and --disable-gpu also destabilise UC on
            #     Windows; they are omitted here intentionally.
            #
            # WinError 10053 / network abort fix:
            #   UC re-downloads chromedriver on every run if it thinks the
            #   cached binary is stale. On networks with strict firewalls
            #   this download gets aborted. Solution: if the patched driver
            #   already exists in the UC cache dir, pass it via
            #   driver_executable_path so UC skips the network step.
            # -------------------------------------------------------
            logger.info("🌐 Starting Chrome (undetected-chromedriver)...")
            uc_options = uc.ChromeOptions()
            uc_options.page_load_strategy = 'eager'
            uc_options.add_argument('--disable-dev-shm-usage')
            uc_options.add_argument('--blink-settings=imagesEnabled=false')
            uc_options.add_argument('--window-size=1920,1080')
            uc_options.add_argument('--lang=en-US,en')
            uc_options.add_argument(f'--user-agent={ua}')
            uc_options.add_experimental_option(
                "prefs",
                {
                    "profile.default_content_setting_values.images": 2,
                    "profile.managed_default_content_settings.images": 2,
                },
            )

            # Locate the UC cache dir (Windows: %APPDATA%\undetected_chromedriver)
            _uc_cache = Path(_os.environ.get("APPDATA", "")) / "undetected_chromedriver" / "undetected_chromedriver.exe"
            _driver_path = str(_uc_cache) if _uc_cache.exists() else None
            if _driver_path:
                logger.info(f"   Using cached UC driver: {_driver_path}")
            else:
                logger.info("   UC driver not cached yet — will auto-download once.")

            try:
                # headless= is handled natively by UC — do NOT also add
                # --headless=new to uc_options or the renderer will crash.
                self.driver = uc.Chrome(
                    options=uc_options,
                    headless=headless,
                    driver_executable_path=_driver_path,  # None = let UC download
                    user_data_dir=str(base_tmp),
                )
                # Suppress annoying WinError 6 on process shutdown (UC bug on Windows)
                _uc_class = self.driver.__class__
                if not hasattr(_uc_class, '_patched_del'):
                    _orig_del = _uc_class.__del__
                    def _silent_del(instance):
                        try:
                            _orig_del(instance)
                        except OSError as e:
                            if e.winerror != 6:
                                raise
                        except Exception:
                            pass
                    _uc_class.__del__ = _silent_del
                    _uc_class._patched_del = True

            except Exception as exc:
                logger.error(f"undetected-chromedriver failed: {exc}")
                raise
        else:
            # -------------------------------------------------------
            # Fallback: plain selenium (less stealthy, may get blocked)
            # -------------------------------------------------------
            logger.warning("⚠ undetected-chromedriver not found — falling back to plain Selenium.")
            logger.warning("  Install with: pip install undetected-chromedriver")
            chrome_options = Options()
            chrome_options.page_load_strategy = 'eager'
            if headless:
                chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--blink-settings=imagesEnabled=false')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--lang=en-US,en')
            chrome_options.add_argument(f'--user-agent={ua}')
            chrome_options.add_experimental_option(
                "prefs",
                {
                    "profile.default_content_setting_values.images": 2,
                    "profile.managed_default_content_settings.images": 2,
                },
            )
            logger.info("🌐 Starting Chrome (plain Selenium)...")
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
            except WebDriverException as exc:
                logger.error(f"Failed to start Chrome: {exc}")
                raise

        self.driver.set_page_load_timeout(FAST_PAGE_LOAD_TIMEOUT if self.fast_mode else 60)
    
    def _wait_and_get_soup(self, wait_seconds: float = 5.0) -> BeautifulSoup:
        """Wait for page body to be present, then return parsed soup."""
        try:
            WebDriverWait(self.driver, FAST_DOM_WAIT_TIMEOUT if self.fast_mode else 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            logger.warning("⚠ Body element wait timed out")
        # Give JavaScript widgets a chance to populate.
        time.sleep(wait_seconds)
        return BeautifulSoup(self.driver.page_source, 'lxml')

    def _maybe_dump_html(self, symbol: str, page_name: str) -> None:
        """Dump rendered HTML to disk when --debug is set, for inspection."""
        if not self.debug:
            return
        try:
            debug_dir = DATA_DIR / "_debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            out = debug_dir / f"{symbol}_{page_name}.html"
            out.write_text(self.driver.page_source, encoding='utf-8')
            logger.info(f"🐞 Dumped HTML → {out}")
        except Exception as exc:
            logger.warning(f"Failed to dump HTML: {exc}")

    def _parse_year_tables(
        self,
        soup: BeautifulSoup,
        label_map: List[Tuple[Any, Dict[str, float], bool]],
        growth_map: Optional[Dict[int, Dict[str, float]]] = None,
    ) -> None:
        """
        Generic year-column table parser reused across finances, ratios,
        cash-flow, and valuation pages.

        *label_map*: list of ``(regex_pattern, target_dict, is_primary)``
        *growth_map*: optional ``{id(primary_dict): growth_dict}`` for bare
        "Change"/"Growth" sub-rows that inherit context from the previous
        primary row.
        """
        if growth_map is None:
            growth_map = {}

        bare_change_re = re.compile(
            r'^\s*(?:%\s*)?'
            r'(?:change|growth|chg\.?|var\.?|variation|delta|δ|'
            r'y\s*[/\-\s]\s*y|yoy|y\-o\-y)'
            r'\s*(?:%|\(%\))?\s*$',
            re.IGNORECASE,
        )

        compiled_patterns = [(re.compile(pat, re.IGNORECASE), tgt, pri)
                             for pat, tgt, pri in label_map]
        estimate_row_re = re.compile(
            r'(?:estimate|est\.?|forecast|consensus|prevision|prévision)',
            re.IGNORECASE,
        )
        current_year = datetime.now().year

        for table in soup.find_all('table'):
            rows = list(table.find_all('tr'))
            if not rows:
                continue

            header_row = rows[0]
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

            years: List[str] = []
            year_indices: List[int] = []
            for i, h in enumerate(headers):
                if re.match(r'^20\d{2}$', h) and int(h) <= current_year:
                    years.append(h)
                    year_indices.append(i)

            if not years:
                continue

            last_primary: Optional[Dict[str, float]] = None

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                label = cells[0].get_text(strip=True).lower()
                if estimate_row_re.search(label):
                    continue
                target: Optional[Dict[str, float]] = None
                is_primary = False

                # Bare growth/change sub-row
                if bare_change_re.match(label):
                    if last_primary is not None:
                        growth_series = growth_map.get(id(last_primary))
                        if growth_series is not None:
                            target = growth_series
                else:
                    for rx, tgt, pri in compiled_patterns:
                        if rx.search(label):
                            target = tgt
                            is_primary = pri
                            break

                if target is not None:
                    for idx, year in zip(year_indices, years):
                        if idx < len(cells):
                            val = parse_number(cells[idx].get_text(strip=True))
                            if val is not None:
                                target[year] = val

                if is_primary:
                    last_primary = target

    def scrape_main_page(self, data: StockData, url_code: str) -> None:
        """Scrape main quote page with JavaScript rendering."""
        url = f"{BASE_URL}/{url_code}/"

        logger.info(f"📄 Loading {url}")
        self.driver.get(url)

        logger.info("⏳ Waiting for JavaScript to render...")
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 5.0)
        self._maybe_dump_html(data.symbol, "main")

        # Build a key/value map from the DOM. MarketScreener lays out their
        # "Key Data" / "Trading Info" widgets as <td>label</td><td>value</td>,
        # which the old regex-on-flattened-text approach couldn't reach.
        kv = extract_kv_pairs(soup)
        if self.debug:
            logger.info(f"🐞 Extracted {len(kv)} KV pairs from main page")
            for label, value in kv[:40]:
                logger.info(f"   {label!r} -> {value!r}")

        page_text = soup.get_text(' ', strip=True)

        # ----- Price -----
        # Prefer DOM-anchored extraction; fall back to regex on flattened text.
        price_value = find_in_kv(kv, [
            r'^(?:Last|Cours|Dernier)\b',
            r'^Quote\b',
        ])
        price = parse_number(price_value) if price_value else None
        if not price:
            for price_pattern in [
                r'(\d[\d\s.,]*\d)\s*MAD\b',
                r'Last\s*(?:Price|Quote)?[:\s]+(\d[\d\s.,]*\d)',
            ]:
                match = re.search(price_pattern, page_text, re.IGNORECASE)
                if match:
                    candidate = parse_number(match.group(1))
                    if candidate and 1 < candidate < 100000:
                        price = candidate
                        break
        if price and 1 < price < 100000:
            data.price = price
            logger.info(f"✓ Price: {price} MAD")

        # ----- Market Cap -----
        # MS labels: "Cap.", "Cap. boursière", "Capitalization", "Market cap."
        mcap_value = find_in_kv(kv, [
            r'^Cap\.?\s*(?:bours|market)?',
            r'^Market\s*Cap',
            r'^Capitali[sz]ation',
            r'^Capitalisation',
        ])
        if mcap_value:
            mcap = parse_number(mcap_value)
            if mcap and mcap > 1e6:
                data.market_cap = mcap
                logger.info(f"✓ Market Cap: {mcap:,.0f} MAD")

        # Regex fallback on flattened text — MS sometimes renders the cap
        # outside any KV-shaped widget (e.g. inside a header banner).
        if not data.market_cap:
            for pattern in [
                r'(?:Market\s*Cap|Cap\.?\s*bours[a-z]*|Capitali[sz]ation)[\s:]+([\d.,\s]+[KMBT]?)\s*(?:MAD|EUR|USD)?',
                r'Cap\.?[\s:]+([\d.,\s]+[KMBT])\s*(?:MAD|EUR|USD)',
            ]:
                m = re.search(pattern, page_text, re.IGNORECASE)
                if m:
                    mcap = parse_number(m.group(1))
                    if mcap and mcap > 1e6:
                        data.market_cap = mcap
                        logger.info(f"✓ Market Cap (fallback): {mcap:,.0f}")
                        break

        # ----- P/E Ratio -----
        # MS labels include the year suffix: "P/E ratio 2025", "PER 2025".
        pe_value = find_in_kv(kv, [
            r'^P\s*/\s*E\b',
            r'^PER\b',
            r'Price\s*/\s*Earnings',
        ])
        if pe_value:
            pe = parse_number(pe_value)
            if pe and 0.1 <= pe <= 300:
                data.pe_ratio = pe
                logger.info(f"✓ P/E Ratio: {pe}")

        # ----- Dividend Yield -----
        div_value = find_in_kv(kv, [
            r'^(?:Dividend\s+)?Yield\b',
            r'^Rendement',
            r'^Div\.?\s*Yield',
        ])
        if div_value:
            div_yield = parse_percent(div_value)
            if div_yield is not None and 0 <= div_yield <= 30:
                data.dividend_yield = div_yield
                logger.info(f"✓ Dividend Yield: {div_yield}%")

        # ----- Price / Book -----
        pb_value = find_in_kv(kv, [
            r'^P\s*/\s*B(?:V)?\b',
            r'^Price\s*/\s*Book',
        ])
        if pb_value:
            pb = parse_number(pb_value)
            if pb and 0.01 <= pb <= 100:
                data.price_to_book = pb
                logger.info(f"✓ P/B: {pb}")

        # ----- 52-week High/Low -----
        # MS uses a variety of labels: "52w High", "52-Week High", "1Y High",
        # "High 1 Year", "Plus haut 1 an", "Plus haut 52 sem.", "Annual High".
        high_value = find_in_kv(kv, [
            r'52[\s\-]*(?:weeks?|w)\s*high',
            r'(?:1\s*Y(?:ear)?|Annual)\s*High',
            r'High\s*1\s*Y(?:ear)?',
            r'Plus\s+haut\s+(?:1\s*an|52)',
            r'(?:Plus|Highest).*52',
        ])
        if high_value:
            high = parse_number(high_value)
            if high and data.price and 0.5 <= high / data.price <= 3:
                data.high_52w = high
                logger.info(f"✓ 52w High: {high}")

        low_value = find_in_kv(kv, [
            r'52[\s\-]*(?:weeks?|w)\s*low',
            r'(?:1\s*Y(?:ear)?|Annual)\s*Low',
            r'Low\s*1\s*Y(?:ear)?',
            r'Plus\s+bas\s+(?:1\s*an|52)',
            r'(?:Plus|Lowest).*52',
        ])
        if low_value:
            low = parse_number(low_value)
            if low and data.price and 0.3 <= low / data.price <= 2:
                data.low_52w = low
                logger.info(f"✓ 52w Low: {low}")

        # ----- Volume -----
        vol_value = find_in_kv(kv, [
            r'^Volume\b',
            r'^Vol\.?\s*(?:moyen|avg|average)?',
            r'Average\s+Volume',
            r'Volume\s+20\s*d',
        ])
        if vol_value:
            vol = parse_number(vol_value)
            if vol and vol >= 0:
                data.volume = int(vol)
                logger.info(f"✓ Volume: {vol:,.0f}")

    def scrape_finances_page(self, data: StockData, url_code: str) -> None:
        """Scrape financial tables."""
        url = f"{BASE_URL}/{url_code}/finances/"

        logger.info(f"📊 Loading financials...")
        self.driver.get(url)
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 3.0)
        self._maybe_dump_html(data.symbol, "finances")

        # The label_map order matters: specific ratios and growth rows are
        # checked BEFORE broad metric rows so that labels like "EV / Sales"
        # don't bleed into hist_revenue and "EPS change" doesn't bleed into
        # hist_eps.  Entries whose target is None are matched-and-skipped by
        # _parse_year_tables (the helper skips None targets after matching).
        # We encode them here via a sentinel empty dict that we discard.
        _skip: Dict[str, float] = {}

        label_map: List[Tuple[str, Dict[str, float], bool]] = [
            # Valuation multiples
            (r'(?:ev\s*/\s*ebitda|enterprise\s*value\s*/\s*ebitda)', data.hist_ev_ebitda, True),
            # Skip all other ratio rows with '/' (EV/Sales, P/E, P/BV, etc.)
            (r'/', _skip, False),
            # Named growth / change rows
            (r'(?:eps|earnings|bpa)\s*(?:growth|change|chg|var|croissance|variation)', data.hist_eps_growth, False),
            (r'(?:revenue|sales)\s*growth', _skip, False),
            # Per-share rows (EPS only here; DPS is sourced from valuation page)
            (r'earnings\s*per\s*share', data.hist_eps, True),
            (r'per\s*share', _skip, False),
            # Absolute metrics (income statement + cash flow)
            # Revenue is intentionally NOT read from /finances/ and is sourced
            # from /finances-income-statement/ in _override_income_statement_metrics().
            (r'(?:net\s*income|net\s*profit)(?!.*(?:margin|growth))', data.hist_net_income, True),
            (r'(?:^eps|earnings\s*per\s*share)', data.hist_eps, True),
            (r'ebitda(?!.*margin)', data.hist_ebitda, True),
            (r'(?:free\s*cash\s*flow|fcf)(?!.*(?:margin|growth|cagr|yield))', data.hist_fcf, True),
            (r'operating\s*cash\s*flow(?!.*(?:margin|growth|cagr))', data.hist_ocf, True),
            (r'(?:capex|capital\s*expenditure)(?!.*(?:margin|growth|cagr))', data.hist_capex, True),
            # Balance sheet
            (r'(?:net\s*debt|financial\s*debt|^debt$)', data.hist_debt, True),
            (r'cash(?!.*flow)(?!.*capex)', data.hist_cash, True),
            (r'(?:shareholders?\s*equity|stockholders?\s*equity|shareholders?\s*funds|^(?:equity|total\s*equity)$)(?!.*return)', data.hist_equity, True),
            # Margins/returns are intentionally NOT read from this page
            # to avoid mixed-denominator variants. We source them only from
            # finances-ratios page in scrape_ratios_page().
        ]

        growth_map = {id(data.hist_eps): data.hist_eps_growth}

        self._parse_year_tables(soup, label_map, growth_map)
        self._override_income_statement_metrics(data, url_code)

    def _override_income_statement_metrics(self, data: StockData, url_code: str) -> None:
        """
        Source-of-truth overrides from the dedicated income statement page:
        - Revenue: "Revenues Before Provision For Loan Losses" (US banks),
          with fallbacks to "Total Revenues", "Net banking income" /
          "Produit net bancaire" for European/Moroccan banks, then generic
          "Revenues" / "Net sales".
        - Net margin: Net Income / Total Revenues
        - DPS: "Dividend Per Share"
        """
        url = f"{BASE_URL}/{url_code}/finances-income-statement/"
        logger.info("📊 Loading income statement overrides...")
        self.driver.get(url)
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 3.0)
        self._maybe_dump_html(data.symbol, "income_statement")

        income_revenue: Dict[str, float] = {}
        total_revenues: Dict[str, float] = {}
        nbi_revenue: Dict[str, float] = {}
        generic_revenue: Dict[str, float] = {}
        income_dps: Dict[str, float] = {}

        # Shared exclusion suffix so none of the revenue patterns pick up
        # derived sub-rows (per-share, growth, margin, yield, change, CAGR).
        _rev_excl = (
            r"(?!.*(?:per\s*share|growth|cagr|margin|yield|change|chg\.?|"
            r"var\.?|variation|y\s*[/\-]\s*y|yoy))"
        )

        label_map: List[Tuple[str, Dict[str, float], bool]] = [
            (
                r"revenues?\s+before\s+provision(?:s)?\s+for\s+loan\s+loss(?:es)?"
                + _rev_excl,
                income_revenue,
                True,
            ),
            (
                r"(?:net\s*banking\s*income|produit\s*net\s*bancaire|^nbi$|^pnb$)"
                + _rev_excl,
                nbi_revenue,
                True,
            ),
            (r"^total\s+revenues?\b" + _rev_excl, total_revenues, True),
            (
                r"(?:^revenues?$|^net\s*sales$|^sales$|^turnover$|^chiffre\s*d[''\u2019]affaires$)"
                + _rev_excl,
                generic_revenue,
                True,
            ),
            (
                r"(?:dividend\s*per\s*share|dividende\s*par\s*action|^dps$)"
                r"(?!.*(?:growth|cagr|yield|payout))",
                income_dps,
                True,
            ),
        ]
        self._parse_year_tables(soup, label_map)

        # Income statement tables sometimes use B/M suffixes; convert to millions.
        self._normalize_to_millions(income_revenue)
        self._normalize_to_millions(total_revenues)
        self._normalize_to_millions(nbi_revenue)
        self._normalize_to_millions(generic_revenue)

        # Fall through candidates in order of bank-accuracy preference.
        revenue_source = (
            income_revenue
            or nbi_revenue
            or total_revenues
            or generic_revenue
        )
        if revenue_source:
            data.hist_revenue.clear()
            data.hist_revenue.update(revenue_source)

        # Net margin = Net Income / Total Revenues (fall back to whichever
        # revenue series actually came through).
        margin_denominator = total_revenues or revenue_source
        computed_margin: Dict[str, float] = {}
        for year, total_rev in margin_denominator.items():
            ni = data.hist_net_income.get(year)
            if ni is None or total_rev is None or abs(total_rev) <= 1e-9:
                continue
            margin = (ni / total_rev) * 100
            if -1000 < margin < 1000:
                computed_margin[year] = round(margin, 2)
        if computed_margin:
            data.hist_net_margin.clear()
            data.hist_net_margin.update(computed_margin)

        if income_dps:
            data.hist_dividend_per_share.clear()
            data.hist_dividend_per_share.update(income_dps)

    def scrape_balance_sheet_page(self, data: StockData, url_code: str) -> None:
        """
        Scrape balance-sheet equity history to fill missing prior year(s),
        especially equity_{t-1} needed for ROE (e.g. 2019 for 2020 ROE).
        """
        url = f"{BASE_URL}/{url_code}/finances-balance-sheet/"
        logger.info("📊 Loading balance sheet...")
        self.driver.get(url)
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 3.0)
        self._maybe_dump_html(data.symbol, "balance_sheet")

        tmp_equity: Dict[str, float] = {}
        label_map: List[Tuple[str, Dict[str, float], bool]] = [
            (
                r'(?:total\s*(?:common\s*)?equity|shareholders?\s*equity|'
                r'stockholders?\s*equity|shareholders?\s*funds|'
                r'^(?:equity|total\s*equity)$)(?!.*(?:growth|cagr|debt|return|ratio))',
                tmp_equity,
                True,
            ),
        ]
        self._parse_year_tables(soup, label_map)
        self._normalize_to_millions(tmp_equity)

        # Fill only missing years; keep existing validated values untouched.
        for year, value in tmp_equity.items():
            if year not in data.hist_equity:
                data.hist_equity[year] = value

    def scrape_ratios_page(self, data: StockData, url_code: str) -> None:
        """Scrape financial ratios page (margins, ROE, ROCE)."""
        url = f"{BASE_URL}/{url_code}/finances-ratios/"

        logger.info(f"📊 Loading financial ratios...")
        self.driver.get(url)
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 3.0)
        self._maybe_dump_html(data.symbol, "ratios")

        # Ratios page is the source of truth for gross/EBIT/EBITDA margins and ROE/ROCE.
        data.hist_gross_margin.clear()
        data.hist_ebit_margin.clear()
        data.hist_ebitda_margin.clear()
        data.hist_roe.clear()
        data.hist_roce.clear()

        label_map: List[Tuple[str, Dict[str, float], bool]] = [
            # Margins (exclude growth/CAGR rows like "Gross Profit, 1 Yr. Growth %")
            (r'gross\s*(?:profit\s*)?margin\s*%?(?!.*(?:growth|cagr))', data.hist_gross_margin, True),
            (r'ebit[^d]?\s*margin\s*%?(?!.*(?:growth|cagr))', data.hist_ebit_margin, True),
            (r'ebitda\s*margin\s*%?(?!.*(?:growth|cagr))', data.hist_ebitda_margin, True),
            # Returns (exclude growth/CAGR rows)
            (r'return\s*on\s*equity\s*%?(?!.*(?:growth|cagr))', data.hist_roe, True),
            (r'(?:return\s*on\s*(?:total\s*)?capital|roce)(?!.*(?:growth|cagr))', data.hist_roce, True),
        ]

        self._parse_year_tables(soup, label_map)

    @staticmethod
    def _normalize_to_millions(series: Dict[str, float]) -> None:
        """
        The cash-flow / balance-sheet page on MarketScreener displays values
        with B/M suffixes (e.g. "6.01B", "398M") which parse_number expands
        to raw MAD.  The finances page, however, stores the same metrics as
        plain numbers in **millions MAD** (footnote ¹MAD in Million).

        To keep every hist_* series in a single unit (millions MAD), convert
        any value whose absolute magnitude ≥ 1 000 000 (i.e. clearly raw MAD
        rather than millions) by dividing by 1 000 000.
        """
        for year, val in series.items():
            if abs(val) >= 1_000_000:
                series[year] = round(val / 1_000_000, 2)

    def scrape_cashflow_page(self, data: StockData, url_code: str) -> None:
        """Scrape cash flow statement page (OCF)."""
        url = f"{BASE_URL}/{url_code}/finances-cash-flow-statement/"

        logger.info(f"📊 Loading cash flow statement...")
        self.driver.get(url)
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 3.0)
        self._maybe_dump_html(data.symbol, "cashflow")

        # Collect into temporary dicts so we can normalize before merging
        # into data.* (which may already have millions-scale values from
        # the finances page).
        tmp_ocf: Dict[str, float] = {}
        tmp_fcf: Dict[str, float] = {}
        tmp_capex: Dict[str, float] = {}
        tmp_cash: Dict[str, float] = {}
        tmp_equity: Dict[str, float] = {}
        _skip: Dict[str, float] = {}

        label_map: List[Tuple[str, Dict[str, float], bool]] = [
            # Skip ratio rows containing '/' FIRST (e.g. "Debt / (EBITDA - Capex)")
            # so they don't pollute the real metric rows below.
            (r'/', _skip, False),
            # Skip growth / CAGR sub-rows
            (r'(?:growth|cagr)\s*%', _skip, False),
            (r'(?:cash\s*from\s*operations?|operating\s*cash\s*flow)(?!.*(?:growth|cagr|margin|liabilities))', tmp_ocf, True),
            (r'(?:free\s*cash\s*flow|(?:^|\b)fcf\b)(?!.*(?:growth|cagr|margin|yield))', tmp_fcf, True),
            (r'(?:capex|capital\s*expenditure)(?!.*(?:growth|cagr|margin))', tmp_capex, True),
            # Balance sheet items also appear on this page
            (r'(?:cash\s*and\s*equivalents|total\s*cash\s*and\s*short)', tmp_cash, True),
            (r'(?:total\s*(?:common\s*)?equity|shareholders?\s*equity)(?!.*(?:growth|cagr|debt|return))', tmp_equity, True),
        ]

        self._parse_year_tables(soup, label_map)

        # Normalize raw-MAD values (from B/M suffixes) → millions MAD
        for tmp in (tmp_ocf, tmp_fcf, tmp_capex, tmp_cash, tmp_equity):
            self._normalize_to_millions(tmp)

        # Merge: cash-flow page fills gaps but does NOT overwrite existing
        # values from the finances page (which are already in millions).
        for src, dst in [
            (tmp_ocf, data.hist_ocf),
            (tmp_fcf, data.hist_fcf),
            (tmp_capex, data.hist_capex),
            (tmp_cash, data.hist_cash),
            (tmp_equity, data.hist_equity),
        ]:
            for year, val in src.items():
                if year not in dst:
                    dst[year] = val

    def scrape_valuation_page(self, data: StockData, url_code: str) -> None:
        """Scrape valuation page for Price-to-Book."""
        url = f"{BASE_URL}/{url_code}/valuation/"

        logger.info(f"📊 Loading valuation page...")
        self.driver.get(url)
        soup = self._wait_and_get_soup(wait_seconds=1.5 if self.fast_mode else 3.0)
        self._maybe_dump_html(data.symbol, "valuation")

        # Try KV extraction first (summary/sidebar)
        if data.price_to_book is None:
            kv = extract_kv_pairs(soup)
            pb_value = find_in_kv(kv, [
                r'Price\s*to\s*book\s*value',
                r'^P\s*/\s*B(?:V|R)?\b',
                r'^PBR\b',
                r'^Price\s*/\s*Book',
            ])
            if pb_value:
                pb = parse_number(pb_value)
                if pb and 0.01 <= pb <= 100:
                    data.price_to_book = pb
                    logger.info(f"✓ P/B (valuation page KV): {pb}")

        # Fallback: parse year-column table for PBR / P/BV row
        if data.price_to_book is None:
            temp_pbv: Dict[str, float] = {}
            label_map: List[Tuple[str, Dict[str, float], bool]] = [
                (r'(?:price\s*to\s*book|p\s*/\s*bv|pbr\b)', temp_pbv, True),
            ]
            self._parse_year_tables(soup, label_map)
            if temp_pbv:
                current_year = str(datetime.now().year)
                candidates = {y: v for y, v in temp_pbv.items() if y <= current_year}
                if candidates:
                    latest = max(candidates.keys())
                    pb = candidates[latest]
                    if 0.01 <= pb <= 100:
                        data.price_to_book = pb
                        logger.info(f"✓ P/B (valuation table, {latest}): {pb}")

        # DPS fallback: only if income-statement override didn't populate it.
        if not data.hist_dividend_per_share:
            temp_dps: Dict[str, float] = {}
            label_map_dps: List[Tuple[str, Dict[str, float], bool]] = [
                (r'(?:dividend\s*per\s*share|dividende\s*par\s*action|^dps$)(?!.*(?:growth|cagr|yield|payout))', temp_dps, True),
            ]
            self._parse_year_tables(soup, label_map_dps)
            if temp_dps:
                data.hist_dividend_per_share.update(temp_dps)

        # Also fill EV/EBITDA history if not already populated
        if not data.hist_ev_ebitda:
            label_map_ev: List[Tuple[str, Dict[str, float], bool]] = [
                (r'(?:ev\s*/\s*ebitda|enterprise\s*value\s*/\s*ebitda)', data.hist_ev_ebitda, True),
            ]
            self._parse_year_tables(soup, label_map_ev)

        # Parse the "Company Valuation" table for bank-friendly multiples and
        # absolute metrics that DCF / Monte Carlo / Relative Valuation need.
        # Order matters — specific labels first, anchored so "EBIT" doesn't
        # swallow "EV / EBIT" and "Capitalization" doesn't swallow
        # "Capitalization / Revenue".
        # Labels may carry unit suffixes or footnote markers
        # (e.g. "Capitalization (M MAD)", "EBIT¹"), so these use word-boundary
        # matching instead of $-anchors. Specific multi-token patterns still
        # come first so "Capitalization / Revenue" and "EV / EBIT" don't bleed
        # into the bare "Capitalization" and "EBIT" patterns below.
        label_map_company_valuation: List[Tuple[str, Dict[str, float], bool]] = [
            (r'^\s*ev\s*/\s*ebit(?!d)', data.ev_ebit_hist, True),
            (r'^\s*capitali[sz]ation\s*/\s*revenue', data.ev_revenue_hist, True),
            (r'^\s*p\s*/\s*e(?!v)', data.pe_ratio_hist, True),
            (r'^\s*pbr(?!\w)', data.pbr_hist, True),
            (r'^\s*capitali[sz]ation(?!\s*/)', data.capitalization_hist, True),
            (r'^\s*ebit(?!da)(?!.*margin)', data.hist_ebit, True),
        ]
        self._parse_year_tables(soup, label_map_company_valuation)

        # Drop spurious zero values — MarketScreener renders "0x" for multiples
        # that don't apply to banks (EV/EBIT, etc.). Treat them as null.
        for multiples_dict in (
            data.pe_ratio_hist,
            data.pbr_hist,
            data.ev_revenue_hist,
            data.ev_ebit_hist,
        ):
            for year in list(multiples_dict.keys()):
                if multiples_dict[year] == 0:
                    multiples_dict.pop(year, None)

        # Capitalization and EBIT may come through with B/M suffixes expanded
        # to raw MAD; collapse to millions so they match the rest of the
        # hist_* series and the model layer.
        self._normalize_to_millions(data.capitalization_hist)
        self._normalize_to_millions(data.hist_ebit)

    # ------------------------------------------------------------------
    # Rate-limit / bot-challenge detection
    # ------------------------------------------------------------------
    _RATE_LIMIT_MARKERS = (
        "just a moment",          # Cloudflare challenge
        "verify you are human",
        "access denied",
        "rate limit",
        "too many requests",
        "temporarily blocked",
        "captcha",
    )

    def looks_rate_limited(self) -> bool:
        try:
            title = (self.driver.title or "").lower()
            body = self.driver.find_element(By.TAG_NAME, "body").text[:2000].lower()
        except Exception:
            return False
        haystack = f"{title}\n{body}"
        return any(marker in haystack for marker in self._RATE_LIMIT_MARKERS)

    def hard_clear_session(self) -> None:
        """
        Wipe cookies + localStorage + sessionStorage + HTTP cache so
        MarketScreener treats us as a brand-new visitor. Used when we hit
        the site's daily article/view limit — clearing the session is what
        actually resets the quota counter.
        """
        try:
            self.driver.delete_all_cookies()
        except Exception as exc:
            logger.warning(f"   delete_all_cookies failed: {exc}")
        try:
            # Storage clears require being on a same-origin page.
            self.driver.get("https://www.marketscreener.com/")
            self.driver.execute_script(
                "try { window.localStorage.clear(); } catch(e) {}"
                "try { window.sessionStorage.clear(); } catch(e) {}"
            )
        except Exception as exc:
            logger.warning(f"   storage clear failed: {exc}")
        # CDP-level cache + cookie wipe (Chrome-only, best effort).
        for cmd in ("Network.clearBrowserCookies", "Network.clearBrowserCache"):
            try:
                self.driver.execute_cdp_cmd(cmd, {})
            except Exception:
                pass
        logger.info("   🧹 cleared cookies + storage + cache")

    def _handle_rate_limit(self, page_name: str) -> None:
        """Clear cookies/cache and wait when rate-limited."""
        logger.warning(f"⚠ Rate-limited on {page_name} — clearing session...")
        self.hard_clear_session()
        wait = random.uniform(10, 20)
        logger.info(f"   ⏳ Waiting {wait:.0f}s before retrying...")
        time.sleep(wait)

    @staticmethod
    def _keep_reported_years_only(data: StockData) -> None:
        """Drop non-year labels and future years to avoid forecast columns."""
        cutoff_year = datetime.now().year
        series_list = [
            data.hist_revenue,
            data.hist_net_income,
            data.hist_eps,
            data.hist_ebitda,
            data.hist_ebit,
            data.hist_fcf,
            data.hist_ocf,
            data.hist_capex,
            data.hist_debt,
            data.hist_cash,
            data.hist_equity,
            data.hist_net_margin,
            data.hist_ebit_margin,
            data.hist_ebitda_margin,
            data.hist_gross_margin,
            data.hist_roe,
            data.hist_roce,
            data.hist_ev_ebitda,
            data.pe_ratio_hist,
            data.pbr_hist,
            data.ev_revenue_hist,
            data.ev_ebit_hist,
            data.capitalization_hist,
            data.fcf_yield_hist,
            data.hist_dividend_per_share,
            data.hist_eps_growth,
        ]
        for series in series_list:
            for key in list(series.keys()):
                key_str = str(key)
                if not re.fullmatch(r"20\d{2}", key_str):
                    series.pop(key, None)
                    continue
                if int(key_str) > cutoff_year:
                    series.pop(key, None)

    @staticmethod
    def _compute_derived_series(data: StockData) -> None:
        """
        Fill in the derived series that the valuation models expect:
          hist_fcf       = hist_ocf - |hist_capex|   (per-year, when both known)
          fcf_yield_hist = hist_fcf / capitalization_hist
          hist_revenue   = capitalization_hist / ev_revenue_hist (fallback)

        hist_fcf is filled only for years the scraper did not already capture
        a direct FCF value — preserving scraped values where they exist.
        """
        for year, ocf in data.hist_ocf.items():
            if year in data.hist_fcf:
                continue
            capex = data.hist_capex.get(year)
            if capex is None:
                continue
            data.hist_fcf[year] = round(ocf - abs(capex), 2)

        for year, fcf in data.hist_fcf.items():
            cap = data.capitalization_hist.get(year)
            if not cap or cap <= 0 or fcf is None:
                continue
            data.fcf_yield_hist[year] = round(fcf / cap, 4)

        # Sanity check: hist_revenue for a bank of ATW's scale should be in
        # the thousands of Million MAD. If the max scraped value is below 100
        # it's almost certainly a per-share or percentage sub-row that slipped
        # past the label filter — discard it and derive from cap / EV-Revenue.
        if data.hist_revenue:
            max_rev = max(abs(v) for v in data.hist_revenue.values() if v is not None)
            if max_rev < 100:
                data.hist_revenue.clear()

        if not data.hist_revenue and data.capitalization_hist and data.ev_revenue_hist:
            for year, cap in data.capitalization_hist.items():
                mult = data.ev_revenue_hist.get(year)
                if not mult or mult <= 0 or not cap or cap <= 0:
                    continue
                data.hist_revenue[year] = round(cap / mult, 2)

    @staticmethod
    def _recompute_roe_average_equity(data: StockData) -> None:
        """ROE = Net Income / average equity ((E_t + E_t-1)/2)."""
        if not data.hist_net_income or not data.hist_equity:
            return

        existing_roe = dict(data.hist_roe)
        recomputed: Dict[str, float] = {}
        for year, ni in data.hist_net_income.items():
            if ni is None:
                continue
            try:
                y = int(str(year))
            except ValueError:
                continue

            eq_t = data.hist_equity.get(str(y))
            eq_prev = data.hist_equity.get(str(y - 1))

            # If prior-year equity is missing, preserve already scraped ROE.
            if eq_t is None or eq_prev is None:
                if year in existing_roe and existing_roe[year] is not None:
                    recomputed[year] = existing_roe[year]
                continue

            avg_equity = (eq_t + eq_prev) / 2
            if abs(avg_equity) <= 1e-9:
                continue
            roe = (ni / avg_equity) * 100
            if -1000 < roe < 1000:
                recomputed[year] = round(roe, 2)

        if recomputed:
            data.hist_roe.clear()
            data.hist_roe.update(recomputed)

    def scrape(self, symbol: str, url_code: str) -> StockData:
        """Periodic fundamentals scrape (annual/semiannual/quarterly tables, no estimates)."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping {symbol} — PERIODIC fundamentals mode")
        logger.info(f"{'='*60}")

        data = StockData(symbol=symbol)

        pages = [
            ("finances",  self.scrape_finances_page),
            ("ratios",    self.scrape_ratios_page),
            ("cashflow",  self.scrape_cashflow_page),
            ("balance_sheet", self.scrape_balance_sheet_page),
            ("valuation", self.scrape_valuation_page),
        ]

        for page_name, scrape_fn in pages:
            scrape_fn(data, url_code)

            if self.looks_rate_limited():
                self._handle_rate_limit(page_name)
                # Retry the same page once after clearing
                scrape_fn(data, url_code)
                if self.looks_rate_limited():
                    data.scrape_warnings.append(f"Rate-limited on {page_name} (gave up after retry)")
                    logger.error(f"   Still rate-limited on {page_name} after clear — skipping remaining pages")
                    break

            time.sleep(random.uniform(1.5, 3.0))

        self._keep_reported_years_only(data)
        self._compute_derived_series(data)
        self._recompute_roe_average_equity(data)
        data.validate()

        return data

    def scrape_daily(self, symbol: str, url_code: str, existing_json_path: Optional[Path] = None) -> StockData:
        """
        Backward-compatible entrypoint.
        Now runs the same periodic fundamentals scrape as scrape().
        """
        _ = existing_json_path
        return self.scrape(symbol, url_code)

    def close(self):
        """Close browser."""
        logger.info("🔒 Closing browser...")
        try:
            self.driver.quit()
        except Exception:
            pass

# =============================================================================
# Main
# =============================================================================

def _safe_print(text: str) -> None:
    """Print text safely on Windows CP1252 terminals — replaces unencodable chars."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode(errors='replace').decode('ascii', errors='replace'))


def _print_summary(stock_data: StockData, output_file: Path) -> None:
    _safe_print(f"\n[OK] Completed {stock_data.symbol}")
    _safe_print(f"   Price: {stock_data.price} MAD" if stock_data.price else "   Price: N/A")
    _safe_print(f"   Market Cap: {stock_data.market_cap:,.0f} MAD" if stock_data.market_cap else "   Market Cap: N/A")
    _safe_print(f"   P/E: {stock_data.pe_ratio}" if stock_data.pe_ratio else "   P/E: N/A")
    _safe_print(f"   P/B: {stock_data.price_to_book}" if stock_data.price_to_book else "   P/B: N/A")
    _safe_print(f"   Div Yield: {stock_data.dividend_yield}%" if stock_data.dividend_yield else "   Div: N/A")
    _safe_print(f"   Revenue: {len(stock_data.hist_revenue)} years")
    _safe_print(f"   EBITDA: {len(stock_data.hist_ebitda)} years")
    _safe_print(f"   Net Income: {len(stock_data.hist_net_income)} years")
    _safe_print(f"   EPS: {len(stock_data.hist_eps)} years | growth: {len(stock_data.hist_eps_growth)} YoY points")
    _safe_print(f"   DPS: {len(stock_data.hist_dividend_per_share)} years")
    _safe_print(f"   Debt: {len(stock_data.hist_debt)} years | Cash: {len(stock_data.hist_cash)} years | Equity: {len(stock_data.hist_equity)} years")
    _safe_print(f"   OCF: {len(stock_data.hist_ocf)} years")
    _safe_print(f"   Margins (gross/net/ebit/ebitda): {len(stock_data.hist_gross_margin)}/{len(stock_data.hist_net_margin)}/{len(stock_data.hist_ebit_margin)}/{len(stock_data.hist_ebitda_margin)} years")
    _safe_print(f"   ROE: {len(stock_data.hist_roe)} years | ROCE: {len(stock_data.hist_roce)} years | EV/EBITDA: {len(stock_data.hist_ev_ebitda)} years")

    # Scalar fields + historical series (each present series counts as 1).
    scalar_checks = [
        bool(stock_data.price), bool(stock_data.market_cap),
        bool(stock_data.pe_ratio), bool(stock_data.price_to_book),
        bool(stock_data.dividend_yield),
        bool(stock_data.high_52w), bool(stock_data.low_52w),
    ]
    history_checks = [
        bool(stock_data.hist_revenue), bool(stock_data.hist_net_income),
        bool(stock_data.hist_eps), bool(stock_data.hist_ebitda),
        bool(stock_data.hist_fcf), bool(stock_data.hist_ocf),
        bool(stock_data.hist_capex),
        bool(stock_data.hist_debt), bool(stock_data.hist_cash), bool(stock_data.hist_equity),
        bool(stock_data.hist_gross_margin),
        bool(stock_data.hist_net_margin), bool(stock_data.hist_ebit_margin),
        bool(stock_data.hist_ebitda_margin),
        bool(stock_data.hist_roe), bool(stock_data.hist_roce),
        bool(stock_data.hist_ev_ebitda),
        bool(stock_data.hist_dividend_per_share),
        bool(stock_data.hist_eps_growth),
    ]
    total_fields = len(scalar_checks) + len(history_checks)
    filled = sum(scalar_checks) + sum(history_checks)
    quality = (filled / total_fields) * 100 if total_fields else 0
    _safe_print(f"   Data Quality: {quality:.0f}% ({filled}/{total_fields})")
    _safe_print(f"   Saved to: {output_file.name}")


def _save_atw_fondamental_json(stock_data: StockData, output_file: Path) -> None:
    """Write ATW fundamentals-only JSON (no realtime merge)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": stock_data.symbol,
        "scrape_timestamp": stock_data.scrape_timestamp,
        "pe_ratio": stock_data.pe_ratio,
        "dividend_yield": stock_data.dividend_yield,
        "price_to_book": stock_data.price_to_book,
        "hist_revenue": stock_data.hist_revenue,
        "hist_net_income": stock_data.hist_net_income,
        "hist_eps": stock_data.hist_eps,
        "hist_ebitda": stock_data.hist_ebitda,
        "hist_fcf": stock_data.hist_fcf,
        "hist_ocf": stock_data.hist_ocf,
        "hist_capex": stock_data.hist_capex,
        "hist_debt": stock_data.hist_debt,
        "hist_cash": stock_data.hist_cash,
        "hist_equity": stock_data.hist_equity,
        "hist_net_margin": stock_data.hist_net_margin,
        "hist_ebit_margin": stock_data.hist_ebit_margin,
        "hist_ebitda_margin": stock_data.hist_ebitda_margin,
        "hist_gross_margin": stock_data.hist_gross_margin,
        "hist_roe": stock_data.hist_roe,
        "hist_roce": stock_data.hist_roce,
        "hist_ev_ebitda": stock_data.hist_ev_ebitda,
        "pe_ratio_hist": stock_data.pe_ratio_hist,
        "pbr_hist": stock_data.pbr_hist,
        "ev_revenue_hist": stock_data.ev_revenue_hist,
        "ev_ebit_hist": stock_data.ev_ebit_hist,
        "capitalization_hist": stock_data.capitalization_hist,
        "hist_ebit": stock_data.hist_ebit,
        "fcf_yield_hist": stock_data.fcf_yield_hist,
        "hist_dividend_per_share": stock_data.hist_dividend_per_share,
        "hist_eps_growth": stock_data.hist_eps_growth,
        "scrape_warnings": stock_data.scrape_warnings,
        "data_source": {
            "marketscreener_periodic": True,
            "merged_with_realtime": False,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    payload = _prune_empty_values(payload)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from database import AtwDatabase
        with AtwDatabase() as db:
            snap, yearly = db.save_fondamental(payload)
            print(f"  DB: fondamental_snapshot +{snap}, fondamental_yearly +{yearly}")
    except Exception as e:
        print(f"  DB save skipped: {e}")


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (dict, list, tuple, set)):
        return len(value) == 0
    return False


def _prune_empty_values(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, item in value.items():
            pruned = _prune_empty_values(item)
            if not _is_empty_value(pruned):
                cleaned[key] = pruned
        return cleaned
    if isinstance(value, list):
        cleaned_list = [_prune_empty_values(item) for item in value]
        return [item for item in cleaned_list if not _is_empty_value(item)]
    return value


def _already_scraped_this_month(output_file: Path) -> bool:
    if not output_file.exists():
        return False
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
        ts_raw = payload.get("scrape_timestamp")
        if not ts_raw:
            return False
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return ts.year == now.year and ts.month == now.month
    except Exception:
        return False


def _save_atw_fondamental_csv(stock_data: StockData, output_file: Path) -> None:
    """Write a single-row ATW fundamentals CSV (overwrites each run)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "scrape_timestamp": stock_data.scrape_timestamp,
        "symbol": stock_data.symbol,
        "price": stock_data.price,
        "market_cap": stock_data.market_cap,
        "volume": stock_data.volume,
        "high_52w": stock_data.high_52w,
        "low_52w": stock_data.low_52w,
        "pe_ratio": stock_data.pe_ratio,
        "dividend_yield": stock_data.dividend_yield,
        "price_to_book": stock_data.price_to_book,
        "hist_revenue_json": json.dumps(stock_data.hist_revenue, ensure_ascii=False, sort_keys=True),
        "hist_net_income_json": json.dumps(stock_data.hist_net_income, ensure_ascii=False, sort_keys=True),
        "hist_eps_json": json.dumps(stock_data.hist_eps, ensure_ascii=False, sort_keys=True),
        "hist_ebitda_json": json.dumps(stock_data.hist_ebitda, ensure_ascii=False, sort_keys=True),
        "hist_fcf_json": json.dumps(stock_data.hist_fcf, ensure_ascii=False, sort_keys=True),
        "hist_ocf_json": json.dumps(stock_data.hist_ocf, ensure_ascii=False, sort_keys=True),
        "hist_capex_json": json.dumps(stock_data.hist_capex, ensure_ascii=False, sort_keys=True),
        "hist_debt_json": json.dumps(stock_data.hist_debt, ensure_ascii=False, sort_keys=True),
        "hist_cash_json": json.dumps(stock_data.hist_cash, ensure_ascii=False, sort_keys=True),
        "hist_equity_json": json.dumps(stock_data.hist_equity, ensure_ascii=False, sort_keys=True),
        "hist_net_margin_json": json.dumps(stock_data.hist_net_margin, ensure_ascii=False, sort_keys=True),
        "hist_ebit_margin_json": json.dumps(stock_data.hist_ebit_margin, ensure_ascii=False, sort_keys=True),
        "hist_ebitda_margin_json": json.dumps(stock_data.hist_ebitda_margin, ensure_ascii=False, sort_keys=True),
        "hist_gross_margin_json": json.dumps(stock_data.hist_gross_margin, ensure_ascii=False, sort_keys=True),
        "hist_roe_json": json.dumps(stock_data.hist_roe, ensure_ascii=False, sort_keys=True),
        "hist_roce_json": json.dumps(stock_data.hist_roce, ensure_ascii=False, sort_keys=True),
        "hist_ev_ebitda_json": json.dumps(stock_data.hist_ev_ebitda, ensure_ascii=False, sort_keys=True),
        "hist_dividend_per_share_json": json.dumps(stock_data.hist_dividend_per_share, ensure_ascii=False, sort_keys=True),
        "hist_eps_growth_json": json.dumps(stock_data.hist_eps_growth, ensure_ascii=False, sort_keys=True),
        "scrape_warnings_json": json.dumps(stock_data.scrape_warnings, ensure_ascii=False),
    }

    with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (ValueError, TypeError):
        return None


def _load_bourse_market_overrides(symbol: str) -> Optional[Dict[str, Any]]:
    """Load ATW market overrides from Bourse Casa daily CSV."""
    csv_path = _ROOT / "data" / f"{symbol}_bourse_casa_full.csv"
    if not csv_path.exists():
        return None

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    if not rows:
        return None

    header_map = {h.strip().lower(): h for h in fieldnames if h}

    def pick_col(*names: str) -> Optional[str]:
        for name in names:
            col = header_map.get(name.lower())
            if col:
                return col
        return None

    date_col = pick_col("Séance", "seance", "date")
    close_col = pick_col("Dernier Cours", "close", "courscourant")
    high_col = pick_col("+haut du jour", "high", "highprice")
    low_col = pick_col("+bas du jour", "low", "lowprice")
    vol_col = pick_col("Volume des échanges", "volume", "cumultitresechanges")
    mcap_col = pick_col("Capitalisation", "capitalisation", "market_cap")

    parsed_rows: List[Dict[str, Any]] = []
    for row in rows:
        raw_date = (row.get(date_col) or "") if date_col else ""
        try:
            trade_date = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d")
        except ValueError:
            continue
        parsed_rows.append(
            {
                "date": trade_date,
                "close": _safe_float(row.get(close_col)) if close_col else None,
                "high": _safe_float(row.get(high_col)) if high_col else None,
                "low": _safe_float(row.get(low_col)) if low_col else None,
                "volume": _safe_float(row.get(vol_col)) if vol_col else None,
                "market_cap": _safe_float(row.get(mcap_col)) if mcap_col else None,
            }
        )

    if not parsed_rows:
        return None

    parsed_rows.sort(key=lambda x: x["date"])
    latest = parsed_rows[-1]
    trailing = parsed_rows[-252:]
    highs = [r["high"] for r in trailing if r["high"] is not None]
    lows = [r["low"] for r in trailing if r["low"] is not None]

    return {
        "price": latest["close"],
        "volume": int(latest["volume"]) if latest["volume"] is not None else None,
        "market_cap": latest["market_cap"],
        "high_52w": max(highs) if highs else None,
        "low_52w": min(lows) if lows else None,
    }


def _apply_market_overrides(stock_data: StockData, overrides: Optional[Dict[str, Any]]) -> None:
    """Apply Bourse Casa market fields to scraped payload."""
    if not overrides:
        return
    if overrides.get("price") is not None:
        stock_data.price = overrides["price"]
    if overrides.get("volume") is not None:
        stock_data.volume = overrides["volume"]
    if overrides.get("market_cap") is not None:
        stock_data.market_cap = overrides["market_cap"]
    if overrides.get("high_52w") is not None:
        stock_data.high_52w = overrides["high_52w"]
    if overrides.get("low_52w") is not None:
        stock_data.low_52w = overrides["low_52w"]


def _to_model_inputs(stock_data: StockData) -> Dict[str, Any]:
    return {
        "identity": {
            "ticker": stock_data.symbol,
            "full_name": ATW_NAME,
            "exchange": "Casablanca Stock Exchange",
            "currency": "MAD",
        },
        "price_performance": {
            "last_price": stock_data.price,
            "high_52w": stock_data.high_52w,
            "low_52w": stock_data.low_52w,
            "volume": stock_data.volume,
        },
        "financials": {
            "net_sales": dict(stock_data.hist_revenue),
            "revenues": dict(stock_data.hist_revenue),
            "net_income": dict(stock_data.hist_net_income),
            "eps": dict(stock_data.hist_eps),
            "ebit": dict(stock_data.hist_ebit),
            "ebitda": dict(stock_data.hist_ebitda),
            "free_cash_flow": dict(stock_data.hist_fcf),
            "operating_cash_flow": dict(stock_data.hist_ocf),
            "capex": dict(stock_data.hist_capex),
            "total_debt": dict(stock_data.hist_debt),
            "cash_and_equivalents": dict(stock_data.hist_cash),
            "shareholders_equity": dict(stock_data.hist_equity),
            "net_margin": dict(stock_data.hist_net_margin),
            "ebit_margin": dict(stock_data.hist_ebit_margin),
            "ebitda_margin": dict(stock_data.hist_ebitda_margin),
            "gross_margin": dict(stock_data.hist_gross_margin),
            "roe": dict(stock_data.hist_roe),
            "roce": dict(stock_data.hist_roce),
            "dividend_per_share": dict(stock_data.hist_dividend_per_share),
        },
        "valuation": {
            "market_cap": stock_data.market_cap,
            "pe_ratio": stock_data.pe_ratio,
            "price_to_book": stock_data.price_to_book,
            "dividend_yield": stock_data.dividend_yield,
            "ev_ebitda_hist": dict(stock_data.hist_ev_ebitda),
            "pe_ratio_hist": dict(stock_data.pe_ratio_hist),
            "pbr_hist": dict(stock_data.pbr_hist),
            "ev_revenue_hist": dict(stock_data.ev_revenue_hist),
            "ev_ebit_hist": dict(stock_data.ev_ebit_hist),
            "capitalization_hist": dict(stock_data.capitalization_hist),
            "fcf_yield_hist": dict(stock_data.fcf_yield_hist),
            "dividend_per_share_hist": dict(stock_data.hist_dividend_per_share),
            "eps_growth_hist": dict(stock_data.hist_eps_growth),
        },
    }


def normalize_stock_data(raw_data: dict) -> dict:
    """Normalize scraped data to consistent units."""
    data = copy.deepcopy(raw_data)
    _normalize_financials(data.get("financials", {}))
    _normalize_valuation(data.get("valuation", {}))
    _derive_missing_values(data)
    return data


def _normalize_financials(fin: dict) -> None:
    if not fin:
        return

    full_mad_fields = [
        "revenues", "cost_of_sales", "gross_profit", "operating_income",
        "ebitda", "ebit", "total_assets", "total_liabilities",
        "shareholders_equity", "cash_and_equivalents", "total_debt",
        "working_capital", "dividends_paid",
    ]

    for field_name in full_mad_fields:
        field_data = fin.get(field_name)
        if not isinstance(field_data, dict):
            continue
        for year, value in field_data.items():
            if value is not None and abs(value) > 100_000:
                field_data[year] = value / 1_000_000

    _normalize_mixed_field(fin, "net_income", "net_margin", "net_sales")
    _reconstruct_net_debt(fin)
    _reconstruct_fcf(fin)
    _reconstruct_capex(fin)
    _reconstruct_ocf(fin)


def _normalize_mixed_field(fin: dict, field_name: str, margin_field: str = None, revenue_field: str = "net_sales") -> None:
    field_data = fin.get(field_name)
    if not isinstance(field_data, dict):
        return

    net_sales = fin.get(revenue_field, {})
    if not net_sales:
        return

    revenue_values = [v for v in net_sales.values() if v is not None and v > 100]
    ref_revenue = statistics.median(revenue_values) if revenue_values else 0

    for year, value in field_data.items():
        if value is None:
            continue
        abs_val = abs(value)
        if abs_val < 100 and ref_revenue > 1000:
            if margin_field and margin_field in fin:
                margin = fin[margin_field].get(year)
                rev = net_sales.get(year)
                if margin is not None and rev is not None:
                    field_data[year] = margin * rev / 100
        elif abs_val > 1_000_000:
            field_data[year] = value / 1_000_000


def _reconstruct_net_debt(fin: dict) -> None:
    nd = fin.get("net_debt")
    debt = fin.get("total_debt", {})
    cash = fin.get("cash_and_equivalents", {})
    if not isinstance(nd, dict):
        return

    for year, value in nd.items():
        if value is not None and abs(value) < 100:
            d = debt.get(year)
            c = cash.get(year)
            if d is not None and c is not None:
                nd[year] = d - c


def _reconstruct_fcf(fin: dict) -> None:
    fcf = fin.get("free_cash_flow")
    ebitda = fin.get("ebitda", {})
    capex_data = fin.get("capex", {})
    net_sales = fin.get("net_sales", {})
    if not isinstance(fcf, dict):
        return

    for year, value in fcf.items():
        if value is not None and abs(value) < 100:
            eb = ebitda.get(year)
            rev = net_sales.get(year)
            if eb and eb > 100:
                cx = capex_data.get(year)
                if cx and cx > 100:
                    capex_val = cx
                elif rev:
                    capex_val = rev * 0.15
                else:
                    capex_val = 0
                fcf[year] = eb * 0.69 - capex_val


def _reconstruct_capex(fin: dict) -> None:
    capex = fin.get("capex")
    net_sales = fin.get("net_sales", {})
    if not isinstance(capex, dict):
        return

    for year, value in capex.items():
        if value is not None and abs(value) < 100:
            rev = net_sales.get(year)
            if rev and rev > 100:
                capex[year] = value * rev / 100


def _reconstruct_ocf(fin: dict) -> None:
    ocf = fin.get("operating_cash_flow")
    net_sales = fin.get("net_sales", {})
    if not isinstance(ocf, dict):
        return

    for year, value in ocf.items():
        if value is not None and abs(value) < 10:
            rev = net_sales.get(year)
            if rev and rev > 100:
                ocf[year] = value * rev


def _normalize_valuation(val: dict) -> None:
    if not val:
        return

    for field in ["market_cap", "enterprise_value"]:
        if val.get(field) and val[field] > 1_000_000:
            val[field] = val[field] / 1_000_000

    if val.get("num_shares"):
        val["num_shares_actual"] = val["num_shares"] * 1000


def _derive_missing_values(data: dict) -> None:
    fin = data.get("financials", {})
    val = data.get("valuation", {})
    price = data.get("price_performance", {})

    if not fin.get("eps") or not any(fin["eps"].values()):
        net_income = fin.get("net_income", {})
        num_shares = val.get("num_shares_actual") or val.get("num_shares", 0) * 1000
        if net_income and num_shares:
            fin["eps"] = {}
            for year, ni in net_income.items():
                if ni is not None:
                    fin["eps"][year] = (ni * 1_000_000) / num_shares

    equity = fin.get("shareholders_equity", {})
    num_shares = val.get("num_shares_actual") or val.get("num_shares", 0) * 1000
    if equity and num_shares:
        fin["book_value_per_share"] = {}
        for year, eq in equity.items():
            if eq is not None:
                fin["book_value_per_share"][year] = (eq * 1_000_000) / num_shares

    ebit = fin.get("ebit", {})
    net_income_dict = fin.get("net_income", {})
    if ebit and net_income_dict:
        fin["interest_expense_approx"] = {}
        for year in ebit:
            e = ebit.get(year)
            ni = net_income_dict.get(year)
            if e is not None and ni is not None:
                fin["interest_expense_approx"][year] = max(0, e - ni / 0.69)

    net_debt = fin.get("net_debt", {})
    ebitda = fin.get("ebitda", {})
    if net_debt and ebitda:
        fin["net_debt_to_ebitda"] = {}
        for year in net_debt:
            nd = net_debt.get(year)
            eb = ebitda.get(year)
            if nd is not None and eb is not None and eb != 0:
                fin["net_debt_to_ebitda"][year] = nd / eb

    data["current_price"] = price.get("last_price")


def _normalize_model_inputs(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return normalize_stock_data(payload)
    except Exception as exc:
        logger.warning(f"Could not apply normalizer: {exc}")
        return payload


def _save_merged_json(stock_data: StockData, market_overrides: Optional[Dict[str, Any]]) -> None:
    merged_flat = asdict(stock_data)
    merged_flat["data_source"] = {
        "marketscreener_v3": True,
        "bourse_casa": bool(market_overrides),
        "merged_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(ATW_MERGED_JSON, "w", encoding="utf-8") as f:
        json.dump(merged_flat, f, indent=2, ensure_ascii=False, default=str)


def _save_model_inputs_json(stock_data: StockData) -> None:
    model_inputs = _to_model_inputs(stock_data)
    normalized_inputs = _normalize_model_inputs(model_inputs)
    with open(ATW_MODEL_INPUTS_JSON, "w", encoding="utf-8") as f:
        json.dump(normalized_inputs, f, indent=2, ensure_ascii=False, default=str)


def main():
    parser = argparse.ArgumentParser(description='ATW monthly fundamentals scraper (no realtime merge)')
    parser.add_argument('--headful', action='store_true', help='Show browser (not headless)')
    parser.add_argument('--debug', action='store_true', help='Dump rendered HTML and KV pairs to data/historical/_debug/')
    parser.add_argument('--slow', action='store_true',
                        help='Disable fast mode (longer waits, normal loading).')
    parser.add_argument('--force', action='store_true',
                        help='Force a new scrape even if this month is already saved.')
    args = parser.parse_args()

    if not args.force and _already_scraped_this_month(ATW_FUNDAMENTAL_JSON):
        logger.info("Monthly fundamentals already scraped for this month. Use --force to run again.")
        return

    scraper: Optional[SeleniumScraper] = SeleniumScraper(
        headless=not args.headful,
        debug=args.debug,
        fast_mode=not args.slow,
    )

    try:
        symbol = ATW_SYMBOL
        url_code = ATW_URL_CODE
        mode_label = "PERIODIC"
        speed_label = "FAST" if not args.slow else "SLOW"

        logger.info(f"\n▶ Scraping {symbol} — {ATW_NAME} ({mode_label}, {speed_label}, MONTHLY)")
        stock_data = scraper.scrape(symbol, url_code)

        # Main required output: fundamentals-only monthly JSON.
        _save_atw_fondamental_json(stock_data, ATW_FUNDAMENTAL_JSON)
        _print_summary(stock_data, ATW_FUNDAMENTAL_JSON)

    finally:
        if scraper is not None:
            scraper.close()


if __name__ == "__main__":
    main()
