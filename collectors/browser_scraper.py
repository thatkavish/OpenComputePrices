"""
Browser-based scraper base class using Playwright.

Renders JS-heavy pricing pages with headless Chromium and extracts
pricing data from the fully rendered DOM. Used for providers whose
pricing pages are entirely client-side rendered.
"""

import json
import logging
import re
from html import unescape
from typing import List, Dict, Any, Optional

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

# Lazy import — only load Playwright when actually needed
_playwright_available = None


def _check_playwright():
    global _playwright_available
    if _playwright_available is None:
        try:
            from playwright.sync_api import sync_playwright
            _playwright_available = True
        except ImportError:
            _playwright_available = False
            logger.warning("Playwright not installed. Run: pip install playwright && python -m playwright install chromium")
    return _playwright_available


class BrowserScraper(BaseCollector):
    """
    Base class for Playwright-based scrapers.

    Subclasses implement:
        url: str                          — page to render
        parse_page(html: str) -> list     — extract rows from rendered HTML
        wait_selector: str (optional)     — CSS selector to wait for before extracting
        wait_timeout: int (optional)      — ms to wait for selector (default 15000)
    """

    url: str = ""
    wait_selector: str = ""
    wait_timeout: int = 15000

    def collect(self) -> List[Dict[str, Any]]:
        if not _check_playwright():
            logger.warning(f"[{self.name}] Skipping — Playwright not available")
            return []

        if not self.url:
            return []

        logger.info(f"[{self.name}] Rendering {self.url} with headless browser")

        html = self._render_page(self.url)
        if not html:
            return []

        rows = self.parse_page(html)
        logger.info(f"[{self.name}] Extracted {len(rows)} rows")
        return rows

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        """Override in subclass to extract pricing rows from rendered HTML."""
        raise NotImplementedError

    def _render_page(self, url: str) -> Optional[str]:
        """Render a page with headless Chromium and return the full HTML."""
        from playwright.sync_api import sync_playwright

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                )
                page = context.new_page()

                # Block heavy resources to speed up rendering
                page.route("**/*.{png,jpg,jpeg,gif,svg,webp,woff,woff2,ttf,eot}", lambda route: route.abort())
                page.route("**/analytics*", lambda route: route.abort())
                page.route("**/gtag*", lambda route: route.abort())
                page.route("**/google-analytics*", lambda route: route.abort())

                page.goto(url, wait_until="domcontentloaded", timeout=45000)

                if self.wait_selector:
                    try:
                        page.wait_for_selector(self.wait_selector, timeout=self.wait_timeout)
                    except Exception:
                        logger.debug(f"[{self.name}] Selector '{self.wait_selector}' not found, continuing anyway")

                # Small extra wait for dynamic content
                page.wait_for_timeout(2000)

                try:
                    self._last_render_text = page.locator("body").inner_text(timeout=5000)
                except Exception:
                    self._last_render_text = ""
                html = page.content()
                browser.close()
                return html
        except Exception as e:
            logger.error(f"[{self.name}] Browser render failed: {e}")
            return None

    @staticmethod
    def extract_tables(html: str) -> List[List[List[str]]]:
        """Extract all HTML tables as lists of rows of cells."""
        tables = []
        for table_match in re.findall(r"<table[^>]*>(.*?)</table>", html, re.S):
            rows = []
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table_match, re.S):
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
                cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
                cells = [re.sub(r"\s+", " ", c).strip() for c in cells]
                if any(cells):
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_text_lines(self, html: str) -> List[str]:
        """Return rendered body text lines, falling back to lightweight HTML text extraction."""
        text = getattr(self, "_last_render_text", "") or self.html_to_text(html)
        lines = []
        for line in text.splitlines():
            line = line.replace("\xa0", " ")
            line = re.sub(r"[ \r\f\v]+", " ", line).strip()
            if line:
                lines.append(line)
        return lines

    @staticmethod
    def html_to_text(html: str) -> str:
        text = re.sub(r"<(script|style|noscript)[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
        text = re.sub(r"</(?:p|div|li|tr|td|th|h[1-6]|span|button)>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        return unescape(text)

    @staticmethod
    def extract_gpu_price_pairs(html: str) -> List[Dict[str, Any]]:
        """Extract (GPU name, price) pairs from rendered HTML using common patterns."""
        results = []
        seen = set()

        # Pattern 1: GPU name near $/hr or $/GPU-hr price
        for match in re.finditer(
            r'((?:NVIDIA\s+)?(?:H100|H200|A100|B200|GB200|L40S?|L4|A40|A10G?|V100|T4|RTX\s*[\w ]+|MI300X?|K80|P100))'
            r'[^$<]{0,300}'
            r'\$([\d.]+)\s*(?:/\s*(?:GPU[- ]?hr|hr|hour|mo))?',
            html, re.I | re.S
        ):
            gpu_raw = match.group(1).strip()
            price_str = match.group(2)
            try:
                price = float(price_str)
            except ValueError:
                continue
            if price <= 0 or price > 500:
                continue
            key = (gpu_raw.upper(), price)
            if key not in seen:
                seen.add(key)
                results.append({"gpu": gpu_raw, "price": price})

        # Pattern 2: price then GPU name
        for match in re.finditer(
            r'\$([\d.]+)\s*(?:/\s*(?:GPU[- ]?hr|hr|hour))?'
            r'[^<]{0,200}'
            r'((?:NVIDIA\s+)?(?:H100|H200|A100|B200|L40S?|L4|A40|V100|T4|RTX\s*[\w]+|MI300X?))',
            html, re.I | re.S
        ):
            price_str = match.group(1)
            gpu_raw = match.group(2).strip()
            try:
                price = float(price_str)
            except ValueError:
                continue
            if price <= 0 or price > 500:
                continue
            key = (gpu_raw.upper(), price)
            if key not in seen:
                seen.add(key)
                results.append({"gpu": gpu_raw, "price": price})

        return results
