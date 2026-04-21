"""
GetDeploying.com GPU pricing aggregator scraper.

Scrapes the cheapest-gpu-cloud guide page which contains HTML tables
with GPU model, VRAM, cheapest price, cheapest provider, and total providers.
Also scrapes individual GPU pages for per-provider pricing breakdowns.
No authentication required.
"""

import json
import logging
import re
import urllib.request
from html import unescape
from html.parser import HTMLParser
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

BASE_URL = "https://getdeploying.com"
INDEX_URL = f"{BASE_URL}/guides/cheapest-gpu-cloud"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Known GPU page slugs on getdeploying.com
GPU_PAGES = [
    "nvidia-h100", "nvidia-a100", "nvidia-h200", "nvidia-l40s",
    "nvidia-a10", "nvidia-l4", "nvidia-a40", "nvidia-t4",
    "nvidia-v100", "nvidia-rtx-4090", "nvidia-rtx-3090",
    "nvidia-rtx-a6000", "nvidia-rtx-a5000", "nvidia-rtx-4080",
    "nvidia-rtx-3080", "nvidia-rtx-3070",
    "nvidia-b200", "nvidia-gb200",
    "amd-mi300x",
]


class _HTMLTextExtractor(HTMLParser):
    """Extract visible text from HTML fragments without leaking tag attributes."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: List[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in ("script", "style"):
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style") and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth and data:
            self._parts.append(data)

    def text(self) -> str:
        return " ".join(self._parts)


def _html_to_text(fragment: str) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(fragment or "")
    parser.close()
    return re.sub(r"\s+", " ", unescape(parser.text())).strip()


def _clean_gpu_detail(detail: str) -> str:
    cleaned = _html_to_text(detail)
    cleaned = re.sub(r"^((?:\d+)x\s+[A-Za-z0-9.+-]+)\s+\1\b", r"\1", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _clean_provider_name(provider_name: str) -> str:
    return re.sub(r"\s+", " ", _html_to_text(provider_name)).strip()


def _clean_billing_text(billing: str) -> str:
    cleaned = _html_to_text(billing)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_html_table(html: str) -> List[List[str]]:
    """Extract rows from the first HTML table found."""
    table_match = re.search(r"<table[^>]*>(.*?)</table>", html, re.S)
    if not table_match:
        return []
    table = table_match.group(1)
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
        cells = [_html_to_text(c) for c in cells]
        if any(cells):
            rows.append(cells)
    return rows


def _parse_price(price_str: str) -> float:
    """Extract a float price from a string like '$1.38'."""
    m = re.search(r"\$?([\d.]+)", price_str)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return 0.0


def _extract_vram(vram_str: str) -> float:
    """Extract VRAM in GB from string like '80 GB'."""
    m = re.search(r"(\d+)\s*GB", vram_str, re.I)
    if m:
        return float(m.group(1))
    return 0.0


class GetDeployingCollector(BaseCollector):
    name = "getdeploying"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[getdeploying] Scraping GPU pricing aggregator")
        all_rows = []

        # 1. Scrape the index page (summary tables)
        try:
            all_rows.extend(self._scrape_index())
        except Exception as e:
            logger.warning(f"[getdeploying] Index scrape failed: {e}")

        # 2. Scrape individual GPU pages for per-provider detail
        for slug in GPU_PAGES:
            try:
                rows = self._scrape_gpu_page(slug)
                all_rows.extend(rows)
            except Exception as e:
                logger.warning(f"[getdeploying] {slug} failed: {e}")

        logger.info(f"[getdeploying] Total: {len(all_rows)} rows")
        return all_rows

    def _scrape_index(self) -> List[Dict[str, Any]]:
        """Scrape the cheapest-gpu-cloud index page tables."""
        html = _fetch(INDEX_URL)
        tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.S)
        rows = []

        for table in tables:
            parsed = []
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
                cells = [_html_to_text(c) for c in cells]
                if any(cells):
                    parsed.append(cells)

            if not parsed:
                continue

            header = [h.lower() for h in parsed[0]]
            # Look for GPU pricing table: "GPU Model", "VRAM", "Cheapest / GPU / hr", ...
            if not any("gpu" in h for h in header):
                continue

            for data_row in parsed[1:]:
                if len(data_row) < 4:
                    continue

                gpu_model = data_row[0]
                vram = _extract_vram(data_row[1]) if len(data_row) > 1 else 0
                price = _parse_price(data_row[2]) if len(data_row) > 2 else 0
                cheapest_provider = data_row[3] if len(data_row) > 3 else ""
                total_providers = data_row[4] if len(data_row) > 4 else ""

                if price <= 0:
                    continue

                rows.append(self.make_row(
                    provider=cheapest_provider.lower().replace(" ", "_").replace(".", ""),
                    instance_type=f"cheapest_{gpu_model}",
                    gpu_name=normalize_gpu_name(gpu_model),
                    gpu_memory_gb=vram,
                    gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=price,
                    price_per_gpu_hour=price,
                    available=True,
                    raw_extra=json.dumps({
                        "cheapest_provider": cheapest_provider,
                        "total_providers": total_providers,
                        "source_page": "index",
                    }, separators=(",", ":")),
                ))

        logger.info(f"[getdeploying] Index: {len(rows)} rows")
        return rows

    def _scrape_gpu_page(self, slug: str) -> List[Dict[str, Any]]:
        """Scrape an individual GPU page like /gpus/nvidia-h100 for per-provider pricing."""
        url = f"{BASE_URL}/gpus/{slug}"
        try:
            html = _fetch(url)
        except Exception as e:
            logger.debug(f"[getdeploying] {slug}: {e}")
            return []

        # Extract GPU name from slug
        gpu_name_raw = slug.replace("nvidia-", "").replace("amd-", "").replace("-", " ").upper()

        tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.S)
        rows = []

        for table in tables:
            parsed_rows = []
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
                cells = [_html_to_text(c) for c in cells]
                if any(cells):
                    parsed_rows.append(cells)

            if not parsed_rows:
                continue

            header = [h.lower() for h in parsed_rows[0]]
            # Look for provider pricing table with $/GPU/h column
            price_col = None
            provider_col = None
            gpu_col = None
            vram_col = None
            vcpu_col = None
            ram_col = None
            billing_col = None

            for i, h in enumerate(header):
                if "$/gpu/h" in h or "gpu/h" in h:
                    price_col = i
                elif "total/h" in h:
                    if price_col is None:
                        price_col = i
                elif "provider" in h:
                    provider_col = i
                elif "gpu" in h and "vram" not in h and "$" not in h:
                    gpu_col = i
                elif "vram" in h:
                    vram_col = i
                elif "vcpu" in h:
                    vcpu_col = i
                elif "ram" in h and "vram" not in h:
                    ram_col = i
                elif "billing" in h:
                    billing_col = i

            if price_col is None or provider_col is None:
                continue

            for data_row in parsed_rows[1:]:
                if len(data_row) <= max(price_col, provider_col):
                    continue

                provider_name = _clean_provider_name(data_row[provider_col])
                price = _parse_price(data_row[price_col])
                if price <= 0 or not provider_name:
                    continue

                # Extract instance details from GPUs column
                gpu_detail = _clean_gpu_detail(data_row[gpu_col]) if gpu_col is not None and gpu_col < len(data_row) else ""
                vram = 0
                if vram_col is not None and vram_col < len(data_row):
                    vram = _extract_vram(data_row[vram_col])

                gpu_count = 1
                count_match = re.match(r"(\d+)x", gpu_detail)
                if count_match:
                    gpu_count = int(count_match.group(1))

                vcpus = ""
                if vcpu_col is not None and vcpu_col < len(data_row):
                    vcpus = data_row[vcpu_col]

                ram_gb = ""
                if ram_col is not None and ram_col < len(data_row):
                    m = re.search(r"(\d+)", data_row[ram_col])
                    if m:
                        ram_gb = m.group(1)

                billing = ""
                if billing_col is not None and billing_col < len(data_row):
                    billing = _clean_billing_text(data_row[billing_col])

                pricing_type = "on_demand"
                if billing and "spot" in billing.lower():
                    pricing_type = "spot"
                elif billing and "reserved" in billing.lower():
                    pricing_type = "reserved"

                rows.append(self.make_row(
                    provider=provider_name.lower().replace(" ", "_").replace(".", ""),
                    instance_type=gpu_detail or f"{slug}_{provider_name}",
                    gpu_name=normalize_gpu_name(gpu_name_raw),
                    gpu_memory_gb=vram,
                    gpu_count=gpu_count,
                    vcpus=vcpus,
                    ram_gb=ram_gb,
                    pricing_type=pricing_type,
                    price_per_hour=price * gpu_count,
                    price_per_gpu_hour=price,
                    available=True,
                    raw_extra=json.dumps({
                        "provider_name": provider_name,
                        "gpu_detail": gpu_detail,
                        "billing": billing,
                        "source_page": slug,
                    }, separators=(",", ":")),
                ))

        if rows:
            logger.info(f"[getdeploying] {slug}: {len(rows)} rows")
        return rows
