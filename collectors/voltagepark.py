"""
Voltage Park GPU pricing scraper.

Scrapes the Voltage Park pricing page which contains JSON-LD structured
data and HTML with GPU pricing. No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

URL = "https://www.voltagepark.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class VoltageParkCollector(BaseCollector):
    name = "voltagepark"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[voltagepark] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[voltagepark] Failed to fetch: {e}")
            return []

        rows = []

        # Method 1: JSON-LD structured data
        ld_blocks = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S
        )
        for ld in ld_blocks:
            try:
                data = json.loads(ld)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "Product":
                        row = self._parse_product_ld(item)
                        if row:
                            rows.append(row)
            except (json.JSONDecodeError, ValueError):
                pass

        # Method 2: HTML pattern extraction
        if not rows:
            seen = set()
            for match in re.finditer(
                r'((?:H100|H200|A100|B200|HGX)[^<]{0,500})',
                html, re.I
            ):
                block = match.group(1)
                clean = re.sub(r'<[^>]+>', ' ', block)
                clean = re.sub(r'\s+', ' ', clean).strip()
                gpu_m = re.search(r'(H100|H200|A100|B200)', clean, re.I)
                price_m = re.search(r'\$([\d.]+)', clean)
                if gpu_m and price_m:
                    gpu = gpu_m.group(1)
                    price = float(price_m.group(1))
                    if price > 0 and (gpu, price) not in seen:
                        seen.add((gpu, price))
                        rows.append(self.make_row(
                            provider="voltagepark",
                            instance_type=gpu,
                            gpu_name=normalize_gpu_name(gpu),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[voltagepark] Total: {len(rows)} rows")
        return rows

    def _parse_product_ld(self, item: dict) -> Dict[str, Any]:
        name = item.get("name", "")
        offers = item.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        try:
            price = float(offers.get("price", 0))
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None
        gpu_m = re.search(r'(H100|H200|A100|B200)', name, re.I)
        if not gpu_m:
            return None
        return self.make_row(
            provider="voltagepark",
            instance_type=name,
            gpu_name=normalize_gpu_name(gpu_m.group(1)),
            gpu_count=1,
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=price,
            available=True,
            raw_extra=json.dumps({"description": item.get("description", "")[:200]}, separators=(",", ":")),
        )
