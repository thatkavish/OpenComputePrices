"""
Hyperstack GPU pricing scraper.

Scrapes the Hyperstack GPU pricing page. The page is largely JS-rendered,
but GPU names and some pricing data are present in the static HTML.
No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

URL = "https://www.hyperstack.cloud/gpu-pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class HyperstackCollector(BaseCollector):
    name = "hyperstack"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[hyperstack] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[hyperstack] Failed to fetch: {e}")
            return []

        rows = []

        # Look for JSON-LD structured data
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

        # Look for inline script data containing pricing
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
        for s in scripts:
            if 'price' in s.lower() and ('gpu' in s.lower() or 'h100' in s.lower()):
                # Try to extract JSON objects with pricing
                json_objs = re.findall(r'\{[^{}]*"(?:price|hourly)[^{}]*\}', s)
                for jo in json_objs:
                    try:
                        d = json.loads(jo)
                        row = self._parse_json_obj(d)
                        if row:
                            rows.append(row)
                    except (json.JSONDecodeError, ValueError):
                        pass

        # Fallback: extract GPU+price patterns from HTML
        if not rows:
            gpu_price_patterns = re.findall(
                r'((?:H100|H200|A100|L40S?|A40|RTX)[^<]{0,300}\$[\d.]+[^<]{0,50})',
                html, re.I
            )
            seen = set()
            for block in gpu_price_patterns:
                clean = re.sub(r'<[^>]+>', ' ', block)
                clean = re.sub(r'\s+', ' ', clean).strip()
                gpu_m = re.search(r'(H100|H200|A100|L40S?|A40|RTX\s*\w+)', clean, re.I)
                price_m = re.search(r'\$([\d.]+)', clean)
                if gpu_m and price_m:
                    gpu = gpu_m.group(1)
                    price = float(price_m.group(1))
                    if price > 0 and (gpu, price) not in seen:
                        seen.add((gpu, price))
                        rows.append(self.make_row(
                            provider="hyperstack",
                            instance_type=gpu,
                            gpu_name=normalize_gpu_name(gpu),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[hyperstack] Total: {len(rows)} rows")
        return rows

    def _parse_product_ld(self, item: dict) -> Dict[str, Any]:
        """Parse a JSON-LD Product."""
        name = item.get("name", "")
        offers = item.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price_str = offers.get("price", "")
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None
        gpu_m = re.search(r'(H100|H200|A100|L40S?|A40|RTX\s*\w+)', name, re.I)
        if not gpu_m:
            return None
        return self.make_row(
            provider="hyperstack",
            instance_type=name,
            gpu_name=normalize_gpu_name(gpu_m.group(1)),
            gpu_count=1,
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=price,
            available=True,
        )

    def _parse_json_obj(self, d: dict) -> Dict[str, Any]:
        """Parse a JSON object with price/gpu fields."""
        gpu_name = d.get("gpu", "") or d.get("name", "") or d.get("gpu_type", "")
        price = d.get("price", 0) or d.get("hourly_price", 0) or d.get("price_per_hour", 0)
        try:
            price = float(price)
        except (ValueError, TypeError):
            return None
        if price <= 0 or not gpu_name:
            return None
        return self.make_row(
            provider="hyperstack",
            instance_type=gpu_name,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_count=1,
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=price,
            available=True,
        )
