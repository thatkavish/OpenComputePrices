"""
Paperspace (DigitalOcean) GPU pricing scraper.

Scrapes the Paperspace pricing page. Heavily JS-rendered.
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

URL = "https://www.paperspace.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class PaperspaceCollector(BaseCollector):
    name = "paperspace"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[paperspace] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[paperspace] Failed to fetch: {e}")
            return []

        rows = []

        # Check for __NEXT_DATA__
        nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
        if nd:
            try:
                data = json.loads(nd.group(1))
                rows.extend(self._extract_from_json(data))
            except (json.JSONDecodeError, ValueError):
                pass

        # Check for JSON-LD
        ld_blocks = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S
        )
        for ld in ld_blocks:
            try:
                data = json.loads(ld)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "Product":
                        name = item.get("name", "")
                        offers = item.get("offers", {})
                        if isinstance(offers, list):
                            offers = offers[0] if offers else {}
                        try:
                            price = float(offers.get("price", 0))
                        except (ValueError, TypeError):
                            continue
                        if price > 0 and re.search(r'gpu|h100|a100|rtx|a6000', name, re.I):
                            rows.append(self.make_row(
                                provider="paperspace",
                                instance_type=name,
                                gpu_name=normalize_gpu_name(name),
                                gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price,
                                price_per_gpu_hour=price,
                                available=True,
                            ))
            except (json.JSONDecodeError, ValueError):
                pass

        # Fallback: search for GPU+price patterns in scripts
        if not rows:
            scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
            for s in scripts:
                if len(s) < 100:
                    continue
                gpu_prices = re.findall(
                    r'"(?:name|gpu|machine)"\s*:\s*"([^"]*(?:H100|A100|RTX|A6000|L40|A4000)[^"]*)"'
                    r'[^}]*"(?:price|hourly|cost)"\s*:\s*["\']?([\d.]+)',
                    s, re.I
                )
                for name, price_str in gpu_prices:
                    try:
                        price = float(price_str)
                    except ValueError:
                        continue
                    if price > 0:
                        rows.append(self.make_row(
                            provider="paperspace",
                            instance_type=name,
                            gpu_name=normalize_gpu_name(name),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        # Last resort: HTML patterns
        if not rows:
            seen = set()
            blocks = re.findall(
                r'((?:H100|A100|RTX\s*\w+|A6000|A5000|L40S?)[^<]{0,300})',
                html, re.I
            )
            for block in blocks:
                clean = re.sub(r'<[^>]+>', ' ', block)
                clean = re.sub(r'\s+', ' ', clean).strip()
                gpu_m = re.search(r'(H100|A100|RTX\s*\w+|A6000|A5000|L40S?)', clean, re.I)
                price_m = re.search(r'\$([\d.]+)', clean)
                if gpu_m and price_m:
                    gpu = gpu_m.group(1)
                    price = float(price_m.group(1))
                    if price > 0 and (gpu.upper(), price) not in seen:
                        seen.add((gpu.upper(), price))
                        rows.append(self.make_row(
                            provider="paperspace",
                            instance_type=gpu,
                            gpu_name=normalize_gpu_name(gpu),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[paperspace] Total: {len(rows)} rows")
        return rows

    def _extract_from_json(self, data, depth=0) -> List[Dict[str, Any]]:
        """Recursively search JSON for GPU pricing."""
        rows = []
        if depth > 6:
            return rows
        if isinstance(data, dict):
            gpu = data.get("gpu", "") or data.get("gpu_type", "") or data.get("name", "")
            price = data.get("price", 0) or data.get("hourlyRate", 0) or data.get("costPerHour", 0)
            if gpu and price:
                try:
                    price = float(price)
                except (ValueError, TypeError):
                    price = 0
                if price > 0 and re.search(r'H100|A100|RTX|A6000|L40|V100|T4|A4000', gpu, re.I):
                    rows.append(self.make_row(
                        provider="paperspace",
                        instance_type=gpu,
                        gpu_name=normalize_gpu_name(gpu),
                        gpu_count=1,
                        pricing_type="on_demand",
                        price_per_hour=price,
                        price_per_gpu_hour=price,
                        available=True,
                    ))
            for v in data.values():
                rows.extend(self._extract_from_json(v, depth + 1))
        elif isinstance(data, list):
            for item in data:
                rows.extend(self._extract_from_json(item, depth + 1))
        return rows
