"""
Salad GPU pricing scraper.

Scrapes the Salad pricing page. Heavily JS-rendered but may contain
pricing data in embedded scripts or meta tags.
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

URL = "https://salad.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class SaladCollector(BaseCollector):
    name = "salad"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[salad] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[salad] Failed to fetch: {e}")
            return []

        rows = []

        # Look for JSON-LD
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
                        price = float(offers.get("price", 0))
                        if price > 0 and re.search(r'gpu|rtx|h100|a100|v100|t4', name, re.I):
                            rows.append(self.make_row(
                                provider="salad",
                                instance_type=name,
                                gpu_name=normalize_gpu_name(name),
                                gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price,
                                price_per_gpu_hour=price,
                                available=True,
                            ))
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

        # Search inline scripts for pricing data
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
        for s in scripts:
            if len(s) < 100 or ('price' not in s.lower() and '$' not in s):
                continue
            # Look for JSON arrays/objects with GPU pricing
            gpu_prices = re.findall(
                r'"(?:name|gpu|model)"\s*:\s*"([^"]*(?:RTX|H100|A100|A6000|V100|T4|L40|A40)[^"]*)"'
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
                        provider="salad",
                        instance_type=name,
                        gpu_name=normalize_gpu_name(name),
                        gpu_count=1,
                        pricing_type="on_demand",
                        price_per_hour=price,
                        price_per_gpu_hour=price,
                        available=True,
                    ))

        # Fallback: extract any GPU + price from static HTML
        if not rows:
            seen = set()
            blocks = re.findall(
                r'((?:H100|A100|RTX\s*\d+|A6000|V100|T4|L40S?|A40|RTX\s*\w+)[^<]{0,300})',
                html, re.I
            )
            for block in blocks:
                clean = re.sub(r'<[^>]+>', ' ', block)
                clean = re.sub(r'\s+', ' ', clean).strip()
                gpu_m = re.search(r'(H100|A100|RTX\s*\d+|A6000|V100|T4|L40S?|A40)', clean, re.I)
                price_m = re.search(r'\$([\d.]+)', clean)
                if gpu_m and price_m:
                    gpu = gpu_m.group(1)
                    price = float(price_m.group(1))
                    if price > 0 and (gpu.upper(), price) not in seen:
                        seen.add((gpu.upper(), price))
                        rows.append(self.make_row(
                            provider="salad",
                            instance_type=gpu,
                            gpu_name=normalize_gpu_name(gpu),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[salad] Total: {len(rows)} rows")
        return rows
