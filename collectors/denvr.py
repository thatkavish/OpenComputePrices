"""
Denvr Dataworks GPU pricing scraper.

Scrapes the Denvr pricing page which contains GPU pricing data
in embedded JSON scripts. No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

URL = "https://www.denvr.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class DenvrCollector(BaseCollector):
    name = "denvr"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[denvr] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[denvr] Failed to fetch: {e}")
            return []

        rows = []

        # Look for JSON data in script tags
        scripts = re.findall(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, re.S)
        for s in scripts:
            try:
                data = json.loads(s)
                rows.extend(self._extract_from_json(data))
            except (json.JSONDecodeError, ValueError):
                pass

        # Also look in inline scripts for pricing data
        all_scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
        for s in all_scripts:
            if len(s) < 200:
                continue
            # Look for JSON objects with gpu + price
            gpu_prices = re.findall(
                r'"(?:name|gpu|model|instance)"\s*:\s*"([^"]*(?:H100|H200|A100|B200|Gaudi|MI300)[^"]*)"'
                r'[^}]*"(?:price|hourly|cost|rate)"\s*:\s*["\']?([\d.]+)',
                s, re.I
            )
            for name, price_str in gpu_prices:
                try:
                    price = float(price_str)
                except ValueError:
                    continue
                if price > 0:
                    gpu_m = re.search(r'(H100|H200|A100|B200|Gaudi\s*\d*|MI300X?)', name, re.I)
                    rows.append(self.make_row(
                        provider="denvr",
                        instance_type=name,
                        gpu_name=normalize_gpu_name(gpu_m.group(1)) if gpu_m else "",
                        gpu_count=1,
                        pricing_type="on_demand",
                        price_per_hour=price,
                        price_per_gpu_hour=price,
                        available=True,
                    ))

        # Fallback: extract GPU+price from static HTML
        if not rows:
            seen = set()
            for match in re.finditer(
                r'((?:H100|H200|A100|B200|Gaudi|MI300)[^<]{0,300})', html, re.I
            ):
                block = match.group(1)
                clean = re.sub(r'<[^>]+>', ' ', block)
                clean = re.sub(r'\s+', ' ', clean).strip()
                gpu_m = re.search(r'(H100|H200|A100|B200|Gaudi\s*\d*|MI300X?)', clean, re.I)
                price_m = re.search(r'\$([\d.]+)', clean)
                if gpu_m and price_m:
                    gpu = gpu_m.group(1)
                    price = float(price_m.group(1))
                    if price > 0 and price < 100 and (gpu.upper(), price) not in seen:
                        seen.add((gpu.upper(), price))
                        rows.append(self.make_row(
                            provider="denvr",
                            instance_type=gpu,
                            gpu_name=normalize_gpu_name(gpu),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[denvr] Total: {len(rows)} rows")
        return rows

    def _extract_from_json(self, data, depth=0) -> List[Dict[str, Any]]:
        rows = []
        if depth > 5:
            return rows
        if isinstance(data, dict):
            gpu = data.get("gpu", "") or data.get("name", "") or data.get("gpu_type", "")
            price = data.get("price", 0) or data.get("hourly", 0) or data.get("price_per_hour", 0)
            if gpu and price:
                gpu_m = re.search(r'(H100|H200|A100|B200|Gaudi|MI300)', str(gpu), re.I)
                if gpu_m:
                    try:
                        p = float(price)
                    except (ValueError, TypeError):
                        p = 0
                    if p > 0:
                        rows.append(self.make_row(
                            provider="denvr",
                            instance_type=str(gpu),
                            gpu_name=normalize_gpu_name(gpu_m.group(1)),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=p,
                            price_per_gpu_hour=p,
                            available=True,
                        ))
            for v in data.values():
                rows.extend(self._extract_from_json(v, depth + 1))
        elif isinstance(data, list):
            for item in data:
                rows.extend(self._extract_from_json(item, depth + 1))
        return rows
