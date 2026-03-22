"""
CoreWeave GPU pricing scraper.

Scrapes the CoreWeave pricing page which contains GPU names and
$/GPU-hr prices in structured HTML. No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

URL = "https://www.coreweave.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

COREWEAVE_GPUS = {
    "H100": {"mem": 80, "variant": "SXM5"},
    "H200": {"mem": 141, "variant": "SXM"},
    "B200": {"mem": 192, "variant": ""},
    "GB200": {"mem": 192, "variant": ""},
    "A100": {"mem": 80, "variant": "SXM4"},
    "A40":  {"mem": 48, "variant": ""},
    "L40S": {"mem": 48, "variant": ""},
    "L40":  {"mem": 48, "variant": ""},
    "RTX A6000": {"mem": 48, "variant": ""},
    "RTX A5000": {"mem": 24, "variant": ""},
    "RTX A4000": {"mem": 16, "variant": ""},
}


class CoreWeaveCollector(BaseCollector):
    name = "coreweave"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[coreweave] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[coreweave] Failed to fetch: {e}")
            return []

        rows = []
        seen = set()

        # Strategy: find GPU name blocks and extract nearby $/GPU-hr or $/hr prices
        for gpu_key in COREWEAVE_GPUS:
            specs = COREWEAVE_GPUS[gpu_key]
            # Escape for regex
            pattern = re.compile(
                r'(?:NVIDIA\s+)?' + re.escape(gpu_key) + r'(?:\s|<|$)',
                re.I
            )
            for match in pattern.finditer(html):
                pos = match.start()
                window = html[pos:pos + 3000]

                # Look for price patterns
                price_patterns = [
                    re.findall(r'\$([\d.]+)/GPU[- ]?hr', window, re.I),
                    re.findall(r'\$([\d.]+)/hr', window[:500], re.I),
                    re.findall(r'\$([\d.]+)\s*per\s*GPU', window[:500], re.I),
                ]

                for prices in price_patterns:
                    for price_str in prices:
                        try:
                            price = float(price_str)
                        except ValueError:
                            continue
                        if price <= 0 or price > 100:
                            continue

                        key = (gpu_key, price)
                        if key in seen:
                            continue
                        seen.add(key)

                        # Determine pricing type from context
                        context = window[:window.find(price_str) + 50].lower()
                        pricing_type = "on_demand"
                        commitment = ""
                        if "reserved" in context or "commit" in context:
                            pricing_type = "reserved"
                            yr_m = re.search(r"(\d)\s*(?:yr|year)", context)
                            if yr_m:
                                commitment = f"{yr_m.group(1)}yr"
                        elif "spot" in context:
                            pricing_type = "spot"

                        rows.append(self.make_row(
                            provider="coreweave",
                            instance_type=f"{gpu_key}_{pricing_type}",
                            gpu_name=normalize_gpu_name(gpu_key),
                            gpu_variant=specs.get("variant", ""),
                            gpu_memory_gb=specs.get("mem", 0),
                            gpu_count=1,
                            pricing_type=pricing_type,
                            commitment_period=commitment,
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))
                    if prices:
                        break  # Got prices for this GPU, move on

        logger.info(f"[coreweave] Total: {len(rows)} rows")
        return rows
