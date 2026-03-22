"""
Prime Intellect GPU pricing collector.

Uses the Prime Intellect API to fetch GPU availability and pricing.
Requires API key (PRIMEINTELLECT_API_KEY env var).
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_URL = "https://api.primeintellect.ai/v1/gpu"
UA = "gpu-pricing-tracker/1.0"


class PrimeIntellectCollector(BaseCollector):
    name = "primeintellect"
    requires_api_key = True
    api_key_env_var = "PRIMEINTELLECT_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[primeintellect] Fetching GPU availability and pricing")

        rows = []
        for endpoint in ["/availability", "/summary"]:
            try:
                url = API_URL + endpoint
                req = urllib.request.Request(url, headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {api_key}",
                    "User-Agent": UA,
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())

                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = data.get("data", data.get("gpus", data.get("items", [])))
                    if isinstance(items, dict):
                        items = [items]
                else:
                    continue

                for item in items:
                    row = self._parse_item(item)
                    if row:
                        rows.append(row)

                logger.info(f"[primeintellect] {endpoint}: {len(items)} items")
            except Exception as e:
                logger.warning(f"[primeintellect] {endpoint} failed: {e}")

        logger.info(f"[primeintellect] Total: {len(rows)} rows")
        return rows

    def _parse_item(self, item: dict) -> Dict[str, Any]:
        """Parse a GPU availability/pricing item."""
        gpu_name = item.get("gpu_type", "") or item.get("gpu_name", "") or item.get("name", "")
        if not gpu_name:
            return None

        price = item.get("price_per_hour", 0) or item.get("price", 0) or item.get("hourly_price", 0)
        try:
            price = float(price)
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        gpu_count = item.get("gpu_count", 1) or item.get("num_gpus", 1) or 1
        gpu_mem = item.get("gpu_memory", 0) or item.get("vram_gb", 0) or 0
        available = item.get("available", True)
        available_count = item.get("available_count", "") or item.get("quantity", "")
        region = item.get("region", "") or item.get("location", "")

        price_per_gpu = price / gpu_count if gpu_count > 0 else price

        return self.make_row(
            provider="primeintellect",
            instance_type=item.get("instance_type", "") or gpu_name,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_memory_gb=gpu_mem,
            gpu_count=gpu_count,
            region=region,
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=round(price_per_gpu, 6),
            available=available,
            available_count=available_count,
            raw_extra=json.dumps({
                k: v for k, v in item.items()
                if k not in ("gpu_type", "gpu_name", "name", "price_per_hour",
                             "price", "hourly_price", "gpu_count", "num_gpus",
                             "gpu_memory", "vram_gb", "available", "region")
            }, separators=(",", ":"), default=str),
        )
