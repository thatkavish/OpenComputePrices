"""
Verda / DataCrunch GPU pricing collector.

Uses the DataCrunch REST API. Requires API key (DATACRUNCH_API_KEY env var).
Unique for dynamic pricing that changes multiple times daily.
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_BASE = "https://api.datacrunch.io/v1"
UA = "gpu-pricing-tracker/1.0"


class DataCrunchCollector(BaseCollector):
    name = "datacrunch"
    requires_api_key = True
    api_key_env_var = "DATACRUNCH_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[datacrunch] Fetching GPU pricing from API")

        rows = []
        for endpoint in ["/instances", "/instance-types", "/pricing"]:
            try:
                url = API_BASE + endpoint
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
                    items = data.get("data", data.get("instances", data.get("items", [])))
                    if isinstance(items, dict):
                        items = list(items.values()) if items else []
                else:
                    continue

                for item in items:
                    row = self._parse_item(item)
                    if row:
                        rows.append(row)

                logger.info(f"[datacrunch] {endpoint}: {len(items)} items")
                if rows:
                    break  # Got data, no need to try other endpoints
            except Exception as e:
                logger.debug(f"[datacrunch] {endpoint} failed: {e}")

        logger.info(f"[datacrunch] Total: {len(rows)} rows")
        return rows

    def _parse_item(self, item: dict) -> Dict[str, Any]:
        """Parse an instance/pricing item."""
        if not isinstance(item, dict):
            return None

        gpu_name = (item.get("gpu_type", "") or item.get("gpu", "") or
                    item.get("gpu_name", "") or item.get("name", ""))
        if not gpu_name:
            return None

        price = (item.get("price_per_hour", 0) or item.get("price", 0) or
                 item.get("hourly_price", 0) or item.get("spot_price", 0))
        try:
            price = float(price)
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        gpu_count = item.get("gpu_count", 1) or item.get("num_gpus", 1) or 1
        try:
            gpu_count = int(gpu_count)
        except (ValueError, TypeError):
            gpu_count = 1

        price_per_gpu = price / gpu_count if gpu_count > 0 else price

        return self.make_row(
            provider="datacrunch",
            instance_type=item.get("instance_type", "") or item.get("id", "") or gpu_name,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_memory_gb=item.get("gpu_memory", "") or item.get("vram_gb", ""),
            gpu_count=gpu_count,
            vcpus=item.get("vcpus", "") or item.get("cpu_count", ""),
            ram_gb=item.get("ram_gb", "") or item.get("memory_gb", ""),
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=round(price_per_gpu, 6),
            available=item.get("available", True),
            raw_extra=json.dumps({
                k: v for k, v in item.items()
                if k not in ("gpu_type", "gpu", "gpu_name", "name", "price_per_hour",
                             "price", "hourly_price", "gpu_count", "num_gpus",
                             "gpu_memory", "vram_gb", "available", "vcpus", "ram_gb")
            }, separators=(",", ":"), default=str),
        )
