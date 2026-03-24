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

API_URL = "https://api.primeintellect.ai/api/v1/availability/gpus"
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
        page = 0
        page_size = 200
        total_fetched = 0

        while True:
            try:
                url = f"{API_URL}?page={page}&pageSize={page_size}"
                req = urllib.request.Request(url, headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {api_key}",
                    "User-Agent": UA,
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())

                items = data.get("items", [])
                total_count = data.get("totalCount", 0)

                for item in items:
                    row = self._parse_item(item)
                    if row:
                        rows.append(row)

                total_fetched += len(items)
                if total_fetched >= total_count or not items:
                    break
                page += 1

            except Exception as e:
                logger.warning(f"[primeintellect] page {page} failed: {e}")
                break

        logger.info(f"[primeintellect] Total: {len(rows)} rows")
        return rows

    def _parse_item(self, item: dict) -> Dict[str, Any]:
        """Parse a GPU availability/pricing item from the API response."""
        gpu_type = item.get("gpuType", "") or item.get("cloudId", "")
        if not gpu_type:
            return None

        prices = item.get("prices", {})
        price = prices.get("onDemand", 0)
        try:
            price = float(price)
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        gpu_count = item.get("gpuCount", 1) or 1
        gpu_mem = item.get("gpuMemory", 0) or 0
        region = item.get("region", "")
        country = item.get("country", "")
        provider = item.get("provider", "primeintellect")
        socket = item.get("socket", "")
        stock = item.get("stockStatus", "")
        available = stock.lower() != "out_of_stock" if stock else True

        price_per_gpu = price / gpu_count if gpu_count > 0 else price

        vcpus = ""
        ram = ""
        vcpu_info = item.get("vcpu", {})
        mem_info = item.get("memory", {})
        if vcpu_info:
            vcpus = vcpu_info.get("defaultCount", "")
        if mem_info:
            ram = mem_info.get("defaultCount", "")

        return self.make_row(
            provider="primeintellect",
            instance_type=item.get("cloudId", gpu_type),
            gpu_name=normalize_gpu_name(gpu_type),
            gpu_variant=socket,
            gpu_memory_gb=gpu_mem,
            gpu_count=gpu_count,
            vcpus=vcpus,
            ram_gb=ram,
            region=region,
            country=country,
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=round(price_per_gpu, 6),
            currency=prices.get("currency", "USD"),
            available=available,
            raw_extra=json.dumps({
                "upstream_provider": provider,
                "socket": socket,
                "stockStatus": stock,
                "dataCenter": item.get("dataCenter", ""),
                "interconnect": item.get("interconnect", ""),
                "interconnectType": item.get("interconnectType", ""),
                "isVariable": prices.get("isVariable", False),
            }, separators=(",", ":"), default=str),
        )
