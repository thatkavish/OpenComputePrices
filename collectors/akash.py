"""
Akash Network GPU pricing collector.

Uses the Akash console API at console-api.akash.network/v1/gpu-prices
which returns structured GPU pricing with availability data.
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

API_URL = "https://console-api.akash.network/v1/gpu-prices"
UA = "gpu-pricing-tracker/1.0"


class AkashCollector(BaseCollector):
    name = "akash"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[akash] Fetching GPU pricing from console API")

        try:
            req = urllib.request.Request(API_URL, headers={
                "User-Agent": UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.error(f"[akash] API request failed: {e}")
            return []

        models = data.get("models", [])
        rows = []

        for model in models:
            vendor = model.get("vendor", "")
            gpu_model = model.get("model", "")
            ram = model.get("ram", "")
            interface = model.get("interface", "")

            availability = model.get("availability", {}) or {}
            total_gpus = availability.get("total", 0)
            available_gpus = availability.get("available", 0)

            provider_avail = model.get("providerAvailability", {}) or {}
            total_providers = provider_avail.get("total", 0)
            avail_providers = provider_avail.get("available", 0)

            price_info = model.get("price", {}) or {}
            currency = price_info.get("currency", "USD")

            # Parse RAM to GB
            gpu_mem = 0
            if ram:
                ram_match = re.search(r"(\d+)", str(ram))
                if ram_match:
                    gpu_mem = int(ram_match.group(1))

            # Extract pricing tiers (min, max, avg, weightedAverage)
            for price_key in ["min", "max", "avg", "weightedAverage"]:
                price_val = price_info.get(price_key, 0)
                if not price_val:
                    continue
                try:
                    price = float(price_val)
                except (ValueError, TypeError):
                    continue
                if price <= 0:
                    continue

                rows.append(self.make_row(
                    provider="akash",
                    instance_type=f"{gpu_model}_{interface}_{price_key}" if interface else f"{gpu_model}_{price_key}",
                    gpu_name=normalize_gpu_name(gpu_model),
                    gpu_variant=interface,
                    gpu_memory_gb=gpu_mem,
                    gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=round(price, 6),
                    price_per_gpu_hour=round(price, 6),
                    currency=currency,
                    available=available_gpus > 0,
                    available_count=available_gpus,
                    raw_extra=json.dumps({
                        "vendor": vendor,
                        "price_tier": price_key,
                        "total_gpus": total_gpus,
                        "total_providers": total_providers,
                        "available_providers": avail_providers,
                    }, separators=(",", ":")),
                ))

        logger.info(f"[akash] Total: {len(rows)} rows from {len(models)} GPU models")
        return rows
