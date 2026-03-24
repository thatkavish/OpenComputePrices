"""
Oracle Cloud Infrastructure GPU pricing collector.

Uses the unauthenticated OCI pricing API.
https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_URL = "https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/"

# GPU-related keywords in displayName / serviceCategory
GPU_KEYWORDS = [
    "gpu", "h100", "h200", "a100", "a10", "l40", "v100",
    "mi300", "gh200", "b200", "gb200", "bare metal gpu",
    "nvidia", "accelerat",
]


class OracleCollector(BaseCollector):
    name = "oracle"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[oracle] Fetching OCI pricing (no auth)")

        try:
            req = urllib.request.Request(API_URL, headers={
                "Accept": "application/json",
                "User-Agent": "OpenComputePrices/1.0",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.error(f"[oracle] API request failed: {e}")
            return []

        items = data.get("items", [])
        logger.info(f"[oracle] Received {len(items)} total products")

        all_rows = []
        for item in items:
            row = self._parse_item(item)
            if row:
                all_rows.append(row)

        logger.info(f"[oracle] Total: {len(all_rows)} GPU rows")
        return all_rows

    def _parse_item(self, item: dict) -> Dict[str, Any]:
        """Parse an OCI pricing item."""
        display_name = item.get("displayName", "") or ""
        part_number = item.get("partNumber", "") or ""
        service_cat = item.get("serviceCategory", "") or ""
        metric_name = item.get("metricName", "") or ""

        # Filter to GPU-related items
        combined = f"{display_name} {part_number} {service_cat}".lower()
        if not any(kw in combined for kw in GPU_KEYWORDS):
            return None

        # Extract USD pricing from currencyCodeLocalizations
        localizations = item.get("currencyCodeLocalizations", [])
        usd_price = None
        pricing_model = ""

        for loc in localizations:
            if loc.get("currencyCode") == "USD":
                prices = loc.get("prices", [])
                for p in prices:
                    model = p.get("model", "")
                    val = p.get("value", 0)
                    if val and val > 0:
                        usd_price = val
                        pricing_model = model
                        break
                break

        if usd_price is None or usd_price <= 0:
            return None

        # Infer GPU name from display name
        dn_lower = display_name.lower()
        gpu_name = ""
        if "h200" in dn_lower:
            gpu_name = "H200"
        elif "h100" in dn_lower:
            gpu_name = "H100"
        elif "a100" in dn_lower:
            gpu_name = "A100"
        elif "a10" in dn_lower and "a100" not in dn_lower:
            gpu_name = "A10"
        elif "l40s" in dn_lower:
            gpu_name = "L40S"
        elif "l40" in dn_lower:
            gpu_name = "L40"
        elif "v100" in dn_lower:
            gpu_name = "V100"
        elif "mi300x" in dn_lower:
            gpu_name = "MI300X"
        elif "gh200" in dn_lower:
            gpu_name = "GH200"
        elif "b200" in dn_lower:
            gpu_name = "B200"
        elif "gb200" in dn_lower:
            gpu_name = "GB200"
        elif "gpu" in dn_lower:
            gpu_name = "GPU (unspecified)"

        if not gpu_name:
            return None

        # Determine price_unit from metric
        price_unit = "hour"
        ml = metric_name.lower()
        if "per hour" in ml or "gpu per hour" in ml:
            price_unit = "hour"
        elif "per month" in ml:
            price_unit = "month"

        return self.make_row(
            provider="oracle",
            instance_type=part_number,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_count=0,  # OCI API doesn't specify per-product
            pricing_type="on_demand",
            price_per_hour=usd_price,
            price_per_gpu_hour=usd_price,
            price_unit=price_unit,
            available=True,
            raw_extra=json.dumps({
                "display_name": display_name,
                "service_category": service_cat,
                "metric_name": metric_name,
                "pricing_model": pricing_model,
            }, separators=(",", ":")),
        )
