"""
Oracle Cloud Infrastructure GPU pricing collector.

Uses the unauthenticated OCI pricing API.
https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/
"""

import json
import logging
import re
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

NODE_GPU_COUNT_PATTERNS = [
    (re.compile(r"\bBM\.GPU\.A10\.\d+\b", re.I), 4),
    (re.compile(r"\bVM\.GPU\.A10\.(\d+)\b", re.I), None),
    (re.compile(r"\b(?:BM|VM)\.GPU\.A100(?:-v2)?\.(\d+)\b", re.I), None),
    (re.compile(r"\b(?:BM|VM)\.GPU\.H100\.(\d+)\b", re.I), None),
    (re.compile(r"\b(?:BM|VM)\.GPU\.H200\.(\d+)\b", re.I), None),
    (re.compile(r"\b(?:BM|VM)\.GPU\.B200\.(\d+)\b", re.I), None),
    (re.compile(r"\b(?:BM|VM)\.GPU\.GB200\.(\d+)\b", re.I), None),
    (re.compile(r"\b(?:BM|VM)\.GPU\.GH200\.(\d+)\b", re.I), None),
    (re.compile(r"\bBM\.GPU4\.(\d+)\b", re.I), None),
]


def _infer_gpu_name(display_name: str) -> str:
    dn_lower = display_name.lower()
    if "gb200" in dn_lower:
        return "GB200"
    if "gh200" in dn_lower:
        return "GH200"
    if "h200" in dn_lower:
        return "H200"
    if "h100" in dn_lower:
        return "H100"
    if "a100" in dn_lower:
        return "A100"
    if "a10" in dn_lower and "a100" not in dn_lower:
        return "A10"
    if "l40s" in dn_lower:
        return "L40S"
    if "l40" in dn_lower:
        return "L40"
    if "v100" in dn_lower:
        return "V100"
    if "mi300x" in dn_lower:
        return "MI300X"
    if "b200" in dn_lower:
        return "B200"
    if "gpu" in dn_lower:
        return "GPU (unspecified)"
    return ""


def _infer_gpu_count(display_name: str, metric_name: str) -> int:
    metric_lower = metric_name.lower()
    if "gpu per hour" in metric_lower:
        return 1

    for pattern, fixed_count in NODE_GPU_COUNT_PATTERNS:
        match = pattern.search(display_name)
        if not match:
            continue
        if fixed_count is not None:
            return fixed_count
        try:
            return int(match.group(1))
        except (TypeError, ValueError, IndexError):
            return 0

    return 0


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

        gpu_name = _infer_gpu_name(display_name)
        if not gpu_name:
            return None

        gpu_count = _infer_gpu_count(display_name, metric_name)
        price_per_gpu = usd_price / gpu_count if gpu_count > 0 else usd_price

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
            gpu_count=gpu_count,
            pricing_type="on_demand",
            price_per_hour=usd_price,
            price_per_gpu_hour=round(price_per_gpu, 6),
            price_unit=price_unit,
            available=True,
            raw_extra=json.dumps({
                "display_name": display_name,
                "service_category": service_cat,
                "metric_name": metric_name,
                "pricing_model": pricing_model,
            }, separators=(",", ":")),
        )
