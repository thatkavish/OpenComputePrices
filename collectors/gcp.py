"""
Google Cloud Platform GPU pricing collector.

Uses the Cloud Billing Catalog API. Requires a free API key (GCP_API_KEY env var).
https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list
"""

import json
import logging
import urllib.request
import urllib.parse
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name, infer_geo_group

logger = logging.getLogger(__name__)

# GCP Compute Engine service ID
COMPUTE_SERVICE_ID = "6F81-5844-456A"
API_BASE = f"https://cloudbilling.googleapis.com/v1/services/{COMPUTE_SERVICE_ID}/skus"

# GPU-related keywords in SKU descriptions
GPU_KEYWORDS = [
    "nvidia", "gpu", "a100", "h100", "h200", "b200", "gb200",
    "v100", "t4", "k80", "p100", "l4", "l40", "a10",
    "a2-highgpu", "a2-megagpu", "a2-ultragpu",
    "a3-highgpu", "a3-megagpu",
    "g2-standard",
]


def _extract_gpu_from_description(desc: str) -> Dict[str, Any]:
    """Parse GPU name from GCP SKU description."""
    dl = desc.lower()

    if "h200" in dl:
        return {"gpu": "H200", "mem": 141, "variant": "SXM"}
    if "h100" in dl and "80gb" in dl:
        return {"gpu": "H100", "mem": 80, "variant": "SXM5"}
    if "h100" in dl:
        return {"gpu": "H100", "mem": 80, "variant": ""}
    if "a100" in dl and "80gb" in dl:
        return {"gpu": "A100", "mem": 80, "variant": "SXM4"}
    if "a100" in dl and "40gb" in dl:
        return {"gpu": "A100", "mem": 40, "variant": ""}
    if "a100" in dl:
        return {"gpu": "A100", "mem": 40, "variant": ""}
    if "v100" in dl:
        return {"gpu": "V100", "mem": 16, "variant": ""}
    if "t4" in dl:
        return {"gpu": "T4", "mem": 16, "variant": ""}
    if "k80" in dl:
        return {"gpu": "K80", "mem": 12, "variant": ""}
    if "p100" in dl:
        return {"gpu": "P100", "mem": 16, "variant": ""}
    if "p4" in dl:
        return {"gpu": "P4", "mem": 8, "variant": ""}
    if "l4" in dl:
        return {"gpu": "L4", "mem": 24, "variant": ""}
    if "l40" in dl:
        return {"gpu": "L40", "mem": 48, "variant": ""}
    if "b200" in dl:
        return {"gpu": "B200", "mem": 192, "variant": ""}
    if "gb200" in dl:
        return {"gpu": "GB200", "mem": 192, "variant": ""}
    if "a10g" in dl:
        return {"gpu": "A10G", "mem": 24, "variant": ""}

    return {"gpu": "", "mem": 0, "variant": ""}


def _classify_usage_type(usage_type: str) -> tuple:
    """Classify GCP usage type into (pricing_type, commitment_period)."""
    ut = usage_type.lower()
    if "preemptible" in ut or "spot" in ut:
        return "spot", ""
    if "commit1yr" in ut or "commitmentyear1" in ut:
        return "committed", "1yr"
    if "commit3yr" in ut or "commitmentyear3" in ut:
        return "committed", "3yr"
    return "on_demand", ""


class GCPCollector(BaseCollector):
    name = "gcp"
    requires_api_key = True
    api_key_env_var = "GCP_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[gcp] Fetching GPU SKUs from Cloud Billing Catalog")

        all_rows = []
        page_token = ""
        page = 0

        while True:
            page += 1
            params = {"key": api_key, "pageSize": 5000}
            if page_token:
                params["pageToken"] = page_token

            url = f"{API_BASE}?{urllib.parse.urlencode(params)}"

            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read().decode())
            except Exception as e:
                logger.error(f"[gcp] API request failed on page {page}: {e}")
                break

            skus = data.get("skus", [])
            for sku in skus:
                rows = self._parse_sku(sku)
                all_rows.extend(rows)

            page_token = data.get("nextPageToken", "")
            if not page_token:
                break

            if page % 10 == 0:
                logger.info(f"[gcp] Page {page}, {len(all_rows)} GPU rows so far")

        logger.info(f"[gcp] Total: {len(all_rows)} rows")
        return all_rows

    def _parse_sku(self, sku: dict) -> List[Dict[str, Any]]:
        """Parse a GCP SKU into pricing rows (one per region)."""
        desc = sku.get("description", "")
        category = sku.get("category", {}) or {}
        resource_family = category.get("resourceFamily", "")
        resource_group = category.get("resourceGroup", "")
        usage_type = category.get("usageType", "")

        # Filter to compute GPU SKUs
        desc_lower = desc.lower()
        is_gpu = any(kw in desc_lower for kw in GPU_KEYWORDS)
        if not is_gpu:
            return []

        # Skip license/premium-image SKUs
        if resource_family not in ("Compute", ""):
            if "GPU" not in resource_group:
                return []

        gpu_info = _extract_gpu_from_description(desc)
        if not gpu_info.get("gpu"):
            return []

        pricing_type, commitment = _classify_usage_type(usage_type)

        # Extract pricing
        pricing_info = sku.get("pricingInfo", [])
        if not pricing_info:
            return []

        pi = pricing_info[0]
        pricing_expr = pi.get("pricingExpression", {}) or {}
        tiered = pricing_expr.get("tieredRates", [])
        usage_unit = pricing_expr.get("usageUnit", "")

        # Get the last tier (most relevant)
        if not tiered:
            return []

        rate = tiered[-1]
        unit_price = rate.get("unitPrice", {}) or {}
        nanos = int(unit_price.get("nanos", 0))
        units = int(unit_price.get("units", 0))
        price = units + nanos / 1e9

        if price <= 0:
            return []

        # Determine regions
        regions = sku.get("serviceRegions", [])
        if not regions:
            regions = ["global"]

        rows = []
        for region in regions:
            rows.append(self.make_row(
                provider="gcp",
                instance_type=sku.get("skuId", ""),
                gpu_name=normalize_gpu_name(gpu_info.get("gpu", "")),
                gpu_variant=gpu_info.get("variant", ""),
                gpu_memory_gb=gpu_info.get("mem", ""),
                gpu_count=1,  # GCP SKUs are per-GPU
                region=region,
                geo_group=infer_geo_group(region),
                pricing_type=pricing_type,
                commitment_period=commitment,
                price_per_hour=round(price, 6),
                price_per_gpu_hour=round(price, 6),
                available=True,
                raw_extra=json.dumps({
                    "sku_id": sku.get("skuId", ""),
                    "description": desc,
                    "resource_family": resource_family,
                    "resource_group": resource_group,
                    "usage_type": usage_type,
                    "usage_unit": usage_unit,
                    "service_display_name": sku.get("serviceDisplayName", ""),
                }, separators=(",", ":")),
            ))

        return rows
