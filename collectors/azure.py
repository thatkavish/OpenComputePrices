"""
Azure VM GPU pricing collector.

Uses the Azure Retail Prices REST API — zero authentication required.
https://prices.azure.com/api/retail/prices
"""

import json
import logging
import urllib.request
import urllib.parse
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name, infer_geo_group

logger = logging.getLogger(__name__)

API_BASE = "https://prices.azure.com/api/retail/prices"

# GPU VM series to look for (case-insensitive contains matching)
GPU_SERIES_KEYWORDS = [
    "Standard_NC",   # A100, V100, K80, T4
    "Standard_ND",   # H100, H200, A100
    "Standard_NV",   # M60, A10, T4
    "Standard_NG",   # GPU-optimized (newer)
]

# Map Azure VM series → GPU info
AZURE_GPU_MAP = {
    "Standard_NC6s_v3":     {"gpu": "V100", "mem": 16, "count": 1, "variant": "SXM2"},
    "Standard_NC12s_v3":    {"gpu": "V100", "mem": 16, "count": 2, "variant": "SXM2"},
    "Standard_NC24s_v3":    {"gpu": "V100", "mem": 16, "count": 4, "variant": "SXM2"},
    "Standard_NC24rs_v3":   {"gpu": "V100", "mem": 16, "count": 4, "variant": "SXM2"},
    "Standard_NC4as_T4_v3":  {"gpu": "T4", "mem": 16, "count": 1, "variant": ""},
    "Standard_NC8as_T4_v3":  {"gpu": "T4", "mem": 16, "count": 1, "variant": ""},
    "Standard_NC16as_T4_v3": {"gpu": "T4", "mem": 16, "count": 1, "variant": ""},
    "Standard_NC64as_T4_v3": {"gpu": "T4", "mem": 16, "count": 4, "variant": ""},
    "Standard_NC24ads_A100_v4": {"gpu": "A100", "mem": 80, "count": 1, "variant": "PCIe"},
    "Standard_NC48ads_A100_v4": {"gpu": "A100", "mem": 80, "count": 2, "variant": "PCIe"},
    "Standard_NC96ads_A100_v4": {"gpu": "A100", "mem": 80, "count": 4, "variant": "PCIe"},
    "Standard_ND96asr_v4":      {"gpu": "A100", "mem": 40, "count": 8, "variant": "SXM4"},
    "Standard_ND96amsr_A100_v4": {"gpu": "A100", "mem": 80, "count": 8, "variant": "SXM4"},
    "Standard_NC40ads_H100_v5":  {"gpu": "H100", "mem": 94, "count": 1, "variant": "NVL"},
    "Standard_NC80adis_H100_v5": {"gpu": "H100", "mem": 94, "count": 2, "variant": "NVL"},
    "Standard_ND96isr_H100_v5":  {"gpu": "H100", "mem": 80, "count": 8, "variant": "SXM5"},
    "Standard_ND96isr_H200_v5":  {"gpu": "H200", "mem": 141, "count": 8, "variant": "SXM"},
    "Standard_NV6":         {"gpu": "Tesla M60", "mem": 8, "count": 1, "variant": ""},
    "Standard_NV12":        {"gpu": "Tesla M60", "mem": 8, "count": 2, "variant": ""},
    "Standard_NV24":        {"gpu": "Tesla M60", "mem": 8, "count": 4, "variant": ""},
    "Standard_NV6s_v2":     {"gpu": "Tesla M60", "mem": 8, "count": 1, "variant": ""},
    "Standard_NV12s_v2":    {"gpu": "Tesla M60", "mem": 8, "count": 2, "variant": ""},
    "Standard_NV24s_v2":    {"gpu": "Tesla M60", "mem": 8, "count": 4, "variant": ""},
    "Standard_NV12s_v3":    {"gpu": "Tesla M60", "mem": 8, "count": 1, "variant": ""},
    "Standard_NV24s_v3":    {"gpu": "Tesla M60", "mem": 8, "count": 2, "variant": ""},
    "Standard_NV48s_v3":    {"gpu": "Tesla M60", "mem": 8, "count": 4, "variant": ""},
    "Standard_NV6ads_A10_v5":  {"gpu": "A10", "mem": 24, "count": 1, "variant": ""},
    "Standard_NV12ads_A10_v5": {"gpu": "A10", "mem": 24, "count": 1, "variant": ""},
    "Standard_NV18ads_A10_v5": {"gpu": "A10", "mem": 24, "count": 1, "variant": ""},
    "Standard_NV36ads_A10_v5": {"gpu": "A10", "mem": 24, "count": 1, "variant": ""},
    "Standard_NV36adms_A10_v5": {"gpu": "A10", "mem": 24, "count": 1, "variant": ""},
    "Standard_NV72ads_A10_v5": {"gpu": "A10", "mem": 24, "count": 2, "variant": ""},
}


def _infer_gpu_from_sku(sku_name: str, product_name: str) -> Dict[str, Any]:
    """Try to infer GPU info from the SKU or product name."""
    # Direct lookup
    base = sku_name.split(" ")[0] if " " in sku_name else sku_name
    if base in AZURE_GPU_MAP:
        return AZURE_GPU_MAP[base]

    # Pattern-based inference from product name
    pn = product_name.lower()
    if "h200" in pn:
        return {"gpu": "H200", "mem": 141, "count": 8, "variant": "SXM"}
    if "h100" in pn:
        return {"gpu": "H100", "mem": 80, "count": 8, "variant": "SXM5"}
    if "a100" in pn:
        count = 8 if "nd96" in sku_name.lower() else 4 if "nc96" in sku_name.lower() else 1
        mem = 80 if "80" in pn else 40
        return {"gpu": "A100", "mem": mem, "count": count, "variant": ""}
    if "v100" in pn:
        return {"gpu": "V100", "mem": 16, "count": 0, "variant": ""}
    if "t4" in pn:
        return {"gpu": "T4", "mem": 16, "count": 0, "variant": ""}
    if "a10" in pn and "a100" not in pn:
        return {"gpu": "A10", "mem": 24, "count": 0, "variant": ""}
    if "m60" in pn:
        return {"gpu": "Tesla M60", "mem": 8, "count": 0, "variant": ""}
    if "k80" in pn:
        return {"gpu": "K80", "mem": 12, "count": 0, "variant": ""}

    return {"gpu": "", "mem": 0, "count": 0, "variant": ""}


def _classify_pricing_type(sku_name: str, meter_name: str) -> tuple:
    """Return (pricing_type, commitment_period) from SKU/meter info."""
    ln = (sku_name + " " + meter_name).lower()
    if "spot" in ln:
        return "spot", ""
    if "low priority" in ln:
        return "spot", ""
    if "3 year" in ln or "three year" in ln:
        return "reserved", "3yr"
    if "1 year" in ln or "one year" in ln:
        return "reserved", "1yr"
    return "on_demand", ""


def _normalize_reservation_term(raw: str) -> str:
    term = raw.strip().lower()
    if "3" in term or "three" in term:
        return "3yr"
    if "1" in term or "one" in term:
        return "1yr"
    return raw.strip()


class AzureCollector(BaseCollector):
    name = "azure"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[azure] Fetching GPU VM pricing (no auth)")
        all_rows = []

        # Query for Virtual Machines service with GPU series
        for series in GPU_SERIES_KEYWORDS:
            rows = self._query_series(series)
            all_rows.extend(rows)
            logger.info(f"[azure] {series}: {len(rows)} rows")

        logger.info(f"[azure] Total: {len(all_rows)} rows")
        return all_rows

    def _query_series(self, series_prefix: str) -> List[Dict[str, Any]]:
        """Query Azure pricing API for a VM series, handling pagination."""
        filter_str = (
            f"serviceName eq 'Virtual Machines' and "
            f"contains(armSkuName, '{series_prefix}')"
        )
        params = {"$filter": filter_str, "api-version": "2023-01-01-preview"}
        url = f"{API_BASE}?{urllib.parse.urlencode(params)}"

        rows = []
        page = 0
        while url:
            page += 1
            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read().decode())
            except Exception as e:
                logger.warning(f"[azure] Page {page} failed for {series_prefix}: {e}")
                break

            items = data.get("Items", [])
            for item in items:
                row = self._parse_item(item)
                if row:
                    rows.append(row)

            url = data.get("NextPageLink", "")

        return rows

    def _parse_item(self, item: dict) -> Dict[str, Any]:
        """Parse a single Azure pricing API item into our schema."""
        sku = item.get("armSkuName", "")
        product_name = item.get("productName", "")
        meter_name = item.get("meterName", "")
        region = item.get("armRegionName", "")
        unit_price = item.get("unitPrice", 0)
        retail_price = item.get("retailPrice", 0)
        unit = item.get("unitOfMeasure", "")
        currency = item.get("currencyCode", "USD")
        sku_id = item.get("skuId", "")
        meter_type = item.get("type", "")
        reservation_term = item.get("reservationTerm", "")

        # Skip non-hourly, zero price
        if "Hour" not in unit:
            return None
        price = retail_price or unit_price
        if price <= 0:
            return None

        # Skip non-GPU or Windows etc unless interesting
        gpu_info = _infer_gpu_from_sku(sku, product_name)
        gpu_name = gpu_info.get("gpu", "")
        if not gpu_name:
            return None

        if meter_type.lower() == "reservation" or reservation_term:
            pricing_type = "reserved"
            commitment = _normalize_reservation_term(reservation_term)
        else:
            pricing_type, commitment = _classify_pricing_type(sku, meter_name)
        gpu_count = gpu_info.get("count", 0)
        price_per_gpu = price / gpu_count if gpu_count > 0 else price
        hourly_price = 0.0 if pricing_type == "reserved" and reservation_term else price
        hourly_price_per_gpu = 0.0 if pricing_type == "reserved" and reservation_term else price_per_gpu
        upfront_price = price if pricing_type == "reserved" and reservation_term else ""
        upfront_price_per_gpu = price_per_gpu if pricing_type == "reserved" and reservation_term else ""

        os_type = "Windows" if "Windows" in product_name else "Linux"
        if "Windows" in meter_name:
            os_type = "Windows"

        return self.make_row(
            provider="azure",
            instance_type=sku,
            instance_family=sku.split("_")[1] if "_" in sku else sku,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_variant=gpu_info.get("variant", ""),
            gpu_memory_gb=gpu_info.get("mem", ""),
            gpu_count=gpu_count,
            region=region,
            geo_group=infer_geo_group(region),
            pricing_type=pricing_type,
            commitment_period=commitment,
            price_per_hour=hourly_price,
            price_per_gpu_hour=round(hourly_price_per_gpu, 6),
            upfront_price=upfront_price,
            upfront_price_per_gpu=round(upfront_price_per_gpu, 6) if upfront_price_per_gpu != "" else "",
            currency=currency,
            os=os_type,
            available=True,
            raw_extra=json.dumps({
                "product_name": product_name,
                "meter_name": meter_name,
                "type": meter_type,
                "reservation_term": reservation_term,
                "unit_of_measure": unit,
                "sku_id": sku_id,
                "service_name": item.get("serviceName", ""),
                "location": item.get("location", ""),
                "effective_start": item.get("effectiveStartDate", ""),
                "is_primary_meter": item.get("isPrimaryMeterRegion", ""),
            }, separators=(",", ":")),
        )
