"""
Akash Network GPU pricing collector.

Uses the same console API that powers Akash's public GPU pricing page:
https://akash.network/pricing/gpus/

The public page renders a single displayed price per GPU model from the API's
``weightedAverage`` field, despite the UI label saying "Starting at". To keep
our canonical dataset aligned with what users actually see on the live pricing
page, this collector emits one row per GPU model using that page-visible price
and preserves the full price summary in ``raw_extra``.
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
UA = "OpenComputePrices/1.0"
PUBLIC_PRICE_METRIC = "weightedAverage"
FALLBACK_PRICE_METRICS = ["avg", "med", "min", "max"]


def _is_implausible_akash_price(gpu_name: str, price: float) -> bool:
    if gpu_name == "GTX 1070 Ti" and price > 20:
        return True
    return False


def _parse_price_number(raw) -> float:
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


def _extract_price_summary(price_info: dict) -> Dict[str, float]:
    summary = {}
    for key in [PUBLIC_PRICE_METRIC, *FALLBACK_PRICE_METRICS]:
        if key in price_info:
            summary[key] = round(_parse_price_number(price_info.get(key)), 6)
    return summary


def _select_public_price(price_info: dict) -> tuple[float, str, Dict[str, float]]:
    summary = _extract_price_summary(price_info)

    preferred = summary.get(PUBLIC_PRICE_METRIC, 0.0)
    if preferred > 0:
        return preferred, PUBLIC_PRICE_METRIC, summary

    for key in FALLBACK_PRICE_METRICS:
        value = summary.get(key, 0.0)
        if value > 0:
            return value, key, summary

    return 0.0, "", summary


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

            price, price_metric, price_summary = _select_public_price(price_info)
            if price <= 0:
                continue

            normalized_gpu = normalize_gpu_name(gpu_model)
            if _is_implausible_akash_price(normalized_gpu, price):
                logger.warning(f"[akash] Dropping implausible {normalized_gpu} {price_metric or 'price'}: {price}")
                continue

            rows.append(self.make_row(
                provider="akash",
                instance_type=f"{gpu_model}_{interface}" if interface else gpu_model,
                gpu_name=normalized_gpu,
                gpu_variant=interface,
                gpu_memory_gb=gpu_mem,
                gpu_count=1,
                region="global",
                pricing_type="on_demand",
                price_per_hour=round(price, 6),
                price_per_gpu_hour=round(price, 6),
                currency=currency,
                available=available_gpus > 0,
                available_count=available_gpus,
                raw_extra=json.dumps({
                    "vendor": vendor,
                    "price_metric": price_metric,
                    "price_summary": price_summary,
                    "total_gpus": total_gpus,
                    "total_providers": total_providers,
                    "available_providers": avail_providers,
                }, separators=(",", ":")),
            ))

        logger.info(f"[akash] Total: {len(rows)} rows from {len(models)} GPU models")
        return rows
