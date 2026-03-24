"""
DeepInfra GPU and inference pricing collector.

Uses the public DeepInfra API at api.deepinfra.com/models/list
which returns model pricing without authentication.
Also scrapes the pricing page for GPU instance pricing.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

MODELS_API = "https://api.deepinfra.com/models/list"
PRICING_URL = "https://deepinfra.com/pricing"
UA = "OpenComputePrices/1.0"
UA_BROWSER = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class DeepInfraCollector(BaseCollector):
    name = "deepinfra"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[deepinfra] Fetching pricing data")
        rows = []

        # Method 1: Public models API
        rows.extend(self._fetch_models_api())

        # Method 2: Scrape pricing page for GPU instance pricing
        rows.extend(self._scrape_pricing_page())

        logger.info(f"[deepinfra] Total: {len(rows)} rows")
        return rows

    def _fetch_models_api(self) -> List[Dict[str, Any]]:
        """Fetch model list with pricing from public API."""
        try:
            req = urllib.request.Request(MODELS_API, headers={
                "User-Agent": UA, "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                models = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning(f"[deepinfra] Models API failed: {e}")
            return []

        if not isinstance(models, list):
            return []

        rows = []
        for model in models:
            if not isinstance(model, dict):
                continue

            model_name = model.get("model_name", "")
            pricing = model.get("pricing", {}) or {}
            input_price = pricing.get("cents_per_input_token", 0) or pricing.get("input", 0)
            output_price = pricing.get("cents_per_output_token", 0) or pricing.get("output", 0)

            if not input_price and not output_price:
                # Check for per-second GPU pricing
                gpu_price = pricing.get("cents_per_second", 0)
                if gpu_price:
                    try:
                        hourly = float(gpu_price) * 3600 / 100  # cents/sec -> $/hr
                    except (ValueError, TypeError):
                        continue
                    if hourly > 0:
                        rows.append(self.make_row(
                            provider="deepinfra",
                            instance_type=model_name,
                            gpu_name="",
                            pricing_type="on_demand",
                            price_per_hour=round(hourly, 6),
                            price_per_gpu_hour=round(hourly, 6),
                            price_unit="hour",
                            available=True,
                            raw_extra=json.dumps({
                                "model_name": model_name,
                                "cents_per_second": gpu_price,
                                "type": model.get("type", ""),
                            }, separators=(",", ":")),
                        ))
                continue

            rows.append(self.make_row(
                provider="deepinfra",
                instance_type=model_name,
                pricing_type="inference",
                price_per_hour=0,
                price_per_gpu_hour=0,
                price_unit="token",
                available=True,
                raw_extra=json.dumps({
                    "model_name": model_name,
                    "cents_per_input_token": input_price,
                    "cents_per_output_token": output_price,
                    "type": model.get("type", ""),
                    "task": model.get("task", ""),
                    "reported_max_tokens": model.get("max_tokens", ""),
                }, separators=(",", ":")),
            ))

        logger.info(f"[deepinfra] Models API: {len(rows)} models")
        return rows

    def _scrape_pricing_page(self) -> List[Dict[str, Any]]:
        """Scrape the pricing page for GPU instance pricing via __NEXT_DATA__."""
        try:
            req = urllib.request.Request(PRICING_URL, headers={"User-Agent": UA_BROWSER})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"[deepinfra] Pricing page failed: {e}")
            return []

        nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
        if not nd:
            return []

        try:
            data = json.loads(nd.group(1))
        except (json.JSONDecodeError, ValueError):
            return []

        props = data.get("props", {}).get("pageProps", {})
        rows = []

        # Extract GPU instance pricing from _pricingPageData
        page_data = props.get("_pricingPageData", [])
        for section in page_data:
            if not isinstance(section, dict):
                continue
            entries = section.get("entries", [])
            section_title = section.get("title", "")
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("name", "") or entry.get("model", "")
                price_str = entry.get("price", "") or entry.get("cost", "")
                if not name or not price_str:
                    continue
                try:
                    price = float(str(price_str).replace("$", "").replace("/hr", "").strip())
                except (ValueError, TypeError):
                    continue
                if price > 0:
                    gpu_m = re.search(r'(H100|H200|A100|A10|L4|L40S?|V100|T4|B200|A40)', name, re.I)
                    rows.append(self.make_row(
                        provider="deepinfra",
                        instance_type=name,
                        gpu_name=normalize_gpu_name(gpu_m.group(1)) if gpu_m else "",
                        pricing_type="on_demand",
                        price_per_hour=price,
                        price_per_gpu_hour=price,
                        available=True,
                        raw_extra=json.dumps({
                            "section": section_title,
                        }, separators=(",", ":")),
                    ))

        if rows:
            logger.info(f"[deepinfra] Pricing page: {len(rows)} GPU rows")
        return rows
