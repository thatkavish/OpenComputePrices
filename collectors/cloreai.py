"""
Clore.ai marketplace GPU pricing collector.

Uses the public Clore.ai marketplace API instead of the removed /pricing page.
The marketplace API reports USD prices per day, so rows normalize to USD/hour.
"""

import http.client
import json
import logging
import re
from typing import Any, Dict, List

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_HOST = "api.clore.ai"
API_PATH = "/v1/marketplace"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _parse_gpu(specs: dict) -> tuple[str, int]:
    gpu_array = specs.get("gpu_array") or []
    gpu_raw = gpu_array[0] if len(gpu_array) == 1 else specs.get("gpu", "")
    if "mixed" in str(gpu_raw).lower():
        return "", 0

    count = 1
    count_match = re.search(r"(\d+)\s*x", specs.get("gpu", ""), re.I)
    if count_match:
        count = int(count_match.group(1))

    gpu_raw = re.sub(r"^\s*\d+\s*x\s*", "", str(gpu_raw), flags=re.I)
    gpu_raw = re.sub(r"\b(?:NVIDIA|GeForce)\b", "", gpu_raw, flags=re.I).strip()
    gpu_name = normalize_gpu_name(gpu_raw)
    return gpu_name, count


def _daily_usd(price: dict, key: str) -> float:
    usd = price.get("usd", {}) or {}
    value = usd.get(key)
    if value in (None, "") and key == "spot":
        value = (price.get("original_usd", {}) or {}).get("bitcoin", {}).get("spot")
    if value in (None, "") and key != "spot":
        value = (price.get("on_demand", {}) or {}).get("USD-Blockchain")
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


class CloreAICollector(BaseCollector):
    name = "cloreai"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[cloreai] Fetching marketplace offers")
        conn = None
        try:
            conn = http.client.HTTPSConnection(API_HOST, timeout=60)
            conn.request("GET", API_PATH, headers={"Accept": "application/json", "User-Agent": UA})
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")
            data = json.loads(body)
        except Exception as e:
            logger.error(f"[cloreai] Marketplace request failed: {e}")
            return []
        finally:
            if conn:
                conn.close()

        rows = []
        for server in data.get("servers", []):
            rows.extend(self._parse_server(server))

        logger.info(f"[cloreai] Total: {len(rows)} rows")
        return rows

    def _parse_server(self, server: dict) -> List[Dict[str, Any]]:
        specs = server.get("specs", {}) or {}
        gpu_name, gpu_count = _parse_gpu(specs)
        if not gpu_name or gpu_count <= 0:
            return []

        price = server.get("price", {}) or {}
        rows = []
        for price_key, pricing_type in [("on_demand_usd", "on_demand"), ("spot", "spot")]:
            daily = _daily_usd(price, price_key)
            if daily <= 0:
                continue
            hourly = daily / 24
            rows.append(self.make_row(
                provider="cloreai",
                instance_type=str(server.get("id", "")),
                gpu_name=gpu_name,
                gpu_memory_gb=specs.get("gpuram", ""),
                gpu_count=gpu_count,
                vcpus=specs.get("cpus", ""),
                ram_gb=round(specs.get("ram", 0), 3) if specs.get("ram") else "",
                storage_desc=specs.get("disk", ""),
                network_desc=json.dumps(specs.get("net", {}), separators=(",", ":"), default=str),
                country=(specs.get("net", {}) or {}).get("cc", ""),
                pricing_type=pricing_type,
                price_per_hour=round(hourly, 6),
                price_per_gpu_hour=round(hourly / gpu_count, 6),
                available=not server.get("rented", False),
                raw_extra=json.dumps({
                    "server_id": server.get("id", ""),
                    "daily_usd": daily,
                    "pricing_source": price_key,
                    "reliability": server.get("reliability", ""),
                    "rating": server.get("rating", ""),
                    "gpu_raw": specs.get("gpu", ""),
                }, separators=(",", ":"), default=str),
            ))
        return rows
