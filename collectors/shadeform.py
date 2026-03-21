"""
Shadeform cross-provider GPU pricing collector.

Aggregates pricing from 21+ providers via a single API.
Requires free API key (SHADEFORM_API_KEY env var).
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name, infer_geo_group

logger = logging.getLogger(__name__)

API_URL = "https://api.shadeform.ai/v1/instances/types"


class ShadeformCollector(BaseCollector):
    name = "shadeform"
    requires_api_key = True
    api_key_env_var = "SHADEFORM_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[shadeform] Fetching cross-provider GPU pricing")

        try:
            req = urllib.request.Request(API_URL, method="GET", headers={
                "Accept": "application/json",
                "X-API-KEY": api_key,
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.error(f"[shadeform] API request failed: {e}")
            return []

        instances = data.get("instance_types", [])
        rows = []

        for inst in instances:
            cloud = inst.get("cloud", "")
            shade_id = inst.get("shade_instance_type", "")
            cloud_id = inst.get("cloud_instance_type", "")
            config = inst.get("configuration", {}) or {}
            hourly = inst.get("hourly_price", 0)
            avail = inst.get("availability", []) or []

            gpu_type = config.get("gpu_type", "")
            gpu_count = config.get("num_gpus", 0)
            gpu_mem = config.get("vram_per_gpu_in_gb", 0)
            vcpus = config.get("num_cpus", "")
            ram = config.get("ram_in_gb", "")
            interconnect = config.get("interconnect", "")
            storage = config.get("storage_in_gb", "")

            try:
                price = float(hourly) / 100  # Shadeform returns cents
            except (ValueError, TypeError):
                continue

            if price <= 0:
                continue

            price_per_gpu = price / gpu_count if gpu_count > 0 else price

            # Parse availability per region
            if avail:
                for a in avail:
                    region = a.get("region", "")
                    is_avail = a.get("available", False)
                    rows.append(self.make_row(
                        provider=cloud.lower(),
                        instance_type=cloud_id or shade_id,
                        gpu_name=normalize_gpu_name(gpu_type),
                        gpu_memory_gb=gpu_mem,
                        gpu_count=gpu_count,
                        gpu_interconnect=interconnect,
                        vcpus=vcpus,
                        ram_gb=ram,
                        storage_desc=f"{storage} GB" if storage else "",
                        region=region,
                        geo_group=infer_geo_group(region),
                        pricing_type="on_demand",
                        price_per_hour=round(price, 6),
                        price_per_gpu_hour=round(price_per_gpu, 6),
                        available=is_avail,
                        raw_extra=json.dumps({
                            "shade_instance_type": shade_id,
                            "cloud": cloud,
                        }, separators=(",", ":")),
                    ))
            else:
                rows.append(self.make_row(
                    provider=cloud.lower(),
                    instance_type=cloud_id or shade_id,
                    gpu_name=normalize_gpu_name(gpu_type),
                    gpu_memory_gb=gpu_mem,
                    gpu_count=gpu_count,
                    gpu_interconnect=interconnect,
                    vcpus=vcpus,
                    ram_gb=ram,
                    storage_desc=f"{storage} GB" if storage else "",
                    pricing_type="on_demand",
                    price_per_hour=round(price, 6),
                    price_per_gpu_hour=round(price_per_gpu, 6),
                    available=True,
                    raw_extra=json.dumps({
                        "shade_instance_type": shade_id,
                        "cloud": cloud,
                    }, separators=(",", ":")),
                ))

        logger.info(f"[shadeform] Total: {len(rows)} rows from {len(set(r['provider'] for r in rows))} providers")
        return rows
