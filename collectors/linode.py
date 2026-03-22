"""
Linode (Akamai) GPU pricing collector.

Uses the public Linode API at api.linode.com/v4/linode/types
which returns all plan types including GPU instances with pricing.
No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name, infer_geo_group

logger = logging.getLogger(__name__)

API_URL = "https://api.linode.com/v4/linode/types"
UA = "gpu-pricing-tracker/1.0"

# Linode GPU type ID patterns
GPU_TYPE_PREFIXES = ["g1-gpu", "g2-gpu", "g3-gpu"]


class LinodeCollector(BaseCollector):
    name = "linode"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[linode] Fetching GPU plans from public API")

        all_rows = []
        page = 1

        while True:
            try:
                url = f"{API_URL}?page={page}&page_size=100"
                req = urllib.request.Request(url, headers={
                    "User-Agent": UA, "Accept": "application/json",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
            except Exception as e:
                logger.error(f"[linode] API request failed: {e}")
                break

            plans = data.get("data", [])
            for plan in plans:
                plan_id = plan.get("id", "")
                plan_class = plan.get("type_class", "")

                # Filter to GPU plans
                is_gpu = plan_class == "gpu" or any(plan_id.startswith(p) for p in GPU_TYPE_PREFIXES)
                if not is_gpu:
                    continue

                label = plan.get("label", "")
                price_info = plan.get("price", {}) or {}
                hourly = price_info.get("hourly", 0)
                monthly = price_info.get("monthly", 0)

                if not hourly and monthly:
                    hourly = monthly / 730.0

                if not hourly or hourly <= 0:
                    continue

                memory = plan.get("memory", 0)  # MB
                ram_gb = round(memory / 1024, 1) if memory else ""
                vcpus = plan.get("vcpus", "")
                disk = plan.get("disk", 0)  # MB
                disk_gb = round(disk / 1024) if disk else ""
                gpus = plan.get("gpus", 0) or 1

                # Infer GPU type from label
                gpu_name = ""
                gpu_mem = 0
                ll = label.lower()
                if "rtx" in ll and "4000" in ll:
                    gpu_name = "RTX 4000 Ada"
                    gpu_mem = 20
                elif "a100" in ll:
                    gpu_name = "A100"
                    gpu_mem = 40
                elif "l40s" in ll:
                    gpu_name = "L40S"
                    gpu_mem = 48

                price_per_gpu = hourly / gpus if gpus > 0 else hourly

                # Base row (global pricing)
                all_rows.append(self.make_row(
                    provider="linode",
                    instance_type=plan_id,
                    gpu_name=normalize_gpu_name(gpu_name) if gpu_name else label,
                    gpu_memory_gb=gpu_mem,
                    gpu_count=gpus,
                    vcpus=vcpus,
                    ram_gb=ram_gb,
                    storage_desc=f"{disk_gb} GB" if disk_gb else "",
                    pricing_type="on_demand",
                    price_per_hour=round(hourly, 6),
                    price_per_gpu_hour=round(price_per_gpu, 6),
                    available=True,
                    raw_extra=json.dumps({
                        "label": label,
                        "monthly": monthly,
                        "transfer": plan.get("transfer", ""),
                        "network_out": plan.get("network_out", ""),
                        "type_class": plan_class,
                    }, separators=(",", ":")),
                ))

                # Regional pricing overrides
                region_prices = plan.get("region_prices", []) or []
                for rp in region_prices:
                    region = rp.get("id", "")
                    r_hourly = rp.get("hourly", 0)
                    r_monthly = rp.get("monthly", 0)
                    if not r_hourly and r_monthly:
                        r_hourly = r_monthly / 730.0
                    if r_hourly and r_hourly > 0 and r_hourly != hourly:
                        r_price_per_gpu = r_hourly / gpus if gpus > 0 else r_hourly
                        all_rows.append(self.make_row(
                            provider="linode",
                            instance_type=f"{plan_id}_{region}",
                            gpu_name=normalize_gpu_name(gpu_name) if gpu_name else label,
                            gpu_memory_gb=gpu_mem,
                            gpu_count=gpus,
                            vcpus=vcpus,
                            ram_gb=ram_gb,
                            region=region,
                            geo_group=infer_geo_group(region),
                            pricing_type="on_demand",
                            price_per_hour=round(r_hourly, 6),
                            price_per_gpu_hour=round(r_price_per_gpu, 6),
                            available=True,
                            raw_extra=json.dumps({
                                "label": label,
                                "regional_monthly": r_monthly,
                            }, separators=(",", ":")),
                        ))

            total_pages = data.get("pages", 1)
            if page >= total_pages:
                break
            page += 1

        logger.info(f"[linode] Total: {len(all_rows)} GPU rows")
        return all_rows
