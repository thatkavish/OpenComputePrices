"""
Lambda Cloud GPU pricing collector.

Uses the Lambda Cloud API to fetch instance types and pricing.
Requires API key (LAMBDA_API_KEY env var).
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_URL = "https://cloud.lambdalabs.com/api/v1/instance-types"


# Known Lambda GPU specs
LAMBDA_GPU_MAP = {
    "gpu_1x_h100_sxm5": {"gpu": "H100", "mem": 80, "count": 1, "variant": "SXM5"},
    "gpu_8x_h100_sxm5": {"gpu": "H100", "mem": 80, "count": 8, "variant": "SXM5"},
    "gpu_1x_h200":       {"gpu": "H200", "mem": 141, "count": 1, "variant": "SXM"},
    "gpu_8x_h200":       {"gpu": "H200", "mem": 141, "count": 8, "variant": "SXM"},
    "gpu_1x_a100":       {"gpu": "A100", "mem": 40, "count": 1, "variant": "SXM4"},
    "gpu_2x_a100":       {"gpu": "A100", "mem": 40, "count": 2, "variant": "SXM4"},
    "gpu_4x_a100":       {"gpu": "A100", "mem": 40, "count": 4, "variant": "SXM4"},
    "gpu_8x_a100":       {"gpu": "A100", "mem": 40, "count": 8, "variant": "SXM4"},
    "gpu_1x_a100_sxm4":  {"gpu": "A100", "mem": 40, "count": 1, "variant": "SXM4"},
    "gpu_8x_a100_80gb_sxm4": {"gpu": "A100", "mem": 80, "count": 8, "variant": "SXM4"},
    "gpu_1x_a10":        {"gpu": "A10", "mem": 24, "count": 1, "variant": ""},
    "gpu_1x_rtx6000":    {"gpu": "RTX 6000 Ada", "mem": 48, "count": 1, "variant": "Ada"},
    "gpu_1x_a6000":      {"gpu": "RTX A6000", "mem": 48, "count": 1, "variant": ""},
    "gpu_2x_a6000":      {"gpu": "RTX A6000", "mem": 48, "count": 2, "variant": ""},
    "gpu_4x_a6000":      {"gpu": "RTX A6000", "mem": 48, "count": 4, "variant": ""},
    "gpu_8x_v100":       {"gpu": "V100", "mem": 16, "count": 8, "variant": "SXM2"},
    "gpu_1x_b200":       {"gpu": "B200", "mem": 192, "count": 1, "variant": ""},
    "gpu_8x_b200":       {"gpu": "B200", "mem": 192, "count": 8, "variant": ""},
}


def _infer_gpu_from_name(instance_name: str) -> Dict[str, Any]:
    """Infer GPU info from Lambda instance type name."""
    if instance_name in LAMBDA_GPU_MAP:
        return LAMBDA_GPU_MAP[instance_name]

    # Try pattern matching: gpu_Nx_<gpu_name>
    parts = instance_name.lower().split("_")
    count = 1
    gpu_name = ""
    for i, p in enumerate(parts):
        if p.endswith("x") and p[:-1].isdigit():
            count = int(p[:-1])
        elif p in ("h100", "h200", "a100", "a10", "v100", "l4", "l40s", "b200", "gb200"):
            gpu_name = p.upper()

    return {"gpu": gpu_name, "mem": 0, "count": count, "variant": ""}


class LambdaCollector(BaseCollector):
    name = "lambda"
    requires_api_key = True
    api_key_env_var = "LAMBDA_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[lambda] Fetching instance types and pricing")

        try:
            req = urllib.request.Request(API_URL, headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.error(f"[lambda] API request failed: {e}")
            return []

        instance_types = data.get("data", {})
        rows = []

        for inst_name, inst_data in instance_types.items():
            inst_info = inst_data.get("instance_type", {})
            price = inst_info.get("price_cents_per_hour", 0)

            if not price or price <= 0:
                continue

            price_usd = price / 100.0

            gpu_info = _infer_gpu_from_name(inst_name)
            gpu_count = gpu_info.get("count", 0) or inst_info.get("specs", {}).get("gpus", 0)

            specs = inst_info.get("specs", {}) or {}
            vcpus = specs.get("vcpus", "")
            ram_gb = specs.get("memory_gib", "")
            storage = specs.get("storage_gib", "")

            price_per_gpu = price_usd / gpu_count if gpu_count > 0 else price_usd

            # Regions with availability
            regions = inst_data.get("regions_with_capacity_available", []) or []
            region_names = [r.get("name", "") for r in regions if isinstance(r, dict)]

            if region_names:
                for region in region_names:
                    rows.append(self.make_row(
                        provider="lambda",
                        instance_type=inst_name,
                        gpu_name=normalize_gpu_name(gpu_info.get("gpu", "")),
                        gpu_variant=gpu_info.get("variant", ""),
                        gpu_memory_gb=gpu_info.get("mem", ""),
                        gpu_count=gpu_count,
                        vcpus=vcpus,
                        ram_gb=ram_gb,
                        storage_desc=f"{storage} GiB" if storage else "",
                        region=region,
                        pricing_type="on_demand",
                        price_per_hour=round(price_usd, 6),
                        price_per_gpu_hour=round(price_per_gpu, 6),
                        available=True,
                        raw_extra=json.dumps({
                            "description": inst_info.get("description", ""),
                        }, separators=(",", ":")),
                    ))
            else:
                rows.append(self.make_row(
                    provider="lambda",
                    instance_type=inst_name,
                    gpu_name=normalize_gpu_name(gpu_info.get("gpu", "")),
                    gpu_variant=gpu_info.get("variant", ""),
                    gpu_memory_gb=gpu_info.get("mem", ""),
                    gpu_count=gpu_count,
                    vcpus=vcpus,
                    ram_gb=ram_gb,
                    storage_desc=f"{storage} GiB" if storage else "",
                    pricing_type="on_demand",
                    price_per_hour=round(price_usd, 6),
                    price_per_gpu_hour=round(price_per_gpu, 6),
                    available=False,
                    raw_extra=json.dumps({
                        "description": inst_info.get("description", ""),
                    }, separators=(",", ":")),
                ))

        logger.info(f"[lambda] Total: {len(rows)} rows")
        return rows
