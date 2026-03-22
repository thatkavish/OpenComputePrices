"""
CUDO Compute GPU pricing collector.

Uses the CUDO Compute REST API at rest.compute.cudo.org/v1/vms/machine-types
which returns machine types with GPU pricing per data center.
No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_URL = "https://rest.compute.cudo.org/v1/vms/machine-types"
UA = "gpu-pricing-tracker/1.0"


class CudoCollector(BaseCollector):
    name = "cudo"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[cudo] Fetching machine types from REST API")

        try:
            req = urllib.request.Request(API_URL, headers={
                "User-Agent": UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.error(f"[cudo] API request failed: {e}")
            return []

        machine_types = data.get("machineTypes", [])
        rows = []

        for mt in machine_types:
            gpu_model = mt.get("gpuModel", "")
            if not gpu_model:
                continue

            datacenter = mt.get("dataCenterId", "")
            machine_type = mt.get("machineType", "")

            # Extract GPU pricing
            gpu_price_hr = mt.get("gpuPriceHr", {}) or {}
            gpu_price_val = gpu_price_hr.get("value", "0")
            try:
                gpu_price = float(gpu_price_val)
            except (ValueError, TypeError):
                continue
            if gpu_price <= 0:
                continue

            # Extract other component prices
            vcpu_price = mt.get("vcpuPriceHr", {}).get("value", "")
            mem_price = mt.get("memoryGibPriceHr", {}).get("value", "")

            min_gpu = mt.get("minGpuPerVm", 0)
            max_gpu = mt.get("maxGpuPerVm", 0)
            min_vcpu_per_gpu = mt.get("minVcpuPerGpu", 0)
            max_vcpu_per_gpu = mt.get("maxVcpuPerGpu", 0)
            gpu_mem = mt.get("gpuMemoryGib", 0)

            gpu_model_id = mt.get("gpuModelId", "")

            rows.append(self.make_row(
                provider="cudo",
                instance_type=f"{machine_type}_{gpu_model_id}" if gpu_model_id else machine_type,
                instance_family=machine_type,
                gpu_name=normalize_gpu_name(gpu_model),
                gpu_memory_gb=gpu_mem,
                gpu_count=1,
                region=datacenter,
                pricing_type="on_demand",
                price_per_hour=gpu_price,
                price_per_gpu_hour=gpu_price,
                available=True,
                raw_extra=json.dumps({
                    "gpu_model_id": gpu_model_id,
                    "cpu_model": mt.get("cpuModel", ""),
                    "vcpu_price_hr": vcpu_price,
                    "mem_price_hr": mem_price,
                    "min_gpu": min_gpu,
                    "max_gpu": max_gpu,
                    "min_vcpu_per_gpu": min_vcpu_per_gpu,
                    "max_vcpu_per_gpu": max_vcpu_per_gpu,
                }, separators=(",", ":")),
            ))

        logger.info(f"[cudo] Total: {len(rows)} rows from {len(machine_types)} machine types")
        return rows
