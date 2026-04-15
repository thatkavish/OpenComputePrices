"""
SkyPilot Catalog collector.

Pulls GPU pricing data from the open-source SkyPilot Catalog CSV files on GitHub.
No authentication required. Apache 2.0 licensed.
"""

import csv
import io
import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import infer_geo_group, normalize_gpu_memory_gb, normalize_gpu_name

logger = logging.getLogger(__name__)

# SkyPilot catalog raw CSV URLs on GitHub.
# Keep this pinned to the latest catalog generation we have validated.
CATALOG_VERSION = "v8"
CATALOG_BASE = (
    "https://raw.githubusercontent.com/skypilot-org/skypilot-catalog/master/"
    f"catalogs/{CATALOG_VERSION}"
)

# Cloud → CSV filename mapping
CATALOGS = {
    "aws":        f"{CATALOG_BASE}/aws/vms.csv",
    "azure":      f"{CATALOG_BASE}/azure/vms.csv",
    "gcp":        f"{CATALOG_BASE}/gcp/vms.csv",
    "lambda":     f"{CATALOG_BASE}/lambda/vms.csv",
    "runpod":     f"{CATALOG_BASE}/runpod/vms.csv",
    "fluidstack": f"{CATALOG_BASE}/fluidstack/vms.csv",
    "vastai":     f"{CATALOG_BASE}/vast/vms.csv",
    "cudo":       f"{CATALOG_BASE}/cudo/vms.csv",
    "paperspace": f"{CATALOG_BASE}/paperspace/vms.csv",
    "nebius":     f"{CATALOG_BASE}/nebius/vms.csv",
    "oci":        f"{CATALOG_BASE}/oci/vms.csv",
    "hyperstack": f"{CATALOG_BASE}/hyperstack/vms.csv",
    "ibm":        f"{CATALOG_BASE}/ibm/vms.csv",
    "scaleway":   f"{CATALOG_BASE}/scaleway/vms.csv",
    "do":         f"{CATALOG_BASE}/do/vms.csv",
}


class SkyPilotCollector(BaseCollector):
    name = "skypilot"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[skypilot] Fetching GPU pricing from SkyPilot Catalog CSVs")

        all_rows = []
        for cloud, url in CATALOGS.items():
            rows = self._fetch_catalog(cloud, url)
            all_rows.extend(rows)
            logger.info(f"[skypilot] {cloud}: {len(rows)} GPU rows")

        logger.info(f"[skypilot] Total: {len(all_rows)} rows")
        return all_rows

    def _fetch_catalog(self, cloud: str, url: str) -> List[Dict[str, Any]]:
        """Fetch and parse a single SkyPilot catalog CSV."""
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "OpenComputePrices/1.0",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                text = resp.read().decode("utf-8")
        except Exception as e:
            logger.warning(f"[skypilot] Failed to fetch {cloud}: {e}")
            return []

        reader = csv.DictReader(io.StringIO(text))
        rows = []

        for record in reader:
            gpu_name = record.get("AcceleratorName", "") or record.get("accelerator_name", "")
            if not gpu_name:
                continue

            # On-demand price
            price_str = record.get("Price", "") or record.get("price", "")
            spot_str = record.get("SpotPrice", "") or record.get("spot_price", "")

            instance_type = record.get("InstanceType", "") or record.get("instance_type", "")
            region = record.get("Region", "") or record.get("region", "")
            zone = record.get("AvailabilityZone", "") or record.get("zone", "")
            vcpus = record.get("vCPUs", "") or record.get("cpus", "")
            ram = record.get("MemoryGiB", "") or record.get("memory", "")
            gpu_count = record.get("AcceleratorCount", "") or record.get("accelerator_count", "")
            gpu_mem = normalize_gpu_memory_gb(
                record.get("GpuInfo", "") or record.get("accelerator_memory", ""),
                gpu_name,
                gpu_count,
            )

            try:
                gpu_count_int = int(float(gpu_count)) if gpu_count else 0
            except (ValueError, TypeError):
                gpu_count_int = 0

            # On-demand row
            if price_str:
                try:
                    price = float(price_str)
                    if price > 0:
                        price_per_gpu = price / gpu_count_int if gpu_count_int > 0 else price
                        rows.append(self.make_row(
                            provider=cloud,
                            instance_type=instance_type,
                            gpu_name=normalize_gpu_name(gpu_name),
                            gpu_memory_gb=gpu_mem,
                            gpu_count=gpu_count_int,
                            vcpus=vcpus,
                            ram_gb=ram,
                            region=region,
                            zone=zone,
                            geo_group=infer_geo_group(region),
                            pricing_type="on_demand",
                            price_per_hour=round(price, 6),
                            price_per_gpu_hour=round(price_per_gpu, 6),
                            available=True,
                        ))
                except (ValueError, TypeError):
                    pass

            # Spot row
            if spot_str:
                try:
                    spot_price = float(spot_str)
                    if spot_price > 0:
                        spot_per_gpu = spot_price / gpu_count_int if gpu_count_int > 0 else spot_price
                        rows.append(self.make_row(
                            provider=cloud,
                            instance_type=instance_type,
                            gpu_name=normalize_gpu_name(gpu_name),
                            gpu_memory_gb=gpu_mem,
                            gpu_count=gpu_count_int,
                            vcpus=vcpus,
                            ram_gb=ram,
                            region=region,
                            zone=zone,
                            geo_group=infer_geo_group(region),
                            pricing_type="spot",
                            price_per_hour=round(spot_price, 6),
                            price_per_gpu_hour=round(spot_per_gpu, 6),
                            available=True,
                        ))
                except (ValueError, TypeError):
                    pass

        return rows
