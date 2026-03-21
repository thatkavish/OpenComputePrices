"""
TensorDock GPU pricing collector.

Uses the TensorDock v2 API:
  - GET /api/v2/locations  — locations with GPU offerings and pricing
  - GET /api/v2/hostnodes  — individual hostnodes with live availability
No authentication required for these discovery endpoints.
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

LOCATIONS_URL = "https://dashboard.tensordock.com/api/v2/locations"
HOSTNODES_URL = "https://dashboard.tensordock.com/api/v2/hostnodes"


def _parse_gpu_name(v0name: str, display_name: str) -> tuple:
    """Extract canonical GPU name and variant from TensorDock v0Name/displayName."""
    dn = display_name or v0name or ""
    dl = dn.lower()

    # Extract VRAM from v0name pattern like "h100-sxm5-80gb"
    vram = 0
    if "80gb" in dl:
        vram = 80
    elif "48gb" in dl:
        vram = 48
    elif "24gb" in dl:
        vram = 24
    elif "16gb" in dl:
        vram = 16
    elif "12gb" in dl:
        vram = 12

    variant = ""
    if "sxm5" in dl:
        variant = "SXM5"
    elif "sxm4" in dl:
        variant = "SXM4"
    elif "sxm" in dl:
        variant = "SXM"
    elif "pcie" in dl:
        variant = "PCIe"
    elif "nvl" in dl:
        variant = "NVL"

    return normalize_gpu_name(dn.split(" ")[0] if " " not in dn else dn.rsplit(" ", 1)[0].replace("NVIDIA ", "").replace("GeForce ", "")), variant, vram


class TensorDockCollector(BaseCollector):
    name = "tensordock"
    requires_api_key = False  # locations works without auth; hostnodes needs key
    api_key_env_var = "TENSORDOCK_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[tensordock] Fetching GPU pricing from v2 API")

        rows = []

        # Try locations endpoint first (aggregated view)
        rows.extend(self._fetch_locations())

        # Also try hostnodes for live per-node availability
        rows.extend(self._fetch_hostnodes())

        logger.info(f"[tensordock] Total: {len(rows)} rows")
        return rows

    def _fetch_locations(self) -> List[Dict[str, Any]]:
        """Fetch from /api/v2/locations — aggregated GPU offerings by location."""
        try:
            req = urllib.request.Request(LOCATIONS_URL, headers={
                "Accept": "application/json",
                "User-Agent": "gpu-pricing-tracker/1.0",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning(f"[tensordock] locations endpoint failed: {e}")
            return []

        locations = data.get("data", {}).get("locations", [])
        rows = []

        for loc in locations:
            loc_id = loc.get("id", "")
            city = loc.get("city", "")
            state = loc.get("stateprovince", "")
            country = loc.get("country", "")
            tier = loc.get("tier", "")
            region_str = f"{city}, {state}, {country}" if state else f"{city}, {country}"

            gpus = loc.get("gpus", [])
            for gpu in gpus:
                v0name = gpu.get("v0Name", "")
                display = gpu.get("displayName", "")
                max_count = gpu.get("max_count", 0)
                price_per_hr = gpu.get("price_per_hr", 0)
                resources = gpu.get("resources", {}) or {}
                pricing = gpu.get("pricing", {}) or {}
                network = gpu.get("network_features", {}) or {}

                if not price_per_hr or price_per_hr <= 0:
                    continue

                gpu_name, variant, vram = _parse_gpu_name(v0name, display)

                rows.append(self.make_row(
                    provider="tensordock",
                    instance_type=v0name,
                    gpu_name=gpu_name,
                    gpu_variant=variant,
                    gpu_memory_gb=vram,
                    gpu_count=1,
                    vcpus=resources.get("max_vcpus", ""),
                    ram_gb=resources.get("max_ram_gb", ""),
                    storage_desc=f"{resources.get('max_storage_gb', '')} GB" if resources.get("max_storage_gb") else "",
                    region=region_str,
                    country=country,
                    pricing_type="on_demand",
                    price_per_hour=round(price_per_hr, 6),
                    price_per_gpu_hour=round(price_per_hr, 6),
                    available=max_count > 0,
                    available_count=max_count,
                    raw_extra=json.dumps({
                        "location_id": loc_id,
                        "tier": tier,
                        "per_vcpu_hr": pricing.get("per_vcpu_hr", ""),
                        "per_gb_ram_hr": pricing.get("per_gb_ram_hr", ""),
                        "per_gb_storage_hr": pricing.get("per_gb_storage_hr", ""),
                        "dedicated_ip": network.get("dedicated_ip_available", ""),
                        "source": "locations",
                    }, separators=(",", ":")),
                ))

        logger.info(f"[tensordock] locations: {len(rows)} rows from {len(locations)} locations")
        return rows

    def _fetch_hostnodes(self) -> List[Dict[str, Any]]:
        """Fetch from /api/v2/hostnodes — per-node live availability. Requires API key."""
        api_key = self.get_api_key()
        headers = {
            "Accept": "application/json",
            "User-Agent": "gpu-pricing-tracker/1.0",
        }
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            req = urllib.request.Request(HOSTNODES_URL, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.warning(f"[tensordock] hostnodes endpoint failed: {e}")
            return []

        hostnodes = data.get("data", {}).get("hostnodes", [])
        rows = []

        for node in hostnodes:
            node_id = node.get("id", "")
            uptime = node.get("uptime_percentage", "")
            avail = node.get("available_resources", {}) or {}
            pricing = node.get("pricing", {}) or {}
            location = node.get("location", {}) or {}

            city = location.get("city", "")
            country = location.get("country", "")
            region_str = f"{city}, {country}" if city else country
            net_speed = location.get("network_speed_gbps", "")

            gpus = avail.get("gpus", [])
            for gpu in gpus:
                v0name = gpu.get("v0Name", "")
                avail_count = gpu.get("availableCount", 0)
                price_per_hr = gpu.get("price_per_hr", 0)

                if not price_per_hr or price_per_hr <= 0:
                    continue

                gpu_name, variant, vram = _parse_gpu_name(v0name, "")

                rows.append(self.make_row(
                    provider="tensordock",
                    instance_type=f"{node_id}_{v0name}",
                    gpu_name=gpu_name,
                    gpu_variant=variant,
                    gpu_memory_gb=vram,
                    gpu_count=1,
                    vcpus=avail.get("vcpu_count", ""),
                    ram_gb=avail.get("ram_gb", ""),
                    storage_desc=f"{avail.get('storage_gb', '')} GB" if avail.get("storage_gb") else "",
                    network_desc=f"{net_speed} Gbps" if net_speed else "",
                    region=region_str,
                    country=country,
                    pricing_type="on_demand",
                    price_per_hour=round(price_per_hr, 6),
                    price_per_gpu_hour=round(price_per_hr, 6),
                    available=avail_count > 0,
                    available_count=avail_count,
                    raw_extra=json.dumps({
                        "hostnode_id": node_id,
                        "uptime_pct": uptime,
                        "per_vcpu_hr": pricing.get("per_vcpu_hr", ""),
                        "per_gb_ram_hr": pricing.get("per_gb_ram_hr", ""),
                        "org_name": location.get("organizationName", ""),
                        "tier": location.get("tier", ""),
                        "source": "hostnodes",
                    }, separators=(",", ":")),
                ))

        logger.info(f"[tensordock] hostnodes: {len(rows)} rows from {len(hostnodes)} nodes")
        return rows
