"""
Vast.ai marketplace GPU pricing collector.

Uses the Vast.ai REST API to fetch real-time marketplace offers.
Requires free API key (VASTAI_API_KEY env var).
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_URL = "https://console.vast.ai/api/v0/bundles/"


class VastAICollector(BaseCollector):
    name = "vastai"
    requires_api_key = True
    api_key_env_var = "VASTAI_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[vastai] Fetching marketplace offers")

        # Query for GPU offers — broad search
        payload = json.dumps({
            "verified": {},
            "type": "on-demand",
            "intended_status": "running",
            "order": [["dph_total", "asc"]],
            "limit": 5000,
        }).encode("utf-8")

        all_rows = []

        for offer_type in ["on-demand", "interruptible"]:
            payload = json.dumps({
                "verified": {},
                "type": offer_type,
                "intended_status": "running",
                "order": [["dph_total", "asc"]],
                "limit": 5000,
            }).encode("utf-8")

            try:
                req = urllib.request.Request(
                    API_URL,
                    data=payload,
                    method="POST",
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Authorization": f"Bearer {api_key}",
                    },
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read().decode())
            except Exception as e:
                logger.error(f"[vastai] API request failed for {offer_type}: {e}")
                continue

            offers = data.get("offers", [])
            pricing_type = "on_demand" if offer_type == "on-demand" else "spot"

            for offer in offers:
                row = self._parse_offer(offer, pricing_type)
                if row:
                    all_rows.append(row)

            logger.info(f"[vastai] {offer_type}: {len(offers)} offers")

        logger.info(f"[vastai] Total: {len(all_rows)} rows")
        return all_rows

    def _parse_offer(self, offer: dict, pricing_type: str) -> Dict[str, Any]:
        gpu_name = offer.get("gpu_name", "")
        if not gpu_name:
            return None

        dph = offer.get("dph_total", 0)
        if not dph or dph <= 0:
            return None

        gpu_count = offer.get("num_gpus", 1)
        gpu_ram = offer.get("gpu_ram", 0)
        gpu_mem_gb = round(gpu_ram / 1024, 1) if gpu_ram > 100 else gpu_ram

        price_per_gpu = dph / gpu_count if gpu_count > 0 else dph

        country = offer.get("geolocation", "")
        if country and "," in country:
            country = country.split(",")[-1].strip()

        return self.make_row(
            provider="vastai",
            instance_type=str(offer.get("id", "")),
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_memory_gb=gpu_mem_gb,
            gpu_count=gpu_count,
            gpu_interconnect=offer.get("pcie_bw", ""),
            vcpus=offer.get("cpu_cores_effective", ""),
            ram_gb=round(offer.get("cpu_ram", 0) / 1024, 1) if offer.get("cpu_ram", 0) > 100 else offer.get("cpu_ram", ""),
            storage_desc=f"{offer.get('disk_space', '')} GB" if offer.get("disk_space") else "",
            network_desc=f"{offer.get('inet_down', '')} Mbps down / {offer.get('inet_up', '')} Mbps up" if offer.get("inet_down") else "",
            region=offer.get("geolocation", ""),
            country=country,
            pricing_type=pricing_type,
            price_per_hour=round(dph, 6),
            price_per_gpu_hour=round(price_per_gpu, 6),
            available=True,
            available_count=1,
            raw_extra=json.dumps({
                "machine_id": offer.get("machine_id", ""),
                "host_id": offer.get("host_id", ""),
                "reliability": offer.get("reliability2", ""),
                "dlperf": offer.get("dlperf", ""),
                "dlperf_per_dphtotal": offer.get("dlperf_per_dphtotal", ""),
                "cuda_max_good": offer.get("cuda_max_good", ""),
                "driver_version": offer.get("driver_version", ""),
                "gpu_frac": offer.get("gpu_frac", ""),
                "gpu_total_ram": offer.get("gpu_total_ram", ""),
                "mobo_name": offer.get("mobo_name", ""),
                "cpu_name": offer.get("cpu_name", ""),
                "verified": offer.get("verified", ""),
                "static_ip": offer.get("static_ip", ""),
                "direct_port_count": offer.get("direct_port_count", ""),
            }, separators=(",", ":")),
        )
