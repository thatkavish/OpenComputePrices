"""
JarvisLabs GPU pricing scraper.

Scrapes the JarvisLabs pricing page which contains clean HTML tables
with GPU type, generation, VRAM, RAM, vCPUs, and $/hour.
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

URL = "https://jarvislabs.ai/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class JarvisLabsCollector(BaseCollector):
    name = "jarvislabs"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[jarvislabs] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[jarvislabs] Failed to fetch: {e}")
            return []

        tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.S)
        all_rows = []

        for table in tables:
            parsed = []
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
                cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
                cells = [re.sub(r"\s+", " ", c).strip() for c in cells]
                if any(cells):
                    parsed.append(cells)

            if len(parsed) < 2:
                continue

            header = [h.lower() for h in parsed[0]]

            # Identify columns
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h and "vram" not in h), None)
            gen_col = next((i for i, h in enumerate(header) if "generation" in h), None)
            vram_col = next((i for i, h in enumerate(header) if "vram" in h), None)
            ram_col = next((i for i, h in enumerate(header) if "ram" in h and "vram" not in h), None)
            vcpu_col = next((i for i, h in enumerate(header) if "vcpu" in h or "cpu" in h), None)
            price_col = next((i for i, h in enumerate(header) if "$/hour" in h or "price" in h or "/hr" in h), None)
            storage_col = next((i for i, h in enumerate(header) if "storage" in h), None)

            if gpu_col is None or price_col is None:
                continue

            for data_row in parsed[1:]:
                if len(data_row) <= max(gpu_col, price_col):
                    continue

                gpu_name_raw = data_row[gpu_col].strip()
                price_str = data_row[price_col].strip()

                m = re.search(r"\$?([\d.]+)", price_str)
                if not m:
                    continue
                price = float(m.group(1))
                if price <= 0:
                    continue

                vram = 0
                if vram_col is not None and vram_col < len(data_row):
                    vm = re.search(r"(\d+)", data_row[vram_col])
                    if vm:
                        vram = float(vm.group(1))

                ram_gb = ""
                if ram_col is not None and ram_col < len(data_row):
                    rm = re.search(r"(\d+)", data_row[ram_col])
                    if rm:
                        ram_gb = rm.group(1)

                vcpus = ""
                if vcpu_col is not None and vcpu_col < len(data_row):
                    vcpus = data_row[vcpu_col]

                generation = ""
                if gen_col is not None and gen_col < len(data_row):
                    generation = data_row[gen_col]

                storage = ""
                if storage_col is not None and storage_col < len(data_row):
                    storage = data_row[storage_col]

                # Detect variant from name
                variant = ""
                nl = gpu_name_raw.lower()
                if "sxm" in nl:
                    variant = "SXM"
                elif "pcie" in nl:
                    variant = "PCIe"

                all_rows.append(self.make_row(
                    provider="jarvislabs",
                    instance_type=gpu_name_raw,
                    gpu_name=normalize_gpu_name(gpu_name_raw),
                    gpu_variant=variant,
                    gpu_memory_gb=vram,
                    gpu_count=1,
                    vcpus=vcpus,
                    ram_gb=ram_gb,
                    storage_desc=storage,
                    pricing_type="on_demand",
                    price_per_hour=price,
                    price_per_gpu_hour=price,
                    available=True,
                    raw_extra=json.dumps({
                        "generation": generation,
                    }, separators=(",", ":")),
                ))

        logger.info(f"[jarvislabs] Total: {len(all_rows)} rows")
        return all_rows
