"""
Massed Compute GPU pricing scraper.

Scrapes the Massed Compute pricing page which contains clean HTML tables
with GPU configurations, VRAM, vCPUs, RAM, storage, and hourly pricing.
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

URL = "https://massedcompute.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class MassedComputeCollector(BaseCollector):
    name = "massedcompute"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[massedcompute] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[massedcompute] Failed to fetch: {e}")
            return []

        # Find GPU section headers to identify which GPU each table belongs to
        # Pattern: heading with GPU name followed by a table
        sections = re.split(r"<(?:h[1-4]|div)[^>]*>", html)
        
        all_rows = []
        current_gpu = ""

        for section in sections:
            # Check if this section introduces a GPU name
            gpu_m = re.search(
                r"(H200|H100|A100|L40S?|A6000|A5000|RTX\s*\d+|A40|V100|B200|MI300X)",
                section[:200], re.I
            )
            if gpu_m:
                current_gpu = gpu_m.group(1).strip()

            # Look for tables in this section
            tables = re.findall(r"<table[^>]*>(.*?)</table>", section, re.S)
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
                qty_col = next((i for i, h in enumerate(header) if "quantity" in h or "gpu" in h), None)
                vram_col = next((i for i, h in enumerate(header) if "vram" in h), None)
                vcpu_col = next((i for i, h in enumerate(header) if "vcpu" in h or "cpu" in h), None)
                ram_col = next((i for i, h in enumerate(header) if "ram" in h and "vram" not in h), None)
                storage_col = next((i for i, h in enumerate(header) if "storage" in h), None)
                price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h), None)

                if price_col is None:
                    continue

                for data_row in parsed[1:]:
                    if len(data_row) <= price_col:
                        continue

                    price_m = re.search(r"\$?([\d.]+)/hr", data_row[price_col])
                    if not price_m:
                        price_m = re.search(r"\$?([\d.]+)", data_row[price_col])
                    if not price_m:
                        continue
                    price = float(price_m.group(1))
                    if price <= 0:
                        continue

                    gpu_count = 1
                    if qty_col is not None and qty_col < len(data_row):
                        qty_m = re.search(r"(\d+)", data_row[qty_col])
                        if qty_m:
                            gpu_count = int(qty_m.group(1))

                    vram = 0
                    if vram_col is not None and vram_col < len(data_row):
                        vm = re.search(r"(\d+)", data_row[vram_col])
                        if vm:
                            vram = int(vm.group(1))

                    vcpus = ""
                    if vcpu_col is not None and vcpu_col < len(data_row):
                        vcpus = data_row[vcpu_col]

                    ram_gb = ""
                    if ram_col is not None and ram_col < len(data_row):
                        rm = re.search(r"(\d+)", data_row[ram_col])
                        if rm:
                            ram_gb = rm.group(1)

                    storage = ""
                    if storage_col is not None and storage_col < len(data_row):
                        storage = data_row[storage_col]

                    price_per_gpu = price / gpu_count if gpu_count > 0 else price

                    all_rows.append(self.make_row(
                        provider="massedcompute",
                        instance_type=f"{current_gpu}x{gpu_count}" if current_gpu else f"gpu_x{gpu_count}",
                        gpu_name=normalize_gpu_name(current_gpu) if current_gpu else "",
                        gpu_memory_gb=vram,
                        gpu_count=gpu_count,
                        vcpus=vcpus,
                        ram_gb=ram_gb,
                        storage_desc=storage,
                        pricing_type="on_demand",
                        price_per_hour=price,
                        price_per_gpu_hour=round(price_per_gpu, 6),
                        available=True,
                    ))

        logger.info(f"[massedcompute] Total: {len(all_rows)} rows")
        return all_rows
