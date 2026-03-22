"""
E2E Networks (E2E Cloud) GPU pricing scraper.

Scrapes the E2E Cloud pricing page which contains clean HTML tables
with GPU instances, VRAM, vCPUs, RAM, and hourly/monthly/annual pricing.
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

URL = "https://www.e2enetworks.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class E2ECollector(BaseCollector):
    name = "e2e"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[e2e] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[e2e] Failed to fetch: {e}")
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

            # Must have GPU item column and hourly pricing
            item_col = next((i for i, h in enumerate(header) if "item" in h or "gpu" in h or "model" in h), None)
            vram_col = next((i for i, h in enumerate(header) if "vram" in h), None)
            vcpu_col = next((i for i, h in enumerate(header) if "vcpu" in h or "cpu" in h), None)
            ram_col = next((i for i, h in enumerate(header) if "ram" in h and "vram" not in h), None)
            hourly_col = next((i for i, h in enumerate(header) if "hourly" in h or "on-demand" in h or "demand" in h), None)
            monthly_col = next((i for i, h in enumerate(header) if "monthly" in h), None)
            annual_col = next((i for i, h in enumerate(header) if "annual" in h), None)

            if item_col is None or (hourly_col is None and monthly_col is None):
                continue

            for data_row in parsed[1:]:
                if len(data_row) <= item_col:
                    continue

                item_name = data_row[item_col].strip()
                if not item_name:
                    continue

                # Must be a GPU item
                gpu_m = re.search(r"(B200|H200|H100|A100|A10|L40S?|V100|T4|A40|MI300)", item_name, re.I)
                if not gpu_m:
                    continue

                # Parse hourly price
                hourly = 0
                if hourly_col is not None and hourly_col < len(data_row):
                    pm = re.search(r"\$?([\d,.]+)", data_row[hourly_col])
                    if pm:
                        hourly = float(pm.group(1).replace(",", ""))

                monthly = 0
                if monthly_col is not None and monthly_col < len(data_row):
                    pm = re.search(r"\$?([\d,.]+)", data_row[monthly_col])
                    if pm:
                        monthly = float(pm.group(1).replace(",", ""))

                annual = 0
                if annual_col is not None and annual_col < len(data_row):
                    pm = re.search(r"\$?([\d,.]+)", data_row[annual_col])
                    if pm:
                        annual = float(pm.group(1).replace(",", ""))

                if not hourly and monthly:
                    hourly = monthly / 730.0

                if hourly <= 0:
                    continue

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

                all_rows.append(self.make_row(
                    provider="e2e",
                    instance_type=item_name,
                    gpu_name=normalize_gpu_name(gpu_m.group(1)),
                    gpu_memory_gb=vram,
                    gpu_count=1,
                    vcpus=vcpus,
                    ram_gb=ram_gb,
                    pricing_type="on_demand",
                    price_per_hour=round(hourly, 6),
                    price_per_gpu_hour=round(hourly, 6),
                    available=True,
                    raw_extra=json.dumps({
                        "monthly": monthly,
                        "annual": annual,
                    }, separators=(",", ":")),
                ))

        logger.info(f"[e2e] Total: {len(all_rows)} rows")
        return all_rows
