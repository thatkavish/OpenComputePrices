"""
Latitude.sh GPU pricing scraper.

Scrapes the Latitude.sh pricing page which contains HTML tables
with GPU plans, specs, and hourly/monthly pricing.
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

URL = "https://www.latitude.sh/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class LatitudeCollector(BaseCollector):
    name = "latitude"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[latitude] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[latitude] Failed to fetch: {e}")
            return []

        tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.S)
        all_rows = []

        for table in tables:
            parsed = []
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
                cells = [re.sub(r"<[^>]+>", " ", c).strip() for c in cells]
                cells = [re.sub(r"\s+", " ", c).strip() for c in cells]
                if any(cells):
                    parsed.append(cells)

            if len(parsed) < 2:
                continue

            header = [h.lower() for h in parsed[0]]

            # Look for GPU pricing table (has "GPU" and "Starting At" or price columns)
            has_gpu = any("gpu" in h for h in header)
            has_price = any("starting" in h or "price" in h or "/hr" in h or "/mo" in h for h in header)
            if not has_gpu or not has_price:
                continue

            plan_col = next((i for i, h in enumerate(header) if "plan" in h), None)
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h and "vram" not in h), None)
            vram_col = next((i for i, h in enumerate(header) if "vram" in h), None)
            storage_col = next((i for i, h in enumerate(header) if "storage" in h), None)
            network_col = next((i for i, h in enumerate(header) if "network" in h), None)
            price_col = next((i for i, h in enumerate(header) if "starting" in h or "price" in h), None)

            for data_row in parsed[1:]:
                if len(data_row) < 3:
                    continue

                # Extract GPU info
                gpu_text = data_row[gpu_col] if gpu_col is not None and gpu_col < len(data_row) else ""
                plan_text = data_row[plan_col] if plan_col is not None and plan_col < len(data_row) else ""
                price_text = data_row[price_col] if price_col is not None and price_col < len(data_row) else ""

                if not gpu_text or not price_text:
                    continue

                # Parse GPU name and count
                gpu_name = ""
                gpu_count = 1
                gpu_m = re.search(r"(\d+)\s*x\s*(.*?)(?:\s*\d+GB|\s*$)", gpu_text, re.I)
                if gpu_m:
                    gpu_count = int(gpu_m.group(1))
                    gpu_name = gpu_m.group(2).strip()
                else:
                    gpu_n = re.search(r"(H100|H200|A100|L40S?|RTX[^,]*|A40|B200|MI\d+\w*)", gpu_text, re.I)
                    if gpu_n:
                        gpu_name = gpu_n.group(1)

                if not gpu_name:
                    continue

                # Parse VRAM
                vram = 0
                vram_text = data_row[vram_col] if vram_col is not None and vram_col < len(data_row) else gpu_text
                vram_m = re.search(r"(\d+)\s*GB\s*VRAM", vram_text, re.I)
                if not vram_m:
                    vram_m = re.search(r"(\d+)\s*GB", vram_text)
                if vram_m:
                    vram = int(vram_m.group(1))

                # Parse prices (hourly and monthly)
                hourly = 0
                monthly = 0
                hourly_m = re.search(r"\$([\d,.]+)/hr", price_text)
                monthly_m = re.search(r"\$([\d,.]+)/mo", price_text)
                if hourly_m:
                    hourly = float(hourly_m.group(1).replace(",", ""))
                if monthly_m:
                    monthly = float(monthly_m.group(1).replace(",", ""))

                if not hourly and monthly:
                    hourly = monthly / 730.0

                if hourly <= 0:
                    continue

                price_per_gpu = hourly / gpu_count if gpu_count > 0 else hourly

                # Detect variant
                variant = ""
                if "sxm" in gpu_text.lower():
                    variant = "SXM"
                elif "pcie" in gpu_text.lower() or "PCIe" in gpu_text:
                    variant = "PCIe"
                elif "nvlink" in gpu_text.lower():
                    variant = "NVLink"

                storage = ""
                if storage_col is not None and storage_col < len(data_row):
                    storage = data_row[storage_col]

                all_rows.append(self.make_row(
                    provider="latitude",
                    instance_type=plan_text or f"{gpu_name}x{gpu_count}",
                    gpu_name=normalize_gpu_name(gpu_name),
                    gpu_variant=variant,
                    gpu_memory_gb=vram,
                    gpu_count=gpu_count,
                    storage_desc=storage,
                    pricing_type="on_demand",
                    price_per_hour=round(hourly, 6),
                    price_per_gpu_hour=round(price_per_gpu, 6),
                    available=True,
                    raw_extra=json.dumps({
                        "monthly": monthly,
                        "gpu_detail": gpu_text[:100],
                    }, separators=(",", ":")),
                ))

        logger.info(f"[latitude] Total: {len(all_rows)} rows")
        return all_rows
