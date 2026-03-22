"""
Novita AI pricing scraper.

Scrapes the Novita pricing page which contains HTML tables with
inference model pricing (per-token) and GPU instance pricing.
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

URL = "https://novita.ai/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class NovitaCollector(BaseCollector):
    name = "novita"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[novita] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[novita] Failed to fetch: {e}")
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

            # Check if this is a GPU instance pricing table
            is_gpu_table = any("gpu" in h for h in header) and any("price" in h or "$/h" in h or "/hr" in h for h in header)
            # Check if this is an inference model table
            is_inference = any("model" in h for h in header) and any("input" in h or "output" in h for h in header)

            if is_gpu_table:
                rows = self._parse_gpu_table(header, parsed[1:])
                all_rows.extend(rows)
            elif is_inference:
                rows = self._parse_inference_table(header, parsed[1:])
                all_rows.extend(rows)

        logger.info(f"[novita] Total: {len(all_rows)} rows")
        return all_rows

    def _parse_gpu_table(self, header, data_rows) -> List[Dict[str, Any]]:
        """Parse a GPU instance pricing table."""
        gpu_col = next((i for i, h in enumerate(header) if "gpu" in h), None)
        price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h), None)
        vram_col = next((i for i, h in enumerate(header) if "vram" in h or "memory" in h), None)
        vcpu_col = next((i for i, h in enumerate(header) if "vcpu" in h or "cpu" in h), None)
        ram_col = next((i for i, h in enumerate(header) if "ram" in h and "vram" not in h), None)

        if gpu_col is None or price_col is None:
            return []

        rows = []
        for data_row in data_rows:
            if len(data_row) <= max(gpu_col, price_col):
                continue

            gpu_name = data_row[gpu_col].strip()
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

            rows.append(self.make_row(
                provider="novita",
                instance_type=gpu_name,
                gpu_name=normalize_gpu_name(gpu_name),
                gpu_memory_gb=vram,
                gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=price,
                price_per_gpu_hour=price,
                available=True,
            ))

        return rows

    def _parse_inference_table(self, header, data_rows) -> List[Dict[str, Any]]:
        """Parse an inference model pricing table."""
        model_col = next((i for i, h in enumerate(header) if "model" in h), None)
        input_col = next((i for i, h in enumerate(header) if "input" in h), None)
        output_col = next((i for i, h in enumerate(header) if "output" in h), None)
        context_col = next((i for i, h in enumerate(header) if "context" in h), None)

        if model_col is None or (input_col is None and output_col is None):
            return []

        rows = []
        for data_row in data_rows:
            if len(data_row) <= model_col:
                continue

            model_name = data_row[model_col].strip()
            if not model_name:
                continue

            input_price = ""
            output_price = ""
            context_len = ""

            if input_col is not None and input_col < len(data_row):
                input_price = data_row[input_col].strip()
            if output_col is not None and output_col < len(data_row):
                output_price = data_row[output_col].strip()
            if context_col is not None and context_col < len(data_row):
                context_len = data_row[context_col].strip()

            # Extract numeric prices
            inp_m = re.search(r"\$?([\d.]+)", input_price)
            out_m = re.search(r"\$?([\d.]+)", output_price)

            rows.append(self.make_row(
                provider="novita",
                instance_type=model_name,
                pricing_type="inference",
                price_per_hour=0,
                price_per_gpu_hour=0,
                price_unit="token",
                available=True,
                raw_extra=json.dumps({
                    "model_name": model_name,
                    "input_price": input_price,
                    "output_price": output_price,
                    "context_length": context_len,
                }, separators=(",", ":")),
            ))

        return rows
