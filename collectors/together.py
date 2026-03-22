"""
Together.ai inference pricing scraper.

Scrapes the Together.ai pricing page which contains HTML tables
with model pricing (per-token) for inference services.
No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector

logger = logging.getLogger(__name__)

URL = "https://www.together.ai/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class TogetherCollector(BaseCollector):
    name = "together"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[together] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[together] Failed to fetch: {e}")
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

            # Look for model pricing tables (model name + input/output pricing)
            model_col = next((i for i, h in enumerate(header) if "model" in h or "name" in h), None)
            input_col = next((i for i, h in enumerate(header) if "input" in h or "prompt" in h), None)
            output_col = next((i for i, h in enumerate(header) if "output" in h or "completion" in h), None)
            context_col = next((i for i, h in enumerate(header) if "context" in h or "token" in h and "price" not in h), None)

            # Also check for GPU instance pricing
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h), None)

            if model_col is not None and (input_col is not None or output_col is not None):
                for data_row in parsed[1:]:
                    if len(data_row) <= model_col:
                        continue
                    model_name = data_row[model_col].strip()
                    if not model_name:
                        continue

                    input_price = data_row[input_col].strip() if input_col is not None and input_col < len(data_row) else ""
                    output_price = data_row[output_col].strip() if output_col is not None and output_col < len(data_row) else ""
                    context_len = data_row[context_col].strip() if context_col is not None and context_col < len(data_row) else ""

                    all_rows.append(self.make_row(
                        provider="together",
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

            elif gpu_col is not None and price_col is not None:
                for data_row in parsed[1:]:
                    if len(data_row) <= max(gpu_col, price_col):
                        continue
                    gpu_name = data_row[gpu_col].strip()
                    price_m = re.search(r"\$?([\d.]+)", data_row[price_col])
                    if not price_m or not gpu_name:
                        continue
                    price = float(price_m.group(1))
                    if price > 0:
                        from schema import normalize_gpu_name
                        all_rows.append(self.make_row(
                            provider="together",
                            instance_type=gpu_name,
                            gpu_name=normalize_gpu_name(gpu_name),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[together] Total: {len(all_rows)} rows")
        return all_rows
