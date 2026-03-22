"""
Crusoe Cloud GPU pricing scraper.

Scrapes the Crusoe pricing page which contains GPU names and $/GPU-hr prices
in structured HTML divs. No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

URL = "https://www.crusoe.ai/cloud/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Known Crusoe GPU offerings with specs
CRUSOE_GPUS = {
    "B200":   {"mem": 192, "variant": ""},
    "H200":   {"mem": 141, "variant": "SXM"},
    "H100":   {"mem": 80,  "variant": "SXM5"},
    "A100":   {"mem": 80,  "variant": "SXM4"},
    "L40S":   {"mem": 48,  "variant": ""},
    "MI300X": {"mem": 192, "variant": ""},
    "A40":    {"mem": 48,  "variant": ""},
}


class CrusoeCollector(BaseCollector):
    name = "crusoe"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[crusoe] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[crusoe] Failed to fetch: {e}")
            return []

        rows = []

        # Strategy: find GPU names in the HTML and locate nearby $/GPU-hr prices.
        # The Crusoe page uses a pattern like:
        #   <div>NVIDIA A100</div> ... <div>80GB</div> ... <div>SXM</div> ... <div>$1.95/GPU-hr</div>

        # Extract all price entries with $/GPU-hr format
        price_entries = re.findall(
            r'\$([\d.]+)/GPU-hr', html
        )

        # Find GPU name blocks near prices - use a sliding window approach
        # Split HTML into chunks around each GPU name
        for gpu_key, specs in CRUSOE_GPUS.items():
            # Find all occurrences of this GPU name
            pattern = re.compile(
                r'(?:NVIDIA\s+)?' + re.escape(gpu_key) + r'(?:\s|<)',
                re.I
            )
            for match in pattern.finditer(html):
                pos = match.start()
                # Look in a window after this GPU name for price
                window = html[pos:pos + 2000]
                prices = re.findall(r'\$([\d.]+)/GPU-hr', window)
                vram_match = re.search(r'(\d+)\s*GB', window)
                interconnect = ""
                if "SXM" in window[:500]:
                    interconnect = "SXM"
                elif "PCIe" in window[:500]:
                    interconnect = "PCIe"

                for price_str in prices:
                    try:
                        price = float(price_str)
                    except ValueError:
                        continue
                    if price <= 0:
                        continue

                    # Determine pricing type from context
                    pricing_type = "on_demand"
                    context_before_price = window[:window.find(price_str)]
                    if "spot" in context_before_price.lower():
                        pricing_type = "spot"
                    elif "reserved" in context_before_price.lower() or "commit" in context_before_price.lower():
                        pricing_type = "reserved"

                    row_id = f"{gpu_key}_{pricing_type}_{price_str}"
                    # Avoid exact duplicates
                    if any(r.get("raw_extra", "").find(row_id) >= 0 for r in rows):
                        continue

                    rows.append(self.make_row(
                        provider="crusoe",
                        instance_type=f"{gpu_key}_{pricing_type}",
                        gpu_name=normalize_gpu_name(gpu_key),
                        gpu_variant=interconnect or specs.get("variant", ""),
                        gpu_memory_gb=specs.get("mem", 0),
                        gpu_count=1,
                        pricing_type=pricing_type,
                        price_per_hour=price,
                        price_per_gpu_hour=price,
                        available=True,
                        raw_extra=json.dumps({
                            "row_id": row_id,
                        }, separators=(",", ":")),
                    ))
                    break  # Take first price per GPU per section

        # Deduplicate by (gpu_name, pricing_type, price)
        seen = set()
        unique_rows = []
        for r in rows:
            key = (r["gpu_name"], r["pricing_type"], r["price_per_gpu_hour"])
            if key not in seen:
                seen.add(key)
                unique_rows.append(r)

        logger.info(f"[crusoe] Total: {len(unique_rows)} rows")
        return unique_rows
