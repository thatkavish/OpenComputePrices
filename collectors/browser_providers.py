"""
Playwright-based scrapers for JS-rendered pricing pages.

Each class targets a specific provider whose pricing page is fully
client-side rendered and has no discoverable free API.
"""

import json
import logging
import re
from typing import List, Dict, Any

from collectors.browser_scraper import BrowserScraper
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CoreWeave — Platinum tier
# ---------------------------------------------------------------------------
class CoreWeaveBrowserCollector(BrowserScraper):
    name = "coreweave"
    url = "https://www.coreweave.com/pricing"
    wait_selector = "[class*='price'], [class*='Price'], table"

    KNOWN_SPECS = {
        "H100": {"mem": 80, "variant": "SXM5"},
        "H200": {"mem": 141, "variant": "SXM"},
        "B200": {"mem": 192, "variant": ""},
        "GB200": {"mem": 192, "variant": ""},
        "A100": {"mem": 80, "variant": "SXM4"},
        "A40": {"mem": 48, "variant": ""},
        "L40S": {"mem": 48, "variant": ""},
        "RTX A6000": {"mem": 48, "variant": ""},
        "RTX A5000": {"mem": 24, "variant": ""},
        "RTX A4000": {"mem": 16, "variant": ""},
    }

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        seen = set()

        # CoreWeave uses custom div rows with class "table-model-name" for GPU names
        # Pattern: NVIDIA <GPU> ... specs ... $XX.XX
        # Match GPU name blocks followed by a price within a wider window
        for match in re.finditer(
            r'(?:NVIDIA\s+)((?:GB200|B200|H200|H100|A100|A40|L40S?|L40|RTX\s*A?\d+)[^<]{0,60})',
            html, re.I
        ):
            gpu_raw = match.group(1).strip()
            pos = match.start()
            # Search a wider window for the price (CoreWeave has specs between name and price)
            window = html[pos:pos + 800]
            # Find all dollar amounts in the window
            prices = re.findall(r'\$([\d.]+)', window)
            if not prices:
                continue
            # Take the first reasonable price (>0.10, <200 per GPU-hr)
            price = 0
            for p_str in prices:
                try:
                    p = float(p_str)
                except ValueError:
                    continue
                if 0.10 < p < 200:
                    price = p
                    break
            if price <= 0:
                continue

            # Clean up GPU name
            gpu_clean = re.sub(r'\s*(NVL\d*|SXM\d*|PCIe|HGX)\s*', ' ', gpu_raw).strip()
            gpu_clean = re.sub(r'\s+\d+$', '', gpu_clean).strip()  # trailing numbers
            gpu_m = re.search(r'(GB200|B200|H200|H100|A100|A40|L40S?|RTX\s*A?\d+)', gpu_clean, re.I)
            if not gpu_m:
                continue
            gn = gpu_m.group(1)

            # Detect variant from context
            variant = ""
            ctx = gpu_raw.lower()
            if "sxm" in ctx:
                variant = "SXM" + (re.search(r'sxm(\d)', ctx).group(1) if re.search(r'sxm(\d)', ctx) else "")
            elif "pcie" in ctx:
                variant = "PCIe"
            elif "nvl" in ctx:
                variant = "NVL"
            elif "hgx" in ctx:
                variant = "HGX"

            key = (gn.upper(), price)
            if key in seen:
                continue
            seen.add(key)

            specs = self.KNOWN_SPECS.get(gn.upper(), {})
            if variant:
                specs_variant = variant
            else:
                specs_variant = specs.get("variant", "")

            rows.append(self.make_row(
                provider="coreweave", instance_type=gpu_raw,
                gpu_name=normalize_gpu_name(gn),
                gpu_variant=specs_variant,
                gpu_memory_gb=specs.get("mem", 0),
                gpu_count=1, pricing_type="on_demand",
                price_per_hour=price, price_per_gpu_hour=price,
                available=True,
            ))

        return rows


# ---------------------------------------------------------------------------
# Together.ai — Silver tier (inference pricing)
# ---------------------------------------------------------------------------
class TogetherBrowserCollector(BrowserScraper):
    name = "together"
    url = "https://www.together.ai/pricing"
    wait_selector = "table, [class*='pricing']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            model_col = next((i for i, h in enumerate(header) if "model" in h or "name" in h), None)
            input_col = next((i for i, h in enumerate(header) if "input" in h or "prompt" in h), None)
            output_col = next((i for i, h in enumerate(header) if "output" in h or "completion" in h), None)
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h), None)

            if model_col is not None and (input_col is not None or output_col is not None):
                for row in table[1:]:
                    if len(row) <= model_col:
                        continue
                    model_name = row[model_col].strip()
                    if not model_name:
                        continue
                    inp = row[input_col].strip() if input_col is not None and input_col < len(row) else ""
                    out = row[output_col].strip() if output_col is not None and output_col < len(row) else ""
                    rows.append(self.make_row(
                        provider="together", instance_type=model_name,
                        pricing_type="inference", price_per_hour=0, price_per_gpu_hour=0,
                        price_unit="token", available=True,
                        raw_extra=json.dumps({"model": model_name, "input": inp, "output": out}, separators=(",", ":")),
                    ))
            elif gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="together", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw), gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        return rows


# ---------------------------------------------------------------------------
# Hyperstack — Bronze tier
# ---------------------------------------------------------------------------
class HyperstackBrowserCollector(BrowserScraper):
    name = "hyperstack"
    url = "https://www.hyperstack.cloud/gpu-pricing"
    wait_selector = "[class*='price'], [class*='gpu'], table"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        # Try tables
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h), None)
            vram_col = next((i for i, h in enumerate(header) if "vram" in h or "memory" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if not pm:
                        continue
                    price = float(pm.group(1))
                    if price <= 0:
                        continue
                    vram = 0
                    if vram_col and vram_col < len(row):
                        vm = re.search(r"(\d+)", row[vram_col])
                        if vm:
                            vram = int(vm.group(1))
                    rows.append(self.make_row(
                        provider="hyperstack", instance_type=gpu_raw,
                        gpu_name=normalize_gpu_name(gpu_raw),
                        gpu_memory_gb=vram, gpu_count=1,
                        pricing_type="on_demand",
                        price_per_hour=price, price_per_gpu_hour=price,
                        available=True,
                    ))

        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="hyperstack", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows


# ---------------------------------------------------------------------------
# Gcore — Silver tier
# ---------------------------------------------------------------------------
class GcoreBrowserCollector(BrowserScraper):
    name = "gcore"
    url = "https://gcore.com/cloud/ai-gpu"
    wait_selector = "[class*='price'], [class*='gpu'], table"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h or "instance" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h or "cost" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"[\$€]?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="gcore", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw), gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="gcore", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows


# ---------------------------------------------------------------------------
# Firmus — Silver tier
# ---------------------------------------------------------------------------
class FirmusBrowserCollector(BrowserScraper):
    name = "firmus"
    url = "https://firmus.ai/pricing"
    wait_selector = "[class*='price'], table"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for pair in self.extract_gpu_price_pairs(html):
            rows.append(self.make_row(
                provider="firmus", instance_type=pair["gpu"],
                gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                available=True,
            ))
        return rows


# ---------------------------------------------------------------------------
# Neysa — Bronze tier
# ---------------------------------------------------------------------------
class NeysaBrowserCollector(BrowserScraper):
    name = "neysa"
    url = "https://www.neysa.ai/pricing"
    wait_selector = "table, [class*='price']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h or "instance" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h), None)
            vram_col = next((i for i, h in enumerate(header) if "vram" in h or "memory" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            vram = 0
                            if vram_col and vram_col < len(row):
                                vm = re.search(r"(\d+)", row[vram_col])
                                if vm:
                                    vram = int(vm.group(1))
                            rows.append(self.make_row(
                                provider="neysa", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw),
                                gpu_memory_gb=vram, gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="neysa", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows


# ---------------------------------------------------------------------------
# GMI Cloud — Bronze tier
# ---------------------------------------------------------------------------
class GMICloudBrowserCollector(BrowserScraper):
    name = "gmicloud"
    url = "https://gmicloud.ai/pricing"
    wait_selector = "[class*='price'], table"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for pair in self.extract_gpu_price_pairs(html):
            rows.append(self.make_row(
                provider="gmicloud", instance_type=pair["gpu"],
                gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                available=True,
            ))
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="gmicloud", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw), gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        return rows


# ---------------------------------------------------------------------------
# Lightning AI — Bronze tier
# ---------------------------------------------------------------------------
class LightningAIBrowserCollector(BrowserScraper):
    name = "lightningai"
    url = "https://lightning.ai/pricing"
    wait_selector = "table, [class*='price']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "accelerator" in h or "machine" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "credit" in h or "cost" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="lightningai", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw), gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="lightningai", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows


# ---------------------------------------------------------------------------
# Salad — Underperforming tier (re-implement with browser)
# ---------------------------------------------------------------------------
class SaladBrowserCollector(BrowserScraper):
    name = "salad"
    url = "https://salad.com/pricing"
    wait_selector = "table, [class*='price'], [class*='gpu']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h or "class" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="salad", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw), gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="salad", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows


# ---------------------------------------------------------------------------
# Clore.ai — Underperforming tier (GPU marketplace)
# ---------------------------------------------------------------------------
class CloreAIBrowserCollector(BrowserScraper):
    name = "cloreai"
    url = "https://clore.ai/pricing"
    wait_selector = "table, [class*='price'], [class*='gpu']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h or "cost" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="cloreai", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw), gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="cloreai", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows


# ---------------------------------------------------------------------------
# Exabits — Underperforming tier
# ---------------------------------------------------------------------------
class ExabitsBrowserCollector(BrowserScraper):
    name = "exabits"
    url = "https://www.exabits.ai/pricing"
    wait_selector = "table, [class*='price']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for pair in self.extract_gpu_price_pairs(html):
            rows.append(self.make_row(
                provider="exabits", instance_type=pair["gpu"],
                gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                available=True,
            ))
        return rows


# ---------------------------------------------------------------------------
# Aethir — Underperforming tier
# ---------------------------------------------------------------------------
class AethirBrowserCollector(BrowserScraper):
    name = "aethir"
    url = "https://www.aethir.com/pricing"
    wait_selector = "table, [class*='price']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for pair in self.extract_gpu_price_pairs(html):
            rows.append(self.make_row(
                provider="aethir", instance_type=pair["gpu"],
                gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                available=True,
            ))
        return rows


# ---------------------------------------------------------------------------
# Qubrid — Bronze tier
# ---------------------------------------------------------------------------
class QubridBrowserCollector(BrowserScraper):
    name = "qubrid"
    url = "https://www.qubrid.com/pricing"
    wait_selector = "table, [class*='price']"

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        for table in self.extract_tables(html):
            if len(table) < 2:
                continue
            header = [h.lower() for h in table[0]]
            gpu_col = next((i for i, h in enumerate(header) if "gpu" in h or "model" in h or "instance" in h), None)
            price_col = next((i for i, h in enumerate(header) if "price" in h or "$/h" in h or "/hr" in h or "cost" in h), None)
            vram_col = next((i for i, h in enumerate(header) if "vram" in h or "memory" in h), None)
            if gpu_col is not None and price_col is not None:
                for row in table[1:]:
                    if len(row) <= max(gpu_col, price_col):
                        continue
                    gpu_raw = row[gpu_col]
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            vram = 0
                            if vram_col and vram_col < len(row):
                                vm = re.search(r"(\d+)", row[vram_col])
                                if vm:
                                    vram = int(vm.group(1))
                            rows.append(self.make_row(
                                provider="qubrid", instance_type=gpu_raw,
                                gpu_name=normalize_gpu_name(gpu_raw),
                                gpu_memory_gb=vram, gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                rows.append(self.make_row(
                    provider="qubrid", instance_type=pair["gpu"],
                    gpu_name=normalize_gpu_name(pair["gpu"]), gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=pair["price"], price_per_gpu_hour=pair["price"],
                    available=True,
                ))
        return rows
