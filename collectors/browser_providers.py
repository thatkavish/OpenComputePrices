"""
Playwright-based scrapers for JS-rendered pricing pages.

Each class targets a specific provider whose pricing page is fully
client-side rendered and has no discoverable free API.
"""

import json
import logging
import re
from html import unescape
from typing import List, Dict, Any, Optional

from collectors.browser_scraper import BrowserScraper
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)


def _parse_price(line: str):
    match = re.search(r"([$€])\s*([\d.]+)", line)
    if not match:
        return None, ""
    currency = "EUR" if match.group(1) == "€" else "USD"
    return float(match.group(2)), currency


def _parse_memory_gb(text: str) -> int:
    match = re.search(r"(\d+)\s*GB", text, re.I)
    return int(match.group(1)) if match else 0


def _parse_gpu_count(text: str):
    match = re.search(r"(\d+(?:\.\d+)?)", str(text or ""))
    if not match:
        return 0
    value = float(match.group(1))
    return int(value) if value.is_integer() else round(value, 6)


def _table_cell_text(raw_html: str) -> str:
    text = re.sub(r"<sup[^>]*>.*?</sup>", "", raw_html or "", flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _normalized_gpu_candidate(raw: str) -> str:
    candidate = normalize_gpu_name(raw or "")
    if not candidate:
        return ""
    if re.fullmatch(r"\d+(?:\.\d+)?", candidate):
        return ""
    return candidate


def _extract_row_gpu(row: List[str], preferred_index: Optional[int] = None) -> tuple[str, str]:
    indices = []
    if preferred_index is not None and 0 <= preferred_index < len(row):
        indices.append(preferred_index)
    indices.extend(i for i in range(len(row)) if i not in indices)

    for idx in indices:
        raw = row[idx].strip()
        normalized = _normalized_gpu_candidate(raw)
        if normalized:
            return raw, normalized
    return "", ""


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
        "L40": {"mem": 48, "variant": ""},
        "L40S": {"mem": 48, "variant": ""},
        "RTX A6000": {"mem": 48, "variant": ""},
        "RTX A5000": {"mem": 24, "variant": ""},
        "RTX A4000": {"mem": 16, "variant": ""},
    }

    def parse_page(self, html: str) -> List[Dict[str, Any]]:
        rows = self._parse_pricing_tables(html)
        if rows:
            return rows

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

            gpu_count = 1
            rows.append(self.make_gpu_row(
                provider="coreweave", instance_type=gpu_raw,
                gpu_name=normalize_gpu_name(gn),
                gpu_variant=specs_variant,
                gpu_memory_gb=specs.get("mem", 0),
                gpu_count=gpu_count, pricing_type="on_demand",
                price_per_hour=price,
                available=True,
            ))

        return rows

    def _parse_pricing_tables(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        seen = set()
        for match in re.finditer(r'<h3[^>]*class="table-model-name">NVIDIA\s+([^<]+)</h3>', html, re.I):
            gpu_raw = re.sub(r"\s+", " ", match.group(1)).strip()
            window = html[match.end():match.end() + 1400]
            cells = [
                _table_cell_text(cell)
                for cell in re.findall(r'<div class="table-v2-cell(?: [^"]*)?">\s*<div>(.*?)</div>', window, re.S)
            ]
            if len(cells) < 6:
                continue
            if "$" in cells[0] or cells[0].strip().upper() in {"", "N/A"}:
                continue

            gpu_count = _parse_gpu_count(cells[0])
            if not gpu_count:
                continue

            price = 0.0
            for cell in cells:
                if "$" not in cell:
                    continue
                try:
                    price = float(cell.replace("$", "").replace(",", "").strip())
                except ValueError:
                    continue
                break
            if price <= 0:
                continue

            gpu_m = re.search(r'(GB200|B200|H200|H100|A100|A40|L40S?|RTX\s*A?\d+)', gpu_raw, re.I)
            if not gpu_m:
                continue
            gn = gpu_m.group(1)
            variant = ""
            ctx = gpu_raw.lower()
            if "sxm" in ctx:
                sxm_match = re.search(r'sxm(\d)', ctx)
                variant = "SXM" + (sxm_match.group(1) if sxm_match else "")
            elif "pcie" in ctx:
                variant = "PCIe"
            elif "nvl" in ctx:
                variant = "NVL"
            elif "hgx" in ctx:
                variant = "HGX"

            key = (gn.upper(), price, gpu_count)
            if key in seen:
                continue
            seen.add(key)

            specs = self.KNOWN_SPECS.get(gn.upper(), {})
            memory_match = re.search(r"(\d+(?:\.\d+)?)", cells[1])
            gpu_memory_gb = (
                int(float(memory_match.group(1))) if memory_match else specs.get("mem", 0)
            )
            rows.append(self.make_gpu_row(
                provider="coreweave",
                instance_type=gpu_raw,
                gpu_name=normalize_gpu_name(gn),
                gpu_variant=variant or specs.get("variant", ""),
                gpu_memory_gb=gpu_memory_gb,
                gpu_count=gpu_count,
                pricing_type="on_demand",
                price_per_hour=price,
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
        for row in self._parse_text_pricing(html):
            rows.append(row)
        if rows:
            return rows

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

    def _parse_text_pricing(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        lines = self.extract_text_lines(html)
        section = ""
        seen = set()
        for i, line in enumerate(lines):
            if line == "On-Demand GPU":
                section = "on_demand"
                continue
            if line == "Reservation":
                section = "reserved"
                continue
            if line == "Spot VM":
                section = "spot"
                continue
            if not section or not line.startswith("NVIDIA "):
                continue

            price = None
            currency = "USD"
            price_idx = None
            for j in range(i + 1, min(i + 7, len(lines))):
                parsed_price, parsed_currency = _parse_price(lines[j])
                if parsed_price:
                    price = parsed_price
                    currency = parsed_currency
                    price_idx = j
                    break
            if not price:
                continue

            gpu_raw = line.replace("NVIDIA", "", 1).strip()
            memory = _parse_memory_gb(lines[i + 1]) if i + 1 < len(lines) else 0
            key = (section, gpu_raw, price, price_idx)
            if key in seen:
                continue
            seen.add(key)
            rows.append(self.make_row(
                provider="hyperstack",
                instance_type=gpu_raw,
                gpu_name=normalize_gpu_name(gpu_raw),
                gpu_memory_gb=memory,
                gpu_count=1,
                pricing_type=section,
                price_per_hour=price,
                price_per_gpu_hour=price,
                currency=currency,
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
        for row in self._parse_text_cards(html):
            rows.append(row)
        if rows:
            return rows

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

    def _parse_text_cards(self, html: str) -> List[Dict[str, Any]]:
        lines = self.extract_text_lines(html)
        rows = []
        gpu_names = {"L40S", "A100", "H100", "H200", "GB200"}
        in_section = False
        for i, line in enumerate(lines):
            if "GPUs for" in line and "AI" in line and "workload" in line:
                in_section = True
                continue
            if in_section and line.startswith("Why choose"):
                break
            if not in_section or line not in gpu_names:
                continue
            price = None
            currency = ""
            for j in range(i + 1, min(i + 6, len(lines))):
                parsed_price, parsed_currency = _parse_price(lines[j])
                if parsed_price:
                    price = parsed_price
                    currency = parsed_currency
                    break
            if not price:
                continue
            rows.append(self.make_row(
                provider="gcore",
                instance_type=line,
                gpu_name=normalize_gpu_name(line),
                gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=price,
                price_per_gpu_hour=price,
                currency=currency or "USD",
                available=True,
                raw_extra=json.dumps({"raw_price_label": "from"}, separators=(",", ":")),
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
                    gpu_raw, gpu_name = _extract_row_gpu(row, gpu_col)
                    if not gpu_name:
                        continue
                    pm = re.search(r"\$?([\d.]+)", row[price_col])
                    if pm:
                        price = float(pm.group(1))
                        if price > 0:
                            rows.append(self.make_row(
                                provider="lightningai", instance_type=gpu_raw,
                                gpu_name=gpu_name, gpu_count=1,
                                pricing_type="on_demand",
                                price_per_hour=price, price_per_gpu_hour=price,
                                available=True,
                            ))
        if not rows:
            for pair in self.extract_gpu_price_pairs(html):
                gpu_name = _normalized_gpu_candidate(pair["gpu"])
                if not gpu_name:
                    continue
                rows.append(self.make_row(
                    provider="lightningai", instance_type=pair["gpu"],
                    gpu_name=gpu_name, gpu_count=1,
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
        for row in self._parse_text_calculator(html):
            rows.append(row)
        if rows:
            return rows

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

    def _parse_text_calculator(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        lines = self.extract_text_lines(html)
        in_calculator = False
        for i, line in enumerate(lines):
            if line == "SaladCloud Pricing Calculator":
                in_calculator = True
                continue
            if in_calculator and line == "Select your priority level":
                break
            if not in_calculator or "Series Cards" in line:
                continue
            gpu_match = re.search(r"\b((?:RTX|GTX)\s+[A-Z0-9 ]+?)\s*\((\d+)\s*GB\)", line, re.I)
            if not gpu_match or i + 1 >= len(lines):
                continue
            price, currency = _parse_price(lines[i + 1])
            if not price:
                continue
            gpu_raw = gpu_match.group(1).strip()
            rows.append(self.make_row(
                provider="salad",
                instance_type=gpu_raw,
                gpu_name=normalize_gpu_name(gpu_raw),
                gpu_memory_gb=int(gpu_match.group(2)),
                gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=price,
                price_per_gpu_hour=price,
                currency=currency or "USD",
                available=True,
                raw_extra=json.dumps({"priority": "batch"}, separators=(",", ":")),
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
        for row in self._parse_text_gpu_vms(html):
            rows.append(row)
        if rows:
            return rows

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

    def _parse_text_gpu_vms(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        lines = self.extract_text_lines(html)
        in_section = False
        for i, line in enumerate(lines):
            if line == "GPU Virtual Machines":
                in_section = True
                continue
            if in_section and line == "Bare Metal Servers":
                break
            if not in_section:
                continue
            gpu_match = re.search(r"NVIDIA\s+(.+?)\s*\((\d+)GB\)\s*-\s*(\d+)\s*GPUs?", line, re.I)
            if not gpu_match or i + 1 >= len(lines):
                continue
            price, currency = _parse_price(lines[i + 1])
            if not price:
                continue
            cells = [cell.strip() for cell in lines[i + 1].split("\t") if cell.strip()]
            gpu_count = int(gpu_match.group(3))
            rows.append(self.make_gpu_row(
                provider="qubrid",
                instance_type=line,
                gpu_name=normalize_gpu_name(gpu_match.group(1)),
                gpu_memory_gb=int(gpu_match.group(2)),
                gpu_count=gpu_count,
                vcpus=cells[0] if len(cells) > 0 else "",
                ram_gb=_parse_memory_gb(cells[1]) if len(cells) > 1 else "",
                storage_desc=cells[2] if len(cells) > 2 else "",
                pricing_type="on_demand",
                price_per_hour=price,
                currency=currency or "USD",
                available=True,
            ))
        return rows
