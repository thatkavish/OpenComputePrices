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
from html import unescape
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

        if not all_rows:
            all_rows.extend(self._parse_text_pricing(html))

        logger.info(f"[massedcompute] Total: {len(all_rows)} rows")
        return all_rows

    def _parse_text_pricing(self, html: str) -> List[Dict[str, Any]]:
        rows = []
        lines = _html_to_lines(html)
        in_section = False
        current_gpu = ""
        current_mem = 0
        seen = set()

        idx = 0
        while idx < len(lines):
            line = lines[idx]
            if line.startswith("GPU Type"):
                in_section = True
                idx += 1
                continue
            if in_section and line == "Bare Metal":
                break
            if not in_section:
                idx += 1
                continue

            if _is_gpu_header(line):
                current_gpu = line
                current_mem = 0
                idx += 1
                continue

            if current_gpu and not current_mem:
                mem_match = re.fullmatch(r"(\d+)\s*GB", line, re.I)
                if mem_match:
                    current_mem = int(mem_match.group(1))
                    idx += 1
                    continue

            if not current_gpu:
                idx += 1
                continue
            count_match = re.match(r"x\s*(\d+)", line, re.I)
            if not count_match:
                idx += 1
                continue

            cells = []
            price = 0.0
            lookahead = idx + 1
            while lookahead < len(lines):
                next_line = lines[lookahead]
                if next_line == "Bare Metal" or _is_gpu_header(next_line) or re.match(r"x\s*\d+", next_line, re.I):
                    break
                price_match = re.search(r"\$([\d.]+)\s*/?\s*hr", next_line, re.I)
                if price_match:
                    price = float(price_match.group(1))
                    break
                if next_line.lower() == "request":
                    break
                cells.append(next_line)
                lookahead += 1

            if price <= 0:
                idx += 1
                continue

            gpu_count = int(count_match.group(1))
            vcpus = cells[1] if len(cells) > 1 else ""
            ram = cells[2] if len(cells) > 2 else ""
            storage = cells[3] if len(cells) > 3 else ""
            key = (current_gpu, gpu_count, price)
            if key in seen:
                idx = max(lookahead, idx + 1)
                continue
            seen.add(key)

            rows.append(self.make_row(
                provider="massedcompute",
                instance_type=f"{current_gpu} x{gpu_count}",
                gpu_name=normalize_gpu_name(current_gpu),
                gpu_memory_gb=current_mem,
                gpu_count=gpu_count,
                vcpus=vcpus,
                ram_gb=_parse_gb(ram),
                storage_desc=storage,
                pricing_type="on_demand",
                price_per_hour=price,
                price_per_gpu_hour=round(price / gpu_count, 6),
                available=True,
            ))
            idx = max(lookahead, idx + 1)

        return rows


def _html_to_lines(html: str) -> list[str]:
    text = re.sub(r"<(script|style|noscript)[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</(?:p|div|li|tr|td|th|h[1-6]|span)>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    lines = []
    for line in unescape(text).splitlines():
        line = line.replace("\xa0", " ")
        line = re.sub(r"[ \r\f\v]+", " ", line).strip()
        if line:
            lines.append(line)
    return lines


def _is_gpu_header(line: str) -> bool:
    return bool(re.fullmatch(
        r"(?:DGX\s+)?(?:H200|H100|A100|A40|A30|L40S?|RTX\s+(?:A?6000|A?5000)(?:\s+\[NVLink\])?(?:\s+ADA)?)(?:\s+(?:SXM\d?|NVL|PCIe))?",
        line,
        re.I,
    ))


def _parse_gb(value: str):
    match = re.search(r"(\d+(?:\.\d+)?)\s*GB", value, re.I)
    if not match:
        return ""
    parsed = float(match.group(1))
    return int(parsed) if parsed.is_integer() else parsed
