"""
Thunder Compute GPU pricing scraper.

Scrapes the Thunder Compute pricing page which embeds JSON-LD structured
data with Product offers containing GPU name, price, and currency.
Also falls back to HTML pattern extraction.
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

URL = "https://www.thundercompute.com/pricing"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class ThunderComputeCollector(BaseCollector):
    name = "thundercompute"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[thundercompute] Scraping pricing page")

        try:
            req = urllib.request.Request(URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[thundercompute] Failed to fetch: {e}")
            return []

        rows = []

        # Method 1: Extract JSON-LD structured data
        ld_blocks = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S
        )
        for ld in ld_blocks:
            try:
                data = json.loads(ld)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    row = self._parse_jsonld(item)
                    if row:
                        rows.append(row)
            except (json.JSONDecodeError, ValueError):
                continue

        # Method 2: Regex extraction of "price": "X.XX" patterns from inline scripts
        price_blocks = re.findall(
            r'"name"\s*:\s*"([^"]*(?:GPU|A100|H100|RTX|A6000|V100|T4|L4|L40)[^"]*)"'
            r'[^}]*"price"\s*:\s*"([\d.]+)"'
            r'[^}]*"priceCurrency"\s*:\s*"(\w+)"',
            html, re.S | re.I
        )
        for name, price_str, currency in price_blocks:
            try:
                price = float(price_str)
            except ValueError:
                continue
            if price <= 0:
                continue

            # Extract GPU name and VRAM from the product name
            gpu_name, vram, variant = self._parse_product_name(name)
            if not gpu_name:
                continue

            # Avoid duplicates if already found via JSON-LD
            if any(r.get("instance_type") == name for r in rows):
                continue

            rows.append(self.make_row(
                provider="thundercompute",
                instance_type=name,
                gpu_name=normalize_gpu_name(gpu_name),
                gpu_variant=variant,
                gpu_memory_gb=vram,
                gpu_count=1,
                pricing_type="on_demand",
                price_per_hour=price,
                price_per_gpu_hour=price,
                currency=currency,
                available=True,
            ))

        # Method 3: HTML meta description fallback
        if not rows:
            meta = re.findall(
                r'((?:H100|A100|H200|A6000|RTX|V100|T4|L4|L40)[^"<]{0,100}\$[\d.]+)',
                html, re.I
            )
            for m in meta:
                clean = re.sub(r"<[^>]+>", " ", m).strip()
                gpu_match = re.search(r"(H100|A100|H200|A6000|V100|T4|L4|L40S?|RTX\s*\w+)", clean, re.I)
                price_match = re.search(r"\$([\d.]+)", clean)
                if gpu_match and price_match:
                    price = float(price_match.group(1))
                    if price > 0:
                        rows.append(self.make_row(
                            provider="thundercompute",
                            instance_type=clean[:80],
                            gpu_name=normalize_gpu_name(gpu_match.group(1)),
                            gpu_count=1,
                            pricing_type="on_demand",
                            price_per_hour=price,
                            price_per_gpu_hour=price,
                            available=True,
                        ))

        logger.info(f"[thundercompute] Total: {len(rows)} rows")
        return rows

    def _parse_jsonld(self, item: dict) -> Dict[str, Any]:
        """Parse a JSON-LD Product item."""
        if not isinstance(item, dict):
            return None
        if item.get("@type") != "Product":
            return None

        name = item.get("name", "")
        offers = item.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}

        price_str = offers.get("price", "")
        currency = offers.get("priceCurrency", "USD")
        desc = item.get("description", "")

        try:
            price = float(price_str)
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        gpu_name, vram, variant = self._parse_product_name(name)
        if not gpu_name:
            return None

        return self.make_row(
            provider="thundercompute",
            instance_type=name,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_variant=variant,
            gpu_memory_gb=vram,
            gpu_count=1,
            pricing_type="on_demand",
            price_per_hour=price,
            price_per_gpu_hour=price,
            currency=currency,
            available=True,
            raw_extra=json.dumps({
                "description": desc[:200],
            }, separators=(",", ":")),
        )

    @staticmethod
    def _parse_product_name(name: str) -> tuple:
        """Extract GPU name, VRAM, variant from product name like 'RTX A6000 On-demand'."""
        nl = name.lower()
        gpu_name = ""
        vram = 0
        variant = ""

        if "h200" in nl:
            gpu_name = "H200"
            vram = 141
        elif "h100" in nl:
            gpu_name = "H100"
            vram = 80
        elif "a100" in nl:
            gpu_name = "A100"
            vram_m = re.search(r"(\d+)\s*gb", nl)
            vram = int(vram_m.group(1)) if vram_m else 80
        elif "a6000" in nl:
            gpu_name = "RTX A6000"
            vram = 48
        elif "a5000" in nl:
            gpu_name = "RTX A5000"
            vram = 24
        elif "a4000" in nl:
            gpu_name = "RTX A4000"
            vram = 16
        elif "rtx 4090" in nl or "rtx4090" in nl:
            gpu_name = "RTX 4090"
            vram = 24
        elif "v100" in nl:
            gpu_name = "V100"
            vram = 16
        elif "t4" in nl:
            gpu_name = "T4"
            vram = 16
        elif "l4" in nl and "l40" not in nl:
            gpu_name = "L4"
            vram = 24
        elif "l40s" in nl:
            gpu_name = "L40S"
            vram = 48
        elif "l40" in nl:
            gpu_name = "L40"
            vram = 48

        if "sxm" in nl:
            variant = "SXM"
        elif "pcie" in nl:
            variant = "PCIe"

        return gpu_name, vram, variant
