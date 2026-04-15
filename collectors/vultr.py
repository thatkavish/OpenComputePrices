"""
Vultr GPU pricing scraper.

Scrapes the Vultr pricing page. The page is heavily JS-rendered but
the GPU Cloud section may contain some extractable data.
No authentication required.
"""

import json
import logging
import re
import urllib.request
from typing import List, Dict, Any, Union

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

PRICING_URL = "https://www.vultr.com/pricing/"
# Vultr also has API docs - check for public pricing endpoint
API_URL = "https://api.vultr.com/v2/plans"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
FULL_GPU_MEMORY_GB = {
    "A100": 80,
    "A16": 16,
    "A40": 48,
    "B200": 192,
    "GB200": 192,
    "H100": 80,
    "H200": 141,
    "L40S": 48,
}


def infer_effective_gpu_count(gpu_name: str, total_gpu_memory_gb) -> Union[float, int]:
    normalized = normalize_gpu_name(gpu_name)
    full_gpu_memory_gb = FULL_GPU_MEMORY_GB.get(normalized)
    if not full_gpu_memory_gb:
        return 1
    try:
        total_gpu_memory_gb = float(total_gpu_memory_gb or 0)
    except (TypeError, ValueError):
        return 1
    if total_gpu_memory_gb <= 0:
        return 1
    effective_count = round(total_gpu_memory_gb / full_gpu_memory_gb, 6)
    if effective_count <= 0:
        return 1
    if effective_count.is_integer():
        return int(effective_count)
    return effective_count


class VultrCollector(BaseCollector):
    name = "vultr"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[vultr] Fetching GPU pricing")

        rows = []

        # Try the public API first (plans endpoint may not need auth)
        rows.extend(self._try_api())

        # Scrape the pricing page as fallback
        if not rows:
            rows.extend(self._scrape_page())

        logger.info(f"[vultr] Total: {len(rows)} rows")
        return rows

    def _try_api(self) -> List[Dict[str, Any]]:
        """Try Vultr public plans API."""
        rows = []
        for plan_type in ["vcg"]:  # vcg = cloud GPU
            try:
                url = f"{API_URL}?type={plan_type}&per_page=500"
                req = urllib.request.Request(url, headers={
                    "User-Agent": UA,
                    "Accept": "application/json",
                })
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())

                plans = data.get("plans", [])
                for plan in plans:
                    plan_rows = self._parse_plan(plan)
                    rows.extend(plan_rows)

                if rows:
                    logger.info(f"[vultr] API returned {len(rows)} GPU plans")
            except Exception as e:
                logger.debug(f"[vultr] API {plan_type} failed: {e}")

        return rows

    def _parse_plan(self, plan: dict) -> List[Dict[str, Any]]:
        """Parse a Vultr plan object. Returns list (on-demand + preemptible)."""
        plan_id = plan.get("id", "")
        gpu_type = plan.get("gpu_type", "") or plan.get("gpu_name", "")
        gpu_vram_gb = plan.get("gpu_vram_gb", 0)
        monthly_cost = plan.get("monthly_cost", 0)
        hourly_cost = plan.get("hourly_cost", 0)
        monthly_preempt = plan.get("monthly_cost_preemptible", 0)
        hourly_preempt = plan.get("hourly_cost_preemptible", 0)
        vcpus = plan.get("vcpu_count", "")
        ram = plan.get("ram", 0)
        disk = plan.get("disk", 0)
        locations = plan.get("locations", [])

        if not gpu_type:
            pid = plan_id.lower()
            if "gpu" not in pid and "vcg" not in pid:
                return []
            gpu_type = plan_id

        # Strip NVIDIA_ prefix for normalization
        gpu_clean = gpu_type.replace("NVIDIA_", "").replace("nvidia_", "")
        gpu_name = normalize_gpu_name(gpu_clean)

        # gpu_vram_gb on Vultr is the total VRAM attached to the slice or VM.
        # Normalize slices and multi-GPU plans to an effective full-GPU count.
        gpu_count = infer_effective_gpu_count(gpu_name, gpu_vram_gb)

        ram_gb = round(ram / 1024, 1) if ram > 1024 else ram

        rows = []

        # On-demand pricing
        if hourly_cost:
            try:
                price = float(hourly_cost)
            except (ValueError, TypeError):
                price = 0
        elif monthly_cost:
            try:
                price = float(monthly_cost) / 730
            except (ValueError, TypeError):
                price = 0
        else:
            price = 0

        if price > 0:
            price_per_gpu = round(price / gpu_count, 6) if gpu_count else round(price, 6)
            rows.append(self.make_row(
                provider="vultr",
                instance_type=plan_id,
                gpu_name=gpu_name,
                gpu_memory_gb=gpu_vram_gb,
                gpu_count=gpu_count,
                vcpus=vcpus,
                ram_gb=ram_gb,
                storage_desc=f"{disk} GB" if disk else "",
                pricing_type="on_demand",
                price_per_hour=round(price, 6),
                price_per_gpu_hour=price_per_gpu,
                available=len(locations) > 0,
                raw_extra=json.dumps({
                    "monthly_cost": monthly_cost,
                    "locations": locations[:10],
                    "bandwidth": plan.get("bandwidth", ""),
                    "deploy_ondemand": plan.get("deploy_ondemand", ""),
                }, separators=(",", ":")),
            ))

        # Preemptible pricing
        if hourly_preempt:
            try:
                preempt_price = float(hourly_preempt)
            except (ValueError, TypeError):
                preempt_price = 0
        elif monthly_preempt:
            try:
                preempt_price = float(monthly_preempt) / 730
            except (ValueError, TypeError):
                preempt_price = 0
        else:
            preempt_price = 0

        if preempt_price > 0:
            price_per_gpu = round(preempt_price / gpu_count, 6) if gpu_count else round(preempt_price, 6)
            rows.append(self.make_row(
                provider="vultr",
                instance_type=f"{plan_id}_preemptible",
                gpu_name=gpu_name,
                gpu_memory_gb=gpu_vram_gb,
                gpu_count=gpu_count,
                vcpus=vcpus,
                ram_gb=ram_gb,
                storage_desc=f"{disk} GB" if disk else "",
                pricing_type="spot",
                price_per_hour=round(preempt_price, 6),
                price_per_gpu_hour=price_per_gpu,
                available=len(locations) > 0 and plan.get("deploy_preemptible", False),
                raw_extra=json.dumps({
                    "monthly_cost_preemptible": monthly_preempt,
                    "locations": locations[:10],
                }, separators=(",", ":")),
            ))

        return rows

    def _scrape_page(self) -> List[Dict[str, Any]]:
        """Scrape the pricing page for GPU info."""
        try:
            req = urllib.request.Request(PRICING_URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"[vultr] Page scrape failed: {e}")
            return []

        rows = []
        seen = set()

        # Look for GPU + price patterns
        for match in re.finditer(
            r'((?:H100|H200|A100|A40|L40S?|B200|MI\d+\w*|NVIDIA[^<]{3,40})[^<]{0,500})',
            html, re.I
        ):
            block = match.group(1)
            clean = re.sub(r'<[^>]+>', ' ', block)
            clean = re.sub(r'\s+', ' ', clean).strip()
            gpu_m = re.search(r'(H100|H200|A100|A40|L40S?|B200|MI\d+\w*)', clean, re.I)
            price_m = re.search(r'\$([\d.]+)(?:\s*/\s*(?:hr|hour|mo))?', clean)
            if gpu_m and price_m:
                gpu = gpu_m.group(1)
                price = float(price_m.group(1))
                if price > 0 and (gpu.upper(), price) not in seen:
                    seen.add((gpu.upper(), price))
                    rows.append(self.make_row(
                        provider="vultr",
                        instance_type=gpu,
                        gpu_name=normalize_gpu_name(gpu),
                        gpu_count=1,
                        pricing_type="on_demand",
                        price_per_hour=price,
                        price_per_gpu_hour=price,
                        available=True,
                    ))

        return rows
