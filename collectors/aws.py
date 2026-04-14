"""
AWS EC2 GPU pricing collector.

Uses the public Bulk Price List API (no authentication required).
Fetches current pricing for all GPU/accelerator instance families across all regions.
"""

import csv
import io
import json
import logging
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name, infer_geo_group

logger = logging.getLogger(__name__)

BASE_URL = "https://pricing.us-east-1.amazonaws.com"

GPU_FAMILIES = [
    "g3.", "g3s.", "g4dn.", "g4ad.", "g5.", "g5g.", "g6.", "g6e.", "g6i.",
    "p2.", "p3.", "p3dn.", "p4d.", "p4de.", "p5.", "p5e.", "p5en.",
    "p6-b200.", "p6-b300.", "p6e-gb200.",
    "trn1.", "trn1n.", "trn2.", "trn2u.",
    "inf1.", "inf2.", "dl1.", "dl2q.",
]

GPU_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-central-2",
    "eu-north-1", "eu-south-1",
    "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
    "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
    "ap-south-1", "ap-south-2", "ap-east-1",
    "ca-central-1", "ca-west-1",
    "sa-east-1",
    "me-south-1", "me-central-1",
    "af-south-1", "il-central-1",
]

INSTANCE_GPU_MAP = {
    "g3":     {"gpu": "Tesla M60",       "mem": 8,   "variant": ""},
    "g3s":    {"gpu": "Tesla M60",       "mem": 8,   "variant": ""},
    "g4dn":   {"gpu": "T4",             "mem": 16,  "variant": ""},
    "g4ad":   {"gpu": "Radeon Pro V520", "mem": 8,   "variant": ""},
    "g5":     {"gpu": "A10G",            "mem": 24,  "variant": ""},
    "g5g":    {"gpu": "T4G",             "mem": 16,  "variant": ""},
    "g6":     {"gpu": "L4",              "mem": 24,  "variant": ""},
    "g6e":    {"gpu": "L40S",            "mem": 48,  "variant": ""},
    "g6i":    {"gpu": "L40S",            "mem": 48,  "variant": ""},
    "p2":     {"gpu": "K80",             "mem": 12,  "variant": ""},
    "p3":     {"gpu": "V100",            "mem": 16,  "variant": "SXM2"},
    "p3dn":   {"gpu": "V100",            "mem": 32,  "variant": "SXM2"},
    "p4d":    {"gpu": "A100",            "mem": 40,  "variant": "SXM4"},
    "p4de":   {"gpu": "A100",            "mem": 80,  "variant": "SXM4"},
    "p5":     {"gpu": "H100",            "mem": 80,  "variant": "SXM5"},
    "p5e":    {"gpu": "H200",            "mem": 141, "variant": "SXM"},
    "p5en":   {"gpu": "H200",            "mem": 141, "variant": "SXM"},
    "p6-b200":  {"gpu": "B200",          "mem": 192, "variant": ""},
    "p6-b300":  {"gpu": "B300",          "mem": 288, "variant": ""},
    "p6e-gb200": {"gpu": "GB200",        "mem": 192, "variant": ""},
    "trn1":   {"gpu": "Trainium",        "mem": 32,  "variant": ""},
    "trn1n":  {"gpu": "Trainium",        "mem": 32,  "variant": ""},
    "trn2":   {"gpu": "Trainium2",       "mem": 96,  "variant": ""},
    "trn2u":  {"gpu": "Trainium2",       "mem": 96,  "variant": ""},
    "inf1":   {"gpu": "Inferentia",      "mem": 8,   "variant": ""},
    "inf2":   {"gpu": "Inferentia2",     "mem": 32,  "variant": ""},
    "dl1":    {"gpu": "Gaudi",           "mem": 32,  "variant": ""},
    "dl2q":   {"gpu": "Qualcomm AI100",  "mem": 16,  "variant": ""},
}

KNOWN_GPU_COUNTS = {
    "p4d.24xlarge": 8, "p4de.24xlarge": 8,
    "p5.48xlarge": 8, "p5e.48xlarge": 8, "p5en.48xlarge": 8,
    "p3.2xlarge": 1, "p3.8xlarge": 4, "p3.16xlarge": 8, "p3dn.24xlarge": 8,
    "p2.xlarge": 1, "p2.8xlarge": 8, "p2.16xlarge": 16,
    "g4dn.xlarge": 1, "g4dn.2xlarge": 1, "g4dn.4xlarge": 1,
    "g4dn.8xlarge": 1, "g4dn.12xlarge": 4, "g4dn.16xlarge": 1, "g4dn.metal": 8,
    "g5.xlarge": 1, "g5.2xlarge": 1, "g5.4xlarge": 1, "g5.8xlarge": 1,
    "g5.12xlarge": 4, "g5.16xlarge": 1, "g5.24xlarge": 4, "g5.48xlarge": 8,
    "g6.xlarge": 1, "g6.2xlarge": 1, "g6.4xlarge": 1, "g6.8xlarge": 1,
    "g6.12xlarge": 4, "g6.16xlarge": 1, "g6.24xlarge": 4, "g6.48xlarge": 8,
    "g6e.xlarge": 1, "g6e.2xlarge": 1, "g6e.4xlarge": 1, "g6e.8xlarge": 1,
    "g6e.12xlarge": 4, "g6e.16xlarge": 1, "g6e.24xlarge": 4, "g6e.48xlarge": 8,
    "trn1.2xlarge": 1, "trn1.32xlarge": 16, "trn1n.32xlarge": 16,
    "trn2.48xlarge": 16, "trn2u.48xlarge": 16,
    "inf1.xlarge": 1, "inf1.2xlarge": 1, "inf1.6xlarge": 4, "inf1.24xlarge": 16,
    "inf2.xlarge": 1, "inf2.8xlarge": 1, "inf2.24xlarge": 6, "inf2.48xlarge": 12,
    "dl1.24xlarge": 8,
}

AWS_OFFER_KEY_FIELDS = [
    "Region Code",
    "Location",
    "Instance Type",
    "TermType",
    "Operating System",
    "Tenancy",
    "Pre Installed S/W",
    "LeaseContractLength",
    "PurchaseOption",
    "OfferingClass",
    "CapacityStatus",
]


def _get_gpu_family(instance_type: str) -> str:
    for fam in sorted(INSTANCE_GPU_MAP.keys(), key=len, reverse=True):
        if instance_type.startswith(fam + ".") or instance_type.startswith(fam):
            return fam
    return ""


def _fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "OpenComputePrices/1.0",
    })
    with urllib.request.urlopen(req, timeout=150) as resp:
        return json.loads(resp.read().decode())


def _classify_price_description(price_description: str, capacity_status: str) -> str:
    desc = (price_description or "").lower()
    status = (capacity_status or "").lower()
    if "capacity block" in desc:
        return "capacity_block"
    if "unused reservation" in desc or status == "unusedcapacityreservation":
        return "capacity_reservation"
    if "reservation" in desc or status == "allocatedcapacityreservation":
        return "capacity_reservation"
    if "dedicated" in desc:
        return "dedicated"
    if "on demand" in desc:
        return "on_demand"
    return "other"


class AWSCollector(BaseCollector):
    name = "aws"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[aws] Fetching current EC2 GPU pricing (no auth)")

        # Get available regions for current version
        try:
            region_data = _fetch_json(
                f"{BASE_URL}/offers/v1.0/aws/AmazonEC2/current/region_index.json"
            )
            available = set(region_data.get("regions", {}).keys())
        except Exception as e:
            logger.error(f"[aws] Failed to get region index: {e}")
            available = set(GPU_REGIONS)

        target_regions = [r for r in GPU_REGIONS if r in available]
        logger.info(f"[aws] Targeting {len(target_regions)} regions")

        all_rows = []
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(self._fetch_region, r): r for r in target_regions
            }
            for fut in as_completed(futures):
                region = futures[fut]
                try:
                    rows = fut.result()
                    all_rows.extend(rows)
                    logger.info(f"[aws] {region}: {len(rows)} rows")
                except Exception as e:
                    logger.warning(f"[aws] {region} failed: {e}")

        logger.info(f"[aws] Total: {len(all_rows)} rows")
        return all_rows

    def _fetch_region(self, region: str) -> List[Dict[str, Any]]:
        url = f"{BASE_URL}/offers/v1.0/aws/AmazonEC2/current/{region}/index.csv"
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "text/csv",
                "Accept-Encoding": "gzip",
                "User-Agent": "OpenComputePrices/1.0",
            })
            with urllib.request.urlopen(req, timeout=360) as resp:
                text = resp.read().decode("utf-8", errors="replace")
        except Exception:
            return []

        lines = text.splitlines()
        if len(lines) < 7:
            return []

        header_line = lines[5]
        data_lines = []
        for line in lines[6:]:
            if any(family in line for family in GPU_FAMILIES):
                data_lines.append(line)

        if not data_lines:
            return []

        reader = csv.reader(io.StringIO(header_line))
        try:
            headers = next(reader)
        except StopIteration:
            return []

        col = {}
        for i, h in enumerate(headers):
            col[h.strip()] = i

        offers = {}
        for line in data_lines:
            try:
                fields = next(csv.reader(io.StringIO(line)))
            except Exception:
                continue

            def _f(name):
                idx = col.get(name, -1)
                return fields[idx] if 0 <= idx < len(fields) else ""

            instance_type = _f("Instance Type")
            if not instance_type:
                continue

            fam = _get_gpu_family(instance_type)
            if not fam:
                continue

            term_type = _f("TermType")
            unit = _f("Unit")
            product_family = _f("Product Family")

            if "Compute Instance" not in product_family:
                continue
            if unit not in ("Hrs", "Quantity"):
                continue

            try:
                component_price = float(_f("PricePerUnit"))
            except (ValueError, TypeError):
                continue
            if component_price < 0:
                continue

            capacity_status = _f("CapacityStatus")
            billing_model = _classify_price_description(_f("PriceDescription"), capacity_status)
            offer_key = tuple(_f(name) for name in AWS_OFFER_KEY_FIELDS) + (billing_model,)
            offer = offers.setdefault(offer_key, {
                "instance_type": instance_type,
                "family": fam,
                "term_type": term_type,
                "region": _f("Region Code") or region,
                "location": _f("Location"),
                "capacity_status": capacity_status,
                "billing_model": billing_model,
                "effective_date": _f("EffectiveDate"),
                "os": _f("Operating System"),
                "tenancy": _f("Tenancy"),
                "pre_installed_sw": _f("Pre Installed S/W"),
                "lease_length": _f("LeaseContractLength"),
                "purchase_option": _f("PurchaseOption"),
                "offering_class": _f("OfferingClass"),
                "vcpus": _f("vCPU"),
                "memory": _f("Memory"),
                "storage": _f("Storage") if "Storage" in col else "",
                "network": _f("Network Performance") if "Network Performance" in col else "",
                "gpu": _f("GPU"),
                "hourly_price": 0.0,
                "upfront_price": 0.0,
            })

            if unit == "Hrs":
                offer["hourly_price"] += component_price
            else:
                offer["upfront_price"] += component_price

        rows = []
        for offer in offers.values():
            price = offer["hourly_price"]
            upfront_price = offer["upfront_price"]
            if price <= 0 and upfront_price <= 0:
                continue

            gpu_info = INSTANCE_GPU_MAP.get(offer["family"], {})
            gpu_name = gpu_info.get("gpu", "Unknown")

            try:
                gpu_count = int(offer["gpu"])
            except (ValueError, TypeError):
                gpu_count = KNOWN_GPU_COUNTS.get(offer["instance_type"], 0)

            price_per_gpu = price / gpu_count if gpu_count > 0 else price
            upfront_price_per_gpu = upfront_price / gpu_count if gpu_count > 0 else upfront_price

            ram_gb = ""
            if offer["memory"]:
                try:
                    ram_gb = float(offer["memory"].replace(" GiB", "").replace(",", "").strip())
                except (ValueError, TypeError):
                    ram_gb = ""

            billing_model = offer.get("billing_model", "")
            pricing_type = "on_demand"
            commitment_period = offer["lease_length"]
            purchase_option = offer["purchase_option"]
            if offer["term_type"] == "Reserved" or billing_model in {"capacity_block", "capacity_reservation"}:
                pricing_type = "reserved"
                if not commitment_period:
                    commitment_period = "capacity_block" if billing_model == "capacity_block" else "capacity_reservation"
                if not purchase_option and billing_model in {"capacity_block", "capacity_reservation"}:
                    purchase_option = billing_model

            rows.append(self.make_row(
                provider="aws",
                instance_type=offer["instance_type"],
                instance_family=offer["family"],
                gpu_name=normalize_gpu_name(gpu_name),
                gpu_variant=gpu_info.get("variant", ""),
                gpu_memory_gb=gpu_info.get("mem", ""),
                gpu_count=gpu_count,
                vcpus=offer["vcpus"],
                ram_gb=ram_gb,
                storage_desc=offer["storage"],
                network_desc=offer["network"],
                region=offer["region"],
                geo_group=infer_geo_group(offer["region"]),
                pricing_type=pricing_type,
                commitment_period=commitment_period,
                price_per_hour=price,
                price_per_gpu_hour=round(price_per_gpu, 6),
                upfront_price=upfront_price,
                upfront_price_per_gpu=round(upfront_price_per_gpu, 6),
                os=offer["os"],
                tenancy=offer["tenancy"],
                pre_installed_sw=offer["pre_installed_sw"],
                available=True,
                raw_extra=json.dumps({
                    "capacity_status": offer["capacity_status"],
                    "billing_model": billing_model,
                    "effective_date": offer["effective_date"],
                    "location": offer["location"],
                    "purchase_option": purchase_option,
                    "offering_class": offer["offering_class"],
                }, separators=(",", ":")),
            ))

        return rows
