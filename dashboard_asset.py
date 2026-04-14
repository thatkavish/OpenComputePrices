#!/usr/bin/env python3
"""
Build the OpenSemi dashboard GPU daily asset from the canonical master CSV.

Usage:
    python dashboard_asset.py
    python dashboard_asset.py --input data/_master.csv --output data/dashboard_gpu_daily.json.gz
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import math
import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from statistics import median

from schema import (
    infer_geo_group,
    normalize_gpu_memory_gb,
    normalize_gpu_name,
    normalize_pricing_type,
    normalize_provider,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SCHEMA_VERSION = "dashboard_gpu_daily.v1"
DEFAULT_RELEASE_TAG = "latest-data"
DEFAULT_OUTPUT_FILENAME = "dashboard_gpu_daily.json.gz"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DEFAULT_INPUT_PATH = os.path.join(DATA_DIR, "_master.csv")
DEFAULT_OUTPUT_PATH = os.path.join(DATA_DIR, DEFAULT_OUTPUT_FILENAME)

QUALITY_SUMMARY_KEYS = [
    "excluded_reserved_or_commitment_rows",
    "excluded_non_gpu_rows",
    "excluded_fractional_or_slice_rows",
    "excluded_aggregate_offer_rows",
    "excluded_price_sanity_rows",
]

PROVIDER_LABELS = {
    "aethir": "Aethir",
    "akash": "Akash",
    "aws": "AWS",
    "azure": "Microsoft Azure",
    "cloreai": "Clore.ai",
    "coreweave": "CoreWeave",
    "crusoe": "Crusoe",
    "cudo": "CUDO Compute",
    "datacrunch": "DataCrunch",
    "deepinfra": "DeepInfra",
    "denvr": "Denvr Dataworks",
    "e2e": "E2E Networks",
    "gcore": "Gcore",
    "gcp": "Google Cloud",
    "getdeploying": "GetDeploying",
    "gmicloud": "GMI Cloud",
    "hyperstack": "Hyperstack",
    "jarvislabs": "JarvisLabs",
    "lambda": "Lambda",
    "latitude": "Latitude.sh",
    "lightningai": "Lightning AI",
    "linode": "Linode",
    "massedcompute": "Massed Compute",
    "novita": "Novita AI",
    "openrouter": "OpenRouter",
    "oracle": "Oracle Cloud",
    "paperspace": "Paperspace",
    "primeintellect": "Prime Intellect",
    "qubrid": "Qubrid",
    "runpod": "Runpod",
    "salad": "Salad",
    "shadeform": "Shadeform",
    "skypilot": "SkyPilot",
    "tensordock": "TensorDock",
    "thundercompute": "Thunder Compute",
    "together": "Together AI",
    "vastai": "Vast.ai",
    "voltagepark": "Voltage Park",
    "vultr": "Vultr",
}

REGION_BUCKETS = {
    "US East": ("us-east", "US East"),
    "US Central": ("us-central", "US Central"),
    "US West": ("us-west", "US West"),
    "Europe": ("europe", "Europe"),
    "APAC": ("apac", "APAC"),
    "Middle East": ("middle-east", "Middle East"),
    "LATAM": ("latin-america", "Latin America"),
    "Latin America": ("latin-america", "Latin America"),
    "Africa": ("africa", "Africa"),
    "Canada": ("canada", "Canada"),
    "US Other": ("us-other", "US Other"),
    "North America": ("unknown", "Unknown"),
    "Unknown": ("unknown", "Unknown"),
}

INTERCONNECT_ALIASES = [
    (re.compile(r"\bnvswitch\b", re.I), ("nvswitch", "NVSwitch")),
    (re.compile(r"\bnvlink\b|\bnvl\b", re.I), ("nvlink", "NVLink")),
    (re.compile(r"\binfiniband\b|\bib\b", re.I), ("infiniband", "InfiniBand")),
    (re.compile(r"\bpcie\b|\bpci[\s_-]?e\b", re.I), ("pcie", "PCIe")),
    (re.compile(r"\bsxm\d*\b", re.I), ("sxm", "SXM")),
]

NON_GPU_PATTERN = re.compile(
    r"\b(?:tpu|inferentia2?|trainium2?|gaudi(?:\s*[23])?|fpga|alveo|qualcomm\s*ai100|habana)\b",
    re.I,
)
GRAPHICS_GPU_PATTERN = re.compile(r"^(?:RTX|GTX)\b|^Radeon\b|^GeForce\b|^Quadro\b", re.I)
DATA_CENTER_GPU_PATTERN = re.compile(
    r"^(?:A10G|A10|A16|A30|A40|A100|B200|B300|GB200|GH200|H\d00|L4|L40S?|MI\d+\w*|"
    r"P4|P40|P100|T4G?|V100|K80|Tesla M60)\b",
    re.I,
)
ALLOWED_PRICE_UNITS = {"", "gpu-hour", "gpu_hour", "hour", "hr"}
SUMMARY_PRICE_TIERS = {"min", "max", "avg", "weightedaverage", "weighted_average"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return slug.strip("-")


def _parse_float(raw) -> float | None:
    if raw is None:
        return None
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _clean_number(value):
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _parse_raw_extra(raw_extra: str) -> dict:
    try:
        parsed = json.loads(raw_extra or "")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _provider_label(provider_slug: str) -> str:
    if provider_slug in PROVIDER_LABELS:
        return PROVIDER_LABELS[provider_slug]
    words = re.split(r"[_-]+", provider_slug)
    label_parts = []
    for word in words:
        if not word:
            continue
        if word in {"ai", "aws", "gcp"}:
            label_parts.append(word.upper())
        else:
            label_parts.append(word.capitalize())
    return " ".join(label_parts) or provider_slug


def _normalize_region_bucket(row: dict) -> tuple[str, str]:
    raw_label = str(row.get("geo_group", "")).strip()
    if not raw_label:
        raw_label = infer_geo_group(row.get("region", ""), row.get("country", ""))
    return REGION_BUCKETS.get(raw_label, ("unknown", "Unknown"))


def _normalize_interconnect(row: dict) -> tuple[str, str]:
    raw = str(row.get("gpu_interconnect", "") or row.get("gpu_variant", "")).strip()
    if not raw:
        return "unknown", "—"
    if _parse_float(raw) is not None:
        return "unknown", "—"
    for pattern, normalized in INTERCONNECT_ALIASES:
        if pattern.search(raw):
            return normalized
    return _slugify(raw) or "unknown", raw


def _gpu_category(gpu_label: str) -> str | None:
    if not gpu_label:
        return None
    if NON_GPU_PATTERN.search(gpu_label):
        return None
    if GRAPHICS_GPU_PATTERN.search(gpu_label):
        return "graphics_card"
    if DATA_CENTER_GPU_PATTERN.search(gpu_label):
        return "data_center"
    return None


def _canonicalize_gpu(raw_gpu_name: str) -> tuple[str, str] | tuple[None, None]:
    gpu_label = normalize_gpu_name(raw_gpu_name or "")
    probe = gpu_label or str(raw_gpu_name or "").strip()
    if not probe:
        return None, None
    if NON_GPU_PATTERN.search(probe):
        return None, None
    category = _gpu_category(probe)
    if category:
        return gpu_label or probe, category
    return None, None


def _is_reserved_like(row: dict, pricing_type: str, raw_extra: dict) -> bool:
    if pricing_type == "reserved":
        return True
    if str(row.get("commitment_period", "")).strip():
        return True
    if (_parse_float(row.get("upfront_price")) or 0) > 0:
        return True
    if (_parse_float(row.get("upfront_price_per_gpu")) or 0) > 0:
        return True
    if str(raw_extra.get("purchase_option", "")).strip():
        return True
    return False


def _is_fractional_or_slice(row: dict, raw_extra: dict) -> bool:
    gpu_count = _parse_float(row.get("gpu_count"))
    if gpu_count is not None and gpu_count < 1:
        return True
    gpu_frac = _parse_float(raw_extra.get("gpu_frac"))
    if gpu_frac is not None and 0 < gpu_frac < 1:
        return True
    haystack = " ".join(
        str(part or "")
        for part in (
            row.get("instance_type", ""),
            row.get("gpu_variant", ""),
            row.get("gpu_interconnect", ""),
            row.get("raw_extra", ""),
        )
    ).lower()
    if re.search(r"\bmig\b|\bslice\b|\bfractional\b|\bshared\s+gpu\b|\bpartial\b", haystack):
        return True
    if re.search(r"(?:^|[-_])\d+c(?:[-_]|$)", haystack):
        return True
    if re.search(r"\b\d+g\.\d+gb\b", haystack):
        return True
    return False


def _is_aggregate_offer(row: dict, raw_extra: dict) -> bool:
    price_tier = str(raw_extra.get("price_tier", "")).strip().lower()
    if price_tier in SUMMARY_PRICE_TIERS:
        return True

    instance_type = str(row.get("instance_type", "")).strip().lower()
    instance_key = re.sub(r"[\s-]+", "_", instance_type)
    if instance_key.startswith("cheapest_"):
        return True
    if instance_key in SUMMARY_PRICE_TIERS:
        return True
    if "weightedaverage" in instance_key or "weighted_average" in instance_key:
        return True
    if instance_key.endswith(("_avg", "_min", "_max")):
        return True
    return False


def _fails_price_sanity(gpu_label: str, pricing_type: str, price_per_gpu_hour: float) -> bool:
    if price_per_gpu_hour <= 0 or price_per_gpu_hour > 500:
        return True

    if gpu_label in {"B200", "GB200"}:
        return price_per_gpu_hour < (2.0 if pricing_type == "on_demand" else 1.0)
    if gpu_label in {"H200", "GH200"}:
        return price_per_gpu_hour < (1.0 if pricing_type == "on_demand" else 0.75)
    if gpu_label == "H100":
        return price_per_gpu_hour < (0.75 if pricing_type == "on_demand" else 0.5)
    if gpu_label in {"MI300X", "MI325X", "MI355X"}:
        return price_per_gpu_hour < (0.75 if pricing_type == "on_demand" else 0.5)
    if gpu_label == "A100":
        return price_per_gpu_hour < (0.3 if pricing_type == "on_demand" else 0.15)
    if gpu_label.startswith(("RTX", "GTX")):
        return price_per_gpu_hour > (20.0 if pricing_type == "on_demand" else 10.0)
    return False


def _normalize_row(row: dict) -> tuple[dict | None, str | None]:
    pricing_type = normalize_pricing_type(row.get("pricing_type", ""))
    if pricing_type == "inference" or pricing_type not in {"on_demand", "spot", "reserved"}:
        return None, None

    raw_extra = _parse_raw_extra(row.get("raw_extra", ""))
    gpu_label, gpu_category = _canonicalize_gpu(row.get("gpu_name", ""))
    if not gpu_label:
        if NON_GPU_PATTERN.search(str(row.get("gpu_name", "") or "")):
            return None, "excluded_non_gpu_rows"
        return None, None

    if _is_reserved_like(row, pricing_type, raw_extra):
        return None, "excluded_reserved_or_commitment_rows"
    if _is_fractional_or_slice(row, raw_extra):
        return None, "excluded_fractional_or_slice_rows"
    if _is_aggregate_offer(row, raw_extra):
        return None, "excluded_aggregate_offer_rows"

    currency = str(row.get("currency", "")).strip().upper()
    if currency not in {"", "USD"}:
        return None, None

    price_unit = str(row.get("price_unit", "")).strip().lower().replace(" ", "-")
    if price_unit not in ALLOWED_PRICE_UNITS:
        return None, None

    price_per_gpu_hour = _parse_float(row.get("price_per_gpu_hour"))
    if price_per_gpu_hour is None:
        return None, None
    if _fails_price_sanity(gpu_label, pricing_type, price_per_gpu_hour):
        return None, "excluded_price_sanity_rows"

    provider_slug = normalize_provider(row.get("provider", "") or row.get("source", ""))
    provider_label = _provider_label(provider_slug)
    gpu_slug = _slugify(gpu_label)
    region_slug, region_label = _normalize_region_bucket(row)
    interconnect_slug, interconnect_label = _normalize_interconnect(row)

    vram_gb = normalize_gpu_memory_gb(
        row.get("gpu_memory_gb", ""),
        gpu_label,
        row.get("gpu_count", ""),
        row.get("gpu_variant", ""),
    )
    if vram_gb == "":
        vram_gb = None
    else:
        vram_gb = _clean_number(vram_gb)

    return {
        "snapshot_date": str(row.get("snapshot_date", "")).strip(),
        "snapshot_ts": str(row.get("snapshot_ts", "")).strip(),
        "provider_slug": provider_slug,
        "provider_label": provider_label,
        "gpu_slug": gpu_slug,
        "gpu_label": gpu_label,
        "gpu_category": gpu_category,
        "interconnect_slug": interconnect_slug,
        "interconnect_label": interconnect_label,
        "region_slug": region_slug,
        "region_label": region_label,
        "pricing_type": pricing_type,
        "price_per_gpu_hour": price_per_gpu_hour,
        "instance_type": str(row.get("instance_type", "")).strip(),
        "vram_gb": vram_gb,
    }, None


def _continuous_dates(min_date: str, max_date: str) -> list[str]:
    if not min_date or not max_date:
        return []
    current = date.fromisoformat(min_date)
    end = date.fromisoformat(max_date)
    dates = []
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _round_money(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value + 1e-12, 2)


def _round_pct(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value + 1e-12, 1)


def _default_release_name(timestamp: str) -> str:
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return "GPU Pricing Data"
    return f"GPU Pricing Data — {dt.strftime('%Y-%m-%d %H:%M UTC')}"


def _build_dashboard_asset_from_rows_iter(
    rows_iter,
    generated_at: str = "",
    release_tag: str = DEFAULT_RELEASE_TAG,
    release_name: str = "",
    release_updated_at: str = "",
) -> dict:
    generated_at = generated_at or _utc_now_iso()
    release_updated_at = release_updated_at or generated_at

    quality_summary = {key: 0 for key in QUALITY_SUMMARY_KEYS}
    min_date = ""
    max_date = ""
    source_last_updated = ""
    gpu_meta = {}
    instance_minima = {}
    latest_on_demand = {}
    latest_spot = {}
    latest_snapshot_date = ""

    for row in rows_iter:
        snapshot_date = str(row.get("snapshot_date", "")).strip()
        snapshot_ts = str(row.get("snapshot_ts", "")).strip()
        if snapshot_date:
            if not min_date or snapshot_date < min_date:
                min_date = snapshot_date
            if not max_date or snapshot_date > max_date:
                max_date = snapshot_date
            if not latest_snapshot_date or snapshot_date > latest_snapshot_date:
                latest_snapshot_date = snapshot_date
                latest_on_demand.clear()
                latest_spot.clear()
        if snapshot_ts and snapshot_ts > source_last_updated:
            source_last_updated = snapshot_ts

        normalized, exclusion_reason = _normalize_row(row)
        if exclusion_reason:
            quality_summary[exclusion_reason] += 1
        if not normalized:
            continue
        gpu_meta[normalized["gpu_slug"]] = {
            "gpu_label": normalized["gpu_label"],
            "gpu_category": normalized["gpu_category"],
        }

        if normalized["pricing_type"] == "on_demand" and normalized["snapshot_date"]:
            key = (
                normalized["snapshot_date"],
                normalized["gpu_slug"],
                normalized["provider_slug"],
                normalized["instance_type"],
            )
            current = instance_minima.get(key)
            if current is None or normalized["price_per_gpu_hour"] < current:
                instance_minima[key] = normalized["price_per_gpu_hour"]

        if normalized["snapshot_date"] != latest_snapshot_date:
            continue

        latest_key = (
            normalized["provider_slug"],
            normalized["gpu_slug"],
            normalized["interconnect_slug"],
            normalized["region_slug"],
        )
        if normalized["pricing_type"] == "on_demand":
            current = latest_on_demand.get(latest_key)
            if current is None or normalized["price_per_gpu_hour"] < current["price_per_gpu_hour"]:
                latest_on_demand[latest_key] = normalized
        elif normalized["pricing_type"] == "spot":
            current = latest_spot.get(latest_key)
            if current is None or normalized["price_per_gpu_hour"] < current["price_per_gpu_hour"]:
                latest_spot[latest_key] = normalized

    if not source_last_updated:
        source_last_updated = f"{max_date}T00:00:00Z" if max_date else generated_at

    dates = _continuous_dates(min_date, max_date)
    date_index = {value: index for index, value in enumerate(dates)}

    # Chart aggregation:
    # 1. per date+gpu+provider+instance keep minimum price
    # 2. per date+gpu+provider median of instance minima
    # 3. per date+gpu median across providers
    provider_instance_prices = defaultdict(list)
    for (snapshot_date, gpu_slug, provider_slug, _instance_type), price in instance_minima.items():
        provider_instance_prices[(snapshot_date, gpu_slug, provider_slug)].append(price)

    provider_medians = {
        key: float(median(prices))
        for key, prices in provider_instance_prices.items()
    }

    market_prices = defaultdict(list)
    for (snapshot_date, gpu_slug, _provider_slug), provider_price in provider_medians.items():
        market_prices[(snapshot_date, gpu_slug)].append(provider_price)

    chart_values_by_gpu = defaultdict(dict)
    for (snapshot_date, gpu_slug), prices in market_prices.items():
        chart_values_by_gpu[gpu_slug][snapshot_date] = float(median(prices))

    ranked_chart_gpus = sorted(
        (
            (
                sum(1 for value in values.values() if value is not None),
                gpu_meta[gpu_slug]["gpu_label"].lower(),
                gpu_slug,
            )
            for gpu_slug, values in chart_values_by_gpu.items()
        ),
        key=lambda item: (-item[0], item[1], item[2]),
    )
    chart_gpu_slugs = [gpu_slug for coverage_days, _label, gpu_slug in ranked_chart_gpus if coverage_days > 0][:15]
    chart_rank_by_gpu = {gpu_slug: index + 1 for index, gpu_slug in enumerate(chart_gpu_slugs)}

    chart_series = []
    rounded_chart_values = {}
    for gpu_slug in chart_gpu_slugs:
        values = [None] * len(dates)
        for snapshot_date, value in chart_values_by_gpu.get(gpu_slug, {}).items():
            if snapshot_date in date_index:
                values[date_index[snapshot_date]] = _round_money(value)
        rounded_chart_values[gpu_slug] = values
        chart_series.append({
            "gpu_slug": gpu_slug,
            "gpu_label": gpu_meta[gpu_slug]["gpu_label"],
            "values": values,
        })

    def change_30d_pct_for_gpu(gpu_slug: str) -> float | None:
        if not dates:
            return None
        values = rounded_chart_values.get(gpu_slug, [])
        if not values:
            return None
        latest_value = values[-1]
        if latest_value is None:
            return None
        target_date = date.fromisoformat(max_date) - timedelta(days=30)
        for cursor in range(len(dates) - 1, -1, -1):
            if date.fromisoformat(dates[cursor]) > target_date:
                continue
            previous_value = values[cursor]
            if previous_value is None or previous_value <= 0:
                continue
            return ((latest_value - previous_value) / previous_value) * 100.0
        return None

    latest_pricing = []
    for key, on_demand_row in latest_on_demand.items():
        spot_row = latest_spot.get(key)
        quality_flags = []
        if on_demand_row["region_slug"] == "unknown":
            quality_flags.append("unknown_region")
        if on_demand_row["vram_gb"] is None:
            quality_flags.append("missing_vram")
        latest_pricing.append({
            "provider_slug": on_demand_row["provider_slug"],
            "provider_label": on_demand_row["provider_label"],
            "gpu_slug": on_demand_row["gpu_slug"],
            "gpu_label": on_demand_row["gpu_label"],
            "gpu_category": on_demand_row["gpu_category"],
            "interconnect_slug": on_demand_row["interconnect_slug"],
            "interconnect_label": on_demand_row["interconnect_label"],
            "region_slug": on_demand_row["region_slug"],
            "region_label": on_demand_row["region_label"],
            "price_per_hr": _round_money(on_demand_row["price_per_gpu_hour"]),
            "spot_per_hr": _round_money(spot_row["price_per_gpu_hour"]) if spot_row else None,
            "vram_gb": on_demand_row["vram_gb"],
            "change_30d_pct": _round_pct(change_30d_pct_for_gpu(on_demand_row["gpu_slug"])),
            "quality_flags": quality_flags,
        })

    latest_pricing.sort(key=lambda row: (
        row["price_per_hr"],
        row["provider_label"],
        row["gpu_label"],
        row["region_label"],
    ))

    catalog_gpu_slugs = set(chart_gpu_slugs)
    catalog_gpu_slugs.update(row["gpu_slug"] for row in latest_pricing)
    gpu_catalog = []
    for gpu_slug in sorted(
        catalog_gpu_slugs,
        key=lambda slug: (
            chart_rank_by_gpu.get(slug, 9999),
            gpu_meta.get(slug, {}).get("gpu_label", slug).lower(),
            slug,
        ),
    ):
        meta = gpu_meta[gpu_slug]
        gpu_catalog.append({
            "gpu_slug": gpu_slug,
            "gpu_label": meta["gpu_label"],
            "gpu_category": meta["gpu_category"],
            "chart_included": gpu_slug in chart_rank_by_gpu,
            "chart_rank": chart_rank_by_gpu.get(gpu_slug),
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "source_release": {
            "tag": release_tag,
            "name": release_name or _default_release_name(release_updated_at),
            "updated_at": release_updated_at,
            "source_last_updated": source_last_updated,
        },
        "coverage": {
            "min_date": min_date,
            "max_date": max_date,
            "latest_pricing_date": max_date,
            "chart_gpu_count": len(chart_series),
            "latest_pricing_rows": len(latest_pricing),
        },
        "quality_summary": quality_summary,
        "gpu_catalog": gpu_catalog,
        "chart": {
            "aggregation": "median_across_providers_of_provider_instance_minima",
            "pricing_kind": "on_demand",
            "default_selected_gpu_slugs": chart_gpu_slugs[:3],
            "dates": dates,
            "series": chart_series,
        },
        "latest_pricing": latest_pricing,
    }


def build_dashboard_asset(
    rows: list[dict],
    generated_at: str = "",
    release_tag: str = DEFAULT_RELEASE_TAG,
    release_name: str = "",
    release_updated_at: str = "",
) -> dict:
    return _build_dashboard_asset_from_rows_iter(
        iter(rows),
        generated_at=generated_at,
        release_tag=release_tag,
        release_name=release_name,
        release_updated_at=release_updated_at,
    )


def write_dashboard_asset(asset: dict, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with gzip.open(output_path, "wt", encoding="utf-8") as f:
        json.dump(asset, f, separators=(",", ":"), ensure_ascii=False)


def read_master_rows(master_csv_path: str) -> list[dict]:
    if not os.path.isfile(master_csv_path):
        return []
    with open(master_csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_dashboard_asset_from_master(
    master_csv_path: str = DEFAULT_INPUT_PATH,
    output_path: str = DEFAULT_OUTPUT_PATH,
    generated_at: str = "",
    release_tag: str = DEFAULT_RELEASE_TAG,
    release_name: str = "",
    release_updated_at: str = "",
) -> dict:
    if not os.path.isfile(master_csv_path):
        asset = _build_dashboard_asset_from_rows_iter(
            (),
            generated_at=generated_at,
            release_tag=release_tag,
            release_name=release_name,
            release_updated_at=release_updated_at,
        )
        write_dashboard_asset(asset, output_path)
        return asset

    with open(master_csv_path, newline="", encoding="utf-8") as f:
        asset = _build_dashboard_asset_from_rows_iter(
            csv.DictReader(f),
            generated_at=generated_at,
            release_tag=release_tag,
            release_name=release_name,
            release_updated_at=release_updated_at,
        )
    write_dashboard_asset(asset, output_path)
    logger.info(
        "Dashboard asset: %s chart GPUs, %s latest pricing rows → %s",
        asset["coverage"]["chart_gpu_count"],
        asset["coverage"]["latest_pricing_rows"],
        output_path,
    )
    return asset


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dashboard_gpu_daily.json.gz from data/_master.csv")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to data/_master.csv")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to dashboard_gpu_daily.json.gz")
    parser.add_argument("--generated-at", default="", help="Override generated_at / release updated timestamp")
    parser.add_argument("--release-tag", default=DEFAULT_RELEASE_TAG, help="Release tag (default: latest-data)")
    parser.add_argument("--release-name", default="", help="Release display name")
    parser.add_argument("--release-updated-at", default="", help="Release updated timestamp (ISO-8601 UTC)")
    args = parser.parse_args()

    asset = build_dashboard_asset_from_master(
        master_csv_path=args.input,
        output_path=args.output,
        generated_at=args.generated_at,
        release_tag=args.release_tag,
        release_name=args.release_name,
        release_updated_at=args.release_updated_at,
    )
    print(args.output)
    print(
        f"chart_gpu_count={asset['coverage']['chart_gpu_count']} "
        f"latest_pricing_rows={asset['coverage']['latest_pricing_rows']}"
    )


if __name__ == "__main__":
    main()
