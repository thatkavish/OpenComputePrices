#!/usr/bin/env python3
"""
Main entry point for GPU pricing data collection.

Usage:
    python collect.py                    # Run all collectors
    python collect.py aws azure          # Run specific collectors
    python collect.py --list             # List available collectors
"""

import argparse
import csv
import logging
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from schema import (
    COLUMNS,
    infer_geo_group,
    normalize_gpu_memory_gb,
    normalize_gpu_name,
    normalize_provider,
    normalize_region,
)
from row_utils import (
    dump_raw_extra_dict as _dump_raw_extra_dict,
    format_float as _format_float,
    parse_float as _parse_float,
    parse_raw_extra_dict as _parse_raw_extra_dict,
)
from storage import (
    BASELINE_STATE_FILENAME,
    append_rows as _append_rows,
    baseline_state_path as _baseline_state_path,
    cleanup_baseline_state as _cleanup_baseline_state_impl,
    dedupe_rows as _dedupe_rows,
    first_snapshot_date as _first_snapshot_date,
    load_baseline_state as _load_baseline_state_impl,
    load_source_rows_for_dates as _load_source_rows_for_dates,
    read_appended_rows as _read_appended_rows,
    replace_appended_rows as _replace_appended_rows,
    rewrite_csv_excluding_dates_and_cutoff as _rewrite_csv_excluding_dates_and_cutoff,
    row_key as _row_key,
)
# Preserve collect.py's private helper names while moving implementations out.
from validation import (
    is_implausible_akash_outlier as _is_implausible_akash_outlier,
    is_malformed_coreweave_price_only_row as _is_malformed_coreweave_price_only_row,
    is_noncanonical_akash_price_tier as _is_noncanonical_akash_price_tier,
    should_keep_existing_row as _should_keep_existing_row,
)


def _load_dotenv():
    """Load .env files into os.environ (stdlib only, no python-dotenv needed)."""
    root = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(root, ".env"),
        os.path.join(root, "collectors", ".env"),
    ]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value


_load_dotenv()

from collector_registry import (
    API_KEY_COLLECTORS,
    BROWSER_COLLECTORS,
    COLLECTOR_TYPES,
    COLLECTORS,
    NO_AUTH_COLLECTORS,
)
from collectors.vultr import infer_effective_gpu_count

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_CURRENCY_CODES = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY", "CNY", "INR", "KRW"}
_PRICE_UNITS = {"hour", "hr", "gpu_hour", "gpu-hour", "second", "month", "token"}
_BOOLEANISH = {"true", "false", "1", "0", ""}
_AZURE_GPU_SPEC_OVERRIDES = {
    "Standard_NC40ads_H100_v5": {"gpu_name": "H100", "gpu_memory_gb": 94, "gpu_count": 1, "gpu_variant": "NVL"},
    "Standard_NC80adis_H100_v5": {"gpu_name": "H100", "gpu_memory_gb": 94, "gpu_count": 2, "gpu_variant": "NVL"},
}
_COREWEAVE_GPU_COUNT_BY_INSTANCE = {
    "A100": 8,
    "GB200 NVL72": 4,
    "L40": 8,
    "L40S": 8,
}
_COREWEAVE_GPU_MEMORY_BY_INSTANCE = {
    "A100": 80,
    "GB200 NVL72": 186,
    "L40": 48,
    "L40S": 48,
}
_AWS_FRONTIER_GPU_MIN_PER_GPU_HOUR = {
    "A100": 0.30,
    "H100": 0.75,
    "H200": 1.00,
    "B200": 2.00,
    "GB200": 2.00,
}


def resolve_collector_names(args) -> list[str]:
    """Resolve CLI selection flags into a concrete ordered list of collector names."""
    requested_sources = []
    if args.sources_csv:
        requested_sources.extend(part.strip() for part in args.sources_csv.split(","))
    if args.sources:
        requested_sources.extend(args.sources)
    requested_sources = [name for name in requested_sources if name]

    if requested_sources:
        names = list(requested_sources)
        if args.browser:
            names = [n for n in names if n in BROWSER_COLLECTORS]
        elif args.no_auth_only:
            names = [n for n in names if n in NO_AUTH_COLLECTORS]
        elif args.no_browser:
            names = [n for n in names if n not in BROWSER_COLLECTORS]
    elif args.browser:
        names = list(BROWSER_COLLECTORS)
    elif args.no_auth_only:
        names = list(NO_AUTH_COLLECTORS)
    elif args.no_browser:
        names = [n for n in COLLECTORS if n not in BROWSER_COLLECTORS]
    else:
        names = list(COLLECTORS.keys())

    return [n for n in names if n not in args.skip]


def _load_baseline_state(data_dir: str) -> dict:
    return _load_baseline_state_impl(data_dir, logger=logger)


def _cleanup_baseline_state(data_dir: str) -> None:
    _cleanup_baseline_state_impl(data_dir, logger=logger)


def _repair_shifted_tail_row(row: dict) -> bool:
    """
    Repair rows written after the upfront-price schema addition under a stale header.

    The telltale shape is currency/price_unit values landing in available/
    available_count, while the two upfront fields are shifted into currency/
    price_unit. We can recover fields through tenancy; any values beyond the old
    tail width were already dropped by the stale-header rewrite.
    """
    available = str(row.get("available", "")).strip().upper()
    price_unit = str(row.get("available_count", "")).strip().lower()
    os_value = str(row.get("os", "")).strip().lower()
    if available not in _CURRENCY_CODES or price_unit not in _PRICE_UNITS or os_value not in _BOOLEANISH:
        return False

    old = {col: row.get(col, "") for col in COLUMNS}
    row["upfront_price"] = old.get("currency", "")
    row["upfront_price_per_gpu"] = old.get("price_unit", "")
    row["currency"] = old.get("available", "")
    row["price_unit"] = old.get("available_count", "")
    row["available"] = old.get("os", "")
    row["available_count"] = old.get("tenancy", "")
    row["os"] = old.get("pre_installed_sw", "")
    row["tenancy"] = old.get("raw_extra", "")
    row["pre_installed_sw"] = ""
    row["raw_extra"] = ""
    return True


def _normalize_existing_row(row: dict) -> bool:
    changed = False
    provider = normalize_provider(row.get("provider", ""))
    if provider != row.get("provider", ""):
        row["provider"] = provider
        changed = True
    gpu_name = normalize_gpu_name(row.get("gpu_name", ""))
    if gpu_name != row.get("gpu_name", ""):
        row["gpu_name"] = gpu_name
        changed = True
    gpu_memory_gb = normalize_gpu_memory_gb(
        row.get("gpu_memory_gb", ""),
        row.get("gpu_name", ""),
        row.get("gpu_count", ""),
        row.get("gpu_variant", ""),
    )
    if str(gpu_memory_gb) != str(row.get("gpu_memory_gb", "")):
        row["gpu_memory_gb"] = gpu_memory_gb
        changed = True
    region = normalize_region(
        row.get("region", ""),
        row.get("provider", ""),
        row.get("country", ""),
        row.get("raw_extra", ""),
        row.get("source", ""),
    )
    if region != row.get("region", ""):
        row["region"] = region
        changed = True
    geo_group = infer_geo_group(row.get("region", ""), row.get("country", ""))
    if geo_group != row.get("geo_group", ""):
        row["geo_group"] = geo_group
        changed = True
    if _repair_aws_existing_row(row):
        changed = True
    if _repair_azure_existing_row(row):
        changed = True
    if _repair_clore_existing_row(row):
        changed = True
    if _repair_akash_existing_row(row):
        changed = True
    if _repair_coreweave_existing_row(row):
        changed = True
    if _repair_vultr_existing_row(row):
        changed = True
    return changed


def _repair_aws_existing_row(row: dict) -> bool:
    if row.get("source") != "aws" and row.get("provider") != "aws":
        return False

    changed = False
    raw_extra = _parse_raw_extra_dict(row.get("raw_extra", ""))
    pricing_type = str(row.get("pricing_type", "")).lower()
    capacity_status = str(raw_extra.get("capacity_status", "")).strip()
    billing_model = str(raw_extra.get("billing_model", "")).strip()
    purchase_option = str(raw_extra.get("purchase_option", "")).strip()
    price_per_gpu_hour = _parse_float(row.get("price_per_gpu_hour"))
    gpu_name = row.get("gpu_name", "")
    suspicious_floor = _AWS_FRONTIER_GPU_MIN_PER_GPU_HOUR.get(gpu_name)

    should_mark_reserved = False
    commitment_period = row.get("commitment_period", "")
    purchase_marker = purchase_option
    billing_marker = billing_model

    if pricing_type == "on_demand":
        if billing_model == "capacity_block" or purchase_option == "capacity_block":
            should_mark_reserved = True
            commitment_period = commitment_period or "capacity_block"
            purchase_marker = purchase_marker or "capacity_block"
            billing_marker = billing_marker or "capacity_block"
        elif billing_model == "capacity_reservation" or purchase_option == "capacity_reservation":
            should_mark_reserved = True
            commitment_period = commitment_period or "capacity_reservation"
            purchase_marker = purchase_marker or "capacity_reservation"
            billing_marker = billing_marker or "capacity_reservation"
        elif capacity_status in {"UnusedCapacityReservation", "AllocatedCapacityReservation"}:
            should_mark_reserved = True
            commitment_period = commitment_period or "capacity_reservation"
            purchase_marker = purchase_marker or "capacity_reservation"
            billing_marker = billing_marker or "capacity_reservation"
        elif suspicious_floor and 0 < price_per_gpu_hour < suspicious_floor:
            should_mark_reserved = True
            commitment_period = commitment_period or "capacity_block"
            purchase_marker = purchase_marker or "capacity_block"
            billing_marker = billing_marker or "capacity_block"

    if should_mark_reserved:
        if row.get("pricing_type") != "reserved":
            row["pricing_type"] = "reserved"
            changed = True
        if row.get("commitment_period", "") != commitment_period:
            row["commitment_period"] = commitment_period
            changed = True
        if raw_extra.get("purchase_option", "") != purchase_marker:
            raw_extra["purchase_option"] = purchase_marker
            changed = True
        if raw_extra.get("billing_model", "") != billing_marker:
            raw_extra["billing_model"] = billing_marker
            changed = True

    if changed:
        row["raw_extra"] = _dump_raw_extra_dict(raw_extra, row.get("raw_extra", ""))
    return changed


def _repair_azure_existing_row(row: dict) -> bool:
    if row.get("source") != "azure" and row.get("provider") != "azure":
        return False

    changed = False
    sku = row.get("instance_type", "")
    spec = _AZURE_GPU_SPEC_OVERRIDES.get(sku)
    if spec:
        for field, value in spec.items():
            if str(row.get(field, "")) != str(value):
                row[field] = value
                changed = True

    price_per_hour = _parse_float(row.get("price_per_hour"))
    price_per_gpu_hour = _parse_float(row.get("price_per_gpu_hour"))
    if row.get("pricing_type") == "on_demand" and price_per_gpu_hour > 500:
        row["pricing_type"] = "reserved"
        row["commitment_period"] = row.get("commitment_period") or "unknown"
        row["upfront_price"] = row.get("upfront_price") or row.get("price_per_hour", "")
        row["upfront_price_per_gpu"] = row.get("upfront_price_per_gpu") or row.get("price_per_gpu_hour", "")
        row["price_per_hour"] = "0"
        row["price_per_gpu_hour"] = "0"
        changed = True

    gpu_count = _parse_float(row.get("gpu_count"))
    if gpu_count <= 0:
        return changed
    if row.get("pricing_type") == "reserved" and _parse_float(row.get("upfront_price")) > 0:
        expected = _format_float(_parse_float(row.get("upfront_price")) / gpu_count)
        if row.get("upfront_price_per_gpu") != expected:
            row["upfront_price_per_gpu"] = expected
            changed = True
    elif price_per_hour > 0:
        expected = _format_float(price_per_hour / gpu_count)
        if row.get("price_per_gpu_hour") != expected:
            row["price_per_gpu_hour"] = expected
            changed = True
    return changed


def _repair_clore_existing_row(row: dict) -> bool:
    if row.get("source") != "cloreai" and row.get("provider") != "cloreai":
        return False

    raw_extra = _parse_raw_extra_dict(row.get("raw_extra", ""))
    source_price = _parse_float(raw_extra.get("source_price_usd"))
    source_unit = str(raw_extra.get("source_price_unit", "")).strip().lower()
    legacy_price = _parse_float(raw_extra.get("daily_usd"))
    changed = False

    if legacy_price > 0:
        source_price = legacy_price
        source_unit = "hour"
        raw_extra.pop("daily_usd", None)
        raw_extra["source_price_usd"] = legacy_price
        raw_extra["source_price_unit"] = "hour"
        changed = True

    if source_price <= 0 or source_unit not in {"hour", "hr", "hourly"}:
        if changed:
            row["raw_extra"] = _dump_raw_extra_dict(raw_extra, row.get("raw_extra", ""))
        return changed

    price_per_hour = _parse_float(row.get("price_per_hour"))
    gpu_count = _parse_float(row.get("gpu_count")) or 1.0
    if gpu_count <= 0:
        gpu_count = 1.0
    expected_per_gpu = source_price / gpu_count
    price_per_gpu_hour = _parse_float(row.get("price_per_gpu_hour"))

    if price_per_hour > 0 and abs((price_per_hour * 24) - source_price) <= 1e-4:
        corrected = _format_float(source_price)
        if row.get("price_per_hour") != corrected:
            row["price_per_hour"] = corrected
            changed = True

    if price_per_gpu_hour > 0 and abs((price_per_gpu_hour * 24) - expected_per_gpu) <= 1e-4:
        corrected = _format_float(expected_per_gpu)
        if row.get("price_per_gpu_hour") != corrected:
            row["price_per_gpu_hour"] = corrected
            changed = True

    if changed:
        row["raw_extra"] = _dump_raw_extra_dict(raw_extra, row.get("raw_extra", ""))
    return changed


def _repair_akash_existing_row(row: dict) -> bool:
    if row.get("source") != "akash" and row.get("provider") != "akash":
        return False

    raw_extra = _parse_raw_extra_dict(row.get("raw_extra", ""))
    changed = False

    price_tier = str(raw_extra.get("price_tier", "")).strip()
    price_metric = str(raw_extra.get("price_metric", "")).strip()
    instance_type = str(row.get("instance_type", "")).strip()

    if not price_metric and price_tier:
        raw_extra["price_metric"] = price_tier
        price_metric = price_tier
        changed = True

    if price_tier:
        raw_extra.pop("price_tier", None)
        changed = True

    if instance_type.endswith("_weightedAverage"):
        row["instance_type"] = instance_type[: -len("_weightedAverage")]
        changed = True

    if changed:
        row["raw_extra"] = _dump_raw_extra_dict(raw_extra, row.get("raw_extra", ""))
    return changed


def _repair_coreweave_existing_row(row: dict) -> bool:
    if row.get("source") != "coreweave" and row.get("provider") != "coreweave":
        return False

    changed = False
    instance_type = re.sub(r"\s+", " ", str(row.get("instance_type", "")).strip())
    gpu_count = _COREWEAVE_GPU_COUNT_BY_INSTANCE.get(instance_type)
    if not gpu_count:
        return False

    if str(row.get("gpu_count", "")) != str(gpu_count):
        row["gpu_count"] = gpu_count
        changed = True

    gpu_memory_gb = _COREWEAVE_GPU_MEMORY_BY_INSTANCE.get(instance_type)
    if gpu_memory_gb is not None and str(row.get("gpu_memory_gb", "")) != str(gpu_memory_gb):
        row["gpu_memory_gb"] = gpu_memory_gb
        changed = True

    price_per_hour = _parse_float(row.get("price_per_hour"))
    if price_per_hour > 0:
        expected = _format_float(price_per_hour / gpu_count)
        if row.get("price_per_gpu_hour") != expected:
            row["price_per_gpu_hour"] = expected
            changed = True

    return changed


def _repair_vultr_existing_row(row: dict) -> bool:
    if row.get("source") != "vultr" and row.get("provider") != "vultr":
        return False

    gpu_count = infer_effective_gpu_count(row.get("gpu_name", ""), row.get("gpu_memory_gb", ""))
    changed = False
    if str(row.get("gpu_count", "")) != str(gpu_count):
        row["gpu_count"] = gpu_count
        changed = True

    price_per_hour = _parse_float(row.get("price_per_hour"))
    if price_per_hour > 0 and gpu_count:
        expected = _format_float(price_per_hour / gpu_count)
        if row.get("price_per_gpu_hour") != expected:
            row["price_per_gpu_hour"] = expected
            changed = True

    return changed


def repair_schema_drift_csv(path: str) -> tuple[int, int, int]:
    """Repair shifted tail columns and canonicalize legacy aliases in one CSV."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return 0, 0, 0

    fd, tmp_path = tempfile.mkstemp(prefix="collect_repair_", suffix=".csv", dir=os.path.dirname(path) or None)
    os.close(fd)
    shifted = 0
    normalized = 0
    dropped = 0
    changed_rows = 0
    try:
        with open(path, newline="", encoding="utf-8") as src, open(tmp_path, "w", newline="", encoding="utf-8") as dst:
            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                changed = False
                if _repair_shifted_tail_row(row):
                    shifted += 1
                    changed = True
                if _normalize_existing_row(row):
                    normalized += 1
                    changed = True
                if not _should_keep_existing_row(row):
                    dropped += 1
                    changed = True
                    changed_rows += 1
                    continue
                if changed:
                    changed_rows += 1
                writer.writerow(row)

        if changed_rows:
            os.replace(tmp_path, path)
        return shifted, normalized, dropped
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def repair_schema_drift_in_data_dir(data_dir: str) -> int:
    """Repair known shifted-tail schema drift across CSVs in a data directory."""
    total_shifted = 0
    total_normalized = 0
    total_dropped = 0
    if not os.path.isdir(data_dir):
        return 0
    for fname in sorted(os.listdir(data_dir)):
        if not fname.endswith(".csv"):
            continue
        shifted, normalized, dropped = repair_schema_drift_csv(os.path.join(data_dir, fname))
        if shifted or normalized or dropped:
            logger.info(f"[repair] {fname}: shifted={shifted:,}, normalized={normalized:,}, dropped={dropped:,}")
            total_shifted += shifted
            total_normalized += normalized
            total_dropped += dropped
    logger.info(f"[repair] Done: shifted={total_shifted:,}, normalized={total_normalized:,}, dropped={total_dropped:,}")
    return total_shifted + total_normalized + total_dropped


def _prune_csv_by_cutoff(path: str, cutoff: str, collect_expired: bool = False) -> tuple[int, list[dict], set[str]]:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return 0, [], set()

    if _first_snapshot_date(path) >= cutoff and os.path.basename(path) != "coreweave.csv":
        return 0, [], set()

    fd, tmp_path = tempfile.mkstemp(prefix="collect_prune_", suffix=".csv", dir=os.path.dirname(path) or None)
    os.close(fd)
    removed = 0
    expired_rows = []
    removed_dates = set()
    try:
        with open(path, newline="", encoding="utf-8") as src, open(tmp_path, "w", newline="", encoding="utf-8") as dst:
            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                snapshot_date = row.get("snapshot_date", "")
                if snapshot_date < cutoff:
                    removed += 1
                    if collect_expired:
                        expired_rows.append(row)
                    continue
                if not _should_keep_existing_row(row):
                    removed += 1
                    if snapshot_date:
                        removed_dates.add(snapshot_date)
                    continue
                writer.writerow(row)

        os.replace(tmp_path, path)
        return removed, expired_rows, removed_dates
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _sort_master_rows(rows: list[dict]) -> list[dict]:
    rows = list(rows)
    rows.sort(key=lambda r: (
        r.get("snapshot_date", ""),
        r.get("provider", ""),
        r.get("gpu_name", ""),
        r.get("region", ""),
        r.get("pricing_type", ""),
        r.get("instance_type", ""),
    ))
    for row in rows:
        row.pop("_source_file", None)
    return rows


def _sort_inference_rows(rows: list[dict]) -> list[dict]:
    rows = list(rows)
    rows.sort(key=lambda r: (
        r.get("snapshot_date", ""),
        r.get("provider", ""),
        r.get("instance_type", ""),
    ))
    for row in rows:
        row.pop("_source_file", None)
    return rows


def _incremental_finalize_existing_data(data_dir: str, no_unify: bool = False) -> bool:
    baseline = _load_baseline_state(data_dir)
    sources_state = baseline.get("sources", {})
    if not sources_state:
        return False

    from collectors.base import RETENTION_DAYS
    from datetime import timedelta
    from unify import unify

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    incremental_rows = []
    touched_sources = 0
    expired_rows = []
    affected_dates = set()

    for fname in sorted(os.listdir(data_dir)):
        if not fname.endswith(".csv") or fname.startswith("_"):
            continue
        path = os.path.join(data_dir, fname)
        baseline_size = int(sources_state.get(fname, {}).get("size", 0))
        new_rows = _read_appended_rows(path, baseline_size)
        if not new_rows:
            continue

        touched_sources += 1
        unique_new_rows = _dedupe_rows(new_rows)
        if len(unique_new_rows) != len(new_rows):
            logger.info(f"[incremental] {fname}: deduped {len(new_rows) - len(unique_new_rows)} newly appended rows")
            _replace_appended_rows(path, baseline_size, unique_new_rows)

        removed, expired, removed_dates = _prune_csv_by_cutoff(path, cutoff, collect_expired=True)
        if removed:
            logger.info(f"[incremental] {fname}: pruned {removed} source rows")
            expired_rows.extend(expired)
            affected_dates.update(removed_dates)
        incremental_rows.extend(unique_new_rows)
        affected_dates.update(row.get("snapshot_date", "") for row in unique_new_rows if row.get("snapshot_date"))

    _cleanup_baseline_state(data_dir)

    archive_path = os.path.join(data_dir, "_expired.csv")
    if expired_rows:
        with open(archive_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in expired_rows:
                writer.writerow(row)
        logger.info(f"Incremental archive write: {len(expired_rows):,} expired rows → {archive_path}")
    elif os.path.isfile(archive_path):
        os.remove(archive_path)

    if not incremental_rows:
        logger.info("No newly appended source rows detected; skipping incremental finalization")
        return False

    logger.info(
        f"Incremental finalization: {len(incremental_rows):,} new rows from {touched_sources} sources "
        f"across {len(affected_dates)} snapshot date(s)"
    )

    if no_unify:
        logger.info("Skipping unified master rebuild (--no-unify)")
        return True

    snapshot_rows = _load_source_rows_for_dates(data_dir, affected_dates)
    gpu_rows = [r for r in snapshot_rows if r.get("pricing_type", "").lower() != "inference"]
    inference_rows = [r for r in snapshot_rows if r.get("pricing_type", "").lower() == "inference"]
    logger.info(
        f"Incremental slice rebuild: {len(snapshot_rows):,} rows across affected dates "
        f"({len(gpu_rows):,} GPU, {len(inference_rows):,} inference)"
    )

    master_path = os.path.join(data_dir, "_master.csv")
    inference_path = os.path.join(data_dir, "_inference.csv")

    removed_master = _rewrite_csv_excluding_dates_and_cutoff(master_path, cutoff, affected_dates)
    removed_inference = _rewrite_csv_excluding_dates_and_cutoff(inference_path, cutoff, affected_dates)
    if removed_master or removed_inference:
        logger.info(
            f"Rewrote generated outputs for affected dates: master={removed_master:,}, inference={removed_inference:,}"
        )

    unified_gpu = unify(gpu_rows, stats=False) if gpu_rows else []
    if unified_gpu:
        _append_rows(master_path, _sort_master_rows(unified_gpu))
        logger.info(f"Incremental master append: {len(unified_gpu):,} rows → {master_path}")

    unified_inference = unify(inference_rows, stats=False) if inference_rows else []
    if unified_inference:
        _append_rows(inference_path, _sort_inference_rows(unified_inference))
        logger.info(f"Incremental inference append: {len(unified_inference):,} rows → {inference_path}")

    return True


def finalize_existing_data(skip_prune: bool = False, no_unify: bool = False) -> bool:
    """Prune source CSVs and rebuild unified outputs from the current data directory."""
    from collectors.base import DATA_DIR

    if not os.path.isdir(DATA_DIR):
        logger.info("No data directory found; skipping finalization")
        return False

    source_csvs = [
        fname for fname in os.listdir(DATA_DIR)
        if fname.endswith(".csv") and not fname.startswith("_")
    ]
    if not source_csvs:
        logger.info("No source CSVs found; skipping finalization")
        return False

    if not skip_prune:
        has_baseline_state = bool(_load_baseline_state(DATA_DIR).get("sources"))
        if has_baseline_state:
            return _incremental_finalize_existing_data(DATA_DIR, no_unify=no_unify)

    if not skip_prune:
        logger.info("Pruning CSVs (dedup + retention)...")
        try:
            from collectors.base import prune_all_csvs
            archive_path = os.path.join(DATA_DIR, "_expired.csv")
            prune_all_csvs(archive_path=archive_path)
        except Exception as e:
            logger.error(f"Pruning failed: {e}", exc_info=True)

    if no_unify:
        logger.info("Skipping unified master rebuild (--no-unify)")
        return True

    logger.info("Building unified master database...")
    try:
        from unify import load_all_sources, unify, save_master, save_inference, MASTER_PATH, INFERENCE_PATH
        all_data = load_all_sources()
        inference_rows = [r for r in all_data if r.get("pricing_type", "").lower() == "inference"]
        gpu_rows = [r for r in all_data if r.get("pricing_type", "").lower() != "inference"]
        logger.info(f"Separated: {len(gpu_rows):,} GPU cloud rows, {len(inference_rows):,} inference rows")
        unified_gpu = unify(gpu_rows, stats=False)
        save_master(unified_gpu)
        if inference_rows:
            unified_inference = unify(inference_rows, stats=False)
            save_inference(unified_inference)
            logger.info(f"Inference database: {len(unified_inference):,} rows → {INFERENCE_PATH}")
    except Exception as e:
        logger.error(f"Unification failed: {e}", exc_info=True)

    return True


def main():
    parser = argparse.ArgumentParser(description="GPU Cloud Pricing Data Collector")
    parser.add_argument("sources", nargs="*", help="Specific sources to collect (default: all)")
    parser.add_argument("--sources-csv", default="", help="Comma-separated sources string for automation/workflows")
    parser.add_argument("--list", action="store_true", help="List available collectors")
    parser.add_argument("--no-auth-only", action="store_true", help="Only run collectors that need no API key")
    parser.add_argument("--browser", action="store_true", help="Only run Playwright browser-based collectors")
    parser.add_argument("--no-browser", action="store_true", help="Exclude Playwright browser-based collectors")
    parser.add_argument("--skip", nargs="*", default=[], help="Collectors to skip")
    parser.add_argument("--skip-prune", action="store_true", help="Skip CSV prune/dedup retention pass")
    parser.add_argument("--no-unify", action="store_true", help="Skip building the unified master database")
    parser.add_argument("--finalize-only", action="store_true", help="Skip collectors and only prune/unify existing data")
    parser.add_argument("--repair-schema-drift", action="store_true", help="Repair known shifted-tail CSV schema drift before finalization")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable collectors:")
        print(f"{'Name':<17} {'Type':<12} {'Auth':<10} {'Env Var'}")
        print("-" * 65)
        for name, cls in COLLECTORS.items():
            c = cls()
            if c.requires_api_key:
                auth = "API key"
            elif c.api_key_env_var:
                auth = "Optional"
            else:
                auth = "None"
            env = c.api_key_env_var or "-"
            ctype = COLLECTOR_TYPES.get(name, "unknown")
            print(f"{name:<17} {ctype:<12} {auth:<10} {env}")
        return

    if args.finalize_only:
        if args.repair_schema_drift:
            from collectors.base import DATA_DIR
            repair_schema_drift_in_data_dir(DATA_DIR)
        finalize_existing_data(skip_prune=args.skip_prune, no_unify=args.no_unify)
        return

    names = resolve_collector_names(args)

    if args.repair_schema_drift:
        from collectors.base import DATA_DIR
        repair_schema_drift_in_data_dir(DATA_DIR)

    now = datetime.now(timezone.utc)
    print(f"\n{'='*70}")
    print(f"  GPU Pricing Data Collection — {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Sources: {', '.join(names) if names else '(none selected)'}")
    print(f"{'='*70}\n")

    results = {}
    total_rows = 0
    t0 = time.time()

    # Pre-filter: skip unknown and missing-key collectors
    runnable = []
    for name in names:
        if name not in COLLECTORS:
            logger.warning(f"Unknown collector: {name}")
            continue
        cls = COLLECTORS[name]
        collector = cls()
        if collector.requires_api_key and not collector.get_api_key():
            logger.warning(f"[{name}] Skipping — missing {collector.api_key_env_var}")
            results[name] = {"status": "skipped", "reason": f"missing {collector.api_key_env_var}", "rows": 0}
            continue
        runnable.append((name, collector))

    if not names:
        logger.info("No collectors selected for this run")
        return

    def _run_one(name_collector):
        name, collector = name_collector
        ct0 = time.time()
        try:
            count = collector.run()
            elapsed = time.time() - ct0
            logger.info(f"[{name}] Done: {count} rows in {elapsed:.1f}s")
            return name, {"status": "ok", "rows": count, "elapsed": f"{elapsed:.1f}s"}
        except Exception as e:
            elapsed = time.time() - ct0
            logger.error(f"[{name}] Failed: {e}", exc_info=True)
            return name, {"status": "error", "error": str(e), "rows": 0, "elapsed": f"{elapsed:.1f}s"}

    # Run collectors in parallel (thread pool — most time is network I/O)
    max_workers = min(8, len(runnable)) if runnable else 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run_one, item): item[0] for item in runnable}
        for future in as_completed(futures):
            name, res = future.result()
            results[name] = res
            total_rows += res.get("rows", 0)

    total_elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*70}")
    print(f"  COLLECTION SUMMARY")
    print(f"{'='*70}")
    print(f"  {'Source':<15} {'Status':<10} {'Rows':>8}  {'Time':>8}  Notes")
    print(f"  {'-'*60}")
    for name, res in results.items():
        status = res["status"]
        rows = res.get("rows", 0)
        elapsed = res.get("elapsed", "")
        notes = res.get("reason", "") or res.get("error", "")
        print(f"  {name:<15} {status:<10} {rows:>8}  {elapsed:>8}  {notes}")
    print(f"  {'-'*60}")
    print(f"  {'TOTAL':<15} {'':10} {total_rows:>8}  {total_elapsed:.1f}s")
    print(f"{'='*70}\n")

    if not args.no_unify and total_rows > 0:
        finalize_existing_data(skip_prune=args.skip_prune, no_unify=args.no_unify)
    elif total_rows > 0 and not args.skip_prune:
        finalize_existing_data(skip_prune=args.skip_prune, no_unify=args.no_unify)

    # Exit with error if all failed
    terminal_statuses = [r["status"] for r in results.values()]
    if terminal_statuses and all(status == "error" for status in terminal_statuses):
        sys.exit(1)


if __name__ == "__main__":
    main()
