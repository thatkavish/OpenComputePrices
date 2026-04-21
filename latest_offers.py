#!/usr/bin/env python3
"""
Build a presentation-oriented latest GPU offers view from the canonical master CSV.

This is intentionally separate from the main unification/finalization pipeline.
It keeps `_master.csv` as the full-fidelity database and emits a derived latest
snapshot CSV that collapses:

1. exact duplicate rows
2. "shadow" rows where another row with the same offer identity and pricing is
   strictly more specific on location/interconnect metadata
"""

import argparse
import csv
import logging
import os
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

from schema import COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
MASTER_PATH = os.path.join(DATA_DIR, "_master.csv")
LATEST_OFFERS_PATH = os.path.join(DATA_DIR, "_latest_gpu_offers.csv")

LOCATION_FIELDS = ["region", "zone", "country", "geo_group", "gpu_interconnect"]
AGGREGATOR_SOURCES = {"getdeploying"}

# Group rows by the dimensions that should describe the same underlying offer
# for presentation purposes. Location/interconnect are excluded so a more
# specific row can dominate a generic one within the same offer group.
GROUP_FIELDS = [
    "snapshot_date",
    "provider",
    "gpu_name",
    "gpu_variant",
    "gpu_memory_gb",
    "gpu_count",
    "vcpus",
    "ram_gb",
    "storage_desc",
    "network_desc",
    "pricing_type",
    "commitment_period",
    "price_per_hour",
    "price_per_gpu_hour",
    "upfront_price",
    "upfront_price_per_gpu",
    "currency",
    "price_unit",
    "available",
    "available_count",
    "os",
    "tenancy",
    "pre_installed_sw",
]

GENERIC_VALUES = {
    "",
    "-",
    "--",
    "unknown",
    "global",
    "worldwide",
    "any",
    "all",
    "none",
    "null",
    "n/a",
    "na",
}


def _text(value) -> str:
    return str(value or "").strip()


def _canon(value) -> str:
    return _text(value).lower()


def _is_generic(value) -> bool:
    return _canon(value) in GENERIC_VALUES


def _group_key(row: Dict[str, str]) -> Tuple[str, ...]:
    return tuple(_canon(row.get(field, "")) for field in GROUP_FIELDS)


def _instance_compatible(left: Dict[str, str], right: Dict[str, str]) -> bool:
    left_type = _canon(left.get("instance_type", ""))
    right_type = _canon(right.get("instance_type", ""))
    left_family = _canon(left.get("instance_family", ""))
    right_family = _canon(right.get("instance_family", ""))

    type_ok = not left_type or not right_type or left_type == right_type
    family_ok = not left_family or not right_family or left_family == right_family
    return type_ok and family_ok


def _same_specific_value(left: Dict[str, str], right: Dict[str, str], field: str) -> bool:
    return _canon(left.get(field, "")) == _canon(right.get(field, ""))


def _rows_equivalent(left: Dict[str, str], right: Dict[str, str]) -> bool:
    if not _instance_compatible(left, right):
        return False
    if _canon(left.get("instance_type", "")) != _canon(right.get("instance_type", "")):
        return False
    if _canon(left.get("instance_family", "")) != _canon(right.get("instance_family", "")):
        return False
    return all(_same_specific_value(left, right, field) for field in LOCATION_FIELDS)


def _row_dominates(left: Dict[str, str], right: Dict[str, str]) -> bool:
    if not _instance_compatible(left, right):
        return False

    strictly_better = False
    for field in LOCATION_FIELDS:
        left_value = _text(left.get(field, ""))
        right_value = _text(right.get(field, ""))
        if _canon(left_value) == _canon(right_value):
            continue
        if not _is_generic(left_value) and _is_generic(right_value):
            strictly_better = True
            continue
        return False

    return strictly_better


def _row_specificity_score(row: Dict[str, str]) -> Tuple[int, int, int]:
    info_fields = sum(0 if _is_generic(row.get(field, "")) else 1 for field in LOCATION_FIELDS)
    populated = sum(1 for field in COLUMNS if _text(row.get(field, "")))
    raw_extra_len = len(_text(row.get("raw_extra", "")))
    return (info_fields, populated, raw_extra_len)


def _merge_rows(preferred: Dict[str, str], other: Dict[str, str]) -> None:
    for field in COLUMNS:
        if not _text(preferred.get(field, "")) and _text(other.get(field, "")):
            preferred[field] = other[field]


def _identity_key(row: Dict[str, str]) -> Tuple[str, ...]:
    return (
        _canon(row.get("snapshot_date", "")),
        _canon(row.get("provider", "")),
        _canon(row.get("gpu_name", "")),
        _canon(row.get("pricing_type", "")),
    )


def _has_any_location_metadata(row: Dict[str, str]) -> bool:
    return any(not _is_generic(row.get(field, "")) for field in LOCATION_FIELDS)


def _drop_generic_aggregator_shadows(rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], int]:
    direct_rows_by_identity: Dict[Tuple[str, ...], List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        if _canon(row.get("source", "")) in AGGREGATOR_SOURCES:
            continue
        direct_rows_by_identity[_identity_key(row)].append(row)

    kept: List[Dict[str, str]] = []
    dropped = 0
    for row in rows:
        if _canon(row.get("source", "")) not in AGGREGATOR_SOURCES:
            kept.append(row)
            continue
        if _has_any_location_metadata(row):
            kept.append(row)
            continue
        if direct_rows_by_identity.get(_identity_key(row)):
            dropped += 1
            continue
        kept.append(row)

    return kept, dropped


def derive_latest_gpu_offers(rows: Iterable[Dict[str, str]]) -> Tuple[List[Dict[str, str]], Dict[str, int]]:
    by_group: Dict[Tuple[str, ...], List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_group[_group_key(row)].append(dict(row))

    output: List[Dict[str, str]] = []
    exact_dropped = 0
    shadow_dropped = 0

    for group_rows in by_group.values():
        kept: List[Dict[str, str]] = []
        for row in sorted(group_rows, key=_row_specificity_score, reverse=True):
            candidate = dict(row)
            suppressor = None
            for existing in kept:
                if _rows_equivalent(existing, candidate):
                    _merge_rows(existing, candidate)
                    exact_dropped += 1
                    suppressor = existing
                    break

                if _row_dominates(existing, candidate):
                    _merge_rows(existing, candidate)
                    shadow_dropped += 1
                    suppressor = existing
                    break

            if suppressor is not None:
                continue

            next_kept: List[Dict[str, str]] = []
            for existing in kept:
                if _row_dominates(candidate, existing):
                    _merge_rows(candidate, existing)
                    shadow_dropped += 1
                    continue
                next_kept.append(existing)

            next_kept.append(candidate)
            kept = next_kept

        output.extend(kept)

    output, aggregator_shadow_dropped = _drop_generic_aggregator_shadows(output)

    output.sort(
        key=lambda row: (
            row.get("provider", ""),
            row.get("gpu_name", ""),
            row.get("pricing_type", ""),
            row.get("price_per_gpu_hour", ""),
            row.get("region", ""),
            row.get("gpu_interconnect", ""),
            row.get("instance_type", ""),
        )
    )
    stats = {
        "input_rows": sum(len(group) for group in by_group.values()),
        "output_rows": len(output),
        "exact_duplicates_dropped": exact_dropped,
        "shadow_rows_dropped": shadow_dropped,
        "aggregator_shadow_rows_dropped": aggregator_shadow_dropped,
    }
    return output, stats


def load_latest_gpu_rows(path: str, snapshot_date: str = "") -> Tuple[str, List[Dict[str, str]]]:
    target_date = snapshot_date
    latest_seen = ""
    rows: List[Dict[str, str]] = []
    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_date = _text(row.get("snapshot_date", ""))
            if not row_date:
                continue
            if _canon(row.get("pricing_type", "")) == "inference":
                continue
            if target_date:
                if row_date != target_date:
                    continue
            else:
                if row_date > latest_seen:
                    latest_seen = row_date
                    rows = []
                if row_date != latest_seen:
                    continue
            rows.append({field: row.get(field, "") for field in COLUMNS})

    resolved_date = target_date or latest_seen
    if not resolved_date:
        raise ValueError(f"No GPU cloud rows found in {path}")
    return resolved_date, rows


def save_rows(rows: List[Dict[str, str]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a derived latest GPU offers view")
    parser.add_argument("--input", default=MASTER_PATH, help="Input master CSV (default: data/_master.csv)")
    parser.add_argument("--output", default=LATEST_OFFERS_PATH, help="Output CSV (default: data/_latest_gpu_offers.csv)")
    parser.add_argument("--date", default="", help="Snapshot date to build (default: latest available)")
    args = parser.parse_args()

    snapshot_date, rows = load_latest_gpu_rows(args.input, snapshot_date=args.date)
    offers, stats = derive_latest_gpu_offers(rows)
    save_rows(offers, args.output)

    logger.info(
        "Latest offers view: %s rows → %s rows for %s (%s exact dupes, %s shadow rows) → %s",
        stats["input_rows"],
        stats["output_rows"],
        snapshot_date,
        stats["exact_duplicates_dropped"],
        stats["shadow_rows_dropped"],
        args.output,
    )


if __name__ == "__main__":
    main()
