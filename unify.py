#!/usr/bin/env python3
"""
Unified master database builder.

Merges all per-source CSVs into a single deduplicated master CSV.
When multiple sources report pricing for the same provider+GPU+region+pricing_type,
the highest-priority source wins, with empty fields backfilled from lower-priority sources.

Usage:
    python unify.py                  # Build master from all source CSVs
    python unify.py --date 2026-03-22  # Build for a specific date only
    python unify.py --stats          # Show overlap/dedup statistics
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from typing import Dict, List, Any, Tuple

from schema import COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
MASTER_PATH = os.path.join(DATA_DIR, "_master.csv")
INFERENCE_PATH = os.path.join(DATA_DIR, "_inference.csv")

# ---------------------------------------------------------------------------
# Source priority tiers (lower number = higher priority = more authoritative)
# ---------------------------------------------------------------------------
# Tier 1: Direct provider APIs — most granular, authoritative
# Tier 2: Provider-specific scrapers — good but less complete
# Tier 3: Open-source catalogs — broad coverage, moderate granularity
# Tier 4: Aggregators — broadest coverage, least granular
# Tier 5: Browser scrapers — best-effort, often sparse

SOURCE_PRIORITY = {
    # --- Tier 1: Direct provider APIs ---
    "aws":             1,
    "azure":           1,
    "gcp":             1,
    "oracle":          1,
    "vultr":           1,
    "akash":           1,
    "cudo":            1,
    "linode":          1,
    "runpod":          1,
    "vastai":          1,
    "lambda":          1,
    "tensordock":      1,
    "deepinfra":       1,
    "openrouter":      1,
    # --- Tier 2: Provider-specific scrapers ---
    "jarvislabs":      2,
    "thundercompute":  2,
    "crusoe":          2,
    "novita":          2,
    "latitude":        2,
    "massedcompute":   2,
    "e2e":             2,
    "voltagepark":     2,
    "denvr":           2,
    "paperspace":      2,
    # --- Tier 2.5: Browser scrapers (direct) ---
    "coreweave":       2,
    "together":        2,
    "lightningai":     2,
    "gmicloud":        2,
    "aethir":          2,
    "hyperstack":      2,
    "gcore":           2,
    "firmus":          2,
    "neysa":           2,
    "salad":           2,
    "cloreai":         2,
    "exabits":         2,
    "qubrid":          2,
    # --- Tier 3: Open-source catalogs ---
    "skypilot":        3,
    "infracost":       3,
    # --- Tier 4: Aggregators ---
    "shadeform":       4,
    "getdeploying":    5,
    # --- Tier 5: API-key collectors not yet configured ---
    "primeintellect":  2,
    "datacrunch":      2,
}

# Fields that constitute the dedup key for cross-source merging.
# Two rows with the same dedup key from different sources represent
# the same underlying offering — the higher-priority source wins.
#
# We use TWO levels of dedup:
#   Level 1 (instance-level): exact instance_type match within same provider
#   Level 2 (GPU-level): same GPU+region+pricing_type within same provider
#       (used when instance_types differ across sources)

DEDUP_KEY_INSTANCE = [
    "snapshot_date", "provider", "instance_type", "pricing_type",
]

DEDUP_KEY_GPU = [
    "snapshot_date", "provider", "gpu_name", "gpu_count",
    "region", "pricing_type", "os",
]

# Fields to prefer from the higher-priority source (never overwrite with lower)
PRIORITY_FIELDS = [
    "price_per_hour", "price_per_gpu_hour", "currency", "price_unit",
    "instance_type", "instance_family",
]

# Fields to backfill from lower-priority sources if empty in the winner
BACKFILL_FIELDS = [
    "gpu_variant", "gpu_memory_gb", "gpu_interconnect",
    "vcpus", "ram_gb", "storage_desc", "network_desc",
    "zone", "country", "geo_group",
    "available", "available_count",
    "commitment_period",
]


def _make_key(row: Dict, fields: List[str]) -> Tuple:
    """Create a hashable dedup key from a row."""
    return tuple(str(row.get(f, "")).strip().lower() for f in fields)


def _source_priority(source: str) -> int:
    """Return priority number for a source (lower = better)."""
    return SOURCE_PRIORITY.get(source, 99)


def _is_empty(val) -> bool:
    """Check if a field value is empty/missing."""
    if val is None:
        return True
    s = str(val).strip()
    return s == "" or s.lower() in ("", "none", "nan", "null")


def _merge_rows(winner: Dict, loser: Dict) -> Dict:
    """
    Merge two rows: winner's priority fields are kept,
    loser's backfill fields fill in gaps.
    """
    merged = dict(winner)
    for field in BACKFILL_FIELDS:
        if _is_empty(merged.get(field)) and not _is_empty(loser.get(field)):
            merged[field] = loser[field]
    return merged


def load_all_sources(date_filter: str = "") -> List[Dict]:
    """Load all rows from all source CSVs in data/."""
    all_rows = []
    for fname in sorted(os.listdir(DATA_DIR)):
        if not fname.endswith(".csv") or fname.startswith("_"):
            continue
        path = os.path.join(DATA_DIR, fname)
        source = fname.replace(".csv", "")
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if date_filter and row.get("snapshot_date", "") != date_filter:
                    continue
                row["_source_file"] = source
                all_rows.append(row)
    return all_rows


def unify(rows: List[Dict], stats: bool = False) -> List[Dict]:
    """
    Deduplicate rows across sources using priority-based resolution.

    Key principle: **preserve all granularity from the best source**.

    Algorithm per provider:
    1. Group rows by source, rank sources by priority
    2. Keep ALL rows from the best (highest-priority) source — this preserves
       per-instance/per-OS/per-tenancy/per-region granularity
    3. From each lower-priority source, only add rows that cover
       (gpu_name, gpu_count, region, pricing_type) combos NOT already
       present from a higher-priority source
    4. When adding a lower-priority row, backfill its empty fields from
       the higher-priority data for the same GPU+region if available
    """
    # Group by provider
    by_provider = defaultdict(list)
    for row in rows:
        provider = row.get("provider", "").strip().lower()
        if provider:
            by_provider[provider].append(row)

    unified = []
    total_input = len(rows)
    total_dropped = 0

    for provider, provider_rows in sorted(by_provider.items()):
        # Group by source within this provider
        by_source = defaultdict(list)
        for row in provider_rows:
            by_source[row.get("source", "")].append(row)

        if len(by_source) <= 1:
            # Single source — pass through everything
            unified.extend(provider_rows)
            continue

        # Rank sources by priority
        ranked_sources = sorted(by_source.keys(), key=_source_priority)

        # Track which (gpu, count, region, pricing_type) combos are already covered
        covered_gpu_combos = set()
        # Track which exact instance_types are already covered
        covered_instances = set()

        for source in ranked_sources:
            source_rows = by_source[source]

            if not covered_gpu_combos:
                # Best source — keep ALL its rows
                unified.extend(source_rows)
                for r in source_rows:
                    covered_instances.add(_make_key(r, DEDUP_KEY_INSTANCE))
                    if not _is_empty(r.get("gpu_name")):
                        covered_gpu_combos.add(_make_key(r, DEDUP_KEY_GPU))
            else:
                # Lower-priority source — only add rows for NEW combos
                for r in source_rows:
                    inst_key = _make_key(r, DEDUP_KEY_INSTANCE)
                    gpu_key = _make_key(r, DEDUP_KEY_GPU)

                    # Skip if exact instance already covered
                    if inst_key in covered_instances:
                        total_dropped += 1
                        continue

                    # Skip if same GPU+region+pricing combo already covered
                    # (unless gpu_name is empty — inference/other pricing)
                    if not _is_empty(r.get("gpu_name")) and gpu_key in covered_gpu_combos:
                        total_dropped += 1
                        continue

                    # This is a new offering not in higher-priority sources — keep it
                    unified.append(r)
                    covered_instances.add(inst_key)
                    if not _is_empty(r.get("gpu_name")):
                        covered_gpu_combos.add(gpu_key)

    if stats:
        print(f"\n  Unification Statistics:")
        print(f"  {'─'*50}")
        print(f"  Input rows (all sources):     {total_input:>8,}")
        print(f"  Output rows (unified):        {len(unified):>8,}")
        print(f"  Cross-source dupes removed:   {total_dropped:>8,}")
        print(f"  Dedup rate:                   {total_dropped / max(total_input, 1) * 100:>7.1f}%")
        print(f"  Providers:                    {len(by_provider):>8,}")

        # Per-provider breakdown
        print(f"\n  Per-provider source resolution:")
        for provider in sorted(by_provider.keys()):
            pr = by_provider[provider]
            sources_in = sorted(set(r.get("source", "") for r in pr))
            in_count = len(pr)
            out_rows = [r for r in unified if r.get("provider", "").strip().lower() == provider]
            out_count = len(out_rows)
            source_counts = defaultdict(int)
            for r in out_rows:
                source_counts[r.get("source", "")] += 1
            winner = max(source_counts, key=source_counts.get) if source_counts else "?"
            if len(sources_in) > 1:
                breakdown = ", ".join(f"{s}:{source_counts.get(s,0)}" for s in sorted(source_counts))
                print(f"    {provider:20s}  {in_count:>6} → {out_count:>6}  primary={winner}  [{breakdown}]")

    return unified


def save_master(rows: List[Dict], path: str = MASTER_PATH):
    """Save unified rows to the master CSV."""
    if not rows:
        logger.info("No rows to save to master")
        return

    # Sort by date, provider, gpu_name, region, pricing_type
    rows.sort(key=lambda r: (
        r.get("snapshot_date", ""),
        r.get("provider", ""),
        r.get("gpu_name", ""),
        r.get("region", ""),
        r.get("pricing_type", ""),
        r.get("instance_type", ""),
    ))

    # Clean up internal fields
    for row in rows:
        row.pop("_source_file", None)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logger.info(f"Master database: {len(rows):,} rows → {path}")


def save_inference(rows: List[Dict], path: str = INFERENCE_PATH):
    """Save inference pricing rows to a separate CSV."""
    if not rows:
        logger.info("No rows to save to inference database")
        return

    # Sort by date, provider, instance_type (model name)
    rows.sort(key=lambda r: (
        r.get("snapshot_date", ""),
        r.get("provider", ""),
        r.get("instance_type", ""),
    ))

    # Clean up internal fields
    for row in rows:
        row.pop("_source_file", None)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logger.info(f"Inference database: {len(rows):,} rows → {path}")


def main():
    parser = argparse.ArgumentParser(description="Build unified master pricing database")
    parser.add_argument("--date", default="", help="Filter to a specific snapshot date (YYYY-MM-DD)")
    parser.add_argument("--stats", action="store_true", help="Show detailed dedup statistics")
    parser.add_argument("--output", default=MASTER_PATH, help="Output path (default: data/_master.csv)")
    parser.add_argument("--inference-output", default=INFERENCE_PATH, help="Inference output path (default: data/_inference.csv)")
    args = parser.parse_args()

    logger.info("Loading all source CSVs...")
    rows = load_all_sources(date_filter=args.date)
    logger.info(f"Loaded {len(rows):,} rows from {len(set(r.get('_source_file','') for r in rows))} sources")

    # Separate inference rows from GPU cloud rows
    inference_rows = [r for r in rows if r.get("pricing_type", "").lower() == "inference"]
    gpu_rows = [r for r in rows if r.get("pricing_type", "").lower() != "inference"]

    logger.info(f"Separated: {len(gpu_rows):,} GPU cloud rows, {len(inference_rows):,} inference rows")

    # Unify GPU cloud rows (with deduplication logic)
    logger.info("Running unification for GPU cloud data...")
    unified_gpu = unify(gpu_rows, stats=args.stats or True)
    save_master(unified_gpu, path=args.output)

    # For inference data, we also unify but with a simpler approach
    # (no GPU-level dedup since inference has no GPU data)
    logger.info("Running unification for inference data...")
    unified_inference = unify(inference_rows, stats=args.stats or False)
    save_inference(unified_inference, path=args.inference_output)

    # Quick summary for GPU database
    providers = sorted(set(r.get("provider", "") for r in unified_gpu))
    gpu_names = sorted(set(r.get("gpu_name", "") for r in unified_gpu if r.get("gpu_name")))
    dates = sorted(set(r.get("snapshot_date", "") for r in unified_gpu))

    print(f"\n  Master Database Summary (GPU Cloud):")
    print(f"  {'─'*50}")
    print(f"  Total rows:      {len(unified_gpu):>8,}")
    print(f"  Providers:       {len(providers):>8,}")
    print(f"  GPU types:       {len(gpu_names):>8,}")
    print(f"  Date range:      {dates[0] if dates else 'none'} → {dates[-1] if dates else 'none'}")
    print(f"  Output:          {args.output}")

    # Quick summary for inference database
    inf_providers = sorted(set(r.get("provider", "") for r in unified_inference))
    inf_dates = sorted(set(r.get("snapshot_date", "") for r in unified_inference))

    print(f"\n  Inference Database Summary:")
    print(f"  {'─'*50}")
    print(f"  Total rows:      {len(unified_inference):>8,}")
    print(f"  Providers:       {len(inf_providers):>8,}")
    print(f"  Date range:      {inf_dates[0] if inf_dates else 'none'} → {inf_dates[-1] if inf_dates else 'none'}")
    print(f"  Output:          {args.inference_output}")
    print()


if __name__ == "__main__":
    main()
