#!/usr/bin/env python3
"""
Backfill historical SkyPilot pricing data with correct per-commit dates.

Clones the SkyPilot Catalog repo and walks git commits to extract
pricing snapshots, each stamped with the actual commit date.

Usage:
    python backfill_skypilot.py                    # Extract all history
    python backfill_skypilot.py --since 2025-01-01 # From a specific date
    python backfill_skypilot.py --fix-archive      # Also fix the release archive
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Dict, Any

from schema import COLUMNS, normalize_gpu_name, infer_geo_group

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CLONE_DIR = "/tmp/skypilot-catalog-backfill"

CATALOG_PATHS = {
    "aws":        "catalogs/v6/aws/vms.csv",
    "azure":      "catalogs/v6/azure/vms.csv",
    "gcp":        "catalogs/v6/gcp/vms.csv",
    "lambda":     "catalogs/v6/lambda/vms.csv",
    "runpod":     "catalogs/v6/runpod/vms.csv",
    "fluidstack": "catalogs/v6/fluidstack/vms.csv",
    "vastai":     "catalogs/v6/vast/vms.csv",
    "cudo":       "catalogs/v6/cudo/vms.csv",
    "paperspace": "catalogs/v6/paperspace/vms.csv",
    "nebius":     "catalogs/v6/nebius/vms.csv",
    "oci":        "catalogs/v6/oci/vms.csv",
    "hyperstack": "catalogs/v6/hyperstack/vms.csv",
    "ibm":        "catalogs/v6/ibm/vms.csv",
    "scaleway":   "catalogs/v6/scaleway/vms.csv",
    "do":         "catalogs/v6/do/vms.csv",
}

# Also check v5 paths (older commits may not have v6)
CATALOG_PATHS_V5 = {k: v.replace("/v6/", "/v5/") for k, v in CATALOG_PATHS.items()}


def clone_repo():
    """Clone SkyPilot catalog with full history (blobless for speed)."""
    if os.path.isdir(os.path.join(CLONE_DIR, ".git")):
        logger.info("Repo already cloned, pulling latest...")
        subprocess.run(["git", "pull"], cwd=CLONE_DIR, capture_output=True)
        return
    logger.info("Cloning SkyPilot catalog (blobless)...")
    subprocess.run([
        "git", "clone", "--filter=blob:none",
        "https://github.com/skypilot-org/skypilot-catalog.git",
        CLONE_DIR,
    ], check=True)
    logger.info("Clone complete")


def get_commit_dates(since: str = "") -> List[Dict[str, str]]:
    """Get all commits with dates, one per day (deduplicated)."""
    cmd = ["git", "log", "--format=%H %aI", "--reverse"]
    if since:
        cmd.append(f"--since={since}")
    result = subprocess.run(cmd, cwd=CLONE_DIR, capture_output=True, text=True)
    
    commits = []
    seen_dates = set()
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.strip().split(" ", 1)
        if len(parts) != 2:
            continue
        sha, date_str = parts
        date = date_str[:10]  # YYYY-MM-DD
        if date not in seen_dates:
            seen_dates.add(date)
            commits.append({"sha": sha, "date": date, "ts": date_str})
    
    return commits


def get_file_at_commit(sha: str, path: str) -> str:
    """Get file contents at a specific commit."""
    result = subprocess.run(
        ["git", "show", f"{sha}:{path}"],
        cwd=CLONE_DIR, capture_output=True, text=True
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def parse_catalog_csv(text: str, cloud: str, snapshot_date: str, snapshot_ts: str) -> List[Dict]:
    """Parse a SkyPilot catalog CSV into schema-conformant rows."""
    if not text.strip():
        return []
    
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    
    for record in reader:
        gpu_name = record.get("AcceleratorName", "") or record.get("accelerator_name", "")
        if not gpu_name:
            continue
        
        price_str = record.get("Price", "") or record.get("price", "")
        spot_str = record.get("SpotPrice", "") or record.get("spot_price", "")
        instance_type = record.get("InstanceType", "") or record.get("instance_type", "")
        region = record.get("Region", "") or record.get("region", "")
        zone = record.get("AvailabilityZone", "") or record.get("zone", "")
        vcpus = record.get("vCPUs", "") or record.get("cpus", "")
        ram = record.get("MemoryGiB", "") or record.get("memory", "")
        gpu_count = record.get("AcceleratorCount", "") or record.get("accelerator_count", "")
        gpu_mem = record.get("GpuInfo", "") or record.get("accelerator_memory", "")
        
        try:
            gpu_count_int = int(float(gpu_count)) if gpu_count else 0
        except (ValueError, TypeError):
            gpu_count_int = 0
        
        base = {col: "" for col in COLUMNS}
        base["snapshot_date"] = snapshot_date
        base["snapshot_ts"] = snapshot_ts
        base["source"] = "skypilot"
        base["provider"] = cloud
        base["instance_type"] = instance_type
        base["gpu_name"] = normalize_gpu_name(gpu_name)
        base["gpu_memory_gb"] = gpu_mem
        base["gpu_count"] = gpu_count_int
        base["vcpus"] = vcpus
        base["ram_gb"] = ram
        base["region"] = region
        base["zone"] = zone
        base["geo_group"] = infer_geo_group(region)
        base["currency"] = "USD"
        base["price_unit"] = "hour"
        base["available"] = True
        
        if price_str:
            try:
                price = float(price_str)
                if price > 0:
                    ppg = price / gpu_count_int if gpu_count_int > 0 else price
                    row = dict(base)
                    row["pricing_type"] = "on_demand"
                    row["price_per_hour"] = round(price, 6)
                    row["price_per_gpu_hour"] = round(ppg, 6)
                    rows.append(row)
            except (ValueError, TypeError):
                pass
        
        if spot_str:
            try:
                spot = float(spot_str)
                if spot > 0:
                    spg = spot / gpu_count_int if gpu_count_int > 0 else spot
                    row = dict(base)
                    row["pricing_type"] = "spot"
                    row["price_per_hour"] = round(spot, 6)
                    row["price_per_gpu_hour"] = round(spg, 6)
                    rows.append(row)
            except (ValueError, TypeError):
                pass
    
    return rows


def backfill(since: str = "", sample_interval: int = 1) -> List[Dict]:
    """
    Walk SkyPilot git history and extract pricing at each commit date.
    
    Args:
        since: Only process commits after this date (YYYY-MM-DD)
        sample_interval: Process every Nth day (1=daily, 7=weekly)
    """
    clone_repo()
    commits = get_commit_dates(since=since)
    logger.info(f"Found {len(commits)} daily commits" + (f" since {since}" if since else ""))
    
    if sample_interval > 1:
        commits = commits[::sample_interval]
        logger.info(f"Sampling every {sample_interval} days → {len(commits)} commits")
    
    all_rows = []
    
    for i, commit in enumerate(commits):
        sha = commit["sha"]
        date = commit["date"]
        ts = commit["ts"]
        
        commit_rows = 0
        for cloud, path in CATALOG_PATHS.items():
            text = get_file_at_commit(sha, path)
            if not text:
                # Try v5 path for older commits
                text = get_file_at_commit(sha, CATALOG_PATHS_V5.get(cloud, ""))
            if not text:
                continue
            
            rows = parse_catalog_csv(text, cloud, date, ts)
            all_rows.extend(rows)
            commit_rows += len(rows)
        
        if (i + 1) % 10 == 0 or i == len(commits) - 1:
            logger.info(f"  [{i+1}/{len(commits)}] {date}: {commit_rows} GPU rows ({len(all_rows)} total)")
    
    logger.info(f"Backfill complete: {len(all_rows)} total rows across {len(commits)} dates")
    return all_rows


def fix_existing_data(backfilled: List[Dict]):
    """
    Replace wrongly-dated rows in skypilot.csv with correctly-dated backfill data.
    Specifically targets rows that have snapshot_date = the import date 
    but should have had proper historical dates.
    """
    skypilot_path = os.path.join(DATA_DIR, "skypilot.csv")
    if not os.path.isfile(skypilot_path):
        logger.warning("skypilot.csv not found")
        return
    
    # Load existing data
    with open(skypilot_path, "r", newline="", encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    
    # Find the "bad" dates — dates where all rows have a single timestamp
    # that doesn't match the range of dates we'd expect
    from collections import Counter
    date_ts = defaultdict(set)
    for r in existing:
        date_ts[r["snapshot_date"]].add(r["snapshot_ts"])
    
    # Build set of dates covered by backfill
    backfill_dates = set(r["snapshot_date"] for r in backfilled)
    
    # Keep existing rows that are NOT from dates covered by backfill
    # (to avoid duplicating data that was correctly dated)
    kept = [r for r in existing if r["snapshot_date"] not in backfill_dates]
    removed = len(existing) - len(kept)
    
    # Add backfill data
    kept.extend(backfilled)
    
    # Sort by date
    kept.sort(key=lambda r: (r.get("snapshot_date", ""), r.get("provider", "")))
    
    # Write back
    with open(skypilot_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in kept:
            writer.writerow(row)
    
    logger.info(f"Fixed skypilot.csv: removed {removed} old rows, "
                f"added {len(backfilled)} backfill rows → {len(kept)} total")


def main():
    parser = argparse.ArgumentParser(description="Backfill SkyPilot historical data with correct dates")
    parser.add_argument("--since", default="2025-09-01", help="Start date (default: 2025-09-01)")
    parser.add_argument("--interval", type=int, default=1, help="Sample every N days (default: 1 = daily)")
    parser.add_argument("--fix-local", action="store_true", help="Replace wrongly-dated rows in skypilot.csv")
    parser.add_argument("--output", default="", help="Write backfill to a separate CSV instead of fixing in-place")
    args = parser.parse_args()
    
    rows = backfill(since=args.since, sample_interval=args.interval)
    
    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        logger.info(f"Wrote {len(rows)} rows to {args.output}")
    
    if args.fix_local:
        fix_existing_data(rows)
    
    if not args.output and not args.fix_local:
        # Just print stats
        dates = sorted(set(r["snapshot_date"] for r in rows))
        providers = sorted(set(r["provider"] for r in rows))
        gpus = sorted(set(r["gpu_name"] for r in rows))
        print(f"\n  Backfill Summary:")
        print(f"  {'─'*50}")
        print(f"  Rows:       {len(rows):>8,}")
        print(f"  Dates:      {len(dates):>8} ({dates[0]} → {dates[-1]})")
        print(f"  Providers:  {len(providers):>8} ({', '.join(providers[:8])}...)")
        print(f"  GPU types:  {len(gpus):>8}")
        print(f"\n  To fix local data:    python backfill_skypilot.py --fix-local")
        print(f"  To write to file:     python backfill_skypilot.py --output backfill.csv")


if __name__ == "__main__":
    main()
