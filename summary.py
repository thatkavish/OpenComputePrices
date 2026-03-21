#!/usr/bin/env python3
"""
Quick dataset summary — shows what's in the data/ directory.

Usage:
    python summary.py              # Full summary
    python summary.py aws          # Summary for one source
"""

import csv
import os
import sys
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def summarize_file(path: str, name: str):
    """Print summary stats for a single CSV file."""
    rows = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        print(f"  {name}: empty")
        return 0

    dates = sorted(set(r.get("snapshot_date", "") for r in rows if r.get("snapshot_date")))
    providers = sorted(set(r.get("provider", "") for r in rows if r.get("provider")))
    gpus = sorted(set(r.get("gpu_name", "") for r in rows if r.get("gpu_name")))
    regions = set(r.get("region", "") for r in rows if r.get("region"))
    pricing_types = sorted(set(r.get("pricing_type", "") for r in rows if r.get("pricing_type")))

    print(f"\n  {'='*60}")
    print(f"  {name} — {len(rows):,} rows")
    print(f"  {'='*60}")
    print(f"  Dates:          {len(dates)} ({dates[0]} → {dates[-1]})" if dates else "  Dates: none")
    print(f"  Providers:      {', '.join(providers[:10])}" + (" ..." if len(providers) > 10 else ""))
    print(f"  GPU types:      {len(gpus)} — {', '.join(gpus[:15])}" + (" ..." if len(gpus) > 15 else ""))
    print(f"  Regions:        {len(regions)}")
    print(f"  Pricing types:  {', '.join(pricing_types)}")

    # Per-GPU breakdown
    gpu_counts = defaultdict(int)
    for r in rows:
        gn = r.get("gpu_name", "") or "(unknown)"
        gpu_counts[gn] += 1

    print(f"\n  By GPU:")
    for gpu, count in sorted(gpu_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"    {gpu:<25} {count:>8,} rows")

    # Per-date breakdown
    if len(dates) > 1:
        print(f"\n  By date:")
        date_counts = defaultdict(int)
        for r in rows:
            date_counts[r.get("snapshot_date", "")] += 1
        for d in sorted(date_counts.keys()):
            print(f"    {d}  {date_counts[d]:>8,} rows")

    return len(rows)


def main():
    sources = sys.argv[1:] if len(sys.argv) > 1 else None

    csv_files = sorted(f for f in os.listdir(DATA_DIR) if f.endswith(".csv"))
    if sources:
        csv_files = [f for f in csv_files if f.replace(".csv", "") in sources]

    if not csv_files:
        print("No data files found.")
        return

    total = 0
    for fname in csv_files:
        path = os.path.join(DATA_DIR, fname)
        name = fname.replace(".csv", "")
        count = summarize_file(path, name)
        total += count

    print(f"\n  {'='*60}")
    print(f"  TOTAL: {total:,} rows across {len(csv_files)} sources")
    print(f"  {'='*60}\n")


if __name__ == "__main__":
    main()
