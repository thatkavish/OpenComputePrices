#!/usr/bin/env python3
"""
Rebuild the release archive with correctly-dated historical data.

Separates skypilot.csv into retained (last 90 days) and archived (older),
then uploads the corrected archive to the GitHub release.
"""

import csv
import gzip
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from collections import Counter

from schema import COLUMNS

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RETENTION_DAYS = 90


def main():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    print(f"90-day cutoff: {cutoff}")

    # Process all CSVs
    all_expired = []
    total_retained = 0

    for fname in sorted(os.listdir(DATA_DIR)):
        if not fname.endswith(".csv") or fname.startswith("_"):
            continue
        path = os.path.join(DATA_DIR, fname)

        with open(path, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if not rows:
            continue

        expired = [r for r in rows if r.get("snapshot_date", "") < cutoff]
        retained = [r for r in rows if r.get("snapshot_date", "") >= cutoff]

        if expired:
            all_expired.extend(expired)
            # Rewrite the source CSV with only retained rows
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writeheader()
                for row in retained:
                    writer.writerow(row)
            dates = sorted(set(r["snapshot_date"] for r in expired))
            print(f"  {fname}: archived {len(expired):,} rows ({dates[0]}→{dates[-1]}), kept {len(retained):,}")

        total_retained += len(retained)

    if not all_expired:
        print("No expired rows to archive.")
        return

    # Sort expired rows
    all_expired.sort(key=lambda r: (r.get("snapshot_date", ""), r.get("source", ""), r.get("provider", "")))

    # Verify dates are correct
    dates = sorted(set(r["snapshot_date"] for r in all_expired))
    sources = Counter(r.get("source", "") for r in all_expired)
    print(f"\nArchive summary:")
    print(f"  Rows:    {len(all_expired):,}")
    print(f"  Dates:   {len(dates)} ({dates[0]} → {dates[-1]})")
    print(f"  Sources: {dict(sources)}")

    # Check for the old bug — all same date
    if len(dates) == 1:
        print(f"\n  WARNING: All rows have the same date ({dates[0]}) — archive may still be wrong!")
        return

    # Write archive CSV
    archive_csv = os.path.join(DATA_DIR, "_expired.csv")
    with open(archive_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in all_expired:
            writer.writerow(row)

    # Also write gzipped version for upload
    archive_gz = "/tmp/archive_historical.csv.gz"
    with gzip.open(archive_gz, "wt", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in all_expired:
            writer.writerow(row)

    gz_size = os.path.getsize(archive_gz)
    print(f"\n  Archive file: {archive_gz} ({gz_size / 1024 / 1024:.1f} MB)")
    print(f"  Retained in data/: {total_retained:,} rows")
    print(f"\n  To upload to release:")
    print(f"    gh release upload latest-data {archive_gz} --clobber")
    print(f"    # Also delete the old bad archive:")
    print(f"    gh release delete-asset latest-data archive_2026-03.csv.gz")


if __name__ == "__main__":
    main()
