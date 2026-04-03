"""
Base collector class. All source collectors inherit from this.
"""

import csv
import glob
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from schema import COLUMNS, normalize_pricing_type, normalize_gpu_variant

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
RETENTION_DAYS = 90


class BaseCollector:
    """
    Base class for all pricing data collectors.

    Subclasses must implement:
        name: str           — unique source identifier (e.g. "aws", "azure")
        collect() -> list   — returns list of row dicts conforming to COLUMNS
    """

    name: str = ""
    requires_api_key: bool = False
    api_key_env_var: str = ""  # e.g. "SHADEFORM_API_KEY"

    def __init__(self):
        self.snapshot_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.snapshot_date = self.snapshot_ts[:10]

    def collect(self) -> List[Dict[str, Any]]:
        """Collect pricing data. Must be implemented by subclass."""
        raise NotImplementedError

    def get_api_key(self) -> Optional[str]:
        """Retrieve API key from environment."""
        if not self.api_key_env_var:
            return None
        key = os.environ.get(self.api_key_env_var, "").strip()
        if not key and self.requires_api_key:
            logger.warning(f"[{self.name}] Missing API key: {self.api_key_env_var}")
        return key or None

    def make_row(self, **kwargs) -> Dict[str, Any]:
        """
        Create a row dict with defaults filled in.
        Caller passes only the fields they know; rest default to empty.
        """
        row = {col: "" for col in COLUMNS}
        row["snapshot_date"] = self.snapshot_date
        row["snapshot_ts"] = self.snapshot_ts
        row["source"] = self.name
        row["currency"] = "USD"
        row["price_unit"] = "hour"
        row.update(kwargs)
        # Normalize terminology across providers
        row["pricing_type"] = normalize_pricing_type(row["pricing_type"])
        row["gpu_variant"] = normalize_gpu_variant(row["gpu_variant"])
        return row

    def save(self, rows: List[Dict[str, Any]]) -> str:
        """
        Append-only: write new rows to CSV without reading existing data.
        Dedup and retention are handled by prune_all_csvs() after all collectors finish.
        """
        if not rows:
            logger.info(f"[{self.name}] No rows to save")
            return ""

        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, f"{self.name}.csv")

        file_exists = os.path.isfile(path) and os.path.getsize(path) > 0

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)

        logger.info(f"[{self.name}] Appended {len(rows)} rows to {path}")
        return path

    def run(self) -> int:
        """Collect and save. Returns row count."""
        rows = self.collect()
        self.save(rows)
        return len(rows)


def prune_all_csvs(archive_path=None):
    """
    Single-pass prune of all source CSVs in DATA_DIR.
    For each CSV:
      1. Dedup exact duplicate timestamps (same snapshot_ts)
      2. Drop rows older than RETENTION_DAYS
      3. Rewrite the CSV with retained rows only
    Expired rows are written to archive_path (if provided) for upload to Releases.
    Skips _master.csv and _inference.csv (generated files).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    total_pruned = 0
    all_expired = []

    for path in sorted(csv_files):
        basename = os.path.basename(path)
        if basename.startswith("_"):
            continue

        rows = []
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                for row in reader:
                    rows.append(row)
        except Exception as e:
            logger.warning(f"[prune] Failed to read {basename}: {e}")
            continue

        if not rows:
            continue

        source = basename.replace(".csv", "")

        # Dedup exact duplicate rows (same snapshot_ts + all fields)
        seen = set()
        unique = []
        for row in rows:
            key = (row.get("snapshot_ts", ""), row.get("provider", ""),
                   row.get("instance_type", ""), row.get("pricing_type", ""),
                   row.get("region", ""), row.get("price_per_hour", ""))
            if key not in seen:
                seen.add(key)
                unique.append(row)
        deduped = len(rows) - len(unique)

        # Separate retained vs expired by date
        retained = []
        expired = []
        for row in unique:
            if row.get("snapshot_date", "") < cutoff:
                expired.append(row)
            else:
                retained.append(row)

        all_expired.extend(expired)

        # Rewrite the source CSV with only retained rows
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in retained:
                writer.writerow(row)

        dropped = deduped + len(expired)
        if dropped:
            total_pruned += dropped
            logger.info(f"[prune] {source}: kept {len(retained)}, "
                        f"expired {len(expired)}, deduped {deduped}")

    # Write all expired rows to a single archive CSV for upload to Releases
    if all_expired and archive_path:
        os.makedirs(os.path.dirname(archive_path), exist_ok=True)
        with open(archive_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in all_expired:
                writer.writerow(row)
        logger.info(f"[prune] Wrote {len(all_expired)} expired rows to {archive_path}")

    logger.info(f"[prune] Done: pruned {total_pruned} rows total, "
                f"{len(all_expired)} expired rows archived")
