"""
Base collector class. All source collectors inherit from this.
"""

import csv
import glob
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from schema import (
    COLUMNS,
    infer_geo_group,
    normalize_gpu_memory_gb,
    normalize_gpu_name,
    normalize_gpu_variant,
    normalize_pricing_type,
    normalize_provider,
    normalize_region,
)

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
RETENTION_DAYS = 90


def migrate_csv_to_current_schema(path: str) -> bool:
    """
    Rewrite a source CSV with the current canonical header if its header is stale.

    Appending rows under an older header can shift tail columns after schema changes.
    When a data row already has the current column count, treat it as a current-schema
    append even if the file header is stale.
    """
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return False

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return False

    if header == COLUMNS:
        return False

    tmp_path = f"{path}.tmp"
    migrated = 0
    with open(path, newline="", encoding="utf-8") as src, open(tmp_path, "w", newline="", encoding="utf-8") as dst:
        reader = csv.reader(src)
        next(reader, None)
        writer = csv.DictWriter(dst, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for values in reader:
            if not any(str(value).strip() for value in values):
                continue
            if len(values) == len(COLUMNS):
                row = dict(zip(COLUMNS, values))
            else:
                row = dict(zip(header, values))
            writer.writerow(row)
            migrated += 1

    os.replace(tmp_path, path)
    logger.info(f"[schema] Migrated {os.path.basename(path)} from {len(header)} to {len(COLUMNS)} columns ({migrated} rows)")
    return True


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
        row["provider"] = normalize_provider(row["provider"])
        row["gpu_name"] = normalize_gpu_name(row["gpu_name"])
        row["gpu_memory_gb"] = normalize_gpu_memory_gb(
            row["gpu_memory_gb"],
            row["gpu_name"],
            row["gpu_count"],
            row["gpu_variant"],
        )
        row["pricing_type"] = normalize_pricing_type(row["pricing_type"])
        row["gpu_variant"] = normalize_gpu_variant(row["gpu_variant"])
        row["region"] = normalize_region(
            row["region"],
            row["provider"],
            row["country"],
            row["raw_extra"],
            row["source"],
        )
        row["geo_group"] = infer_geo_group(row["region"], row["country"])
        return row

    @staticmethod
    def should_save_row(row: Dict[str, Any]) -> bool:
        """Keep inference rows and GPU/accelerator rows with a concrete device name."""
        if str(row.get("pricing_type", "")).lower() == "inference":
            return True
        return bool(str(row.get("gpu_name", "")).strip())

    def save(self, rows: List[Dict[str, Any]]) -> str:
        """
        Append-only: write new rows to CSV without reading existing data.
        Dedup and retention are handled by prune_all_csvs() after all collectors finish.
        """
        if not rows:
            logger.info(f"[{self.name}] No rows to save")
            return ""

        original_count = len(rows)
        rows = [row for row in rows if self.should_save_row(row)]
        dropped = original_count - len(rows)
        if dropped:
            logger.info(f"[{self.name}] Dropped {dropped} rows without a concrete GPU/accelerator name")
        if not rows:
            logger.info(f"[{self.name}] No rows to save")
            return ""

        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, f"{self.name}.csv")

        migrate_csv_to_current_schema(path)
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

        # Dedup exact duplicate rows (same snapshot_ts + all canonical fields)
        seen = set()
        unique = []
        for row in rows:
            key = tuple(row.get(col, "") for col in COLUMNS)
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
