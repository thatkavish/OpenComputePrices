"""
Base collector class. All source collectors inherit from this.
"""

import csv
import json
import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from schema import COLUMNS

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


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
        key = os.environ.get(self.api_key_env_var, "")
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
        return row

    def save(self, rows: List[Dict[str, Any]]) -> str:
        """
        Append rows to the source's CSV file.
        Returns the file path written.
        """
        if not rows:
            logger.info(f"[{self.name}] No rows to save")
            return ""

        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, f"{self.name}.csv")
        file_exists = os.path.isfile(path)

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)

        logger.info(f"[{self.name}] Saved {len(rows)} rows to {path}")
        return path

    def run(self) -> int:
        """Collect and save. Returns row count."""
        try:
            rows = self.collect()
            self.save(rows)
            return len(rows)
        except Exception as e:
            logger.error(f"[{self.name}] Collection failed: {e}", exc_info=True)
            return 0
