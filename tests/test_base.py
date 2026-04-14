import csv
import os
import tempfile
import unittest

from collectors.base import BaseCollector, migrate_csv_to_current_schema
from schema import COLUMNS


class BaseCollectorTests(unittest.TestCase):
    def test_should_save_row_keeps_inference_and_drops_blank_gpu_compute_rows(self):
        self.assertFalse(BaseCollector.should_save_row({"pricing_type": "on_demand", "gpu_name": ""}))
        self.assertTrue(BaseCollector.should_save_row({"pricing_type": "inference", "gpu_name": ""}))
        self.assertTrue(BaseCollector.should_save_row({"pricing_type": "on_demand", "gpu_name": "Inferentia"}))

    def test_migrate_csv_to_current_schema_preserves_current_rows_under_stale_header(self):
        old_columns = [col for col in COLUMNS if col not in {"upfront_price", "upfront_price_per_gpu"}]
        current_row = {col: "" for col in COLUMNS}
        current_row.update({
            "snapshot_date": "2026-04-06",
            "snapshot_ts": "2026-04-06T00:00:00Z",
            "source": "aws",
            "provider": "aws",
            "price_per_hour": "1.24",
            "price_per_gpu_hour": "1.24",
            "upfront_price": "0.0",
            "upfront_price_per_gpu": "0.0",
            "currency": "USD",
            "price_unit": "hour",
            "available": "True",
            "os": "Linux",
            "tenancy": "Dedicated",
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "aws.csv")
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(old_columns)
                writer.writerow([current_row[col] for col in COLUMNS])

            self.assertTrue(migrate_csv_to_current_schema(path))

            with open(path, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(rows[0]["upfront_price"], "0.0")
        self.assertEqual(rows[0]["upfront_price_per_gpu"], "0.0")
        self.assertEqual(rows[0]["currency"], "USD")
        self.assertEqual(rows[0]["price_unit"], "hour")
        self.assertEqual(rows[0]["available"], "True")
        self.assertEqual(rows[0]["os"], "Linux")
        self.assertEqual(rows[0]["tenancy"], "Dedicated")
