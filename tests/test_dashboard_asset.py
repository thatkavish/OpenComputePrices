import csv
import gzip
import json
import os
import tempfile
import unittest

from dashboard_asset import SCHEMA_VERSION, build_dashboard_asset, build_dashboard_asset_from_master
from schema import COLUMNS


class DashboardAssetTests(unittest.TestCase):
    @staticmethod
    def _row(**overrides):
        row = {col: "" for col in COLUMNS}
        row.update({
            "snapshot_date": "2026-04-01",
            "snapshot_ts": "2026-04-01T00:00:00Z",
            "source": "aws",
            "provider": "aws",
            "instance_type": "p5.48xlarge",
            "gpu_name": "H100",
            "gpu_memory_gb": "80",
            "gpu_count": "8",
            "region": "us-east-1",
            "pricing_type": "on_demand",
            "price_per_hour": "16.0",
            "price_per_gpu_hour": "2.0",
            "currency": "USD",
            "price_unit": "hour",
            "os": "Linux",
            "available": "True",
        })
        row.update(overrides)
        return row

    def test_build_dashboard_asset_generates_expected_dashboard_payload(self):
        rows = [
            self._row(
                snapshot_date="2026-03-01",
                snapshot_ts="2026-03-01T00:00:00Z",
                price_per_hour="8.0",
                price_per_gpu_hour="1.0",
            ),
            self._row(
                snapshot_date="2026-03-01",
                snapshot_ts="2026-03-01T00:00:00Z",
                source="azure",
                provider="azure",
                instance_type="Standard_NC40ads_H100_v5",
                gpu_count="1",
                price_per_hour="2.0",
                price_per_gpu_hour="2.0",
                region="westeurope",
            ),
            self._row(
                snapshot_date="2026-03-01",
                snapshot_ts="2026-03-01T00:00:00Z",
                source="runpod",
                provider="runpod",
                instance_type="runpod-a100",
                gpu_name="A100",
                gpu_memory_gb="80",
                gpu_count="1",
                price_per_hour="0.4",
                price_per_gpu_hour="0.4",
                region="us-central-1",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                price_per_hour="16.0",
                price_per_gpu_hour="2.0",
                os="Linux",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                price_per_hour="32.0",
                price_per_gpu_hour="4.0",
                os="Windows",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                instance_type="p5e.48xlarge",
                price_per_hour="48.0",
                price_per_gpu_hour="6.0",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                pricing_type="spot",
                price_per_hour="10.64",
                price_per_gpu_hour="1.33",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="azure",
                provider="azure",
                instance_type="Standard_NC40ads_H100_v5",
                gpu_count="1",
                price_per_hour="3.0",
                price_per_gpu_hour="3.0",
                region="westeurope",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="runpod",
                provider="runpod",
                instance_type="runpod-a100",
                gpu_name="A100",
                gpu_memory_gb="80",
                gpu_count="1",
                price_per_hour="0.8",
                price_per_gpu_hour="0.8",
                region="us-central-1",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="lambda",
                provider="lambda",
                instance_type="gpu_1x_rtxa5000",
                gpu_name="RTX A5000",
                gpu_memory_gb="",
                gpu_count="1",
                price_per_hour="4.5",
                price_per_gpu_hour="4.5",
                region="moon-1",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                pricing_type="reserved",
                commitment_period="1yr",
                price_per_hour="8.0",
                price_per_gpu_hour="1.0",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="aws",
                provider="aws",
                instance_type="trn1.32xlarge",
                gpu_name="Trainium",
                gpu_count="16",
                price_per_hour="12.0",
                price_per_gpu_hour="0.75",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="vultr",
                provider="vultr",
                instance_type="vcg-a100-1c-40gb",
                gpu_name="A100",
                gpu_memory_gb="40",
                gpu_count="1",
                price_per_hour="0.5",
                price_per_gpu_hour="0.5",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="vastai",
                provider="vastai",
                instance_type="cheapest_A100",
                gpu_name="A100",
                gpu_memory_gb="80",
                gpu_count="1",
                region="global",
                price_per_hour="0.6",
                price_per_gpu_hour="0.6",
            ),
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                source="gcp",
                provider="gcp",
                instance_type="a3-highgpu-1g",
                gpu_count="1",
                price_per_hour="0.1",
                price_per_gpu_hour="0.1",
                region="us-central1",
            ),
        ]

        asset = build_dashboard_asset(
            rows,
            generated_at="2026-04-14T04:52:07Z",
            release_tag="latest-data",
            release_name="GPU Pricing Data — 2026-04-14 02:15 UTC",
            release_updated_at="2026-04-14T04:52:07Z",
        )

        self.assertEqual(asset["schema_version"], SCHEMA_VERSION)
        self.assertEqual(asset["coverage"]["min_date"], "2026-03-01")
        self.assertEqual(asset["coverage"]["max_date"], "2026-04-01")
        self.assertEqual(asset["coverage"]["latest_pricing_date"], "2026-04-01")
        self.assertEqual(asset["coverage"]["chart_gpu_count"], 3)
        self.assertEqual(asset["coverage"]["latest_pricing_rows"], 4)
        self.assertEqual(asset["quality_summary"], {
            "excluded_reserved_or_commitment_rows": 1,
            "excluded_non_gpu_rows": 1,
            "excluded_fractional_or_slice_rows": 1,
            "excluded_aggregate_offer_rows": 1,
            "excluded_price_sanity_rows": 1,
        })
        self.assertEqual(asset["chart"]["default_selected_gpu_slugs"], ["a100", "h100", "rtx-a5000"])
        self.assertEqual(asset["chart"]["dates"][0], "2026-03-01")
        self.assertEqual(asset["chart"]["dates"][-1], "2026-04-01")
        self.assertEqual(len(asset["chart"]["dates"]), 32)

        series_by_gpu = {series["gpu_slug"]: series for series in asset["chart"]["series"]}
        self.assertEqual(series_by_gpu["h100"]["values"][0], 1.5)
        self.assertIsNone(series_by_gpu["h100"]["values"][1])
        self.assertEqual(series_by_gpu["h100"]["values"][-1], 3.5)
        self.assertEqual(series_by_gpu["a100"]["values"][0], 0.4)
        self.assertEqual(series_by_gpu["a100"]["values"][-1], 0.8)
        self.assertIsNone(series_by_gpu["rtx-a5000"]["values"][0])
        self.assertEqual(series_by_gpu["rtx-a5000"]["values"][-1], 4.5)

        latest_by_key = {
            (row["provider_slug"], row["gpu_slug"]): row
            for row in asset["latest_pricing"]
        }
        self.assertEqual(latest_by_key[("aws", "h100")]["price_per_hr"], 2.0)
        self.assertEqual(latest_by_key[("aws", "h100")]["spot_per_hr"], 1.33)
        self.assertEqual(latest_by_key[("aws", "h100")]["change_30d_pct"], 133.3)
        self.assertEqual(latest_by_key[("azure", "h100")]["provider_label"], "Microsoft Azure")
        self.assertEqual(latest_by_key[("runpod", "a100")]["change_30d_pct"], 100.0)
        self.assertEqual(
            latest_by_key[("lambda", "rtx-a5000")]["quality_flags"],
            ["unknown_region", "missing_vram"],
        )
        self.assertEqual(latest_by_key[("lambda", "rtx-a5000")]["gpu_category"], "graphics_card")

        catalog_by_gpu = {row["gpu_slug"]: row for row in asset["gpu_catalog"]}
        self.assertEqual(catalog_by_gpu["a100"]["chart_rank"], 1)
        self.assertEqual(catalog_by_gpu["h100"]["chart_rank"], 2)
        self.assertEqual(catalog_by_gpu["rtx-a5000"]["chart_rank"], 3)

    def test_build_dashboard_asset_from_master_writes_valid_gzip_json(self):
        rows = [
            self._row(
                snapshot_date="2026-04-01",
                snapshot_ts="2026-04-01T12:00:00Z",
                price_per_hour="16.0",
                price_per_gpu_hour="2.0",
            ),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            master_path = os.path.join(temp_dir, "_master.csv")
            output_path = os.path.join(temp_dir, "dashboard_gpu_daily.json.gz")
            with open(master_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

            build_dashboard_asset_from_master(
                master_csv_path=master_path,
                output_path=output_path,
                generated_at="2026-04-14T04:52:07Z",
                release_name="GPU Pricing Data — 2026-04-14 02:15 UTC",
                release_updated_at="2026-04-14T04:52:07Z",
            )

            with gzip.open(output_path, "rt", encoding="utf-8") as f:
                payload = json.load(f)

        self.assertEqual(payload["schema_version"], SCHEMA_VERSION)
        self.assertEqual(payload["coverage"]["latest_pricing_rows"], 1)
        self.assertEqual(payload["latest_pricing"][0]["gpu_slug"], "h100")
