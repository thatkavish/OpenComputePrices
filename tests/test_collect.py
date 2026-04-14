import argparse
import csv
import gzip
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

import collect
from schema import COLUMNS


class _BoomCollector:
    requires_api_key = False
    api_key_env_var = ""

    def run(self):
        raise RuntimeError("boom")


class _ApiKeyCollector:
    requires_api_key = True
    api_key_env_var = "FAKE_API_KEY"

    def get_api_key(self):
        return None


class CollectTests(unittest.TestCase):
    @staticmethod
    def _row(**overrides):
        row = {col: "" for col in COLUMNS}
        row.update({
            "snapshot_date": "2099-01-01",
            "snapshot_ts": "2099-01-01T00:00:00Z",
            "source": "aws",
            "provider": "aws",
            "instance_type": "p5.48xlarge",
            "instance_family": "p5",
            "gpu_name": "H100",
            "gpu_count": "8",
            "region": "us-east-1",
            "pricing_type": "on_demand",
            "price_per_hour": "10.0",
            "price_per_gpu_hour": "1.25",
            "currency": "USD",
            "price_unit": "hour",
            "os": "Linux",
            "available": "True",
        })
        row.update(overrides)
        return row

    def test_resolve_collector_names_supports_browser_and_non_browser_filters(self):
        args = argparse.Namespace(
            sources=["aws", "coreweave", "runpod"],
            sources_csv="",
            browser=False,
            no_auth_only=False,
            no_browser=True,
            skip=[],
        )
        self.assertEqual(collect.resolve_collector_names(args), ["aws", "runpod"])

        args.browser = True
        args.no_browser = False
        self.assertEqual(collect.resolve_collector_names(args), ["coreweave"])

        args.browser = False
        args.no_auth_only = True
        self.assertEqual(collect.resolve_collector_names(args), ["aws"])

    def test_sources_csv_is_merged_with_positional_sources(self):
        args = argparse.Namespace(
            sources=["coreweave"],
            sources_csv="aws, azure ,",
            browser=False,
            no_auth_only=False,
            no_browser=False,
            skip=[],
        )
        self.assertEqual(collect.resolve_collector_names(args), ["aws", "azure", "coreweave"])

    def test_main_exits_non_zero_when_all_collectors_fail(self):
        with mock.patch.dict(collect.COLLECTORS, {"boom": _BoomCollector}, clear=True), \
             mock.patch.object(collect, "NO_AUTH_COLLECTORS", []), \
             mock.patch.object(collect, "BROWSER_COLLECTORS", []), \
             mock.patch.object(collect, "API_KEY_COLLECTORS", []), \
             mock.patch("sys.argv", ["collect.py", "boom", "--no-unify"]), \
             redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as exc:
                collect.main()

        self.assertEqual(exc.exception.code, 1)

    def test_required_api_key_collectors_are_skipped_without_secret(self):
        with mock.patch.dict(collect.COLLECTORS, {"secured": _ApiKeyCollector}, clear=True), \
             mock.patch.object(collect, "NO_AUTH_COLLECTORS", []), \
             mock.patch.object(collect, "BROWSER_COLLECTORS", []), \
             mock.patch.object(collect, "API_KEY_COLLECTORS", ["secured"]), \
             mock.patch("sys.argv", ["collect.py", "secured", "--no-unify"]), \
             redirect_stdout(io.StringIO()):
            collect.main()

    def test_finalize_only_runs_without_collectors(self):
        with mock.patch.object(collect, "finalize_existing_data") as finalize_mock, \
             mock.patch("sys.argv", ["collect.py", "--finalize-only"]), \
             redirect_stdout(io.StringIO()):
            collect.main()

        finalize_mock.assert_called_once_with(skip_prune=False, no_unify=False)

    def test_repair_shifted_tail_row_restores_currency_and_availability_fields(self):
        row = self._row(
            upfront_price="",
            upfront_price_per_gpu="",
            currency="0.0",
            price_unit="0.0",
            available="USD",
            available_count="hour",
            os="True",
            tenancy="",
            pre_installed_sw="Linux",
            raw_extra="Dedicated",
        )

        self.assertTrue(collect._repair_shifted_tail_row(row))
        self.assertEqual(row["upfront_price"], "0.0")
        self.assertEqual(row["upfront_price_per_gpu"], "0.0")
        self.assertEqual(row["currency"], "USD")
        self.assertEqual(row["price_unit"], "hour")
        self.assertEqual(row["available"], "True")
        self.assertEqual(row["available_count"], "")
        self.assertEqual(row["os"], "Linux")
        self.assertEqual(row["tenancy"], "Dedicated")
        self.assertEqual(row["pre_installed_sw"], "")
        self.assertEqual(row["raw_extra"], "")

    def test_normalize_existing_row_repairs_azure_h100_v5_gpu_count(self):
        row = self._row(
            source="azure",
            provider="azure",
            instance_type="Standard_NC40ads_H100_v5",
            gpu_name="H100",
            gpu_count="8",
            gpu_memory_gb="80",
            gpu_variant="SXM5",
            price_per_hour="6.98",
            price_per_gpu_hour="0.8725",
        )

        self.assertTrue(collect._normalize_existing_row(row))
        self.assertEqual(row["gpu_count"], 1)
        self.assertEqual(row["gpu_memory_gb"], 94)
        self.assertEqual(row["gpu_variant"], "NVL")
        self.assertEqual(row["price_per_gpu_hour"], "6.98")

    def test_normalize_existing_row_marks_impossible_azure_hourly_price_as_reserved(self):
        row = self._row(
            source="azure",
            provider="azure",
            instance_type="Standard_NC48ads_A100_v4",
            gpu_name="A100",
            gpu_count="2",
            price_per_hour="42066.0",
            price_per_gpu_hour="21033.0",
            upfront_price="",
            upfront_price_per_gpu="",
        )

        self.assertTrue(collect._normalize_existing_row(row))
        self.assertEqual(row["pricing_type"], "reserved")
        self.assertEqual(row["commitment_period"], "unknown")
        self.assertEqual(row["price_per_hour"], "0")
        self.assertEqual(row["price_per_gpu_hour"], "0")
        self.assertEqual(row["upfront_price"], "42066.0")
        self.assertEqual(row["upfront_price_per_gpu"], "21033")

    def test_normalize_existing_row_repairs_aws_capacity_block(self):
        row = self._row(
            source="aws",
            provider="aws",
            gpu_name="H100",
            gpu_memory_gb="H100",
            price_per_hour="0.125",
            price_per_gpu_hour="0.015625",
            raw_extra='{"capacity_status":"Used","location":"US East (N. Virginia)"}',
        )

        self.assertTrue(collect._normalize_existing_row(row))
        self.assertEqual(row["pricing_type"], "reserved")
        self.assertEqual(row["commitment_period"], "capacity_block")
        self.assertEqual(row["gpu_memory_gb"], 80)
        self.assertIn('"purchase_option":"capacity_block"', row["raw_extra"])

    def test_should_keep_existing_row_drops_implausible_akash_gtx1070ti_outlier(self):
        row = self._row(
            source="akash",
            provider="akash",
            instance_type="gtx1070ti_PCIe_avg",
            gpu_name="GTX 1070 Ti",
            price_per_hour="55.83",
            price_per_gpu_hour="55.83",
        )

        self.assertFalse(collect._should_keep_existing_row(row))

    def test_incremental_finalize_rebuilds_only_affected_snapshot_dates(self):
        preserved_master_row = self._row(
            snapshot_date="2098-12-31",
            snapshot_ts="2098-12-31T00:00:00Z",
            raw_extra='{"marker":"preserved"}',
        )
        old_shadeform_row = self._row(
            source="shadeform",
            snapshot_date="2099-01-01",
            snapshot_ts="2099-01-01T00:00:00Z",
            price_per_hour="12.0",
            price_per_gpu_hour="1.5",
            raw_extra='{"marker":"old-shadeform"}',
        )
        old_master_same_date = self._row(
            source="shadeform",
            snapshot_date="2099-01-01",
            snapshot_ts="2099-01-01T00:00:00Z",
            price_per_hour="12.0",
            price_per_gpu_hour="1.5",
            raw_extra='{"marker":"replace-me"}',
        )
        new_aws_row = self._row(
            snapshot_date="2099-01-01",
            snapshot_ts="2099-01-01T12:00:00Z",
            price_per_hour="11.0",
            price_per_gpu_hour="1.375",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            aws_path = os.path.join(tmpdir, "aws.csv")
            with open(aws_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writeheader()

            baseline_size = os.path.getsize(aws_path)

            with open(aws_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writerow(new_aws_row)

            shadeform_path = os.path.join(tmpdir, "shadeform.csv")
            with open(shadeform_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writeheader()
                writer.writerow(old_shadeform_row)

            master_path = os.path.join(tmpdir, "_master.csv")
            with open(master_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writeheader()
                writer.writerow(preserved_master_row)
                writer.writerow(old_master_same_date)

            with open(os.path.join(tmpdir, "_baseline_state.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {"sources": {
                        "aws.csv": {"size": baseline_size},
                        "shadeform.csv": {"size": os.path.getsize(shadeform_path)},
                    }},
                    f,
                )

            with mock.patch("collectors.base.DATA_DIR", tmpdir):
                collect.finalize_existing_data()

            with open(master_path, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["raw_extra"], '{"marker":"preserved"}')
            self.assertEqual(rows[1]["source"], "aws")
            self.assertEqual(rows[1]["price_per_hour"], "11.0")
            self.assertEqual(rows[1]["snapshot_date"], "2099-01-01")
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "_baseline_state.json")))

            dashboard_path = os.path.join(tmpdir, "dashboard_gpu_daily.json.gz")
            self.assertTrue(os.path.isfile(dashboard_path))
            with gzip.open(dashboard_path, "rt", encoding="utf-8") as f:
                dashboard = json.load(f)

            self.assertEqual(dashboard["coverage"]["max_date"], "2099-01-01")
            self.assertEqual(dashboard["coverage"]["latest_pricing_rows"], 1)
