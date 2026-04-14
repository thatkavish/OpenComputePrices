import csv
import io
import os
import tempfile
import unittest
from unittest import mock

from collectors.aws import AWSCollector
from collectors.base import prune_all_csvs
from schema import COLUMNS


class _FakeHTTPResponse:
    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _build_aws_csv(rows):
    header = [
        "SKU",
        "OfferTermCode",
        "RateCode",
        "TermType",
        "PriceDescription",
        "EffectiveDate",
        "StartingRange",
        "EndingRange",
        "Unit",
        "PricePerUnit",
        "CurrencyCode",
        "Product Family",
        "Region Code",
        "Location",
        "Instance Type",
        "LeaseContractLength",
        "PurchaseOption",
        "OfferingClass",
        "Operating System",
        "Tenancy",
        "Pre Installed S/W",
        "vCPU",
        "Memory",
        "Storage",
        "Network Performance",
        "GPU",
        "CapacityStatus",
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    for idx in range(5):
        writer.writerow([f"metadata-{idx}"])
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


class AWSCollectorTests(unittest.TestCase):
    def test_capacity_block_rows_are_kept_but_classified_as_reserved(self):
        csv_text = _build_aws_csv([
            [
                "sku1", "offer1", "rate1", "OnDemand", "$55.165 per On Demand SUSE p5.48xlarge Instance Hour",
                "2026-04-01T00:00:00Z", "", "", "Hrs", "55.165", "USD",
                "Compute Instance", "us-east-1", "US East (N. Virginia)", "p5.48xlarge",
                "", "", "", "SUSE", "Shared", "NA",
                "192", "768 GiB", "2 x 3800 NVMe SSD", "100 Gigabit", "8", "Used",
            ],
            [
                "sku2", "offer2", "rate2", "OnDemand", "$0.125 per Capacity Block SUSE p5.48xlarge Instance Hour",
                "2026-04-01T00:00:00Z", "", "", "Hrs", "0.125", "USD",
                "Compute Instance", "us-east-1", "US East (N. Virginia)", "p5.48xlarge",
                "", "", "", "SUSE", "Shared", "NA",
                "192", "768 GiB", "2 x 3800 NVMe SSD", "100 Gigabit", "8", "Used",
            ],
        ])

        with mock.patch("collectors.aws.urllib.request.urlopen", return_value=_FakeHTTPResponse(csv_text)):
            rows = AWSCollector()._fetch_region("us-east-1")

        self.assertEqual(len(rows), 2)
        by_commitment = {row["commitment_period"]: row for row in rows}
        self.assertEqual(by_commitment[""]["pricing_type"], "on_demand")
        self.assertEqual(by_commitment[""]["price_per_gpu_hour"], 6.895625)
        self.assertEqual(by_commitment["capacity_block"]["pricing_type"], "reserved")
        self.assertEqual(by_commitment["capacity_block"]["price_per_gpu_hour"], 0.015625)
        self.assertIn('"billing_model":"capacity_block"', by_commitment["capacity_block"]["raw_extra"])

    def test_fetch_region_combines_hourly_and_upfront_reserved_components(self):
        csv_text = _build_aws_csv([
            [
                "sku1", "offer1", "rate1", "Reserved", "Partial upfront hourly",
                "2026-04-01T00:00:00Z", "", "", "Hrs", "1.25", "USD",
                "Compute Instance", "us-east-1", "US East (N. Virginia)", "g5.12xlarge",
                "1yr", "Partial Upfront", "standard", "Linux", "Shared", "NA",
                "48", "192 GiB", "1 x 3800 NVMe SSD", "25 Gigabit", "4", "Used",
            ],
            [
                "sku1", "offer1", "rate2", "Reserved", "Partial upfront fee",
                "2026-04-01T00:00:00Z", "", "", "Quantity", "5000", "USD",
                "Compute Instance", "us-east-1", "US East (N. Virginia)", "g5.12xlarge",
                "1yr", "Partial Upfront", "standard", "Linux", "Shared", "NA",
                "48", "192 GiB", "1 x 3800 NVMe SSD", "25 Gigabit", "4", "Used",
            ],
            [
                "sku2", "offer2", "rate3", "Reserved", "All upfront fee",
                "2026-04-01T00:00:00Z", "", "", "Quantity", "12000", "USD",
                "Compute Instance", "us-east-1", "US East (N. Virginia)", "g5.48xlarge",
                "3yr", "All Upfront", "standard", "Linux", "Shared", "NA",
                "192", "768 GiB", "2 x 3800 NVMe SSD", "100 Gigabit", "8", "Used",
            ],
        ])

        with mock.patch("collectors.aws.urllib.request.urlopen", return_value=_FakeHTTPResponse(csv_text)):
            rows = AWSCollector()._fetch_region("us-east-1")

        self.assertEqual(len(rows), 2)

        by_instance = {row["instance_type"]: row for row in rows}
        partial = by_instance["g5.12xlarge"]
        self.assertEqual(partial["pricing_type"], "reserved")
        self.assertEqual(partial["commitment_period"], "1yr")
        self.assertEqual(partial["price_per_hour"], 1.25)
        self.assertEqual(partial["price_per_gpu_hour"], 0.3125)
        self.assertEqual(partial["upfront_price"], 5000.0)
        self.assertEqual(partial["upfront_price_per_gpu"], 1250.0)

        upfront = by_instance["g5.48xlarge"]
        self.assertEqual(upfront["pricing_type"], "reserved")
        self.assertEqual(upfront["commitment_period"], "3yr")
        self.assertEqual(upfront["price_per_hour"], 0.0)
        self.assertEqual(upfront["price_per_gpu_hour"], 0.0)
        self.assertEqual(upfront["upfront_price"], 12000.0)
        self.assertEqual(upfront["upfront_price_per_gpu"], 1500.0)

    def test_prune_keeps_distinct_reserved_rows_that_share_hourly_price(self):
        rows = [
            {
                "snapshot_date": "2026-04-01",
                "snapshot_ts": "2026-04-01T00:00:00Z",
                "source": "aws",
                "provider": "aws",
                "instance_type": "g5.48xlarge",
                "instance_family": "g5",
                "gpu_name": "A10G",
                "gpu_variant": "",
                "gpu_memory_gb": "24",
                "gpu_count": "8",
                "gpu_interconnect": "",
                "vcpus": "192",
                "ram_gb": "768",
                "storage_desc": "2 x 3800 NVMe SSD",
                "network_desc": "100 Gigabit",
                "region": "us-east-1",
                "zone": "",
                "country": "",
                "geo_group": "US",
                "pricing_type": "reserved",
                "commitment_period": "1yr",
                "price_per_hour": "0.0",
                "price_per_gpu_hour": "0.0",
                "upfront_price": "12000.0",
                "upfront_price_per_gpu": "1500.0",
                "currency": "USD",
                "price_unit": "hour",
                "available": "True",
                "available_count": "",
                "os": "Linux",
                "tenancy": "Shared",
                "pre_installed_sw": "NA",
                "raw_extra": "{\"purchase_option\":\"All Upfront\"}",
            },
            {
                "snapshot_date": "2026-04-01",
                "snapshot_ts": "2026-04-01T00:00:00Z",
                "source": "aws",
                "provider": "aws",
                "instance_type": "g5.48xlarge",
                "instance_family": "g5",
                "gpu_name": "A10G",
                "gpu_variant": "",
                "gpu_memory_gb": "24",
                "gpu_count": "8",
                "gpu_interconnect": "",
                "vcpus": "192",
                "ram_gb": "768",
                "storage_desc": "2 x 3800 NVMe SSD",
                "network_desc": "100 Gigabit",
                "region": "us-east-1",
                "zone": "",
                "country": "",
                "geo_group": "US",
                "pricing_type": "reserved",
                "commitment_period": "3yr",
                "price_per_hour": "0.0",
                "price_per_gpu_hour": "0.0",
                "upfront_price": "18000.0",
                "upfront_price_per_gpu": "2250.0",
                "currency": "USD",
                "price_unit": "hour",
                "available": "True",
                "available_count": "",
                "os": "Linux",
                "tenancy": "Shared",
                "pre_installed_sw": "NA",
                "raw_extra": "{\"purchase_option\":\"All Upfront\"}",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "aws.csv")
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

            with mock.patch("collectors.base.DATA_DIR", tmpdir):
                prune_all_csvs()

            with open(path, newline="", encoding="utf-8") as f:
                kept = list(csv.DictReader(f))

        self.assertEqual(len(kept), 2)
