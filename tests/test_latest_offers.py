import unittest

from latest_offers import derive_latest_gpu_offers
from schema import COLUMNS


def _row(**overrides):
    row = {column: "" for column in COLUMNS}
    row.update({
        "snapshot_date": "2026-04-14",
        "provider": "hyperstack",
        "instance_type": "h100",
        "gpu_name": "H100",
        "gpu_memory_gb": "80",
        "gpu_count": "1",
        "pricing_type": "on_demand",
        "price_per_hour": "1.90",
        "price_per_gpu_hour": "1.90",
        "currency": "USD",
        "price_unit": "hour",
        "source": "hyperstack",
    })
    row.update(overrides)
    return row


class LatestOffersTests(unittest.TestCase):
    def test_drops_exact_duplicate_rows(self):
        rows = [
            _row(region="global", geo_group="Unknown"),
            _row(region="global", geo_group="Unknown"),
        ]

        offers, stats = derive_latest_gpu_offers(rows)

        self.assertEqual(len(offers), 1)
        self.assertEqual(stats["exact_duplicates_dropped"], 1)
        self.assertEqual(stats["shadow_rows_dropped"], 0)

    def test_drops_generic_shadow_row_when_specific_row_has_more_metadata(self):
        rows = [
            _row(region="global", geo_group="Unknown"),
            _row(region="ca-tor", country="CA", geo_group="Canada", gpu_interconnect="PCIe"),
        ]

        offers, stats = derive_latest_gpu_offers(rows)

        self.assertEqual(len(offers), 1)
        self.assertEqual(stats["shadow_rows_dropped"], 1)
        self.assertEqual(offers[0]["region"], "ca-tor")
        self.assertEqual(offers[0]["gpu_interconnect"], "PCIe")

    def test_keeps_multiple_specific_regions_for_same_offer(self):
        rows = [
            _row(region="global", geo_group="Unknown"),
            _row(region="eu-west", geo_group="Europe"),
            _row(region="ap-se", geo_group="APAC"),
        ]

        offers, stats = derive_latest_gpu_offers(rows)

        self.assertEqual(len(offers), 2)
        self.assertEqual(stats["shadow_rows_dropped"], 1)
        self.assertEqual(sorted(row["region"] for row in offers), ["ap-se", "eu-west"])

    def test_keeps_rows_with_different_instance_types_even_if_one_is_more_specific(self):
        rows = [
            _row(instance_type="h100-a", region="global", geo_group="Unknown"),
            _row(instance_type="h100-b", region="ca-tor", country="CA", geo_group="Canada", gpu_interconnect="PCIe"),
        ]

        offers, stats = derive_latest_gpu_offers(rows)

        self.assertEqual(len(offers), 2)
        self.assertEqual(stats["shadow_rows_dropped"], 0)


if __name__ == "__main__":
    unittest.main()
