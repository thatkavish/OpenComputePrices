import unittest

from collectors.gcp import GCPCollector, _extract_gpu_from_description


class GCPCollectorTests(unittest.TestCase):
    def test_gb200_description_is_not_downgraded_to_b200(self):
        gpu = _extract_gpu_from_description("Nvidia GB200 GPU running in us-central1")

        self.assertEqual(gpu["gpu"], "GB200")
        self.assertEqual(gpu["mem"], 192)

    def test_gpu_skus_are_kept_as_per_gpu_hour_rows(self):
        rows = GCPCollector()._parse_sku({
            "skuId": "sku-h100",
            "description": "Nvidia H100 80GB GPU running in Iowa",
            "category": {
                "resourceFamily": "Compute",
                "resourceGroup": "GPU",
                "usageType": "OnDemand",
            },
            "pricingInfo": [{
                "pricingExpression": {
                    "usageUnit": "h",
                    "tieredRates": [{
                        "unitPrice": {"units": "12", "nanos": 500000000},
                    }],
                },
            }],
            "serviceRegions": ["us-central1"],
        })

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "H100")
        self.assertEqual(rows[0]["gpu_count"], 1)
        self.assertEqual(rows[0]["price_per_hour"], 12.5)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 12.5)


if __name__ == "__main__":
    unittest.main()
