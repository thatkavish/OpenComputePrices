import unittest

from collectors.oracle import OracleCollector


class OracleCollectorTests(unittest.TestCase):
    def test_gpu_per_hour_rows_are_single_gpu_offers(self):
        row = OracleCollector()._parse_item({
            "displayName": "OCI - Compute - GPU - GB200",
            "partNumber": "B110979",
            "serviceCategory": "Compute - GPU",
            "metricName": "GPU Per Hour",
            "currencyCodeLocalizations": [{
                "currencyCode": "USD",
                "prices": [{"model": "PAY_AS_YOU_GO", "value": 16}],
            }],
        })

        self.assertEqual(row["gpu_name"], "GB200")
        self.assertEqual(row["gpu_count"], 1)
        self.assertEqual(row["price_per_hour"], 16)
        self.assertEqual(row["price_per_gpu_hour"], 16)

    def test_node_per_hour_rows_divide_price_across_known_gpu_count(self):
        row = OracleCollector()._parse_item({
            "displayName": "Oracle Cloud VMware Solution - BM.GPU.A10.64 - 1 Year Commit",
            "partNumber": "B108807",
            "serviceCategory": "Compute - VMware",
            "metricName": "Node Per Hour",
            "currencyCodeLocalizations": [{
                "currencyCode": "USD",
                "prices": [{"model": "COMMIT", "value": 13}],
            }],
        })

        self.assertEqual(row["gpu_name"], "A10")
        self.assertEqual(row["gpu_count"], 4)
        self.assertEqual(row["price_per_hour"], 13)
        self.assertEqual(row["price_per_gpu_hour"], 3.25)

    def test_gh200_is_not_downgraded_to_h200(self):
        row = OracleCollector()._parse_item({
            "displayName": "OCI - Compute - GPU - GH200",
            "partNumber": "part-gh200",
            "serviceCategory": "Compute - GPU",
            "metricName": "GPU Per Hour",
            "currencyCodeLocalizations": [{
                "currencyCode": "USD",
                "prices": [{"model": "PAY_AS_YOU_GO", "value": 20}],
            }],
        })

        self.assertEqual(row["gpu_name"], "GH200")


if __name__ == "__main__":
    unittest.main()
