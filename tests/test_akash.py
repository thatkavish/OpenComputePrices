import json
import unittest
from unittest import mock

from collectors.akash import AkashCollector


class _FakeHTTPResponse:
    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class AkashCollectorTests(unittest.TestCase):
    def test_collect_uses_public_page_weighted_average_metric(self):
        payload = {
            "models": [
                {
                    "vendor": "nvidia",
                    "model": "h100",
                    "ram": "80Gi",
                    "interface": "SXM5",
                    "availability": {"total": 63, "available": 16},
                    "providerAvailability": {"total": 2, "available": 2},
                    "price": {
                        "currency": "USD",
                        "min": 1.17,
                        "max": 2.02,
                        "avg": 1.60,
                        "weightedAverage": 1.48,
                        "med": 1.60,
                    },
                }
            ]
        }

        with mock.patch(
            "collectors.akash.urllib.request.urlopen",
            return_value=_FakeHTTPResponse(json.dumps(payload)),
        ):
            rows = AkashCollector().collect()

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["gpu_name"], "H100")
        self.assertEqual(row["instance_type"], "h100_SXM5")
        self.assertEqual(row["price_per_hour"], 1.48)
        self.assertEqual(row["price_per_gpu_hour"], 1.48)
        self.assertEqual(row["available_count"], 16)
        self.assertIn('"price_metric":"weightedAverage"', row["raw_extra"])
        self.assertIn('"min":1.17', row["raw_extra"])
        self.assertIn('"max":2.02', row["raw_extra"])

    def test_collect_falls_back_when_weighted_average_missing(self):
        payload = {
            "models": [
                {
                    "vendor": "nvidia",
                    "model": "h200",
                    "ram": "141Gi",
                    "interface": "SXM5",
                    "availability": {"total": 36, "available": 13},
                    "providerAvailability": {"total": 2, "available": 2},
                    "price": {
                        "currency": "USD",
                        "min": 2.24,
                        "max": 2.86,
                        "avg": 2.55,
                        "weightedAverage": 0,
                        "med": 2.55,
                    },
                }
            ]
        }

        with mock.patch(
            "collectors.akash.urllib.request.urlopen",
            return_value=_FakeHTTPResponse(json.dumps(payload)),
        ):
            rows = AkashCollector().collect()

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["price_per_hour"], 2.55)
        self.assertEqual(row["price_per_gpu_hour"], 2.55)
        self.assertIn('"price_metric":"avg"', row["raw_extra"])


if __name__ == "__main__":
    unittest.main()
