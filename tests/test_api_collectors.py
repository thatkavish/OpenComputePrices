import json
import unittest
from unittest import mock

from collectors.cloreai import CloreAICollector
from collectors.vastai import VastAICollector


class _FakeHTTPResponse:
    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class APICollectorTests(unittest.TestCase):
    def test_vastai_uses_current_offer_type_values(self):
        payloads = []

        def fake_urlopen(req, timeout=0):
            payloads.append(json.loads(req.data.decode("utf-8")))
            return _FakeHTTPResponse('{"offers":[]}')

        with mock.patch.dict("os.environ", {"VASTAI_API_KEY": "secret"}), \
             mock.patch("collectors.vastai.urllib.request.urlopen", side_effect=fake_urlopen):
            VastAICollector().collect()

        self.assertEqual([payload["type"] for payload in payloads], ["ondemand", "bid"])
        self.assertEqual(payloads[0]["rentable"], {"eq": True})
        self.assertEqual(payloads[0]["rented"], {"eq": False})

    def test_cloreai_marketplace_hourly_usd_prices_are_preserved(self):
        rows = CloreAICollector()._parse_server({
            "id": 123,
            "rented": False,
            "price": {"usd": {"on_demand_usd": 12.0, "spot": 6.0}},
            "specs": {
                "gpu": "2x NVIDIA GeForce RTX 5090",
                "gpu_array": ["RTX 5090"],
                "gpuram": 32,
                "cpus": "16/32",
                "ram": 128,
                "disk": "NVMe 1TB",
                "net": {"cc": "US"},
            },
        })

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["pricing_type"], "on_demand")
        self.assertEqual(rows[0]["gpu_name"], "RTX 5090")
        self.assertEqual(rows[0]["gpu_count"], 2)
        self.assertEqual(rows[0]["price_per_hour"], 12.0)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 6.0)
        self.assertEqual(rows[1]["pricing_type"], "spot")
        self.assertEqual(rows[1]["price_per_hour"], 6.0)
