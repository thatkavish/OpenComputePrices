import unittest

from unify import unify


class UnifyTests(unittest.TestCase):
    def test_lower_priority_duplicate_backfills_missing_fields_on_winner(self):
        rows = [
            {
                "snapshot_date": "2026-04-01",
                "provider": "aws",
                "source": "aws",
                "instance_type": "p5.48xlarge",
                "pricing_type": "on_demand",
                "gpu_name": "H100",
                "gpu_count": "8",
                "region": "us-east-1",
                "os": "Linux",
                "network_desc": "",
                "zone": "",
                "price_per_hour": "10.0",
            },
            {
                "snapshot_date": "2026-04-01",
                "provider": "aws",
                "source": "shadeform",
                "instance_type": "p5.48xlarge",
                "pricing_type": "on_demand",
                "gpu_name": "H100",
                "gpu_count": "8",
                "region": "us-east-1",
                "os": "Linux",
                "network_desc": "3200 Gbps",
                "zone": "use1-az1",
                "price_per_hour": "11.0",
            },
        ]

        merged = unify(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["source"], "aws")
        self.assertEqual(merged[0]["network_desc"], "3200 Gbps")
        self.assertEqual(merged[0]["zone"], "use1-az1")
        self.assertEqual(merged[0]["price_per_hour"], "10.0")

    def test_inference_rows_with_empty_gpu_name_still_deduplicate_by_priority(self):
        rows = [
            {
                "snapshot_date": "2026-04-01",
                "provider": "openrouter",
                "source": "openrouter",
                "instance_type": "openai/gpt-4.1",
                "pricing_type": "inference",
                "gpu_name": "",
                "gpu_count": "",
                "region": "",
                "os": "",
                "available": "",
                "commitment_period": "",
            },
            {
                "snapshot_date": "2026-04-01",
                "provider": "openrouter",
                "source": "shadeform",
                "instance_type": "openai/gpt-4.1",
                "pricing_type": "inference",
                "gpu_name": "",
                "gpu_count": "",
                "region": "",
                "os": "",
                "available": "true",
                "commitment_period": "monthly",
            },
        ]

        merged = unify(rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["source"], "openrouter")
        self.assertEqual(merged[0]["available"], "true")
        self.assertEqual(merged[0]["commitment_period"], "monthly")
