import unittest

from collectors.vultr import VultrCollector, infer_effective_gpu_count


class VultrCollectorTests(unittest.TestCase):
    def test_infer_effective_gpu_count_handles_fractional_and_multi_gpu_vram(self):
        self.assertEqual(infer_effective_gpu_count("A100", 4), 0.05)
        self.assertEqual(infer_effective_gpu_count("A100", 40), 0.5)
        self.assertEqual(infer_effective_gpu_count("A100", 160), 2)
        self.assertEqual(infer_effective_gpu_count("L40S", 384), 8)

    def test_parse_plan_normalizes_fractional_gpu_slice(self):
        collector = VultrCollector()
        rows = collector._parse_plan({
            "id": "vcg-a100-6c-60g-40vram",
            "gpu_type": "NVIDIA_A100",
            "gpu_vram_gb": 40,
            "hourly_cost": 1.199,
            "vcpu_count": 6,
            "ram": 60,
            "disk": 1400,
            "locations": ["ewr"],
        })

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "A100")
        self.assertEqual(rows[0]["gpu_memory_gb"], 40)
        self.assertEqual(rows[0]["gpu_count"], 0.5)
        self.assertEqual(rows[0]["price_per_hour"], 1.199)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 2.398)

    def test_parse_plan_normalizes_multi_gpu_memory_to_gpu_count(self):
        collector = VultrCollector()
        rows = collector._parse_plan({
            "id": "vcg-a100-24c-240g-160vram",
            "gpu_type": "NVIDIA_A100",
            "gpu_vram_gb": 160,
            "hourly_cost": 4.795,
            "vcpu_count": 24,
            "ram": 240,
            "disk": 1400,
            "locations": ["ewr"],
        })

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_count"], 2)
        self.assertEqual(rows[0]["price_per_hour"], 4.795)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 2.3975)
