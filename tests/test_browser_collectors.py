import unittest

from collectors.browser_providers import (
    GcoreBrowserCollector,
    HyperstackBrowserCollector,
    QubridBrowserCollector,
    SaladBrowserCollector,
)
from collectors.massedcompute import MassedComputeCollector


class BrowserCollectorParserTests(unittest.TestCase):
    def test_hyperstack_text_pricing_extracts_shifted_page_layout(self):
        collector = HyperstackBrowserCollector()
        collector._last_render_text = "\n".join([
            "On-Demand GPU",
            "Pricing",
            "GPU Model",
            "VRAM (GB)",
            "Pricing Per Hour",
            "NVIDIA H200 SXM",
            "141",
            "22",
            "225",
            "$3.50",
            "Reservation",
            "Pricing",
            "NVIDIA H100 SXM",
            "$2.04",
        ])

        rows = collector.parse_page("")

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["provider"], "hyperstack")
        self.assertEqual(rows[0]["gpu_name"], "H200")
        self.assertEqual(rows[0]["pricing_type"], "on_demand")
        self.assertEqual(rows[0]["price_per_hour"], 3.5)
        self.assertEqual(rows[1]["pricing_type"], "reserved")

    def test_gcore_text_cards_extract_euro_prices(self):
        collector = GcoreBrowserCollector()
        collector._last_render_text = "\n".join([
            "GPUs for every AI workload",
            "L40S",
            "Optimized for AI inference",
            "From €1.08",
            "/hour",
            "Get started",
            "Why choose Gcore as your GPU cloud provider?",
        ])

        rows = collector.parse_page("")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "L40S")
        self.assertEqual(rows[0]["currency"], "EUR")
        self.assertEqual(rows[0]["price_per_hour"], 1.08)

    def test_salad_text_calculator_extracts_gpu_prices(self):
        collector = SaladBrowserCollector()
        collector._last_render_text = "\n".join([
            "SaladCloud Pricing Calculator",
            "RTX 5090 (32 GB)",
            "$0.25 per hour",
            "GTX 1050 Ti (4 GB)",
            "$0.01 per hour",
            "Select your priority level",
        ])

        rows = collector.parse_page("")

        self.assertEqual([row["gpu_name"] for row in rows], ["RTX 5090", "GTX 1050 Ti"])
        self.assertEqual(rows[1]["gpu_memory_gb"], 4)
        self.assertEqual(rows[1]["price_per_hour"], 0.01)

    def test_qubrid_text_vm_table_extracts_multi_gpu_per_gpu_price(self):
        collector = QubridBrowserCollector()
        collector._last_render_text = "\n".join([
            "GPU Virtual Machines",
            "INSTANCE\tVCPU\tRAM\tSTORAGE\tON DEMAND (/HR)",
            "NVIDIA H100 (80GB) - 8 GPUs",
            "128\t1600 GB\t20000 GB\t$30.64",
            "Bare Metal Servers",
        ])

        rows = collector.parse_page("")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "H100")
        self.assertEqual(rows[0]["gpu_count"], 8)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 3.83)

    def test_massedcompute_text_fallback_extracts_rows_without_html_tables(self):
        collector = MassedComputeCollector()
        html = """
        <div>GPU Type</div><div>QTY</div><div>vRAM</div><div>vCPU</div><div>RAM</div><div>Storage</div><div>Price</div>
        <div>H100 PCIe</div><div>80 GB</div>
        <div>x 4</div><div>320</div><div>64</div><div>512 GB</div><div>5000 GB</div><div>$9.40/hr</div>
        <div>Bare Metal</div>
        """

        rows = collector._parse_text_pricing(html)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "H100")
        self.assertEqual(rows[0]["gpu_count"], 4)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 2.35)
