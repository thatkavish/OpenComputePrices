import unittest

from collectors.browser_providers import (
    CoreWeaveBrowserCollector,
    GcoreBrowserCollector,
    HyperstackBrowserCollector,
    LightningAIBrowserCollector,
    QubridBrowserCollector,
    SaladBrowserCollector,
)
from collectors.massedcompute import MassedComputeCollector


class BrowserCollectorParserTests(unittest.TestCase):
    def test_coreweave_table_rows_use_explicit_gpu_count(self):
        collector = CoreWeaveBrowserCollector()
        html = """
        <div role="listitem" class="table-row-v2 w-dyn-item kubernetes-gpu-pricing">
          <div class="w-embed">
            <div class="table-grid">
              <div class="table-v2-cell table-v2-cell--name">
                <h3 data-product="nvidia-a100" class="table-model-name">NVIDIA A100</h3>
              </div>
              <div class="table-v2-cell"><div>8</div></div>
              <div class="table-v2-cell"><div>80</div></div>
              <div class="table-v2-cell"><div>128</div></div>
              <div class="table-v2-cell"><div>2,048</div></div>
              <div class="table-v2-cell"><div>7.68</div></div>
              <div class="table-v2-cell"><div>$21.60</div></div>
            </div>
          </div>
        </div>
        """

        rows = collector.parse_page(html)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "A100")
        self.assertEqual(rows[0]["gpu_count"], 8)
        self.assertEqual(rows[0]["gpu_memory_gb"], 80)
        self.assertEqual(rows[0]["price_per_hour"], 21.6)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 2.7)

    def test_coreweave_gpu_count_parses_footnote_suffix(self):
        collector = CoreWeaveBrowserCollector()
        html = """
        <div role="listitem" class="table-row-v2 w-dyn-item kubernetes-gpu-pricing">
          <div class="w-embed">
            <div class="table-grid">
              <div class="table-v2-cell table-v2-cell--name">
                <h3 data-product="nvidia-gb200-nvl72" class="table-model-name">NVIDIA GB200 NVL72</h3>
              </div>
              <div class="table-v2-cell"><div>4^1</div></div>
              <div class="table-v2-cell"><div>186</div></div>
              <div class="table-v2-cell"><div>144</div></div>
              <div class="table-v2-cell"><div>960</div></div>
              <div class="table-v2-cell"><div>30.72</div></div>
              <div class="table-v2-cell"><div>$42.00</div></div>
            </div>
          </div>
        </div>
        """

        rows = collector.parse_page(html)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "GB200")
        self.assertEqual(rows[0]["gpu_count"], 4)
        self.assertEqual(rows[0]["gpu_memory_gb"], 186)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 10.5)

    def test_coreweave_skips_mobile_price_only_duplicate_cells(self):
        collector = CoreWeaveBrowserCollector()
        html = """
        <div role="listitem" class="table-row-v2 w-dyn-item gpu-pricing-and-kubernetes-gpu-pricing">
          <div class="w-embed">
            <div class="table-grid">
              <div class="table-v2-cell table-v2-cell--name">
                <h3 data-product="hgx-h100" class="table-model-name">NVIDIA HGX H100</h3>
              </div>
              <div class="table-v2-cell"><div>8</div></div>
              <div class="table-v2-cell"><div>80</div></div>
              <div class="table-v2-cell"><div>128</div></div>
              <div class="table-v2-cell"><div>2,048</div></div>
              <div class="table-v2-cell"><div>61.44</div></div>
              <div class="table-v2-cell"><div>$49.24</div></div>
              <div class="table-cell-column table-cell-column-left">
                <h3 data-product="hgx-h100" class="table-model-name">NVIDIA HGX H100</h3>
              </div>
              <div class="table-v2-cell"><div>$19.71</div></div>
              <div class="table-v2-cell"><div>$6.16</div></div>
            </div>
          </div>
        </div>
        """

        rows = collector.parse_page(html)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gpu_name"], "H100")
        self.assertEqual(rows[0]["gpu_count"], 8)
        self.assertEqual(rows[0]["gpu_memory_gb"], 80)
        self.assertEqual(rows[0]["price_per_hour"], 49.24)
        self.assertEqual(rows[0]["price_per_gpu_hour"], 6.155)

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

    def test_lightning_table_falls_back_from_machine_column_to_accelerator_name(self):
        collector = LightningAIBrowserCollector()
        html = """
        <table>
          <tr><th>Machine</th><th>Accelerator</th><th>Price</th></tr>
          <tr><td>1</td><td>H100</td><td>$2.89</td></tr>
        </table>
        """

        rows = collector.parse_page(html)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["instance_type"], "H100")
        self.assertEqual(rows[0]["gpu_name"], "H100")
        self.assertEqual(rows[0]["price_per_hour"], 2.89)

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
