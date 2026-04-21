import unittest
from unittest import mock

from collectors.getdeploying import GetDeployingCollector


SAMPLE_GPU_PAGE = """
<table>
  <tr>
    <th>Provider</th>
    <th>GPUs</th>
    <th>Total VRAM</th>
    <th>vCPUs</th>
    <th>RAM</th>
    <th>Billing</th>
    <th>$/GPU/h</th>
    <th>Total/h</th>
  </tr>
  <tr>
    <td>AWS</td>
    <td>
      <div class="flex">
        <span>1x H100</span>
        <span
          x-data="{
            show: false,
            adjustPosition() {
              const theoreticalRight = 99 > 10;
            }
          }"
        ></span>
        <span>80GB (p5.4xlarge)</span>
      </div>
    </td>
    <td>80GB</td>
    <td>16</td>
    <td>256GB</td>
    <td>
      <span>On-Demand</span>
      <span
        x-data="{
          show: false,
          adjustPosition() {
            const theoreticalLeft = 1 > 0;
          }
        }"
      ></span>
      <span>Pay-as-you-go pricing. No term commitments.</span>
    </td>
    <td>$6.88</td>
    <td>$6.88</td>
  </tr>
</table>
"""


class GetDeployingCollectorTests(unittest.TestCase):
    @mock.patch("collectors.getdeploying._fetch", return_value=SAMPLE_GPU_PAGE)
    def test_gpu_page_ignores_tooltip_javascript_in_cells(self, _fetch):
        collector = GetDeployingCollector()

        rows = collector._scrape_gpu_page("nvidia-h100")

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["provider"], "aws")
        self.assertEqual(row["instance_type"], "1x H100 80GB (p5.4xlarge)")
        self.assertEqual(row["gpu_count"], 1)
        self.assertEqual(row["price_per_gpu_hour"], 6.88)
        self.assertEqual(row["price_per_hour"], 6.88)
        self.assertNotIn("tooltip", row["instance_type"].lower())
        self.assertNotIn("tooltip", row["raw_extra"].lower())


if __name__ == "__main__":
    unittest.main()
