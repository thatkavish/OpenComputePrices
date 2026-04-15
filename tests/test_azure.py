import unittest

from collectors.azure import AzureCollector, _infer_gpu_from_sku


class AzureCollectorTests(unittest.TestCase):
    def test_unmapped_nd96_h100_variant_still_uses_eight_gpus(self):
        gpu = _infer_gpu_from_sku(
            "Standard_ND96isrf_H100_v5",
            "Virtual Machines ND H100 v5 Series",
        )

        self.assertEqual(gpu["gpu"], "H100")
        self.assertEqual(gpu["count"], 8)
        self.assertEqual(gpu["variant"], "SXM5")

    def test_ncads_h100_v5_uses_correct_gpu_count(self):
        row = AzureCollector()._parse_item({
            "armSkuName": "Standard_NC40ads_H100_v5",
            "productName": "Virtual Machines NCadsH100v5 Series",
            "meterName": "NC40adsH100v5",
            "armRegionName": "eastus",
            "unitPrice": 6.98,
            "retailPrice": 6.98,
            "unitOfMeasure": "1 Hour",
            "currencyCode": "USD",
            "skuId": "DZH318Z0F8W2/000N",
            "type": "Consumption",
        })

        self.assertEqual(row["gpu_name"], "H100")
        self.assertEqual(row["gpu_variant"], "NVL")
        self.assertEqual(row["gpu_memory_gb"], 94)
        self.assertEqual(row["gpu_count"], 1)
        self.assertEqual(row["price_per_gpu_hour"], 6.98)

    def test_reservation_price_is_not_treated_as_hourly_on_demand(self):
        row = AzureCollector()._parse_item({
            "armSkuName": "Standard_NC48ads_A100_v4",
            "productName": "Virtual Machines NCads A100 v4 Series",
            "meterName": "NC48ads A100 v4",
            "armRegionName": "eastus",
            "unitPrice": 42066.0,
            "retailPrice": 42066.0,
            "unitOfMeasure": "1 Hour",
            "currencyCode": "USD",
            "skuId": "DZH318Z09TGJ/0065",
            "type": "Reservation",
            "reservationTerm": "1 Year",
        })

        self.assertEqual(row["pricing_type"], "reserved")
        self.assertEqual(row["commitment_period"], "1yr")
        self.assertEqual(row["price_per_hour"], 0.0)
        self.assertEqual(row["price_per_gpu_hour"], 0.0)
        self.assertEqual(row["upfront_price"], 42066.0)
        self.assertEqual(row["upfront_price_per_gpu"], 21033.0)
