import unittest

from schema import (
    infer_geo_group,
    normalize_gpu_memory_gb,
    normalize_gpu_name,
    normalize_provider,
    normalize_region,
)


class SchemaNormalizationTests(unittest.TestCase):
    def test_provider_aliases_and_sponsorship_suffixes_are_canonicalized(self):
        self.assertEqual(normalize_provider("runpod_our_sponsor"), "runpod")
        self.assertEqual(normalize_provider("lambda_labs"), "lambda")
        self.assertEqual(normalize_provider("packet.ai_sponsored"), "packet_ai")
        self.assertEqual(normalize_provider("oracle_cloud"), "oracle")
        self.assertEqual(normalize_provider("google cloud"), "gcp")

    def test_gpu_aliases_and_invalid_names_are_normalized(self):
        cases = {
            "RTXA2000": "RTX A2000",
            "RTXPro6000": "RTX PRO 6000",
            "pro6000se": "RTX PRO 6000 Server Edition",
            "pro6000we": "RTX PRO 6000 Workstation Edition",
            "rtxpro6000blackwellmaxqworkstationedition": "RTX PRO 6000 Blackwell Max-Q Workstation Edition",
            "gtx1070ti": "GTX 1070 Ti",
            "gtx1050ti": "GTX 1050 Ti",
            "p40": "P40",
            "gaudi2": "Gaudi 2",
            "GPU (unspecified)": "",
            "CPU": "",
            "1": "",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(normalize_gpu_name(raw), expected)

    def test_region_codes_are_bucketed_for_display(self):
        cases = {
            "us-east-1": "US East",
            "eastus2": "US East",
            "us-west-2": "US West",
            "westus3": "US West",
            "us-central1": "US Central",
            "southcentralus": "US Central",
            "CA": "Canada",
            "eu-central-1": "Europe",
            "europe-west4": "Europe",
            "ap-northeast-1": "APAC",
            "southeastasia": "APAC",
            "sa-east-1": "LATAM",
            "brazilsouth": "LATAM",
            "me-central-1": "Middle East",
            "israelcentral": "Middle East",
            "af-south-1": "Africa",
            "": "Unknown",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(infer_geo_group(raw), expected)

        self.assertEqual(infer_geo_group("", "NL"), "Europe")

    def test_gpu_memory_is_normalized_from_skypilot_gpuinfo_and_aliases(self):
        gpu_info = "{'Gpus': [{'Name': 'H100', 'Manufacturer': 'NVIDIA', 'Count': 8, 'MemoryInfo': {'SizeInMiB': 81920}}], 'TotalGpuMemoryInMiB': 655360}"
        self.assertEqual(normalize_gpu_memory_gb(gpu_info, "H100", 8), 80)
        self.assertEqual(normalize_gpu_memory_gb("H100", "H100", 1), 80)
        self.assertEqual(normalize_gpu_memory_gb("A100 80GB", "A100", 1), 80)
        self.assertEqual(normalize_gpu_memory_gb("A100 40GB", "A100", 1), 40)
        self.assertEqual(normalize_gpu_memory_gb("nonsense", "A100", 1), "")

    def test_region_is_backfilled_from_provider_defaults_and_raw_extra(self):
        self.assertEqual(normalize_region("", "akash", "", ""), "global")
        self.assertEqual(normalize_region("", "oracle", "", ""), "global")
        self.assertEqual(normalize_region("", "vultr", "", '{"locations":["ewr","nrt"]}'), "ewr")
        self.assertEqual(normalize_region("", "runpod", "US", ""), "US")
        self.assertEqual(normalize_region("", "aws", "", "", "getdeploying"), "global")
