"""
Infracost Cloud Pricing API collector.

Free GraphQL API covering AWS, Azure, and GCP with 3M+ prices.
https://pricing.api.infracost.io/graphql
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name, infer_geo_group

logger = logging.getLogger(__name__)

API_URL = "https://pricing.api.infracost.io/graphql"

# GraphQL query for GPU instances
QUERY = """
query($filter: ProductFilter!, $after: String) {
  products(filter: $filter, first: 1000, after: $after) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      productHash
      vendorName
      service
      productFamily
      region
      sku
      attributes {
        key
        value
      }
      prices(filter: {purchaseOption: "on_demand"}) {
        USD
        unit
        description
        startUsageAmount
        purchaseOption
      }
    }
  }
}
"""

# Instance families with GPUs
GPU_FAMILIES_AWS = ["p2", "p3", "p4d", "p4de", "p5", "p5e", "p5en",
                     "g4dn", "g5", "g6", "g6e", "trn1", "trn2", "inf2"]
GPU_FAMILIES_AZURE = ["NC", "ND", "NV"]
GPU_FAMILIES_GCP = ["a2-", "a3-", "g2-", "n1-"] # n1 with attached GPUs


class InfracostCollector(BaseCollector):
    name = "infracost"
    requires_api_key = True
    api_key_env_var = "INFRACOST_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[infracost] Fetching GPU pricing via GraphQL")

        all_rows = []

        # Query each vendor
        for vendor in ["aws", "azure", "gcp"]:
            rows = self._query_vendor(vendor)
            all_rows.extend(rows)
            logger.info(f"[infracost] {vendor}: {len(rows)} rows")

        logger.info(f"[infracost] Total: {len(all_rows)} rows")
        return all_rows

    def _query_vendor(self, vendor: str) -> List[Dict[str, Any]]:
        """Query Infracost for GPU-related products from a vendor."""
        vendor_filter = {
            "aws": {"vendorName": "aws", "service": "AmazonEC2", "productFamily": "Compute Instance"},
            "azure": {"vendorName": "azure", "service": "Virtual Machines"},
            "gcp": {"vendorName": "gcp", "service": "Compute Engine"},
        }

        filt = vendor_filter.get(vendor, {})
        rows = []
        cursor = None
        api_key = self.get_api_key()

        while True:
            variables = {"filter": filt}
            if cursor:
                variables["after"] = cursor

            payload = json.dumps({"query": QUERY, "variables": variables}).encode("utf-8")

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            if api_key:
                headers["X-Api-Key"] = api_key

            try:
                req = urllib.request.Request(
                    API_URL,
                    data=payload,
                    method="POST",
                    headers=headers,
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read().decode())
            except Exception as e:
                logger.warning(f"[infracost] {vendor} query failed: {e}")
                break

            products_data = data.get("data", {}).get("products", {})
            nodes = products_data.get("nodes", [])
            page_info = products_data.get("pageInfo", {})

            for node in nodes:
                parsed = self._parse_node(node, vendor)
                if parsed:
                    rows.append(parsed)

            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor", "")
            if not cursor:
                break

        return rows

    def _parse_node(self, node: dict, vendor: str) -> Dict[str, Any]:
        """Parse a product node into our schema."""
        attrs = {}
        for a in node.get("attributes", []):
            attrs[a.get("key", "")] = a.get("value", "")

        instance_type = attrs.get("instanceType", "") or attrs.get("armSkuName", "") or node.get("sku", "")
        if not instance_type:
            return None

        # Filter to GPU instances
        is_gpu = False
        it_lower = instance_type.lower()
        if vendor == "aws":
            is_gpu = any(it_lower.startswith(f) for f in ["p2.", "p3.", "p4d.", "p4de.", "p5.", "p5e.", "p5en.",
                                                            "g4dn.", "g5.", "g6.", "g6e.",
                                                            "trn1.", "trn2.", "inf2.", "dl1."])
        elif vendor == "azure":
            is_gpu = any(kw in it_lower for kw in ["_nc", "_nd", "_nv"])
        elif vendor == "gcp":
            is_gpu = any(it_lower.startswith(f) for f in ["a2-", "a3-", "g2-"]) or "gpu" in attrs.get("machineType", "").lower()

        if not is_gpu:
            return None

        # Get pricing
        prices = node.get("prices", [])
        if not prices:
            return None

        usd_price = None
        for p in prices:
            try:
                usd_price = float(p.get("USD", "0"))
                if usd_price > 0:
                    break
            except (ValueError, TypeError):
                continue

        if not usd_price or usd_price <= 0:
            return None

        region = node.get("region", "")
        gpu_name = attrs.get("gpu", "") or attrs.get("gpuName", "")
        gpu_count = 0
        try:
            gpu_count = int(attrs.get("gpuCount", "0") or attrs.get("gpu", "0"))
        except (ValueError, TypeError):
            pass

        price_per_gpu = usd_price / gpu_count if gpu_count > 0 else usd_price

        return self.make_row(
            provider=vendor,
            instance_type=instance_type,
            gpu_name=normalize_gpu_name(gpu_name),
            gpu_count=gpu_count,
            vcpus=attrs.get("vcpu", ""),
            ram_gb=attrs.get("memory", "").replace(" GiB", "").replace(" GB", ""),
            region=region,
            geo_group=infer_geo_group(region),
            pricing_type="on_demand",
            price_per_hour=round(usd_price, 6),
            price_per_gpu_hour=round(price_per_gpu, 6),
            available=True,
            raw_extra=json.dumps({
                "source": "infracost",
                "product_hash": node.get("productHash", ""),
                "product_family": node.get("productFamily", ""),
                "os": attrs.get("operatingSystem", ""),
                "tenancy": attrs.get("tenancy", ""),
            }, separators=(",", ":")),
        )
