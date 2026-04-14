"""
RunPod GPU pricing collector.

Uses the RunPod GraphQL API.
Requires API key (RUNPOD_API_KEY env var).
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector
from schema import normalize_gpu_name

logger = logging.getLogger(__name__)

API_URL = "https://api.runpod.io/graphql"

QUERY = """
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    secureCloud
    communityCloud
    lowestPrice(input: {gpuCount: 1}) {
      minimumBidPrice
      uninterruptablePrice
    }
    securePrice
    communityPrice
    communitySpotPrice
    secureSpotPrice
    oneWeekPrice
    oneMonthPrice
    threeMonthPrice
    sixMonthPrice
    oneYearPrice
  }
}
"""


class RunPodCollector(BaseCollector):
    name = "runpod"
    requires_api_key = True
    api_key_env_var = "RUNPOD_API_KEY"

    def collect(self) -> List[Dict[str, Any]]:
        api_key = self.get_api_key()
        if not api_key:
            return []

        logger.info("[runpod] Fetching GPU pricing via GraphQL")

        payload = json.dumps({"query": QUERY}).encode("utf-8")
        try:
            data = self._post_graphql(payload, api_key)
        except Exception as e:
            logger.error(f"[runpod] GraphQL request failed: {e}")
            return []

        gpu_types = data.get("data", {}).get("gpuTypes", [])
        rows = []

        for gpu in gpu_types:
            gpu_id = gpu.get("id", "")
            display_name = gpu.get("displayName", "")
            mem_gb = gpu.get("memoryInGb", 0)

            # Secure cloud pricing
            secure_price = gpu.get("securePrice")
            if secure_price and secure_price > 0:
                rows.append(self.make_row(
                    provider="runpod",
                    instance_type=f"{gpu_id}_secure",
                    gpu_name=normalize_gpu_name(display_name),
                    gpu_memory_gb=mem_gb,
                    gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=secure_price,
                    price_per_gpu_hour=secure_price,
                    available=gpu.get("secureCloud", False),
                    raw_extra=json.dumps({"tier": "secure", "gpu_id": gpu_id}, separators=(",", ":")),
                ))

            # Community cloud pricing
            community_price = gpu.get("communityPrice")
            if community_price and community_price > 0:
                rows.append(self.make_row(
                    provider="runpod",
                    instance_type=f"{gpu_id}_community",
                    gpu_name=normalize_gpu_name(display_name),
                    gpu_memory_gb=mem_gb,
                    gpu_count=1,
                    pricing_type="on_demand",
                    price_per_hour=community_price,
                    price_per_gpu_hour=community_price,
                    available=gpu.get("communityCloud", False),
                    raw_extra=json.dumps({"tier": "community", "gpu_id": gpu_id}, separators=(",", ":")),
                ))

            # Secure spot
            secure_spot = gpu.get("secureSpotPrice")
            if secure_spot and secure_spot > 0:
                rows.append(self.make_row(
                    provider="runpod",
                    instance_type=f"{gpu_id}_secure_spot",
                    gpu_name=normalize_gpu_name(display_name),
                    gpu_memory_gb=mem_gb,
                    gpu_count=1,
                    pricing_type="spot",
                    price_per_hour=secure_spot,
                    price_per_gpu_hour=secure_spot,
                    available=gpu.get("secureCloud", False),
                    raw_extra=json.dumps({"tier": "secure_spot", "gpu_id": gpu_id}, separators=(",", ":")),
                ))

            # Community spot
            community_spot = gpu.get("communitySpotPrice")
            if community_spot and community_spot > 0:
                rows.append(self.make_row(
                    provider="runpod",
                    instance_type=f"{gpu_id}_community_spot",
                    gpu_name=normalize_gpu_name(display_name),
                    gpu_memory_gb=mem_gb,
                    gpu_count=1,
                    pricing_type="spot",
                    price_per_hour=community_spot,
                    price_per_gpu_hour=community_spot,
                    available=gpu.get("communityCloud", False),
                    raw_extra=json.dumps({"tier": "community_spot", "gpu_id": gpu_id}, separators=(",", ":")),
                ))

            # Commitment pricing
            for period_key, period_label in [
                ("oneWeekPrice", "1wk"), ("oneMonthPrice", "1mo"),
                ("threeMonthPrice", "3mo"), ("sixMonthPrice", "6mo"),
                ("oneYearPrice", "1yr"),
            ]:
                commit_price = gpu.get(period_key)
                if commit_price and commit_price > 0:
                    rows.append(self.make_row(
                        provider="runpod",
                        instance_type=f"{gpu_id}_committed_{period_label}",
                        gpu_name=normalize_gpu_name(display_name),
                        gpu_memory_gb=mem_gb,
                        gpu_count=1,
                        pricing_type="reserved",
                        commitment_period=period_label,
                        price_per_hour=commit_price,
                        price_per_gpu_hour=commit_price,
                        available=True,
                        raw_extra=json.dumps({"tier": f"committed_{period_label}", "gpu_id": gpu_id}, separators=(",", ":")),
                    ))

            # Lowest price / bid info
            lowest = gpu.get("lowestPrice") or {}
            bid_price = lowest.get("minimumBidPrice")
            if bid_price and bid_price > 0:
                rows.append(self.make_row(
                    provider="runpod",
                    instance_type=f"{gpu_id}_bid_min",
                    gpu_name=normalize_gpu_name(display_name),
                    gpu_memory_gb=mem_gb,
                    gpu_count=1,
                    pricing_type="spot",
                    price_per_hour=bid_price,
                    price_per_gpu_hour=bid_price,
                    available=True,
                    raw_extra=json.dumps({"tier": "minimum_bid", "gpu_id": gpu_id}, separators=(",", ":")),
                ))

        logger.info(f"[runpod] Total: {len(rows)} rows across {len(gpu_types)} GPU types")
        return rows

    def _post_graphql(self, payload: bytes, api_key: str) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "OpenComputePrices/1.0",
        }
        try:
            return self._post_graphql_once(API_URL, payload, headers)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:500]
            if e.code not in (401, 403):
                raise RuntimeError(f"{e}; body={body}") from e
            logger.warning(f"[runpod] Bearer auth failed ({e.code}); retrying with api_key query parameter")
            fallback_url = f"{API_URL}?{urllib.parse.urlencode({'api_key': api_key})}"
            fallback_headers = dict(headers)
            fallback_headers.pop("Authorization", None)
            try:
                return self._post_graphql_once(fallback_url, payload, fallback_headers)
            except urllib.error.HTTPError as fallback_error:
                fallback_body = fallback_error.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"{fallback_error}; body={fallback_body}") from fallback_error

    @staticmethod
    def _post_graphql_once(url: str, payload: bytes, headers: dict) -> dict:
        req = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers=headers,
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
