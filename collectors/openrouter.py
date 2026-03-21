"""
OpenRouter inference pricing collector.

Freely queryable, no auth required.
Returns per-token pricing for 300+ models across 60+ providers.
"""

import json
import logging
import urllib.request
from typing import List, Dict, Any

from collectors.base import BaseCollector

logger = logging.getLogger(__name__)

API_URL = "https://openrouter.ai/api/v1/models"


class OpenRouterCollector(BaseCollector):
    name = "openrouter"
    requires_api_key = False

    def collect(self) -> List[Dict[str, Any]]:
        logger.info("[openrouter] Fetching model pricing (no auth)")

        try:
            req = urllib.request.Request(API_URL, headers={
                "Accept": "application/json",
                "User-Agent": "gpu-pricing-tracker/1.0",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            logger.error(f"[openrouter] API request failed: {e}")
            return []

        models = data.get("data", [])
        rows = []

        for model in models:
            model_id = model.get("id", "")
            pricing = model.get("pricing", {})
            if not pricing:
                continue

            prompt_price = pricing.get("prompt", "0")
            completion_price = pricing.get("completion", "0")
            image_price = pricing.get("image", "0")

            try:
                prompt_f = float(prompt_price)
                completion_f = float(completion_price)
            except (ValueError, TypeError):
                continue

            # Skip free models
            if prompt_f <= 0 and completion_f <= 0:
                continue

            context_length = model.get("context_length", "")
            top_provider = model.get("top_provider", {}) or {}
            architecture = model.get("architecture", {}) or {}

            rows.append(self.make_row(
                provider="openrouter",
                instance_type=model_id,
                gpu_name="",  # Inference — GPU not specified
                pricing_type="inference",
                price_per_hour=0,  # Token-based, not hourly
                price_per_gpu_hour=0,
                price_unit="token",
                available=True,
                raw_extra=json.dumps({
                    "model_id": model_id,
                    "model_name": model.get("name", ""),
                    "prompt_price_per_token": prompt_price,
                    "completion_price_per_token": completion_price,
                    "image_price": image_price,
                    "context_length": context_length,
                    "max_completion_tokens": top_provider.get("max_completion_tokens", ""),
                    "modality": architecture.get("modality", ""),
                    "tokenizer": architecture.get("tokenizer", ""),
                    "instruct_type": architecture.get("instruct_type", ""),
                    "description": model.get("description", "")[:200],
                }, separators=(",", ":")),
            ))

        logger.info(f"[openrouter] Total: {len(rows)} models")
        return rows
