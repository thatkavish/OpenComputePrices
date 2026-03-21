#!/usr/bin/env python3
"""
Main entry point for GPU pricing data collection.

Usage:
    python collect.py                    # Run all collectors
    python collect.py aws azure          # Run specific collectors
    python collect.py --list             # List available collectors
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from collectors.aws import AWSCollector
from collectors.azure import AzureCollector
from collectors.oracle import OracleCollector
from collectors.openrouter import OpenRouterCollector
from collectors.shadeform import ShadeformCollector
from collectors.runpod import RunPodCollector
from collectors.vastai import VastAICollector
from collectors.tensordock import TensorDockCollector
from collectors.lambda_cloud import LambdaCollector
from collectors.gcp import GCPCollector
from collectors.infracost import InfracostCollector
from collectors.skypilot import SkyPilotCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Registry of all collectors
COLLECTORS = {
    # --- No auth required ---
    "aws": AWSCollector,
    "azure": AzureCollector,
    "oracle": OracleCollector,
    "openrouter": OpenRouterCollector,
    "tensordock": TensorDockCollector,
    "infracost": InfracostCollector,
    "skypilot": SkyPilotCollector,
    # --- Free API key required ---
    "shadeform": ShadeformCollector,
    "runpod": RunPodCollector,
    "vastai": VastAICollector,
    "lambda": LambdaCollector,
    "gcp": GCPCollector,
}

NO_AUTH_COLLECTORS = ["aws", "azure", "oracle", "openrouter", "tensordock", "skypilot"]
API_KEY_COLLECTORS = ["shadeform", "runpod", "vastai", "lambda", "gcp", "infracost"]


def main():
    parser = argparse.ArgumentParser(description="GPU Cloud Pricing Data Collector")
    parser.add_argument("sources", nargs="*", help="Specific sources to collect (default: all)")
    parser.add_argument("--list", action="store_true", help="List available collectors")
    parser.add_argument("--no-auth-only", action="store_true", help="Only run collectors that need no API key")
    parser.add_argument("--skip", nargs="*", default=[], help="Collectors to skip")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable collectors:")
        print(f"{'Name':<15} {'Auth Required':<15} {'Env Var'}")
        print("-" * 55)
        for name, cls in COLLECTORS.items():
            c = cls()
            auth = "API key" if c.requires_api_key else "None"
            env = c.api_key_env_var or "-"
            print(f"{name:<15} {auth:<15} {env}")
        return

    # Determine which collectors to run
    if args.sources:
        names = args.sources
    elif args.no_auth_only:
        names = NO_AUTH_COLLECTORS
    else:
        names = list(COLLECTORS.keys())

    names = [n for n in names if n not in args.skip]

    now = datetime.now(timezone.utc)
    print(f"\n{'='*70}")
    print(f"  GPU Pricing Data Collection — {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Sources: {', '.join(names)}")
    print(f"{'='*70}\n")

    results = {}
    total_rows = 0
    t0 = time.time()

    for name in names:
        if name not in COLLECTORS:
            logger.warning(f"Unknown collector: {name}")
            continue

        cls = COLLECTORS[name]
        collector = cls()

        # Skip if API key required but missing
        if collector.requires_api_key and not collector.get_api_key():
            logger.warning(f"[{name}] Skipping — missing {collector.api_key_env_var}")
            results[name] = {"status": "skipped", "reason": f"missing {collector.api_key_env_var}", "rows": 0}
            continue

        logger.info(f"[{name}] Starting collection...")
        ct0 = time.time()
        try:
            count = collector.run()
            elapsed = time.time() - ct0
            results[name] = {"status": "ok", "rows": count, "elapsed": f"{elapsed:.1f}s"}
            total_rows += count
            logger.info(f"[{name}] Done: {count} rows in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - ct0
            results[name] = {"status": "error", "error": str(e), "rows": 0, "elapsed": f"{elapsed:.1f}s"}
            logger.error(f"[{name}] Failed: {e}", exc_info=True)

    total_elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*70}")
    print(f"  COLLECTION SUMMARY")
    print(f"{'='*70}")
    print(f"  {'Source':<15} {'Status':<10} {'Rows':>8}  {'Time':>8}  Notes")
    print(f"  {'-'*60}")
    for name, res in results.items():
        status = res["status"]
        rows = res.get("rows", 0)
        elapsed = res.get("elapsed", "")
        notes = res.get("reason", "") or res.get("error", "")
        print(f"  {name:<15} {status:<10} {rows:>8}  {elapsed:>8}  {notes}")
    print(f"  {'-'*60}")
    print(f"  {'TOTAL':<15} {'':10} {total_rows:>8}  {total_elapsed:.1f}s")
    print(f"{'='*70}\n")

    # Exit with error if all failed
    if all(r["status"] == "error" for r in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
