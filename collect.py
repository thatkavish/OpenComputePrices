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
import os
import sys
import time
from datetime import datetime, timezone


def _load_dotenv():
    """Load .env files into os.environ (stdlib only, no python-dotenv needed)."""
    root = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(root, ".env"),
        os.path.join(root, "collectors", ".env"),
    ]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value


_load_dotenv()

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
from collectors.skypilot import SkyPilotCollector
from collectors.getdeploying import GetDeployingCollector
from collectors.jarvislabs import JarvisLabsCollector
from collectors.thundercompute import ThunderComputeCollector
from collectors.crusoe import CrusoeCollector
from collectors.novita import NovitaCollector
from collectors.akash import AkashCollector
from collectors.cudo import CudoCollector
from collectors.vultr import VultrCollector
from collectors.paperspace import PaperspaceCollector
from collectors.primeintellect import PrimeIntellectCollector
from collectors.datacrunch import DataCrunchCollector
from collectors.deepinfra import DeepInfraCollector
from collectors.linode import LinodeCollector
from collectors.latitude import LatitudeCollector
from collectors.massedcompute import MassedComputeCollector
from collectors.e2e import E2ECollector
from collectors.voltagepark import VoltageParkCollector
from collectors.denvr import DenvrCollector
from collectors.browser_providers import (
    CoreWeaveBrowserCollector,
    TogetherBrowserCollector,
    HyperstackBrowserCollector,
    GcoreBrowserCollector,
    FirmusBrowserCollector,
    NeysaBrowserCollector,
    GMICloudBrowserCollector,
    LightningAIBrowserCollector,
    SaladBrowserCollector,
    CloreAIBrowserCollector,
    ExabitsBrowserCollector,
    AethirBrowserCollector,
    QubridBrowserCollector,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Registry of all collectors
COLLECTORS = {
    # --- No auth required (APIs) ---
    "aws": AWSCollector,
    "azure": AzureCollector,
    "oracle": OracleCollector,
    "openrouter": OpenRouterCollector,
    "tensordock": TensorDockCollector,
    "skypilot": SkyPilotCollector,
    # --- No auth required (web scrapers) ---
    "getdeploying": GetDeployingCollector,
    "jarvislabs": JarvisLabsCollector,
    "thundercompute": ThunderComputeCollector,
    "crusoe": CrusoeCollector,
    "novita": NovitaCollector,
    "akash": AkashCollector,
    "cudo": CudoCollector,
    "vultr": VultrCollector,
    "paperspace": PaperspaceCollector,
    "deepinfra": DeepInfraCollector,
    "linode": LinodeCollector,
    "latitude": LatitudeCollector,
    "massedcompute": MassedComputeCollector,
    "e2e": E2ECollector,
    "voltagepark": VoltageParkCollector,
    "denvr": DenvrCollector,
    # --- No auth required (Playwright browser scrapers) ---
    "coreweave": CoreWeaveBrowserCollector,
    "together": TogetherBrowserCollector,
    "hyperstack": HyperstackBrowserCollector,
    "gcore": GcoreBrowserCollector,
    "firmus": FirmusBrowserCollector,
    "neysa": NeysaBrowserCollector,
    "gmicloud": GMICloudBrowserCollector,
    "lightningai": LightningAIBrowserCollector,
    "salad": SaladBrowserCollector,
    "cloreai": CloreAIBrowserCollector,
    "exabits": ExabitsBrowserCollector,
    "aethir": AethirBrowserCollector,
    "qubrid": QubridBrowserCollector,
    # --- Free API key required ---
    "shadeform": ShadeformCollector,
    "runpod": RunPodCollector,
    "vastai": VastAICollector,
    "lambda": LambdaCollector,
    "gcp": GCPCollector,
    "primeintellect": PrimeIntellectCollector,
    "datacrunch": DataCrunchCollector,
}

NO_AUTH_COLLECTORS = [
    "aws", "azure", "oracle", "openrouter", "tensordock", "skypilot",
    "getdeploying", "jarvislabs", "thundercompute", "crusoe", "novita",
    "akash", "cudo", "vultr", "paperspace",
    "deepinfra", "linode", "latitude", "massedcompute", "e2e",
    "voltagepark", "denvr",
]
BROWSER_COLLECTORS = [
    "coreweave", "together", "hyperstack", "gcore", "firmus",
    "neysa", "gmicloud", "lightningai", "salad",
    "cloreai", "exabits", "aethir", "qubrid",
]
API_KEY_COLLECTORS = [
    "shadeform", "runpod", "vastai", "lambda", "gcp",
    "primeintellect", "datacrunch",
]


def main():
    parser = argparse.ArgumentParser(description="GPU Cloud Pricing Data Collector")
    parser.add_argument("sources", nargs="*", help="Specific sources to collect (default: all)")
    parser.add_argument("--list", action="store_true", help="List available collectors")
    parser.add_argument("--no-auth-only", action="store_true", help="Only run collectors that need no API key")
    parser.add_argument("--browser", action="store_true", help="Only run Playwright browser-based collectors")
    parser.add_argument("--skip", nargs="*", default=[], help="Collectors to skip")
    parser.add_argument("--no-unify", action="store_true", help="Skip building the unified master database")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable collectors:")
        print(f"{'Name':<17} {'Type':<12} {'Auth':<10} {'Env Var'}")
        print("-" * 65)
        for name, cls in COLLECTORS.items():
            c = cls()
            auth = "API key" if c.requires_api_key else "None"
            env = c.api_key_env_var or "-"
            ctype = "browser" if name in BROWSER_COLLECTORS else "scraper" if name in NO_AUTH_COLLECTORS else "api-key"
            print(f"{name:<17} {ctype:<12} {auth:<10} {env}")
        return

    # Determine which collectors to run
    if args.sources:
        names = args.sources
    elif args.browser:
        names = BROWSER_COLLECTORS
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

    # Build unified master database unless --no-unify
    if not args.no_unify and total_rows > 0:
        logger.info("Building unified master database...")
        try:
            from unify import load_all_sources, unify, save_master, save_inference, MASTER_PATH, INFERENCE_PATH
            all_data = load_all_sources()
            # Separate inference rows from GPU cloud rows
            inference_rows = [r for r in all_data if r.get("pricing_type", "").lower() == "inference"]
            gpu_rows = [r for r in all_data if r.get("pricing_type", "").lower() != "inference"]
            logger.info(f"Separated: {len(gpu_rows):,} GPU cloud rows, {len(inference_rows):,} inference rows")
            # Unify and save GPU cloud data
            unified_gpu = unify(gpu_rows, stats=False)
            save_master(unified_gpu)
            # Unify and save inference data
            if inference_rows:
                unified_inference = unify(inference_rows, stats=False)
                save_inference(unified_inference)
                logger.info(f"Inference database: {len(unified_inference):,} rows → {INFERENCE_PATH}")
        except Exception as e:
            logger.error(f"Unification failed: {e}", exc_info=True)

    # Exit with error if all failed
    if results and all(r["status"] == "error" for r in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
