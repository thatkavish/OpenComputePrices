"""Validation and filtering rules for persisted pricing rows."""

import re

from row_utils import parse_float, parse_raw_extra_dict


def should_keep_existing_row(row: dict) -> bool:
    if is_implausible_akash_outlier(row):
        return False
    if is_noncanonical_akash_price_tier(row):
        return False
    if is_malformed_coreweave_price_only_row(row):
        return False
    if str(row.get("pricing_type", "")).lower() == "inference":
        return True
    gpu_name = str(row.get("gpu_name", "")).strip()
    if not gpu_name:
        return False
    return re.fullmatch(r"\d+(?:\.\d+)?", gpu_name) is None


def is_malformed_coreweave_price_only_row(row: dict) -> bool:
    if row.get("source") != "coreweave" and row.get("provider") != "coreweave":
        return False
    if str(row.get("pricing_type", "")).lower() != "on_demand":
        return False

    gpu_count = parse_float(row.get("gpu_count"))
    price_per_hour = parse_float(row.get("price_per_hour"))
    price_per_gpu_hour = parse_float(row.get("price_per_gpu_hour"))
    if gpu_count <= 0 or price_per_hour <= 0:
        return False
    if abs(gpu_count - round(gpu_count)) <= 1e-6:
        return False

    return abs(gpu_count - price_per_hour) <= 1e-6 and abs(price_per_gpu_hour - 1.0) <= 1e-6


def is_implausible_akash_outlier(row: dict) -> bool:
    if row.get("source") != "akash" and row.get("provider") != "akash":
        return False
    if row.get("gpu_name") != "GTX 1070 Ti":
        return False
    return parse_float(row.get("price_per_gpu_hour")) > 20


def is_noncanonical_akash_price_tier(row: dict) -> bool:
    if row.get("source") != "akash" and row.get("provider") != "akash":
        return False

    raw_extra = parse_raw_extra_dict(row.get("raw_extra", ""))
    price_metric = str(raw_extra.get("price_metric", "") or raw_extra.get("price_tier", "")).strip()
    if price_metric:
        return price_metric not in {"weightedAverage", ""}

    instance_type = str(row.get("instance_type", "")).strip()
    match = re.search(r"_(min|max|avg|med|weightedAverage)$", instance_type)
    if not match:
        return False
    return match.group(1) != "weightedAverage"
