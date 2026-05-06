"""Small helpers for normalizing row-level primitive values."""

import json


def parse_float(raw) -> float:
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


def format_float(value: float) -> str:
    return f"{round(value, 6):.6f}".rstrip("0").rstrip(".") or "0"


def parse_raw_extra_dict(raw_extra: str) -> dict:
    try:
        parsed = json.loads(raw_extra or "")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def dump_raw_extra_dict(raw_extra: dict, fallback: str = "") -> str:
    if raw_extra:
        return json.dumps(raw_extra, separators=(",", ":"), default=str)
    return fallback
