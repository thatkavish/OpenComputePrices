"""
Standardized schema for GPU cloud pricing data.

Every collector normalizes its output to this schema before writing.
Maximum granularity: every dimension that affects price gets its own column.
"""

import re

# Canonical column order for all output CSVs
COLUMNS = [
    # --- Collection metadata ---
    "snapshot_date",          # ISO date when this data was collected (YYYY-MM-DD)
    "snapshot_ts",            # Full ISO timestamp of collection (YYYY-MM-DDTHH:MM:SSZ)
    "source",                 # Collector that produced this row (e.g. "aws", "azure")

    # --- Provider & instance ---
    "provider",               # Cloud provider name (e.g. "aws", "gcp", "lambda")
    "instance_type",          # Provider-specific instance/offer ID (e.g. "p5.48xlarge")
    "instance_family",        # Instance family (e.g. "p5", "NC_A100_v4")

    # --- GPU specification ---
    "gpu_name",               # Canonical GPU name (e.g. "H100", "A100", "RTX 4090")
    "gpu_variant",            # SXM, PCIe, NVL, HGX, etc. Empty if unknown
    "gpu_memory_gb",          # Per-GPU memory in GB (float)
    "gpu_count",              # Number of GPUs in this offering (int)
    "gpu_interconnect",       # NVLink, NVSwitch, PCIe, etc. Empty if unknown

    # --- Instance specs ---
    "vcpus",                  # vCPU count (int or float)
    "ram_gb",                 # System RAM in GB (float)
    "storage_desc",           # Storage description (e.g. "2x1900 NVMe SSD")
    "network_desc",           # Network description (e.g. "100 Gbps")

    # --- Location ---
    "region",                 # Provider-specific region code (e.g. "us-east-1")
    "zone",                   # Availability zone if available
    "country",                # ISO country code if derivable
    "geo_group",              # Geographic grouping: US, EU, APAC, etc.

    # --- Pricing ---
    "pricing_type",           # on_demand, spot, preemptible, reserved, committed, interruptible
    "commitment_period",      # e.g. "1yr", "3yr", "1wk", "" for on-demand/spot
    "price_per_hour",         # Instance-level hourly price (float, USD)
    "price_per_gpu_hour",     # Per-GPU hourly price (float, USD)
    "currency",               # ISO currency code (usually "USD")
    "price_unit",             # "hour", "second", "month" — what the raw price was in

    # --- Availability ---
    "available",              # True/False/null — is this offering currently available?
    "available_count",        # Number of available instances/GPUs if known (int or null)

    # --- Operating environment ---
    "os",                     # Operating system (e.g. "Linux", "Windows")
    "tenancy",                # Shared, Dedicated, Host
    "pre_installed_sw",       # Pre-installed software (e.g. "NA", "SQL Std")

    # --- Extra ---
    "raw_extra",              # JSON string with any additional provider-specific fields
]

# GPU name normalization map
GPU_NAME_MAP = {
    # NVIDIA data center — H-series
    "nvidia h100": "H100", "h100": "H100",
    "h100 sxm": "H100", "h100 pcie": "H100", "h100 sxm5": "H100",
    "h100-sxm": "H100", "h100-80g-sxm5": "H100", "h100-80g-pcie": "H100",
    "h100-80gb": "H100", "h100-80gb-sxm": "H100",
    "h100 80gb": "H100", "h100 80gb sxm": "H100",
    "h100-sxm5-80gb": "H100", "h100-pcie-80gb": "H100",
    "h100-80g-pcie-nvlink": "H100",
    "nvidia h200": "H200", "h200": "H200",
    "h200-sxm": "H200", "h200-141gb": "H200",
    "h200-141g-sxm": "H200",
    # NVIDIA data center — A-series
    "nvidia a100": "A100", "a100": "A100",
    "a100 sxm": "A100", "a100 pcie": "A100", "a100 sxm4": "A100",
    "a100-sxm4-40gb": "A100", "a100-sxm4-80gb": "A100",
    "a100-pcie-40gb": "A100", "a100-pcie-80gb": "A100",
    "a100-80gb": "A100", "a100-80g": "A100", "a100-40gb": "A100",
    "a100-80gb-sxm": "A100", "a100-80gb-pcie": "A100",
    "a100-80g-pcie": "A100", "a100-80g-pcie-nvlink": "A100",
    "a100 80gb": "A100", "a100 40gb": "A100",
    "nvidia a10g": "A10G", "a10g": "A10G", "a10g-24gb": "A10G",
    "nvidia a10": "A10", "a10": "A10", "a10-24gb": "A10",
    "a10-pcie-24gb": "A10",
    "nvidia a40": "A40", "a40": "A40", "a40-pcie-48gb": "A40",
    # NVIDIA data center — L-series
    "nvidia l4": "L4", "l4": "L4", "l4-pcie-24gb": "L4", "l4 pcie": "L4",
    "nvidia l40": "L40", "l40": "L40", "l40-pcie-48gb": "L40",
    "nvidia l40s": "L40S", "l40s": "L40S", "l40s-pcie-48gb": "L40S", "l40s pcie": "L40S",
    # NVIDIA data center — T/V/K/P/M series
    "nvidia t4": "T4", "t4": "T4", "t4-16gb": "T4",
    "nvidia v100": "V100", "v100": "V100",
    "tesla v100": "V100", "tesla v100 sxm2": "V100", "tesla v100 sxm3": "V100",
    "v100-sxm2-16gb": "V100", "v100-sxm2-32gb": "V100",
    "v100-sxm3-32gb": "V100",
    "nvidia k80": "K80", "k80": "K80", "tesla k80": "K80",
    "tesla m60": "Tesla M60", "m60": "Tesla M60",
    "nvidia p100": "P100", "p100": "P100", "tesla p100": "P100",
    "p100-pcie-16gb": "P100",
    "nvidia p4": "P4", "p4": "P4", "tesla p4": "P4",
    # NVIDIA Blackwell
    "nvidia b200": "B200", "b200": "B200", "b200-192gb": "B200",
    "nvidia b300": "B300", "b300": "B300",
    "nvidia gb200": "GB200", "gb200": "GB200",
    # Consumer / workstation GPUs
    "rtx 4090": "RTX 4090", "geforce rtx 4090": "RTX 4090",
    "nvidia geforce rtx 4090": "RTX 4090",
    "rtx4090": "RTX 4090", "geforcertx4090-pcie-24gb": "RTX 4090",
    "rtx 4080": "RTX 4080", "geforce rtx 4080": "RTX 4080", "rtx4080": "RTX 4080",
    "rtx 3090": "RTX 3090", "geforce rtx 3090": "RTX 3090", "rtx3090": "RTX 3090",
    "geforcertx3090-pcie-24gb": "RTX 3090",
    "rtx 3080": "RTX 3080", "geforce rtx 3080": "RTX 3080", "rtx3080": "RTX 3080",
    "rtx 5090": "RTX 5090", "geforce rtx 5090": "RTX 5090", "rtx5090": "RTX 5090",
    "geforcertx5090-pcie-32gb": "RTX 5090",
    "rtx a6000": "RTX A6000", "a6000": "RTX A6000", "rtxa6000": "RTX A6000",
    "rtx a6000 ada": "RTX A6000",
    "rtx 6000 ada": "RTX 6000 Ada", "rtx6000ada": "RTX 6000 Ada",
    "rtx a5000": "RTX A5000", "a5000": "RTX A5000", "rtxa5000": "RTX A5000",
    "rtx 5000 ada": "RTX 5000 Ada", "rtx5000ada": "RTX 5000 Ada",
    "rtx a4500": "RTX A4500", "rtxa4500": "RTX A4500",
    "rtx a4000": "RTX A4000", "a4000": "RTX A4000", "rtxa4000": "RTX A4000",
    # AMD
    "mi300x": "MI300X", "amd mi300x": "MI300X",
    "mi250x": "MI250X", "amd mi250x": "MI250X",
    "radeon pro v520": "Radeon Pro V520",
    # AWS custom
    "trainium": "Trainium", "trainium2": "Trainium2",
    "inferentia": "Inferentia", "inferentia2": "Inferentia2",
    "gaudi": "Gaudi", "gaudi hl-205": "Gaudi",
    "gaudi 2": "Gaudi 2", "gaudi 3": "Gaudi 3",
    "qualcomm ai100": "Qualcomm AI100",
    # SkyPilot-specific formats
    "t4g": "T4G",
    "gh200": "GH200",
}

# Regex patterns for fallback normalization
_GPU_REGEX_PATTERNS = [
    (re.compile(r"(?:nvidia\s*)?(?:geforce\s*)?rtx\s*(\d{4})", re.I), lambda m: f"RTX {m.group(1)}"),
    (re.compile(r"(?:nvidia\s*)?h(\d00)", re.I), lambda m: f"H{m.group(1)}"),
    (re.compile(r"(?:nvidia\s*)?a100", re.I), lambda m: "A100"),
    (re.compile(r"(?:nvidia\s*)?a10g", re.I), lambda m: "A10G"),
    (re.compile(r"(?:nvidia\s*)?a10(?!\d)", re.I), lambda m: "A10"),
    (re.compile(r"(?:nvidia\s*)?a40", re.I), lambda m: "A40"),
    (re.compile(r"(?:nvidia\s*)?l40s", re.I), lambda m: "L40S"),
    (re.compile(r"(?:nvidia\s*)?l40(?!s)", re.I), lambda m: "L40"),
    (re.compile(r"(?:nvidia\s*)?l4(?!\d)", re.I), lambda m: "L4"),
    (re.compile(r"(?:nvidia\s*)?(?:tesla\s*)?v100", re.I), lambda m: "V100"),
    (re.compile(r"(?:nvidia\s*)?t4(?!\d|g)", re.I), lambda m: "T4"),
    (re.compile(r"(?:nvidia\s*)?(?:tesla\s*)?k80", re.I), lambda m: "K80"),
    (re.compile(r"(?:nvidia\s*)?b200", re.I), lambda m: "B200"),
    (re.compile(r"(?:nvidia\s*)?gb200", re.I), lambda m: "GB200"),
    (re.compile(r"mi300x", re.I), lambda m: "MI300X"),
]


def normalize_gpu_name(raw: str) -> str:
    """Normalize a GPU name to canonical form."""
    if not raw:
        return ""
    key = raw.strip().lower()

    # Direct lookup
    if key in GPU_NAME_MAP:
        return GPU_NAME_MAP[key]

    # Try with common separators replaced
    key_normalized = key.replace("-", " ").replace("_", " ")
    key_normalized = re.sub(r"\s+", " ", key_normalized).strip()
    if key_normalized in GPU_NAME_MAP:
        return GPU_NAME_MAP[key_normalized]

    # Strip trailing specs like "pcie 24gb", "sxm5 80gb"
    key_stripped = re.sub(r"\s*(pcie|sxm\d?|nvlink|hgx)\s*\d*\s*g?b?$", "", key_normalized).strip()
    if key_stripped in GPU_NAME_MAP:
        return GPU_NAME_MAP[key_stripped]

    # Regex fallback
    for pattern, builder in _GPU_REGEX_PATTERNS:
        m = pattern.search(raw)
        if m:
            return builder(m)

    return raw.strip()


# Region → geo group mapping helpers
GEO_PREFIXES = {
    "us-": "US", "ca-": "US",
    "eu-": "EU", "me-": "EU",
    "ap-": "APAC",
    "sa-": "LATAM",
    "af-": "AFRICA",
    "il-": "EU",
    # Azure
    "east": "US", "west": "US", "central": "US", "south": "US", "north": "EU",
}


def infer_geo_group(region: str) -> str:
    """Best-effort geographic grouping from region code."""
    if not region:
        return ""
    r = region.lower()
    for prefix, group in GEO_PREFIXES.items():
        if r.startswith(prefix):
            return group
    return ""
