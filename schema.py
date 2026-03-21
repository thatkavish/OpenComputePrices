"""
Standardized schema for GPU cloud pricing data.

Every collector normalizes its output to this schema before writing.
Maximum granularity: every dimension that affects price gets its own column.
"""

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
    # NVIDIA data center
    "nvidia h100": "H100",
    "h100": "H100",
    "h100 sxm": "H100",
    "h100 pcie": "H100",
    "h100 sxm5": "H100",
    "nvidia h200": "H200",
    "h200": "H200",
    "nvidia a100": "A100",
    "a100": "A100",
    "a100 sxm": "A100",
    "a100 pcie": "A100",
    "a100 sxm4": "A100",
    "nvidia a10g": "A10G",
    "a10g": "A10G",
    "nvidia a10": "A10",
    "a10": "A10",
    "nvidia l4": "L4",
    "l4": "L4",
    "nvidia l40": "L40",
    "l40": "L40",
    "nvidia l40s": "L40S",
    "l40s": "L40S",
    "nvidia t4": "T4",
    "t4": "T4",
    "nvidia v100": "V100",
    "v100": "V100",
    "tesla v100": "V100",
    "nvidia k80": "K80",
    "k80": "K80",
    "tesla k80": "K80",
    "tesla m60": "Tesla M60",
    "nvidia b200": "B200",
    "b200": "B200",
    "nvidia b300": "B300",
    "b300": "B300",
    "nvidia gb200": "GB200",
    "gb200": "GB200",
    "nvidia p100": "P100",
    "p100": "P100",
    "tesla p100": "P100",
    # Consumer
    "rtx 4090": "RTX 4090",
    "geforce rtx 4090": "RTX 4090",
    "rtx 4080": "RTX 4080",
    "rtx 3090": "RTX 3090",
    "geforce rtx 3090": "RTX 3090",
    "rtx 3080": "RTX 3080",
    "rtx 5090": "RTX 5090",
    "geforce rtx 5090": "RTX 5090",
    "rtx a6000": "RTX A6000",
    "a6000": "RTX A6000",
    "rtx a5000": "RTX A5000",
    "a5000": "RTX A5000",
    "rtx a4000": "RTX A4000",
    "a4000": "RTX A4000",
    "rtx 6000 ada": "RTX 6000 Ada",
    # AMD
    "mi300x": "MI300X",
    "amd mi300x": "MI300X",
    "radeon pro v520": "Radeon Pro V520",
    # AWS custom
    "trainium": "Trainium",
    "trainium2": "Trainium2",
    "inferentia": "Inferentia",
    "inferentia2": "Inferentia2",
    "gaudi": "Gaudi",
    "gaudi 2": "Gaudi 2",
    "gaudi 3": "Gaudi 3",
}


def normalize_gpu_name(raw: str) -> str:
    """Normalize a GPU name to canonical form."""
    if not raw:
        return ""
    key = raw.strip().lower()
    return GPU_NAME_MAP.get(key, raw.strip())


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
