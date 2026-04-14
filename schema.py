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
    "geo_group",              # Geographic display bucket: US East, Europe, APAC, etc.

    # --- Pricing ---
    "pricing_type",           # on_demand, spot, reserved, inference (normalized)
    "commitment_period",      # e.g. "1yr", "3yr", "1wk", "" for on-demand/spot
    "price_per_hour",         # Instance-level hourly price (float, USD)
    "price_per_gpu_hour",     # Per-GPU hourly price (float, USD)
    "upfront_price",          # One-time upfront contract price (float, USD)
    "upfront_price_per_gpu",  # Per-GPU upfront contract price (float, USD)
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

# Provider name normalization map
PROVIDER_NAME_MAP = {
    "do": "digitalocean",
    "digital_ocean": "digitalocean",
    "gcp": "gcp",
    "google_cloud": "gcp",
    "googlecloud": "gcp",
    "lambda_labs": "lambda",
    "lambdalabs": "lambda",
    "massed_compute": "massedcompute",
    "massedcompute": "massedcompute",
    "oci": "oracle",
    "oracle_cloud": "oracle",
    "packet.ai": "packet_ai",
    "packet_ai": "packet_ai",
    "runpod": "runpod",
    "thunder_compute": "thundercompute",
    "thundercompute": "thundercompute",
}

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
    "nvidia a16": "A16", "a16": "A16",
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
    "nvidia p40": "P40", "p40": "P40", "tesla p40": "P40",
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
    "rtx 6000": "RTX 6000", "rtx6000": "RTX 6000",
    "rtx 6000 ada": "RTX 6000 Ada", "rtx6000ada": "RTX 6000 Ada",
    "rtx pro 6000": "RTX PRO 6000", "rtxpro6000": "RTX PRO 6000",
    "nvidia rtx pro 6000": "RTX PRO 6000",
    "rtx pro 6000 se": "RTX PRO 6000 Server Edition",
    "rtxpro6000se": "RTX PRO 6000 Server Edition",
    "pro6000se": "RTX PRO 6000 Server Edition",
    "nvidia rtx pro 6000 server edition": "RTX PRO 6000 Server Edition",
    "rtx pro 6000 server edition": "RTX PRO 6000 Server Edition",
    "rtx pro 6000 workstation edition": "RTX PRO 6000 Workstation Edition",
    "rtxpro6000we": "RTX PRO 6000 Workstation Edition",
    "pro6000we": "RTX PRO 6000 Workstation Edition",
    "rtx pro 6000 blackwell max-q workstation edition": "RTX PRO 6000 Blackwell Max-Q Workstation Edition",
    "rtxpro6000blackwellmaxqworkstationedition": "RTX PRO 6000 Blackwell Max-Q Workstation Edition",
    "rtx a5000": "RTX A5000", "a5000": "RTX A5000", "rtxa5000": "RTX A5000",
    "rtx 5000 ada": "RTX 5000 Ada", "rtx5000ada": "RTX 5000 Ada",
    "rtx a4500": "RTX A4500", "rtxa4500": "RTX A4500",
    "rtx a4000": "RTX A4000", "a4000": "RTX A4000", "rtxa4000": "RTX A4000",
    "rtx a2000": "RTX A2000", "a2000": "RTX A2000", "rtxa2000": "RTX A2000",
    "gtx 1070 ti": "GTX 1070 Ti", "gtx1070ti": "GTX 1070 Ti",
    "gtx 1050 ti": "GTX 1050 Ti", "gtx1050ti": "GTX 1050 Ti",
    # AMD
    "mi300x": "MI300X", "amd mi300x": "MI300X",
    "mi250x": "MI250X", "amd mi250x": "MI250X",
    "radeon pro v520": "Radeon Pro V520",
    # AWS custom
    "trainium": "Trainium", "trainium2": "Trainium2",
    "inferentia": "Inferentia", "inferentia2": "Inferentia2",
    "gaudi": "Gaudi", "gaudi hl-205": "Gaudi",
    "gaudi 2": "Gaudi 2", "gaudi2": "Gaudi 2",
    "gaudi 3": "Gaudi 3", "gaudi3": "Gaudi 3",
    "qualcomm ai100": "Qualcomm AI100",
    # SkyPilot-specific formats
    "t4g": "T4G",
    "gh200": "GH200",
}

INVALID_GPU_NAMES = {
    "1",
    "cpu",
    "gpu",
    "gpu (unspecified)",
    "none",
    "n/a",
    "na",
    "unknown",
    "unspecified",
}

# Regex patterns for fallback normalization
_GPU_REGEX_PATTERNS = [
    (re.compile(r"rtx\s*pro\s*6000\s*blackwell\s*max[-\s]*q\s*workstation\s*edition", re.I),
     lambda m: "RTX PRO 6000 Blackwell Max-Q Workstation Edition"),
    (re.compile(r"rtx\s*pro\s*6000\s*(?:server\s*edition|se)\b", re.I),
     lambda m: "RTX PRO 6000 Server Edition"),
    (re.compile(r"rtx\s*pro\s*6000\s*(?:workstation\s*edition|we)\b", re.I),
     lambda m: "RTX PRO 6000 Workstation Edition"),
    (re.compile(r"(?:nvidia\s*)?rtx\s*pro\s*6000", re.I), lambda m: "RTX PRO 6000"),
    (re.compile(r"(?:nvidia\s*)?rtx\s*a\s*(\d{4})", re.I), lambda m: f"RTX A{m.group(1)}"),
    (re.compile(r"(?:nvidia\s*)?(?:geforce\s*)?rtx\s*(\d{4})", re.I), lambda m: f"RTX {m.group(1)}"),
    (re.compile(r"(?:nvidia\s*)?gtx\s*(\d{4})\s*ti", re.I), lambda m: f"GTX {m.group(1)} Ti"),
    (re.compile(r"(?:nvidia\s*)?gtx\s*(\d{4})", re.I), lambda m: f"GTX {m.group(1)}"),
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
    (re.compile(r"(?:nvidia\s*)?(?:tesla\s*)?p40", re.I), lambda m: "P40"),
    (re.compile(r"(?:nvidia\s*)?b200", re.I), lambda m: "B200"),
    (re.compile(r"(?:nvidia\s*)?gb200", re.I), lambda m: "GB200"),
    (re.compile(r"mi300x", re.I), lambda m: "MI300X"),
    (re.compile(r"gaudi\s*2", re.I), lambda m: "Gaudi 2"),
    (re.compile(r"gaudi\s*3", re.I), lambda m: "Gaudi 3"),
]


def normalize_provider(raw: str) -> str:
    """Normalize provider names and remove presentation-only sponsorship suffixes."""
    if not raw:
        return ""
    key = raw.strip().lower().replace("-", "_").replace(" ", "_")
    key = re.sub(r"_(?:our_)?sponsor(?:ed)?$", "", key)
    key = re.sub(r"_sponsored$", "", key)
    return PROVIDER_NAME_MAP.get(key, key)


def normalize_gpu_name(raw: str) -> str:
    """Normalize a GPU name to canonical form."""
    if not raw:
        return ""
    key = raw.strip().lower()
    if key in INVALID_GPU_NAMES:
        return ""

    # Direct lookup
    if key in GPU_NAME_MAP:
        return GPU_NAME_MAP[key]

    # Try with common separators replaced
    key_normalized = key.replace("-", " ").replace("_", " ")
    key_normalized = re.sub(r"\s+", " ", key_normalized).strip()
    if key_normalized in INVALID_GPU_NAMES:
        return ""
    if key_normalized in GPU_NAME_MAP:
        return GPU_NAME_MAP[key_normalized]

    key_compact = re.sub(r"[^a-z0-9]+", "", key)
    if key_compact in GPU_NAME_MAP:
        return GPU_NAME_MAP[key_compact]

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


# Region → display bucket mapping helpers
COUNTRY_GEO_GROUPS = {
    "US": "US Other",
    "CA": "Canada",
    "MX": "LATAM",
    "BR": "LATAM",
    "CL": "LATAM",
    "AR": "LATAM",
    "CZ": "Europe",
    "DE": "Europe",
    "ES": "Europe",
    "FR": "Europe",
    "GB": "Europe",
    "IE": "Europe",
    "IS": "Europe",
    "IT": "Europe",
    "NL": "Europe",
    "NO": "Europe",
    "PL": "Europe",
    "RO": "Europe",
    "SE": "Europe",
    "CH": "Europe",
    "IL": "Middle East",
    "AE": "Middle East",
    "QA": "Middle East",
    "SA": "Middle East",
    "AU": "APAC",
    "ID": "APAC",
    "IN": "APAC",
    "JP": "APAC",
    "KR": "APAC",
    "MY": "APAC",
    "NZ": "APAC",
    "SG": "APAC",
    "ZA": "Africa",
}

_US_EAST_PREFIXES = ("us-east", "useast", "eastus")
_US_WEST_PREFIXES = ("us-west", "uswest", "westus", "westcentralus")
_US_CENTRAL_PREFIXES = ("us-central", "uscentral", "centralus", "northcentralus", "southcentralus")
_CANADA_PREFIXES = ("ca-", "canada", "cacentr", "caeast")
_EUROPE_PREFIXES = (
    "eu-", "europe", "westeurope", "northeurope", "uk", "germany", "france",
    "switzerland", "sweden", "norway", "italy", "poland", "spain",
)
_APAC_PREFIXES = (
    "ap-", "asia", "eastasia", "southeastasia", "australia", "japan",
    "korea", "centralindia", "southindia", "westindia", "jioindia",
    "indonesia", "malaysia", "newzealand",
)
_LATAM_PREFIXES = ("sa-", "brazil", "southamerica", "mexico", "chile")
_MIDDLE_EAST_PREFIXES = ("me-", "uae", "qatar", "israel", "il-")
_AFRICA_PREFIXES = ("af-", "southafrica")


# ── Pricing type normalization ────────────────────────────────────────────────
# Different providers use different words for the same economic concept.
# We normalize to four values: on_demand, spot, reserved, inference.

_PRICING_TYPE_MAP = {
    # Canonical (pass-through)
    "on_demand": "on_demand",
    "spot": "spot",
    "reserved": "reserved",
    "inference": "inference",
    # Synonyms → spot
    "preemptible": "spot",       # GCP legacy, Vultr
    "interruptible": "spot",     # Vast.ai
    "bid": "spot",               # RunPod bid pricing
    # Synonyms → reserved
    "committed": "reserved",     # GCP CUD, RunPod term pricing
    "commitment": "reserved",
    "savings_plan": "reserved",  # AWS Savings Plans (future-proof)
}


def normalize_pricing_type(raw: str) -> str:
    """Normalize pricing type to one of: on_demand, spot, reserved, inference."""
    if not raw:
        return "on_demand"
    key = raw.strip().lower().replace("-", "_").replace(" ", "_")
    return _PRICING_TYPE_MAP.get(key, raw)


# ── GPU variant normalization ─────────────────────────────────────────────────
# Providers report form factors inconsistently ("SXM5", "sxm", "PCIe", "PCIE").

_VARIANT_MAP = {
    "sxm": "SXM", "sxm4": "SXM", "sxm5": "SXM",
    "pcie": "PCIe", "pci-e": "PCIe", "pci_e": "PCIe",
    "nvl": "NVL", "nvlink": "NVL",
    "hgx": "HGX",
}


def normalize_gpu_variant(raw: str) -> str:
    """Normalize GPU variant/form-factor to consistent casing."""
    if not raw:
        return ""
    key = raw.strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    # Direct lookup
    if key in _VARIANT_MAP:
        return _VARIANT_MAP[key]
    # Try with separators preserved
    key2 = raw.strip().lower()
    if key2 in _VARIANT_MAP:
        return _VARIANT_MAP[key2]
    return raw.strip()


def infer_geo_group(region: str, country: str = "") -> str:
    """Best-effort display bucket from provider region code or country code."""
    raw_region = (region or "").strip()
    raw_country = (country or "").strip().upper()
    if raw_region.upper() in COUNTRY_GEO_GROUPS:
        return COUNTRY_GEO_GROUPS[raw_region.upper()]
    if raw_country in COUNTRY_GEO_GROUPS:
        return COUNTRY_GEO_GROUPS[raw_country]
    if not raw_region:
        return "Unknown"

    r = raw_region.lower().replace("_", "-").replace(" ", "")
    if r.startswith(_US_EAST_PREFIXES):
        return "US East"
    if r.startswith(_US_WEST_PREFIXES):
        return "US West"
    if r.startswith(_US_CENTRAL_PREFIXES):
        return "US Central"
    if r.startswith(_CANADA_PREFIXES):
        return "Canada"
    if r.startswith(_MIDDLE_EAST_PREFIXES):
        return "Middle East"
    if r.startswith(_EUROPE_PREFIXES):
        return "Europe"
    if r.startswith(_APAC_PREFIXES):
        return "APAC"
    if r.startswith(_LATAM_PREFIXES):
        return "LATAM"
    if r.startswith(_AFRICA_PREFIXES):
        return "Africa"
    if r.startswith("us") or r.startswith("usdod") or r.startswith("usgov"):
        return "US Other"
    if r.startswith("na-") or r.startswith("northamerica"):
        return "North America"
    return "Unknown"
