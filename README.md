# GPU Cloud Pricing Tracker

**Open-source historical GPU cloud pricing dataset**, automatically updated daily via GitHub Actions.

Collects pricing data from **40+ GPU cloud providers** across hyperscalers, neoclouds, marketplaces, aggregators, and inference platforms — storing every dimension that affects price at maximum granularity.

## Data Schema

Every row in the dataset contains:

| Column | Description |
|--------|-------------|
| `snapshot_date` | Date of collection (YYYY-MM-DD) |
| `snapshot_ts` | Full ISO timestamp |
| `source` | Collector that produced this row |
| `provider` | Cloud provider (aws, azure, gcp, lambda, etc.) |
| `instance_type` | Provider-specific instance/offer ID |
| `gpu_name` | Canonical GPU name (H100, A100, RTX 4090, etc.) |
| `gpu_variant` | SXM, PCIe, NVL, HGX — empty if unknown |
| `gpu_memory_gb` | Per-GPU memory in GB |
| `gpu_count` | Number of GPUs in this offering |
| `gpu_interconnect` | NVLink, NVSwitch, PCIe, etc. |
| `vcpus` | vCPU count |
| `ram_gb` | System RAM in GB |
| `region` | Provider-specific region code |
| `zone` | Availability zone |
| `pricing_type` | on_demand, spot, preemptible, reserved, committed, interruptible, inference |
| `commitment_period` | 1yr, 3yr, 1wk, etc. |
| `price_per_hour` | Instance-level hourly price (USD) |
| `price_per_gpu_hour` | Per-GPU hourly price (USD) |
| `available` | Whether this offering is currently available |
| `available_count` | Number available if known |
| `os` | Operating system |
| `raw_extra` | JSON with additional provider-specific fields |

Full schema in [`schema.py`](schema.py).

## Data Sources

### No Authentication Required
| Source | Coverage | Notes |
|--------|----------|-------|
| **AWS Bulk Price List** | All GPU EC2 instances, all regions | On-demand pricing for p2–p6, g3–g6, trn, inf families |
| **Azure Retail Prices** | All GPU VMs, all regions | On-demand + spot + reserved |
| **Oracle Cloud** | GPU instances | Uniform cross-region pricing |
| **OpenRouter** | 300+ inference models, 60+ providers | Per-token pricing |
| **TensorDock** | 45+ GPU models, marketplace | On-demand + spot |
| **Infracost** | AWS + Azure + GCP (3M+ prices) | Free GraphQL API |
| **SkyPilot Catalog** | 10+ clouds via open-source CSVs | AWS, GCP, Azure, Lambda, RunPod, Vast, FluidStack, etc. |

### Free API Key Required
| Source | Coverage | Env Var |
|--------|----------|---------|
| **Shadeform** | 21+ providers via single API | `SHADEFORM_API_KEY` |
| **RunPod** | 30+ GPU types, spot + committed | `RUNPOD_API_KEY` |
| **Vast.ai** | 17K+ GPUs, marketplace pricing | `VASTAI_API_KEY` |
| **Lambda Cloud** | H100, H200, A100, B200 | `LAMBDA_API_KEY` |
| **GCP Billing Catalog** | All GPU SKUs, all regions | `GCP_API_KEY` |

## Quick Start

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/gpu-pricing-tracker.git
cd gpu-pricing-tracker

# Run all no-auth collectors
python collect.py --no-auth-only

# Run specific collectors
python collect.py aws azure openrouter

# Run all (requires API keys in env)
export SHADEFORM_API_KEY=your_key
export RUNPOD_API_KEY=your_key
python collect.py

# List available collectors
python collect.py --list
```

No external dependencies — uses only the Python standard library.

## GitHub Actions

The workflow at `.github/workflows/collect.yml` runs daily at 06:00 UTC and:

1. Runs all collectors (skips API-key ones if secrets not configured)
2. Appends new rows to `data/{source}.csv`
3. Commits and pushes changes automatically

### Setup

1. Fork/clone this repo
2. Add API keys as [repository secrets](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions):
   - `SHADEFORM_API_KEY` — [Get free key](https://shadeform.ai)
   - `RUNPOD_API_KEY` — [Get free key](https://runpod.io)
   - `VASTAI_API_KEY` — [Get free key](https://vast.ai)
   - `LAMBDA_API_KEY` — [Get free key](https://lambdalabs.com)
   - `GCP_API_KEY` — [Get free key](https://console.cloud.google.com/apis/credentials)
3. Enable GitHub Actions in your fork
4. Optionally trigger manually via Actions → "Collect GPU Pricing Data" → "Run workflow"

## File Structure

```
gpu-pricing-tracker/
├── collect.py              # Main entry point
├── schema.py               # Standardized schema & GPU name normalization
├── collectors/
│   ├── base.py             # Base collector class
│   ├── aws.py              # AWS Bulk Price List (no auth)
│   ├── azure.py            # Azure Retail Prices (no auth)
│   ├── oracle.py           # OCI pricing (no auth)
│   ├── openrouter.py       # OpenRouter models (no auth)
│   ├── tensordock.py       # TensorDock marketplace (no auth)
│   ├── infracost.py        # Infracost GraphQL (no auth)
│   ├── skypilot.py         # SkyPilot Catalog CSVs (no auth)
│   ├── shadeform.py        # Shadeform aggregator (free key)
│   ├── runpod.py           # RunPod GraphQL (free key)
│   ├── vastai.py           # Vast.ai marketplace (free key)
│   ├── lambda_cloud.py     # Lambda Cloud (free key)
│   └── gcp.py              # GCP Billing Catalog (free key)
├── data/                   # Collected pricing CSVs (one per source)
├── .github/workflows/
│   └── collect.yml         # Daily GitHub Actions workflow
├── requirements.txt        # No external deps
└── README.md
```

## License

MIT
