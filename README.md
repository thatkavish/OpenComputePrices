# GPU Cloud Pricing Tracker

**Open-source historical GPU cloud pricing dataset**, automatically updated daily via GitHub Actions.

Collects pricing data from **40+ GPU cloud providers** across hyperscalers, neoclouds, marketplaces, aggregators, and inference platforms — storing every dimension that affects price at maximum granularity.

## Data Schema

Every row in the dataset contains:

| Column               | Description                                                                 |
| -------------------- | --------------------------------------------------------------------------- |
| `snapshot_date`      | Date of collection (YYYY-MM-DD)                                             |
| `snapshot_ts`        | Full ISO timestamp                                                          |
| `source`             | Collector that produced this row                                            |
| `provider`           | Cloud provider (aws, azure, gcp, lambda, etc.)                              |
| `instance_type`      | Provider-specific instance/offer ID                                         |
| `gpu_name`           | Canonical GPU name (H100, A100, RTX 4090, etc.)                             |
| `gpu_variant`        | SXM, PCIe, NVL, HGX — empty if unknown                                      |
| `gpu_memory_gb`      | Per-GPU memory in GB                                                        |
| `gpu_count`          | Number of GPUs in this offering                                             |
| `gpu_interconnect`   | NVLink, NVSwitch, PCIe, etc.                                                |
| `vcpus`              | vCPU count                                                                  |
| `ram_gb`             | System RAM in GB                                                            |
| `region`             | Provider-specific region code                                               |
| `zone`               | Availability zone                                                           |
| `pricing_type`       | on_demand, spot, preemptible, reserved, committed, interruptible, inference |
| `commitment_period`  | 1yr, 3yr, 1wk, etc.                                                         |
| `price_per_hour`     | Instance-level hourly price (USD)                                           |
| `price_per_gpu_hour` | Per-GPU hourly price (USD)                                                  |
| `available`          | Whether this offering is currently available                                |
| `available_count`    | Number available if known                                                   |
| `os`                 | Operating system                                                            |
| `raw_extra`          | JSON with additional provider-specific fields                               |

Full schema in [`schema.py`](schema.py).

## Databases

This project maintains two separate unified databases:

### `_master.csv` — GPU Cloud Pricing

Contains 115K+ rows of GPU compute pricing (per-hour, per-GPU-hour) from cloud providers, neoclouds, and marketplaces.

| Metric              | Value                                                |
| ------------------- | ---------------------------------------------------- |
| GPU Types           | 271 (H100, A100, L40S, L4, T4, V100, etc.)           |
| Providers           | 65+ (AWS, Azure, GCP, RunPod, Lambda, Vast.ai, etc.) |
| Pricing Types       | on_demand, spot, preemptible, reserved, committed    |
| Geographic Coverage | Global (US, EU, APAC, LATAM, Africa)                 |

### `_inference.csv` — Model Inference Pricing

Contains 600+ rows of LLM inference pricing (per-token) from inference providers and APIs.

| Metric           | Value                                                    |
| ---------------- | -------------------------------------------------------- |
| Models           | 600+ (Llama, GPT, Claude, Mistral, Qwen, DeepSeek, etc.) |
| Providers        | 4+ (OpenRouter, DeepInfra, Novita, Together)             |
| Pricing Unit     | Per-token (input/output)                                 |
| Model Categories | Chat, embeddings, vision, code generation                |

### Database Separation

The databases are automatically separated during the unification process:

- Rows with `pricing_type=inference` → `_inference.csv`
- All other pricing types → `_master.csv`

## Data Sources

### No Authentication Required — APIs

| Source                  | Coverage                                 | ~Rows/day | Notes                                                       |
| ----------------------- | ---------------------------------------- | --------- | ----------------------------------------------------------- |
| **AWS Bulk Price List** | All GPU EC2 instances, 27 regions        | ~99K      | On-demand pricing for p2–p6, g3–g6, trn, inf (17 GPU types) |
| **Azure Retail Prices** | All GPU VMs (NC/ND/NV series)            | ~8,500    | On-demand + spot + reserved, all regions                    |
| **Oracle Cloud**        | GPU instances (H100, A100, MI300X, etc.) | ~33       | Uniform cross-region pricing                                |
| **OpenRouter**          | 300+ inference models, 60+ providers     | ~320      | Per-token pricing (inference, not GPU-hour)                 |
| **TensorDock**          | GPU marketplace (locations + hostnodes)  | ~40       | Per-GPU hourly pricing with live availability               |
| **SkyPilot Catalog**    | 15 clouds via open-source CSVs           | ~12K      | AWS, GCP, Azure, Lambda, RunPod, Vast, OCI, Nebius, etc.    |
| **Akash Network**       | 18 GPU models, decentralized marketplace | ~55       | Min/max/avg/weighted pricing + availability                 |
| **CUDO Compute**        | GPU machine types across data centers    | ~9        | Per-GPU hourly pricing per datacenter                       |
| **Vultr**               | GPU cloud plans (A16, A40, A100, L40S)   | ~58       | On-demand + preemptible via public API                      |

### No Authentication Required — Web Scrapers

| Source              | Coverage                             | ~Rows/day   | Notes                                              |
| ------------------- | ------------------------------------ | ----------- | -------------------------------------------------- |
| **GetDeploying**    | 55+ providers, 18 GPU models         | ~400        | Best free aggregator; per-provider pricing per GPU |
| **JarvisLabs**      | H200, H100, A100, RTX, L4            | ~12         | Clean HTML tables with specs                       |
| **Thunder Compute** | A6000, A100, H100                    | ~4          | JSON-LD structured data                            |
| **Crusoe Cloud**    | B200, H200, H100, A100, L40S, MI300X | ~8          | GPU-hr pricing from HTML                           |
| **Novita AI**       | 80+ inference models                 | ~81         | Per-token pricing tables                           |
| **Hyperstack**      | H100, H200, A100, L40                | best-effort | JS-rendered; extracts what’s in static HTML        |
| **Salad**           | Consumer + datacenter GPUs           | best-effort | JS-rendered                                        |
| **Paperspace**      | H100, A100, RTX, A6000               | best-effort | JS-rendered                                        |

### Free API Key Required

| Source                  | Coverage                                     | Env Var                  |
| ----------------------- | -------------------------------------------- | ------------------------ |
| **Shadeform**           | 21+ providers via single API                 | `SHADEFORM_API_KEY`      |
| **RunPod**              | 30+ GPU types, spot + committed + bid        | `RUNPOD_API_KEY`         |
| **Vast.ai**             | 17K+ GPUs, on-demand + interruptible         | `VASTAI_API_KEY`         |
| **Lambda Cloud**        | H100, H200, A100, B200                       | `LAMBDA_API_KEY`         |
| **GCP Billing Catalog** | All GPU SKUs, all regions                    | `GCP_API_KEY`            |
| **Infracost**           | AWS + Azure + GCP deep coverage              | `INFRACOST_API_KEY`      |
| **Prime Intellect**     | GPU availability and pricing                 | `PRIMEINTELLECT_API_KEY` |
| **DataCrunch / Verda**  | Dynamic pricing (changes multiple times/day) | `DATACRUNCH_API_KEY`     |

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

The workflow at `.github/workflows/collect.yml` runs every 6 hours at 00:00, 06:00, 12:00, and 18:00 UTC and:

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
   - `INFRACOST_API_KEY` — [Get free key](https://www.infracost.io/docs/integrations/infracost_api/)
   - `PRIMEINTELLECT_API_KEY` — [Get free key](https://docs.primeintellect.ai)
   - `DATACRUNCH_API_KEY` — [Get free key](https://datacrunch.io)
   - `TENSORDOCK_API_KEY` — [Get free key](https://dashboard.tensordock.com/developers) (optional)
3. Enable GitHub Actions in your fork
4. Optionally trigger manually via Actions → "Collect GPU Pricing Data" → "Run workflow"

## File Structure

```
gpu-pricing-tracker/
├── collect.py              # Main entry point (25 collectors registered)
├── schema.py               # Standardized schema & GPU name normalization
├── summary.py              # Quick dataset inspection tool
├── collectors/
│   ├── base.py             # Base collector class (with dedup logic)
│   ├── # --- APIs (no auth) ---
│   ├── aws.py              # AWS Bulk Price List
│   ├── azure.py            # Azure Retail Prices
│   ├── oracle.py           # OCI pricing
│   ├── openrouter.py       # OpenRouter inference models
│   ├── tensordock.py       # TensorDock v2 marketplace
│   ├── skypilot.py         # SkyPilot Catalog (15 clouds)
│   ├── akash.py            # Akash Network GPU marketplace
│   ├── cudo.py             # CUDO Compute REST API
│   ├── vultr.py            # Vultr public plans API
│   ├── # --- Web scrapers (no auth) ---
│   ├── getdeploying.py     # GetDeploying aggregator (55+ providers)
│   ├── jarvislabs.py       # JarvisLabs pricing tables
│   ├── thundercompute.py   # Thunder Compute JSON-LD
│   ├── crusoe.py           # Crusoe Cloud pricing
│   ├── novita.py           # Novita AI inference pricing
│   ├── hyperstack.py       # Hyperstack (best-effort)
│   ├── salad.py            # Salad (best-effort)
│   ├── paperspace.py       # Paperspace/DigitalOcean (best-effort)
│   ├── # --- APIs (free key required) ---
│   ├── shadeform.py        # Shadeform aggregator
│   ├── runpod.py           # RunPod GraphQL
│   ├── vastai.py           # Vast.ai marketplace
│   ├── lambda_cloud.py     # Lambda Cloud
│   ├── gcp.py              # GCP Billing Catalog
│   ├── infracost.py        # Infracost GraphQL
│   ├── primeintellect.py   # Prime Intellect
│   └── datacrunch.py       # DataCrunch / Verda
├── data/                   # Collected pricing CSVs (one per source)
├── .github/workflows/
│   └── collect.yml         # Daily GitHub Actions workflow
├── requirements.txt        # No external deps
└── README.md
```

## License

MIT
