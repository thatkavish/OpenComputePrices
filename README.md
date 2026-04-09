# OpenComputePrices

**Open-source historical GPU and inference cloud pricing dataset**, automatically updated daily via GitHub Actions.

Collects pricing data from **42 collectors across 65+ GPU cloud providers** — hyperscalers, neoclouds, marketplaces, aggregators, and inference platforms — storing every dimension that affects price at maximum granularity.

## Data Schema

Every row in the dataset contains 33 columns:

| Column               | Description                                                   |
| -------------------- | ------------------------------------------------------------- |
| `snapshot_date`      | Date of collection (YYYY-MM-DD)                               |
| `snapshot_ts`        | Full ISO timestamp                                            |
| `source`             | Collector that produced this row                              |
| `provider`           | Cloud provider (aws, azure, gcp, lambda, etc.)                |
| `instance_type`      | Provider-specific instance/offer ID                           |
| `instance_family`    | Instance family (e.g. "p5", "NC_A100_v4")                     |
| `gpu_name`           | Canonical GPU name (H100, A100, RTX 4090, etc.)               |
| `gpu_variant`        | SXM, PCIe, NVL, HGX — normalized casing, empty if unknown     |
| `gpu_memory_gb`      | Per-GPU memory in GB                                          |
| `gpu_count`          | Number of GPUs in this offering                               |
| `gpu_interconnect`   | NVLink, NVSwitch, PCIe, etc.                                  |
| `vcpus`              | vCPU count                                                    |
| `ram_gb`             | System RAM in GB                                              |
| `storage_desc`       | Storage description (e.g. "2x1900 NVMe SSD")                  |
| `network_desc`       | Network description (e.g. "100 Gbps")                         |
| `region`             | Provider-specific region code                                 |
| `zone`               | Availability zone                                             |
| `country`            | ISO country code if derivable                                 |
| `geo_group`          | Geographic grouping (US, EU, APAC, LATAM, AFRICA)             |
| `pricing_type`       | on_demand, spot, reserved, inference (normalized — see below) |
| `commitment_period`  | 1yr, 3yr, 1wk, etc.                                           |
| `price_per_hour`     | Instance-level hourly price (USD)                             |
| `price_per_gpu_hour` | Per-GPU hourly price (USD)                                    |
| `upfront_price`      | One-time upfront contract price in USD (reserved/committed)   |
| `upfront_price_per_gpu` | Per-GPU upfront contract price in USD                      |
| `currency`           | ISO currency code (usually "USD")                             |
| `price_unit`         | What the raw price was in: "hour", "token", etc.              |
| `available`          | Whether this offering is currently available                  |
| `available_count`    | Number of available instances/GPUs if known                   |
| `os`                 | Operating system (e.g. "Linux", "Windows")                    |
| `tenancy`            | Shared, Dedicated, or Host                                    |
| `pre_installed_sw`   | Pre-installed software (e.g. "NA", "SQL Std")                 |
| `raw_extra`          | JSON with additional provider-specific fields                 |

Full schema and GPU name normalization logic in [`schema.py`](schema.py).

### Normalized Terminology

Cloud providers use different words for the same pricing concepts. We normalize to **four canonical values** for `pricing_type`:

| Normalized  | Provider terms mapped                                                                     |
| ----------- | ----------------------------------------------------------------------------------------- |
| `on_demand` | On-demand, pay-as-you-go                                                                  |
| `spot`      | Spot (AWS/Azure/GCP), Preemptible (GCP legacy), Interruptible (Vast.ai), Bid (RunPod)     |
| `reserved`  | Reserved Instances (AWS/Azure), Committed Use Discounts (GCP), Committed pricing (RunPod) |
| `inference` | Per-token model inference pricing (OpenRouter, DeepInfra, Novita, Together)               |

GPU variants are also normalized: `SXM5`→`SXM`, `pcie`→`PCIe`, `nvlink`→`NVL`, etc.

## Data Access

Data is stored as a compressed archive in [GitHub Releases](../../releases/tag/latest-data) (too large for git). Download the latest dataset:

```bash
# Download and extract (requires gh CLI)
gh release download latest-data -p 'data.tar.gz'
mkdir -p data && tar xzf data.tar.gz -C data/

# Or via curl
curl -L https://github.com/thatkavish/OpenComputePrices/releases/download/latest-data/data.tar.gz | tar xz -C data/
```

The archive contains per-source CSVs (`aws.csv`, `azure.csv`, etc.) and two unified databases:

### `_master.csv` — GPU Cloud Pricing

GPU compute pricing (per-hour, per-GPU-hour) from cloud providers, neoclouds, and marketplaces.

| Metric              | Value                                                |
| ------------------- | ---------------------------------------------------- |
| Providers           | 65+ (AWS, Azure, GCP, RunPod, Lambda, Vast.ai, etc.) |
| Pricing Types       | on_demand, spot, reserved                            |
| Geographic Coverage | Global (US, EU, APAC, LATAM, Africa)                 |

### `_inference.csv` — Model Inference Pricing

LLM inference pricing (per-token) from inference providers and APIs.

| Metric           | Value                                        |
| ---------------- | -------------------------------------------- |
| Providers        | 4+ (OpenRouter, DeepInfra, Novita, Together) |
| Pricing Unit     | Per-token (input/output)                     |
| Model Categories | Chat, embeddings, vision, code generation    |

### Database Separation

The databases are automatically separated during the unification step (`unify.py`):

- Rows with `pricing_type=inference` → `_inference.csv`
- All other pricing types → `_master.csv`

## Data Sources

### No Authentication Required — APIs

| Source           | Coverage                                 | Notes                                                    |
| ---------------- | ---------------------------------------- | -------------------------------------------------------- |
| **AWS**          | All GPU EC2 instances, 27 regions        | On-demand + reserved pricing for p2–p6, g3–g6, trn, inf, including upfront RI components |
| **Azure**        | All GPU VMs (NC/ND/NV series)            | On-demand + spot + reserved, all regions                 |
| **Oracle Cloud** | GPU instances (H100, A100, MI300X, etc.) | Uniform cross-region pricing                             |
| **OpenRouter**   | 300+ inference models, 60+ providers     | Per-token pricing (inference, not GPU-hour)              |
| **SkyPilot**     | 15 clouds via open-source CSVs           | AWS, GCP, Azure, Lambda, RunPod, Vast, OCI, Nebius, etc. |
| **Akash**        | 18 GPU models, decentralized marketplace | Min/max/avg/weighted pricing + availability              |
| **CUDO Compute** | GPU machine types across data centers    | Per-GPU hourly pricing per datacenter                    |
| **Vultr**        | GPU cloud plans (A16, A40, A100, L40S)   | On-demand + preemptible via public API                   |
| **Linode**       | GPU plans (A100, L40S, RTX 4000 Ada)     | Public API with regional pricing overrides               |
| **DeepInfra**    | Inference models + GPU instances         | Public models API + pricing page scraping                |

### No Authentication Required — Web Scrapers

| Source              | Coverage                             | Notes                                              |
| ------------------- | ------------------------------------ | -------------------------------------------------- |
| **GetDeploying**    | 55+ providers, 18 GPU models         | Best free aggregator; per-provider pricing per GPU |
| **JarvisLabs**      | H200, H100, A100, RTX, L4            | Clean HTML tables with specs                       |
| **Thunder Compute** | A6000, A100, H100                    | JSON-LD structured data                            |
| **Crusoe Cloud**    | B200, H200, H100, A100, L40S, MI300X | GPU-hr pricing from HTML                           |
| **Novita AI**       | 80+ inference models                 | Per-token pricing tables                           |
| **Paperspace**      | H100, A100, RTX, A6000               | HTML scraping                                      |
| **Latitude.sh**     | Bare-metal GPU servers               | HTML tables with hourly/monthly pricing            |
| **Massed Compute**  | GPU configs with detailed specs      | HTML tables with VRAM, vCPU, RAM, storage          |
| **E2E Networks**    | H100, A100, L40S, V100               | Indian cloud; hourly/monthly/annual pricing        |
| **Voltage Park**    | H100, H200, A100, B200               | JSON-LD structured data + HTML fallback            |
| **Denvr Dataworks** | H100, A100, Gaudi, MI300X            | Embedded JSON + HTML extraction                    |

### No Authentication Required — Playwright Browser Scrapers

These collectors use headless Chromium to render JS-heavy pricing pages. Requires `playwright` (`pip install playwright && python -m playwright install chromium`).

| Source           | Coverage                         | Notes                                  |
| ---------------- | -------------------------------- | -------------------------------------- |
| **CoreWeave**    | H100, H200, B200, A100, L40S     | GPU-hour pricing from rendered DOM     |
| **Together.ai**  | Inference models + GPU instances | Per-token inference + GPU-hour pricing |
| **Hyperstack**   | H100, H200, A100, L40            | Table extraction from rendered page    |
| **Gcore**        | GPU cloud instances              | Table + fallback pattern extraction    |
| **Firmus**       | GPU instances                    | GPU/price pair extraction              |
| **Neysa**        | GPU instances                    | Table extraction with VRAM             |
| **GMI Cloud**    | GPU instances                    | GPU/price pairs + table fallback       |
| **Lightning AI** | GPU accelerators                 | Table extraction from rendered page    |
| **Salad**        | Consumer + datacenter GPUs       | Table + GPU/price pair extraction      |
| **Clore.ai**     | GPU marketplace                  | Table + GPU/price pair extraction      |
| **Exabits**      | GPU instances                    | GPU/price pair extraction              |
| **Aethir**       | GPU instances                    | GPU/price pair extraction              |
| **Qubrid**       | GPU instances                    | Table extraction with VRAM             |

### Free API Key Required

| Source              | Coverage                                     | Env Var                  |
| ------------------- | -------------------------------------------- | ------------------------ |
| **TensorDock**      | GPU marketplace (locations + hostnodes)      | `TENSORDOCK_API_KEY`     |
| **Shadeform**       | 21+ providers via single API                 | `SHADEFORM_API_KEY`      |
| **RunPod**          | 30+ GPU types, spot + committed + bid        | `RUNPOD_API_KEY`         |
| **Vast.ai**         | 17K+ GPUs, on-demand + interruptible         | `VASTAI_API_KEY`         |
| **Lambda Cloud**    | H100, H200, A100, B200                       | `LAMBDA_API_KEY`         |
| **GCP**             | All GPU SKUs, all regions                    | `GCP_API_KEY`            |
| **Prime Intellect** | GPU availability and pricing                 | `PRIMEINTELLECT_API_KEY` |
| **DataCrunch**      | Dynamic pricing (changes multiple times/day) | `DATACRUNCH_API_KEY`     |

## Quick Start

```bash
# Clone and download data
git clone https://github.com/thatkavish/OpenComputePrices.git
cd OpenComputePrices
gh release download latest-data -p 'data.tar.gz'
mkdir -p data && tar xzf data.tar.gz -C data/

# Run all no-auth API + scraper collectors (no Playwright needed)
python collect.py --no-auth-only

# Run all non-browser collectors, including API-key collectors when configured
python collect.py --no-browser

# Run specific collectors
python collect.py aws azure openrouter

# Run a comma-separated list safely (useful in automation)
python collect.py --sources-csv "aws,azure,openrouter"

# Run browser-based collectors (requires Playwright)
pip install playwright && python -m playwright install chromium
python collect.py --browser

# Run all (requires API keys in env)
export SHADEFORM_API_KEY=your_key
export RUNPOD_API_KEY=your_key
python collect.py

# List available collectors
python collect.py --list

# Skip specific collectors
python collect.py --skip aws skypilot

# Build unified databases without collecting
python unify.py --stats

# Inspect the dataset
python summary.py

# Run local checks
PYTHONPYCACHEPREFIX=/tmp/pycache python -m compileall collect.py collectors release_data.py schema.py summary.py unify.py
python -m unittest discover -s tests -v
```

Core collectors use only the Python standard library. Browser-based collectors additionally require [Playwright](https://playwright.dev/python/) — see `requirements.txt`.

## GitHub Actions

The workflow at `.github/workflows/collect.yml` runs every 12 hours at 00:00 and 12:00 UTC across three jobs:

1. **`collect-api`** — Runs all API + scraper collectors and stores intermediate source CSVs
2. **`collect-browser`** — Runs Playwright browser-based collectors on top of the API artifact
3. **`finalize-data`** — Prunes retained source CSVs, rebuilds only the affected snapshot-date unified slices once, and uploads the release

Manual workflow runs respect the `sources` input across the collection jobs:

- The API job runs only the requested non-browser sources
- The browser job runs only the requested browser sources
- `no_auth_only=true` skips browser and API-key collectors entirely

Data is stored in [GitHub Releases](../../releases/tag/latest-data) — the workflow downloads the latest archive once, collects source CSVs in stages, then incrementally re-finalizes only the snapshot dates touched by that run before uploading the updated archive.

**Data lifecycle:**

- **Active window (90 days):** Per-source CSVs in `data/` keep both daily snapshots, capped at 90 days
- **Archive:** Rows older than 90 days are compressed and uploaded as monthly archive assets in the same Release (e.g. `archive_2026-01.csv.gz`)
- **Dedup:** Exact duplicate rows are removed during pruning

### Setup

1. Fork/clone this repo
2. Add API keys as [environment secrets](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions) under an environment named `.env`, or as repository secrets:
   - `TENSORDOCK_API_KEY` — [Get free key](https://dashboard.tensordock.com/developers)
   - `SHADEFORM_API_KEY` — [Get free key](https://shadeform.ai)
   - `RUNPOD_API_KEY` — [Get free key](https://runpod.io)
   - `VASTAI_API_KEY` — [Get free key](https://vast.ai)
   - `LAMBDA_API_KEY` — [Get free key](https://lambdalabs.com)
   - `GCP_API_KEY` — [Get free key](https://console.cloud.google.com/apis/credentials)
   - `PRIMEINTELLECT_API_KEY` — [Get free key](https://docs.primeintellect.ai)
   - `DATACRUNCH_API_KEY` — [Get free key](https://datacrunch.io)
3. Enable GitHub Actions in your fork
4. Optionally trigger manually via Actions → "Collect GPU Pricing Data" → "Run workflow"

## Development

- Project metadata lives in [`pyproject.toml`](pyproject.toml)
- CI runs in [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

## File Structure

```
OpenComputePrices/
├── collect.py                  # Main entry point — runs collectors, builds unified DBs
├── unify.py                    # Merges per-source CSVs into deduplicated master DBs
├── schema.py                   # Standardized 33-column schema & GPU name normalization
├── summary.py                  # Quick dataset inspection tool
├── requirements.txt            # Playwright (optional for browser collectors)
├── collectors/
│   ├── base.py                 # Base collector class with append-only save + prune/archive
│   ├── browser_scraper.py      # Base class for Playwright browser-based scrapers
│   ├── browser_providers.py    # 13 Playwright browser scrapers (CoreWeave, Together, etc.)
│   ├── # --- No-auth APIs ---
│   ├── aws.py                  # AWS Bulk Price List
│   ├── azure.py                # Azure Retail Prices
│   ├── oracle.py               # Oracle Cloud (OCI)
│   ├── openrouter.py           # OpenRouter inference models
│   ├── tensordock.py           # TensorDock v2 marketplace
│   ├── skypilot.py             # SkyPilot Catalog (15 clouds)
│   ├── akash.py                # Akash Network
│   ├── cudo.py                 # CUDO Compute
│   ├── vultr.py                # Vultr
│   ├── linode.py               # Linode (Akamai)
│   ├── deepinfra.py            # DeepInfra models + GPU pricing
│   ├── # --- No-auth scrapers ---
│   ├── getdeploying.py         # GetDeploying aggregator (55+ providers)
│   ├── jarvislabs.py           # JarvisLabs
│   ├── thundercompute.py       # Thunder Compute
│   ├── crusoe.py               # Crusoe Cloud
│   ├── novita.py               # Novita AI inference
│   ├── paperspace.py           # Paperspace / DigitalOcean
│   ├── latitude.py             # Latitude.sh
│   ├── massedcompute.py        # Massed Compute
│   ├── e2e.py                  # E2E Networks
│   ├── voltagepark.py          # Voltage Park
│   ├── denvr.py                # Denvr Dataworks
│   ├── # --- Free API key required ---
│   ├── shadeform.py            # Shadeform aggregator
│   ├── runpod.py               # RunPod
│   ├── vastai.py               # Vast.ai marketplace
│   ├── lambda_cloud.py         # Lambda Cloud
│   ├── gcp.py                  # GCP Billing Catalog
│   ├── primeintellect.py       # Prime Intellect
│   └── datacrunch.py           # DataCrunch / Verda
├── data/                       # Active data (last 90 days, gitignored; download from Releases)
├── .github/workflows/
│   └── collect.yml             # Automated collection (every 12 hours, stores data in Releases)
└── README.md
```

## License

MIT — see [LICENSE](LICENSE).
