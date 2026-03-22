## Best sources, ranked by usefulness

### 1 Must-have sources for a serious open dataset

**AWS**

- **Current list pricing:** AWS Price List Bulk API / price files expose current AWS service pricing, including EC2 pricing metadata in bulk. ([AWS Documentation][1])
- **Historical:** EC2 Spot has a native history endpoint plus documented **90-day** spot price history. ([AWS Documentation][2])
- **Why it matters:** one of the very few major clouds with an actual first-party historical series you can query.

**Azure**

- **Current list pricing:** Azure Retail Prices API is explicitly **unauthenticated** and returns retail rates programmatically. ([Microsoft Learn][3])
- **Historical:** no first-party public history that I found.
- **Why it matters:** best hyperscaler source for frictionless current polling.

**Google Cloud**

- **Current list pricing:** Google’s Pricing API / Catalog API exposes public pricing information; Cloud Billing API usage is free of charge. ([Google Cloud Documentation][4])
- **Historical:** no first-party public time series that I found.
- **Caveat:** you generally need to enable the API / use an API key. ([Google Cloud Documentation][5])

**Oracle Cloud Infrastructure**

- **Current list pricing:** OCI publishes a public price list and GPU compute pricing tables, including GPU instance SKUs. ([Oracle][6])
- **Historical:** no first-party public history that I found.
- **Why it matters:** useful for AMD MI-series and some newer accelerator SKUs.

**Vast.ai**

- **Current snapshot:** public marketplace pricing page with real-time pricing, model-level cards, and live marketplace rates. ([Vast AI][7])
- **Historical:** the public pricing UI exposes **30D / 90D / 180D** history views. ([Vast AI][7])
- **Why it matters:** probably one of the highest-value non-hyperscaler sources for open historical collection.

**GetDeploying**

- **Current snapshot:** public GPU index tracking thousands of offerings across dozens of providers. ([GetDeploying][8])
- **Historical:** GPU pages explicitly say they **track availability and price history over time**. ([GetDeploying][9])
- **Why it matters:** best free aggregator for cross-provider normalization.

### 2) Strong current-snapshot sources you should poll yourself

**Shadeform**

- Public docs say it shows availability, pricing, and specs across **15+ cloud providers**, and the API examples show querying instance types sorted by price. ([Shadeform Documentation][10])
- **Historical:** I did not find a public first-party historical series.
- **Caveat:** API access requires an API key, but the docs are public and the platform claims no markup over direct provider pricing. ([Shadeform Documentation][11])

**Prime Intellect**

- Public API docs expose GPU availability and price fields, plus a GPU summary endpoint. ([Prime Intellect Docs][12])
- **Historical:** I did not find public first-party historical series.
- **Caveat:** requires an API key with availability-read permission. ([Prime Intellect Docs][13])

**Akash**

- Public GPU pricing page with real-time availability and “starting at” hourly pricing. ([Akash Network][14])
- **Historical:** I did not find a public historical API/page.
- **Why it matters:** important decentralized supply source.

**Salad**

- Public pricing calculator with hourly prices by GPU class and configuration. ([Salad][15])
- **Historical:** no public first-party history found.
- **Why it matters:** strong source for lower-end / consumer GPU market pricing.

**Hyperstack**

- Public GPU pricing page with on-demand and reservation pricing for multiple GPU models. ([Hyperstack][16])
- **Historical:** no public first-party history found.

**Lambda**

- Public pricing / instances pages with per-GPU-hour pricing for many instance types. ([Lambda][17])
- **Historical:** no public first-party history found.

**JarvisLabs**

- Public pricing page with live hourly prices for H200, H100, A100, RTX-class GPUs. ([Jarvis Labs][18])
- **Historical:** no public first-party history found.

**CUDO Compute**

- Public pricing page with on-demand and committed GPU pricing. ([CUDO Compute][19])
- **Historical:** no public first-party history found.

**Crusoe**

- Public cloud pricing page with GPU-hr pricing for H200/H100 and others. ([Crusoe][20])
- **Historical:** no public first-party history found.

**TensorDock**

- Public site advertises GPU pricing and specific hourly offers like H100 SXM. ([TensorDock][21])
- **Historical:** no public first-party history found.

**Thunder Compute**

- Public pricing page with hourly pricing and public comparisons. ([Thunder Compute][22])
- **Historical:** no public first-party history found.

**DigitalOcean / Paperspace**

- Public pricing page exists for GPU offerings. ([Paperspace][23])
- **Historical:** no public first-party history found.

**Vultr**

- Public pricing pages exist, though some exact on-demand values appear to be surfaced more via deploy/API flows than a static tariff page. ([Vultr][24])
- **Historical:** no public first-party history found.

**Novita**

- Public pricing page plus public docs for GPU instance pricing. ([Novita AI][25])
- **Historical:** no public first-party history found.

**Verda / DataCrunch**

- Public docs/blog pages expose pricing examples and current pricing views. ([Verda Cloud Docs][26])
- **Historical:** I did not find a clean first-party public time series.

**Packet.ai**

- Public site advertises on-demand GPU offerings. ([Packet.ai][27])
- **Historical:** no public first-party history found.

**UpCloud**

- Public pricing pages and calculator exist; GPU pricing is publicly referenced. ([UpCloud][28])
- **Historical:** no public first-party history found.

If your bar is **real historical pricing data that is already available for free**, the strongest sources I found are:

- **AWS Spot**: first-party, native, queryable, but only about **90 days** back. ([AWS Documentation][2])
- **Vast.ai**: public marketplace pages with **30D / 90D / 180D** views. ([Vast AI][7])
- **GetDeploying**: public GPU pages that say they track **price history over time**. ([GetDeploying][9])

Everything else is mostly **current snapshot only**, meaning your plan to poll them regularly is exactly right. ([Shadeform Documentation][10])

For an open dataset, I would start with this collection:

**Tier A**

- AWS
- Azure
- GCP
- OCI
- Vast.ai
- GetDeploying
- Shadeform
- Prime Intellect

These give you the best mix of hyperscaler coverage, marketplace pricing, and cross-provider aggregation. ([AWS Documentation][1])

**Tier B**

- Akash
- Salad
- Hyperstack
- Lambda
- JarvisLabs
- CUDO
- Crusoe
- TensorDock
- Thunder Compute
- DigitalOcean/Paperspace
- Vultr
- Novita
- Verda/DataCrunch
- Packet.ai
- UpCloud ([Akash Network][14])

[1]: https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html?utm_source=chatgpt.com "Calling AWS services and prices using the AWS Price List"
[2]: https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSpotPriceHistory.html?utm_source=chatgpt.com "DescribeSpotPriceHistory - Amazon Elastic Compute Cloud"
[3]: https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices?utm_source=chatgpt.com "Azure Retail Prices REST API overview"
[4]: https://docs.cloud.google.com/billing/docs/how-to/get-pricing-information-api?utm_source=chatgpt.com "Get Google Cloud pricing information | Cloud Billing"
[5]: https://docs.cloud.google.com/billing/v1/how-tos/catalog-api?utm_source=chatgpt.com "Get publicly available Google Cloud pricing information"
[6]: https://www.oracle.com/cloud/price-list/?utm_source=chatgpt.com "OCI Price List"
[7]: https://vast.ai/pricing?srsltid=AfmBOor39m1pl00LQNgJ-Y9AZwhTa9BqsUIlNR6EvsWts9K5r1QWENkC "GPU Pricing — Live Marketplace Rates | Vast.ai"
[8]: https://getdeploying.com/guides/cheapest-gpu-cloud "Cheapest GPU Cloud: Compare Prices Across 55+ Providers"
[9]: https://getdeploying.com/gpus/nvidia-b100 "Nvidia B100 - Price, Specs & Cloud Providers"
[10]: https://docs.shadeform.ai/getting-started/introduction?utm_source=chatgpt.com "Introduction - Shadeform Documentation"
[11]: https://docs.shadeform.ai/api-reference/authentication?utm_source=chatgpt.com "Authentication - Introduction - Shadeform Documentation"
[12]: https://docs.primeintellect.ai/api-reference/availability/get-gpu-availability?utm_source=chatgpt.com "Get Gpu Availability"
[13]: https://docs.primeintellect.ai/api-reference/check-gpu-availability?utm_source=chatgpt.com "Get Availability Information"
[14]: https://akash.network/pricing/gpus/?utm_source=chatgpt.com "GPU Pricing and Availability"
[15]: https://salad.com/pricing?utm_source=chatgpt.com "Salad GPU Cloud Pricing | Rent GPUs from $0.02/hr"
[16]: https://www.hyperstack.cloud/gpu-pricing?utm_source=chatgpt.com "Hyperstack AI Cloud Pricing | On-Demand, Reserved and ..."
[17]: https://lambda.ai/pricing?utm_source=chatgpt.com "AI Cloud Pricing | GPU Compute & AI Infrastructure"
[18]: https://jarvislabs.ai/?utm_source=chatgpt.com "Jarvis Labs: Rent GPUs Online | H100 & A100 GPUs from ..."
[19]: https://www.cudocompute.com/pricing?utm_source=chatgpt.com "Pricing - GPU and CPU cloud resources"
[20]: https://www.crusoe.ai/cloud/pricing?utm_source=chatgpt.com "Crusoe Cloud Pricing for AI Compute & Inference"
[21]: https://tensordock.com/?utm_source=chatgpt.com "TensorDock — Easy & Affordable Cloud GPUs"
[22]: https://www.thundercompute.com/pricing?utm_source=chatgpt.com "Pricing"
[23]: https://www.paperspace.com/pricing?utm_source=chatgpt.com "Pricing | DigitalOcean"
[24]: https://www.vultr.com/pricing/?utm_source=chatgpt.com "More Cloud, Less Money."
[25]: https://novita.ai/pricing?utm_source=chatgpt.com "Pricing"
[26]: https://docs.datacrunch.io/gpu-instances/pricing-and-billing?utm_source=chatgpt.com "Pricing and Billing - Overview | Verda Cloud Docs"
[27]: https://packet.ai/?utm_source=chatgpt.com "Packet.ai - On-Demand GPU Cloud for AI & ML"
[28]: https://upcloud.com/global/pricing/?utm_source=chatgpt.com "Fixed prices and zero-cost data transfer"
