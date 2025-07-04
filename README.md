
## Imputeman

**Imputeman** is a high-level orchestrator that glues together three core capabilities—**search**, **scrape** and **extract**—into a single, easy-to-use library. Its underlying structure is intentionally built upon Google Search, BrightData and OpenAI paid APIs to make it a production-ready data imputation package.

### Submodules & Roles

1. **`serpengine` (SERPEngine)**

   * **Purpose:** Discover relevant URLs for a natural-language query.
   * **How it works:**

     * Calls Google Custom Search API or scrapes Google HTML results.
     * Applies regex, domain or semantic LLM filters.
     * Returns a `SerpEngineOp` with links, cost metrics and per-source stats.

2. **`brightdata.auto` (`scrape_url` / `scrape_urls_async`)**
   
   * **Purpose:** Fetch raw HTML or JSON from each URL.
   * **How it works:**

     * Routes to the correct Bright Data scraper module (Amazon, LinkedIn, etc.) based on URL patterns.
     * Triggers and polls snapshot jobs, returning either structured rows or raw HTML.
     * Falls back to a Playwright-based Brightdata `BrowserAPI` for unsupported or blocked pages.

3. **`extracthero` (ExtractHero + FilterHero + ParseHero)**

   * **Purpose:** Extract structured fields from raw page content.
   * **How it works:**

     * **Filter phase:**

       * If data is raw HTML, a custom HTML reducer shortens tags without disturbing underlying content.
       * Data can be JSON or string; the LLM is queried to filter only the relevant parts.
     * **Parse phase:** Converts filtered text into key–value pairs, applies regex validation, and returns an `ExtractOp`.
   * **Schema:** You supply a `List[ItemToExtract]` defining field names, descriptions, example values, and optional regex validators.

4. **`LLMService`**

   * **Purpose:** Unified LLM gateway for all LLM based semantic operations.
   * **How it works:**
     * myllmservice.py holds all LLM call logic 
     * GenerationRequest objects are created and GenerationResult objects are received which contains
     query results and all relevant metadata such as cost and timestamps etc 
     * Since all LLM call goes through one layer, it controls/manages global RPM/TPM and concurrency limits.
 

---

## Overall Purpose

* **End-to-end data pipeline** in one class:

  1. **Search:** natural-language query → list of URLs
  2. **Scrape:** URLs → raw page data
  3. **Extract:** raw data → structured JSON
* Single entry points (`collect_sync` or async `collect`) replace complex, multi-step boilerplate.



## ImputeHub

**ImputeHub** is the central coordinator for running large-scale, concurrent scraping pipelines powered by Imputeman. It manages job submission, global rate-limits, shared resources and result aggregation across hundreds or thousands of tasks.

### Core Responsibilities

1. **Job Registry**

   * Accepts new scraping jobs (query + extraction schema + options).
   * Assigns each job a unique ID for tracking and cancellation.

2. **Global Rate-Limiting**

   * Maintains a single **Bright Data semaphore** to cap concurrent snapshots.
   * Maintains a single **LLM semaphore** (via shared `MyLLMService`) to cap concurrent API calls.
   * Ensures no Imputeman instance exceeds plan quotas.

3. **Worker Pool**

   * Spawns a configurable number of async workers.
   * Each worker pulls jobs from an `asyncio.Queue`, instantiates an Imputeman, injects shared semaphores, and runs `collect()`.
   * Handles individual job retries, failures and backoff.

4. **Shared Resources**

   * **One** `MyLLMService` for all LLM operations (semantic filters, parsing).
   * **One** `aiohttp.ClientSession` inside Bright Data helpers to reuse TCP connections.
   * Credential/config overrides applied once at ImputeHub init.

5. **Result Aggregation & Metrics**

   * Collects per-job outputs and errors in a results queue.
   * Streams metrics (cost, latency, success-rate) to logs or a metrics endpoint (e.g. Prometheus).
   * Provides `.run_all()` for batch execution or `.submit_job()`/`.get_result()` for streaming.

6. **Operational Control**

   * Exposes methods to **pause**, **resume**, or **cancel** specific jobs or the entire queue.
   * Supports dynamic scaling of worker pool size and semaphore limits at runtime.

---

### High-Level API

```python
from imputehub import ImputeHub, JobConfig, ItemToExtract

hub = ImputeHub(
    max_bright_concurrency=50,
    max_llm_concurrency=25,
    worker_pool_size=20,
    google_key="…",
    bright_token="…",
    openai_key="…"
)

schema = [ItemToExtract(name="title", desc="Page title")]

# Submit jobs
for q in queries:
    hub.submit_job(JobConfig(query=q, schema=schema, top_k=5))

# Run them all and gather results
import asyncio
results = asyncio.run(hub.run_all())
# results: List[Tuple[job_id, output_dict]]
```

---

### Why Use ImputeHub?

* **Scalability:** Orchestrate thousands of parallel pipelines without exceeding external API rate-limits.
* **Simplicity:** One place to configure credentials, concurrency and worker count.
* **Resilience:** Per-job isolation and retry logic prevent cascading failures.
* **Observability:** Centralized metrics and logging for cost, performance, and error rates.
* **Control:** Pause/resume or cancel in-flight jobs; adjust throughput on-the-fly.


