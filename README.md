
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

