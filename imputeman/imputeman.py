# imputeman/imputeman.py

# python -m imputeman.imputeman
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from serpengine import SERPEngine
from serpengine.schemes import SerpEngineOp
from extracthero import ExtractHero, WhatToRetain
from extracthero.schemes import ExtractOp
from brightdata.auto import scrape_urls_async, scrape_url
from brightdata.models import ScrapeResult
from imputeman.myllmservice import MyLLMService
from imputeman.utils import _resolve


@dataclass
class ImputeOp:
    """
    Encapsulates one end-to-end run of Imputeman:
      - the original query & schema
      - the raw SERP & link list
      - per-URL scrape & extract results
      - summary metrics
    """
    query: str
    schema: List[WhatToRetain]
    search_op: SerpEngineOp
    links: List[str]
    scrape_results_dict: Dict[str, ScrapeResult]
    extract_ops: Dict[str, ExtractOp]
    hits: int
    extracted: int
    elapsed: float
    results: Dict[str, Any]


class Imputeman:
    """
    One self-contained search → scrape → extract worker.

    • Async entrypoint: `await run(...)`
    • Sync  entrypoint: `run_sync(...)`

    Intermediate state exposed on:
      - self.search_op
      - self.scrape_results_dict
      - self.extract_ops
    """

    def __init__(
        self,
        *,
        serpengine_cfg: Optional[dict] = None,
        extracthero_cfg: Optional[dict] = None,
        OPENAI_API_KEY: Optional[str] = None,
        BRIGHTDATA_TOKEN: Optional[str] = None,
        BRIGHTDATA_BROWSERAPI_USERNAME: Optional[str] = None,
        BRIGHTDATA_BROWSERAPI_PASSWORD: Optional[str] = None,
        BRIGHTDATA_WEBUNCLOKCER_BEARER: Optional[str] = None,
        BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING: Optional[str] = None,
        GOOGLE_SEARCH_API_KEY: Optional[str] = None,
        GOOGLE_CSE_ID: Optional[str] = None,
        my_llm_service: Optional[MyLLMService] = None,
        scrape_concurrency: int = 8,
    ) -> None:
        # Resolve credentials (env‐fallback)
        self.openai_api_key = _resolve("OPENAI_API_KEY", OPENAI_API_KEY)
        self.bright_token = _resolve("BRIGHTDATA_TOKEN", BRIGHTDATA_TOKEN)
        self.browser_user = _resolve(
            "BRIGHTDATA_BROWSERAPI_USERNAME", BRIGHTDATA_BROWSERAPI_USERNAME
        )
        self.browser_pass = _resolve(
            "BRIGHTDATA_BROWSERAPI_PASSWORD", BRIGHTDATA_BROWSERAPI_PASSWORD
        )
        self.unlock_bearer = _resolve(
            "BRIGHTDATA_WEBUNCLOKCER_BEARER", BRIGHTDATA_WEBUNCLOKCER_BEARER
        )
        self.unlock_appzone = _resolve(
            "BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING", BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING
        )
        self.google_api_key = _resolve("GOOGLE_SEARCH_API_KEY", GOOGLE_SEARCH_API_KEY)
        self.google_cse_id = _resolve("GOOGLE_CSE_ID", GOOGLE_CSE_ID)

        # Shared, rate-limited LLM
        self.llm = my_llm_service or MyLLMService()

        # Subcomponents
        serp_cfg = serpengine_cfg or {}
        serp_cfg.setdefault("GOOGLE_SEARCH_API_KEY", self.google_api_key)
        serp_cfg.setdefault("GOOGLE_CSE_ID", self.google_cse_id)
        self.serp = SERPEngine(**serp_cfg)

        extr_cfg = extracthero_cfg or {}
        self.extractor = ExtractHero(llm=self.llm, **extr_cfg)

        # Control concurrency of Bright Data jobs
        self._scrape_sem = asyncio.Semaphore(scrape_concurrency)

        # Placeholders for intermediate state
        self.search_op: Optional[SerpEngineOp] = None
        self.scrape_results_dict: Dict[str, ScrapeResult] = {}
        self.extract_ops: Dict[str, ExtractOp] = {}
    
    # ────────────────────────────── Helpers ──────────────────────────────
    
    def run_serpop(self, query: str, top_k: int) -> List[str]:
        """Sync search → populate self.search_op & return links."""
        self.search_op = self.serp.collect(
            query=query, num_urls=top_k, output_format="object"
        )
        return self.search_op.all_links()

    async def run_serpop_async(self, query: str, top_k: int) -> List[str]:
        """Async search → populate self.search_op & return links."""
        self.search_op = await self.serp.collect_async(
            query=query, num_urls=top_k, output_format="object"
        )
        return self.search_op.all_links()
    
    async def _scrape_one(
        self,
        url: str,
        *,
        poll_interval: int,
        poll_timeout: int,
        fallback_to_browser_api: bool,
        on_progress: Callable[[str, dict], None] | None,
    ) -> Any | None:
        """Internal: trigger & poll one URL."""
        async with self._scrape_sem:
            res = await scrape_urls_async(
                [url],
                bearer_token=self.bright_token,
                poll_interval=poll_interval,
                poll_timeout=poll_timeout,
                fallback_to_browser_api=fallback_to_browser_api,
            )
            sr = res[url]
            self.scrape_results_dict[url] = sr
            if on_progress and getattr(sr, "status", None) == "ready":
                on_progress("scraped", {"url": url})
            return sr.data if getattr(sr, "status", None) == "ready" else None

    async def _extract_one(
        self,
        url: str,
        raw: Any,
        schema: List[WhatToRetain],
        on_progress: Callable[[str, dict], None] | None,
    ) -> Any:
        """Internal: run ExtractHero.async on raw HTML/JSON."""
        op = await self.extractor.extract_async(
            raw,
            schema,
            text_type="html" if isinstance(raw, str) else "dict",
        )
        self.extract_ops[url] = op
        if on_progress:
            on_progress("extracted", {"url": url, "content": op.content})
        return op.content

    # ───────────────────────────── Public – Async ─────────────────────────

    async def run(
        self,
        query: str,
        schema: List[WhatToRetain],
        *,
        top_k: int = 5,
        poll_interval: int = 8,
        poll_timeout: int = 180,
        fallback_to_browser_api: bool = False,
        on_progress: Callable[[str, dict], None] | None = None,
    ) -> ImputeOp:
        """
        Fully-async end-to-end run.
        """
        
        self.run_custom_keyword_imputation()

        t0 = time.time()
        links = await self.run_serpop_async(query, top_k)
        total = len(links)
        print(f"{total} links found; scraping started…")
        
        tasks: Dict[asyncio.Task, str] = {
            asyncio.create_task(
                self._scrape_one(
                    url,
                    poll_interval=poll_interval,
                    poll_timeout=poll_timeout,
                    fallback_to_browser_api=fallback_to_browser_api,
                    on_progress=on_progress,
                )
            ): url
            for url in links
        }

        results: Dict[str, Any] = {}
        for i, task in enumerate(asyncio.as_completed(tasks), start=1):
            url = tasks[task]
            raw = await task
            if raw is None:
                sr = self.scrape_results_dict.get(url)
                print(f"[{i}/{total}] {url!r} ❌ scrape failed")
                print(sr)
                print(" ")
                print(" ")
                continue
            content = await self._extract_one(url, raw, schema, on_progress)
            results[url] = content
            print(f"[{i}/{total}] {url!r} ✅ scraped & extracted")

        elapsed = round(time.time() - t0, 2)
        return ImputeOp(
            query=query,
            schema=schema,
            search_op=self.search_op,  # type: ignore
            links=links,
            scrape_results_dict=self.scrape_results_dict,
            extract_ops=self.extract_ops,
            hits=total,
            extracted=len(results),
            elapsed=elapsed,
            results=results,
        )
    
    # ───────────────────────────── Public – Sync ──────────────────────────

    def run_sync(
        self,
        query: str,
        schema: List[WhatToRetain],
        *,
        top_k: int = 5,
        poll_interval: int = 8,
        poll_timeout: int = 180,
        fallback_to_browser_api: bool = True,
    ) -> ImputeOp:
        """
        Blocking variant — convenient for quick tests or notebooks.
        """
        t0 = time.time()
        links = self.run_serpop(query, top_k)
        total = len(links)


        print(" ") 

        for e in links:
            print(e)
        
        print(" ") 
        print(f"{total} links found; scraping started…")
        
        results: Dict[str, Any] = {}
        for i, url in enumerate(links, start=1):
            print(f"[{i}/{total}] scraping {url}")
            sr = scrape_url(
                url,
                bearer_token=self.bright_token,
                poll_interval=poll_interval,
                poll_timeout=poll_timeout,
                fallback_to_browser_api=fallback_to_browser_api,
            )
            self.scrape_results_dict[url] = sr
            if getattr(sr, "status", None) != "ready":
                print("   ✖ scrape failed; skipping")
                print(sr)
                print(" ")
                print(" ")
                continue
    #           html_char_size: int | None = None
    # row_count: Optional[int] = None
    # field_count: Optional[int] = None
            print("   → scraped:")
            print(f" html_char_size: {sr.html_char_size}, row_count: {sr.row_count}, field_count : {sr.field_count}, cost : {sr.cost}")
            print(" ")
            print("Extracting…")
            op = self.extractor.extract(
                sr.data,
                schema,
                text_type="html" if isinstance(sr.data, str) else "dict",
            )
            print("Extracted:")


#  success: bool                   # Whether filtering succeeded
#     content: Any                    # The filtered corpus (text) for parsing
#     usage: Optional[Dict[str, Any]] # LLM usage stats (tokens, cost, etc.)
#     elapsed_time: float             # Time in seconds that the filter step took
#     config: ExtractConfig           # The ExtractConfig used for this filter run
#     reduced_html: Optional[str]     # Reduced HTML (if HTMLReducer was applied)
#     html_reduce_op:     Optional[Any] = None   # holds the full Domreducer ReduceOperation object


# class ReduceOperation:
#     # ── required (no defaults) ──────────────────────────────────
#     success: bool
#     js_method_needed: bool
#     total_char: int
#     total_token: int
#     raw_data: str

#     # ── optional (have defaults) ────────────────────────────────
#     reduced_data: Optional[str] = None
#     reduced_total_char: Optional[int] = None
#     reduced_total_token: Optional[int] = None
#     token_reducement_percentage: Optional[float] = None
#     error: Optional[str] = None
#     reducement_details: Dict[str, Dict[str, int]] = field(default_factory=dict)


            # op.filter_op.html_reduce_op.total_token

            print(f"html_token_reduction: {op.filter_op.html_reduce_op.total_token}->>>{op.filter_op.html_reduce_op.reduced_total_token}")
            print(f"filtering elapsed_time: {op.filter_op.elapsed_time}")
            print(f"parser elapsed_time: {op.parse_op.elapsed_time}")
            
          

            print("   → extraction finished")
            self.extract_ops[url] = op
            results[url] = op.content
            print("   ✔ extracted")
        
        elapsed = round(time.time() - t0, 2)
        return ImputeOp(
            query=query,
            schema=schema,
            search_op=self.search_op,  # type: ignore
            links=links,
            scrape_results_dict=self.scrape_results_dict,
            extract_ops=self.extract_ops,
            hits=total,
            extracted=len(results),
            elapsed=elapsed,
            results=results,
        )


# ─────────────────────────── CLI SMOKE-TEST ────────────────────────────

if __name__ == "__main__":

    
    schema = [
        
        WhatToRetain(
            name="attributes",
            desc="all technical attributes",
            # example="OpenAI launches GPT-4",
        ),
        # WhatToRetain(
        #     name="headline",
        #     desc="Main story headline",
        #     example="OpenAI launches GPT-4",
        # ),
        # WhatToRetain(
        #     name="published_date",
        #     desc="Publication date (ISO-8601)",
        #     regex_validator=r"\d{4}-\d{2}-\d{2}",
        #     example="2025-06-01",
        # ),
    ]

    
    # schema = [
    #     WhatToRetain(
    #         name="life_story",
    #         desc="life story in terms of what/how/when a person did",
    #         # example="OpenAI launches new model",
    #     )
       
    # ]



    # schema = [
    #     WhatToRetain(
    #         name="packaging",
    #         desc="used in electronical components to state ",
    #         # example="OpenAI launches new model",
    #     )
       
    # ]
    
    
    iman = Imputeman()
    
    impute_op = iman.run_sync("bav99", schema=schema, top_k=1)
    # print(impute_op.results)
    
    
    
    print(impute_op)
