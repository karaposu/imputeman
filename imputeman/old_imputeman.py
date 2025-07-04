# imputeman/imputeman.py
from __future__ import annotations

# to run python -m imputeman.imputeman

import asyncio, os, time
from typing import Any, Callable, Dict, List, Optional

from serpengine import SERPEngine
from extracthero import ExtractHero, ItemToExtract
from brightdata import scrape_urls_async, scrape_url
from imputeman.myllmservice import MyLLMService
from imputeman.utils       import _resolve

class Imputeman:
    """
    One self-contained search → scrape → extract worker.
    
    • Blocking path  : `collect_sync`  
    • Non-blocking   : `collect`     

    Exposes intermediate results on:
      - self.search_op       : the SERPEngineOp
      - self.scrape_results  : dict[url -> ScrapeResult]
      - self.extract_ops     : dict[url -> ExtractOp]
    """

    def __init__(
        self,
        *,
        serpengine_cfg:  Optional[dict] = None,
        extracthero_cfg: Optional[dict] = None,
        # credential overrides (env fall-back) ──
        OPENAI_API_KEY:                          str | None = None,
        BRIGHTDATA_TOKEN:                        str | None = None,
        BRIGHTDATA_BROWSERAPI_USERNAME:          str | None = None,
        BRIGHTDATA_BROWSERAPI_PASSWORD:          str | None = None,
        BRIGHTDATA_WEBUNCLOKCER_BEARER:          str | None = None,
        BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING: str | None = None,
        GOOGLE_SEARCH_API_KEY:                   str | None = None,
        GOOGLE_CSE_ID:                           str | None = None,
        # shared LLM service
        my_llm_service:                         MyLLMService | None = None,
        # limits
        scrape_concurrency: int = 8,
    ) -> None:
        # ── resolve secrets ────────────────────────────────────────────
        self.openai_api_key  = _resolve("OPENAI_API_KEY",  OPENAI_API_KEY)
        self.bright_token    = _resolve("BRIGHTDATA_TOKEN", BRIGHTDATA_TOKEN)
        self.browser_user    = _resolve("BRIGHTDATA_BROWSERAPI_USERNAME", BRIGHTDATA_BROWSERAPI_USERNAME)
        self.browser_pass    = _resolve("BRIGHTDATA_BROWSERAPI_PASSWORD", BRIGHTDATA_BROWSERAPI_PASSWORD)
        self.unlock_bearer   = _resolve("BRIGHTDATA_WEBUNCLOKCER_BEARER", BRIGHTDATA_WEBUNCLOKCER_BEARER)
        self.unlock_appzone  = _resolve("BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING", BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING)
        self.google_api_key  = _resolve("GOOGLE_SEARCH_API_KEY", GOOGLE_SEARCH_API_KEY)
        self.google_cse_id   = _resolve("GOOGLE_CSE_ID", GOOGLE_CSE_ID)

        # ── shared LLM (rate-limited) ──────────────────────────────────
        self.llm = my_llm_service or MyLLMService()

        # ── sub-components ─────────────────────────────────────────────
        serp_cfg = serpengine_cfg or {}
        serp_cfg.setdefault("GOOGLE_SEARCH_API_KEY", self.google_api_key)
        serp_cfg.setdefault("GOOGLE_CSE_ID",         self.google_cse_id)
        self.serp = SERPEngine(**serp_cfg)

        extr_cfg = extracthero_cfg or {}
        self.extractor = ExtractHero(llm=self.llm, **extr_cfg)

        # ── local semaphores ───────────────────────────────────────────
        self._scrape_sem = asyncio.Semaphore(scrape_concurrency)

        # ── placeholders for intermediate results ──────────────────────
        self.search_op:    Any           = None
        self.scrape_results: Dict[str, Any]  = {}
        self.extract_ops:  Dict[str, Any]  = {}

    # ───────────────────────────── PRIVATE HELPERS ────────────────────────

    async def _scrape_one(
        self,
        url: str,
        *,
        poll_interval: int,
        poll_timeout: int,
        fallback_to_browser_api: bool,
        on_progress: Callable[[str,dict],None] | None,
    ) -> Any | None:
        async with self._scrape_sem:
            res_map = await scrape_urls_async(
                [url],
                bearer_token=self.bright_token,
                poll_interval=poll_interval,
                poll_timeout=poll_timeout,
                fallback_to_browser_api=fallback_to_browser_api,
            )
            sr = res_map[url]
            # store raw ScrapeResult
            self.scrape_results[url] = sr
            if on_progress and getattr(sr, "status", None) == "ready":
                on_progress("scraped", {"url": url})
            return sr.data if getattr(sr, "status", None) == "ready" else None

    async def _extract_one(
        self,
        url: str,
        raw: Any,
        schema: List[ItemToExtract],
        on_progress: Callable[[str,dict],None] | None,
    ) -> Any:
        op = await self.extractor.extract_async(
            raw,
            schema,
            text_type="html" if isinstance(raw, str) else "dict",
        )
        # store full ExtractOp
        self.extract_ops[url] = op
        if on_progress:
            on_progress("extracted", {"url": url, "content": op.content})
        return op.content

    # ======================================================================
    #                           PUBLIC – ASYNC
    # ======================================================================
    async def collect(
        self,
        query: str,
        schema: List[ItemToExtract],
        *,
        top_k: int = 20,
        poll_interval: int = 8,
        poll_timeout: int = 180,
        fallback_to_browser_api: bool = False,
        on_progress: Callable[[str, dict], None] | None = None,
    ) -> ImputeOp:
        t0 = time.time()

        # 1️⃣ Search
        self.search_op = await self.serp.collect_async(
            query=query,
            num_urls=top_k,
            output_format="object",
        )
        urls = self.search_op.all_links()
        total = len(urls)

        # 2️⃣ Schedule scrapes
        tasks = {
            asyncio.create_task(
                self._scrape_one(
                    u,
                    poll_interval=poll_interval,
                    poll_timeout=poll_timeout,
                    fallback_to_browser_api=fallback_to_browser_api,
                    on_progress=on_progress,
                )
            ): u for u in urls
        }

        # 3️⃣ Stream scrape → extract
        for i, task in enumerate(asyncio.as_completed(tasks), start=1):
            url = tasks[task]
            raw = await task
            if raw is None:
                continue
            await self._extract_one(url, raw, schema, on_progress)

        elapsed = round(time.time() - t0, 2)

        return ImputeOp(
            query=query,
            what_to_extract=schema,
            serp_op=self.search_op,
            all_links=urls,
            crawl_ops=self.scrape_results,
            extract_ops=self.extract_ops,
            elapsed=elapsed,
        )

    # async def collect(
    #     self,
    #     query: str,
    #     schema: List[ItemToExtract],
    #     *,
    #     top_k: int = 20,
    #     poll_interval: int = 8,
    #     poll_timeout: int = 180,
    #     fallback_to_browser_api: bool = False,
    #     on_progress: Callable[[str,dict],None] | None = None,
    # ) -> Dict[str,Any]:
    #     t0 = time.time()

    #     # 1️⃣  Search
    #     self.search_op = await self.serp.collect_async(
    #         query=query,
    #         num_urls=top_k,
    #         output_format="object",
    #     )
    #     urls = self.search_op.all_links()
    #     total = len(urls)
    #     print(f"{total} links found; scraping started…")
        
    #     # 2️⃣  Schedule scrapes
    #     tasks = {
    #         asyncio.create_task(
    #             self._scrape_one(
    #                 u,
    #                 poll_interval=poll_interval,
    #                 poll_timeout=poll_timeout,
    #                 fallback_to_browser_api=fallback_to_browser_api,
    #                 on_progress=on_progress,
    #             )
    #         ): u
    #         for u in urls
    #     }

    #     # 3️⃣  Stream scrape → extract
    #     results: Dict[str,Any] = {}
    #     for i, task in enumerate(asyncio.as_completed(tasks), start=1):
    #         url = tasks[task]
    #         raw = await task
    #         if raw is None:
    #             print(f"[{i}/{total}] {url!r} ❌ scrape failed")
    #             continue
    #         content = await self._extract_one(url, raw, schema, on_progress)
    #         results[url] = content
    #         print(f"[{i}/{total}] {url!r} ✅ scraped & extracted")

    #     return {
    #         "query":     query,
    #         "hits":      total,
    #         "extracted": len(results),
    #         "elapsed":   round(time.time() - t0, 2),
    #         "results":   results,
    #     }

    # # ======================================================================
    # #                           PUBLIC – SYNC
    # # ======================================================================

    def collect_sync(
        self,
        query: str,
        schema: List[ItemToExtract],
        *,
        top_k: int = 5,
        poll_interval: int = 8,
        poll_timeout: int = 180,
        fallback_to_browser_api: bool = True,
    ) -> Dict[str,Any]:
        t0 = time.time()

        # 1️⃣  Search
        self.search_op = self.serp.collect(
            query=query,
            num_urls=top_k,
            output_format="object",
        )
        urls = self.search_op.all_links()
        total = len(urls)
        print(f"{total} links found; scraping started…")

        # 2️⃣  Sequential scrape → extract
        results: Dict[str,Any] = {}
        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{total}] scraping {url}")
            sr = scrape_url(
                url,
                fallback_to_browser_api=fallback_to_browser_api,
            )
            self.scrape_results[url] = sr
            # print(sr.content)
            if getattr(sr, "status", None) != "ready":
                print(sr)
                print("   ✖ scrape failed; skipping")
                continue

            print("   → scraped, extracting…")
            op = self.extractor.extract(
                sr.data,
                schema,
                text_type="html" if isinstance(sr.data, str) else "dict",
            )
            
            self.extract_ops[url] = op
            results[url] = op.content
            print(op.content)
            print("   ✔ extracted")
         
        return {
            "query":     query,
            "hits":      total,
            "extracted": len(results),
            "elapsed":   round(time.time() - t0, 2),
            "results":   results,
        }

# ──────────────────────────── CLI SMOKE-TEST ─────────────────────────────

if __name__ == "__main__":
    schema = [
        ItemToExtract(
            name="headline",
            desc="Main story headline",
            example="OpenAI launches new model",
        ),
        ItemToExtract(
            name="published_date",
            desc="Publication date (ISO-8601)",
            regex_validator=r"\d{4}-\d{2}-\d{2}",
            example="2025-05-30",
        ),
    ]

    schema = [
        ItemToExtract(
            name="life_story",
            desc="life story in terms of what/how/when a person did",
            # example="OpenAI launches new model",
        )
        # ItemToExtract(
        #     name="published_date",
        #     desc="Publication date (ISO-8601)",
        #     regex_validator=r"\d{4}-\d{2}-\d{2}",
        #     example="2025-05-30",
        # ),
    ]
    
    iman = Imputeman()
   # r=iman.collect_sync("OpenAI news", schema=schema)

    r=iman.collect_sync("Enes Kuzucu", schema=schema)
    
    print(r)



""" 
scraping https://openai.com/news/  (via BrowserAPI)
    
    successful ( len(html)= 1234355 cost=$0.00001, 




"""