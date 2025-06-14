# imputeman.py
from __future__ import annotations
import os, asyncio, time
from typing import List, Dict, Any, Optional

from serpengine import SERPEngine
from extracthero import ExtractHero, ItemToExtract
from brightdata.auto import scrape_urls_async, scrape_urls, scrape_url
from imputeman.myllmservice import MyLLMService  # whatever concrete subclass you use
from imputeman.utils import _resolve


class Imputeman:
    # ───────────── init ─────────────
    def __init__(
        self,
        *,
        serpengine_cfg: dict | None = None,
        extracthero_cfg: dict | None = None,
        # credential overrides (all optional)
        OPENAI_API_KEY: str | None = None,
        BRIGHTDATA_TOKEN: str | None = None,
        BRIGHTDATA_BROWSERAPI_USERNAME: str | None = None,
        BRIGHTDATA_BROWSERAPI_PASSWORD: str | None = None,
        BRIGHTDATA_WEBUNCLOKCER_BEARER: str | None = None,
        BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING: str | None = None,
        GOOGLE_SEARCH_API_KEY: str | None = None,
        GOOGLE_CSE_ID: str | None = None,
        # shared LLM service
        # my_llm_service: MyLLMService | None = None,
        my_llm_service = None,
        # concurrency
        scrape_concurrency: int = 8,
    ):
        # ── credentials (env fall-back) ──
        self.openai_api_key   = _resolve("OPENAI_API_KEY",  OPENAI_API_KEY)
        self.bright_token     = _resolve("BRIGHTDATA_TOKEN", BRIGHTDATA_TOKEN)
        self.browser_user     = _resolve("BRIGHTDATA_BROWSERAPI_USERNAME", BRIGHTDATA_BROWSERAPI_USERNAME)
        self.browser_pass     = _resolve("BRIGHTDATA_BROWSERAPI_PASSWORD", BRIGHTDATA_BROWSERAPI_PASSWORD)
        self.unlock_bearer    = _resolve("BRIGHTDATA_WEBUNCLOKCER_BEARER", BRIGHTDATA_WEBUNCLOKCER_BEARER)
        self.unlock_appzone   = _resolve("BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING", BRIGHTDATA_WEBUNCLOKCER_APP_ZONE_STRING)
        self.google_api_key   = _resolve("GOOGLE_SEARCH_API_KEY", GOOGLE_SEARCH_API_KEY)
        self.google_cse_id    = _resolve("GOOGLE_CSE_ID", GOOGLE_CSE_ID)

        # ── shared LLM service ──
        self.llm = my_llm_service or MyLLMService()

        # ── sub-components ──
        serp_cfg = serpengine_cfg or {}
        serp_cfg.setdefault("GOOGLE_SEARCH_API_KEY", self.google_api_key)
        serp_cfg.setdefault("GOOGLE_CSE_ID",        self.google_cse_id)

        self.serp = SERPEngine(  **serp_cfg )
        
        extr_cfg = extracthero_cfg or {}
        self.extractor = ExtractHero(
            llm=self.llm,
            **extr_cfg
        )

        # ── internal controls ──
        self._scrape_sem = asyncio.Semaphore(scrape_concurrency)

    async def collect(
        self,
        query: str,
        schema: List[ItemToExtract],
        *,
        top_k: int = 20,
        poll_interval: int = 8,
        poll_timeout: int = 180,
    ) -> Dict[str, Any]:
        t0 = time.time()

        # 1️⃣ Search
        search_op = self.serp.collect(
            query=query,
            num_urls=top_k,
            output_format="object",
        )
        urls: List[str] = search_op.all_links()
        total = len(urls)
        print(f"{total} links found; scraping started…")

        # 2️⃣ Helpers
        async def _scrape_one(url: str) -> Any | None:
            async with self._scrape_sem:
                res_map = await scrape_urls_async(
                    [url],
                    bearer_token=self.bright_token,
                    poll_interval=poll_interval,
                    poll_timeout=poll_timeout,
                    fallback_to_browser_api=False,
                )
                sr = res_map[url]
                return sr.data if getattr(sr, "status", None) == "ready" else None

        async def _extract_one(url: str, raw: Any) -> Any:
            op = self.extractor.extract(
                raw,
                schema,
                text_type="html" if isinstance(raw, str) else "dict",
            )
            return op.content

        # 3️⃣ Spawn scrape tasks
        scrape_tasks: Dict[asyncio.Task, str] = {
            asyncio.create_task(_scrape_one(url)): url
            for url in urls
        }

        # 4️⃣ Stream scrape → extract
        extracted: Dict[str, Any] = {}
        for i, task in enumerate(asyncio.as_completed(scrape_tasks), start=1):
            url = scrape_tasks[task]
            raw = await task
            if raw is not None:
                content = await _extract_one(url, raw)
                extracted[url] = content
                print(f"[{i}/{total}] {url!r} scraped and extracted.")
            else:
                print(f"[{i}/{total}] {url!r} scrape failed; skipping.")

        elapsed = round(time.time() - t0, 2)
        return {
            "query":   query,
            "hits":    total,
            "extracted": len(extracted),
            "elapsed": elapsed,
            "results": extracted,
        }
    
    
    def collect_sync(
        self,
        query: str,
        schema: List[ItemToExtract],
        *,
        top_k: int = 20,
        poll_interval: int = 8,
        poll_timeout: int = 180,
    ) -> Dict[str, Any]:
        t0 = time.time()

        # 1️⃣ Search (sync)
        search_op = self.serp.collect(
            query=query,
            num_urls=top_k,
            output_format="object",
        )
        urls: List[str] = search_op.all_links()
        total = len(urls)
        print(f"{total} links found; scraping started…")

        # 2️⃣ Sequential scrape → extract
        extracted: Dict[str, Any] = {}
        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{total}] scraping {url}")
            # scrape sync

            sr = scrape_url(
                url,
                bearer_token=self.bright_token,
                poll_interval=poll_interval,
                poll_timeout=poll_timeout,
                fallback_to_browser_api=True,
            )
            # res_map = scrape_urls(
            #     [url],
            #     bearer_token=self.bright_token,
            #     poll_interval=poll_interval,
            #     poll_timeout=poll_timeout,
            #     fallback=False,
            # )
            # sr = res_map.get(url)
            if getattr(sr, "status", None) == "ready":
                raw = sr.data
                print(f"  → scraped, extracting…")
                op = self.extractor.extract(
                    raw,
                    schema,
                    text_type="html" if isinstance(raw, str) else "dict",
                )
                extracted[url] = op.content
                print(f"  ✔ extracted")
            else:
                print(f"  ✖ scrape failed ({getattr(sr, 'error', 'unknown')}); skipping")

        elapsed = round(time.time() - t0, 2)
        return {
            "query":     query,
            "hits":      total,
            "extracted": len(extracted),
            "elapsed":   elapsed,
            "results":   extracted,
        }

