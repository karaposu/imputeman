# imputeman/services/fast_path_service.py

import asyncio
import logging
from typing import Dict, Optional, List
from brightdata.models import ScrapeResult
from brightdata.auto import scrape_url_async
from ..core.config import FastPathConfig, FastPathMode
from ..core.entities import EntityToImpute

logger = logging.getLogger(__name__)


class FastPathService:
    """Service for direct domain scraping without SERP or extraction"""
    
    def __init__(self, config: FastPathConfig, bearer_token: str):
        self.config = config
        self.bearer_token = bearer_token
        self._scrape_sem = asyncio.Semaphore(2)  # Limit concurrent fast path scrapes
    
    async def execute_fast_path(
        self, 
        entity: EntityToImpute
    ) -> Dict[str, ScrapeResult]:
        """
        Execute fast path scraping for configured domains
        
        Returns:
            Dict mapping URL to ScrapeResult (raw scrape data, no extraction)
        """
        if not self.config.enabled:
            return {}
        
        # Generate URLs for all enabled domains
        fast_path_urls = self.config.get_fast_path_urls(entity.name)
        
        if not fast_path_urls:
            logger.warning("Fast path enabled but no domains configured")
            return {}
        
        logger.info(f"🚀 Executing fast path for {len(fast_path_urls)} domains")
        
        # Create scraping tasks
        tasks = []
        for domain, url in fast_path_urls.items():
            task = asyncio.create_task(
                self._scrape_fast_path_url(domain, url)
            )
            tasks.append((domain, url, task))
        
        # Execute all fast path scrapes concurrently
        results = {}
        for domain, url, task in tasks:
            try:
                scrape_result = await task
                results[url] = scrape_result
                
                # Log result
                if scrape_result.success:
                    size = len(scrape_result.data) if scrape_result.data else 0
                    logger.info(f"✅ Fast path {domain}: {size:,} chars, ${scrape_result.cost:.4f}")
                else:
                    logger.warning(f"❌ Fast path {domain} failed: {scrape_result.error}")
                    
            except Exception as e:
                logger.error(f"Fast path {domain} error: {e}")
                results[url] = ScrapeResult(
                    success=False,
                    url=url,
                    status="error",
                    error=str(e),
                    cost=0.0
                )
        
        return results
    
    
    
    # imputeman/services/fast_path_service.py

    async def _scrape_fast_path_url(
        self, 
        domain: str, 
        url: str
    ) -> ScrapeResult:
        """Scrape a single fast path URL"""
        
        logger.info(f"⚡ Fast path scraping {domain}: {url[:60]}...")
        
        async with self._scrape_sem:
            try:
                scrape_result = await scrape_url_async(
                    url,
                    bearer_token=self.bearer_token,
                    poll_interval=self.config.poll_interval,
                    poll_timeout=self.config.poll_timeout,
                    fallback_to_browser_api=False,  # Fast path doesn't use browser fallback
                )
                
                # Validate result size based on data type
                if scrape_result and scrape_result.success:
                    if scrape_result.fallback_used:
                        # HTML data - check string length
                        data_size = len(scrape_result.data) if isinstance(scrape_result.data, str) else 0
                    else:
                        # JSON data - check serialized size or array length
                        if isinstance(scrape_result.data, list):
                            # For arrays, more meaningful to check item count
                            data_size = len(scrape_result.data) * 1000  # Rough estimate
                        elif isinstance(scrape_result.data, dict):
                            import json
                            data_size = len(json.dumps(scrape_result.data))
                        else:
                            data_size = len(str(scrape_result.data))
                    
                    if data_size < self.config.min_result_size:
                        logger.warning(
                            f"Fast path {domain} result too small: "
                            f"{data_size} < {self.config.min_result_size} "
                            f"(type: {'HTML' if scrape_result.fallback_used else 'JSON'})"
                        )
                
                return scrape_result
                
            except Exception as e:
                logger.error(f"Fast path scrape failed for {domain}: {e}")
                return ScrapeResult(
                    success=False,
                    url=url,
                    status="error",
                    error=str(e),
                    cost=0.0
                )
        
    def is_fast_path_successful(self, results: Dict[str, ScrapeResult]) -> bool:
        """Check if fast path produced usable results"""
        if not results:
            return False
        
        for url, result in results.items():
            if result.success and result.data:
                if result.fallback_used:
                    # HTML - check character count
                    if isinstance(result.data, str) and len(result.data) >= self.config.min_result_size:
                        return True
                else:
                    # JSON - check if we have actual data
                    if isinstance(result.data, list) and len(result.data) > 0:
                        # At least one item in the array
                        return True
                    elif isinstance(result.data, dict) and len(result.data) > 0:
                        # Non-empty dictionary
                        return True
                    elif result.data:  # Any other non-empty data
                        return True
        
        return False