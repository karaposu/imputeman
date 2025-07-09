# imputeman/services/scraper_service.py
"""Web scraping service using BrightData exclusively"""

import asyncio
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

# Use BrightData's ScrapeResult directly
from brightdata.models import ScrapeResult
from ..core.config import ScrapeConfig


class ScraperService:
    """
    Service for handling web scraping operations using BrightData exclusively
    
    All scraping goes through BrightData for consistent, high-quality results.
    """
    
    def __init__(self, config: ScrapeConfig):
        self.config = config
        self._executor = ThreadPoolExecutor(max_workers=config.concurrent_limit)

    # NEW: Streaming scraping method
    def create_scrape_tasks(self, urls: List[str]) -> Dict[asyncio.Task, str]:
        """
        Create individual scrape tasks for streaming processing
        
        Args:
            urls: List of URLs to scrape
            
        Returns:
            Dict mapping asyncio.Task -> URL for use with asyncio.as_completed()
        """
        scrape_tasks = {}
        
        for url in urls:
            # Create individual scrape task for each URL
            task = asyncio.create_task(self._scrape_single_url_as_dict(url))
            scrape_tasks[task] = url
            
        return scrape_tasks
    
    async def _scrape_single_url_as_dict(self, url: str) -> Dict[str, ScrapeResult]:
        """Scrape single URL and return as dict format expected by extractor"""
        result = await self._brightdata_scrape(url)
        return {url: result}
    
    async def scrape_urls(self, urls: List[str]) -> Dict[str, ScrapeResult]:
        """
        Scrape multiple URLs using BrightData (with clean logging)
        """
        if not urls:
            return {}
        
        # Create scraping tasks
        tasks = []
        for url in urls:
            task = asyncio.create_task(self._scrape_single_url(url))
            tasks.append((url, task))
        
        # Execute all scraping tasks concurrently
        scrape_results = {}
        for url, task in tasks:
            try:
                result = await task
                scrape_results[url] = result
            except Exception as e:
                # Create failed result
                scrape_results[url] = ScrapeResult(
                    url=url,
                    status="failed",
                    data=None,
                    cost=0.0,
                    error=str(e)
                )
        
        return scrape_results
    
    async def _scrape_single_url(self, url: str) -> ScrapeResult:
        """
        Scrape a single URL using BrightData
        
        Args:
            url: URL to scrape
            
        Returns:
            BrightData ScrapeResult
        """
        return await self._brightdata_scrape(url)
    
    async def _scrape_single_url_with_semaphore(self, url: str, semaphore: asyncio.Semaphore) -> ScrapeResult:
        """
        Scrape a single URL with semaphore control using BrightData
        
        Args:
            url: URL to scrape
            semaphore: Asyncio semaphore for concurrency control
            
        Returns:
            BrightData ScrapeResult for the URL
        """
        async with semaphore:
            return await self._brightdata_scrape(url)
    
    async def _brightdata_scrape(self, url: str) -> ScrapeResult:
        """
        Scrape using real BrightData service exclusively
        
        Args:
            url: URL to scrape
            
        Returns:
            BrightData ScrapeResult with real costs and data
        """
        try:
            # Use real BrightData integration
            from brightdata.auto import scrape_url_async
            
            # Call actual BrightData service with increased timeout
            brightdata_result = await scrape_url_async(
                url,
                bearer_token=self.config.bearer_token,
                poll_interval=self.config.poll_interval,
                poll_timeout=200,  # Increased timeout to 200 seconds
                fallback_to_browser_api=True,
            )
            
            # Return BrightData result directly (no field mapping needed!)
            if brightdata_result:
                return brightdata_result
            else:
                # Create failed result if BrightData returns None
                return ScrapeResult(
                    success=False,
                    url=url,
                    status="failed",
                    data=None,
                    error="BrightData returned None",
                    cost=0.0
                )
            
        except ImportError:
            return ScrapeResult(
                success=False,
                url=url,
                status="failed",
                data=None,
                error="BrightData module not available - install brightdata package",
                cost=0.0
            )
        except Exception as e:
            return ScrapeResult(
                success=False,
                url=url,
                status="failed",
                data=None,
                error=f"BrightData error: {str(e)}",
                cost=0.0
            )
    
    def calculate_actual_costs(self, scrape_results: Dict[str, ScrapeResult]) -> Dict[str, float]:
        """
        Calculate actual costs from BrightData scraping results
        
        Args:
            scrape_results: Dictionary of URL -> BrightData ScrapeResult
            
        Returns:
            Dictionary with cost metrics
        """
        costs = [result.cost for result in scrape_results.values() if result.cost is not None]
        successful_costs = [result.cost for result in scrape_results.values() 
                           if result.success and result.cost is not None]
        
        return {
            "total_cost": sum(costs),
            "successful_scrapes_cost": sum(successful_costs),
            "avg_cost_per_scrape": sum(costs) / len(costs) if costs else 0.0,
            "avg_cost_per_successful": sum(successful_costs) / len(successful_costs) if successful_costs else 0.0,
        }

    def get_cost_summary(self, scrape_results: Dict[str, ScrapeResult]) -> str:
        """
        Get a human-readable cost summary for BrightData results
        
        Args:
            scrape_results: Dictionary of URL -> BrightData ScrapeResult
            
        Returns:
            Formatted cost summary string
        """
        costs = self.calculate_actual_costs(scrape_results)
        
        return (
            f"💰 Scraping cost: ${costs['total_cost']:.4f}"
        )
    
    async def close(self):
        """Clean up resources"""
        self._executor.shutdown(wait=True)