# imputeman/tasks/scrape_tasks.py
"""Scraping tasks for Imputeman pipeline using Prefect"""

from typing import Dict, List, Any
from prefect import task, get_run_logger
import asyncio

from brightdata.models import ScrapeResult
from ..services.scraper_service import ScraperService
from ..core.config import ScrapeConfig


@task(
    name="scrape-urls",
    description="Scrape multiple URLs using BrightData",
    retries=1,
    retry_delay_seconds=5
)
async def scrape_urls_task(
    urls: List[str],
    config: ScrapeConfig
) -> Dict[str, ScrapeResult]:
    """
    Scrape multiple URLs concurrently
    
    Args:
        urls: List of URLs to scrape
        config: Scraping configuration
        
    Returns:
        Dictionary mapping URL to ScrapeResult
    """
    logger = get_run_logger()
    logger.info(f"Starting to scrape {len(urls)} URLs")
    
    # Initialize scraper service
    scraper = ScraperService(config)
    
    try:
        # Execute scraping
        results = await scraper.scrape_urls(urls)
        
        # Log results summary
        successful = sum(1 for r in results.values() if r.status == "ready")
        failed = len(results) - successful
        
        logger.info(f"Scraping completed: {successful} successful, {failed} failed")
        
        # Log individual results
        for url, result in results.items():
            if result.status == "ready":
                data_size = len(result.data) if result.data else 0
                cost = getattr(result, 'cost', 0.0)
                logger.debug(f"✅ {url[:50]}... - {data_size:,} chars, ${cost:.4f}")
            else:
                logger.warning(f"❌ {url[:50]}... - Status: {result.status}")
        
        return results
        
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise
    finally:
        await scraper.close()


@task(
    name="analyze-scrape-costs",
    description="Analyze costs and success metrics from scraping results"
)
async def analyze_scrape_costs_task(
    scrape_results: Dict[str, ScrapeResult]
) -> Dict[str, Any]:
    """
    Analyze scraping results for costs and success metrics
    
    Args:
        scrape_results: Dictionary of URL to ScrapeResult
        
    Returns:
        Dictionary with cost analysis and metrics
    """
    logger = get_run_logger()
    
    total_cost = 0.0
    successful_scrapes = 0
    failed_scrapes = 0
    total_chars = 0
    costs_by_url = {}
    
    for url, result in scrape_results.items():
        # Get cost (default to 0 if not available)
        cost = getattr(result, 'cost', 0.0) or 0.0
        total_cost += cost
        costs_by_url[url] = cost
        
        if result.status == "ready":
            successful_scrapes += 1
            if result.data:
                total_chars += len(result.data)
        else:
            failed_scrapes += 1
    
    # Calculate averages
    avg_cost_per_url = total_cost / len(scrape_results) if scrape_results else 0
    avg_cost_per_success = total_cost / successful_scrapes if successful_scrapes > 0 else 0
    avg_chars_per_success = total_chars / successful_scrapes if successful_scrapes > 0 else 0
    
    analysis = {
        "total_cost": total_cost,
        "successful_scrapes": successful_scrapes,
        "failed_scrapes": failed_scrapes,
        "total_urls": len(scrape_results),
        "success_rate": successful_scrapes / len(scrape_results) if scrape_results else 0,
        "avg_cost_per_url": avg_cost_per_url,
        "avg_cost_per_success": avg_cost_per_success,
        "total_characters": total_chars,
        "avg_chars_per_success": avg_chars_per_success,
        "costs_by_url": costs_by_url,
        "most_expensive_url": max(costs_by_url.items(), key=lambda x: x[1])[0] if costs_by_url else None,
        "cheapest_url": min(costs_by_url.items(), key=lambda x: x[1])[0] if costs_by_url else None
    }
    
    # Log analysis summary
    logger.info(f"Scrape cost analysis:")
    logger.info(f"  - Total cost: ${total_cost:.4f}")
    logger.info(f"  - Success rate: {analysis['success_rate']:.1%} ({successful_scrapes}/{len(scrape_results)})")
    logger.info(f"  - Avg cost per URL: ${avg_cost_per_url:.4f}")
    logger.info(f"  - Avg cost per success: ${avg_cost_per_success:.4f}")
    logger.info(f"  - Total data scraped: {total_chars:,} characters")
    
    if analysis['most_expensive_url'] and total_cost > 0:
        max_cost = costs_by_url[analysis['most_expensive_url']]
        logger.info(f"  - Most expensive: {analysis['most_expensive_url'][:50]}... (${max_cost:.4f})")
    
    return analysis