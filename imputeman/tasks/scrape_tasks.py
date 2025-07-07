# imputeman/tasks/scrape_tasks.py
"""Web scraping tasks using Prefect"""

from typing import List, Dict
from prefect import task, get_run_logger

from ..core.entities import ScrapeResult, SerpResult
from ..core.config import ScrapeConfig, BudgetScrapeConfig
from ..services import get_service_registry


@task(
    retries=3,
    retry_delay_seconds=5,
    tags=["scrape", "web"]
)
async def scrape_urls_task(
    serp_result: SerpResult,
    config: ScrapeConfig
) -> Dict[str, ScrapeResult]:
    """
    Scrape multiple URLs concurrently
    
    Args:
        serp_result: SERP results containing URLs to scrape
        config: Scraping configuration
        
    Returns:
        Dictionary mapping URLs to ScrapeResult objects
    """
    logger = get_run_logger()
    
    if not serp_result.success or not serp_result.links:
        logger.warning("No valid links to scrape")
        return {}
    
    logger.info(f"Starting to scrape {len(serp_result.links)} URLs")
    
    # Use scraper service
    registry = get_service_registry()
    scrape_results = await registry.scraper.scrape_urls(serp_result.links)
    
    # Log results
    successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
    total_cost = sum(r.cost for r in scrape_results.values())
    
    logger.info(f"Scraping completed: {successful_scrapes}/{len(serp_result.links)} successful, total cost: ${total_cost:.2f}")
    
    # Check if we exceeded cost threshold
    if total_cost > config.max_cost_threshold:
        logger.warning(f"Scraping cost ${total_cost:.2f} exceeded threshold ${config.max_cost_threshold}")
    
    return scrape_results


@task(
    retries=2,
    retry_delay_seconds=3,
    tags=["scrape", "budget", "web"]
)
async def budget_scrape_urls_task(
    serp_result: SerpResult,
    config: BudgetScrapeConfig
) -> Dict[str, ScrapeResult]:
    """
    Budget-conscious scraping with reduced costs
    
    Args:
        serp_result: SERP results containing URLs to scrape
        config: Budget scraping configuration
        
    Returns:
        Dictionary mapping URLs to ScrapeResult objects
    """
    logger = get_run_logger()
    logger.info(f"Starting budget scraping for {len(serp_result.links)} URLs")
    
    # Use the same scraping service but with budget config
    if not serp_result.success or not serp_result.links:
        logger.warning("No valid links to scrape")
        return {}
    
    # Use scraper service with budget configuration
    registry = get_service_registry()
    # Create a temporary scraper with budget config
    budget_scraper = registry.__class__(registry.config)
    budget_scraper.config.scrape_config = config
    
    scrape_results = await budget_scraper.scraper.scrape_urls(serp_result.links)
    
    # Log results
    successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
    total_cost = sum(r.cost for r in scrape_results.values())
    
    logger.info(f"Budget scraping completed: {successful_scrapes}/{len(serp_result.links)} successful, total cost: ${total_cost:.2f}")
    
    return scrape_results


@task(
    tags=["scrape", "analyze"]
)
async def analyze_scrape_costs_task(
    scrape_results: Dict[str, ScrapeResult]
) -> Dict[str, float]:
    """
    Analyze scraping costs and performance
    
    Args:
        scrape_results: Results from scraping operation
        
    Returns:
        Cost analysis metrics
    """
    logger = get_run_logger()
    
    total_cost = sum(result.cost for result in scrape_results.values())
    successful_scrapes = sum(1 for result in scrape_results.values() if result.status == "ready")
    total_scrapes = len(scrape_results)
    
    avg_cost_per_scrape = total_cost / max(successful_scrapes, 1)
    success_rate = successful_scrapes / max(total_scrapes, 1)
    
    analysis = {
        "total_cost": total_cost,
        "successful_scrapes": successful_scrapes,
        "total_scrapes": total_scrapes,
        "avg_cost_per_scrape": avg_cost_per_scrape,
        "success_rate": success_rate
    }
    
    logger.info(f"Scrape analysis: {analysis}")
    return analysis


# All scraping logic is now handled by the ScraperService
# The _scrape_single_url and related helper functions 
# have been moved to the service layer for better separation of concerns