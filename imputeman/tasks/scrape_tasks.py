# imputeman/tasks/scrape_tasks.py
"""Web scraping tasks using Prefect"""

from typing import List, Dict
from prefect import task, get_run_logger

from serpengine.schemes import SerpEngineOp
from brightdata.models import ScrapeResult
from ..core.config import ScrapeConfig, BudgetScrapeConfig, PipelineConfig
from ..services import get_service_registry


@task(
    retries=3,
    retry_delay_seconds=5,
    tags=["scrape", "web"]
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
        Dictionary mapping URLs to ScrapeResult objects
    """
    logger = get_run_logger()
    
    if not urls:
        logger.warning("No URLs to scrape")
        return {}
    
    logger.info(f"Starting to scrape {len(urls)} URLs")
    
    # Create pipeline config for service registry
    pipeline_config = PipelineConfig()
    pipeline_config.scrape_config = config
    
    # Use scraper service
    registry = get_service_registry(pipeline_config)
    scrape_results = await registry.scraper.scrape_urls(urls)
    
    # Log results
    successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
    total_cost = sum(r.cost for r in scrape_results.values() if hasattr(r, 'cost') and r.cost)
    
    logger.info(f"Scraping completed: {successful_scrapes}/{len(urls)} successful, total cost: ${total_cost:.2f}")
    
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
    urls: List[str],
    config: BudgetScrapeConfig
) -> Dict[str, ScrapeResult]:
    """
    Budget-conscious scraping with reduced costs
    
    Args:
        urls: List of URLs to scrape
        config: Budget scraping configuration
        
    Returns:
        Dictionary mapping URLs to ScrapeResult objects
    """
    logger = get_run_logger()
    logger.info(f"Starting budget scraping for {len(urls)} URLs")
    
    if not urls:
        logger.warning("No URLs to scrape")
        return {}
    
    # Create pipeline config with budget scrape config
    pipeline_config = PipelineConfig()
    pipeline_config.scrape_config = config  # Use budget config as scrape config
    
    # Use scraper service with budget configuration
    registry = get_service_registry(pipeline_config)
    scrape_results = await registry.scraper.scrape_urls(urls)
    
    # Log results
    successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
    total_cost = sum(r.cost for r in scrape_results.values() if hasattr(r, 'cost') and r.cost)
    
    logger.info(f"Budget scraping completed: {successful_scrapes}/{len(urls)} successful, total cost: ${total_cost:.2f}")
    
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
    
    total_cost = sum(result.cost for result in scrape_results.values() if hasattr(result, 'cost') and result.cost)
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


@task(
    tags=["scrape", "extract-urls"]
)
def extract_urls_from_serp_task(serp_result: SerpEngineOp) -> List[str]:
    """
    Extract URLs from SERP results for scraping
    
    Args:
        serp_result: SerpEngineOp with search results
        
    Returns:
        List of URLs to scrape
    """
    logger = get_run_logger()
    
    # Use the built-in all_links() method
    urls = serp_result.all_links()
    
    logger.info(f"Extracted {len(urls)} URLs from SERP results")
    return urls