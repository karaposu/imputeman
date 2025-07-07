# imputeman/tasks/serp_tasks.py
"""SERP/Search engine tasks using Prefect"""

import time
from typing import List
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

from ..core.entities import SerpResult
from ..core.config import SerpConfig
from ..services import get_service_registry


@task(
    retries=3,
    retry_delay_seconds=2,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    tags=["serp", "search"]
)
async def search_serp_task(
    query: str, 
    config: SerpConfig,
    top_k: int = None
) -> SerpResult:
    """
    Execute SERP search and return structured results
    
    Args:
        query: Search query string
        config: SERP configuration
        top_k: Number of results to return (overrides config if provided)
        
    Returns:
        SerpResult with links and metadata
    """
    logger = get_run_logger()
    logger.info(f"Starting SERP search for query: '{query}'")
    
    # Get service registry and perform search
    registry = get_service_registry()
    result = await registry.serp.search(query, top_k)
    
    logger.info(f"SERP search completed: {len(result.links)} links found in {result.elapsed_time:.2f}s")
    return result


@task(
    retries=2,
    retry_delay_seconds=5,
    tags=["serp", "validate"]
)
async def validate_serp_results_task(serp_result: SerpResult) -> SerpResult:
    """
    Validate and filter SERP results
    
    Args:
        serp_result: Raw SERP results to validate
        
    Returns:
        Validated SerpResult with filtered links
    """
    logger = get_run_logger()
    logger.info(f"Validating {len(serp_result.links)} SERP results")
    
    if not serp_result.success:
        logger.warning("Received failed SERP result, skipping validation")
        return serp_result
    
    # Use service for validation
    registry = get_service_registry()
    valid_links = await registry.serp.validate_urls(serp_result.links)
    
    # Update result with filtered links
    serp_result.links = valid_links
    serp_result.total_results = len(valid_links)
    serp_result.success = len(valid_links) > 0
    
    if len(valid_links) != len(serp_result.links):
        logger.info(f"Filtered to {len(valid_links)} valid links")
    
    return serp_result