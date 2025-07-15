# imputeman/tasks/serp_tasks.py
"""SERP/Search engine tasks using Prefect"""

import time
from typing import List
from prefect import task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

from serpengine.schemes import SerpEngineOp
from ..core.config import SerpConfig, PipelineConfig
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
) -> SerpEngineOp:
    """
    Execute SERP search and return structured results
    
    Args:
        query: Search query string
        config: SERP configuration
        top_k: Number of results to return (overrides config if provided)
        
    Returns:
        SerpEngineOp with search results from multiple channels
    """
    logger = get_run_logger()
    logger.info(f"Starting SERP search for query: '{query}'")
    
    # Create a minimal pipeline config with just SERP config
    from ..core.config import PipelineConfig
    pipeline_config = PipelineConfig()
    pipeline_config.serp_config = config
    
    # Get service registry and perform search
    registry = get_service_registry(pipeline_config)
    result = await registry.serp.search(query, top_k)
    
    # Log summary
    total_results = len(result.results)
    channels_used = len(result.channels)
    total_cost = result.usage.cost
    
    logger.info(
        f"SERP search completed: {total_results} links found from {channels_used} channels "
        f"in {result.elapsed_time:.2f}s (cost: ${total_cost:.4f})"
    )
    
    # Log per-channel breakdown
    for channel in result.channels:
        logger.debug(
            f"  Channel {channel.name}: {len(channel.results)} results, "
            f"${channel.usage.cost:.4f}, {channel.elapsed_time:.2f}s"
        )
    
    return result


@task(
    retries=2,
    retry_delay_seconds=5,
    tags=["serp", "validate"]
)
async def validate_serp_results_task(serp_result: SerpEngineOp) -> SerpEngineOp:
    """
    Validate and filter SERP results
    
    Args:
        serp_result: Raw SERP results to validate
        
    Returns:
        Validated SerpEngineOp with filtered links
    """
    logger = get_run_logger()
    initial_count = len(serp_result.results)
    logger.info(f"Validating {initial_count} SERP results")
    
    if not serp_result.results:
        logger.warning("No SERP results to validate")
        return serp_result
    
    # Extract URLs for validation
    urls = serp_result.all_links()
    
    # Use service for validation (we'll need to create a service instance)
    from ..services.serp_service import SerpService
    from ..core.config import SerpConfig
    
    serp_service = SerpService(SerpConfig())
    
    # Filter results based on validation
    validated_results = []
    for hit in serp_result.results:
        if serp_service._is_valid_url(hit.link):
            validated_results.append(hit)
        else:
            logger.debug(f"Filtered out invalid URL: {hit.link}")
    
    # Update the results
    serp_result.results = validated_results
    
    # Update channel results too
    for channel in serp_result.channels:
        channel_validated = []
        for hit in channel.results:
            if serp_service._is_valid_url(hit.link):
                channel_validated.append(hit)
        channel.results = channel_validated
    
    filtered_count = initial_count - len(validated_results)
    if filtered_count > 0:
        logger.info(f"Filtered out {filtered_count} invalid links, {len(validated_results)} remain")
    
    return serp_result


@task(
    tags=["serp", "extract"]
)
def extract_urls_from_serp_task(serp_result: SerpEngineOp) -> List[str]:
    """
    Extract clean list of URLs from SERP results
    
    Args:
        serp_result: SerpEngineOp result
        
    Returns:
        List of unique URLs
    """
    logger = get_run_logger()
    
    # Use the built-in method
    urls = serp_result.all_links()
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    logger.info(f"Extracted {len(unique_urls)} unique URLs from SERP results")
    return unique_urls