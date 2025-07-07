# imputeman/tasks/extract_tasks.py
"""Data extraction tasks using Prefect"""

from typing import Dict, Any, List
from prefect import task, get_run_logger

from ..core.entities import ExtractResult, ScrapeResult, WhatToRetain
from ..core.config import ExtractConfig
from ..services import get_service_registry


@task(
    retries=2,
    retry_delay_seconds=3,
    tags=["extract", "ai"]
)
async def extract_data_task(
    scrape_results: Dict[str, ScrapeResult],
    schema: List[WhatToRetain],
    config: ExtractConfig
) -> Dict[str, ExtractResult]:
    """
    Extract structured data from scraped content
    
    Args:
        scrape_results: Results from web scraping
        schema: Expected data schema for extraction
        config: Extraction configuration
        
    Returns:
        Dictionary mapping URLs to ExtractResult objects
    """
    logger = get_run_logger()
    
    # Filter successful scrapes
    valid_scrapes = {
        url: result for url, result in scrape_results.items()
        if result.status == "ready" and result.data
    }
    
    if not valid_scrapes:
        logger.warning("No valid scrape results for extraction")
        return {}
    
    logger.info(f"Extracting data from {len(valid_scrapes)} scraped pages")
    
    # Use extractor service
    registry = get_service_registry()
    extract_results = await registry.extractor.extract_from_scrapes(valid_scrapes, schema)
    
    # Log results
    successful_extractions = sum(1 for r in extract_results.values() if r.success)
    total_cost = sum(r.cost for r in extract_results.values())
    total_tokens = sum(r.tokens_used for r in extract_results.values())
    
    logger.info(f"Extraction completed: {successful_extractions}/{len(valid_scrapes)} successful")
    logger.info(f"Total cost: ${total_cost:.2f}, Total tokens: {total_tokens}")
    
    return extract_results


# All extraction logic is now handled by the ExtractorService
# The _extract_single_page and _simulate_extraction functions 
# have been moved to the service layer for better separation of concerns


@task(
    tags=["extract", "validate"]
)
async def validate_extractions_task(
    extract_results: Dict[str, ExtractResult],
    config: ExtractConfig
) -> Dict[str, ExtractResult]:
    """
    Validate extraction results and filter by confidence threshold
    
    Args:
        extract_results: Raw extraction results
        config: Extraction configuration with confidence threshold
        
    Returns:
        Filtered extraction results meeting quality criteria
    """
    logger = get_run_logger()
    
    valid_extractions = {}
    low_confidence_count = 0
    
    for url, result in extract_results.items():
        if result.success and result.confidence_score >= config.confidence_threshold:
            valid_extractions[url] = result
        else:
            if result.success:
                low_confidence_count += 1
                logger.debug(f"Filtering out low confidence extraction from {url} (confidence: {result.confidence_score})")
    
    logger.info(f"Validation completed: {len(valid_extractions)} valid extractions")
    if low_confidence_count > 0:
        logger.info(f"Filtered out {low_confidence_count} low-confidence extractions")
    
    return valid_extractions


@task(
    tags=["extract", "aggregate"]
)
async def aggregate_final_data_task(
    extract_results: Dict[str, ExtractResult]
) -> Dict[str, Any]:
    """
    Aggregate extraction results into final dataset
    
    Args:
        extract_results: Validated extraction results
        
    Returns:
        Aggregated final data
    """
    logger = get_run_logger()
    
    if not extract_results:
        logger.warning("No extraction results to aggregate")
        return {}
    
    # Simple aggregation strategy - combine all successful extractions
    final_data = {}
    source_urls = []
    
    for url, result in extract_results.items():
        if result.success and result.content:
            source_urls.append(url)
            # Merge content, handling conflicts by keeping highest confidence
            for key, value in result.content.items():
                if key not in final_data:
                    final_data[key] = {
                        "value": value,
                        "confidence": result.confidence_score,
                        "source": url
                    }
                elif result.confidence_score > final_data[key]["confidence"]:
                    final_data[key] = {
                        "value": value,
                        "confidence": result.confidence_score,
                        "source": url
                    }
    
    # Add metadata
    final_data["_metadata"] = {
        "source_urls": source_urls,
        "total_sources": len(source_urls),
        "aggregation_timestamp": time.time()
    }
    
    logger.info(f"Aggregated data from {len(source_urls)} sources with {len(final_data)-1} fields")
    
    return final_data