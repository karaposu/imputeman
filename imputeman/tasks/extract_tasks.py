# imputeman/tasks/extract_tasks.py
"""Data extraction tasks using Prefect"""

import time
from typing import Dict, Any, List
from prefect import task, get_run_logger

from ..core.entities import ScrapeResult, WhatToRetain
from ..core.config import ExtractConfig
from ..services import get_service_registry
from extracthero import ExtractOp


@task(
    retries=2,
    retry_delay_seconds=3,
    tags=["extract", "ai"]
)
async def extract_data_task(
    scrape_results: Dict[str, ScrapeResult],
    schema: List[WhatToRetain],
    config: ExtractConfig
) -> Dict[str, ExtractOp]:
    """
    Extract structured data from scraped content
    
    Args:
        scrape_results: Results from web scraping
        schema: Expected data schema for extraction
        config: Extraction configuration
        
    Returns:
        Dictionary mapping URLs to ExtractOp objects
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
    
    # Log results using ExtractOp's built-in metrics
    successful_extractions = sum(1 for r in extract_results.values() if r.success)
    
    # Calculate totals from ExtractOp usage data
    total_cost = 0.0
    total_tokens = 0
    
    for extract_op in extract_results.values():
        if extract_op.usage:
            # Add costs if present in usage dict
            if 'cost' in extract_op.usage:
                total_cost += extract_op.usage['cost']
            # Add tokens if present (could be under various keys)
            for key in ['total_tokens', 'tokens', 'prompt_tokens', 'completion_tokens']:
                if key in extract_op.usage:
                    if key in ['prompt_tokens', 'completion_tokens']:
                        total_tokens += extract_op.usage[key]
                    elif key == 'total_tokens' or key == 'tokens':
                        total_tokens = extract_op.usage[key]  # Use total if available
                        break
    
    logger.info(f"Extraction completed: {successful_extractions}/{len(valid_scrapes)} successful")
    if total_cost > 0 or total_tokens > 0:
        logger.info(f"Total cost: ${total_cost:.2f}, Total tokens: {total_tokens}")
    
    return extract_results


# All extraction logic is now handled by the ExtractorService
# The _extract_single_page and _simulate_extraction functions 
# have been moved to the service layer for better separation of concerns


@task(
    tags=["extract", "validate"]
)
async def validate_extractions_task(
    extract_results: Dict[str, ExtractOp],
    config: ExtractConfig
) -> Dict[str, ExtractOp]:
    """
    Validate extraction results based on success status
    
    Note: ExtractOp doesn't have a confidence_score field, so we validate
    based on the success flag and whether content exists.
    
    Args:
        extract_results: Raw extraction results
        config: Extraction configuration
        
    Returns:
        Filtered extraction results that were successful
    """
    logger = get_run_logger()
    
    valid_extractions = {}
    failed_count = 0
    
    for url, result in extract_results.items():
        if result.success and result.content is not None:
            valid_extractions[url] = result
        else:
            failed_count += 1
            if not result.success:
                logger.debug(f"Filtering out failed extraction from {url}")
            else:
                logger.debug(f"Filtering out extraction with no content from {url}")
    
    logger.info(f"Validation completed: {len(valid_extractions)} valid extractions")
    if failed_count > 0:
        logger.info(f"Filtered out {failed_count} failed/empty extractions")
    
    return valid_extractions


@task(
    tags=["extract", "aggregate"]
)
async def aggregate_final_data_task(
    extract_results: Dict[str, ExtractOp]
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
            # ExtractOp.content can be a dict or other structure
            if isinstance(result.content, dict):
                # Merge content, using most recent values
                for key, value in result.content.items():
                    if key not in final_data:
                        final_data[key] = {
                            "value": value,
                            "source": url,
                            "extraction_time": result.elapsed_time
                        }
                    else:
                        # Keep the value from the fastest extraction (assuming faster = more confident)
                        if result.elapsed_time < final_data[key].get("extraction_time", float('inf')):
                            final_data[key] = {
                                "value": value,
                                "source": url,
                                "extraction_time": result.elapsed_time
                            }
            else:
                # If content is not a dict, store it under a generic key
                final_data[f"content_from_{url}"] = {
                    "value": result.content,
                    "source": url,
                    "extraction_time": result.elapsed_time
                }
    
    # Add metadata
    final_data["_metadata"] = {
        "source_urls": source_urls,
        "total_sources": len(source_urls),
        "aggregation_timestamp": time.time()
    }
    
    logger.info(f"Aggregated data from {len(source_urls)} sources with {len(final_data)-1} fields")
    
    return final_data