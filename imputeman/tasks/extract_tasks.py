# imputeman/tasks/extract_tasks.py
"""Extraction tasks for Imputeman pipeline using Prefect"""

from typing import Dict, List, Any
from prefect import task, get_run_logger

from brightdata.models import ScrapeResult
from extracthero.schemes import ExtractOp
from ..services.extractor_service import ExtractorService
from ..core.entities import WhatToRetain
from ..core.config import ExtractConfig


@task(
    name="extract-data",
    description="Extract structured data from scraped HTML using ExtractHero",
    retries=1,
    retry_delay_seconds=3
)
async def extract_data_task(
    scrape_results: Dict[str, ScrapeResult],
    schema: List[WhatToRetain],
    config: ExtractConfig
) -> Dict[str, ExtractOp]:
    """
    Extract structured data from scrape results
    
    Args:
        scrape_results: Dictionary of URL to ScrapeResult
        schema: List of WhatToRetain defining extraction schema
        config: Extraction configuration
        
    Returns:
        Dictionary mapping URL to ExtractOp results
    """
    logger = get_run_logger()
    
    # Filter for successful scrapes only
    successful_scrapes = {
        url: result for url, result in scrape_results.items()
        if result.status == "ready" and result.data
    }
    
    if not successful_scrapes:
        logger.warning("No successful scrapes to extract from")
        return {}
    
    logger.info(f"Starting extraction from {len(successful_scrapes)} successful scrapes")
    
    # Initialize extractor service
    extractor = ExtractorService(config)
    
    try:
        # Execute extraction
        extract_results = await extractor.extract_from_scrapes(
            successful_scrapes,
            schema
        )
        
        # Log results summary
        successful = sum(1 for r in extract_results.values() if r.success)
        failed = len(extract_results) - successful
        
        logger.info(f"Extraction completed: {successful} successful, {failed} failed")
        
        # Log individual results
        for url, result in extract_results.items():
            if result.success:
                # Log token usage if available
                if hasattr(result, 'stage_tokens') and result.stage_tokens:
                    total_reduction = 0
                    first_input = 0
                    last_output = 0
                    
                    for i, (stage, tokens) in enumerate(result.stage_tokens.items()):
                        if i == 0:
                            first_input = tokens.get('input', 0)
                        last_output = tokens.get('output', 0)
                    
                    if first_input > 0:
                        total_reduction = ((first_input - last_output) / first_input) * 100
                    
                    logger.debug(f"✅ {url[:50]}... - {total_reduction:.1f}% token reduction")
                else:
                    logger.debug(f"✅ {url[:50]}... - Extraction successful")
                    
                # Log content preview
                if result.content:
                    if isinstance(result.content, dict):
                        logger.debug(f"   Extracted {len(result.content)} fields")
                    elif isinstance(result.content, list):
                        logger.debug(f"   Extracted list with {len(result.content)} items")
            else:
                error_msg = getattr(result, 'error', 'Unknown error')
                logger.warning(f"❌ {url[:50]}... - {error_msg}")
        
        return extract_results
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise
    finally:
        await extractor.close()