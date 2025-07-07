# imputeman/flows/main_flow.py
"""Main Imputeman flow orchestration using Prefect"""

import time
from typing import Dict, Any, List
from prefect import flow, get_run_logger

from ..core.entities import EntityToImpute, ImputeResult, WhatToRetain
from ..core.config import PipelineConfig, get_default_config
from ..tasks.serp_tasks import search_serp_task, validate_serp_results_task
from ..tasks.scrape_tasks import (
    scrape_urls_task, 
    budget_scrape_urls_task, 
    analyze_scrape_costs_task
)
from ..tasks.extract_tasks import (
    extract_data_task, 
    validate_extractions_task, 
    aggregate_final_data_task
)


@flow(
    name="imputeman-pipeline",
    description="Complete entity imputation pipeline with conditional logic",
    version="1.0",
    retries=1,
    retry_delay_seconds=10
)
async def imputeman_flow(
    entity: EntityToImpute,
    schema: List[WhatToRetain],
    config: PipelineConfig = None,
    top_k: int = None
) -> ImputeResult:
    """
    Main Imputeman pipeline flow with conditional branching
    
    Args:
        entity: Entity to impute data for
        schema: Expected data schema for extraction
        config: Pipeline configuration (uses default if None)
        top_k: Number of search results (overrides config if provided)
        
    Returns:
        ImputeResult with complete pipeline results
    """
    logger = get_run_logger()
    start_time = time.time()
    
    # Use default config if none provided
    if config is None:
        config = get_default_config()
    
    logger.info(f"Starting imputeman pipeline for entity: {entity.name}")
    
    # Initialize result object
    result = ImputeResult(entity=entity, schema=schema)
    
    try:
        # Stage 1: Search (SERP)
        logger.info("🔍 Starting search stage...")
        serp_result = await search_serp_task(
            query=entity.name, 
            config=config.serp_config,
            top_k=top_k
        )
        
        if not serp_result.success:
            logger.error("Search stage failed, cannot continue pipeline")
            result.success = False
            result.total_elapsed_time = time.time() - start_time
            return result
        
        # Validate search results
        serp_result = await validate_serp_results_task(serp_result)
        result.serp_result = serp_result
        
        # Stage 2: Scraping (with conditional logic)
        logger.info("🕷️ Starting scraping stage...")
        scrape_results = await scrape_urls_task(serp_result, config.scrape_config)
        
        # Analyze scraping costs
        cost_analysis = await analyze_scrape_costs_task(scrape_results)
        
        # Conditional: If scraping was too expensive, try budget approach
        if cost_analysis["total_cost"] > config.cost_threshold_for_budget_mode:
            logger.warning(f"Scraping cost ${cost_analysis['total_cost']:.2f} exceeded threshold")
            logger.info("🚧 Switching to budget scraping mode...")
            
            # Try budget scraping for remaining URLs or retry failed ones
            budget_scrape_results = await budget_scrape_urls_task(
                serp_result, 
                config.budget_scrape_config
            )
            
            # Combine or choose better results
            scrape_results = _merge_scrape_results(scrape_results, budget_scrape_results)
        
        result.scrape_results = scrape_results
        
        # Check if we have enough successful scrapes to continue
        successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
        if successful_scrapes == 0:
            logger.error("No successful scrapes, cannot continue to extraction")
            result.success = False
            result.total_elapsed_time = time.time() - start_time
            return result
        
        # Stage 3: Extraction
        logger.info("🧠 Starting extraction stage...")
        extract_results = await extract_data_task(
            scrape_results, 
            schema, 
            config.extract_config
        )
        
        # Validate extractions
        validated_extractions = await validate_extractions_task(
            extract_results, 
            config.extract_config
        )
        
        result.extract_results = extract_results
        
        # Check if we have enough quality extractions
        if len(validated_extractions) < config.min_successful_extractions:
            logger.warning(f"Only {len(validated_extractions)} successful extractions, below minimum {config.min_successful_extractions}")
            
            # Conditional: Try with lower confidence threshold
            relaxed_config = config.extract_config
            relaxed_config.confidence_threshold = 0.5  # Lower threshold
            
            logger.info("🔄 Retrying validation with relaxed criteria...")
            validated_extractions = await validate_extractions_task(
                extract_results, 
                relaxed_config
            )
        
        # Stage 4: Final aggregation
        if validated_extractions:
            logger.info("📊 Aggregating final results...")
            final_data = await aggregate_final_data_task(validated_extractions)
            result.final_data = final_data
            result.success = True
        else:
            logger.error("No valid extractions available for aggregation")
            result.success = False
        
        # Calculate final metrics
        result.total_cost = (
            sum(r.cost for r in scrape_results.values()) +
            sum(r.cost for r in extract_results.values())
        )
        result.total_elapsed_time = time.time() - start_time
        
        logger.info(f"✅ Pipeline completed successfully in {result.total_elapsed_time:.2f}s")
        logger.info(f"💰 Total cost: ${result.total_cost:.2f}")
        logger.info(f"📈 Results: {result.successful_extractions} extractions from {result.total_urls_scraped} URLs")
        
        return result
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        result.success = False
        result.total_elapsed_time = time.time() - start_time
        result.metadata["error"] = str(e)
        return result


def _merge_scrape_results(primary_results, secondary_results):
    """
    Merge scrape results, preferring successful results from either source
    
    Args:
        primary_results: First scraping attempt results
        secondary_results: Secondary/budget scraping results
        
    Returns:
        Merged scrape results with best available data
    """
    merged = primary_results.copy()
    
    for url, secondary_result in secondary_results.items():
        if url in merged:
            primary_result = merged[url]
            # Use secondary result if primary failed and secondary succeeded
            if primary_result.status != "ready" and secondary_result.status == "ready":
                merged[url] = secondary_result
        else:
            merged[url] = secondary_result
    
    return merged


@flow(
    name="imputeman-simple",
    description="Simplified pipeline without conditional logic",
    version="1.0"
)
async def simple_imputeman_flow(
    entity: EntityToImpute,
    schema: List[WhatToRetain],
    top_k: int = 5
) -> ImputeResult:
    """
    Simplified pipeline for basic use cases without complex conditional logic
    
    Args:
        entity: Entity to impute data for
        schema: Expected data schema
        top_k: Number of search results
        
    Returns:
        ImputeResult with pipeline results
    """
    config = get_default_config()
    config.serp_config.top_k_results = top_k
    
    return await imputeman_flow(entity, schema, config, top_k)


# Convenience function for backwards compatibility
async def run_imputeman_async(
    entity: EntityToImpute,
    schema: List[WhatToRetain],
    top_k: int = 10
) -> ImputeResult:
    """
    Async convenience function to run the imputeman pipeline
    
    Args:
        entity: Entity to impute
        schema: Data schema
        top_k: Number of search results
        
    Returns:
        ImputeResult
    """
    return await imputeman_flow(entity, schema, top_k=top_k)