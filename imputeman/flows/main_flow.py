# imputeman/flows/main_flow.py
"""Main Imputeman flow orchestration using Prefect - simplified version with cost analysis"""

import time
from typing import List
from prefect import flow, get_run_logger

from ..models import ImputeOp, PipelineStatus
from ..core.entities import EntityToImpute, WhatToRetain
from ..core.config import PipelineConfig, get_default_config
from ..tasks.serp_tasks import search_serp_task, extract_urls_from_serp_task
from ..tasks.scrape_tasks import scrape_urls_task, analyze_scrape_costs_task
from ..tasks.extract_tasks import extract_data_task


@flow(
    name="imputeman-pipeline",
    description="Entity imputation pipeline with cost analysis",
    version="2.0",
    retries=1,
    retry_delay_seconds=10
)
async def imputeman_flow(
    entity: EntityToImpute,
    schema: List[WhatToRetain],
    config: PipelineConfig = None,
    top_k: int = None
) -> ImputeOp:
    """
    Main Imputeman pipeline flow with cost analysis
    
    Args:
        entity: Entity to impute data for
        schema: Expected data schema for extraction
        config: Pipeline configuration (uses default if None)
        top_k: Number of search results (overrides config if provided)
        
    Returns:
        ImputeOp with complete pipeline results
    """
    logger = get_run_logger()
    start_time = time.time()
    
    # Use default config if none provided
    if config is None:
        config = get_default_config()
    
    logger.info(f"Starting imputeman pipeline for entity: {entity.name}")
    
    # Build search query
    query = f"{entity.name}"
    if entity.identifier_context:
        query += f" {entity.identifier_context}"
    if entity.impute_task_purpose:
        query += f" {entity.impute_task_purpose}"
    
    # Initialize ImputeOp
    impute_op = ImputeOp(query=query, schema=schema)
    impute_op.update_status(PipelineStatus.RUNNING)
    
    try:
        # Stage 1: Search (SERP)
        logger.info("🔍 Starting search stage...")
        serp_start = time.time()
        
        serp_result = await search_serp_task(
            query=query, 
            config=config.serp_config,
            top_k=top_k or config.serp_config.top_k_results
        )
        
        # Update ImputeOp with search results
        impute_op.search_op = serp_result
        impute_op.performance.serp_duration = time.time() - serp_start
        impute_op.costs.serp_cost = serp_result.usage.cost if serp_result.usage else 0.0
        
        if not serp_result.results:
            logger.error("Search stage failed - no results found")
            impute_op.update_status(PipelineStatus.FAILED, "No search results")
            impute_op.finalize(success=False)
            return impute_op
        
        # Extract URLs
        urls = await extract_urls_from_serp_task(serp_result)
        impute_op.urls = urls
        impute_op.mark_serp_completed(len(urls))
        
        logger.info(f"Found {len(urls)} URLs from search")
        
        # Stage 2: Scraping
        logger.info("🕷️ Starting scraping stage...")
        scrape_results = await scrape_urls_task(urls, config.scrape_config)
        
        # Analyze scraping costs
        cost_analysis = await analyze_scrape_costs_task(scrape_results)
        logger.info(f"Scraping cost analysis: ${cost_analysis['total_cost']:.4f} for {cost_analysis['successful_scrapes']} successful scrapes")
        
        # Log warnings if costs are high
        if cost_analysis['total_cost'] > 10.0:  # $10 threshold
            logger.warning(f"⚠️ High scraping costs detected: ${cost_analysis['total_cost']:.4f}")
        
        # Update ImputeOp
        impute_op.scrape_results = scrape_results
        for url, result in scrape_results.items():
            impute_op.mark_url_scraped(url, result.status == "ready")
        
        # Check if we have any successful scrapes
        successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
        if successful_scrapes == 0:
            logger.error("No successful scrapes, cannot continue to extraction")
            impute_op.update_status(PipelineStatus.FAILED, "No successful scrapes")
            impute_op.finalize(success=False)
            return impute_op
        
        logger.info(f"Successfully scraped {successful_scrapes}/{len(urls)} URLs")
        
        # Stage 3: Extraction
        logger.info("🧠 Starting extraction stage...")
        extract_results = await extract_data_task(
            scrape_results, 
            schema, 
            config.extract_config
        )
        
        # Update ImputeOp
        impute_op.extract_results = extract_results
        for url, result in extract_results.items():
            impute_op.mark_url_extracted(url, result.success)
        
        # Check if we have any successful extractions
        successful_extractions = sum(1 for r in extract_results.values() if r.success)
        if successful_extractions == 0:
            logger.warning("No successful extractions")
            impute_op.finalize(success=False)
        else:
            logger.info(f"Successfully extracted from {successful_extractions} URLs")
            impute_op.finalize(success=True)
        
        # Calculate final metrics
        impute_op.performance.total_elapsed_time = time.time() - start_time
        
        logger.info(f"✅ Pipeline completed in {impute_op.performance.total_elapsed_time:.2f}s")
        logger.info(f"💰 Total cost: ${impute_op.costs.total_cost:.2f}")
        logger.info(
            f"📈 Results: {impute_op.performance.successful_extractions} extractions "
            f"from {impute_op.performance.successful_scrapes} scrapes"
        )
        
        return impute_op
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        impute_op.errors.append(str(e))
        impute_op.update_status(PipelineStatus.FAILED, str(e))
        impute_op.finalize(success=False)
        return impute_op
        
        # Validate extractions
        validated_extractions = await validate_extractions_task(extract_results, config.extract_config)
        logger.info(f"Validated {len(validated_extractions)} quality extractions")
        
        # Check if we have any successful extractions
        if len(validated_extractions) == 0:
            logger.warning("No valid extractions after validation")
            impute_op.finalize(success=False)
        else:
            logger.info(f"Successfully extracted and validated from {len(validated_extractions)} URLs")
            impute_op.finalize(success=True)
        
        # Calculate final metrics
        impute_op.performance.total_elapsed_time = time.time() - start_time
        
        logger.info(f"✅ Pipeline completed in {impute_op.performance.total_elapsed_time:.2f}s")
        logger.info(f"💰 Total cost: ${impute_op.costs.total_cost:.2f}")
        logger.info(
            f"📈 Results: {impute_op.performance.successful_extractions} extractions "
            f"from {impute_op.performance.successful_scrapes} scrapes"
        )
        
        return impute_op
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        impute_op.errors.append(str(e))
        impute_op.update_status(PipelineStatus.FAILED, str(e))
        impute_op.finalize(success=False)
        return impute_op


@flow(
    name="imputeman-simple",
    description="Simplified pipeline entry point",
    version="2.0"
)
async def simple_imputeman_flow(
    entity: EntityToImpute,
    schema: List[WhatToRetain],
    top_k: int = 5
) -> ImputeOp:
    """
    Simplified pipeline for basic use cases
    
    Args:
        entity: Entity to impute data for
        schema: Expected data schema
        top_k: Number of search results
        
    Returns:
        ImputeOp with pipeline results
    """
    config = get_default_config()
    config.serp_config.top_k_results = top_k
    
    return await imputeman_flow(entity, schema, config, top_k)


# Convenience function for backwards compatibility
async def run_imputeman_async(
    entity: EntityToImpute,
    schema: List[WhatToRetain],
    top_k: int = 10
) -> ImputeOp:
    """
    Async convenience function to run the imputeman pipeline
    
    Args:
        entity: Entity to impute
        schema: Data schema
        top_k: Number of search results
        
    Returns:
        ImputeOp
    """
    return await imputeman_flow(entity, schema, top_k=top_k)