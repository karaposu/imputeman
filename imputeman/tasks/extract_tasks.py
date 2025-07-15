# imputeman/tasks/extract_tasks.py
"""Data extraction tasks using Prefect"""

from typing import Dict, List
from prefect import task, get_run_logger

from extracthero.schemes import ExtractOp
from brightdata.models import ScrapeResult
from ..core.entities import WhatToRetain
from ..core.config import ExtractConfig, PipelineConfig
from ..services import get_service_registry


@task(
    retries=2,
    retry_delay_seconds=3,
    tags=["extract", "llm"]
)
async def extract_data_task(
    scrape_results: Dict[str, ScrapeResult],
    schema: List[WhatToRetain],
    config: ExtractConfig
) -> Dict[str, ExtractOp]:
    """
    Extract structured data from scraped HTML using LLM
    
    Args:
        scrape_results: Dictionary of URL -> ScrapeResult
        schema: Extraction schema defining what to extract
        config: Extraction configuration
        
    Returns:
        Dictionary of URL -> ExtractOp with extracted data
    """
    logger = get_run_logger()
    
    # Filter for successful scrapes
    successful_scrapes = {
        url: result for url, result in scrape_results.items()
        if result.status == "ready" and result.data
    }
    
    if not successful_scrapes:
        logger.warning("No successful scrapes to extract from")
        return {}
    
    logger.info(f"Starting extraction from {len(successful_scrapes)} documents")
    
    # Create pipeline config for service registry
    pipeline_config = PipelineConfig()
    pipeline_config.extract_config = config
    
    # Use extractor service
    registry = get_service_registry(pipeline_config)
    
    # Configure extractor if needed
    if hasattr(config, 'extraction_model'):
        model_name = config.extraction_model
    else:
        model_name = None
    
    extract_results = await registry.extractor.extract_from_scrapes(
        successful_scrapes, 
        schema,
        model_name=model_name
    )
    
    # Log results using ExtractOp's enhanced metrics
    successful_extractions = sum(1 for r in extract_results.values() if r.success)
    
    # Calculate totals from ExtractOp usage data
    total_cost = 0.0
    total_tokens = 0
    total_chars_trimmed = 0
    
    for url, extract_op in extract_results.items():
        # Cost calculation - ExtractOp combines usage from all phases
        if extract_op.usage:
            # The new ExtractHero puts total_cost in usage
            if 'total_cost' in extract_op.usage:
                total_cost += extract_op.usage['total_cost']
            elif 'cost' in extract_op.usage:
                total_cost += extract_op.usage['cost']
        
        # Token calculation from combined usage
        if extract_op.usage:
            # Look for various token keys
            if 'total_tokens' in extract_op.usage:
                total_tokens += extract_op.usage['total_tokens']
            elif 'prompt_tokens' in extract_op.usage and 'completion_tokens' in extract_op.usage:
                total_tokens += extract_op.usage['prompt_tokens'] + extract_op.usage['completion_tokens']
        
        # Track trimming
        if extract_op.trimmed_to:
            total_chars_trimmed += 1
            logger.debug(f"Document {url[:40]}... was trimmed to {extract_op.trimmed_to:,} chars")
    
    logger.info(
        f"Extraction completed: {successful_extractions}/{len(successful_scrapes)} successful, "
        f"total cost: ${total_cost:.4f}"
    )
    
    if total_tokens > 0:
        logger.info(f"Total tokens used: {total_tokens:,}")
    
    if total_chars_trimmed > 0:
        logger.info(f"Documents trimmed: {total_chars_trimmed}")
    
    return extract_results


@task(
    tags=["extract", "validate"]
)
async def validate_extractions_task(
    extract_results: Dict[str, ExtractOp],
    config: ExtractConfig
) -> List[ExtractOp]:
    """
    Validate extraction results based on success and content quality
    
    Args:
        extract_results: Dictionary of extraction results
        config: Extraction configuration with confidence threshold
        
    Returns:
        List of validated ExtractOp objects that meet criteria
    """
    logger = get_run_logger()
    logger.info(f"Validating {len(extract_results)} extraction results")
    
    validated = []
    
    for url, extract_op in extract_results.items():
        if not extract_op.success:
            logger.debug(f"Skipping failed extraction from {url}: {extract_op.error}")
            continue
        
        # Check if extraction has content
        if not extract_op.content:
            logger.debug(f"Skipping empty extraction from {url}")
            continue
        
        # Check completeness - all schema fields should have values
        if isinstance(extract_op.content, dict):
            # Count non-empty fields
            filled_fields = sum(1 for v in extract_op.content.values() if v)
            total_fields = len(extract_op.content)
            
            if total_fields > 0:
                completeness = filled_fields / total_fields
                
                # Use config threshold or default to 0.5
                min_completeness = getattr(config, 'min_field_completeness', 0.5)
                
                if completeness < min_completeness:
                    logger.debug(
                        f"Skipping incomplete extraction from {url} "
                        f"(completeness: {completeness:.2%}, required: {min_completeness:.2%})"
                    )
                    continue
        
        # Check extraction quality metrics if available
        if extract_op.stage_tokens:
            # Log token reduction efficiency
            if "Filter" in extract_op.stage_tokens:
                filter_tokens = extract_op.stage_tokens["Filter"]
                if filter_tokens.get("input", 0) > 0:
                    reduction = 1 - (filter_tokens.get("output", 0) / filter_tokens["input"])
                    logger.debug(f"Filter reduction for {url}: {reduction:.1%}")
        
        validated.append(extract_op)
        logger.debug(f"Validated extraction from {url}")
    
    logger.info(f"Validated {len(validated)}/{len(extract_results)} extractions")
    return validated


@task(
    tags=["extract", "aggregate"]
)
async def aggregate_final_data_task(
    validated_extractions: List[ExtractOp]
) -> Dict[str, any]:
    """
    Aggregate validated extractions into final dataset
    
    Args:
        validated_extractions: List of validated extraction results
        
    Returns:
        Aggregated data dictionary
    """
    logger = get_run_logger()
    logger.info(f"Aggregating {len(validated_extractions)} validated extractions")
    
    if not validated_extractions:
        logger.warning("No extractions to aggregate")
        return {}
    
    # Aggregation strategy: merge all extractions, with conflict resolution
    aggregated = {}
    source_urls = []
    field_sources = {}  # Track which URL provided each field
    
    # Sort by extraction quality (using elapsed time as proxy - faster = better)
    sorted_extractions = sorted(
        validated_extractions, 
        key=lambda x: x.elapsed_time
    )
    
    for extract_op in sorted_extractions:
        if extract_op.content and isinstance(extract_op.content, dict):
            # Get source URL from the extraction (if stored)
            source_url = "unknown"  # Would need to enhance ExtractOp to store source URL
            source_urls.append(source_url)
            
            for key, value in extract_op.content.items():
                if value:  # Only consider non-empty values
                    if key not in aggregated:
                        # First occurrence of this field
                        aggregated[key] = value
                        field_sources[key] = {
                            "source": source_url,
                            "extraction_time": extract_op.elapsed_time,
                            "token_efficiency": _calculate_token_efficiency(extract_op)
                        }
                    else:
                        # Field already exists - use quality metrics to decide
                        existing_efficiency = field_sources[key].get("token_efficiency", 0)
                        new_efficiency = _calculate_token_efficiency(extract_op)
                        
                        # Replace if new extraction is more efficient
                        if new_efficiency > existing_efficiency:
                            aggregated[key] = value
                            field_sources[key] = {
                                "source": source_url,
                                "extraction_time": extract_op.elapsed_time,
                                "token_efficiency": new_efficiency
                            }
    
    # Add metadata about the aggregation
    aggregated["_metadata"] = {
        "source_urls": list(set(source_urls)),
        "total_sources": len(validated_extractions),
        "field_sources": field_sources,
        "aggregation_strategy": "quality_based_merge"
    }
    
    # Log aggregation summary
    logger.info(f"Aggregated {len(aggregated)-1} fields from {len(validated_extractions)} sources")
    
    # Log field distribution
    field_source_counts = {}
    for field, info in field_sources.items():
        source = info["source"]
        field_source_counts[source] = field_source_counts.get(source, 0) + 1
    
    for source, count in field_source_counts.items():
        logger.debug(f"  Source {source}: contributed {count} fields")
    
    return aggregated


def _calculate_token_efficiency(extract_op: ExtractOp) -> float:
    """
    Calculate token efficiency score for an extraction
    
    Higher score = better efficiency
    """
    if not extract_op.stage_tokens:
        return 0.5  # Default middle score
    
    # Calculate overall token reduction
    initial_tokens = 0
    final_tokens = 0
    
    # Get first stage input
    for stage, tokens in extract_op.stage_tokens.items():
        if initial_tokens == 0 and "input" in tokens:
            initial_tokens = tokens["input"]
        break
    
    # Get last stage output
    stages = list(extract_op.stage_tokens.keys())
    if stages and "output" in extract_op.stage_tokens[stages[-1]]:
        final_tokens = extract_op.stage_tokens[stages[-1]]["output"]
    
    if initial_tokens > 0 and final_tokens > 0:
        reduction_ratio = 1 - (final_tokens / initial_tokens)
        # Higher reduction = better efficiency
        return min(reduction_ratio, 0.99)  # Cap at 0.99
    
    return 0.5  # Default if we can't calculate


@task(
    tags=["extract", "analyze"]
)
async def analyze_extraction_performance_task(
    extract_results: Dict[str, ExtractOp]
) -> Dict[str, any]:
    """
    Analyze extraction performance metrics
    
    Args:
        extract_results: All extraction results
        
    Returns:
        Performance analysis dictionary
    """
    logger = get_run_logger()
    
    analysis = {
        "total_extractions": len(extract_results),
        "successful": 0,
        "failed": 0,
        "total_cost": 0.0,
        "total_tokens": 0,
        "avg_elapsed_time": 0.0,
        "trimmed_documents": 0,
        "stage_metrics": {},
        "errors": []
    }
    
    elapsed_times = []
    
    for url, extract_op in extract_results.items():
        if extract_op.success:
            analysis["successful"] += 1
        else:
            analysis["failed"] += 1
            if extract_op.error:
                analysis["errors"].append({
                    "url": url[:50] + "...",
                    "error": extract_op.error
                })
        
        # Cost and tokens
        if extract_op.usage:
            analysis["total_cost"] += extract_op.usage.get("total_cost", 0)
            analysis["total_tokens"] += extract_op.usage.get("total_tokens", 0)
        
        # Timing
        elapsed_times.append(extract_op.elapsed_time)
        
        # Trimming
        if extract_op.trimmed_to:
            analysis["trimmed_documents"] += 1
        
        # Stage metrics
        if extract_op.stage_tokens:
            for stage, tokens in extract_op.stage_tokens.items():
                if stage not in analysis["stage_metrics"]:
                    analysis["stage_metrics"][stage] = {
                        "total_input_tokens": 0,
                        "total_output_tokens": 0,
                        "count": 0
                    }
                
                analysis["stage_metrics"][stage]["total_input_tokens"] += tokens.get("input", 0)
                analysis["stage_metrics"][stage]["total_output_tokens"] += tokens.get("output", 0)
                analysis["stage_metrics"][stage]["count"] += 1
    
    # Calculate averages
    if elapsed_times:
        analysis["avg_elapsed_time"] = sum(elapsed_times) / len(elapsed_times)
    
    # Calculate stage efficiency
    for stage, metrics in analysis["stage_metrics"].items():
        if metrics["total_input_tokens"] > 0:
            metrics["avg_reduction"] = 1 - (metrics["total_output_tokens"] / metrics["total_input_tokens"])
        else:
            metrics["avg_reduction"] = 0
    
    logger.info(f"Extraction performance analysis completed")
    logger.info(f"Success rate: {analysis['successful']}/{analysis['total_extractions']}")
    logger.info(f"Average time: {analysis['avg_elapsed_time']:.2f}s")
    logger.info(f"Total cost: ${analysis['total_cost']:.4f}")
    
    return analysis