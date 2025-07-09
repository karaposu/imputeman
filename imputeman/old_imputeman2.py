# imputeman.py
"""
to run:  python -m imputeman.imputeman
"""


import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
import statistics
import logging

from .core.entities import EntityToImpute, WhatToRetain
from .core.config import PipelineConfig, get_development_config, get_production_config
from .services import ServiceRegistry
from .models import ImputeOp, PipelineStatus
from extracthero.schemes import ExtractOp


logger = logging.getLogger(__name__)


class Imputeman:
    """
    Main Imputeman orchestrator for AI-powered data imputation pipeline.
    
    Provides streaming parallelization: extraction starts immediately when each scrape completes.
    Uses ImputeOp for comprehensive real-time tracking and performance metrics.
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or get_development_config()
        self.registry = ServiceRegistry(self.config)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    async def run(
        self,
        entity: Union[str, EntityToImpute],
        schema: List[WhatToRetain],
        max_urls: int = None,
        enable_streaming: bool = True,
        capture_detailed_metrics: bool = True
    ) -> ImputeOp:
        """
        Execute complete Imputeman pipeline with streaming parallelization.
        
        Args:
            entity: Entity to search for (str name or EntityToImpute object)
            schema: List of fields to extract
            max_urls: Maximum URLs to process (uses config default if None)
            enable_streaming: If True, use streaming extraction (recommended)
            capture_detailed_metrics: If True, capture comprehensive timing data
            
        Returns:
            ImputeOp with extracted data, metrics, and real-time status tracking
        """
        start_time = time.time()
        max_urls = max_urls or self.config.serp_config.top_k_results
        
        # Convert string to EntityToImpute if needed
        entity = _ensure_entity(entity)
        
        # Create ImputeOp for comprehensive tracking
        query = f"{entity.name}"
        if entity.identifier_context:
            query += f" {entity.identifier_context}"
        if entity.impute_task_purpose:
            query += f" {entity.impute_task_purpose}"
            
        impute_op = ImputeOp(
            query=query,
            schema=schema
        )
        
        self.logger.info(f"🚀 Starting Imputeman pipeline for: {entity.name}")
        impute_op.update_status(PipelineStatus.INITIALIZING, f"Starting pipeline for {entity.name}")
        
        try:
            # Phase 1: SERP - Find URLs
            impute_op.start_running()
            serp_start = time.time()
            self.logger.info("🔍 Executing SERP phase...")
            
            serp_result = await self.registry.serp.search(query, top_k=max_urls)
            impute_op.search_op = serp_result  # Store SERP operation
            
            serp_duration = time.time() - serp_start
            impute_op.performance.serp_duration = serp_duration
            
            if not serp_result.success or not serp_result.links:
                error_msg = f"SERP failed: {serp_result.metadata if hasattr(serp_result, 'metadata') else 'Unknown error'}"
                impute_op.errors.append(error_msg)
                impute_op.update_status(PipelineStatus.FAILED, error_msg)
                self.logger.error(error_msg)
                impute_op.finalize(success=False)
                return impute_op
            
            # Update SERP completion
            found_urls = serp_result.links[:max_urls]
            impute_op.urls = found_urls
            impute_op.mark_serp_completed(len(found_urls))
            impute_op.costs.serp_cost = getattr(serp_result, 'cost', 0.0) or 0.0
            
            self.logger.info(f"✅ Found {len(found_urls)} URLs in {serp_duration:.2f}s")
            
            # Phase 2: Streaming Scrape + Extract
            if enable_streaming:
                await self._execute_streaming_pipeline(impute_op, capture_detailed_metrics)
            else:
                await self._execute_batch_pipeline(impute_op, capture_detailed_metrics)
            
            # Phase 3: Finalize results
            impute_op.performance.total_elapsed_time = time.time() - start_time
            success = impute_op.status_details.urls_extracted > 0
            impute_op.finalize(success=success)
            
            # Log summary
            self._log_execution_summary(impute_op)
            
            return impute_op
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            impute_op.errors.append(error_msg)
            impute_op.performance.total_elapsed_time = time.time() - start_time
            impute_op.update_status(PipelineStatus.FAILED, error_msg)
            self.logger.error(f"❌ Pipeline execution failed: {e}", exc_info=True)
            impute_op.finalize(success=False)
            return impute_op
        
        finally:
            await self.registry.close_all()
    
    async def _execute_streaming_pipeline(self, impute_op: ImputeOp, capture_metrics: bool):
        """Execute streaming pipeline: extract immediately as each scrape completes"""
        
        self.logger.info("⚡ Executing streaming scrape + extract pipeline")
        
        # Start all scrapes concurrently
        scrape_tasks = {}
        for url in impute_op.urls:
            task = asyncio.create_task(self._scrape_with_metrics(url, capture_metrics))
            scrape_tasks[task] = url
            impute_op.mark_url_scraping(url)
        
        # Process as each scrape completes (streaming!)
        pending_tasks = set(scrape_tasks.keys())
        
        while pending_tasks:
            done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            
            for completed_task in done:
                url = scrape_tasks[completed_task]
                
                try:
                    scrape_result, _ = await completed_task  # Simplified - no detailed metrics for now
                    
                    # Track scrape completion
                    scrape_success = scrape_result and any(r.status == "ready" for r in scrape_result.values())
                    impute_op.mark_url_scraped(url, scrape_success)
                    
                    if scrape_success:
                        # Store scrape result
                        impute_op.scrape_results.update(scrape_result)
                        
                        # Track scrape cost
                        for scrape_res in scrape_result.values():
                            scrape_cost = getattr(scrape_res, 'cost', 0) or 0
                            impute_op.costs.scrape_cost += scrape_cost
                        
                        self.logger.info(f"✅ Scrape completed for {url[:40]}... - Starting extraction IMMEDIATELY")
                        
                        # Mark extraction starting
                        impute_op.mark_url_extracting(url)
                        
                        # Immediately start extraction
                        extract_result, _ = await self._extract_with_metrics(
                            scrape_result, impute_op.schema, url, capture_metrics
                        )
                        
                        # Track extraction completion
                        extract_success = extract_result and any(r.success for r in extract_result.values())
                        impute_op.mark_url_extracted(url, extract_success)
                        
                        if extract_success:
                            impute_op.extract_results.update(extract_result)
                            
                            # Track extraction cost
                            for extract_op in extract_result.values():
                                if extract_op.usage and 'total_cost' in extract_op.usage:
                                    impute_op.costs.extraction_cost += extract_op.usage['total_cost']
                                elif extract_op.usage and 'cost' in extract_op.usage:
                                    impute_op.costs.extraction_cost += extract_op.usage['cost']
                            
                            self.logger.info(f"✅ Extraction completed for {url[:40]}...")
                            
                            # Store first successful content if available
                            if not impute_op.content and extract_success:
                                for extract_op in extract_result.values():
                                    if extract_op.success and extract_op.content:
                                        impute_op.content = extract_op.content
                                        break
                        else:
                            self.logger.warning(f"⚠️ Extraction failed for {url[:40]}...")
                    else:
                        self.logger.warning(f"⚠️ Scrape failed for {url[:40]}...")
                        
                except Exception as e:
                    error_msg = f"Processing failed for {url}: {str(e)}"
                    impute_op.errors.append(error_msg)
                    impute_op.mark_url_scraped(url, False)  # Mark as failed
                    self.logger.error(f"❌ Processing failed for {url[:40]}...: {e}")
    
    async def _execute_batch_pipeline(self, impute_op: ImputeOp, capture_metrics: bool):
        """Execute batch pipeline: scrape all, then extract all"""
        
        self.logger.info("📦 Executing batch scrape + extract pipeline")
        
        # Phase 1: Scrape all URLs
        for url in impute_op.urls:
            impute_op.mark_url_scraping(url)
        
        scrape_results = await self.registry.scraper.scrape_urls(impute_op.urls)
        impute_op.scrape_results = scrape_results
        
        # Track scrape results
        for url, scrape_result in scrape_results.items():
            scrape_success = scrape_result.status == "ready"
            impute_op.mark_url_scraped(url, scrape_success)
            
            if scrape_success:
                scrape_cost = getattr(scrape_result, 'cost', 0) or 0
                impute_op.costs.scrape_cost += scrape_cost
        
        # Phase 2: Extract from all successful scrapes
        successful_scrapes = {
            url: scrape_result for url, scrape_result in scrape_results.items()
            if scrape_result.status == "ready"
        }
        
        if successful_scrapes:
            # Mark extractions starting
            for url in successful_scrapes.keys():
                impute_op.mark_url_extracting(url)
            
            extract_results = await self.registry.extractor.extract_from_scrapes(successful_scrapes, impute_op.schema)
            impute_op.extract_results = extract_results
            
            # Track extraction results
            for url, extract_result in extract_results.items():
                extract_success = extract_result.success
                impute_op.mark_url_extracted(url, extract_success)
                
                if extract_success:
                    if extract_result.usage and 'total_cost' in extract_result.usage:
                        impute_op.costs.extraction_cost += extract_result.usage['total_cost']
                    elif extract_result.usage and 'cost' in extract_result.usage:
                        impute_op.costs.extraction_cost += extract_result.usage['cost']
                    
                    # Store first successful content
                    if not impute_op.content and extract_result.content:
                        impute_op.content = extract_result.content
    
    async def _scrape_with_metrics(self, url: str, capture_metrics: bool):
        """
        Scrape URL (simplified without detailed timing metrics for now)
        
        Note: ImputeOp already provides comprehensive metrics tracking.
        Future enhancement could add ScrapeTimingMetrics class for granular timing data.
        """
        
        scrape_result = await self.registry.scraper.scrape_urls([url])
        return scrape_result, None
    
    async def _extract_with_metrics(self, scrape_result, schema, url: str, capture_metrics: bool):
        """
        Extract data (simplified without detailed timing metrics for now)
        
        Note: ImputeOp already provides comprehensive metrics tracking.
        Future enhancement could add ExtractionTimingMetrics class for granular timing data.
        """
        
        extract_result = await self.registry.extractor.extract_from_scrapes(scrape_result, schema)
        return extract_result, None
    
    def _log_execution_summary(self, impute_op: ImputeOp):
        """Log comprehensive execution summary using ImputeOp data"""
        
        self.logger.info("🎯 Imputeman Pipeline Results:")
        self.logger.info(f"   ✅ Overall Success: {impute_op.success}")
        
        if impute_op.performance.urls_found > 0:
            success_rate = impute_op.performance.successful_extractions / impute_op.performance.urls_found
            self.logger.info(f"   📊 Success Rate: {success_rate:.1%}")
        
        self.logger.info(f"   ⏱️ Total Duration: {impute_op.performance.total_elapsed_time:.2f}s")
        self.logger.info(f"   💰 Total Cost: ${impute_op.costs.total_cost:.4f}")
        self.logger.info(f"   🔗 URLs: {impute_op.performance.urls_found} found → {impute_op.performance.successful_scrapes} scraped → {impute_op.performance.successful_extractions} extracted")
        
        if impute_op.performance.time_to_first_result:
            self.logger.info(f"   ⚡ Time to First Result: {impute_op.performance.time_to_first_result:.2f}s")
        
        # Live summary
        self.logger.info(f"   📈 Live Summary: {impute_op.get_live_summary()}")
        
        if impute_op.errors:
            self.logger.warning(f"   ⚠️ Errors Encountered: {len(impute_op.errors)}")
            for error in impute_op.errors:
                self.logger.warning(f"      • {error}")


# Helper function for entity conversion
def _ensure_entity(entity: Union[str, EntityToImpute]) -> EntityToImpute:
    """Convert string to EntityToImpute if needed"""
    if isinstance(entity, str):
        return EntityToImpute(name=entity)
    return entity


async def main():
    """Example usage of Imputeman with explicit instantiation pattern"""
    
    # Define what to extract
    schema = [
        WhatToRetain(name="component_type", desc="Type of electronic component"),
        # WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
        # WhatToRetain(name="package_type", desc="Physical package type")
    ]
    
    # Create entity (can also just use string "BAV99")
    entity = EntityToImpute(name="BAV99")
    
    # Get configuration
    config = get_development_config()
    
    # Explicit instantiation and execution - better visibility!
    imputeman = Imputeman(config)
    impute_op = await imputeman.run(
        entity=entity,  # or just "BAV99"
        schema=schema,
        max_urls=5,
        enable_streaming=True,
        capture_detailed_metrics=True
    )
    
    # Analyze results using ImputeOp's comprehensive tracking
    print(f"\n🎯 Imputeman Results:")
    print(f"   Success: {impute_op.success}")
    print(f"   Extracted data: {len(impute_op.extract_results)} items")
    print(f"   Total cost: ${impute_op.costs.total_cost:.4f}")
    print(f"   Live summary: {impute_op.get_live_summary()}")
    
    # Access detailed metrics
    print(f"\n📊 Detailed Metrics:")
    print(f"   Status: {impute_op.status_details}")
    print(f"   Performance: Success rate {impute_op.performance.successful_extractions}/{impute_op.performance.urls_found}")
    print(f"   Cost breakdown: SERP=${impute_op.costs.serp_cost:.4f}, Scrape=${impute_op.costs.scrape_cost:.4f}, Extract=${impute_op.costs.extraction_cost:.4f}")
    
    # Check extracted content
    if impute_op.content:
        print(f"\n📋 Extracted Content:")
        print(f"   {impute_op.content}")
    
    return impute_op


def main_sync():
    """Synchronous wrapper for main - explicit pattern"""
    return asyncio.run(main())


if __name__ == "__main__":
    # Example of explicit pattern
    asyncio.run(main())