# imputeman/impute_engine.py
"""
ImputeEngine - Handles all implementation details for the Imputeman pipeline

This engine contains all the complex logic, detailed logging, metrics tracking,
error handling, and coordination between services. The main Imputeman class
simply orchestrates by calling these clean methods.

python -m imputeman.impute_engine
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import logging

from .core.entities import EntityToImpute, WhatToRetain
from .core.config import PipelineConfig
from .services import ServiceRegistry
from .models import ImputeOp, PipelineStatus
from extracthero.schemes import ExtractOp


class ImputeEngine:
    """
    Implementation engine for Imputeman pipeline.
    
    Handles all complex logic, detailed logging, metrics tracking, and service coordination.
    Provides clean interfaces that return data/status to the orchestrator.
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.registry = ServiceRegistry(config)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def initialize(self, entity: Union[str, EntityToImpute], schema: List[WhatToRetain]) -> ImputeOp:
        """
        Initialize ImputeOp with proper setup and logging
        
        Args:
            entity: Entity to search for (str or EntityToImpute)
            schema: List of fields to extract
            
        Returns:
            Initialized ImputeOp ready for processing
        """
        # Convert string to EntityToImpute if needed
        if isinstance(entity, str):
            entity = EntityToImpute(name=entity)
        
        # Build search query
        query = f"{entity.name}"
        if entity.identifier_context:
            query += f" {entity.identifier_context}"
        if entity.impute_task_purpose:
            query += f" {entity.impute_task_purpose}"
            
        # Create ImputeOp
        impute_op = ImputeOp(query=query, schema=schema)
        
        # Setup logging and initial status
        self.logger.info(f"🚀 Starting Imputeman pipeline for: {entity.name}")
        impute_op.update_status(PipelineStatus.INITIALIZING, f"Starting pipeline for {entity.name}")
        
        return impute_op
    
    async def search(self, impute_op: ImputeOp, max_urls: int = None) -> List[str]:
        """
        Execute SERP search and return found URLs
        
        Args:
            impute_op: Current pipeline operation
            max_urls: Maximum URLs to return
            
        Returns:
            List of URLs found, empty list if search failed
        """
        max_urls = max_urls or self.config.serp_config.top_k_results
        
        # Start running phase
        impute_op.start_running()
        serp_start = time.time()
        self.logger.info("🔍 Executing SERP phase...")
        
        try:
            # Execute search
            serp_result = await self.registry.serp.search(impute_op.query, top_k=max_urls)
            impute_op.search_op = serp_result
            
            # Track timing and costs
            serp_duration = time.time() - serp_start
            impute_op.performance.serp_duration = serp_duration
            impute_op.costs.serp_cost = getattr(serp_result, 'cost', 0.0) or 0.0
            
            # Check success
            if not serp_result.success or not serp_result.links:
                error_msg = f"SERP failed: {getattr(serp_result, 'metadata', 'Unknown error')}"
                impute_op.errors.append(error_msg)
                impute_op.update_status(PipelineStatus.FAILED, error_msg)
                self.logger.error(error_msg)
                return []
            
            # Success - prepare URLs
            found_urls = serp_result.links[:max_urls]
            impute_op.urls = found_urls
            impute_op.mark_serp_completed(len(found_urls))
            
            self.logger.info(f"✅ Found {len(found_urls)} URLs in {serp_duration:.2f}s")
            return found_urls
            
        except Exception as e:
            error_msg = f"SERP search failed: {str(e)}"
            impute_op.errors.append(error_msg)
            impute_op.update_status(PipelineStatus.FAILED, error_msg)
            self.logger.error(f"❌ SERP search failed: {e}", exc_info=True)
            return []
    
    async def process_urls(
        self, 
        impute_op: ImputeOp, 
        urls: List[str], 
        streaming: bool = True,
        capture_metrics: bool = True
    ):
        """
        Process URLs with scraping and extraction
        
        Args:
            impute_op: Current pipeline operation
            urls: URLs to process
            streaming: If True, use streaming (extract as scrapes complete)
            capture_metrics: If True, capture detailed timing data
        """
        if not urls:
            self.logger.warning("No URLs to process")
            return
        
        # Choose processing strategy
        if streaming:
            await self._execute_streaming_pipeline(impute_op, capture_metrics)
        else:
            await self._execute_batch_pipeline(impute_op, capture_metrics)
    
    def finalize(self, impute_op: ImputeOp, start_time: float) -> ImputeOp:
        """
        Finalize pipeline with metrics calculation and final logging
        
        Args:
            impute_op: Current pipeline operation
            start_time: Pipeline start time for total duration
            
        Returns:
            Finalized ImputeOp with complete metrics
        """
        # Calculate final timing
        impute_op.performance.total_elapsed_time = time.time() - start_time
        
        # Determine success
        success = impute_op.status_details.urls_extracted > 0
        impute_op.finalize(success=success)
        
        # Log comprehensive summary
        self._log_execution_summary(impute_op)
        
        return impute_op
    
    async def cleanup(self):
        """Clean up resources"""
        await self.registry.close_all()
    
    # ========== PRIVATE IMPLEMENTATION METHODS ==========
    
    async def _execute_streaming_pipeline(self, impute_op: ImputeOp, capture_metrics: bool):
        """Execute streaming pipeline: extract immediately as each scrape completes"""
        
        self.logger.info("⚡ Executing streaming scrape + extract pipeline")
        
        # Start all scrapes concurrently
        scrape_tasks = {}
        for url in impute_op.urls:
            task = asyncio.create_task(self._scrape_single_url(url, capture_metrics))
            scrape_tasks[task] = url
            impute_op.mark_url_scraping(url)
        
        # Process as each scrape completes (streaming!)
        pending_tasks = set(scrape_tasks.keys())
        
        while pending_tasks:
            done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            
            for completed_task in done:
                url = scrape_tasks[completed_task]
                await self._handle_completed_scrape(completed_task, url, impute_op, capture_metrics)
    
    async def _execute_batch_pipeline(self, impute_op: ImputeOp, capture_metrics: bool):
        """Execute batch pipeline: scrape all, then extract all"""
        
        self.logger.info("📦 Executing batch scrape + extract pipeline")
        
        # Phase 1: Scrape all URLs
        for url in impute_op.urls:
            impute_op.mark_url_scraping(url)
        
        scrape_results = await self.registry.scraper.scrape_urls(impute_op.urls)
        impute_op.scrape_results = scrape_results
        
        # Track and log scrape results
        await self._process_batch_scrape_results(impute_op, scrape_results)
        
        # Phase 2: Extract from successful scrapes
        successful_scrapes = {
            url: scrape_result for url, scrape_result in scrape_results.items()
            if scrape_result.status == "ready"
        }
        
        if successful_scrapes:
            await self._process_batch_extractions(impute_op, successful_scrapes, capture_metrics)
    


    async def _handle_completed_scrape(
        self, 
        completed_task: asyncio.Task, 
        url: str, 
        impute_op: ImputeOp, 
        capture_metrics: bool
    ):
        """Handle a completed scrape task in streaming mode"""
        
        try:
            scrape_result, scrape_metrics = await completed_task
            
            # Track scrape completion
            scrape_success = scrape_result and any(r.status == "ready" for r in scrape_result.values())
            impute_op.mark_url_scraped(url, scrape_success)
            
            if scrape_success:
                # Store scrape result and get details for enhanced logging
                impute_op.scrape_results.update(scrape_result)
                scrape_cost, html_size = self._extract_scrape_details(scrape_result)
                impute_op.costs.scrape_cost += scrape_cost
                
                # Enhanced scrape completion log with size and cost
                self.logger.info(f"✅ Scraped {url[:40]}... ({html_size:,} chars, ${scrape_cost:.4f})")
                
                # Start extraction - the extractor service will handle phase logging
                impute_op.mark_url_extracting(url)
                
                extract_result, extract_metrics = await self._extract_from_scrape(
                    scrape_result, impute_op.schema, url, capture_metrics
                )
                
                # Handle extraction completion
                await self._handle_extraction_result(extract_result, url, impute_op)
                
            else:
                self.logger.warning(f"⚠️ Scrape failed for {url[:40]}...")
                
        except Exception as e:
            error_msg = f"Processing failed for {url}: {str(e)}"
            impute_op.errors.append(error_msg)
            impute_op.mark_url_scraped(url, False)
            self.logger.error(f"❌ Processing failed for {url[:40]}...: {e}")





    # async def _handle_completed_scrape(
    #     self, 
    #     completed_task: asyncio.Task, 
    #     url: str, 
    #     impute_op: ImputeOp, 
    #     capture_metrics: bool
    # ):
    #     """Handle a completed scrape task in streaming mode"""
        
    #     try:
    #         scrape_result, scrape_metrics = await completed_task
            
    #         # Track scrape completion
    #         scrape_success = scrape_result and any(r.status == "ready" for r in scrape_result.values())
    #         impute_op.mark_url_scraped(url, scrape_success)
            
    #         if scrape_success:
    #             # Store scrape result and log with details
    #             impute_op.scrape_results.update(scrape_result)
    #             scrape_cost, html_size = self._extract_scrape_details(scrape_result)
    #             impute_op.costs.scrape_cost += scrape_cost
                
    #             self.logger.info(f"✅ Scraped {url[:40]}... ({html_size:,} chars, ${scrape_cost:.4f})")
                
    #             # Immediately start extraction
    #             impute_op.mark_url_extracting(url)
    #             self.logger.info(f"🧠 Started extracting[Filtering] from {url[:40]}...")
                
    #             extract_result, extract_metrics = await self._extract_from_scrape(
    #                 scrape_result, impute_op.schema, url, capture_metrics
    #             )
                
    #             # Handle extraction completion
    #             await self._handle_extraction_result(extract_result, url, impute_op)
                
    #         else:
    #             self.logger.warning(f"⚠️ Scrape failed for {url[:40]}...")
                
    #     except Exception as e:
    #         error_msg = f"Processing failed for {url}: {str(e)}"
    #         impute_op.errors.append(error_msg)
    #         impute_op.mark_url_scraped(url, False)
    #         self.logger.error(f"❌ Processing failed for {url[:40]}...: {e}")





    
    async def _process_batch_scrape_results(self, impute_op: ImputeOp, scrape_results: Dict):
        """Process and log batch scrape results"""
        
        for url, scrape_result in scrape_results.items():
            scrape_success = scrape_result.status == "ready"
            impute_op.mark_url_scraped(url, scrape_success)
            
            if scrape_success:
                scrape_cost, html_size = self._extract_scrape_details({url: scrape_result})
                impute_op.costs.scrape_cost += scrape_cost
                self.logger.info(f"✅ Scraped {url[:40]}... ({html_size:,} chars, ${scrape_cost:.4f})")
            else:
                self.logger.warning(f"⚠️ Scrape failed for {url[:40]}...")
    
    async def _process_batch_extractions(self, impute_op: ImputeOp, successful_scrapes: Dict, capture_metrics: bool):
        """Process batch extractions"""
        
        # Mark extractions starting
        for url in successful_scrapes.keys():
            impute_op.mark_url_extracting(url)
            self.logger.info(f"🧠 Started extracting[Filtering] from {url[:40]}...")
        
        # Execute extractions
        extract_results = await self.registry.extractor.extract_from_scrapes(successful_scrapes, impute_op.schema)
        impute_op.extract_results = extract_results
        
        # Track results
        for url, extract_result in extract_results.items():
            await self._handle_extraction_result({url: extract_result}, url, impute_op)
    
    async def _handle_extraction_result(self, extract_result: Dict, url: str, impute_op: ImputeOp):
        """Handle extraction result and update tracking"""
        
        extract_success = extract_result and any(r.success for r in extract_result.values())
        impute_op.mark_url_extracted(url, extract_success)
        
        if extract_success:
            impute_op.extract_results.update(extract_result)
            
            # Track extraction cost
            extract_cost = self._calculate_extraction_cost(extract_result)
            impute_op.costs.extraction_cost += extract_cost
            
            self.logger.info(f"✅ Extracted from {url[:40]}... (${extract_cost:.4f})")
            
            # Store first successful content if available
            if not impute_op.content:
                for extract_op in extract_result.values():
                    if extract_op.success and extract_op.content:
                        impute_op.content = extract_op.content
                        break
        else:
            self.logger.warning(f"⚠️ Extraction failed for {url[:40]}...")
    
    async def _scrape_single_url(self, url: str, capture_metrics: bool):
        """Scrape a single URL and optionally capture metrics"""
        
        scrape_result = await self.registry.scraper.scrape_urls([url])
        
        # Future: Could add detailed timing metrics here
        scrape_metrics = None
        
        return scrape_result, scrape_metrics
    
    async def _extract_from_scrape(self, scrape_result: Dict, schema: List[WhatToRetain], url: str, capture_metrics: bool):
        """Extract data from scrape result"""
        
        extract_result = await self.registry.extractor.extract_from_scrapes(scrape_result, schema)
        
        # Future: Could add detailed timing metrics here
        extract_metrics = None
        
        return extract_result, extract_metrics
    
    def _extract_scrape_details(self, scrape_result: Dict) -> tuple[float, int]:
        """Extract cost and HTML size from scrape result"""
        
        total_cost = 0.0
        total_size = 0
        
        for scrape_res in scrape_result.values():
            total_cost += getattr(scrape_res, 'cost', 0) or 0
            if hasattr(scrape_res, 'data') and scrape_res.data:
                total_size += len(scrape_res.data)
        
        return total_cost, total_size
    
    def _calculate_extraction_cost(self, extract_result: Dict) -> float:
        """Calculate total extraction cost from result"""
        
        total_cost = 0.0
        
        for extract_op in extract_result.values():
            if extract_op.usage and 'total_cost' in extract_op.usage:
                total_cost += extract_op.usage['total_cost']
            elif extract_op.usage and 'cost' in extract_op.usage:
                total_cost += extract_op.usage['cost']
        
        return total_cost
    
    def _log_execution_summary(self, impute_op: ImputeOp):
        """Log comprehensive execution summary"""
        
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
        
        # Cost breakdown summary
        if impute_op.costs.total_cost > 0:
            self.logger.info(f"   💰 Cost Breakdown: SERP=${impute_op.costs.serp_cost:.4f}, Scrape=${impute_op.costs.scrape_cost:.4f}, Extract=${impute_op.costs.extraction_cost:.4f}")


# ========== TESTING / DEMONSTRATION ==========

async def main():
    """
    Test ImputeEngine methods independently
    
    Demonstrates that the engine methods work correctly and can be called
    step by step for testing and debugging purposes.
    """
    from .core.config import get_development_config
    from .core.entities import EntityToImpute, WhatToRetain
    
    print("🔧 Testing ImputeEngine methods independently...")
    print("=" * 60)
    
    # Setup
    config = get_development_config()
    engine = ImputeEngine(config)
    start_time = time.time()
    
    try:
        # Define what to extract
        schema = [
            WhatToRetain(name="component_type", desc="Type of electronic component"),
            WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
            WhatToRetain(name="package_type", desc="Physical package type")
        ]
        
        entity = EntityToImpute(name="BAV99")
        
        print(f"🎯 Testing with entity: {entity.name}")
        print(f"📋 Schema: {len(schema)} fields to extract")
        print()
        
        # Step 1: Initialize
        print("🔄 Step 1: Testing engine.initialize()...")
        impute_op = engine.initialize(entity, schema)
        print(f"   ✅ ImputeOp created: {impute_op.query}")
        print(f"   📊 Status: {impute_op.status}")
        print()
        
        # Step 2: Search
        print("🔄 Step 2: Testing engine.search()...")
        urls = await engine.search(impute_op, max_urls=5)
        print(f"   ✅ Found {len(urls)} URLs")
        if urls:
            for i, url in enumerate(urls, 1):
                print(f"      {i}. {url[:60]}...")
        else:
            print("   ⚠️ No URLs found - search may have failed")
        print()
        
        # Step 3: Process URLs (if we have them)
        if urls:
            print("🔄 Step 3: Testing engine.process_urls() with streaming...")
            await engine.process_urls(
                impute_op, 
                urls, 
                streaming=True, 
                capture_metrics=True
            )
            print(f"   ✅ Processing completed")
            print(f"   📊 Scraped: {impute_op.performance.successful_scrapes}/{len(urls)}")
            print(f"   📊 Extracted: {impute_op.performance.successful_extractions}/{len(urls)}")
            print()
        else:
            print("🔄 Step 3: Skipping process_urls() - no URLs to process")
            print()
        
        # Step 4: Finalize
        print("🔄 Step 4: Testing engine.finalize()...")
        final_impute_op = engine.finalize(impute_op, start_time)
        print(f"   ✅ Pipeline finalized")
        print(f"   📊 Final success: {final_impute_op.success}")
        print(f"   ⏱️ Total time: {final_impute_op.performance.total_elapsed_time:.2f}s")
        print(f"   💰 Total cost: ${final_impute_op.costs.total_cost:.4f}")
        print()
        
        # Results Summary
        print("📋 ENGINE TEST RESULTS:")
        print("=" * 40)
        print(f"🎯 Entity: {entity.name}")
        print(f"🔍 URLs found: {len(impute_op.urls)}")
        print(f"🕷️ Successful scrapes: {impute_op.performance.successful_scrapes}")
        print(f"🧠 Successful extractions: {impute_op.performance.successful_extractions}")
        print(f"💰 Total cost: ${impute_op.costs.total_cost:.4f}")
        print(f"⏱️ Total duration: {impute_op.performance.total_elapsed_time:.2f}s")
        print(f"✅ Overall success: {impute_op.success}")
        
        # Show extracted content sample
        if impute_op.content:
            print(f"\n📄 Sample extracted content:")
            for key, value in list(impute_op.content.items())[:2]:  # Show first 2 fields
                preview = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                print(f"   {key}: {preview}")
        
        # Show live summary
        print(f"\n📈 Live Summary: {impute_op.get_live_summary()}")
        
        return impute_op
        
    except Exception as e:
        print(f"❌ Engine test failed: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        # Step 5: Cleanup
        print(f"\n🔄 Step 5: Testing engine.cleanup()...")
        await engine.cleanup()
        print(f"   ✅ Engine cleanup completed")


def main_sync():
    """Synchronous wrapper for engine testing"""
    return asyncio.run(main())


if __name__ == "__main__":
    print("🚀 Running ImputeEngine independent test...")
    print("   Command: python -m imputeman.impute_engine")
    print()
    
    result = main_sync()
    
    if result:
        print(f"\n🎉 ImputeEngine test completed successfully!")
        print(f"🔧 All engine methods are working properly.")
    else:
        print(f"\n💥 ImputeEngine test failed!")
        print(f"🔧 Check the error messages above.")