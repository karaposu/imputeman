# imputeman.py
# to run python -m imputeman.imputeman

import asyncio
import time
from typing import List, Optional, Union
import logging

from .core.entities import EntityToImpute, WhatToRetain
from .core.config import PipelineConfig, get_development_config
from .models import ImputeOp
from .impute_engine import ImputeEngine


logger = logging.getLogger(__name__)


class Imputeman:
    """
    Clean orchestrator for AI-powered data imputation pipeline.
    
    Uses ImputeEngine for all implementation details, providing a simple
    and readable interface for pipeline execution.
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or get_development_config()
        self.engine = ImputeEngine(self.config)
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
        Execute complete Imputeman pipeline with clean orchestration.
        
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
        
        try:
            # Step 1: Initialize pipeline
            impute_op = self.engine.initialize(entity, schema)
            
            # Step 2: Search for URLs
            urls = await self.engine.search(impute_op, max_urls=max_urls)
            if not urls:
                return self.engine.finalize(impute_op, start_time)
            
            # Step 3: Process URLs (scrape + extract)
            await self.engine.process_urls(
                impute_op, 
                urls, 
                streaming=enable_streaming,
                capture_metrics=capture_detailed_metrics
            )
            
            # Step 4: Finalize and return results
            return self.engine.finalize(impute_op, start_time)
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline execution failed: {e}", exc_info=True)
            return self._handle_pipeline_failure(e, entity, schema, start_time, locals().get('impute_op'))
        
        finally:
            await self.engine.cleanup()
    
    def _handle_pipeline_failure(
        self, 
        exception: Exception, 
        entity: Union[str, EntityToImpute], 
        schema: List[WhatToRetain], 
        start_time: float,
        impute_op: Optional[ImputeOp] = None
    ) -> ImputeOp:
        """
        Handle pipeline failures with three-level fallback strategy.
        
        Level 1: Use existing impute_op if available (graceful recovery)
        Level 2: Create new impute_op via engine (semi-graceful) 
        Level 3: Manual ImputeOp creation (emergency fallback)
        
        Args:
            exception: The exception that caused the failure
            entity: Original entity being processed
            schema: Original schema being used
            start_time: Pipeline start time
            impute_op: Existing ImputeOp if available
            
        Returns:
            Failed ImputeOp with error details and partial results if possible
        """
        error_msg = f"Pipeline execution failed: {str(exception)}"
        
        # Level 1: Use existing impute_op (graceful recovery)
        if impute_op is not None:
            impute_op.errors.append(error_msg)
            return self.engine.finalize(impute_op, start_time)
        
        # Level 2: Create minimal failed operation (semi-graceful)
        try:
            failed_op = self.engine.initialize(entity, schema)
            failed_op.errors.append(error_msg)
            return self.engine.finalize(failed_op, start_time)
        except Exception:
            # Level 3: Last resort manual creation (emergency fallback)
            from .models import ImputeOp, PipelineStatus
            failed_op = ImputeOp(query=str(entity), schema=schema)
            failed_op.status = PipelineStatus.FAILED
            failed_op.errors.append(error_msg)
            return failed_op
    
    async def run_search_only(
        self,
        entity: Union[str, EntityToImpute],
        max_urls: int = None
    ) -> List[str]:
        """
        Run only the search phase and return URLs.
        
        Useful for testing or when you only need to find relevant URLs.
        
        Args:
            entity: Entity to search for
            max_urls: Maximum URLs to return
            
        Returns:
            List of URLs found
        """
        max_urls = max_urls or self.config.serp_config.top_k_results
        
        try:
            # Minimal schema just for initialization
            dummy_schema = [WhatToRetain(name="test", desc="test")]
            impute_op = self.engine.initialize(entity, dummy_schema)
            urls = await self.engine.search(impute_op, max_urls=max_urls)
            return urls
        finally:
            await self.engine.cleanup()
    
    async def run_batch_mode(
        self,
        entity: Union[str, EntityToImpute],
        schema: List[WhatToRetain],
        max_urls: int = None,
        capture_detailed_metrics: bool = True
    ) -> ImputeOp:
        """
        Run pipeline in batch mode (scrape all, then extract all).
        
        Alternative to streaming mode for specific use cases.
        
        Args:
            entity: Entity to search for
            schema: List of fields to extract
            max_urls: Maximum URLs to process
            capture_detailed_metrics: If True, capture timing data
            
        Returns:
            ImputeOp with results
        """
        return await self.run(
            entity=entity,
            schema=schema,
            max_urls=max_urls,
            enable_streaming=False,  # Force batch mode
            capture_detailed_metrics=capture_detailed_metrics
        )


# Helper function for entity conversion (if needed elsewhere)
def _ensure_entity(entity: Union[str, EntityToImpute]) -> EntityToImpute:
    """Convert string to EntityToImpute if needed"""
    if isinstance(entity, str):
        return EntityToImpute(name=entity)
    return entity


async def main():
    """Example usage of clean Imputeman orchestrator"""
    
    # Define what to extract
    schema = [
        WhatToRetain(name="component_type", desc="Type of electronic component"),
        WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
        WhatToRetain(name="package_type", desc="Physical package type")
    ]
    
    # Create entity (can also just use string "BAV99")
    entity = EntityToImpute(name="BAV99")
    
    # Get configuration
    config = get_development_config()
    
    print("🚀 Testing Clean Imputeman Orchestrator")
    print("=" * 50)
    print(f"🎯 Entity: {entity.name}")
    print(f"📋 Schema: {len(schema)} fields")
    print()
    
    # Explicit instantiation and execution - better visibility!
    imputeman = Imputeman(config)
    
    print("🔄 Running full pipeline with streaming...")
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
        print(f"\n📋 Extracted Content Sample:")
        for key, value in list(impute_op.content.items())[:2]:  # Show first 2 fields
            preview = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
            print(f"   {key}: {preview}")
    
    # Demonstrate search-only functionality
    print(f"\n🔄 Testing search-only functionality...")
    urls = await imputeman.run_search_only(entity, max_urls=3)
    print(f"   Found {len(urls)} URLs:")
    for i, url in enumerate(urls, 1):
        print(f"   {i}. {url[:60]}...")
    
    # Demonstrate batch mode
    print(f"\n🔄 Testing batch mode...")
    batch_result = await imputeman.run_batch_mode(entity, schema[:1], max_urls=2)  # Smaller test
    print(f"   Batch result: {batch_result.success} ({batch_result.performance.successful_extractions} extractions)")
    
    print(f"\n✅ All Imputeman orchestrator tests completed!")
    
    return impute_op


def main_sync():
    """Synchronous wrapper for main - explicit pattern"""
    return asyncio.run(main())


if __name__ == "__main__":
    print("🚀 Running Clean Imputeman Orchestrator...")
    print("   Command: python -m imputeman.imputeman")
    print()
    
    result = main_sync()
    
    if result and result.success:
        print(f"\n🎉 Imputeman orchestrator test completed successfully!")
    else:
        print(f"\n💥 Imputeman orchestrator test had issues - check logs above.")