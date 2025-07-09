# imputeman/services/extractor_service.py
"""Simplified extractor service that directly uses ExtractHero - WITH DEBUG LOGGING"""

import logging
logger = logging.getLogger(__name__)

import asyncio
from typing import Dict, List, Union, Any
from ..core.config import ExtractConfig
from ..core.entities import WhatToRetain
from brightdata.models import ScrapeResult

# Import ExtractHero
try:
    from extracthero import ExtractHero
    from extracthero.schemes import WhatToRetain as ExtractHeroWhatToRetain, ExtractConfig as ExtractHeroConfig, ExtractOp
    EXTRACTHERO_AVAILABLE = True
except ImportError:
    EXTRACTHERO_AVAILABLE = False


class ExtractorService:
    """
    Simple service that wraps ExtractHero for the Imputeman pipeline
    """
    
    def __init__(self, config: ExtractConfig):
        self.config = config
        
        # Initialize ExtractHero
        if EXTRACTHERO_AVAILABLE:
            extracthero_config = ExtractHeroConfig()
            self.extract_hero = ExtractHero(config=extracthero_config)
            print(f"✅ ExtractHero initialized successfully")
        else:
            self.extract_hero = None
            print(f"❌ ExtractHero not available!")

    async def extract_from_scrapes(
        self, 
        scrape_results: Dict[str, ScrapeResult], 
        schema: List[WhatToRetain]
    ) -> Dict[str, ExtractOp]:
        """
        Extract data from scrape results using ExtractHero with URL context logging
        
        Args:
            scrape_results: Dictionary of URL -> ScrapeResult
            schema: List of WhatToRetain extraction schema
            
        Returns:
            Dictionary of URL -> ExtractOp (from extracthero)
        """
        print(f"🔍 DEBUG: Starting extraction from {len(scrape_results)} scrape results")
        
        if not self.extract_hero:
            error_msg = "ExtractHero not available"
            print(f"❌ DEBUG: {error_msg}")
            raise Exception(error_msg)
        
        # Filter successful scrapes
        valid_scrapes = {
            url: result for url, result in scrape_results.items()
            if result.status == "ready" and result.data
        }
        
        print(f"🔍 DEBUG: Found {len(valid_scrapes)} valid scrapes out of {len(scrape_results)} total")
        
        if not valid_scrapes:
            print(f"❌ DEBUG: No valid scrapes found!")
            for url, result in scrape_results.items():
                print(f"   - {url}: status={result.status}, has_data={bool(result.data)}")
            return {}
        
        # Convert schema to extracthero format
        extracthero_schema = self._convert_schema(schema)
        print(f"🔍 DEBUG: Converted schema to {len(extracthero_schema)} ExtractHero fields")
        
        # Extract from each valid scrape with URL context
        extract_tasks = []
        for url, scrape_result in valid_scrapes.items():
            print(f"🔍 DEBUG: Creating extraction task for {url[:50]}...")
            task = self._extract_single_with_url_context(url, scrape_result.data, extracthero_schema)
            extract_tasks.append((url, task))
        
        # Execute extractions
        print(f"🔍 DEBUG: Executing {len(extract_tasks)} extraction tasks...")
        results = await asyncio.gather(*[task for _, task in extract_tasks], return_exceptions=True)
        
        # Build results dictionary
        extract_results = {}
        for (url, _), result in zip(extract_tasks, results):
            if isinstance(result, Exception):
                print(f"❌ DEBUG: Extraction failed for {url}: {result}")
                extract_results[url] = self._create_failed_extract_op(str(result))
            else:
                print(f"✅ DEBUG: Extraction succeeded for {url}: success={result.success}")
                extract_results[url] = result
        
        print(f"🔍 DEBUG: Returning {len(extract_results)} extraction results")
        return extract_results

    async def _extract_single_with_url_context(self, url: str, html_data: str, schema: List[ExtractHeroWhatToRetain]) -> ExtractOp:
        """Extract from a single HTML document with URL context in logs"""
        
        print(f"🔍 DEBUG: Starting single extraction for {url[:50]}...")
        print(f"🔍 DEBUG: HTML data length: {len(html_data)} chars")
        print(f"🔍 DEBUG: Schema length: {len(schema)} fields")
        
        # Monkey patch the extract_hero methods to add URL context
        original_filter_async = self.extract_hero.filter_async
        original_parse_async = self.extract_hero.parse_async
        
        async def filter_with_context(*args, **kwargs):
            print(f"🧠 Started extracting[Filtering] from {url[:40]}...")
            try:
                result = await original_filter_async(*args, **kwargs)
                if result.success:
                    print(f"✅ Filter completed for {url[:40]}...")
                else:
                    print(f"❌ DEBUG: Filter failed for {url}: {result.error if hasattr(result, 'error') else 'Unknown error'}")
                return result
            except Exception as e:
                print(f"❌ DEBUG: Filter exception for {url}: {e}")
                raise
        
        async def parse_with_context(*args, **kwargs):
            print(f"🧠 Started extracting[Parsing] from {url[:40]}...")
            try:
                result = await original_parse_async(*args, **kwargs)
                if result.success:
                    print(f"✅ Parse completed for {url[:40]}...")
                else:
                    print(f"❌ DEBUG: Parse failed for {url}: {result.error if hasattr(result, 'error') else 'Unknown error'}")
                return result
            except Exception as e:
                print(f"❌ DEBUG: Parse exception for {url}: {e}")
                raise
        
        # Temporarily replace methods
        self.extract_hero.filter_async = filter_with_context
        self.extract_hero.parse_async = parse_with_context
        
        try:
            print(f"🔍 DEBUG: Calling extract_hero.extract_async for {url[:50]}...")
            result = await self.extract_hero.extract_async(
                text=html_data,
                extraction_spec=schema,
                text_type="html"
            )
            print(f"🔍 DEBUG: extract_async completed for {url[:50]}: success={result.success}")
            return result
        except Exception as e:
            print(f"❌ DEBUG: extract_async exception for {url}: {e}")
            raise
        finally:
            # Restore original methods
            self.extract_hero.filter_async = original_filter_async
            self.extract_hero.parse_async = original_parse_async
    
    def _convert_schema(self, schema: List[WhatToRetain]) -> List[ExtractHeroWhatToRetain]:
        """Convert WhatToRetain to ExtractHero format"""
        converted = []
        for item in schema:
            converted.append(ExtractHeroWhatToRetain(
                name=item.name,
                desc=item.desc,
                example=getattr(item, 'example', None)
            ))
        return converted
    
    def _create_failed_extract_op(self, error_msg: str) -> ExtractOp:
        """Create a failed ExtractOp for error cases"""
        from extracthero.schemes import FilterOp, ParseOp
        import time
        
        start_time = time.time()
        
        failed_filter_op = FilterOp(
            success=False,
            content=None,
            usage=None,
            elapsed_time=0.0,
            config=self.extract_hero.config,
            reduced_html=None,
            error=error_msg
        )
        
        failed_parse_op = ParseOp(
            success=False,
            content=None,
            usage=None,
            elapsed_time=0.0,
            config=self.extract_hero.config,
            error="Parse phase not reached"
        )
        
        return ExtractOp.from_operations(
            filter_op=failed_filter_op,
            parse_op=failed_parse_op,
            start_time=start_time,
            content=None
        )
    
    # ... (other methods remain the same)
    
    async def extract_from_scrapes_streaming(
        self, 
        scrape_tasks: Dict[asyncio.Task, str],  # Task -> URL mapping
        schema: List[WhatToRetain]
    ) -> Dict[str, ExtractOp]:
        """
        Stream extraction: extract as soon as each scrape completes
        
        Args:
            scrape_tasks: Dict mapping asyncio.Task -> URL
            schema: Extraction schema
            
        Returns:
            Dict mapping URL -> ExtractOp (results arrive incrementally)
        """
        extract_results = {}
        
        # Process scrapes as they complete (streaming!)
        for completed_scrape_task in asyncio.as_completed(scrape_tasks.keys()):
            try:
                url = scrape_tasks[completed_scrape_task]
                scrape_result = await completed_scrape_task
                
                # Immediately extract from this completed scrape
                if self._is_scrape_successful(scrape_result):
                    extract_result = await self.extract_from_single_scrape(scrape_result, schema)
                    extract_results.update(extract_result)
                else:
                    logger.debug(f"Scrape failed for {url}, skipping extraction")
                    
            except Exception as e:
                url = scrape_tasks.get(completed_scrape_task, "unknown")
                logger.debug(f"Streaming extraction failed for {url}: {e}")
        
        return extract_results
    
    async def extract_from_single_scrape(self, scrape_result, schema):
        """Extract from a single completed scrape result"""
        return await self.extract_from_scrapes(scrape_result, schema)
    
    def _is_scrape_successful(self, scrape_result):
        """Check if scrape result is valid for extraction"""
        return any(r.status == "ready" and r.data for r in scrape_result.values())
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get basic service info (no tracking)"""
        return {
            "service_type": "ExtractorService",
            "extracthero_available": self.extract_hero is not None,
            "config": str(self.config)
        }
    
    async def close(self):
        """Clean up resources"""
        pass