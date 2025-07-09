# imputeman/services/extractor_service.py
"""Simplified extractor service that directly uses ExtractHero"""

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
        else:
            self.extract_hero = None

    
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
                
                print(f"🔄 Scrape completed for {url[:40]}..., starting extraction immediately")
                
                # Immediately extract from this completed scrape
                if self._is_scrape_successful(scrape_result):
                    extract_result = await self.extract_from_single_scrape(scrape_result, schema)
                    extract_results.update(extract_result)
                    
                    print(f"✅ Extraction completed for {url[:40]}...")
                else:
                    print(f"⚠️  Scrape failed for {url[:40]}..., skipping extraction")
                    
            except Exception as e:
                url = scrape_tasks.get(completed_scrape_task, "unknown")
                print(f"❌ Streaming extraction failed for {url}: {e}")
        
        return extract_results
    
    async def extract_from_single_scrape(self, scrape_result, schema):
        """Extract from a single completed scrape result"""
        return await self.extract_from_scrapes(scrape_result, schema)
    
    def _is_scrape_successful(self, scrape_result):
        """Check if scrape result is valid for extraction"""
        return any(r.status == "ready" and r.data for r in scrape_result.values())




    
    async def extract_from_html(
        self, 
        html_content: str, 
        extraction_schema: List[WhatToRetain],
        reduce_html: bool = True
    ) -> ExtractOp:
        """
        Extract structured data from HTML content
        
        Args:
            html_content: Raw HTML content
            extraction_schema: List of WhatToRetain objects defining extraction schema
            reduce_html: Whether to reduce HTML size before extraction
            
        Returns:
            ExtractOp with extracted data
        """
        if not self.extract_hero:
            raise Exception("ExtractHero not available")
        
        # Convert WhatToRetain to ExtractHero format
        extracthero_schema = self._convert_schema(extraction_schema)
        
        # Use ExtractHero for extraction
        return await self.extract_hero.extract_async(
            text=html_content,
            extraction_spec=extracthero_schema,
            text_type="html",
            reduce_html=reduce_html
        )
    
    async def extract_from_json(
        self, 
        json_data, 
        extraction_schema: List[WhatToRetain]
    ) -> ExtractOp:
        """
        Extract structured data from JSON data
        
        Args:
            json_data: JSON data (dict or string)
            extraction_schema: List of WhatToRetain objects defining extraction schema
            
        Returns:
            ExtractOp with extracted data
        """
        if not self.extract_hero:
            raise Exception("ExtractHero not available")
        
        # Convert WhatToRetain to ExtractHero format
        extracthero_schema = self._convert_schema(extraction_schema)
        
        # Determine input type
        if isinstance(json_data, str):
            text_type = "json"
            input_data = json_data
        else:
            text_type = "dict"
            input_data = json_data
        
        # Use ExtractHero for extraction
        return await self.extract_hero.extract_async(
            text=input_data,
            extraction_spec=extracthero_schema,
            text_type=text_type
        )

    async def extract_from_scrapes(
        self, 
        scrape_results: Dict[str, ScrapeResult], 
        schema: List[WhatToRetain]
    ) -> Dict[str, ExtractOp]:
        """
        Extract data from scrape results using ExtractHero
        
        Args:
            scrape_results: Dictionary of URL -> ScrapeResult
            schema: List of WhatToRetain extraction schema
            
        Returns:
            Dictionary of URL -> ExtractOp (from extracthero)
        """
        if not self.extract_hero:
            raise Exception("ExtractHero not available")
        
        # Filter successful scrapes
        valid_scrapes = {
            url: result for url, result in scrape_results.items()
            if result.status == "ready" and result.data
        }
        
        if not valid_scrapes:
            return {}
        
        # Convert schema to extracthero format
        extracthero_schema = self._convert_schema(schema)
        
        # Extract from each valid scrape
        extract_tasks = [
            self._extract_single(url, scrape_result.data, extracthero_schema)
            for url, scrape_result in valid_scrapes.items()
        ]
        
        results = await asyncio.gather(*extract_tasks, return_exceptions=True)
        
        # Build results dictionary
        extract_results = {}
        for (url, _), result in zip(valid_scrapes.items(), results):
            if isinstance(result, Exception):
                # Create a failed ExtractOp
                extract_results[url] = self._create_failed_extract_op(str(result))
            else:
                extract_results[url] = result
        
        return extract_results
    
    async def _extract_single(self, url: str, html_data: str, schema: List[ExtractHeroWhatToRetain]) -> ExtractOp:
        """Extract from a single HTML document"""
        return await self.extract_hero.extract_async(
            text=html_data,
            extraction_spec=schema,
            text_type="html"
        )
    
    def _convert_schema(self, schema: List[WhatToRetain]) -> List[ExtractHeroWhatToRetain]:
        """Convert WhatToRetain to ExtractHero format"""
        return [
            ExtractHeroWhatToRetain(
                name=item.name,
                desc=item.desc,
                example=item.example
            )
            for item in schema
        ]
    
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