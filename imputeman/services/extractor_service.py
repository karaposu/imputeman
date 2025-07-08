# imputeman/services/extractor_service.py
"""AI-powered data extraction service for structured data extraction from HTML"""

import asyncio
import time
import json
from typing import Dict, Any, List, Optional, Union
import httpx

from ..core.config import ExtractConfig
from ..core.entities import WhatToRetain, ScrapeResult
from brightdata.models import ScrapeResult
from extracthero import ExtractOp

# Import ExtractHero for the actual extraction work
try:
    from extracthero import ExtractHero
    from extracthero.schemes import WhatToRetain as ExtractHeroWhatToRetain, ExtractConfig as ExtractHeroConfig
    EXTRACTHERO_AVAILABLE = True
except ImportError:
    EXTRACTHERO_AVAILABLE = False


class ExtractorService:
    """
    Service for AI-powered data extraction from HTML content
    
    This service abstracts away the details of different extraction methods
    and provides a consistent interface for structured data extraction.
    """
    
    def __init__(self, config: ExtractConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout_seconds)
        )
        
        # Initialize ExtractHero if available
        if EXTRACTHERO_AVAILABLE:
            # Create ExtractHero config
            extracthero_config = ExtractHeroConfig()
            self.extract_hero = ExtractHero(config=extracthero_config)
        else:
            self.extract_hero = None
    
    # ==================== New Methods for Test Compatibility ====================
    
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
            ExtractOp with extracted data (includes usage and elapsed_time)
        """
        try:
            if not self.extract_hero:
                raise Exception("ExtractHero not available")
            
            # Convert WhatToRetain to ExtractHero format
            extracthero_schema = self._convert_schema_to_extracthero(extraction_schema)
            
            # Use ExtractHero for extraction
            extract_op = await self.extract_hero.extract_async(
                text=html_content,
                extraction_spec=extracthero_schema,
                text_type="html",
                reduce_html=reduce_html
            )
            
            # ExtractOp already contains all metrics we need:
            # - extract_op.success
            # - extract_op.elapsed_time
            # - extract_op.usage
            # - extract_op.content
            
            return extract_op
            
        except Exception as e:
            # Return a failed ExtractOp
            from extracthero.schemes import FilterOp, ParseOp
            
            start_time = time.time()
            
            # Create failed filter and parse ops
            failed_filter_op = FilterOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                reduced_html=None,
                error=f"HTML extraction failed: {str(e)}"
            )
            
            failed_parse_op = ParseOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                error="Parse phase not reached"
            )
            
            # Create ExtractOp using from_operations
            return ExtractOp.from_operations(
                filter_op=failed_filter_op,
                parse_op=failed_parse_op,
                start_time=start_time,
                content=None
            )
    
    async def extract_from_json(
        self, 
        json_data: Union[Dict[str, Any], str], 
        extraction_schema: List[WhatToRetain]
    ) -> ExtractOp:
        """
        Extract structured data from JSON data
        
        Args:
            json_data: JSON data (dict or string)
            extraction_schema: List of WhatToRetain objects defining extraction schema
            
        Returns:
            ExtractOp with extracted data (includes usage and elapsed_time)
        """
        try:
            if not self.extract_hero:
                raise Exception("ExtractHero not available")
            
            # Convert WhatToRetain to ExtractHero format
            extracthero_schema = self._convert_schema_to_extracthero(extraction_schema)
            
            # Determine input type
            if isinstance(json_data, str):
                text_type = "json"
                input_data = json_data
            else:
                text_type = "dict"
                input_data = json_data
            
            # Use ExtractHero for extraction
            extract_op = await self.extract_hero.extract_async(
                text=input_data,
                extraction_spec=extracthero_schema,
                text_type=text_type
            )
            
            return extract_op
            
        except Exception as e:
            # Return a failed ExtractOp
            from extracthero.schemes import FilterOp, ParseOp
            
            start_time = time.time()
            
            # Create failed filter and parse ops
            failed_filter_op = FilterOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                reduced_html=None,
                error=f"JSON extraction failed: {str(e)}"
            )
            
            failed_parse_op = ParseOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                error="Parse phase not reached"
            )
            
            return ExtractOp.from_operations(
                filter_op=failed_filter_op,
                parse_op=failed_parse_op,
                start_time=start_time,
                content=None
            )
    
    async def extract_batch(
        self, 
        html_contents: Dict[str, str], 
        extraction_schema: List[WhatToRetain]
    ) -> Dict[str, ExtractOp]:
        """
        Extract structured data from multiple HTML contents
        
        Args:
            html_contents: Dictionary of identifier -> HTML content
            extraction_schema: List of WhatToRetain objects defining extraction schema
            
        Returns:
            Dictionary of identifier -> ExtractOp
        """
        extract_tasks = []
        
        for identifier, html_content in html_contents.items():
            task = self._extract_single_html(identifier, html_content, extraction_schema)
            extract_tasks.append(task)
        
        results = await asyncio.gather(*extract_tasks, return_exceptions=True)
        
        # Process results
        batch_results = {}
        for identifier, result in zip(html_contents.keys(), results):
            if isinstance(result, Exception):
                # Create failed ExtractOp
                from extracthero.schemes import FilterOp, ParseOp
                
                start_time = time.time()
                
                failed_filter_op = FilterOp(
                    success=False,
                    content=None,
                    usage=None,
                    elapsed_time=0.0,
                    config=self.extract_hero.config if self.extract_hero else None,
                    reduced_html=None,
                    error=str(result)
                )
                
                failed_parse_op = ParseOp(
                    success=False,
                    content=None,
                    usage=None,
                    elapsed_time=0.0,
                    config=self.extract_hero.config if self.extract_hero else None,
                    error="Parse phase not reached"
                )
                
                batch_results[identifier] = ExtractOp.from_operations(
                    filter_op=failed_filter_op,
                    parse_op=failed_parse_op,
                    start_time=start_time,
                    content=None
                )
            else:
                batch_results[identifier] = result
        
        return batch_results
    
    async def _extract_single_html(
        self, 
        identifier: str, 
        html_content: str, 
        extraction_schema: List[WhatToRetain]
    ) -> ExtractOp:
        """Helper method for batch extraction"""
        try:
            if not self.extract_hero:
                raise Exception("ExtractHero not available")
            
            # Convert schema
            extracthero_schema = self._convert_schema_to_extracthero(extraction_schema)
            
            # Extract using ExtractHero
            extract_op = await self.extract_hero.extract_async(
                text=html_content,
                extraction_spec=extracthero_schema,
                text_type="html"
            )
            
            return extract_op
            
        except Exception as e:
            # Return failed ExtractOp
            from extracthero.schemes import FilterOp, ParseOp
            
            start_time = time.time()
            
            failed_filter_op = FilterOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                reduced_html=None,
                error=f"Batch extraction failed: {str(e)}"
            )
            
            failed_parse_op = ParseOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                error="Parse phase not reached"
            )
            
            return ExtractOp.from_operations(
                filter_op=failed_filter_op,
                parse_op=failed_parse_op,
                start_time=start_time,
                content=None
            )
    
    def set_confidence_threshold(self, threshold: float):
        """Set confidence threshold for extractions"""
        self.config.confidence_threshold = threshold
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """
        Get usage statistics from ExtractOp results
        
        Note: This now returns a simple message since we don't track
        cumulative stats. Individual ExtractOp objects contain their
        own usage and timing information.
        """
        return {
            "message": "Usage statistics are available in individual ExtractOp results",
            "info": "Each ExtractOp contains 'usage' and 'elapsed_time' fields"
        }
    
    # ==================== Original Methods (for ScrapeResult compatibility) ====================
    
    async def extract_from_scrapes(
        self, 
        scrape_results: Dict[str, ScrapeResult], 
        schema: List[WhatToRetain]
    ) -> Dict[str, ExtractOp]:
        """
        Extract structured data from multiple scrape results
        
        Args:
            scrape_results: Dictionary of URL -> ScrapeResult
            schema: List of WhatToRetain objects defining extraction schema
            
        Returns:
            Dictionary of URL -> ExtractOp
        """
        # Filter successful scrapes
        valid_scrapes = {
            url: result for url, result in scrape_results.items()
            if result.status == "ready" and result.data
        }
        
        if not valid_scrapes:
            return {}
        
        # Extract from each valid scrape
        extract_tasks = [
            self.extract_single(url, scrape_result, schema)
            for url, scrape_result in valid_scrapes.items()
        ]
        
        results = await asyncio.gather(*extract_tasks, return_exceptions=True)
        
        # Process results
        extract_results = {}
        for (url, _), result in zip(valid_scrapes.items(), results):
            if isinstance(result, Exception):
                # Create failed ExtractOp
                from extracthero.schemes import FilterOp, ParseOp
                
                start_time = time.time()
                
                failed_filter_op = FilterOp(
                    success=False,
                    content=None,
                    usage=None,
                    elapsed_time=0.0,
                    config=self.extract_hero.config if self.extract_hero else None,
                    reduced_html=None,
                    error=str(result)
                )
                
                failed_parse_op = ParseOp(
                    success=False,
                    content=None,
                    usage=None,
                    elapsed_time=0.0,
                    config=self.extract_hero.config if self.extract_hero else None,
                    error="Parse phase not reached"
                )
                
                extract_results[url] = ExtractOp.from_operations(
                    filter_op=failed_filter_op,
                    parse_op=failed_parse_op,
                    start_time=start_time,
                    content=None
                )
            else:
                extract_results[url] = result
        
        return extract_results
    
    async def extract_single(
        self, 
        url: str, 
        scrape_result: ScrapeResult, 
        schema: List[WhatToRetain]
    ) -> ExtractOp:
        """
        Extract structured data from a single scraped page
        
        Args:
            url: URL of the page
            scrape_result: Scraping result containing HTML
            schema: Extraction schema
            
        Returns:
            ExtractOp with extracted data (includes usage and elapsed_time)
        """
        try:
            if not self.extract_hero:
                raise Exception("ExtractHero not available")
            
            # Use ExtractHero for extraction
            extracthero_schema = self._convert_schema_to_extracthero(schema)
            
            extract_op = await self.extract_hero.extract_async(
                text=scrape_result.data,
                extraction_spec=extracthero_schema,
                text_type="html"
            )
            
            return extract_op
            
        except Exception as e:
            # Return failed ExtractOp
            from extracthero.schemes import FilterOp, ParseOp
            
            start_time = time.time()
            
            failed_filter_op = FilterOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                reduced_html=None,
                error=f"Extraction failed: {str(e)}"
            )
            
            failed_parse_op = ParseOp(
                success=False,
                content=None,
                usage=None,
                elapsed_time=0.0,
                config=self.extract_hero.config if self.extract_hero else None,
                error="Parse phase not reached"
            )
            
            return ExtractOp.from_operations(
                filter_op=failed_filter_op,
                parse_op=failed_parse_op,
                start_time=start_time,
                content=None
            )
    
    # ==================== Helper Methods ====================
    
    def _convert_schema_to_extracthero(self, schema: List[WhatToRetain]) -> List[ExtractHeroWhatToRetain]:
        """Convert WhatToRetain schema to ExtractHero format"""
        if not EXTRACTHERO_AVAILABLE:
            raise Exception("ExtractHero not available")
        
        extracthero_schema = []
        for item in schema:
            extracthero_item = ExtractHeroWhatToRetain(
                name=item.name,
                desc=item.desc,
                example=item.example
            )
            extracthero_schema.append(extracthero_item)
        
        return extracthero_schema
    
    async def close(self):
        """Clean up resources"""
        await self.client.aclose()