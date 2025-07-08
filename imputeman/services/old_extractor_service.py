# imputeman/services/extractor_service.py
"""Simplified extractor service that directly uses ExtractHero"""

import asyncio
from typing import Dict, List
from ..core.config import ExtractConfig
from ..core.entities import WhatToRetain
from brightdata.models import ScrapeResult
import time

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
    
    
    async def close(self):
        """Clean up resources"""
        pass




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
        start_time = time.time()
        
        
        try:
            if not self.extract_hero:
                raise Exception("ExtractHero not available")
            
            # Convert WhatToRetain to ExtractHero format
          
            
            # Use ExtractHero for extraction
            extract_op = await self.extract_hero.extract_async(
                text=html_content,
                extraction_spec=extraction_schema,
                text_type="html",
                reduce_html=reduce_html
            )
            
            
            return extract_op
            
        except Exception as e:
            # Return a failed ExtractOp
            from extracthero.schemes import FilterOp, ParseOp
            
          
            
            # Create failed filter and parse ops
            failed_filter_op = FilterOp(
                success=False,
                content=None,
                usage=None,
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
            
            return ExtractOp(
                filter_op=failed_filter_op,
                parse_op=failed_parse_op,
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
            ExtractOp with extracted data
        """
       
        
        try:
            if not self.extract_hero:
                raise Exception("ExtractHero not available")
            
            # Convert WhatToRetain to ExtractHero format
           
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
                extraction_spec=extraction_schema,
                text_type=text_type
            )
            
            
            
          
            return extract_op
            
        except Exception as e:
            # Return a failed ExtractOp
            from extracthero.schemes import FilterOp, ParseOp
            
            
            # Create failed filter and parse ops
            failed_filter_op = FilterOp(
                success=False,
                content=None,
                usage=None,
                config=self.extract_hero.config if self.extract_hero else None,
                reduced_html=None,
                error=f"JSON extraction failed: {str(e)}"
            )
            
            failed_parse_op = ParseOp(
                success=False,
                content=None,
                usage=None,
              
                config=self.extract_hero.config if self.extract_hero else None,
                error="Parse phase not reached"
            )
            
            return ExtractOp(
                filter_op=failed_filter_op,
                parse_op=failed_parse_op,
                content=None
            )