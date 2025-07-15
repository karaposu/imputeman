# imputeman/services/extractor_service.py

# to run python -m imputeman.services.extractor_service
"""Extractor service that uses the updated ExtractHero with enhanced features"""

import asyncio
import logging
from typing import Dict, List, Union, Any, Optional
from ..core.config import ExtractConfig
from ..core.entities import WhatToRetain
from brightdata.models import ScrapeResult

# Import ExtractHero - required dependency
from extracthero import ExtractHero
from extracthero.schemes import (
    WhatToRetain as ExtractHeroWhatToRetain, 
    ExtractConfig as ExtractHeroConfig, 
    ExtractOp
)

logger = logging.getLogger(__name__)


class ExtractorService:
    """
    Service that wraps ExtractHero for the Imputeman pipeline
    
    Features:
    - HTML reduction for better extraction
    - Token tracking across stages
    - Streaming extraction support
    - Configurable trimming for large documents
    """
    
    def __init__(self, config: ExtractConfig):
        self.config = config
        
        # Initialize ExtractHero with config
        extracthero_config = ExtractHeroConfig()
        self.extract_hero = ExtractHero(config=extracthero_config)
        
        # Default settings
        self.reduce_html = True  # Enable HTML reduction by default
        self.trim_char_length = 100000  # Trim documents larger than 100k chars
        self.filter_strategy = "contextual"  # Default filter strategy
    
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
                
                logger.info(f"🔄 Scrape completed for {url[:40]}..., starting extraction immediately")
                
                # Immediately extract from this completed scrape
                if self._is_scrape_successful(scrape_result):
                    extract_result = await self.extract_from_single_scrape(scrape_result, schema)
                    extract_results.update(extract_result)
                    
                    # Log extraction details
                    for extract_url, extract_op in extract_result.items():
                        if extract_op.success:
                            logger.info(f"✅ Extraction completed for {extract_url[:40]}...")
                            if extract_op.trimmed_to:
                                logger.debug(f"   ⚠️  Document was trimmed to {extract_op.trimmed_to:,} chars")
                            if extract_op.stage_tokens:
                                logger.debug(f"   📊 {extract_op.token_summary}")
                        else:
                            logger.warning(f"❌ Extraction failed for {extract_url[:40]}...: {extract_op.error}")
                else:
                    logger.warning(f"⚠️  Scrape failed for {url[:40]}..., skipping extraction")
                    
            except Exception as e:
                url = scrape_tasks.get(completed_scrape_task, "unknown")
                logger.error(f"❌ Streaming extraction failed for {url}: {e}")
        
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
        reduce_html: Optional[bool] = None,
        filter_strategy: Optional[str] = None,
        model_name: Optional[str] = None
    ) -> ExtractOp:
        """
        Extract structured data from HTML content
        
        Args:
            html_content: Raw HTML content
            extraction_schema: List of WhatToRetain objects defining extraction schema
            reduce_html: Whether to reduce HTML size before extraction
            filter_strategy: Filter strategy to use ("contextual", "liberal", "inclusive", etc.)
            model_name: Specific model to use for extraction
            
        Returns:
            ExtractOp with extracted data and detailed metrics
        """
        # Convert WhatToRetain to ExtractHero format
        extracthero_schema = self._convert_schema(extraction_schema)
        
        # Use provided settings or defaults
        reduce_html = reduce_html if reduce_html is not None else self.reduce_html
        filter_strategy = filter_strategy or self.filter_strategy
        
        # Use ExtractHero for extraction with new API
        return await self.extract_hero.extract_async(
            text=html_content,
            extraction_spec=extracthero_schema,
            filter_strategy=filter_strategy,
            reduce_html=reduce_html,
            model_name=model_name,
            trim_char_length=self.trim_char_length
        )
    
    async def extract_from_json(
        self, 
        json_data: Union[str, dict], 
        extraction_schema: List[WhatToRetain],
        model_name: Optional[str] = None
    ) -> ExtractOp:
        """
        Extract structured data from JSON data
        
        Args:
            json_data: JSON data (dict or string)
            extraction_schema: List of WhatToRetain objects defining extraction schema
            model_name: Specific model to use for extraction
            
        Returns:
            ExtractOp with extracted data
        """
        # Convert WhatToRetain to ExtractHero format
        extracthero_schema = self._convert_schema(extraction_schema)
        
        # Use ExtractHero for extraction
        return await self.extract_hero.extract_async(
            text=json_data,
            extraction_spec=extracthero_schema,
            filter_strategy="liberal",  # More liberal for JSON
            reduce_html=False,  # Not HTML
            model_name=model_name
        )

    async def extract_from_scrapes(
        self, 
        scrape_results: Dict[str, ScrapeResult], 
        schema: List[WhatToRetain],
        model_name: Optional[str] = None
    ) -> Dict[str, ExtractOp]:
        """
        Extract data from scrape results using ExtractHero
        
        Args:
            scrape_results: Dictionary of URL -> ScrapeResult
            schema: List of WhatToRetain extraction schema
            model_name: Specific model to use for extraction
            
        Returns:
            Dictionary of URL -> ExtractOp (from extracthero)
        """
        # Filter successful scrapes
        valid_scrapes = {
            url: result for url, result in scrape_results.items()
            if result.status == "ready" and result.data
        }
        
        if not valid_scrapes:
            logger.warning("No valid scrapes to extract from")
            return {}
        
        # Convert schema to extracthero format
        extracthero_schema = self._convert_schema(schema)
        
        # Extract from each valid scrape
        extract_tasks = []
        for url, scrape_result in valid_scrapes.items():
            # Log progress
            logger.info(f"🧠 Starting extraction[Filtering] from {url[:40]}...")
            
            # Create extraction task with appropriate settings
            task = self._extract_single(
                url, 
                scrape_result.data, 
                extracthero_schema,
                model_name=model_name
            )
            extract_tasks.append(task)
        
        results = await asyncio.gather(*extract_tasks, return_exceptions=True)
        
        # Build results dictionary
        extract_results = {}
        for (url, _), result in zip(valid_scrapes.items(), results):
            if isinstance(result, Exception):
                logger.error(f"Extraction failed for {url}: {result}")
                # Create a failed ExtractOp
                extract_results[url] = self._create_failed_extract_op(str(result))
            else:
                extract_results[url] = result
                
                # Log token usage if available
                if result.stage_tokens:
                    logger.debug(f"Token usage for {url[:40]}...:")
                    for stage, tokens in result.stage_tokens.items():
                        logger.debug(f"  {stage}: {tokens.get('input', 0)} → {tokens.get('output', 0)} tokens")
        
        return extract_results
    
    async def _extract_single(
        self, 
        url: str, 
        html_data: str, 
        schema: List[ExtractHeroWhatToRetain],
        model_name: Optional[str] = None
    ) -> ExtractOp:
        """Extract from a single HTML document"""
        # Determine if we should reduce HTML based on size
        reduce_html = self.reduce_html and len(html_data) > 10000  # Only reduce large docs
        
        extract_op = await self.extract_hero.extract_async(
            text=html_data,
            extraction_spec=schema,
            filter_strategy=self.filter_strategy,
            reduce_html=reduce_html,
            model_name=model_name,
            trim_char_length=self.trim_char_length
        )
        
        # Log extraction phase completion
        if extract_op.success:
            logger.info(f"✅ Extracted[Parsing] from {url[:40]}...")
        else:
            logger.warning(f"❌ Extraction failed for {url[:40]}...")
        
        return extract_op
    
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
            error=error_msg
        )
        
        failed_parse_op = ParseOp(
            success=False,
            content=None,
            usage=None,
            elapsed_time=0.0,
            config=self.extract_hero.config,
            error="Parse phase not reached due to filter failure"
        )
        
        return ExtractOp.from_operations(
            filter_op=failed_filter_op,
            parse_op=failed_parse_op,
            start_time=start_time,
            content=None
        )
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get basic service info"""
        return {
            "service_type": "ExtractorService",
            "extracthero_version": "2.0",  # New version with enhanced features
            "config": {
                "reduce_html": self.reduce_html,
                "trim_char_length": self.trim_char_length,
                "filter_strategy": self.filter_strategy
            }
        }
    
    async def close(self):
        """Clean up resources"""
        pass
    
    def set_filter_strategy(self, strategy: str):
        """
        Set the filter strategy for extractions
        
        Args:
            strategy: One of "contextual", "liberal", "inclusive", "base"
        """
        valid_strategies = ["contextual", "liberal", "inclusive", "base"]
        if strategy in valid_strategies:
            self.filter_strategy = strategy
        else:
            raise ValueError(f"Invalid strategy. Must be one of: {valid_strategies}")
    
    def set_trim_length(self, char_length: Optional[int]):
        """
        Set the character length for trimming large documents
        
        Args:
            char_length: Maximum characters before trimming (None to disable)
        """
        self.trim_char_length = char_length


async def main():
    """
    Test the ExtractorService independently
    
    Run with: python -m imputeman.services.extractor_service
    """
    print("=== Testing ExtractorService ===")
    print()
    
    # Initialize service
    try:
        from ..core.config import ExtractConfig
        config = ExtractConfig()
    except:
        # Fallback for standalone testing
        config = type('ExtractConfig', (), {
            'extraction_model': 'gpt-4o-mini',
            'confidence_threshold': 0.7,
            'max_tokens': 4000
        })()
    
    service = ExtractorService(config)
    
    print(f"✅ ExtractorService initialized")
    print(f"   - HTML reduction: {service.reduce_html}")
    print(f"   - Trim length: {service.trim_char_length:,} chars")
    print(f"   - Filter strategy: {service.filter_strategy}")
    print()
    
    # Define test schema
    from ..core.entities import WhatToRetain
    
    schema = [
        WhatToRetain(
            name="product_name",
            desc="Name or title of the product",
            example="iPhone 15 Pro"
        ),
        WhatToRetain(
            name="price",
            desc="Product price with currency",
            example="$999"
        ),
        WhatToRetain(
            name="features",
            desc="Key features or specifications",
            example="5G, A17 Pro chip, titanium design"
        )
    ]
    
    # Test 1: Basic HTML extraction
    print("Testing basic HTML extraction...")
    html_content = """
    <html>
    <body>
        <div class="product-page">
            <h1 class="title">iPhone 15 Pro</h1>
            <div class="pricing">
                <span class="price">$999</span>
                <span class="currency">USD</span>
            </div>
            <div class="features">
                <h2>Key Features</h2>
                <ul>
                    <li>A17 Pro chip with 6-core GPU</li>
                    <li>Pro camera system with 48MP main camera</li>
                    <li>Titanium design with Action Button</li>
                    <li>All-day battery life</li>
                </ul>
            </div>
            <div class="specs">
                <p>Display: 6.1-inch Super Retina XDR</p>
                <p>Storage: 128GB, 256GB, 512GB, 1TB</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    print(f"📄 Input HTML: {len(html_content)} chars")
    
    try:
        result = await service.extract_from_html(html_content, schema)
        
        print(f"\n📊 Extraction Results:")
        print(f"   Success: {result.success}")
        
        if result.success:
            print(f"   Extracted content:")
            for key, value in result.content.items():
                print(f"     - {key}: {value}")
            
            print(f"\n   Performance:")
            print(f"     - Total time: {result.elapsed_time:.2f}s")
            if result.usage:
                print(f"     - Total cost: ${result.usage.get('total_cost', 0):.4f}")
            
            # Show token usage
            if result.stage_tokens:
                print(f"\n   Token usage by stage:")
                for stage, tokens in result.stage_tokens.items():
                    reduction = 0
                    if tokens.get('input', 0) > 0:
                        reduction = (1 - tokens.get('output', 0) / tokens['input']) * 100
                    print(f"     - {stage}: {tokens.get('input', 0)} → {tokens.get('output', 0)} tokens ({reduction:.1f}% reduction)")
        else:
            print(f"   ❌ Extraction failed: {result.error}")
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 2: Test with large document and trimming
    print("\n" + "="*50)
    print("Testing document trimming...")
    
    # Create a large document
    large_html = f"""
    <html>
    <body>
        <h1>Large Product Catalog</h1>
        {"<div class='product'>Product description here...</div>" * 1000}
        <div class="target-product">
            <h2>Special Product</h2>
            <p>Price: $199</p>
            <p>Features: Advanced features here</p>
        </div>
    </body>
    </html>
    """
    
    print(f"📄 Large document: {len(large_html):,} chars")
    
    # Set smaller trim length
    service.set_trim_length(50000)
    print(f"   Setting trim length to: 50,000 chars")
    
    result = await service.extract_from_html(
        large_html, 
        [WhatToRetain(name="special_product", desc="Information about the special product")]
    )
    
    if result.trimmed_to:
        print(f"   ✂️  Document was trimmed to {result.trimmed_to:,} chars")
    
    print(f"   Success: {result.success}")
    if result.success and result.content:
        print(f"   Found content: {bool(result.content)}")
    
    # Test 3: Different filter strategies
    print("\n" + "="*50)
    print("Testing filter strategies...")
    
    test_html = """
    <div class="products">
        <div class="main-product">
            <h2>Main Product: Laptop Pro</h2>
            <p>Price: $1299</p>
        </div>
        <div class="related">
            <h3>You might also like:</h3>
            <p>Laptop Case - $49</p>
            <p>Mouse - $29</p>
        </div>
    </div>
    """
    
    strategies = ["contextual", "liberal", "inclusive"]
    
    for strategy in strategies:
        print(f"\n   Testing '{strategy}' strategy:")
        service.set_filter_strategy(strategy)
        
        result = await service.extract_from_html(
            test_html,
            [WhatToRetain(name="main_product_info", desc="Information about the main product only")]
        )
        
        if result.success:
            filtered_length = len(str(result.filter_content)) if result.filter_content else 0
            print(f"     - Filtered content length: {filtered_length} chars")
            print(f"     - Extracted: {result.content}")
    
    # Test 4: Batch extraction from multiple sources
    print("\n" + "="*50)
    print("Testing batch extraction...")
    
    # Simulate scrape results
    scrape_results = {
        "https://example1.com": type('ScrapeResult', (), {
            'status': 'ready',
            'data': '<html><body><h1>Product A</h1><p>Price: $100</p></body></html>'
        })(),
        "https://example2.com": type('ScrapeResult', (), {
            'status': 'ready',
            'data': '<html><body><h1>Product B</h1><p>Price: $200</p></body></html>'
        })(),
        "https://example3.com": type('ScrapeResult', (), {
            'status': 'failed',
            'data': None
        })()
    }
    
    simple_schema = [
        WhatToRetain(name="product", desc="Product name"),
        WhatToRetain(name="price", desc="Product price")
    ]
    
    extract_results = await service.extract_from_scrapes(scrape_results, simple_schema)
    
    print(f"📊 Batch extraction results:")
    print(f"   Total URLs: {len(scrape_results)}")
    print(f"   Successful extractions: {sum(1 for r in extract_results.values() if r.success)}")
    
    for url, extract_op in extract_results.items():
        status = "✅" if extract_op.success else "❌"
        print(f"\n   {status} {url}")
        if extract_op.success:
            print(f"      Content: {extract_op.content}")
            print(f"      Time: {extract_op.elapsed_time:.2f}s")
        else:
            print(f"      Error: {extract_op.error}")
    
    # Test 5: JSON extraction
    print("\n" + "="*50)
    print("Testing JSON extraction...")
    
    json_data = {
        "products": [
            {
                "name": "Widget Pro",
                "price": "$299",
                "features": ["Fast", "Reliable", "Efficient"]
            }
        ],
        "metadata": {
            "last_updated": "2024-01-15",
            "total_products": 1
        }
    }
    
    result = await service.extract_from_json(json_data, schema)
    
    print(f"📊 JSON extraction:")
    print(f"   Success: {result.success}")
    if result.success:
        print(f"   Content: {result.content}")
    
    # Summary
    print("\n" + "="*50)
    print("📈 Service Statistics:")
    stats = service.get_usage_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    await service.close()
    print("\n✅ All tests completed!")


def main_sync():
    """Synchronous wrapper for testing"""
    return asyncio.run(main())


if __name__ == "__main__":
    print("🚀 Running ExtractorService tests...")
    print("   Command: python -m imputeman.services.extractor_service")
    print()
    
    main_sync()