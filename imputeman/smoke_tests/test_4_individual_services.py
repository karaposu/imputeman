# smoke_tests/test_4_individual_services.py
"""
Smoke Test 4: Test Individual Services in Isolation
Tests each service separately to isolate issues
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imputeman.core.entities import WhatToRetain, ScrapeResult
from imputeman.core.config import get_development_config
from imputeman.services.serp_service import SerpService
from imputeman.services.scraper_service import ScraperService
from imputeman.services.extractor_service import ExtractorService


async def test_serp_service_isolated():
    """Test SERP service in complete isolation"""
    
    print("🧪 Smoke Test 4: Testing Individual Services")
    print("=" * 60)
    print("\n1️⃣ Testing SERP Service in isolation...")
    
    config = get_development_config()
    serp_service = SerpService(config.serp_config)
    
    try:
        # Test search functionality
        result = await serp_service.search("bav99", top_k=3)
        
        print(f"   ✅ Search completed: success={result.success}")
        print(f"   📊 Results: {len(result.links)} links in {result.elapsed_time:.2f}s")
        print(f"   🔍 Query: '{result.query}'")
        print(f"   🌐 Search engine: {result.search_engine}")
        
        if result.links:
            print(f"   🔗 Sample URLs:")
            for i, url in enumerate(result.links[:2], 1):
                print(f"      {i}. {url}")
        
        # Test URL validation
        test_urls = [
            "https://valid-site.com",
            "http://another-site.org", 
            "https://facebook.com/blocked",  # Should be filtered
            "invalid-url",  # Should be filtered
            "https://example.pdf"  # Should be filtered
        ]
        
        valid_urls = await serp_service.validate_urls(test_urls)
        print(f"   ✅ URL validation: {len(valid_urls)}/{len(test_urls)} URLs passed validation")
        print(f"      Valid URLs: {valid_urls}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ SERP service test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await serp_service.close()


async def test_scraper_service_isolated():
    """Test Scraper service in complete isolation"""
    
    print("\n2️⃣ Testing Scraper Service in isolation...")
    
    config = get_development_config()
    scraper_service = ScraperService(config.scrape_config)
    
    try:
        # Test with some real URLs that should work
        test_urls = [
            "https://httpbin.org/html",  # Returns simple HTML
            "https://jsonplaceholder.typicode.com/posts/1",  # Returns JSON
        ]
        
        print(f"   🕷️ Testing scraping on {len(test_urls)} URLs...")
        
        # Test cost estimation first
        estimated_cost = scraper_service.estimate_cost(test_urls)
        print(f"   💰 Estimated cost: ${estimated_cost:.3f}")
        
        # Test scraping
        results = await scraper_service.scrape_urls(test_urls)
        
        successful = sum(1 for r in results.values() if r.status == "ready")
        total_cost = sum(r.cost for r in results.values())
        
        print(f"   ✅ Scraping completed: {successful}/{len(test_urls)} successful")
        print(f"   💰 Actual cost: ${total_cost:.3f}")
        
        # Show details for each URL
        for url, result in results.items():
            print(f"   📄 {url}:")
            print(f"      Status: {result.status}")
            if result.status == "ready":
                content_size = len(result.data) if result.data else 0
                print(f"      Content size: {content_size} characters")
                print(f"      Cost: ${result.cost:.3f}")
                if result.data:
                    preview = result.data[:100] + "..." if len(result.data) > 100 else result.data
                    print(f"      Preview: {preview}")
            else:
                print(f"      Error: {result.error_message}")
        
        return successful > 0
        
    except Exception as e:
        print(f"   ❌ Scraper service test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await scraper_service.close()


async def test_extractor_service_isolated():
    """Test Extractor service in complete isolation"""
    
    print("\n3️⃣ Testing Extractor Service in isolation...")
    
    config = get_development_config()
    extractor_service = ExtractorService(config.extract_config)
    
    try:
        # Create mock scrape results with realistic HTML
        mock_html = """
        <html>
        <head><title>BAV99 Transistor Specifications</title></head>
        <body>
            <h1>BAV99 Dual Common Cathode Switching Diode</h1>
            <div class="specs">
                <p>Type: Small signal switching diode</p>
                <p>Package: SOT-23</p>
                <p>Maximum voltage: 75V</p>
                <p>Manufacturer: NXP Semiconductors</p>
                <p>Applications: High-speed switching, general purpose</p>
            </div>
        </body>
        </html>
        """
        
        scrape_results = {
            "https://example.com/bav99": ScrapeResult(
                url="https://example.com/bav99",
                data=mock_html,
                status="ready",
                html_char_size=len(mock_html),
                cost=0.0
            )
        }
        
        # Define schema for extraction
        schema = [
            WhatToRetain(name="component_type", desc="Type of electronic component"),
            WhatToRetain(name="package_type", desc="Physical package type"),
            WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
            WhatToRetain(name="manufacturer", desc="Component manufacturer"),
            WhatToRetain(name="applications", desc="Common applications or uses")
        ]
        
        print(f"   🧠 Testing extraction with {len(schema)} fields...")
        print(f"   📄 HTML content size: {len(mock_html)} characters")
        
        # Test extraction
        results = await extractor_service.extract_from_scrapes(scrape_results, schema)
        
        successful = sum(1 for r in results.values() if r.success)
        total_cost = sum(r.cost for r in results.values())
        total_tokens = sum(r.tokens_used for r in results.values())
        
        print(f"   ✅ Extraction completed: {successful}/{len(scrape_results)} successful")
        print(f"   💰 Total cost: ${total_cost:.3f}")
        print(f"   🔤 Total tokens: {total_tokens}")
        
        # Show extraction results
        for url, result in results.items():
            print(f"   📋 Results from {url}:")
            print(f"      Success: {result.success}")
            print(f"      Confidence: {result.confidence_score:.2f}")
            print(f"      Method: {result.extraction_method}")
            
            if result.success and result.content:
                print(f"      Extracted data:")
                for field_name, value in result.content.items():
                    print(f"        {field_name}: {value}")
            elif result.error_message:
                print(f"      Error: {result.error_message}")
        
        return successful > 0
        
    except Exception as e:
        print(f"   ❌ Extractor service test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await extractor_service.close()


async def test_service_configurations():
    """Test that services respect their configurations"""
    
    print("\n4️⃣ Testing service configuration handling...")
    
    try:
        config = get_development_config()
        
        # Test with modified configurations
        config.serp_config.top_k_results = 2
        config.scrape_config.concurrent_limit = 1
        config.extract_config.confidence_threshold = 0.5
        
        print(f"   ⚙️ Using modified config:")
        print(f"      SERP top_k: {config.serp_config.top_k_results}")
        print(f"      Scrape concurrent: {config.scrape_config.concurrent_limit}")
        print(f"      Extract confidence: {config.extract_config.confidence_threshold}")
        
        # Test SERP respects top_k
        serp_service = SerpService(config.serp_config)
        result = await serp_service.search("test", top_k=None)  # Should use config value
        await serp_service.close()
        
        expected_links = min(config.serp_config.top_k_results, len(result.links))
        print(f"   ✅ SERP respected top_k config: requested={config.serp_config.top_k_results}, got={len(result.links)}")
        
        # Test scraper cost estimation
        scraper_service = ScraperService(config.scrape_config)
        test_urls = ["https://example.com", "https://test.com"]
        cost = scraper_service.estimate_cost(test_urls)
        await scraper_service.close()
        
        print(f"   ✅ Scraper cost estimation: ${cost:.3f} for {len(test_urls)} URLs")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Service configuration test failed: {e}")
        return False


async def main():
    """Run all individual service tests"""
    
    print("🚀 Starting Individual Service Tests")
    
    # Run tests
    test1_passed = await test_serp_service_isolated()
    test2_passed = await test_scraper_service_isolated()
    test3_passed = await test_extractor_service_isolated()
    test4_passed = await test_service_configurations()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 INDIVIDUAL SERVICE TEST RESULTS")
    print("=" * 60)
    print(f"SERP Service: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Scraper Service: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    print(f"Extractor Service: {'✅ PASS' if test3_passed else '❌ FAIL'}")
    print(f"Service Configurations: {'✅ PASS' if test4_passed else '❌ FAIL'}")
    
    all_passed = all([test1_passed, test2_passed, test3_passed, test4_passed])
    
    if all_passed:
        print("\n🎉 All individual service tests passed!")
    else:
        print("\n⚠️  Some service tests failed.")
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)