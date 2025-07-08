# smoke_tests/test_1_run_imputeman_without_prefect.py
"""
Smoke Test 1: Run Imputeman core logic without Prefect
Tests the service layer directly to verify basic functionality works

python -m imputeman.smoke_tests.test_1_run_imputeman_without_prefect
"""

import asyncio
import sys
import os

# For package-relative imports
from ..core.entities import EntityToImpute, WhatToRetain
from ..core.config import get_development_config
from ..services import ServiceRegistry


async def test_services_directly():
    """Test each service individually without Prefect orchestration"""
    
    print("🧪 Smoke Test 1: Testing services directly without Prefect")
    print("=" * 60)
    
    # Setup
    entity = EntityToImpute(
        name="bav99",
        identifier_context="electronic component",
        impute_task_purpose="smoke test"
    )
    
    schema = [
        WhatToRetain(name="component_type", desc="Type of electronic component"),
        WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
        WhatToRetain(name="package_type", desc="Physical package type")
    ]
    
    config = get_development_config()
    registry = ServiceRegistry(config)
    
    try:
        # Test 1: SERP Service
        print("\n1️⃣ Testing SERP Service...")
        serp_result = await registry.serp.search(entity.name, top_k=3)
        
        print(f"   ✅ Search completed in {serp_result.elapsed_time:.2f}s")
        print(f"   📊 Found {len(serp_result.links)} URLs")
        print(f"   🔗 URLs: {serp_result.links[:2]}...")  # Show first 2 URLs
        
        if not serp_result.success:
            print(f"   ⚠️  Search failed: {serp_result.metadata}")
            return False
        
        # Test 2: Scraper Service  
        print("\n2️⃣ Testing Scraper Service...")
        if serp_result.links:
            # Test with just the first URL for speed
            test_urls = serp_result.links[:2]
            scrape_results = await registry.scraper.scrape_urls(test_urls)
            
            successful_scrapes = sum(1 for r in scrape_results.values() if r.status == "ready")
            total_scrape_cost = sum(r.cost or 0.0 for r in scrape_results.values())
            
            print(f"   ✅ Scraping completed")
            print(f"   📊 {successful_scrapes}/{len(test_urls)} successful scrapes")
            print(f"   💰 Total scrape cost: ${total_scrape_cost:.3f}")
            
            # Show sample scraped content
            for url, result in list(scrape_results.items())[:1]:
                if result.status == "ready" and result.data:
                    content_preview = result.data[:200] + "..." if len(result.data) > 200 else result.data
                    print(f"   📄 Sample content: {content_preview}")
                else:
                    error_msg = getattr(result, 'error_message', getattr(result, 'error', 'Unknown error'))
                    print(f"   ❌ Scrape failed for {url}: {error_msg}")
        else:
            print("   ⚠️  No URLs to scrape")
            return False
        
        # Test 3: Extractor Service (Updated for ExtractHero)
        print("\n3️⃣ Testing Extractor Service...")
        if scrape_results:
            extract_results = await registry.extractor.extract_from_scrapes(scrape_results, schema)
            
            successful_extractions = sum(1 for r in extract_results.values() if r.success)
            
            # Calculate metrics from ExtractHero's ExtractOp structure
            total_tokens = 0
            total_extract_cost = 0.0
            
            for result in extract_results.values():
                # ExtractHero stores usage in result.usage (combined from filter + parse phases)
                if result.usage:
                    print(f"   🔍 Usage structure: {result.usage}")  # Debug: show actual structure
                    
                    # Sum all numeric values that might represent tokens
                    for key, value in result.usage.items():
                        if isinstance(value, (int, float)) and 'token' in key.lower():
                            total_tokens += value
                    
                    # Look for cost fields
                    if 'total_cost' in result.usage:
                        total_extract_cost += result.usage['total_cost']
                    elif 'cost' in result.usage:
                        total_extract_cost += result.usage['cost']
            
            print(f"   ✅ Extraction completed")
            print(f"   📊 {successful_extractions}/{len(extract_results)} successful extractions")
            print(f"   🔤 Total tokens used: {total_tokens}")
            print(f"   💰 Total extraction cost: ${total_extract_cost:.3f}")
            
            # Show extracted data
            for url, result in extract_results.items():
                if result.success and result.content:
                    print(f"   📋 Extracted from {url}:")
                    for field_name, value in result.content.items():
                        print(f"      {field_name}: {value}")
                    
                    # ExtractHero doesn't have a single confidence_score, but we can show success
                    filter_success = result.filter_op.success if result.filter_op else False
                    parse_success = result.parse_op.success if result.parse_op else False
                    print(f"      filter_success: {filter_success}, parse_success: {parse_success}")
                    print(f"      elapsed_time: {result.elapsed_time:.2f}s")
                    break
        else:
            print("   ⚠️  No scrape results to extract from")
            return False
        
        print("\n🎉 All services working correctly!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        await registry.close_all()


async def test_simple_integration():
    """Test simple integration flow without Prefect"""
    
    print("\n" + "=" * 60)
    print("🔄 Testing Simple Integration Flow")
    print("=" * 60)
    
    entity = EntityToImpute(name="bav99")
    schema = [WhatToRetain(name="component_type", desc="Type of component")]
    
    config = get_development_config()
    registry = ServiceRegistry(config)
    
    try:
        # Run the full pipeline manually
        print("Running full pipeline...")
        
        # Step 1: Search
        serp_result = await registry.serp.search(entity.name, top_k=2)
        
        # Step 2: Scrape
        scrape_results = {}
        if serp_result.success and serp_result.links:
            scrape_results = await registry.scraper.scrape_urls(serp_result.links[:1])
        
        # Step 3: Extract  
        extract_results = {}
        if scrape_results:
            extract_results = await registry.extractor.extract_from_scrapes(scrape_results, schema)
        
        # Calculate total costs (Updated for ExtractHero)
        scrape_cost = sum(r.cost or 0.0 for r in scrape_results.values())
        
        extract_cost = 0.0
        for result in extract_results.values():
            if result.usage:
                print(f"   🔍 Extract usage: {result.usage}")  # Debug: show actual structure
                if 'total_cost' in result.usage:
                    extract_cost += result.usage['total_cost']
                elif 'cost' in result.usage:
                    extract_cost += result.usage['cost']
        
        total_cost = scrape_cost + extract_cost
        successful_extractions = sum(1 for r in extract_results.values() if r.success)
        
        print(f"✅ Pipeline completed!")
        print(f"💰 Total cost: ${total_cost:.3f} (scrape: ${scrape_cost:.3f}, extract: ${extract_cost:.3f})")
        print(f"📊 Successful extractions: {successful_extractions}")
        
        # Show final data
        if extract_results:
            print("📋 Final extracted data:")
            for url, result in extract_results.items():
                if result.success:
                    print(f"  From {url}: {result.content}")
                    print(f"    - Filter phase: {result.filter_op.success if result.filter_op else 'N/A'}")
                    print(f"    - Parse phase: {result.parse_op.success if result.parse_op else 'N/A'}")
                    print(f"    - Total time: {result.elapsed_time:.2f}s")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await registry.close_all()


async def main():
    """Run all smoke tests"""
    
    print("🚀 Starting Imputeman Smoke Tests")
    print("Testing core functionality without Prefect orchestration")
    
    # Run tests
    test1_passed = await test_services_directly()
    test2_passed = await test_simple_integration()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SMOKE TEST RESULTS")
    print("=" * 60)
    print(f"Test 1 (Services): {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Test 2 (Integration): {'✅ PASS' if test2_passed else '❌ FAIL'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All smoke tests passed! Ready for Prefect integration.")
    else:
        print("\n⚠️  Some tests failed. Fix issues before proceeding to Prefect.")
    
    return test1_passed and test2_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)