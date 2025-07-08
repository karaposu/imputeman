# smoke_tests/test_8_integration_basic.py
"""
Smoke Test 8: Run Imputeman core logic without Prefect
Tests the service layer directly to verify basic functionality works

python -m imputeman.smoke_tests.test_8_integration_basic
"""


import asyncio
import sys
import os

# For package-relative imports
from ..core.entities import EntityToImpute, WhatToRetain
from ..core.config import get_development_config
from ..services import ServiceRegistry


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

    test2_passed = await test_simple_integration()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SMOKE TEST RESULTS")
    print("=" * 60)
   
    print(f"Test basic Integration: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    
    if test2_passed:
        print("\n🎉 All smoke tests passed! Ready for Prefect integration.")
    else:
        print("\n⚠️  Some tests failed. Fix issues before proceeding to Prefect.")
    
    return  test2_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

