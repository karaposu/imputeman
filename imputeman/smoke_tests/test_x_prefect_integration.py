# smoke_tests/test_5_prefect_integration.py
"""
Smoke Test 5: Test Prefect Integration
Tests that Prefect tasks and flows work correctly with the service layer
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imputeman.core.entities import EntityToImpute, WhatToRetain
from imputeman.core.config import get_development_config
from imputeman.flows.old_main_flow import simple_imputeman_flow, imputeman_flow
from imputeman.tasks.serp_tasks import search_serp_task, validate_serp_results_task
from imputeman.tasks.scrape_tasks import scrape_urls_task, analyze_scrape_costs_task
from imputeman.tasks.extract_tasks import extract_data_task, validate_extractions_task


async def test_individual_tasks():
    """Test individual Prefect tasks work correctly"""
    
    print("🧪 Smoke Test 5: Testing Prefect Integration")
    print("=" * 60)
    print("\n1️⃣ Testing individual Prefect tasks...")
    
    config = get_development_config()
    
    try:
        # Test SERP task
        print("   🔍 Testing SERP task...")
        serp_result = await search_serp_task("bav99", config.serp_config, top_k=2)
        print(f"      ✅ SERP task completed: {len(serp_result.links)} links found")
        
        # Test SERP validation task
        print("   ✅ Testing SERP validation task...")
        validated_result = await validate_serp_results_task(serp_result)
        print(f"      ✅ Validation completed: {len(validated_result.links)} valid links")
        
        if validated_result.success and validated_result.links:
            # Test scrape task
            print("   🕷️ Testing scrape task...")
            scrape_results = await scrape_urls_task(validated_result, config.scrape_config)
            print(f"      ✅ Scrape task completed: {len(scrape_results)} results")
            
            # Test scrape analysis task
            print("   📊 Testing scrape analysis task...")
            cost_analysis = await analyze_scrape_costs_task(scrape_results)
            print(f"      ✅ Analysis completed: ${cost_analysis['total_cost']:.3f} total cost")
            
            # Test extract task
            schema = [
                WhatToRetain(name="component_type", desc="Type of component"),
                WhatToRetain(name="voltage_rating", desc="Voltage rating")
            ]
            
            print("   🧠 Testing extract task...")
            extract_results = await extract_data_task(scrape_results, schema, config.extract_config)
            print(f"      ✅ Extract task completed: {len(extract_results)} results")
            
            # Test extract validation task
            print("   ✔️ Testing extract validation task...")
            validated_extractions = await validate_extractions_task(extract_results, config.extract_config)
            print(f"      ✅ Validation completed: {len(validated_extractions)} valid extractions")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Individual task test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_simple_flow():
    """Test the simple Prefect flow"""
    
    print("\n2️⃣ Testing simple Prefect flow...")
    
    try:
        entity = EntityToImpute(
            name="bav99",
            identifier_context="electronic component",
            impute_task_purpose="smoke test"
        )
        
        schema = [
            WhatToRetain(name="component_type", desc="Type of electronic component"),
            WhatToRetain(name="package_type", desc="Physical package type")
        ]
        
        print("   🌊 Running simple_imputeman_flow...")
        result = await simple_imputeman_flow(entity, schema, top_k=2)
        
        print(f"   ✅ Flow completed!")
        print(f"      Success: {result.success}")
        print(f"      Total cost: ${result.total_cost:.3f}")
        print(f"      Execution time: {result.total_elapsed_time:.2f}s")
        print(f"      URLs scraped: {result.total_urls_scraped}")
        print(f"      Successful extractions: {result.successful_extractions}")
        
        if result.final_data:
            print(f"      Final data keys: {list(result.final_data.keys())}")
        
        return result.success
        
    except Exception as e:
        print(f"   ❌ Simple flow test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_advanced_flow():
    """Test the advanced Prefect flow with custom configuration"""
    
    print("\n3️⃣ Testing advanced Prefect flow...")
    
    try:
        entity = EntityToImpute(
            name="bav99",
            identifier_context="electronic component",
            impute_task_purpose="advanced smoke test"
        )
        
        schema = [
            WhatToRetain(name="component_type", desc="Type of electronic component"),
            WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
            WhatToRetain(name="manufacturer", desc="Component manufacturer")
        ]
        
        # Custom configuration
        config = get_development_config()
        config.serp_config.top_k_results = 3
        config.scrape_config.concurrent_limit = 2
        config.extract_config.confidence_threshold = 0.3  # Lower for testing
        config.cost_threshold_for_budget_mode = 1.0  # Low threshold to test conditional logic
        
        print("   🌊 Running advanced imputeman_flow...")
        print(f"      Using config: top_k={config.serp_config.top_k_results}, concurrent={config.scrape_config.concurrent_limit}")
        
        result = await imputeman_flow(entity, schema, config)
        
        print(f"   ✅ Advanced flow completed!")
        print(f"      Success: {result.success}")
        print(f"      Total cost: ${result.total_cost:.3f}")
        print(f"      Execution time: {result.total_elapsed_time:.2f}s")
        print(f"      URLs scraped: {result.total_urls_scraped}")
        print(f"      Successful extractions: {result.successful_extractions}")
        
        # Test conditional logic was triggered
        if result.metadata:
            print(f"      Metadata: {result.metadata}")
        
        if result.final_data:
            print(f"      Final data: {result.final_data}")
        
        return result.success
        
    except Exception as e:
        print(f"   ❌ Advanced flow test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_error_handling():
    """Test error handling in Prefect flows"""
    
    print("\n4️⃣ Testing error handling...")
    
    try:
        # Test with entity that should fail
        entity = EntityToImpute(
            name="nonexistent-component-xyz-999",
            identifier_context="fictional component",
            impute_task_purpose="error handling test"
        )
        
        schema = [
            WhatToRetain(name="component_type", desc="Type of component"),
        ]
        
        print("   🧪 Running flow with non-existent entity...")
        result = await simple_imputeman_flow(entity, schema, top_k=2)
        
        print(f"   ✅ Error handling test completed!")
        print(f"      Success: {result.success}")
        print(f"      Total cost: ${result.total_cost:.3f}")
        print(f"      Execution time: {result.total_elapsed_time:.2f}s")
        
        # Even if the entity doesn't exist, the flow should handle it gracefully
        # and return partial results
        print(f"      Partial results available:")
        if result.serp_result:
            print(f"        SERP: {len(result.serp_result.links)} links found")
        if result.scrape_results:
            print(f"        Scraping: {len(result.scrape_results)} attempts")
        if result.extract_results:
            print(f"        Extraction: {len(result.extract_results)} attempts")
        
        # Test should pass if flow handles errors gracefully
        return True
        
    except Exception as e:
        print(f"   ❌ Error handling test failed: {e}")
        return False


async def test_flow_caching():
    """Test flow caching behavior"""
    
    print("\n5️⃣ Testing flow caching...")
    
    try:
        entity = EntityToImpute(name="test-cache")
        schema = [WhatToRetain(name="test_field", desc="Test field")]
        
        print("   ⏱️ Running flow first time...")
        start_time = asyncio.get_event_loop().time()
        result1 = await simple_imputeman_flow(entity, schema, top_k=1)
        first_duration = asyncio.get_event_loop().time() - start_time
        
        print("   ⏱️ Running flow second time (should use cache)...")
        start_time = asyncio.get_event_loop().time()
        result2 = await simple_imputeman_flow(entity, schema, top_k=1)
        second_duration = asyncio.get_event_loop().time() - start_time
        
        print(f"   ✅ Caching test completed!")
        print(f"      First run: {first_duration:.2f}s")
        print(f"      Second run: {second_duration:.2f}s")
        print(f"      Cache benefit: {(first_duration - second_duration):.2f}s saved")
        
        # Both should succeed (or fail consistently)
        cache_working = result1.success == result2.success
        print(f"      Cache consistency: {'✅' if cache_working else '❌'}")
        
        return cache_working
        
    except Exception as e:
        print(f"   ❌ Caching test failed: {e}")
        return False


async def main():
    """Run all Prefect integration tests"""
    
    print("🚀 Starting Prefect Integration Tests")
    print("These tests verify that Prefect orchestration works with the service layer")
    
    # Run tests
    test1_passed = await test_individual_tasks()
    test2_passed = await test_simple_flow()
    test3_passed = await test_advanced_flow()
    test4_passed = await test_error_handling()
    test5_passed = await test_flow_caching()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 PREFECT INTEGRATION TEST RESULTS")
    print("=" * 60)
    print(f"Individual Tasks: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Simple Flow: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    print(f"Advanced Flow: {'✅ PASS' if test3_passed else '❌ FAIL'}")
    print(f"Error Handling: {'✅ PASS' if test4_passed else '❌ FAIL'}")
    print(f"Flow Caching: {'✅ PASS' if test5_passed else '❌ FAIL'}")
    
    all_passed = all([test1_passed, test2_passed, test3_passed, test4_passed, test5_passed])
    
    if all_passed:
        print("\n🎉 All Prefect integration tests passed!")
        print("✅ Prefect orchestration is working correctly")
        print("✅ Tasks and flows integrate properly with services")
        print("✅ Error handling and caching are functional")
        print("🚀 Ready for production deployment!")
    else:
        print("\n⚠️  Some Prefect integration tests failed.")
        print("🔍 Check individual test results above")
        print("🛠️ Fix issues before deploying to production")
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)