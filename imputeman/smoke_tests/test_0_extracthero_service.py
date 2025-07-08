# smoke_tests/test_0_extracthero_service.py
"""
Smoke Test 0F: Test ExtractHero service layer integration
Tests ExtractorService wrapper integration with ExtractHero functionality

python -m imputeman.smoke_tests.test_0_extracthero_service
"""

import asyncio
import sys
import os
import json
from time import time

# Import the service and config
from ..core.config import get_development_config
from ..services import ServiceRegistry
from ..services.extractor_service import ExtractorService
from ..core.entities import WhatToRetain

# Import ExtractHero for comparison
try:
    from extracthero import ExtractHero
    EXTRACTHERO_AVAILABLE = True
except ImportError:
    EXTRACTHERO_AVAILABLE = False


async def test_extractor_service_creation():
    """Test if ExtractorService can be created without errors"""
    
    print("\n🔧 Testing ExtractorService creation")
    print("=" * 50)
    
    try:
        config = get_development_config()
        print("✅ Config loaded successfully")
        print(f"📋 Extract config: {config.extract_config}")
        print(f"   - Confidence threshold: {config.extract_config.confidence_threshold}")
        print(f"   - Max retries: {config.extract_config.max_retries}")
        print(f"   - Timeout: {config.extract_config.timeout_seconds}s")
        
        # Create service registry
        registry = ServiceRegistry(config)
        print("✅ ServiceRegistry created")
        
        # Check if extractor service exists
        extractor_service = registry.extractor
        print(f"✅ ExtractorService created: {type(extractor_service)}")
        
        # Check service attributes
        print(f"📋 Config available: {hasattr(extractor_service, 'config')}")
        print(f"📋 ExtractHero available: {hasattr(extractor_service, 'extract_hero')}")
        
        if hasattr(extractor_service, 'extract_hero'):
            print(f"📋 ExtractHero type: {type(extractor_service.extract_hero)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Service creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_basic_html_extraction():
    """Test basic HTML extraction through service layer"""
    
    print("\n🌐 Testing Service + Basic HTML Extraction")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        # Sample HTML content
        sample_html = """
        <html><body>
          <div class="product">
            <h2 class="title">Professional Camera Lens</h2>
            <span class="price">$899.99</span>
            <p class="description">85mm f/1.4 lens with image stabilization</p>
            <div class="specs">
              <p>Focal length: 85mm</p>
              <p>Aperture: f/1.4</p>
              <p>Weather sealed: Yes</p>
            </div>
          </div>
        </body></html>
        """
        
        # Define extraction schema
        extraction_schema = [
            WhatToRetain(
                name="product_title",
                desc="Main product title",
                example="Professional Camera Lens"
            ),
            WhatToRetain(
                name="price",
                desc="Product price with currency",
                example="$899.99"
            ),
            WhatToRetain(
                name="specifications",
                desc="Technical specifications",
                example="85mm f/1.4, weather sealed"
            )
        ]
        
        print(f"🔍 Testing HTML extraction through service...")
        print(f"📋 Schema items: {len(extraction_schema)}")
        
        # Test extraction through service
        result = await extractor_service.extract_from_html(
            html_content=sample_html,
            extraction_schema=extraction_schema
        )
        
        print(f"✅ Service HTML extraction completed")
        print(f"📊 Success: {result.success}")
        print(f"📊 Confidence: {result.confidence_score}")
        
        if result.success and result.extracted_data:
            print(f"📊 Extracted data type: {type(result.extracted_data)}")
            print(f"📊 Data preview: {str(result.extracted_data)[:200]}...")
            
            # Validate extracted data structure
            if isinstance(result.extracted_data, dict) and result.extracted_data:
                print(f"📊 Extraction validation: ✅ Valid structured data")
                print(f"📊 Extracted fields: {list(result.extracted_data.keys())}")
                return True
            else:
                print(f"📊 Extraction validation: ❌ Invalid data structure")
                return False
        else:
            print(f"📊 Extraction validation: ❌ No data extracted")
            if result.error:
                print(f"📊 Error: {result.error}")
            return False
            
    except Exception as e:
        print(f"❌ Service HTML extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_batch_extraction():
    """Test batch extraction through service layer"""
    
    print("\n🔄 Testing Service + Batch Extraction")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        # Multiple HTML samples
        html_samples = {
            "camera_lens": """
            <div class="product">
                <h1>Canon 85mm f/1.4L</h1>
                <span class="price">$1,599.00</span>
                <p>Professional portrait lens</p>
            </div>
            """,
            "memory_card": """
            <div class="product">
                <h1>SanDisk 128GB SD Card</h1>
                <span class="price">$45.99</span>
                <p>High-speed memory card</p>
            </div>
            """,
            "camera_body": """
            <div class="product">
                <h1>Canon EOS R5</h1>
                <span class="price">$3,899.00</span>
                <p>Professional mirrorless camera</p>
            </div>
            """
        }
        
        extraction_schema = [
            WhatToRetain(
                name="product_name",
                desc="Product name or title",
                example="Canon 85mm f/1.4L"
            ),
            WhatToRetain(
                name="price",
                desc="Product price",
                example="$1,599.00"
            )
        ]
        
        print(f"🔄 Testing batch extraction with {len(html_samples)} items...")
        
        # Test batch extraction
        if hasattr(extractor_service, 'extract_batch'):
            results = await extractor_service.extract_batch(
                html_contents=html_samples,
                extraction_schema=extraction_schema
            )
            
            print(f"✅ Batch extraction completed")
            print(f"📊 Results type: {type(results)}")
            print(f"📊 Results count: {len(results)}")
            
            successful_extractions = 0
            total_confidence = 0.0
            
            for item_id, result in results.items():
                if result.success:
                    successful_extractions += 1
                    total_confidence += result.confidence_score
                    print(f"   🔗 {item_id}: ✅ (confidence: {result.confidence_score:.2f})")
                    if result.extracted_data:
                        print(f"      Data: {result.extracted_data}")
                else:
                    print(f"   🔗 {item_id}: ❌ Failed")
                    if result.error_message:
                        print(f"      Error: {result.error_message}")
            
            avg_confidence = total_confidence / successful_extractions if successful_extractions > 0 else 0
            print(f"📊 Success rate: {successful_extractions}/{len(html_samples)} ({successful_extractions/len(html_samples)*100:.0f}%)")
            print(f"📊 Average confidence: {avg_confidence:.2f}")
            
            # Success if all extractions worked
            return successful_extractions == len(html_samples)
        else:
            print("❌ Service doesn't have extract_batch method")
            return False
            
    except Exception as e:
        print(f"❌ Batch extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_json_extraction():
    """Test JSON extraction through service layer"""
    
    print("\n📄 Testing Service + JSON Extraction")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        # JSON sample data
        json_data = {
            "product": {
                "title": "Gaming Laptop",
                "price": 1299.99,
                "currency": "USD",
                "specifications": {
                    "cpu": "Intel i7-12700H",
                    "gpu": "RTX 4060",
                    "ram": "16GB DDR5",
                    "storage": "1TB NVMe SSD"
                }
            },
            "availability": "in_stock",
            "reviews": {
                "average_rating": 4.5,
                "total_reviews": 342
            }
        }
        
        extraction_schema = [
            WhatToRetain(
                name="product_info",
                desc="Main product information including title and price",
                example="Gaming Laptop - $1299.99"
            ),
            WhatToRetain(
                name="specs",
                desc="Technical specifications",
                example="Intel i7, RTX 4060, 16GB RAM"
            )
        ]
        
        print(f"🔍 Testing JSON extraction through service...")
        
        # Test JSON extraction through service
        if hasattr(extractor_service, 'extract_from_json'):
            result = await extractor_service.extract_from_json(
                json_data=json_data,
                extraction_schema=extraction_schema
            )
            
            print(f"✅ Service JSON extraction completed")
            print(f"📊 Success: {result.success}")
            print(f"📊 Confidence: {result.confidence_score}")
            
            if result.success and result.extracted_data:
                print(f"📊 Data type: {type(result.extracted_data)}")
                print(f"📊 Extracted data: {result.extracted_data}")
                
                if isinstance(result.extracted_data, dict) and result.extracted_data:
                    return True
                else:
                    print(f"📊 Invalid data structure")
                    return False
            else:
                print(f"📊 No data extracted")
                return False
        else:
            print("❌ Service doesn't have extract_from_json method")
            return False
            
    except Exception as e:
        print(f"❌ JSON extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_configuration_options():
    """Test service configuration options"""
    
    print("\n⚙️ Testing Service Configuration Options")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        sample_html = """
        <div class="complex-page">
            <h1>Smart Watch Pro</h1>
            <span class="price">$299.99</span>
            <div class="features">Advanced fitness tracking</div>
        </div>
        """
        
        extraction_schema = [
            WhatToRetain(
                name="watch_info",
                desc="Smart watch information",
                example="Smart Watch Pro - $299.99"
            )
        ]
        
        # Test with different configuration options
        print(f"🔄 Testing with HTML reduction enabled...")
        
        if hasattr(extractor_service, 'extract_from_html'):
            result_reduced = await extractor_service.extract_from_html(
                html_content=sample_html,
                extraction_schema=extraction_schema,
                reduce_html=True
            )
            
            print(f"✅ HTML reduction extraction completed")
            print(f"📊 Success: {result_reduced.success}")
            
            # Test with HTML reduction disabled
            print(f"🔄 Testing with HTML reduction disabled...")
            result_full = await extractor_service.extract_from_html(
                html_content=sample_html,
                extraction_schema=extraction_schema,
                reduce_html=False
            )
            
            print(f"✅ Full HTML extraction completed")
            print(f"📊 Success: {result_full.success}")
            
            # Test with custom confidence threshold
            print(f"🔄 Testing with custom confidence threshold...")
            if hasattr(extractor_service, 'set_confidence_threshold'):
                extractor_service.set_confidence_threshold(0.9)
                result_high_confidence = await extractor_service.extract_from_html(
                    html_content=sample_html,
                    extraction_schema=extraction_schema
                )
                print(f"✅ High confidence extraction completed")
                print(f"📊 Success: {result_high_confidence.success}")
                
                # Reset confidence threshold
                extractor_service.set_confidence_threshold(config.extract_config.confidence_threshold)
            
            # Success if at least basic configurations work
            basic_configs_work = result_reduced.success and result_full.success
            return basic_configs_work
        else:
            print("❌ Service doesn't have extract_from_html method")
            return False
            
    except Exception as e:
        print(f"❌ Configuration options test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_error_handling():
    """Test service error handling with problematic inputs"""
    
    print("\n⚠️ Testing Service Error Handling")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        extraction_schema = [
            WhatToRetain(
                name="test_field",
                desc="Test field",
                example="test"
            )
        ]
        
        error_scenarios = []
        
        # Test empty HTML
        print(f"🧪 Testing empty HTML handling...")
        if hasattr(extractor_service, 'extract_from_html'):
            result_empty = await extractor_service.extract_from_html(
                html_content="",
                extraction_schema=extraction_schema
            )
            error_scenarios.append(("empty_html", result_empty))
            print(f"📊 Empty HTML result: {'✅ Handled' if not result_empty.success else '⚠️ Unexpected success'}")
        
        # Test invalid JSON
        print(f"🧪 Testing invalid JSON handling...")
        if hasattr(extractor_service, 'extract_from_json'):
            try:
                result_invalid_json = await extractor_service.extract_from_json(
                    json_data="invalid json string",
                    extraction_schema=extraction_schema
                )
                error_scenarios.append(("invalid_json", result_invalid_json))
                print(f"📊 Invalid JSON result: {'✅ Handled' if not result_invalid_json.success else '⚠️ Unexpected success'}")
            except Exception as e:
                print(f"📊 Invalid JSON: ✅ Exception caught gracefully ({type(e).__name__})")
                error_scenarios.append(("invalid_json", "exception_caught"))
        
        # Test malformed HTML
        print(f"🧪 Testing malformed HTML handling...")
        malformed_html = "<html><body><div>Unclosed tags<span>More unclosed</body>"
        if hasattr(extractor_service, 'extract_from_html'):
            result_malformed = await extractor_service.extract_from_html(
                html_content=malformed_html,
                extraction_schema=extraction_schema
            )
            error_scenarios.append(("malformed_html", result_malformed))
            print(f"📊 Malformed HTML result: {'✅ Handled' if result_malformed is not None else '❌ Failed'}")
        
        # Test empty schema
        print(f"🧪 Testing empty schema handling...")
        if hasattr(extractor_service, 'extract_from_html'):
            try:
                result_empty_schema = await extractor_service.extract_from_html(
                    html_content="<html><body>Test content</body></html>",
                    extraction_schema=[]
                )
                error_scenarios.append(("empty_schema", result_empty_schema))
                print(f"📊 Empty schema result: {'✅ Handled' if result_empty_schema is not None else '❌ Failed'}")
            except Exception as e:
                print(f"📊 Empty schema: ✅ Exception caught gracefully ({type(e).__name__})")
                error_scenarios.append(("empty_schema", "exception_caught"))
        
        # Success if all error scenarios were handled gracefully (no crashes)
        handled_gracefully = len(error_scenarios) >= 3  # At least 3 scenarios tested
        print(f"📊 Error scenarios tested: {len(error_scenarios)}")
        print(f"📊 All handled gracefully: {'✅ Yes' if handled_gracefully else '❌ No'}")
        
        return handled_gracefully
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_performance_metrics():
    """Test service performance and usage metrics"""
    
    print("\n📊 Testing Service Performance Metrics")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        sample_html = """
        <div class="product">
            <h1>Test Product</h1>
            <span class="price">$99.99</span>
            <p>Product description here</p>
        </div>
        """
        
        extraction_schema = [
            WhatToRetain(
                name="product_summary",
                desc="Product summary with name and price",
                example="Test Product - $99.99"
            )
        ]
        
        print(f"🔍 Testing performance metrics collection...")
        
        # Perform extraction and measure time
        start_time = time()
        if hasattr(extractor_service, 'extract_from_html'):
            result = await extractor_service.extract_from_html(
                html_content=sample_html,
                extraction_schema=extraction_schema
            )
            end_time = time()
            
            extraction_time = end_time - start_time
            print(f"✅ Extraction completed in {extraction_time:.2f}s")
            print(f"📊 Success: {result.success}")
            
            # Check for performance metrics
            metrics_available = []
            
            if hasattr(result, 'elapsed_time'):
                metrics_available.append("elapsed_time")
                print(f"📊 Processing time: {result.elapsed_time:.2f}s")
            
            if hasattr(result, 'tokens_used'):
                metrics_available.append("tokens_used")
                print(f"📊 Token usage: {result.tokens_used}")
            
            if hasattr(result, 'confidence_score'):
                metrics_available.append("confidence_score")
                print(f"📊 Confidence score: {result.confidence_score:.2f}")
            
            if hasattr(result, 'cost'):
                metrics_available.append("cost")
                print(f"📊 Cost estimate: {result.cost}")
            
            # Check service-level metrics
            if hasattr(extractor_service, 'get_usage_stats'):
                stats = extractor_service.get_usage_stats()
                print(f"📊 Service usage stats: {stats}")
                metrics_available.append("usage_stats")
            
            print(f"📊 Available metrics: {metrics_available}")
            
            # Success if extraction worked and some metrics are available
            has_basic_metrics = len(metrics_available) >= 1
            return result.success and has_basic_metrics
        else:
            print("❌ Service doesn't have extract_from_html method")
            return False
            
    except Exception as e:
        print(f"❌ Performance metrics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_direct_extracthero_integration():
    """Test service integration with direct ExtractHero functionality"""
    
    print("\n🔗 Testing Direct ExtractHero Integration")
    print("=" * 50)
    
    if not EXTRACTHERO_AVAILABLE:
        print("❌ ExtractHero not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        extractor_service = registry.extractor
        
        # Test direct access to ExtractHero
        if hasattr(extractor_service, 'extract_hero'):
            extract_hero = extractor_service.extract_hero
            print(f"✅ ExtractHero accessible: {type(extract_hero)}")
            
            # Test direct ExtractHero usage through service
            sample_html = """
            <div class="item">
                <h2>Direct Test Item</h2>
                <span>$49.99</span>
            </div>
            """
            
            from extracthero.schemes import WhatToRetain as ExtractHeroWhatToRetain
            
            extraction_spec = [
                ExtractHeroWhatToRetain(
                    name="item_name",
                    desc="Item name",
                    example="Direct Test Item"
                ),
                ExtractHeroWhatToRetain(
                    name="price",
                    desc="Item price",
                    example="$49.99"
                )
            ]
            
            print(f"🔄 Testing direct ExtractHero call...")
            direct_result = extract_hero.extract(
                text=sample_html,
                extraction_spec=extraction_spec,
                text_type="html"
            )
            
            print(f"✅ Direct ExtractHero call completed")
            print(f"📊 Filter success: {direct_result.filter_op.success}")
            print(f"📊 Parse success: {direct_result.parse_op.success}")
            
            direct_success = (direct_result.filter_op.success and 
                            direct_result.parse_op.success and 
                            direct_result.content is not None)
            
            print(f"📊 Direct integration: {'✅ Working' if direct_success else '❌ Failed'}")
            
            return direct_success
        else:
            print("❌ Service doesn't expose extract_hero")
            return False
            
    except Exception as e:
        print(f"❌ Direct ExtractHero integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all extractor service integration tests"""
    
    print("🚀 Testing ExtractHero Service Layer Integration")
    print("Testing service layer integration with ExtractHero functionality")
    
    # Run tests and collect results
    test_results = {
        "service_creation": await test_extractor_service_creation(),
        "basic_html_extraction": await test_service_basic_html_extraction(),
        "batch_extraction": await test_service_batch_extraction(),
        "json_extraction": await test_service_json_extraction(),
        "configuration_options": await test_service_configuration_options(),
        "error_handling": await test_service_error_handling(),
        "performance_metrics": await test_service_performance_metrics(),
        "direct_extracthero_integration": await test_service_direct_extracthero_integration(),
    }
    
    # Calculate results
    total_passed = sum(test_results.values())
    total_tests = len(test_results)
    pass_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 EXTRACTOR SERVICE INTEGRATION TEST RESULTS")
    print("=" * 50)
    
    # Individual test results
    for test_name, result in test_results.items():
        formatted_name = test_name.replace("_", " ").title()
        status = "✅ OK" if result else "❌ FAILED"
        print(f"{formatted_name:<30} {status}")
    
    # Overall analysis
    print("\n" + "=" * 50)
    print("📊 OVERALL RESULTS")
    print("=" * 50)
    print(f"📊 Tests Passed: {total_passed}/{total_tests} ({pass_rate:.0f}%)")
    print(f"📊 Success Criteria: ALL tests must pass (100%)")
    
    # Determine success - ALL tests must pass
    all_tests_passed = total_passed == total_tests
    
    if all_tests_passed:
        print("\n🎉 ExtractHero service is fully functional!")
        print("   ALL tests passed - ready for pipeline integration.")
    else:
        failed_tests = [name for name, result in test_results.items() if not result]
        print(f"\n⚠️ ExtractHero service has {len(failed_tests)} failing test(s):")
        
        for failed_test in failed_tests:
            formatted_name = failed_test.replace("_", " ").title()
            print(f"   ❌ {formatted_name}")
        
        print(f"\n   Fix ALL failing tests before proceeding.")
        print(f"   Current: {total_passed}/{total_tests} - Required: {total_tests}/{total_tests}")
    
    return all_tests_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)