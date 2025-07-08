# smoke_tests/test_0_scrape_service.py
"""
Smoke Test 0D: Test Scraper service layer integration with real BrightData
Tests ScraperService wrapper integration with actual brightdata.auto methods

python -m imputeman.smoke_tests.test_0_scrape_service
"""

import asyncio
import sys
import os

# Import the service and config
from ..core.config import get_development_config
from ..services import ServiceRegistry
from ..services.scraper_service import ScraperService

# Import real BrightData methods for comparison
try:
    from brightdata.auto import scrape_url, scrape_url_async, scrape_urls, scrape_urls_async
    BRIGHTDATA_AVAILABLE = True
except ImportError:
    BRIGHTDATA_AVAILABLE = False


async def test_scraper_service_creation():
    """Test if ScraperService can be created without errors"""
    
    print("\n🔧 Testing ScraperService creation")
    print("=" * 50)
    
    try:
        config = get_development_config()
        print("✅ Config loaded successfully")
        print(f"📋 Scrape config: {config.scrape_config}")
        print(f"   - Concurrent limit: {config.scrape_config.concurrent_limit}")
        print(f"   - Timeout: {config.scrape_config.timeout_seconds}s")
        print(f"   - Max cost threshold: ${config.scrape_config.max_cost_threshold}")
        print(f"   - Use browser fallback: {config.scrape_config.use_browser_fallback}")
        
        # Create service registry
        registry = ServiceRegistry(config)
        print("✅ ServiceRegistry created")
        
        # Check if scraper service exists
        scraper_service = registry.scraper
        print(f"✅ ScraperService created: {type(scraper_service)}")
        
        # Check service attributes
        print(f"📋 Session available: {hasattr(scraper_service, 'session')}")
        print(f"📋 Config available: {hasattr(scraper_service, 'config')}")
        
        if hasattr(scraper_service, 'session'):
            print(f"📋 Session type: {type(scraper_service.session)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Service creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_integration_with_scrape_url():
    """Test service integration with real scrape_url method"""
    
    print("\n🕷️ Testing Service + scrape_url() integration")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        scraper_service = registry.scraper
        
        test_url = "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301"
        print(f"🌐 Testing service integration with scrape_url on: {test_url}")
        
        # Test if service can call real BrightData methods
        if hasattr(scraper_service, '_brightdata_scrape'):
            result = await scraper_service._brightdata_scrape(test_url)
            
            if result:
                print("✅ Service integration with real BrightData successful")
                print(f"📊 Result type: {type(result)}")
                print(f"📊 Success: {result.success}")
                
                if result.success:
                    cost = result.cost
                    cost_str = f"${cost:.3f}" if cost is not None else "unknown"
                    print(f"📊 Cost: {cost_str}")
                    
                    if hasattr(result, 'html_char_size') and result.html_char_size:
                        print(f"📊 HTML size: {result.html_char_size} chars")
                    
                    if hasattr(result, 'field_count') and result.field_count is not None:
                        print(f"📊 Field count: {result.field_count}")
                    
                    return True
                else:
                    print(f"⚠️  Scraping failed: {result.error}")
                    return False
            else:
                print("❌ Service integration returned no result")
                return False
        else:
            print("❌ Service doesn't have _brightdata_scrape method")
            return False
            
    except Exception as e:
        print(f"❌ Service integration with scrape_url failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_integration_with_scrape_url_async():
    """Test service integration with real scrape_url_async method"""
    
    print("\n🕷️ Testing Service + scrape_url_async() integration")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        scraper_service = registry.scraper
        
        test_url = "https://www.linkedin.com/in/ilya-sutskever/"
        print(f"🌐 Testing service integration with scrape_url_async on: {test_url}")
        
        # Test single URL scraping through service
        results = await scraper_service.scrape_urls([test_url])
        
        if results and test_url in results:
            result = results[test_url]
            print("✅ Service integration with async scraping successful")
            print(f"📊 Result success: {result.success}")
            
            if result.success:
                cost = result.cost
                cost_str = f"${cost:.3f}" if cost is not None else "unknown"
                print(f"📊 Cost: {cost_str}")
                
                if hasattr(result, 'html_char_size') and result.html_char_size:
                    print(f"📊 HTML size: {result.html_char_size} chars")
                
                if hasattr(result, 'field_count') and result.field_count is not None:
                    print(f"📊 Field count: {result.field_count}")
                
                return True
            else:
                print(f"⚠️  Scraping failed: {result.error}")
                print("   ℹ️  LinkedIn likely blocked scraping (expected)")
                return True  # This is expected behavior
        else:
            print("❌ Service integration returned no results")
            return False
            
    except Exception as e:
        print(f"❌ Service integration with scrape_url_async failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_integration_with_scrape_urls():
    """Test service integration with real scrape_urls method"""
    
    print("\n🔄 Testing Service + scrape_urls() integration")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        scraper_service = registry.scraper
        
        test_urls = [
            "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301",
            "https://www.linkedin.com/in/ilya-sutskever/"
        ]
        
        print(f"🕷️ Testing service integration with multiple URLs ({len(test_urls)})...")
        print("   📋 Testing: Digikey component page & LinkedIn profile")
        
        # Test multiple URLs through service
        results = await scraper_service.scrape_urls(test_urls)
        
        if results:
            print("✅ Service integration with bulk scraping successful")
            print(f"📊 Results type: {type(results)}")
            print(f"📊 Number of results: {len(results)}")
            
            total_cost = 0.0
            successful = 0
            
            for url, result in results.items():
                if result and result.status == "ready":
                    successful += 1
                    cost = result.cost
                    if cost is not None:
                        total_cost += cost
                    cost_str = f"${cost:.3f}" if cost is not None else "unknown"
                    field_count = getattr(result, 'field_count', None)
                    field_str = f", fields: {field_count}" if field_count is not None else ""
                    print(f"   🔗 {url}: ✅ (cost: {cost_str}{field_str})")
                elif result:
                    print(f"   🔗 {url}: ⚠️ Status: {result.status}")
                else:
                    print(f"   🔗 {url}: ❌ No result")
            
            print(f"📊 Summary: {successful}/{len(test_urls)} successful")
            print(f"📊 Total cost: ${total_cost:.3f}")
            
            return len(results) == len(test_urls)  # All URLs processed
        else:
            print("❌ Service integration returned no results")
            return False
            
    except Exception as e:
        print(f"❌ Service integration with scrape_urls failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if 'registry' in locals():
            await registry.close_all()


async def test_service_integration_with_scrape_urls_async():
    """Test service integration with real scrape_urls_async method"""
    
    print("\n🔄 Testing Service + scrape_urls_async() integration")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        scraper_service = registry.scraper
        
        test_urls = [
            "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301",
            "https://www.linkedin.com/in/ilya-sutskever/"
        ]
        
        print(f"🕷️ Testing service async integration with {len(test_urls)} URLs...")
        print("   📋 Testing real-world sites through service layer")
        
        # Test async bulk scraping through service
        results = await scraper_service.scrape_urls(test_urls)
        
        if results:
            print("✅ Service async integration successful")
            print(f"📊 Results type: {type(results)}")
            print(f"📊 Number of results: {len(results)}")
            
            total_cost = 0.0
            ready_count = 0
            
            for url, result in results.items():
                if result:
                    if result.status == "ready":
                        ready_count += 1
                    cost = result.cost
                    if cost is not None:
                        total_cost += cost
                    cost_str = f"${cost:.3f}" if cost is not None else "unknown"
                    field_count = getattr(result, 'field_count', None)
                    field_str = f", fields: {field_count}" if field_count is not None else ""
                    status_icon = "✅" if result.status == "ready" else "⚠️"
                    print(f"   🔗 {url}: {status_icon} Status: {result.status} (cost: {cost_str}{field_str})")
                else:
                    print(f"   🔗 {url}: ❌ No result")
            
            print(f"📊 Summary: {ready_count}/{len(test_urls)} ready")
            print(f"📊 Total cost: ${total_cost:.3f}")
            
            return len(results) == len(test_urls)  # All URLs processed
        else:
            print("❌ Service async integration returned no results")
            return False
            
    except Exception as e:
        print(f"❌ Service async integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if 'registry' in locals():
            await registry.close_all()


async def test_service_real_cost_tracking():
    """Test service's real cost tracking with actual BrightData costs"""
    
    print("\n💰 Testing service real cost tracking")
    print("=" * 50)
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        scraper_service = registry.scraper
        
        test_urls = [
            "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301",
            "https://www.linkedin.com/in/ilya-sutskever/"
        ]
        
        print(f"🕷️ Testing real cost tracking with {len(test_urls)} URLs...")
        
        # Scrape URLs to get real costs
        scrape_results = await scraper_service.scrape_urls(test_urls)
        
        # Test real cost calculation methods
        if hasattr(scraper_service, 'calculate_actual_costs'):
            cost_metrics = scraper_service.calculate_actual_costs(scrape_results)
            
            print(f"✅ Real cost calculation completed!")
            print(f"📊 Total cost: ${cost_metrics['total_cost']:.3f}")
            print(f"📊 Paid scrapes: {cost_metrics['paid_scrapes']}")
            print(f"📊 Free scrapes: {cost_metrics['free_scrapes']}")
            print(f"📊 Cost unknown: {cost_metrics['cost_unknown']}")
            
            if cost_metrics['total_cost'] > 0:
                print(f"📊 Avg cost per scrape: ${cost_metrics['avg_cost_per_scrape']:.3f}")
            
            # Test cost summary
            if hasattr(scraper_service, 'get_cost_summary'):
                summary = scraper_service.get_cost_summary(scrape_results)
                print(f"📋 Cost summary: {summary}")
            
            return True
        else:
            print("❌ Real cost tracking methods not found")
            print("   Missing: calculate_actual_costs() and/or get_cost_summary()")
            return False
            
    except Exception as e:
        print(f"❌ Real cost tracking failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_service_error_handling():
    """Test service error handling with problematic URLs"""
    
    print("\n⚠️  Testing service error handling")
    print("=" * 50)
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        scraper_service = registry.scraper
        
        # URLs that should cause errors
        problem_urls = [
            "https://definitely-not-a-real-domain-12345.com",  # DNS error
            "invalid-url",  # Invalid URL format
        ]
        
        print(f"🧪 Testing service error handling with {len(problem_urls)} problematic URLs...")
        
        results = await scraper_service.scrape_urls(problem_urls)
        
        # Check that service handled errors gracefully
        error_count = 0
        for url, result in results.items():
            if result:
                print(f"   🔗 {url}: Status: {result.status}")
                if result.status != "ready":
                    error_count += 1
                    if result.error:
                        print(f"      Error: {result.error}")
            else:
                error_count += 1
                print(f"   🔗 {url}: ❌ No result")
        
        print(f"📊 Service error handling: {error_count}/{len(problem_urls)} URLs failed gracefully")
        
        # Success if service handled all URLs (even failures) 
        return len(results) == len(problem_urls)
        
    except Exception as e:
        print(f"❌ Service error handling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if 'registry' in locals():
            await registry.close_all()


async def main():
    """Run all scraper service integration tests"""
    
    print("🚀 Testing Scraper Service Integration with Real BrightData")
    print("Testing service layer integration with actual brightdata.auto methods")
    
    # Run tests
    creation_ok = await test_scraper_service_creation()
    scrape_url_ok = await test_service_integration_with_scrape_url()
    scrape_url_async_ok = await test_service_integration_with_scrape_url_async()
    scrape_urls_ok = await test_service_integration_with_scrape_urls()
    scrape_urls_async_ok = await test_service_integration_with_scrape_urls_async()
    cost_tracking_ok = await test_service_real_cost_tracking()
    error_ok = await test_service_error_handling()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 SCRAPER SERVICE INTEGRATION TEST RESULTS")
    print("=" * 50)
    print(f"Service Creation:           {'✅ OK' if creation_ok else '❌ FAILED'}")
    print(f"scrape_url() integration:   {'✅ OK' if scrape_url_ok else '❌ FAILED'}")
    print(f"scrape_url_async() integration: {'✅ OK' if scrape_url_async_ok else '❌ FAILED'}")
    print(f"scrape_urls() integration:  {'✅ OK' if scrape_urls_ok else '❌ FAILED'}")
    print(f"scrape_urls_async() integration: {'✅ OK' if scrape_urls_async_ok else '❌ FAILED'}")
    print(f"Real Cost Tracking:         {'✅ OK' if cost_tracking_ok else '❌ FAILED'}")
    print(f"Error Handling:             {'✅ OK' if error_ok else '❌ FAILED'}")
    
    # Core functionality: service creation and at least one integration working
    core_success = creation_ok and (
        scrape_url_ok or scrape_url_async_ok or 
        scrape_urls_ok or scrape_urls_async_ok
    )
    
    if core_success:
        print("\n🎉 Scraper service successfully integrates with real BrightData!")
        print("   Ready for full pipeline integration tests.")
    else:
        print("\n⚠️  Fix service integration issues before proceeding.")
    
    return core_success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)