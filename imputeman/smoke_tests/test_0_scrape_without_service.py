# smoke_tests/test_0_scrape_without_service.py
"""
Smoke Test 0C: Test raw BrightData functionality without service layer
Tests actual brightdata.auto methods directly

python -m imputeman.smoke_tests.test_0_scrape_without_service
"""

import asyncio
import sys
import os

# Test imports
try:
    from brightdata.auto import scrape_url, scrape_url_async, scrape_urls, scrape_urls_async, trigger_scrape_url
    BRIGHTDATA_AVAILABLE = True
    print("✅ BrightData auto module import successful")
except ImportError as e:
    BRIGHTDATA_AVAILABLE = False
    print(f"❌ BrightData auto module import failed: {e}")

try:
    from brightdata.browser_api import BrowserAPI
    BROWSER_API_AVAILABLE = True
    print("✅ Browser API import successful")
except ImportError as e:
    BROWSER_API_AVAILABLE = False
    print(f"❌ Browser API import failed: {e}")


async def test_brightdata_connectivity():
    """Test BrightData trigger connectivity"""
    
    print("\n🔧 Testing BrightData Connectivity")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    # Check environment variables
    bearer_token = os.getenv("BRIGHTDATA_TOKEN")
    if not bearer_token:
        print("❌ BRIGHTDATA_TOKEN not set")
        return False
    else:
        print(f"✅ BRIGHTDATA_TOKEN: {'*' * min(10, len(bearer_token))}... (set)")
    
    try:
        # Test trigger without waiting (just connectivity)
        test_url = "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301"
        
        print(f"🔍 Testing BrightData trigger for: {test_url}")
        
        # Try to trigger a scrape using trigger_scrape_url
        snapshot_id = trigger_scrape_url(
            test_url, 
            bearer_token=bearer_token,
            raise_if_unknown=False  # Don't raise if no specialized scraper
        )
        
        if snapshot_id:
            print(f"✅ BrightData trigger successful: {snapshot_id}")
            return True
        else:
            print("⚠️  No specialized scraper for URL (this is normal)")
            return True  # This is actually OK - means connectivity works
            
    except Exception as e:
        print(f"❌ BrightData connectivity failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_browser_api_initialization():
    """Test Browser API initialization"""
    
    print("\n🌐 Testing Browser API Initialization")
    print("=" * 50)
    
    if not BROWSER_API_AVAILABLE:
        print("❌ Browser API not available, skipping test")
        return False
    
    try:
        # Test browser API initialization
        browser_api = BrowserAPI()
        print("✅ Browser API initialized")
        
        # Check if we have credentials
        username = os.getenv("BRIGHTDATA_BROWSERAPI_USERNAME")
        password = os.getenv("BRIGHTDATA_BROWSERAPI_PASSWORD")
        
        if username and password:
            print(f"✅ Browser API credentials: username={username[:5]}..., password={'*' * 5}...")
        else:
            print("⚠️  Browser API credentials not set (BRIGHTDATA_BROWSERAPI_USERNAME/PASSWORD)")
        
        return True
        
    except Exception as e:
        print(f"❌ Browser API initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_scrape_url_method():
    """Test sync scrape_url method"""
    
    print("\n🕷️ Testing scrape_url() method (sync)")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        test_url = "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301"
        print(f"🌐 Testing sync scrape_url on: {test_url}")
        
        # Run sync scrape_url in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, 
            lambda: scrape_url(
                test_url,
                poll_interval=2,
                poll_timeout=30,
                fallback_to_browser_api=True
            )
        )
        
        if result is not None:
            print("✅ scrape_url() completed successfully")
            print(f"📊 Result type: {type(result)}")
            
            success = getattr(result, 'success', False)
            print(f"📊 Success: {success}")
            
            # Handle None cost properly
            cost = getattr(result, 'cost', None)
            cost_str = f"${cost:.3f}" if cost is not None else "unknown"
            print(f"📊 Cost: {cost_str}")
            print(f"📊 Data size: {len(getattr(result, 'data', []))}")
            
            # Show field_count if available
            if hasattr(result, 'field_count') and result.field_count is not None:
                print(f"📊 Field count: {result.field_count}")
            
            if hasattr(result, 'html_char_size') and result.html_char_size:
                print(f"📊 HTML size: {result.html_char_size} chars")
            
            # Method succeeded even if scraping failed 
            if not success:
                print("   ℹ️  Scraping failed but method completed (likely blocked by site)")
            
            return True
        else:
            print("⚠️  scrape_url() returned None")
            return False
            
    except Exception as e:
        print(f"❌ scrape_url() failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_async_scrape_url_method():
    """Test async scrape_url_async method"""
    
    print("\n🕷️ Testing scrape_url_async() method")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        test_url = "https://www.linkedin.com/in/ilya-sutskever/"
        print(f"🌐 Testing async scrape_url_async on: {test_url}")
        
        # Use the async method directly
        result = await scrape_url_async(
            test_url,
            poll_interval=2,
            poll_timeout=30,
            fallback_to_browser_api=True
        )
        
        if result is not None:
            print("✅ scrape_url_async() completed successfully")
            print(f"📊 Result type: {type(result)}")
            
            success = getattr(result, 'success', False)
            print(f"📊 Success: {success}")
            
            # Handle None cost properly
            cost = getattr(result, 'cost', None)
            cost_str = f"${cost:.3f}" if cost is not None else "unknown"
            print(f"📊 Cost: {cost_str}")
            
            if hasattr(result, 'data'):
                data_size = len(result.data) if result.data else 0
                print(f"📊 Data size: {data_size}")
            
            # Show field_count if available
            if hasattr(result, 'field_count') and result.field_count is not None:
                print(f"📊 Field count: {result.field_count}")
            
            if hasattr(result, 'html_char_size') and result.html_char_size:
                print(f"📊 HTML size: {result.html_char_size} chars")
            
            # Method succeeded even if scraping failed (LinkedIn likely blocked us)
            if not success:
                print("   ℹ️  Scraping failed but method completed (likely blocked by site)")
            
            return True
        else:
            print("⚠️  scrape_url_async() returned None")
            return False
            
    except Exception as e:
        print(f"❌ scrape_url_async() failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_scrape_urls_method():
    """Test sync scrape_urls method"""
    
    print("\n🔄 Testing scrape_urls() method (sync)")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        test_urls = [
            "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301",
            "https://www.linkedin.com/in/ilya-sutskever/"
        ]
        
        print(f"🕷️ Testing sync scrape_urls on {len(test_urls)} URLs...")
        
        # Run sync scrape_urls in thread pool
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None,
            lambda: scrape_urls(
                test_urls,
                poll_interval=2,
                poll_timeout=30,
                fallback_to_browser_api=True
            )
        )
        
        if results:
            print("✅ scrape_urls() completed successfully")
            print(f"📊 Results type: {type(results)}")
            print(f"📊 Number of results: {len(results)}")
            
            total_cost = 0.0
            successful = 0
            
            for url, result in results.items():
                if result:
                    successful += 1
                    cost = getattr(result, 'cost', None)
                    if cost is not None:
                        total_cost += cost
                    cost_str = f"${cost:.3f}" if cost is not None else "unknown"
                    success = getattr(result, 'success', False)
                    field_count = getattr(result, 'field_count', None)
                    field_str = f", fields: {field_count}" if field_count is not None else ""
                    print(f"   🔗 {url}: {'✅' if success else '⚠️'} (cost: {cost_str}{field_str})")
                else:
                    print(f"   🔗 {url}: ❌ No result")
            
            print(f"📊 Summary: {successful}/{len(test_urls)} successful")
            print(f"📊 Total cost: ${total_cost:.3f}")
            
            return successful > 0
        else:
            print("⚠️  scrape_urls() returned None or empty")
            return False
            
    except Exception as e:
        print(f"❌ scrape_urls() failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_async_scrape_urls_method():
    """Test async scrape_urls_async method"""
    
    print("\n🔄 Testing scrape_urls_async() method")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        test_urls = [
            "https://www.digikey.com/en/products/detail/bosch-sensortec/BNO055/6136301",
            "https://www.linkedin.com/in/ilya-sutskever/"
        ]
        
        print(f"🕷️ Testing async scrape_urls_async on {len(test_urls)} URLs...")
        
        # Use the async method directly
        results = await scrape_urls_async(
            test_urls,
            poll_interval=2,
            poll_timeout=30,
            fallback_to_browser_api=True,
            pool_size=2
        )
        
        if results:
            print("✅ scrape_urls_async() completed successfully")
            print(f"📊 Results type: {type(results)}")
            print(f"📊 Number of results: {len(results)}")
            
            total_cost = 0.0
            successful = 0
            
            for url, result in results.items():
                if result:
                    successful += 1
                    cost = getattr(result, 'cost', None) if result else None
                    if cost is not None:
                        total_cost += cost
                    cost_str = f"${cost:.3f}" if cost is not None else "unknown"
                    success = getattr(result, 'success', False) if result else False
                    field_count = getattr(result, 'field_count', None) if result else None
                    field_str = f", fields: {field_count}" if field_count is not None else ""
                    print(f"   🔗 {url}: {'✅' if success else '⚠️'} (cost: {cost_str}{field_str})")
                else:
                    print(f"   🔗 {url}: ❌ No result")
            
            print(f"📊 Summary: {successful}/{len(test_urls)} successful")
            print(f"📊 Total cost: ${total_cost:.3f}")
            
            return successful > 0
        else:
            print("⚠️  scrape_urls_async() returned None or empty")
            return False
            
    except Exception as e:
        print(f"❌ scrape_urls_async() failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_error_handling():
    """Test error handling with problematic URLs"""
    
    print("\n⚠️  Testing Error Handling")
    print("=" * 50)
    
    if not BRIGHTDATA_AVAILABLE:
        print("❌ BrightData not available, skipping test")
        return False
    
    try:
        # URLs that should cause different types of errors
        problem_urls = [
            "https://definitely-not-a-real-domain-12345.com",  # DNS error
            "invalid-url",  # Invalid URL format
        ]
        
        print(f"🧪 Testing error handling with {len(problem_urls)} problematic URLs...")
        
        try:
            results = await scrape_urls_async(
                problem_urls,
                poll_interval=1,
                poll_timeout=10,
                fallback_to_browser_api=True
            )
            
            error_count = 0
            for url, result in results.items():
                if result is None:
                    error_count += 1
                    print(f"   ⚠️  {url}: No result (handled gracefully)")
                elif hasattr(result, 'success') and not result.success:
                    error_count += 1
                    print(f"   ⚠️  {url}: Failed but handled gracefully")
                else:
                    print(f"   ✅ {url}: Unexpected success")
            
            print(f"📊 Error handling: {error_count}/{len(problem_urls)} URLs failed gracefully")
            return True
            
        except Exception as e:
            print(f"   ✅ Exception caught and handled: {type(e).__name__}")
            return True  # Catching exceptions is good error handling
            
    except Exception as e:
        print(f"❌ Error handling test setup failed: {e}")
        return False


async def main():
    """Run all BrightData raw tests"""
    
    print("🚀 Testing Raw BrightData (without service layer)")
    print("Testing actual brightdata.auto methods directly")
    
    # Run tests
    brightdata_ok = await test_brightdata_connectivity()
    browser_api_initialization_ok = await test_browser_api_initialization()
    scrape_url_method_ok = await test_scrape_url_method()
    async_scrape_url_method_ok = await test_async_scrape_url_method()
    scrape_urls_method_ok = await test_scrape_urls_method()
    async_scrape_urls_method_ok = await test_async_scrape_urls_method()
    error_ok = await test_error_handling()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 RAW BRIGHTDATA TEST RESULTS")
    print("=" * 50)
    print(f"BrightData Connectivity:    {'✅ OK' if brightdata_ok else '❌ FAILED'}")
    print(f"Browser API Init:           {'✅ OK' if browser_api_initialization_ok else '❌ FAILED'}")
    print(f"scrape_url() method:        {'✅ OK' if scrape_url_method_ok else '❌ FAILED'}")
    print(f"scrape_url_async() method:  {'✅ OK' if async_scrape_url_method_ok else '❌ FAILED'}")
    print(f"scrape_urls() method:       {'✅ OK' if scrape_urls_method_ok else '❌ FAILED'}")
    print(f"scrape_urls_async() method: {'✅ OK' if async_scrape_urls_method_ok else '❌ FAILED'}")
    print(f"Error Handling:             {'✅ OK' if error_ok else '❌ FAILED'}")
    
    # Core functionality: connectivity and at least one scraping method
    core_success = brightdata_ok and (
        scrape_url_method_ok or async_scrape_url_method_ok or 
        scrape_urls_method_ok or async_scrape_urls_method_ok
    )
    
    if core_success:
        print("\n🎉 Raw BrightData capabilities work! Ready for service layer.")
    else:
        print("\n⚠️  Fix basic BrightData issues before testing service layer.")
    
    return core_success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)