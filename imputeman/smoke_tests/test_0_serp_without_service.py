# smoke_tests/test_0_serp_without_service.py
"""
Smoke Test 0A: Test raw SERPEngine functionality without service layer
Tests SERPEngine directly to verify the underlying library works

python -m imputeman.smoke_tests.test_0_serp_without_service
"""

import asyncio
import sys
import os

# Test the raw SERPEngine import and basic functionality
try:
    from serpengine.serpengine import SERPEngine
    SERPENGINE_AVAILABLE = True
    print("✅ SERPEngine import successful")
except ImportError as e:
    SERPENGINE_AVAILABLE = False
    print(f"❌ SERPEngine import failed: {e}")


async def test_serpengine_direct():
    """Test SERPEngine directly without any service wrapper"""
    
    print("\n🧪 Testing SERPEngine directly")
    print("=" * 50)
    
    if not SERPENGINE_AVAILABLE:
        print("❌ SERPEngine not available, skipping test")
        return False
    
    try:
        # Initialize SERPEngine
        print("🔧 Initializing SERPEngine...")
        engine = SERPEngine()
        print("✅ SERPEngine initialized successfully")
        
        # Check what methods are available
        print(f"📋 Available methods: {[method for method in dir(engine) if not method.startswith('_')]}")
        
        # Test basic search - using sync method first
        print("\n🔍 Testing sync search...")
        query = "bav99"
        
        try:
            # Use the sync collect method that we know exists
            result = engine.collect(
                query=query,
                num_urls=3,
                search_sources=["google_search_via_api"],
                output_format="object"  # Get structured objects
            )
            
            print(f"✅ Sync search completed!")
            print(f"📊 Result type: {type(result)}")
            
            # Check if result has expected attributes
            if hasattr(result, 'all_links'):
                links = result.all_links()
                print(f"🔗 Found {len(links)} URLs")
                print(f"🔗 URLs: {links[:2]}...")  # Show first 2
            else:
                print(f"📋 Result attributes: {dir(result)}")
                
            return True
            
        except Exception as e:
            print(f"❌ Sync search failed: {e}")
            print(f"   Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            return False
            
    except Exception as e:
        print(f"❌ SERPEngine initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_env_variables():
    """Test if required environment variables are set"""
    
    print("\n🔧 Testing environment variables")
    print("=" * 50)
    
    required_vars = [
        "GOOGLE_SEARCH_API_KEY",
        "GOOGLE_CSE_ID"
    ]
    
    all_present = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {'*' * min(10, len(value))}... (set)")
        else:
            print(f"❌ {var}: Not set")
            all_present = False
    
    return all_present


async def main():
    """Run all basic SERP tests"""
    
    print("🚀 Testing Raw SERPEngine (without service layer)")
    print("Testing the underlying SERPEngine library directly")
    
    # Test environment
    env_ok = await test_env_variables()
    
    # Test SERPEngine
    serp_ok = await test_serpengine_direct()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 RAW SERP TEST RESULTS")
    print("=" * 50)
    print(f"Environment: {'✅ OK' if env_ok else '❌ MISSING VARS'}")
    print(f"SERPEngine:  {'✅ OK' if serp_ok else '❌ FAILED'}")
    
    success = env_ok and serp_ok
    
    if success:
        print("\n🎉 Raw SERPEngine works! Issue is in service layer.")
    else:
        print("\n⚠️  Fix these issues before testing service layer.")
    
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)