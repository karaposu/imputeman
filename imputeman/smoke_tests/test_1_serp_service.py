# smoke_tests/test_1_serp_service.py
"""
Smoke Test 1: Test SERP service layer in isolation
Tests the SerpService wrapper to identify service-specific issues

python -m imputeman.smoke_tests.test_1_serp_service
"""

import asyncio
import sys
import os

# Import the service and config
from ..core.config import get_development_config
from ..services import ServiceRegistry


async def test_serp_service_creation():
    """Test if SerpService can be created without errors"""
    
    print("\n🔧 Testing SerpService creation")
    print("=" * 50)
    
    try:
        config = get_development_config()
        print("✅ Config loaded successfully")
        print(f"📋 SERP config: {config.serp_config}")  # Fixed: serp -> serp_config
        
        # Create service registry
        registry = ServiceRegistry(config)
        print("✅ ServiceRegistry created")
        
        # Check if SERP service exists
        serp_service = registry.serp
        print(f"✅ SerpService created: {type(serp_service)}")
        
        # Check attributes
        print(f"📋 Engine available: {hasattr(serp_service, 'engine')}")
        if hasattr(serp_service, 'engine'):
            print(f"📋 Engine type: {type(serp_service.engine)}")
            print(f"📋 Engine methods: {[m for m in dir(serp_service.engine) if not m.startswith('_')]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Service creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_serp_service_mock_search():
    """Test the mock search functionality"""
    
    print("\n🔍 Testing mock search functionality")
    print("=" * 50)
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        serp_service = registry.serp
        
        # Force mock search by accessing private method
        if hasattr(serp_service, '_mock_search'):
            print("🎭 Testing _mock_search method...")
            mock_links = await serp_service._mock_search("bav99", 3)
            print(f"✅ Mock search returned {len(mock_links)} links")
            print(f"🔗 Links: {mock_links[:2]}...")
            return True
        else:
            print("❌ _mock_search method not found")
            return False
            
    except Exception as e:
        print(f"❌ Mock search failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_serp_service_search_method():
    """Test the main search method that's failing"""
    
    print("\n🔍 Testing SerpService.search() method")
    print("=" * 50)
    
    try:
        config = get_development_config()
        registry = ServiceRegistry(config)
        serp_service = registry.serp
        
        print("🎯 Calling serp_service.search()...")
        result = await serp_service.search("bav99", top_k=3)
        
        print(f"✅ Search method completed!")
        print(f"📊 Result type: {type(result)}")
        print(f"📊 Success: {result.success}")
        print(f"📊 Links: {len(result.links)}")
        print(f"📊 Metadata: {result.metadata}")
        
        if result.success:
            print(f"🔗 URLs: {result.links[:2]}...")
            return True
        else:
            print(f"⚠️  Search unsuccessful: {result.metadata}")
            return False
            
    except Exception as e:
        print(f"❌ Search method failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        if 'registry' in locals():
            await registry.close_all()


async def test_serpengine_method_issue():
    """Test the specific method that's causing the error"""
    
    print("\n🔍 Testing SERPEngine method call issue")
    print("=" * 50)
    
    try:
        from serpengine.serpengine import SERPEngine
        
        engine = SERPEngine()
        print("✅ SERPEngine created")
        
        # Check if collect_async exists
        if hasattr(engine, 'collect_async'):
            print("✅ collect_async method found")
        else:
            print("❌ collect_async method NOT found")
            print(f"📋 Available methods: {[m for m in dir(engine) if 'collect' in m.lower()]}")
        
        # Check if collect exists (sync version)
        if hasattr(engine, 'collect'):
            print("✅ collect method found")
        else:
            print("❌ collect method NOT found")
        
        # Check if _format attribute exists
        if hasattr(engine, '_format'):
            print("✅ _format attribute found")
        else:
            print("❌ _format attribute NOT found")
            print(f"📋 Available attributes with 'format': {[a for a in dir(engine) if 'format' in a.lower()]}")
        
        return True
        
    except Exception as e:
        print(f"❌ SERPEngine method check failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all SERP service tests"""
    
    print("🚀 Testing SERP Service Layer")
    print("Isolating service-specific issues")
    
    # Run tests
    creation_ok = await test_serp_service_creation()
    mock_ok = await test_serp_service_mock_search()
    method_issue_ok = await test_serpengine_method_issue()
    search_ok = await test_serp_service_search_method()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 SERP SERVICE TEST RESULTS")
    print("=" * 50)
    print(f"Service Creation: {'✅ OK' if creation_ok else '❌ FAILED'}")
    print(f"Mock Search:     {'✅ OK' if mock_ok else '❌ FAILED'}")
    print(f"Method Check:    {'✅ OK' if method_issue_ok else '❌ FAILED'}")
    print(f"Search Method:   {'✅ OK' if search_ok else '❌ FAILED'}")
    
    success = creation_ok and method_issue_ok
    
    if success and not search_ok:
        print("\n🔧 Service layer has bugs - check method calls!")
    elif success and search_ok:
        print("\n🎉 SERP service works! Ready for integration tests.")
    else:
        print("\n⚠️  Fix these issues before proceeding.")
    
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)