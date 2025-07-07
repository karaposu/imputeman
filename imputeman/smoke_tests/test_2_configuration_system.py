# smoke_tests/test_2_configuration_system.py
"""
Smoke Test 2: Test Configuration System
Verifies that all configuration classes work correctly
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imputeman.core.config import (
    PipelineConfig, 
    SerpConfig, 
    ScrapeConfig, 
    ExtractConfig,
    BudgetScrapeConfig,
    get_default_config,
    get_development_config,
    get_production_config
)


def test_config_creation():
    """Test that all config classes can be created successfully"""
    
    print("🧪 Smoke Test 2: Testing Configuration System")
    print("=" * 60)
    
    try:
        print("\n1️⃣ Testing individual config classes...")
        
        # Test SerpConfig
        serp_config = SerpConfig()
        print(f"   ✅ SerpConfig: retries={serp_config.max_retries}, top_k={serp_config.top_k_results}")
        
        # Test ScrapeConfig
        scrape_config = ScrapeConfig()
        print(f"   ✅ ScrapeConfig: concurrent={scrape_config.concurrent_limit}, cost_threshold=${scrape_config.max_cost_threshold}")
        
        # Test BudgetScrapeConfig
        budget_config = BudgetScrapeConfig()
        print(f"   ✅ BudgetScrapeConfig: concurrent={budget_config.concurrent_limit}, cost_threshold=${budget_config.max_cost_threshold}")
        
        # Test ExtractConfig
        extract_config = ExtractConfig()
        print(f"   ✅ ExtractConfig: model={extract_config.extraction_model}, confidence={extract_config.confidence_threshold}")
        
        # Test PipelineConfig
        pipeline_config = PipelineConfig()
        print(f"   ✅ PipelineConfig: caching={pipeline_config.enable_caching}, max_cost=${pipeline_config.max_total_cost}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Config creation failed: {e}")
        return False


def test_preset_configs():
    """Test preset configuration functions"""
    
    print("\n2️⃣ Testing preset configuration functions...")
    
    try:
        # Test default config
        default_config = get_default_config()
        print(f"   ✅ Default config: SERP top_k={default_config.serp_config.top_k_results}")
        
        # Test development config
        dev_config = get_development_config()
        print(f"   ✅ Development config: SERP top_k={dev_config.serp_config.top_k_results}")
        print(f"      Reduced limits for dev: concurrent={dev_config.scrape_config.concurrent_limit}")
        
        # Test production config
        prod_config = get_production_config()
        print(f"   ✅ Production config: SERP top_k={prod_config.serp_config.top_k_results}")
        print(f"      Higher limits for prod: concurrent={prod_config.scrape_config.concurrent_limit}")
        
        # Verify dev config has lower limits than prod
        assert dev_config.serp_config.top_k_results <= prod_config.serp_config.top_k_results
        assert dev_config.scrape_config.concurrent_limit <= prod_config.scrape_config.concurrent_limit
        print("   ✅ Dev config correctly has lower limits than prod config")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Preset config test failed: {e}")
        return False


def test_config_customization():
    """Test that configs can be customized properly"""
    
    print("\n3️⃣ Testing configuration customization...")
    
    try:
        # Create and customize config
        config = get_default_config()
        
        # Modify settings
        config.serp_config.top_k_results = 15
        config.scrape_config.max_cost_threshold = 25.0
        config.extract_config.confidence_threshold = 0.9
        
        # Verify changes took effect
        assert config.serp_config.top_k_results == 15
        assert config.scrape_config.max_cost_threshold == 25.0
        assert config.extract_config.confidence_threshold == 0.9
        
        print("   ✅ Configuration customization works correctly")
        print(f"      Modified SERP top_k: {config.serp_config.top_k_results}")
        print(f"      Modified scrape cost threshold: ${config.scrape_config.max_cost_threshold}")
        print(f"      Modified extract confidence: {config.extract_config.confidence_threshold}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Config customization failed: {e}")
        return False


def test_environment_variables():
    """Test environment variable loading"""
    
    print("\n4️⃣ Testing environment variable integration...")
    
    try:
        # Test with mock environment variables
        import os
        
        # Set some test environment variables
        os.environ["SERP_API_KEY"] = "test_serp_key"
        os.environ["BRIGHT_DATA_TOKEN"] = "test_bright_token"
        os.environ["OPENAI_API_KEY"] = "test_openai_key"
        
        # Create configs and check if they pick up env vars
        serp_config = SerpConfig()
        scrape_config = ScrapeConfig()
        extract_config = ExtractConfig()
        
        print(f"   ✅ SERP API key loaded: {'test_serp_key' in (serp_config.api_key or '')}")
        print(f"   ✅ Bright Data token loaded: {'test_bright_token' in (scrape_config.bearer_token or '')}")
        print(f"   ✅ OpenAI API key loaded: {'test_openai_key' in (extract_config.api_key or '')}")
        
        # Clean up
        del os.environ["SERP_API_KEY"]
        del os.environ["BRIGHT_DATA_TOKEN"] 
        del os.environ["OPENAI_API_KEY"]
        
        return True
        
    except Exception as e:
        print(f"   ❌ Environment variable test failed: {e}")
        return False


def test_config_validation():
    """Test configuration validation and edge cases"""
    
    print("\n5️⃣ Testing configuration validation...")
    
    try:
        # Test creating config with extreme values
        config = PipelineConfig()
        
        # Test boundary values
        config.serp_config.max_retries = 0  # Should work (no retries)
        config.scrape_config.timeout_seconds = 1.0  # Very short timeout
        config.extract_config.confidence_threshold = 0.0  # Accept everything
        
        print("   ✅ Boundary value configurations accepted")
        
        # Test that configs maintain their types
        assert isinstance(config.serp_config.max_retries, int)
        assert isinstance(config.scrape_config.timeout_seconds, float)
        assert isinstance(config.extract_config.confidence_threshold, float)
        
        print("   ✅ Configuration types maintained correctly")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Config validation failed: {e}")
        return False


def main():
    """Run all configuration tests"""
    
    print("🚀 Starting Configuration System Tests")
    
    # Run tests
    test1_passed = test_config_creation()
    test2_passed = test_preset_configs()
    test3_passed = test_config_customization()
    test4_passed = test_environment_variables()
    test5_passed = test_config_validation()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 CONFIGURATION TEST RESULTS")
    print("=" * 60)
    print(f"Config Creation: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Preset Configs: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    print(f"Customization: {'✅ PASS' if test3_passed else '❌ FAIL'}")
    print(f"Environment Variables: {'✅ PASS' if test4_passed else '❌ FAIL'}")
    print(f"Validation: {'✅ PASS' if test5_passed else '❌ FAIL'}")
    
    all_passed = all([test1_passed, test2_passed, test3_passed, test4_passed, test5_passed])
    
    if all_passed:
        print("\n🎉 All configuration tests passed!")
    else:
        print("\n⚠️  Some configuration tests failed.")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)