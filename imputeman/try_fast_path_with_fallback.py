# python -m imputeman.try_fast_path_with_fallback

import asyncio
from imputeman import Imputeman
from imputeman.core.config import PipelineConfig, FastPathConfig, FastPathMode
from imputeman.core.entities import EntityToImpute, WhatToRetain


async def run_fast_path_with_fallback_example():
    """Test fast path with fallback to normal pipeline"""
    
    # Configure for fast path with fallback
    config = PipelineConfig()
    config.fast_path_config.enabled = True
    config.fast_path_config.mode = FastPathMode.FAST_PATH_WITH_FALLBACK
    config.fast_path_config.digikey_enabled = True
    config.fast_path_config.mouser_enabled = False  # Can enable if needed
    config.fast_path_config.poll_timeout = 30.0
    config.fast_path_config.poll_interval = 5.0
    config.fast_path_config.min_result_size = 1000  # Adjust based on expected data
    
    # Limit normal path URLs for testing
    config.serp_config.top_k_results = 5
    
    # Test cases - choose one:
    
    # Case 1: Component that exists on DigiKey (fast path should succeed)
    entity = EntityToImpute(name="BAV99")
    
    # Case 2: Component that might not be on DigiKey (fast path fails, fallback runs)
    # entity = EntityToImpute(name="XYZ123DOESNOTEXIST")
    
    # Case 3: Real component with specific context
    # entity = EntityToImpute(
    #     name="STM32F407VGT6",
    #     identifier_context="microcontroller ARM Cortex-M4"
    # )
    
    # Define extraction schema for fallback path
    schema = [
        WhatToRetain(name="component_type", desc="Type of electronic component"),
        WhatToRetain(name="manufacturer", desc="Component manufacturer"),
        WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
        WhatToRetain(name="package_type", desc="Physical package type"),
        WhatToRetain(name="datasheet_url", desc="Link to datasheet PDF")
    ]
    
    print(f"🧪 Testing Fast Path with Fallback Mode")
    print(f"   Entity: {entity.name}")
    print(f"   Schema: {len(schema)} fields")
    print("=" * 60)
    
    # Run pipeline
    imputeman = Imputeman(config)
    impute_op = await imputeman.run(
        entity=entity,
        schema=schema,
        enable_streaming=True
    )
    
    # Analyze results
    print("\n📊 Results Analysis:")
    print("=" * 60)
    
    # Check if fast path was attempted
    if impute_op.performance.fast_path_attempted:
        print("✅ Fast path was attempted")
        print(f"   Duration: {impute_op.performance.fast_path_duration:.2f}s")
        
        # Check fast path results
        if impute_op.fast_path_results:
            fast_path_success = any(
                r.success and r.data for r in impute_op.fast_path_results.values()
            )
            
            if fast_path_success:
                print("✅ Fast path succeeded!")
                for url, result in impute_op.fast_path_results.items():
                    if result.success:
                        print(f"   📦 {url[:50]}...")
                        print(f"      - HTML size: {result.html_char_size:,} chars")
                        print(f"      - Rows: {result.row_count}")
                        print(f"      - Fields: {result.field_count}")
                        print(f"      - Cost: ${result.cost}")
                        
                        # Save fast path data
                        saved_path = result.save_data_to_file(
                            dir_="fast_path_results",
                            filename=f"fast_path_{entity.name}"
                        )
                        print(f"      - Saved to: {saved_path}")
            else:
                print("❌ Fast path failed - falling back to normal path")
                for url, result in impute_op.fast_path_results.items():
                    print(f"   ❌ {url[:50]}...: {result.error or 'No data'}")
        else:
            print("❌ Fast path returned no results - falling back")
    else:
        print("⚠️ Fast path was not attempted")
    
    # Check if normal path ran (fallback)
    if impute_op.search_op and impute_op.search_op.results:
        print("\n📡 Normal path (SERP) was executed:")
        print(f"   - Found {len(impute_op.urls)} URLs")
        print(f"   - Cost: ${impute_op.costs.serp_cost:.4f}")
        
        # Show first few URLs
        for i, url in enumerate(impute_op.urls[:3]):
            print(f"   {i+1}. {url[:60]}...")
        if len(impute_op.urls) > 3:
            print(f"   ... and {len(impute_op.urls) - 3} more")
    
    # Check extraction results
    if impute_op.extract_results:
        print(f"\n🧠 Extraction completed:")
        print(f"   - Extracted from {len(impute_op.extract_results)} URLs")
        print(f"   - Cost: ${impute_op.costs.extraction_cost:.4f}")
        
        # Show extracted data
        for i, (url, extract_op) in enumerate(impute_op.extract_results.items()):
            if i >= 2:  # Show first 2
                break
            print(f"\n   📄 {url[:50]}...")
            if extract_op.success and extract_op.content:
                print(f"      ✅ Extracted successfully:")
                # Pretty print the content
                if isinstance(extract_op.content, dict):
                    for key, value in extract_op.content.items():
                        print(f"         - {key}: {value}")
                else:
                    print(f"         {extract_op.content}")
            else:
                print(f"      ❌ Extraction failed: {extract_op.error}")
    
    # Summary
    print("\n🎯 Final Summary:")
    print("=" * 60)
    print(f"✅ Overall Success: {impute_op.success}")
    print(f"⏱️ Total Duration: {impute_op.performance.total_elapsed_time:.2f}s")
    print(f"💰 Total Cost: ${impute_op.costs.total_cost:.4f}")
    print(f"   - SERP: ${impute_op.costs.serp_cost:.4f}")
    print(f"   - Scrape: ${impute_op.costs.scrape_cost:.4f}")
    print(f"   - Extract: ${impute_op.costs.extraction_cost:.4f}")
    
    # Determine which path was used
    if impute_op.fast_path_results and not impute_op.search_op:
        print("🚀 Result: Fast path only (no fallback needed)")
    elif impute_op.fast_path_results and impute_op.search_op:
        print("🔄 Result: Fast path failed, fallback executed")
    else:
        print("📡 Result: Normal path only")
    
    return impute_op


async def test_multiple_entities():
    """Test multiple entities to see different behaviors"""
    
    test_entities = [
        # Should succeed on fast path
        EntityToImpute(name="1N4148"),  # Common diode
        
        # Might need fallback
        EntityToImpute(name="OBSOLETEPART123"),  # Likely not on DigiKey
        
        # Complex search that benefits from context
        EntityToImpute(
            name="LM358",
            identifier_context="operational amplifier dual"
        ),
    ]
    
    print("🧪 Testing Multiple Entities with Fast Path + Fallback")
    print("=" * 60)
    
    for entity in test_entities:
        print(f"\n📌 Testing: {entity.name}")
        print("-" * 40)
        
        config = PipelineConfig()
        config.fast_path_config.enabled = True
        config.fast_path_config.mode = FastPathMode.FAST_PATH_WITH_FALLBACK
        config.fast_path_config.digikey_enabled = True
        config.serp_config.top_k_results = 3  # Limit for testing
        
        imputeman = Imputeman(config)
        
        try:
            impute_op = await imputeman.run(
                entity=entity,
                schema=[
                    WhatToRetain(name="part_number", desc="Manufacturer part number"),
                    WhatToRetain(name="price", desc="Unit price"),
                ],
                enable_streaming=True
            )
            
            # Quick summary
            fast_path_used = bool(impute_op.fast_path_results and any(
                r.success for r in impute_op.fast_path_results.values()
            ))
            normal_path_used = bool(impute_op.search_op)
            
            print(f"   Fast path: {'✅ Success' if fast_path_used else '❌ Failed'}")
            print(f"   Fallback: {'✅ Executed' if normal_path_used else '➖ Not needed'}")
            print(f"   Total cost: ${impute_op.costs.total_cost:.4f}")
            print(f"   Duration: {impute_op.performance.total_elapsed_time:.1f}s")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")


# Run examples
if __name__ == "__main__":
    # Run single entity test
    print("=" * 60)
    print("1️⃣ Single Entity Test")
    print("=" * 60)
    result = asyncio.run(run_fast_path_with_fallback_example())
    
    # Uncomment to run multiple entity test
    # print("\n\n")
    # print("=" * 60)
    # print("2️⃣ Multiple Entity Test")
    # print("=" * 60)
    # asyncio.run(test_multiple_entities())