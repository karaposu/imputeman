# smoke_tests/test_4_extracthero_without_service.py
"""
Smoke Test 4: Test ExtractHero core functionality without service layer
Tests ExtractHero and FilterHero direct integration with LLM services

python -m imputeman.smoke_tests.test_4_extracthero_without_service
"""

import asyncio
import sys
import os
import json
from time import time

# Import the core ExtractHero components
from extracthero import ExtractHero, FilterHero
from extracthero.schemes import (
    ExtractConfig,
    WhatToRetain,
    ExtractOp,
    FilterOp,
    ParseOp
)
from extracthero.myllmservice import MyLLMService
from extracthero.utils import load_html


async def test_extracthero_creation():
    """Test if ExtractHero can be created without errors"""
    
    print("\n🔧 Testing ExtractHero creation")
    print("=" * 50)
    
    try:
        # Test default creation
        extractor = ExtractHero()
        print("✅ ExtractHero created with defaults")
        print(f"📋 Config type: {type(extractor.config)}")
        print(f"📋 LLM service type: {type(extractor.llm)}")
        print(f"📋 FilterHero type: {type(extractor.filter_hero)}")
        print(f"📋 ParseHero type: {type(extractor.parse_hero)}")
        
        # Test custom config creation
        config = ExtractConfig()
        custom_llm = MyLLMService()
        extractor_custom = ExtractHero(config=config, llm=custom_llm)
        print("✅ ExtractHero created with custom config and LLM")
        
        # Check core components
        components = ['filter_hero', 'parse_hero']
        for component in components:
            if hasattr(extractor, component):
                print(f"📋 Component available: {component}")
            else:
                print(f"❌ Component missing: {component}")
                return False
        
        # Check method availability  
        methods = ['extract', 'extract_async']
        for method in methods:
            if hasattr(extractor, method):
                print(f"📋 Method available: {method}")
            else:
                print(f"❌ Method missing: {method}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ ExtractHero creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    

async def test_basic_html_extraction():
    """Test basic HTML extraction functionality"""
    
    print("\n🌐 Testing basic HTML extraction")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        # Simple HTML sample
        sample_html = """
        <html><body>
          <div class="product">
            <h2 class="title">Wireless Keyboard</h2>
            <span class="price">€49.99</span>
            <p class="description">Compact wireless keyboard with RGB lighting</p>
          </div>
          <div class="product">
            <h2 class="title">USB-C Hub</h2>
            <span class="price">€29.50</span>
            <p class="description">7-in-1 USB-C hub with HDMI output</p>
          </div>
        </body></html>
        """
        
        # Define extraction spec
        extraction_spec = [
            WhatToRetain(
                name="title",
                desc="Product title",
                example="Wireless Keyboard"
            ),
            WhatToRetain(
                name="price",
                desc="Product price with currency symbol",
                example="€49.99"
            )
        ]
        
        print(f"🔍 Testing extraction from sample HTML...")
        print(f"📋 Extraction spec: {len(extraction_spec)} items")
        
        # Perform extraction
        result: ExtractOp = extractor.extract(
            sample_html, 
            extraction_spec, 
            text_type="html"
        )
        
        print(f"✅ Extraction completed")
        print(f"📊 Filter phase success: {result.filter_op.success}")
        print(f"📊 Parse phase success: {result.parse_op.success}")
        
        if result.filter_op.success:
            print(f"📊 Filtered content length: {len(str(result.filter_op.content)) if result.filter_op.content else 0} chars")
            if result.filter_op.reduced_html:
                print(f"📊 HTML reduction applied: {len(result.filter_op.reduced_html)} chars")
        
        # CRITICAL: Check that actual content was extracted, not just LLM response
        content_extracted = False
        if result.parse_op.success and result.content:
            print(f"📊 Parsed content type: {type(result.content)}")
            print(f"📊 Parsed content preview: {str(result.content)[:200]}...")
            
            # Validate that content is meaningful (not empty/None)
            if isinstance(result.content, (dict, list)) and result.content:
                content_extracted = True
                print(f"📊 Content validation: ✅ Valid structured data")
            elif isinstance(result.content, str) and result.content.strip():
                content_extracted = True
                print(f"📊 Content validation: ✅ Valid text content")
            else:
                print(f"📊 Content validation: ❌ Empty or invalid content")
        else:
            print(f"📊 Content validation: ❌ No content extracted")
            if result.parse_op.error:
                print(f"📊 Parse error: {result.parse_op.error}")
            else:
                print(f"📊 Parse error: No error message available")
            
            # Show additional debugging info
            print(f"📊 Parse op details:")
            print(f"   - Success: {result.parse_op.success}")
            print(f"   - Content: {result.parse_op.content}")
            print(f"   - Error: {result.parse_op.error}")
            print(f"   - Usage: {result.parse_op.usage}")
            
            # Check if there's a generation result with raw LLM output
            if hasattr(result.parse_op, 'generation_result') and result.parse_op.generation_result:
                gen_result = result.parse_op.generation_result
                print(f"📊 LLM generation details:")
                print(f"   - LLM Success: {gen_result.success}")
                print(f"   - LLM Content type: {type(gen_result.content)}")
                if isinstance(gen_result.content, str):
                    print(f"   - LLM Raw output preview: {gen_result.content[:200]}...")
                else:
                    print(f"   - LLM Content: {gen_result.content}")
            
            # Show filter content that was sent to parser
            if result.filter_op.success and result.filter_op.content:
                print(f"📊 Filtered content sent to parser:")
                # filter_content_preview = str(result.filter_op.content)[:300]
                filter_content_preview = str(result.filter_op.content)
                print(f"   - Preview: {filter_content_preview}...")
                print(f"   - Type: {type(result.filter_op.content)}")
                print(f"   - Length: {len(str(result.filter_op.content))} chars")
        
        # Success requires BOTH phases to work AND actual content to be extracted
        overall_success = result.filter_op.success and result.parse_op.success and content_extracted
        print(f"📊 Overall success: {overall_success}")
        
        return overall_success
        
    except Exception as e:
        print(f"❌ Basic HTML extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_async_extraction():
    """Test async extraction functionality"""
    
    print("\n⚡ Testing async extraction")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        sample_html = """
        <html><body>
          <div class="hero-product">
            <h1>Premium Laptop Stand</h1>
            <span class="price">$89.99</span>
            <div class="features">
              <p>Adjustable height and angle</p>
              <p>Compatible with 13-17 inch laptops</p>
            </div>
          </div>
        </body></html>
        """
        
        extraction_spec = WhatToRetain(
            name="product_info",
            desc="Complete product information including title, price, and features",
            example="Premium Laptop Stand - $89.99 with adjustable features"
        )
        
        print(f"🔄 Testing async extraction...")
        
        # Perform async extraction
        start_time = time()
        result: ExtractOp = await extractor.extract_async(
            sample_html,
            extraction_spec,
            text_type="html"
        )
        end_time = time()
        
        print(f"✅ Async extraction completed in {end_time - start_time:.2f}s")
        print(f"📊 Filter phase success: {result.filter_op.success}")
        print(f"📊 Parse phase success: {result.parse_op.success}")
        
        # Validate actual content extraction
        content_extracted = False
        if result.parse_op.success and result.content:
            print(f"📊 Result content type: {type(result.content)}")
            print(f"📊 Result preview: {str(result.content)[:150]}...")
            
            # Check for meaningful content
            if isinstance(result.content, (dict, list)) and result.content:
                content_extracted = True
                print(f"📊 Content validation: ✅ Valid data extracted")
            elif isinstance(result.content, str) and result.content.strip():
                content_extracted = True
                print(f"📊 Content validation: ✅ Valid text extracted")
            else:
                print(f"📊 Content validation: ❌ Empty content")
        else:
            print(f"📊 Content validation: ❌ No content extracted")
            if result.parse_op.error:
                print(f"📊 Parse error: {result.parse_op.error}")
        
        overall_success = result.filter_op.success and result.parse_op.success and content_extracted
        print(f"📊 Overall async success: {overall_success}")
        
        return overall_success
        
    except Exception as e:
        print(f"❌ Async extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_json_extraction():
    """Test JSON extraction functionality"""
    
    print("\n📄 Testing JSON extraction")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        # JSON sample
        sample_json = {
            "product": {
                "name": "Gaming Mouse",
                "price": 79.99,
                "currency": "USD",
                "specs": {
                    "dpi": 16000,
                    "buttons": 8,
                    "wireless": True
                }
            },
            "availability": "in_stock",
            "reviews": {
                "average": 4.5,
                "count": 1250
            }
        }
        
        extraction_spec = [
            WhatToRetain(
                name="product",
                desc="Product information",
                example={"name": "Gaming Mouse", "price": 79.99}
            ),
            WhatToRetain(
                name="reviews",
                desc="Review information",
                example={"average": 4.5, "count": 1250}
            )
        ]
        
        print(f"🔍 Testing JSON extraction (dict input)...")
        
        # Test with dict input
        result = extractor.extract(
            sample_json,
            extraction_spec,
            text_type="dict"
        )
        
        print(f"✅ JSON dict extraction completed")
        print(f"📊 Filter success: {result.filter_op.success}")
        print(f"📊 Parse success: {result.parse_op.success}")
        
        dict_success = False
        if result.parse_op.success and result.content:
            print(f"📊 Result type: {type(result.content)}")
            print(f"📊 Result keys: {list(result.content.keys()) if isinstance(result.content, dict) else 'Not a dict'}")
            
            if isinstance(result.content, dict) and result.content:
                dict_success = True
                print(f"📊 Dict extraction: ✅ Valid data")
            else:
                print(f"📊 Dict extraction: ❌ Invalid data")
        else:
            print(f"📊 Dict extraction: ❌ No content extracted")
        
        # Test with JSON string input
        print(f"🔍 Testing JSON extraction (string input)...")
        json_string = json.dumps(sample_json)
        
        result_str = extractor.extract(
            json_string,
            extraction_spec,
            text_type="json"
        )
        
        print(f"✅ JSON string extraction completed")
        print(f"📊 String filter success: {result_str.filter_op.success}")
        print(f"📊 String parse success: {result_str.parse_op.success}")
        
        string_success = False
        if result_str.parse_op.success and result_str.content:
            if isinstance(result_str.content, dict) and result_str.content:
                string_success = True
                print(f"📊 String extraction: ✅ Valid data")
            else:
                print(f"📊 String extraction: ❌ Invalid data")
        else:
            print(f"📊 String extraction: ❌ No content extracted")
        
        overall_json_success = (result.filter_op.success and result.parse_op.success and dict_success and
                               result_str.filter_op.success and result_str.parse_op.success and string_success)
        
        return overall_json_success
        
    except Exception as e:
        print(f"❌ JSON extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_filterhero_standalone():
    """Test FilterHero standalone functionality"""
    
    print("\n🔍 Testing FilterHero standalone")
    print("=" * 50)
    
    try:
        config = ExtractConfig()
        filter_hero = FilterHero(config)
        
        sample_text = """
        Product: Professional Camera Lens
        Price: $599.99
        Brand: CanonTech
        Features:
        - 85mm focal length
        - f/1.4 maximum aperture
        - Image stabilization
        - Weather sealed
        
        Related products:
        - Camera Body: $1299.99
        - Lens Filter: $49.99
        """
        
        extraction_spec = WhatToRetain(
            name="main_product",
            desc="Information about the main product only (not related products)",
            example="Professional Camera Lens - $599.99 by CanonTech"
        )
        
        print(f"🔄 Testing FilterHero with text input...")
        
        # Test filtering
        filter_result: FilterOp = filter_hero.run(
            sample_text,
            extraction_spec,
            text_type=None  # plain text
        )
        
        print(f"✅ FilterHero completed")
        print(f"📊 Success: {filter_result.success}")
        
        if filter_result.success and filter_result.content:
            print(f"📊 Filtered content length: {len(str(filter_result.content))} chars")
            print(f"📊 Content preview: {str(filter_result.content)[:200]}...")
        
        if filter_result.usage:
            print(f"📊 LLM usage: {filter_result.usage}")
        
        return filter_result.success
        
    except Exception as e:
        print(f"❌ FilterHero standalone test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_html_reduction():
    """Test HTML reduction functionality"""
    
    print("\n📝 Testing HTML reduction")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        complex_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Product Page</title>
            <script>console.log('analytics');</script>
            <style>.hidden { display: none; }</style>
        </head>
        <body>
            <header>Navigation here</header>
            <main>
                <div class="product-info">
                    <h1>Smart Watch Pro</h1>
                    <p class="price">$299.99</p>
                    <div class="description">
                        Advanced fitness tracking with heart rate monitor
                    </div>
                </div>
                <div class="hidden">Hidden content</div>
            </main>
            <footer>Footer content</footer>
            <script>trackPageView();</script>
        </body>
        </html>
        """
        
        extraction_spec = WhatToRetain(
            name="watch_info",
            desc="Smart watch product information",
            example="Smart Watch Pro - $299.99"
        )
        
        print(f"🔍 Testing with HTML reduction enabled...")
        print(f"📊 Original HTML length: {len(complex_html)} chars")
        
        # Test with HTML reduction
        result_reduced = extractor.extract(
            complex_html,
            extraction_spec,
            text_type="html",
            reduce_html=True
        )
        
        print(f"✅ HTML reduction extraction completed")
        print(f"📊 Filter success: {result_reduced.filter_op.success}")
        print(f"📊 Parse success: {result_reduced.parse_op.success}")
        
        reduced_content_ok = False
        if result_reduced.parse_op.success and result_reduced.content:
            if isinstance(result_reduced.content, (dict, list, str)) and result_reduced.content:
                reduced_content_ok = True
                print(f"📊 Reduced HTML content: ✅ Valid")
            else:
                print(f"📊 Reduced HTML content: ❌ Invalid")
        else:
            print(f"📊 Reduced HTML content: ❌ No content")
        
        if result_reduced.filter_op.reduced_html:
            print(f"📊 Reduced HTML length: {len(result_reduced.filter_op.reduced_html)} chars")
            reduction_pct = (1 - len(result_reduced.filter_op.reduced_html) / len(complex_html)) * 100
            print(f"📊 Reduction percentage: {reduction_pct:.1f}%")
        
        # Test without HTML reduction
        print(f"🔍 Testing with HTML reduction disabled...")
        
        result_full = extractor.extract(
            complex_html,
            extraction_spec,
            text_type="html",
            reduce_html=False
        )
        
        print(f"✅ Full HTML extraction completed")
        print(f"📊 Full filter success: {result_full.filter_op.success}")
        print(f"📊 Full parse success: {result_full.parse_op.success}")
        
        full_content_ok = False
        if result_full.parse_op.success and result_full.content:
            if isinstance(result_full.content, (dict, list, str)) and result_full.content:
                full_content_ok = True
                print(f"📊 Full HTML content: ✅ Valid")
            else:
                print(f"📊 Full HTML content: ❌ Invalid")
        else:
            print(f"📊 Full HTML content: ❌ No content")
        
        overall_success = (result_reduced.filter_op.success and result_reduced.parse_op.success and reduced_content_ok and
                          result_full.filter_op.success and result_full.parse_op.success and full_content_ok)
        
        return overall_success
        
    except Exception as e:
        print(f"❌ HTML reduction test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_enforce_llm_based_filter_for_dict():
    """Test enforce_llm_based_filter configuration with dict input"""
    
    print("\n⚙️ Testing enforce_llm_based_filter for dict")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        # Test data - dict that normally would use JSON fast-path
        json_data = {
            "speaker": "Bluetooth Speaker Pro",
            "price": "$59.99",
            "brand": "AudioTech",
            "features": ["Wireless", "Waterproof", "20hr battery"],
            "unrelated_item": "USB Cable - $12.99"
        }
        
        extraction_spec = WhatToRetain(
            name="speaker",
            desc="Bluetooth speaker information only (not cables or accessories)",
            example="Bluetooth Speaker Pro - $59.99 by AudioTech",
            wrt_to_source_filter_desc="Keep only the main speaker product, exclude accessories"
        )
        
        # Test WITHOUT enforce_llm_based_filter (should use JSON fast-path)
        print(f"🔄 Testing normal dict processing (JSON fast-path)...")
        result_normal = extractor.extract(
            json_data,
            extraction_spec,
            text_type="dict",
            enforce_llm_based_filter=False
        )
        
        print(f"✅ Normal dict processing completed")
        print(f"📊 Normal filter success: {result_normal.filter_op.success}")
        print(f"📊 Normal parse success: {result_normal.parse_op.success}")
        if result_normal.content:
            print(f"📊 Normal result: {result_normal.content}")
        
        # Test WITH enforce_llm_based_filter (should force LLM filtering)
        print(f"🔄 Testing enforced LLM filtering on dict...")
        result_enforced = extractor.extract(
            json_data,
            extraction_spec,
            text_type="dict",
            enforce_llm_based_filter=True
        )
        
        print(f"✅ Enforced LLM filtering completed")
        print(f"📊 Enforced filter success: {result_enforced.filter_op.success}")
        print(f"📊 Enforced parse success: {result_enforced.parse_op.success}")
        if result_enforced.content:
            print(f"📊 Enforced result: {result_enforced.content}")
        
        # Compare results
        print(f"📊 Results comparison:")
        print(f"   - Normal (fast-path) vs Enforced (LLM) filtering")
        print(f"   - Both should work but may produce different filtered content")
        
        normal_ok = (result_normal.filter_op.success and result_normal.parse_op.success and 
                    result_normal.content is not None)
        
        enforced_ok = (result_enforced.filter_op.success and result_enforced.parse_op.success and 
                      result_enforced.content is not None)
        
        print(f"📊 Normal dict processing: {'✅ OK' if normal_ok else '❌ FAILED'}")
        print(f"📊 Enforced LLM filtering: {'✅ OK' if enforced_ok else '❌ FAILED'}")
        
        return normal_ok and enforced_ok
        
    except Exception as e:
        print(f"❌ Enforce LLM filter test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_filter_specs_separately():
    """Test filter_separately configuration with detailed results"""
    
    print("\n🔄 Testing filter_specs_separately configuration")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        sample_html = """
        <div class="products">
            <div class="main-product">
                <h2>Bluetooth Speaker Pro</h2>
                <span class="price">$89.99</span>
                <p class="desc">Premium wireless speaker with deep bass</p>
            </div>
            <div class="accessories">
                <h3>Charging Cable</h3>
                <span class="price">$12.99</span>
                <p class="desc">USB-C charging cable for speaker</p>
            </div>
            <div class="related">
                <h3>Phone Stand</h3>
                <span class="price">$24.99</span>
                <p class="desc">Adjustable phone stand</p>
            </div>
        </div>
        """
        
        extraction_specs = [
            WhatToRetain(
                name="main_speaker",
                desc="The primary Bluetooth speaker product information",
                example="Bluetooth Speaker Pro - $89.99 - Premium wireless speaker"
            ),
            WhatToRetain(
                name="speaker_accessories",
                desc="Accessories specifically for the speaker (cables, cases, etc.)",
                example="Charging Cable - $12.99 - USB-C charging cable"
            ),
            WhatToRetain(
                name="related_products",
                desc="Related but separate products (not speaker accessories)",
                example="Phone Stand - $24.99 - Adjustable phone stand"
            )
        ]
        
        # Test filter_separately=False (combined filtering - default)
        print(f"🔄 Testing COMBINED filtering (filter_separately=False)...")
        result_combined = extractor.extract(
            sample_html,
            extraction_specs,
            text_type="html",
            filter_separately=False
        )
        
        print(f"✅ Combined filtering completed")
        print(f"📊 Combined filter success: {result_combined.filter_op.success}")
        print(f"📊 Combined parse success: {result_combined.parse_op.success}")
        print(f"📊 Combined result type: {type(result_combined.content)}")
        if result_combined.content:
            print(f"📊 Combined FULL RESULT:")
            if isinstance(result_combined.content, dict):
                for key, value in result_combined.content.items():
                    print(f"     {key}: {value}")
            else:
                print(f"     {result_combined.content}")
        
        combined_ok = (result_combined.filter_op.success and result_combined.parse_op.success and 
                      result_combined.content is not None)
        
        print(f"\n" + "-" * 40)
        
        # Test filter_separately=True (separate filtering for each spec)
        print(f"🔄 Testing SEPARATE filtering (filter_separately=True)...")
        result_separate = extractor.extract(
            sample_html,
            extraction_specs,
            text_type="html",
            filter_separately=True
        )
        
        print(f"✅ Separate filtering completed")
        print(f"📊 Separate filter success: {result_separate.filter_op.success}")
        print(f"📊 Separate parse success: {result_separate.parse_op.success}")
        print(f"📊 Separate result type: {type(result_separate.content)}")
        if result_separate.content:
            print(f"📊 Separate FULL RESULT:")
            if isinstance(result_separate.content, dict):
                for key, value in result_separate.content.items():
                    print(f"     {key}: {value}")
            else:
                print(f"     {result_separate.content}")
        
        separate_ok = (result_separate.filter_op.success and result_separate.parse_op.success and 
                      result_separate.content is not None)
        
        # Analysis
        print(f"\n📊 CONFIGURATION COMPARISON:")
        print(f"📊 Combined filtering: {'✅ OK' if combined_ok else '❌ FAILED'}")
        print(f"📊 Separate filtering: {'✅ OK' if separate_ok else '❌ FAILED'}")
        
        print(f"\n📊 EXPECTED DIFFERENCES:")
        print(f"   - Combined: Single LLM call processes all specs together")
        print(f"   - Separate: Multiple LLM calls, one per spec (more precise)")
        print(f"   - Separate filtering may yield more targeted results per spec")
        
        return combined_ok and separate_ok
        
    except Exception as e:
        print(f"❌ Filter separately test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_error_handling():
    """Test error handling with problematic inputs"""
    
    print("\n⚠️ Testing error handling")
    print("=" * 50)
    
    try:
        extractor = ExtractHero()
        
        extraction_spec = WhatToRetain(
            name="test",
            desc="Test extraction",
            example="test"
        )
        
        # Test invalid JSON
        print(f"🧪 Testing invalid JSON handling...")
        result_bad_json = extractor.extract(
            '{"invalid": json syntax}',
            extraction_spec,
            text_type="json"
        )
        
        print(f"📊 Invalid JSON result - Success: {result_bad_json.filter_op.success}")
        if not result_bad_json.filter_op.success:
            print(f"📊 Error handled gracefully: {result_bad_json.filter_op.error}")
        
        # Test type mismatch
        print(f"🧪 Testing type mismatch handling...")
        result_type_error = extractor.extract(
            "not a dict",
            extraction_spec,
            text_type="dict"
        )
        
        print(f"📊 Type mismatch result - Success: {result_type_error.filter_op.success}")
        if not result_type_error.filter_op.success:
            print(f"📊 Error handled gracefully: {result_type_error.filter_op.error}")
        
        # Test empty content
        print(f"🧪 Testing empty content handling...")
        result_empty = extractor.extract(
            "",
            extraction_spec,
            text_type="html"
        )
        
        print(f"📊 Empty content result - Success: {result_empty.filter_op.success and result_empty.parse_op.success}")
        
        # Success if errors were handled gracefully (no exceptions thrown)
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_real_html_file():
    """Test with real HTML file if available"""
    
    print("\n📁 Testing with real HTML file")
    print("=" * 50)
    
    try:
        # Try to load a real HTML file if it exists
        try:
            html_content = load_html("extracthero/simple_html_sample_2.html")
            print(f"✅ Loaded real HTML file")
            print(f"📊 HTML file length: {len(html_content)} chars")
        except:
            print("⚠️ Real HTML file not found, using sample HTML")
            html_content = """
            <html><body>
                <div class="main-product">
                    <h1>Digital Camera</h1>
                    <div class="price">$899.99</div>
                    <div class="specs">
                        <p>24MP sensor</p>
                        <p>4K video recording</p>
                        <p>WiFi connectivity</p>
                    </div>
                </div>
            </body></html>
            """
        
        extractor = ExtractHero()
        
        extraction_spec = [
            WhatToRetain(
                name="product_title",
                desc="Main product title",
                example="Digital Camera"
            ),
            WhatToRetain(
                name="price",
                desc="Product price",
                example="$899.99"
            ),
            WhatToRetain(
                name="specifications",
                desc="Product specifications and features",
                example="24MP sensor, 4K video, WiFi"
            )
        ]
        
        print(f"🔍 Extracting from real/sample HTML...")
        
        result = extractor.extract(
            html_content,
            extraction_spec,
            text_type="html"
        )
        
        print(f"✅ Real HTML extraction completed")
        print(f"📊 Filter success: {result.filter_op.success}")
        print(f"📊 Parse success: {result.parse_op.success}")
        
        if result.filter_op.success and result.filter_op.content:
            print(f"📊 Filtered content length: {len(str(result.filter_op.content))} chars")
        
        content_extracted = False
        if result.parse_op.success and result.content:
            print(f"📊 Parsed result type: {type(result.content)}")
            print(f"📊 Result preview: {str(result.content)[:300]}...")
            
            if isinstance(result.content, (dict, list)) and result.content:
                content_extracted = True
                print(f"📊 Content validation: ✅ Valid structured data")
            elif isinstance(result.content, str) and result.content.strip():
                content_extracted = True
                print(f"📊 Content validation: ✅ Valid text content")
            else:
                print(f"📊 Content validation: ❌ Empty content")
        else:
            print(f"📊 Content validation: ❌ No content extracted")
            if result.parse_op.error:
                print(f"📊 Parse error: {result.parse_op.error}")
        
        overall_success = result.filter_op.success and result.parse_op.success and content_extracted
        return overall_success
        
    except Exception as e:
        print(f"❌ Real HTML file test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all ExtractHero tests"""
    
    print("🚀 Testing ExtractHero Core Functionality")
    print("Testing extraction pipeline without service layer integration")
    
    # Run tests and collect results
    test_results = {
        # "creation": await test_extracthero_creation(),
        # "basic_html": await test_basic_html_extraction(),
        # "async_extraction": await test_async_extraction(),
        # "json_extraction": await test_json_extraction(),
        # "filterhero_standalone": await test_filterhero_standalone(),
        # "html_reduction": await test_html_reduction(),
        # "enforce_llm_filter": await test_enforce_llm_based_filter_for_dict(),
        #"filter_separately": await test_filter_specs_separately(),
        # "error_handling": await test_error_handling(),
        "real_html_file": await test_real_html_file(),
    }
    
    # Calculate results
    total_passed = sum(test_results.values())
    total_tests = len(test_results)
    pass_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 EXTRACTHERO CORE FUNCTIONALITY TEST RESULTS")
    print("=" * 50)
    
    # Individual test results
    for test_name, result in test_results.items():
        formatted_name = test_name.replace("_", " ").title()
        status = "✅ OK" if result else "❌ FAILED"
        print(f"{formatted_name:<25} {status}")
    
    # Overall analysis
    print("\n" + "=" * 50)
    print("📊 OVERALL RESULTS")
    print("=" * 50)
    print(f"📊 Tests Passed: {total_passed}/{total_tests} ({pass_rate:.0f}%)")
    print(f"📊 Success Criteria: ALL tests must pass (100%)")
    
    # Determine success - ALL tests must pass
    all_tests_passed = total_passed == total_tests
    
    if all_tests_passed:
        print("\n🎉 ExtractHero is fully functional!")
        print("   ALL tests passed - ready for service layer integration.")
    else:
        failed_tests = [name for name, result in test_results.items() if not result]
        print(f"\n⚠️ ExtractHero has {len(failed_tests)} failing test(s):")
        
        for failed_test in failed_tests:
            formatted_name = failed_test.replace("_", " ").title()
            print(f"   ❌ {formatted_name}")
        
        print(f"\n   Fix ALL failing tests before proceeding.")
        print(f"   Current: {total_passed}/{total_tests} - Required: {total_tests}/{total_tests}")
    
    return all_tests_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)