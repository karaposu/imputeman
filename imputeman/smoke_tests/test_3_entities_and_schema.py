# smoke_tests/test_3_entities_and_schema.py
"""
Smoke Test 3: Test Entity and Schema System
Verifies that all entity classes and schema definitions work correctly
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imputeman.core.entities import (
    EntityToImpute,
    WhatToRetain,
    SerpResult,
    ScrapeResult,
    ExtractResult,
    ImputeResult,
    PipelineStageResult
)


def test_entity_creation():
    """Test that all entity classes can be created successfully"""
    
    print("🧪 Smoke Test 3: Testing Entity and Schema System")
    print("=" * 60)
    
    try:
        print("\n1️⃣ Testing entity creation...")
        
        # Test EntityToImpute
        entity = EntityToImpute(
            name="bav99",
            identifier_context="electronic component",
            impute_task_purpose="testing"
        )
        print(f"   ✅ EntityToImpute: name='{entity.name}', context='{entity.identifier_context}'")
        
        # Test WhatToRetain
        field = WhatToRetain(
            name="component_type",
            desc="Type of electronic component",
            example="NPN transistor"
        )
        print(f"   ✅ WhatToRetain: name='{field.name}', desc='{field.desc}'")
        
        # Test SerpResult
        serp_result = SerpResult(
            query="test query",
            links=["https://example.com"],
            total_results=1,
            search_engine="google",
            elapsed_time=1.5,
            success=True
        )
        print(f"   ✅ SerpResult: query='{serp_result.query}', success={serp_result.success}")
        
        # Test ScrapeResult
        scrape_result = ScrapeResult(
            url="https://example.com",
            data="<html>test</html>",
            status="ready",
            html_char_size=18,
            cost=0.5
        )
        print(f"   ✅ ScrapeResult: url='{scrape_result.url}', status='{scrape_result.status}'")
        
        # Test ExtractResult
        extract_result = ExtractResult(
            url="https://example.com",
            content={"component_type": "transistor"},
            confidence_score=0.9,
            success=True
        )
        print(f"   ✅ ExtractResult: success={extract_result.success}, confidence={extract_result.confidence_score}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Entity creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_entity_validation():
    """Test entity validation and edge cases"""
    
    print("\n2️⃣ Testing entity validation...")
    
    try:
        # Test EntityToImpute validation
        try:
            entity = EntityToImpute(name="")  # Empty name should fail
            print("   ❌ Empty entity name should have failed validation")
            return False
        except ValueError:
            print("   ✅ Empty entity name correctly rejected")
        
        try:
            entity = EntityToImpute(name="   ")  # Whitespace-only name should fail
            print("   ❌ Whitespace-only entity name should have failed validation")
            return False
        except ValueError:
            print("   ✅ Whitespace-only entity name correctly rejected")
        
        # Test valid entity with minimal info
        entity = EntityToImpute(name="test")
        print(f"   ✅ Minimal valid entity: name='{entity.name}'")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Entity validation test failed: {e}")
        return False


def test_schema_creation():
    """Test schema creation with different field types"""
    
    print("\n3️⃣ Testing schema creation...")
    
    try:
        # Electronic component schema
        electronic_schema = [
            WhatToRetain(name="component_type", desc="Type of component"),
            WhatToRetain(name="voltage_rating", desc="Maximum voltage", example="75V"),
            WhatToRetain(name="package_type", desc="Physical package", example="SOT-23"),
            WhatToRetain(name="manufacturer", desc="Who makes it", example="NXP")
        ]
        print(f"   ✅ Electronic component schema: {len(electronic_schema)} fields")
        
        # Pharmaceutical schema
        pharma_schema = [
            WhatToRetain(name="chemical_formula", desc="Molecular formula", example="C9H8O4"),
            WhatToRetain(name="mechanism", desc="How it works"),
            WhatToRetain(name="dosage", desc="Typical dose", example="325mg"),
        ]
        print(f"   ✅ Pharmaceutical schema: {len(pharma_schema)} fields")
        
        # Company schema
        company_schema = [
            WhatToRetain(name="valuation", desc="Company value", example="$80B"),
            WhatToRetain(name="employees", desc="Number of staff", example="500"),
            WhatToRetain(name="founded", desc="Year established", example="2015")
        ]
        print(f"   ✅ Company schema: {len(company_schema)} fields")
        
        # Test schema validation
        for schema in [electronic_schema, pharma_schema, company_schema]:
            for field in schema:
                assert field.name.strip(), f"Field name cannot be empty: {field}"
                assert field.desc.strip(), f"Field description cannot be empty: {field}"
        
        print("   ✅ All schema fields validated successfully")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Schema creation failed: {e}")
        return False


def test_result_aggregation():
    """Test that results can be properly aggregated"""
    
    print("\n4️⃣ Testing result aggregation...")
    
    try:
        # Create entity and schema
        entity = EntityToImpute(name="test_component")
        schema = [
            WhatToRetain(name="type", desc="Component type"),
            WhatToRetain(name="voltage", desc="Voltage rating")
        ]
        
        # Create mock results
        serp_result = SerpResult(
            query="test_component",
            links=["https://site1.com", "https://site2.com"],
            total_results=2,
            search_engine="mock",
            elapsed_time=0.5,
            success=True
        )
        
        scrape_results = {
            "https://site1.com": ScrapeResult(
                url="https://site1.com",
                data="<html>Component data</html>",
                status="ready",
                cost=0.1
            ),
            "https://site2.com": ScrapeResult(
                url="https://site2.com", 
                data="<html>More data</html>",
                status="ready",
                cost=0.15
            )
        }
        
        extract_results = {
            "https://site1.com": ExtractResult(
                url="https://site1.com",
                content={"type": "transistor", "voltage": "75V"},
                confidence_score=0.9,
                success=True,
                cost=0.02
            ),
            "https://site2.com": ExtractResult(
                url="https://site2.com",
                content={"type": "transistor", "voltage": "80V"},
                confidence_score=0.8,
                success=True,
                cost=0.03
            )
        }
        
        # Create final ImputeResult
        result = ImputeResult(
            entity=entity,
            schema=schema,
            serp_result=serp_result,
            scrape_results=scrape_results,
            extract_results=extract_results,
            final_data={"type": "transistor", "voltage": "75V"},
            total_cost=0.30,
            success=True
        )
        
        # Test result properties
        assert result.successful_extractions == 2
        assert result.total_urls_scraped == 2
        assert result.successful_scrapes == 2
        
        print(f"   ✅ ImputeResult created successfully")
        print(f"      Successful extractions: {result.successful_extractions}")
        print(f"      URLs scraped: {result.total_urls_scraped}")
        print(f"      Total cost: ${result.total_cost}")
        print(f"      Final data: {result.final_data}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Result aggregation failed: {e}")
        return False


def test_metadata_handling():
    """Test metadata and additional fields handling"""
    
    print("\n5️⃣ Testing metadata handling...")
    
    try:
        # Test metadata in various results
        serp_result = SerpResult(
            query="test",
            links=["https://example.com"],
            total_results=1,
            search_engine="test",
            elapsed_time=1.0,
            success=True,
            metadata={"api_calls": 1, "rate_limited": False}
        )
        
        scrape_result = ScrapeResult(
            url="https://example.com",
            data="content",
            status="ready",
            metadata={"method": "simple_http", "response_size": 1000}
        )
        
        extract_result = ExtractResult(
            url="https://example.com",
            content={"field": "value"},
            success=True,
            metadata={"model_used": "gpt-4", "tokens": 500}
        )
        
        # Verify metadata is accessible
        assert "api_calls" in serp_result.metadata
        assert "method" in scrape_result.metadata
        assert "model_used" in extract_result.metadata
        
        print("   ✅ Metadata handling works correctly")
        print(f"      SERP metadata: {serp_result.metadata}")
        print(f"      Scrape metadata: {scrape_result.metadata}")
        print(f"      Extract metadata: {extract_result.metadata}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Metadata handling failed: {e}")
        return False


def main():
    """Run all entity and schema tests"""
    
    print("🚀 Starting Entity and Schema System Tests")
    
    # Run tests
    test1_passed = test_entity_creation()
    test2_passed = test_entity_validation()
    test3_passed = test_schema_creation()
    test4_passed = test_result_aggregation()
    test5_passed = test_metadata_handling()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 ENTITY & SCHEMA TEST RESULTS")
    print("=" * 60)
    print(f"Entity Creation: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Entity Validation: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    print(f"Schema Creation: {'✅ PASS' if test3_passed else '❌ FAIL'}")
    print(f"Result Aggregation: {'✅ PASS' if test4_passed else '❌ FAIL'}")
    print(f"Metadata Handling: {'✅ PASS' if test5_passed else '❌ FAIL'}")
    
    all_passed = all([test1_passed, test2_passed, test3_passed, test4_passed, test5_passed])
    
    if all_passed:
        print("\n🎉 All entity and schema tests passed!")
    else:
        print("\n⚠️  Some entity and schema tests failed.")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)