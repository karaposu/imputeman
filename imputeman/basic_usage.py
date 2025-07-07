# examples/basic_usage.py
"""Basic usage examples for Imputeman with Prefect"""

import asyncio
import sys
import os

# Add the parent directory to the path so we can import imputeman
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imputeman.core.entities import EntityToImpute, WhatToRetain
from imputeman.core.config import get_development_config
from imputeman.flows.main_flow import imputeman_flow, simple_imputeman_flow


async def basic_example():
    """Basic example of running the imputeman pipeline"""
    
    # Create entity to impute - bav99 is an electronic component part number
    entity = EntityToImpute(
        name="bav99",
        identifier_context="electronic component part number",
        impute_task_purpose="component specification research"
    )
    
    # Define expected data schema for electronic component
    # Extract technical specifications we DON'T already know
    schema = [
        WhatToRetain(
            name="component_type",
            desc="Type of electronic component",
            example="NPN transistor"
        ),
        WhatToRetain(
            name="voltage_rating",
            desc="Maximum voltage rating in volts",
            example="75V"
        ),
        WhatToRetain(
            name="current_rating",
            desc="Maximum current rating in amperes",
            example="0.2A"
        ),
        WhatToRetain(
            name="package_type",
            desc="Physical package type",
            example="SOT-23"
        ),
        WhatToRetain(
            name="manufacturer",
            desc="Component manufacturer",
            example="NXP Semiconductors"
        ),
        WhatToRetain(
            name="applications",
            desc="Common applications or use cases",
            example="switching, amplification"
        )
    ]
    
    # Run the pipeline
    print("🚀 Starting Imputeman pipeline...")
    result = await simple_imputeman_flow(entity, schema, top_k=5)
    
    # Print results
    if result.success:
        print("✅ Pipeline completed successfully!")
        print(f"💰 Total cost: ${result.total_cost:.2f}")
        print(f"⏱️ Total time: {result.total_elapsed_time:.2f}s")
        print(f"📊 Successful extractions: {result.successful_extractions}")
        print(f"🔗 URLs scraped: {result.total_urls_scraped}")
        
        if result.final_data:
            print(f"\n📋 Technical specifications for {entity.name}:")
            for key, value in result.final_data.items():
                if not key.startswith('_'):  # Skip metadata
                    print(f"  {key}: {value}")
    else:
        print("❌ Pipeline failed")
        if "error" in result.metadata:
            print(f"Error: {result.metadata['error']}")


async def advanced_example():
    """Advanced example with a different entity type - pharmaceutical compound"""
    
    # Create entity for a pharmaceutical compound
    entity = EntityToImpute(
        name="aspirin",
        identifier_context="pharmaceutical compound",
        impute_task_purpose="drug research and development"
    )
    
    # Custom schema for pharmaceutical research
    schema = [
        WhatToRetain(
            name="chemical_formula",
            desc="Chemical molecular formula",
            example="C9H8O4"
        ),
        WhatToRetain(
            name="molecular_weight", 
            desc="Molecular weight in g/mol",
            example="180.16"
        ),
        WhatToRetain(
            name="mechanism_of_action",
            desc="How the drug works in the body",
            example="COX enzyme inhibition"
        ),
        WhatToRetain(
            name="therapeutic_class",
            desc="Drug classification or category",
            example="NSAID, antiplatelet agent"
        ),
        WhatToRetain(
            name="half_life",
            desc="Elimination half-life from body",
            example="2-3 hours"
        ),
        WhatToRetain(
            name="common_dosage",
            desc="Typical dosage for adults",
            example="325-650mg every 4-6 hours"
        ),
        WhatToRetain(
            name="side_effects",
            desc="Common adverse effects",
            example="stomach irritation, bleeding risk"
        )
    ]
    
    # Use development configuration with custom settings
    config = get_development_config()
    config.serp_config.top_k_results = 8
    config.cost_threshold_for_budget_mode = 50.0  # Lower threshold
    config.extract_config.confidence_threshold = 0.8  # Higher quality requirement
    
    print("🚀 Starting advanced Imputeman pipeline...")
    result = await imputeman_flow(entity, schema, config)
    
    # Detailed results analysis
    print(f"\n📊 Pipeline Results for {entity.name}:")
    print(f"Success: {'✅' if result.success else '❌'}")
    print(f"Total Cost: ${result.total_cost:.2f}")
    print(f"Execution Time: {result.total_elapsed_time:.2f}s")
    
    if result.serp_result:
        print(f"Search Results: {len(result.serp_result.links)} URLs found")
    
    if result.scrape_results:
        successful_scrapes = sum(1 for r in result.scrape_results.values() if r.status == "ready")
        print(f"Scraping: {successful_scrapes}/{len(result.scrape_results)} successful")
    
    if result.extract_results:
        print(f"Extraction: {result.successful_extractions}/{len(result.extract_results)} successful")
    
    # Show final data
    if result.final_data and result.success:
        print("\n📋 Extracted Information:")
        for field_name in schema.keys():
            if field_name in result.final_data:
                data_point = result.final_data[field_name]
                if isinstance(data_point, dict) and "value" in data_point:
                    confidence = data_point.get("confidence", 0.0)
                    print(f"  {field_name}: {data_point['value']} (confidence: {confidence:.2f})")
                else:
                    print(f"  {field_name}: {data_point}")


async def error_handling_example():
    """Example showing error handling and partial results"""
    
    # Create entity that might be challenging to find data for
    entity = EntityToImpute(
        name="xyz999-nonexistent",
        identifier_context="electronic component part number", 
        impute_task_purpose="testing error handling"
    )
    
    # Try to extract information about this non-existent component
    schema = [
        WhatToRetain(
            name="component_type", 
            desc="Type of electronic component"
        ),
        WhatToRetain(
            name="manufacturer",
            desc="Component manufacturer"
        ),
        WhatToRetain(
            name="availability_status",
            desc="Current availability status (active, obsolete, etc.)"
        )
    ]
    
    print("🧪 Testing error handling with non-existent component...")
    result = await simple_imputeman_flow(entity, schema, top_k=3)
    
    if not result.success:
        print("❌ Pipeline failed as expected for non-existent component")
        print("📊 Partial results still available:")
        
        if result.serp_result:
            print(f"  - Search attempted: {result.serp_result.query}")
            print(f"  - Links found: {len(result.serp_result.links)}")
        
        if result.scrape_results:
            print(f"  - Scraping attempted on {len(result.scrape_results)} URLs")
        
        print(f"  - Total time: {result.total_elapsed_time:.2f}s")
        print(f"  - Total cost: ${result.total_cost:.2f}")


if __name__ == "__main__":
    print("Imputeman with Prefect - Basic Examples\n")
    
    # Run examples
    asyncio.run(basic_example())
    print("\n" + "="*50 + "\n")
    
    asyncio.run(advanced_example())
    print("\n" + "="*50 + "\n")
    
    asyncio.run(error_handling_example())