# try.py


# python -m imputeman.try
import asyncio
import json
from imputeman import Imputeman
from imputeman.core.entities import EntityToImpute, WhatToRetain
from imputeman.core.config import get_development_config


async def main():
    """Test Imputeman with Turkish company entity"""
    
    # Define extraction schema
    schema = [
        WhatToRetain(name="company_information", desc="generic information about company"),
        WhatToRetain(name="company's links", desc="address of company pages or socials, should be a webpage"),
        # WhatToRetain(name="social_media_handlers", desc="company's social media handlers, should be a webpage")
    ]
    
    # Entity name - Turkish motorcycle company


    #entity_name="FÇ TEKSTİL SANAYİ VE DIŞ TİCARET ANONİM ŞİRKETİ"
    # entity_name = "MOTORCU HAYRİ MOTORSİKLET VE BİSİKLET SANAYİ VE TİCARET LİMİTED ŞİRKETİ"
    entity_name = "Istanbuldaki tekstil firmalari"


    entity = EntityToImpute(name=entity_name)
    
    # Get configuration
    config = get_development_config()
    
    print("🚀 Testing Imputeman with Turkish Company")
    print("=" * 60)
    print(f"🎯 Entity: {entity_name}")
    print(f"📋 Schema: {len(schema)} fields")
    print()
    
    # Initialize and run Imputeman
    imputeman = Imputeman(config)
    
    print("🔄 Running pipeline...")
    print()
    
    try:
        impute_op = await imputeman.run(
            entity=entity,
            schema=schema,
            max_urls=5,
            enable_streaming=True,
            capture_detailed_metrics=True
        )
        
        # Print results summary
        print(f"\n🎯 Imputeman Results:")
        print(f"   Success: {impute_op.success}")
        print(f"   Extracted data: {len(impute_op.extract_results)} items")
        print(f"   Total cost: ${impute_op.costs.total_cost:.4f}")
        print(f"   Live summary: {impute_op.get_live_summary()}")
        
        # Show errors if any
        if impute_op.errors:
            print(f"\n⚠️ Errors encountered:")
            for error in impute_op.errors:
                print(f"   - {error}")
        
        # Pretty print successful extractions
        print(f"\n✨ Successful Extractions:")
        print("=" * 60)
        
        if impute_op.extract_results:
            success_count = 0
            for url, extract_op in impute_op.extract_results.items():
                if extract_op.success and extract_op.content:
                    success_count += 1
                    print(f"\n📌 Source #{success_count}: {url[:80]}...")
                    print("-" * 60)
                    
                    # Handle different content types
                    content = extract_op.content
                    
                    if isinstance(content, list):
                        # List of items
                        for i, item in enumerate(content):
                            print(f"\n  Item {i+1}:")
                            if isinstance(item, dict):
                                for key, value in item.items():
                                    print(f"    • {key}: {value}")
                            else:
                                print(f"    {item}")
                    
                    elif isinstance(content, dict):
                        # Single dictionary
                        for key, value in content.items():
                            print(f"  • {key}:")
                            if isinstance(value, list):
                                for v in value:
                                    print(f"      - {v}")
                            elif isinstance(value, dict):
                                print(json.dumps(value, indent=6, ensure_ascii=False))
                            else:
                                print(f"      {value}")
                    
                    else:
                        # Other types
                        print(f"  {content}")
                    
                    # Show extraction metrics
                    if hasattr(extract_op, 'usage') and extract_op.usage:
                        cost = extract_op.usage.get('total_cost', 0) or extract_op.usage.get('cost', 0)
                        print(f"\n  💰 Extraction cost: ${cost:.4f}")
                    
                    if hasattr(extract_op, 'elapsed_time'):
                        print(f"  ⏱️  Extraction time: {extract_op.elapsed_time:.2f}s")
            
            if success_count == 0:
                print("\n  ❌ No successful extractions found")
        else:
            print("\n  ❌ No extraction results available")
        
        # Print performance summary
        print(f"\n📊 Performance Summary:")
        print("=" * 60)
        print(f"  URLs found: {impute_op.performance.urls_found}")
        print(f"  Successful scrapes: {impute_op.performance.successful_scrapes}")
        print(f"  Successful extractions: {impute_op.performance.successful_extractions}")
        print(f"  Total time: {impute_op.performance.total_elapsed_time:.2f}s")
        print(f"  Cost breakdown:")
        print(f"    - SERP: ${impute_op.costs.serp_cost:.4f}")
        print(f"    - Scrape: ${impute_op.costs.scrape_cost:.4f}")
        print(f"    - Extract: ${impute_op.costs.extraction_cost:.4f}")
        print(f"    - Total: ${impute_op.costs.total_cost:.4f}")
        
        return impute_op
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        return None


def pretty_print_json(data, indent=2):
    """Helper to pretty print JSON data"""
    return json.dumps(data, indent=indent, ensure_ascii=False)


if __name__ == "__main__":
    # Run the async main function
    result = asyncio.run(main())
    
    if result and result.success:
        print(f"\n✅ Test completed successfully!")
    else:
        print(f"\n⚠️ Test completed with issues")