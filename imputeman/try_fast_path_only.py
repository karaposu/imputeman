# python -m imputeman.try_fast_path_only

import asyncio
from imputeman import Imputeman
from imputeman.core.config import get_digikey_fast_only_config, FastPathConfig
from imputeman.core.entities import EntityToImpute


async def run_fast_path_example():
    # Configure for fast path only
    config = get_digikey_fast_only_config()
    
    # Create entity
    # entity = EntityToImpute(name="BAV99")
    # entity = EntityToImpute(name="STM32F407VGT6")
    entity = EntityToImpute(name="RGC1A23D15KGU")
    
    # 
    # https://www.digikey.com/en/products/result?keywords=RGC1A23D15KGU


    

    
    # Run pipeline - will only do fast path scraping
    imputeman = Imputeman(config)
    impute_op = await imputeman.run(
        entity=entity,
        schema=[],  # Schema not needed for fast path
        max_urls=0  # Not used in fast path mode
    )
    
    # Access raw scrape results
    if impute_op.success and impute_op.scrape_results:
        for url, scrape_result in impute_op.scrape_results.items():
            if scrape_result.success:
                print(f"URL: {url}")
                print(f"Data size: {len(scrape_result.data)} chars")
                print(f"Cost: ${scrape_result.cost}")
                
                # Save raw HTML/data if needed
                # scrape_result.save_data_to_file(
                #     dir_="fast_path_results",
                #     filename=f"digikey_{entity.name}"
                # )
    
    return impute_op

# Run it
if __name__ == "__main__":
    result = asyncio.run(run_fast_path_example())