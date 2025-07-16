# imputeman/utils/scrape_and_save.py (or add to existing utils.py)

# python -m imputeman.utils.scrape_and_save

import asyncio
from pathlib import Path
from typing import Optional, Union
from ..services.scraper_service import ScraperService
from ..core.config import ScrapeConfig
from brightdata.models import ScrapeResult
import logging

logger = logging.getLogger(__name__)


def scrape_and_save(
    url: str,
    save_dir: Union[str, Path] = "./scraped_data",
    filename: Optional[str] = None,
    bearer_token: Optional[str] = None,
    pretty_json: bool = True,
    overwrite: bool = False,
    poll_timeout: float = 120.0
) -> tuple[ScrapeResult, Optional[Path]]:
    """
    Scrape a URL and save the result to a file.
    
    Args:
        url: URL to scrape
        save_dir: Directory to save the scraped data
        filename: Optional filename (auto-generated if None)
        bearer_token: BrightData API token (uses env var if None)
        pretty_json: Pretty-print JSON files
        overwrite: Overwrite existing files
        poll_timeout: Timeout for BrightData polling
        
    Returns:
        Tuple of (ScrapeResult, saved_file_path)
        Path will be None if save failed or data was empty
        
    Example:
        >>> result, path = scrape_and_save("https://example.com")
        >>> print(f"Scraped: {result.success}, Saved to: {path}")
    """
    # Create async wrapper and run it
    async def _scrape_async():
        # Create scraper config
        config = ScrapeConfig(
            bearer_token=bearer_token,
            poll_timeout=poll_timeout
        )
        
        # Initialize scraper service
        scraper = ScraperService(config)
        
        try:
            # Scrape the URL
            logger.info(f"🕷️ Scraping {url}...")
            scrape_results = await scraper.scrape_urls([url])
            
            # Get the result for our URL
            scrape_result = scrape_results.get(url)
            
            if not scrape_result:
                logger.error(f"❌ No scrape result returned for {url}")
                return ScrapeResult(
                    success=False,
                    url=url,
                    status="error",
                    error="No result returned from scraper"
                ), None
            
            # Log scrape status
            if scrape_result.success and scrape_result.status == "ready":
                size = len(scrape_result.data) if scrape_result.data else 0
                logger.info(f"✅ Scrape successful: {size:,} characters, cost: ${scrape_result.cost or 0:.4f}")
                
                # Save the data
                try:
                    saved_path = scrape_result.save_data_to_file(
                        filename=filename,
                        dir_=save_dir,
                        pretty_json=pretty_json,
                        overwrite=overwrite,
                        raise_if_empty=False  # Don't raise, just return None
                    )
                    logger.info(f"💾 Saved to: {saved_path}")
                    return scrape_result, saved_path
                    
                except Exception as e:
                    logger.error(f"❌ Failed to save: {e}")
                    return scrape_result, None
            else:
                logger.error(f"❌ Scrape failed: {scrape_result.error or 'Unknown error'}")
                return scrape_result, None
                
        except Exception as e:
            logger.error(f"❌ Scraping error: {e}")
            return ScrapeResult(
                success=False,
                url=url,
                status="error",
                error=str(e)
            ), None
        finally:
            await scraper.close()
    
    # Run the async function synchronously
    return asyncio.run(_scrape_async())


# Even simpler version if you just want the basics
def quick_scrape(url: str, save_dir: str = "./scraped_data") -> Optional[Path]:
    """
    Simple scrape and save - returns just the file path.
    
    Args:
        url: URL to scrape
        save_dir: Where to save files
        
    Returns:
        Path to saved file, or None if failed
        
    Example:
        >>> path = quick_scrape("https://example.com")
        >>> if path:
        ...     print(f"Saved to: {path}")
    """
    result, path = scrape_and_save(url, save_dir)
    return path


# Example usage
if __name__ == "__main__":
    # Test the function

    test_url="https://www.digikey.com/en/products/result?keywords=RGC1A23D15KGU"
  #  test_url= "https://venkel.com/part/BAV99-EVL"
    # test_url = "https://www.example.com"
    
    print(f"Testing scrape_and_save with {test_url}")
    result, path = scrape_and_save(test_url)
    
    if result.success:
        print(f"✅ Success!")
        print(f"   Status: {result.status}")
        print(f"   Cost: ${result.cost or 0:.4f}")
        print(f"   Size: {result.html_char_size or len(result.data or '')} chars")
        print(f"   Saved to: {path}")
    else:
        print(f"❌ Failed: {result.error}")