# imputeman/services/serp_service.py
"""SERP (Search Engine Results Page) service for web search operations"""

import asyncio
from typing import List, Dict, Any, Optional
import logging

from serpengine.serpengine import SERPEngine
from serpengine.schemas import SerpEngineOp, SearchHit, UsageInfo, SerpChannelOp
from ..core.config import SerpConfig

logger = logging.getLogger(__name__)


class SerpService:
    """
    Service for handling search engine API calls using SERPEngine
    
    This service uses the production SERPEngine library which supports
    multiple search channels (Google API, SerpAPI, DataForSEO, etc.)
    
    Smart URL fetching strategy:
    - Always fetches minimum 15 URLs internally for better coverage
    - If user requests < 15 URLs, fetches 15 but returns only requested amount
    - If user requests > 15 URLs, fetches exactly what they requested
    - This provides a buffer in case some URLs fail during scraping/extraction
    """
    
    # Minimum URLs to fetch for better coverage
    MIN_URLS_TO_FETCH = 15
    
    def __init__(self, config: SerpConfig):
        self.config = config
        
        # Initialize SERPEngine with specific channels
        # Always try google_api and serpapi
        channels_to_try = ["google_api", "serpapi"]
        self.engine = SERPEngine(channels=channels_to_try)
        
        # Log available channels
        if self.engine.available_channels:
            logger.info(f"SERPEngine initialized with channels: {self.engine.available_channels}")
        else:
            logger.warning("No search channels could be initialized")
            logger.warning("Please set API credentials for at least one channel:")
            logger.warning("  - Google API: GOOGLE_SEARCH_API_KEY and GOOGLE_CSE_ID")
            logger.warning("  - SerpAPI: SERPAPI_API_KEY")
            raise ValueError(
                "No search channels available. Please check your API credentials. "
                "Set GOOGLE_SEARCH_API_KEY and GOOGLE_CSE_ID for google_api, "
                "or SERPAPI_API_KEY for serpapi."
            )
    
    async def search(self, query: str, top_k: int = None) -> SerpEngineOp:
        """
        Execute search query using SERPEngine and return results
        
        Strategy:
        - Always fetch at least MIN_URLS_TO_FETCH (15) URLs for better coverage
        - If user requests less than 15, fetch 15 but return only requested amount
        - If user requests more than 15, fetch exactly what they requested
        
        Args:
            query: Search query string
            top_k: Number of results to return (will fetch min 15 internally)
            
        Returns:
            SerpEngineOp with search results and metadata
        """
        requested_urls = top_k or self.config.top_k_results
        
        # Determine how many to actually fetch
        urls_to_fetch = max(requested_urls, self.MIN_URLS_TO_FETCH)
        
        #logger.debug(f"🔍 SERP search strategy: Requested {requested_urls} URLs, fetching {urls_to_fetch} for better coverage")
        
        try:
            # Use async search for better performance
            serp_result = await self._search_with_serpengine_async(query, urls_to_fetch)
            
            # If we fetched more than requested, trim the results
            if urls_to_fetch > requested_urls and serp_result.results:
               # logger.info(f"✂️  Trimming results from {len(serp_result.results)} to {requested_urls} as requested")
                
                # Trim the main results list
                serp_result.results = serp_result.results[:requested_urls]
                
                # Also trim channel-specific results if they exist
                for channel in serp_result.channels:
                    if channel.results and len(channel.results) > requested_urls:
                        channel.results = channel.results[:requested_urls]
            
            # Log summary
           # logger.info(f"✅ SERP search completed: {len(serp_result.results)} results returned from {len(serp_result.channels)} channels")
            
            # Log per-channel results
            for channel in serp_result.channels:
                logger.debug(f"   📡 {channel.name}: {len(channel.results)} results, ${channel.usage.cost:.4f}")
            
            # Log individual links
            for i, hit in enumerate(serp_result.results, 1):
                logger.debug(f"        🔗 Link {i}: {hit.link} (from {hit.channel_name}, rank #{hit.channel_rank})")
            
            return serp_result
            
        except Exception as e:
            logger.error(f"SERP search failed: {e}", exc_info=True)
            # Re-raise the exception - let the caller handle it
            raise
    
    async def _search_with_serpengine_async(self, query: str, top_k: int) -> SerpEngineOp:
        """
        Search using the SERPEngine library with async support
        
        Args:
            query: Search query string
            top_k: Number of results to return per channel
            
        Returns:
            SerpEngineOp with full search results
        """
        # Use all available channels (google_api and/or serpapi)
        # SERPEngine will only use the ones with valid credentials
        serp_result = await self.engine.collect_async(
            query=query,
            num_of_links_per_channel=top_k,
            search_sources=None,  # None means use all available channels
            output_format="object",  # Get SerpEngineOp object
            regex_based_link_validation=True,
            allow_links_forwarding_to_files=False,  # Filter out PDFs, etc.
            
            # Use the new features from config!
            activate_interleaving=self.config.activate_interleaving,
            deduplicate_links=self.config.deduplicate_links
        )
    
        return serp_result
    
    def extract_urls_from_result(self, serp_result: SerpEngineOp) -> List[str]:
        """
        Extract clean list of URLs from SerpEngineOp
        
        Args:
            serp_result: SerpEngineOp from search
            
        Returns:
            List of unique, valid URLs
        """
        # Use the built-in all_links property
        urls = serp_result.all_links
        
        # Additional validation if needed
        validated_urls = []
        seen_urls = set()
        
        for url in urls:
            if self._is_valid_url(url) and url not in seen_urls:
                validated_urls.append(url)
                seen_urls.add(url)
        
        return validated_urls
    
    def _is_valid_url(self, url: str) -> bool:
        """
        Basic URL validation and filtering
        
        Args:
            url: URL to validate
            
        Returns:
            True if URL is valid and allowed
        """
        if not url or not isinstance(url, str):
            return False
        
        url = url.strip()
        
        # Must be HTTP/HTTPS
        if not url.startswith(('http://', 'https://')):
            return False
        
        # Block social media and irrelevant domains
        blocked_domains = [
            'facebook.com', 'twitter.com', 'instagram.com',
            'tiktok.com', 'youtube.com', 'pinterest.com',
            'reddit.com'  # Often not useful for component data
        ]
        
        for domain in blocked_domains:
            if domain in url.lower():
                return False
        
        return True
    
    def get_channel_statistics(self, serp_result: SerpEngineOp) -> Dict[str, Any]:
        """
        Get statistics about search channels used
        
        Args:
            serp_result: SerpEngineOp from search
            
        Returns:
            Dict with channel statistics
        """
        stats = {
            "total_results": len(serp_result.results),
            "total_cost": serp_result.usage.cost,
            "total_time": serp_result.elapsed_time,
            "channels_used": len(serp_result.channels),
            "by_channel": {}
        }
        
        # Get results grouped by channel
        results_by_channel = serp_result.results_by_channel()
        
        for channel in serp_result.channels:
            channel_results = results_by_channel.get(channel.name, [])
            stats["by_channel"][channel.name] = {
                "results": len(channel_results),
                "cost": channel.usage.cost,
                "time": channel.elapsed_time,
                "top_result": channel_results[0].title if channel_results else None
            }
        
        return stats
    
    async def close(self):
        """Clean up resources"""
        # SERPEngine doesn't need explicit cleanup, but keep for interface consistency
        pass
    
    def create_empty_result(self, error_msg: str = "") -> SerpEngineOp:
        """
        Create an empty SerpEngineOp for error cases
        
        Args:
            error_msg: Optional error message
            
        Returns:
            Empty SerpEngineOp with no results
        """
        return SerpEngineOp(
            channels=[],
            usage=UsageInfo(cost=0.0),
            results=[],
            elapsed_time=0.0
        )


async def main():
    """
    Test the SerpService independently
    
    Run with: python -m imputeman.services.serp_service
    """
    print("=== Testing SerpService with minimum URLs logic ===")
    print()
    
    # Initialize service
    try:
        from ..core.config import SerpConfig
        config = SerpConfig(top_k_results=5)
    except:
        # Fallback for standalone testing
        config = type('SerpConfig', (), {
            'top_k_results': 5,
            'search_engines': ['google_api', 'serpapi'],
            'timeout_seconds': 30.0
        })()
    
    try:
        service = SerpService(config)
        print(f"✅ SerpEngine initialized with channels: {service.engine.available_channels}")
    except Exception as e:
        print(f"❌ Failed to initialize SerpService: {e}")
        print("\nPlease ensure:")
        print("1. serpengine is installed: pip install serpengine")
        print("2. You have valid API credentials set as environment variables")
        print("   - GOOGLE_SEARCH_API_KEY and GOOGLE_CSE_ID for google_api")
        print("   - SERPAPI_API_KEY for serpapi")
        print("   - DATAFORSEO_USERNAME and DATAFORSEO_PASSWORD for dataforseo")
        return
    
    print()
    
    # Test 1: Request 5 URLs (should fetch 15 internally)
    print("Test 1: Requesting 5 URLs (will fetch 15 internally)")
    query = "Python web scraping BeautifulSoup"
    print(f"🔍 Query: '{query}'")
    
    try:
        result = await service.search(query, top_k=5)
        
        print(f"\n📊 Results:")
        print(f"   Returned: {len(result.results)} URLs (requested 5)")
        print(f"   Cost: ${result.usage.cost:.4f}")
        print(f"   Time: {result.elapsed_time:.2f}s")
        
        # Show sample results
        if result.results:
            print(f"\n🔗 URLs returned:")
            for i, hit in enumerate(result.results, 1):
                print(f"   {i}. {hit.link}")
    
    except Exception as e:
        print(f"\n❌ Search failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*50)
    
    # Test 2: Request 20 URLs (should fetch 20)
    print("\nTest 2: Requesting 20 URLs (will fetch 20)")
    
    try:
        result = await service.search(query, top_k=20)
        
        print(f"\n📊 Results:")
        print(f"   Returned: {len(result.results)} URLs (requested 20)")
        print(f"   Cost: ${result.usage.cost:.4f}")
        
    except Exception as e:
        print(f"\n❌ Search failed: {e}")
    
    await service.close()
    print("\n✅ Tests completed!")


def main_sync():
    """Synchronous wrapper for testing"""
    return asyncio.run(main())


if __name__ == "__main__":
    main_sync()