# imputeman/services/serp_service.py
"""SERP (Search Engine Results Page) service for web search operations"""

import asyncio
import time
from typing import List, Dict, Any, Optional

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from ..core.config import SerpConfig
from ..core.entities import SerpResult

# Import the actual SERPEngine
try:
    from serpengine.serpengine import SERPEngine
    SERPENGINE_AVAILABLE = True
except ImportError:
    SERPENGINE_AVAILABLE = False


class SerpService:
    """
    Service for handling search engine API calls using SERPEngine
    
    This service uses the production SERPEngine library for Google Custom Search
    and provides a consistent interface for search operations.
    """
    
    def __init__(self, config: SerpConfig):
        self.config = config
        
        if SERPENGINE_AVAILABLE:
            self.engine = SERPEngine()
        else:
            self.engine = None
            # print("⚠️  SERPEngine not available, falling back to mock search")
    
    async def search(self, query: str, top_k: int = None) -> SerpResult:
        """
        Execute search query using SERPEngine and return results
        
        Args:
            query: Search query string
            top_k: Number of results to return (overrides config)
            
        Returns:
            SerpResult with search results and metadata
        """
        start_time = time.time()
        top_k = top_k or self.config.top_k_results
        
        try:
            if SERPENGINE_AVAILABLE and self.engine:
              
                links = await self._search_with_serpengine(query, top_k)
                
                
                # Log each link individually
                for i, link in enumerate(links, 1):
                    logger.debug(f"🔗 SERP Link {i}: {link}")
            
                
                search_engine = "google_custom_search"
            else:
                # Fallback to mock search for development
                print("SERP NOT AVAILABLE")
               
            
            elapsed_time = time.time() - start_time
            
            return SerpResult(
                query=query,
                links=links,
                total_results=len(links),
                search_engine=search_engine,
                elapsed_time=elapsed_time,
                success=len(links) > 0,
                metadata={
                    "top_k_requested": top_k,
                    "api_calls": 1,
                    "serpengine_used": SERPENGINE_AVAILABLE
                }
            )
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            return SerpResult(
                query=query,
                links=[],
                total_results=0,
                search_engine="error",
                elapsed_time=elapsed_time,
                success=False,
                metadata={"error": str(e), "error_type": type(e).__name__}
            )
    
    async def _search_with_serpengine(self, query: str, top_k: int) -> List[str]:
        """
        Search using the official SERPEngine library
        
        Args:
            query: Search query string
            top_k: Number of results to return
            
        Returns:
            List of URLs from search results
        """
        # Use SERPEngine's async collect method
        serpengine_op = await self.engine.collect_async(
            query=query,
            num_urls=top_k,
            search_sources=["google_search_via_api"],  # Use Google Custom Search API
            output_format="object"  # Get structured objects
        )
        
        # Extract URLs from SERPEngine results
        links = serpengine_op.all_links()
        
      
        
        return links[:top_k]
    
    
    
    async def validate_urls(self, urls: List[str]) -> List[str]:
        """
        Validate and filter URLs
        
        Args:
            urls: List of URLs to validate
            
        Returns:
            List of valid, unique URLs
        """
        valid_urls = []
        seen_urls = set()
        
        for url in urls:
            if self._is_valid_url(url) and url not in seen_urls:
                valid_urls.append(url)
                seen_urls.add(url)
        
        return valid_urls
    
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
        
        # Block file downloads that might not contain useful text
        blocked_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']
        for ext in blocked_extensions:
            if url.lower().endswith(ext):
                return False
        
        return True
    
    async def close(self):
        """Clean up resources"""
        if hasattr(self, 'client'):
            await self.client.aclose()


# For backward compatibility and testing without SERPEngine
class RateLimiter:
    """Simple rate limiter for API calls"""
    
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.calls = []
    
    async def acquire(self):
        """Wait if necessary to respect rate limits"""
        now = time.time()
        
        # Remove calls older than 1 minute
        self.calls = [call_time for call_time in self.calls if now - call_time < 60]
        
        # If we're at the limit, wait
        if len(self.calls) >= self.calls_per_minute:
            wait_time = 60 - (now - self.calls[0])
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        # Record this call
        self.calls.append(now)