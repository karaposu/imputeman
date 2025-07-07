# imputeman/services/__init__.py
"""Service layer for Imputeman - handles external integrations and business logic"""

from .serp_service import SerpService
from .scraper_service import ScraperService
from .extractor_service import ExtractorService

from ..core.config import PipelineConfig


class ServiceRegistry:
    """
    Central registry for all services used in the Imputeman pipeline
    
    This provides a single point of access to all services and handles
    their lifecycle management (creation, configuration, cleanup).
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self._serp_service = None
        self._scraper_service = None
        self._extractor_service = None
    
    @property
    def serp(self) -> SerpService:
        """Get or create SERP service"""
        if self._serp_service is None:
            self._serp_service = SerpService(self.config.serp_config)
        return self._serp_service
    
    @property
    def scraper(self) -> ScraperService:
        """Get or create scraper service"""
        if self._scraper_service is None:
            self._scraper_service = ScraperService(self.config.scrape_config)
        return self._scraper_service
    
    @property
    def extractor(self) -> ExtractorService:
        """Get or create extractor service"""
        if self._extractor_service is None:
            self._extractor_service = ExtractorService(self.config.extract_config)
        return self._extractor_service
    
    async def close_all(self):
        """Clean up all services"""
        if self._serp_service:
            await self._serp_service.close()
        if self._scraper_service:
            await self._scraper_service.close()
        if self._extractor_service:
            await self._extractor_service.close()


# Global service registry instance (initialized by tasks)
_service_registry = None


def get_service_registry(config: PipelineConfig = None) -> ServiceRegistry:
    """
    Get the global service registry
    
    Args:
        config: Pipeline configuration (required for first call)
        
    Returns:
        ServiceRegistry instance
    """
    global _service_registry
    
    if _service_registry is None:
        if config is None:
            from ..core.config import get_default_config
            config = get_default_config()
        _service_registry = ServiceRegistry(config)
    
    return _service_registry


async def cleanup_services():
    """Clean up the global service registry"""
    global _service_registry
    if _service_registry:
        await _service_registry.close_all()
        _service_registry = None


__all__ = [
    "SerpService",
    "ScraperService", 
    "ExtractorService",
    "ServiceRegistry",
    "get_service_registry",
    "cleanup_services"
]