# imputeman/core/config.py

from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
import os


class FastPathMode(Enum):
    """Modes for fast path execution"""
    DISABLED = "disabled"              # Run normal SERP → Scrape → Extract only
    FAST_PATH_ONLY = "fast_path_only"  # Run fast path only, return even if fails
    FAST_PATH_WITH_FALLBACK = "fast_path_with_fallback"  # Try fast path first, fallback to normal if fails


@dataclass
class FastPathConfig:
    """Configuration for domain-specific fast path shortcuts"""
    enabled: bool = False
    mode: FastPathMode = FastPathMode.DISABLED
    
    # Domain-specific configurations
    digikey_enabled: bool = True
    digikey_base_url: str = "https://www.digikey.com/en/products/result?keywords="
    
    # Add more domains as needed
    mouser_enabled: bool = False
    mouser_base_url: str = "https://www.mouser.com/Search/Refine?Keyword="
    
    # Timeout specific to fast path (might be different from general scraping)
    poll_timeout: float = 30.0  # Shorter timeout for fast path
    poll_interval: float = 5.0
    
    # Quality thresholds
    min_result_size: int = 1000  # Minimum chars to consider fast path successful
    
    def get_fast_path_urls(self, entity_name: str) -> Dict[str, str]:
        """Generate fast path URLs for enabled domains"""
        urls = {}
        
        if self.digikey_enabled:
            urls['digikey'] = f"{self.digikey_base_url}{entity_name}"
            
        if self.mouser_enabled:
            urls['mouser'] = f"{self.mouser_base_url}{entity_name}"
            
        # Add more domains as configured
        return urls


@dataclass
class SerpConfig:
    """Configuration for SERP/search tasks"""
    top_k_results: int = 10
    activate_interleaving: bool = True
    deduplicate_links: bool = True
    

@dataclass  
class ScrapeConfig:
    """Configuration for web scraping tasks"""
    bearer_token: Optional[str] = None
    poll_interval: float = 10.0
    poll_timeout: float = 120.0
    flexible_timeout: bool = True
    concurrent_limit: int = 5
    
    def __post_init__(self):
        if not self.bearer_token:
            self.bearer_token = os.getenv("BRIGHT_DATA_TOKEN")


@dataclass
class ExtractConfig:
    """Configuration for data extraction tasks"""
    pass


@dataclass
class PipelineConfig:
    """Master configuration for the entire pipeline"""
    serp_config: SerpConfig = field(default_factory=SerpConfig)
    scrape_config: ScrapeConfig = field(default_factory=ScrapeConfig)
    extract_config: ExtractConfig = field(default_factory=ExtractConfig)
    fast_path_config: FastPathConfig = field(default_factory=FastPathConfig)  # New!
    
    # Quality control
    min_scrape_chars: int = 5000


# Preset configurations for the three cases

def get_default_config() -> PipelineConfig:
    """Get default pipeline configuration (Case 1: Normal path only)"""
    config = PipelineConfig()
    config.fast_path_config.enabled = False
    config.fast_path_config.mode = FastPathMode.DISABLED
    return config


def get_digikey_fast_only_config() -> PipelineConfig:
    """Case 2: DigiKey fast path only, return even if fails"""
    config = PipelineConfig()
    
    # Enable fast path in "only" mode
    config.fast_path_config.enabled = True
    config.fast_path_config.mode = FastPathMode.FAST_PATH_ONLY
    config.fast_path_config.digikey_enabled = True
    
    # Adjust timeouts for faster response
    config.fast_path_config.poll_timeout = 200.0
    config.fast_path_config.poll_interval = 10.0
    
    return config


def get_digikey_fast_with_fallback_config() -> PipelineConfig:
    """Case 3: Try DigiKey fast path first, fallback to normal if fails"""
    config = PipelineConfig()
    
    # Enable fast path with fallback
    config.fast_path_config.enabled = True
    config.fast_path_config.mode = FastPathMode.FAST_PATH_WITH_FALLBACK
    config.fast_path_config.digikey_enabled = True
    
    # Fast path settings
    config.fast_path_config.poll_timeout = 30.0  # Quick timeout for fast path
    config.fast_path_config.min_result_size = 5000  # Need substantial content
    
    # Normal path settings (for fallback)
    config.serp_config.top_k_results = 5
    config.scrape_config.poll_timeout = 120.0
    
    return config


def get_development_config() -> PipelineConfig:
    """Get configuration optimized for development/testing"""
    config = PipelineConfig()
    
    # Standard development settings
    config.serp_config.top_k_results = 5
    config.serp_config.activate_interleaving = True
    config.serp_config.deduplicate_links = True
    
    config.scrape_config.poll_timeout = 60.0
    config.scrape_config.flexible_timeout = True
    config.min_scrape_chars = 3000
    
    # Fast path disabled by default in dev
    config.fast_path_config.enabled = False
    config.fast_path_config.mode = FastPathMode.DISABLED
    
    return config


def get_production_config() -> PipelineConfig:
    """Get configuration optimized for production"""
    config = PipelineConfig()
    
    # Production optimizations
    config.serp_config.top_k_results = 15
    config.serp_config.activate_interleaving = True
    config.serp_config.deduplicate_links = True
    
    config.scrape_config.poll_timeout = 180.0
    config.scrape_config.flexible_timeout = True
    config.min_scrape_chars = 10000
    
    # Consider enabling fast path with fallback in production
    config.fast_path_config.enabled = True
    config.fast_path_config.mode = FastPathMode.FAST_PATH_WITH_FALLBACK
    config.fast_path_config.digikey_enabled = True
    config.fast_path_config.min_result_size = 10000
    
    return config