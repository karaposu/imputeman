# imputeman/core/config.py
"""Configuration classes for Imputeman pipeline stages"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import os


@dataclass
class SerpConfig:
    """Configuration for SERP/search tasks"""
    max_retries: int = 3
    retry_delay_seconds: int = 2
    timeout_seconds: float = 30.0
    rate_limit_per_minute: Optional[int] = None
    top_k_results: int = 10
    search_engines: list = field(default_factory=lambda: ["google", "bing"])
    api_key: Optional[str] = None
    
    def __post_init__(self):
        if not self.api_key:
            self.api_key = os.getenv("SERP_API_KEY")


@dataclass  
class ScrapeConfig:
    """Configuration for web scraping tasks"""
    max_retries: int = 3
    retry_delay_seconds: int = 5
    timeout_seconds: float = 60.0
    concurrent_limit: int = 500
    rate_limit_per_minute: Optional[int] = 60
    use_browser_fallback: bool = True
    max_cost_threshold: float = 100.0  # Dollar amount
    bearer_token: Optional[str] = None
    poll_interval: float = 10.0
    poll_timeout: float = 120.0
    
    def __post_init__(self):
        if not self.bearer_token:
            self.bearer_token = os.getenv("BRIGHT_DATA_TOKEN")


@dataclass
class ExtractConfig:
    """Configuration for data extraction tasks"""
    max_retries: int = 2
    retry_delay_seconds: int = 3
    timeout_seconds: float = 120.0
    confidence_threshold: float = 0.7
    max_tokens: int = 4000
    extraction_model: str = "gpt-4"
    fallback_model: str = "gpt-3.5-turbo"
    api_key: Optional[str] = None
    
    def __post_init__(self):
        if not self.api_key:
            self.api_key = os.getenv("OPENAI_API_KEY")


@dataclass
class BudgetScrapeConfig(ScrapeConfig):
    """Cheaper scraping configuration for cost-conscious workflows"""
    concurrent_limit: int = 2
    rate_limit_per_minute: Optional[int] = 30
    timeout_seconds: float = 30.0
    use_browser_fallback: bool = False
    max_cost_threshold: float = 20.0


@dataclass
class PipelineConfig:
    """Master configuration for the entire pipeline"""
    serp_config: SerpConfig = field(default_factory=SerpConfig)
    scrape_config: ScrapeConfig = field(default_factory=ScrapeConfig)
    budget_scrape_config: BudgetScrapeConfig = field(default_factory=BudgetScrapeConfig)
    extract_config: ExtractConfig = field(default_factory=ExtractConfig)
    
    # Global pipeline settings
    enable_caching: bool = True
    cache_duration_hours: int = 24
    max_total_cost: float = 200.0
    enable_cost_monitoring: bool = True
    log_level: str = "INFO"
    
    # Conditional flow settings
    cost_threshold_for_budget_mode: float = 100.0
    min_successful_extractions: int = 1
    quality_threshold: float = 0.8


def get_default_config() -> PipelineConfig:
    """Get default pipeline configuration"""
    return PipelineConfig()


def get_development_config() -> PipelineConfig:
    """Get configuration optimized for development/testing"""
    config = PipelineConfig()
    
    # Reduce limits for development
    config.serp_config.top_k_results = 3
    config.scrape_config.concurrent_limit = 2
    config.scrape_config.timeout_seconds = 30.0
    config.extract_config.max_tokens = 2000
    config.max_total_cost = 50.0
    
    return config


def get_production_config() -> PipelineConfig:
    """Get configuration optimized for production"""
    config = PipelineConfig()
    
    # Production optimizations
    config.serp_config.top_k_results = 15
    config.scrape_config.concurrent_limit = 10
    config.scrape_config.rate_limit_per_minute = 120
    config.extract_config.max_tokens = 8000
    config.max_total_cost = 500.0
    
    return config