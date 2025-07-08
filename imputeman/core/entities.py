# imputeman/core/entities.py
"""Core entity definitions for Imputeman"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
from brightdata.models import ScrapeResult

from extracthero import ExtractOp,ParseOp, FilterOp


@dataclass
class WhatToRetain:
    """Schema definition for what data to extract - compatible with extracthero"""
    name: str
    desc: str
    example: Optional[str] = None


@dataclass
class EntityToImpute:
    """Entity that needs data imputation"""
    name: str
    identifier_context: Optional[str] = None
    impute_task_purpose: Optional[str] = None
    
    def __post_init__(self):
        if not self.name.strip():
            raise ValueError("Entity name cannot be empty")


@dataclass
class SerpResult:
    """Result from search engine operation"""
    query: str
    links: List[str]
    total_results: int
    search_engine: str
    elapsed_time: float
    success: bool
    metadata: Dict[str, Any] = field(default_factory=dict)


# @dataclass
# class ScrapeResult:
#     """Result from web scraping operation"""
#     url: str
#     data: Optional[str]
#     status: str  # "ready", "failed", "timeout", etc.
#     html_char_size: Optional[int] = None
#     row_count: Optional[int] = None
#     field_count: Optional[int] = None
#     cost: float = 0.0
#     elapsed_time: float = 0.0
#     error_message: Optional[str] = None
#     metadata: Dict[str, Any] = field(default_factory=dict)


# @dataclass
# class ExtractResult:
#     """Result from data extraction operation"""
#     url: str
#     content: Optional[Dict[str, Any]]
#     confidence_score: float = 0.0
#     tokens_used: int = 0
#     cost: float = 0.0
#     elapsed_time: float = 0.0
#     extraction_method: str = "default"
#     success: bool = False
#     error_message: Optional[str] = None
#     metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ImputeResult:
    """Final result from the complete imputation pipeline"""
    entity: EntityToImpute
    schema: List[WhatToRetain]
    serp_result: Optional[SerpResult] = None
    scrape_results: Dict[str, ScrapeResult] = field(default_factory=dict)
    extract_results: Dict[str, ExtractOp] = field(default_factory=dict)
    final_data: Dict[str, Any] = field(default_factory=dict)
    total_cost: float = 0.0
    total_elapsed_time: float = 0.0
    success: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def successful_extractions(self) -> int:
        """Count of successful extractions"""
        return sum(1 for result in self.extract_results.values() if result.success)
    
    @property
    def total_urls_scraped(self) -> int:
        """Total number of URLs that were scraped"""
        return len(self.scrape_results)
    
    @property
    def successful_scrapes(self) -> int:
        """Count of successful scrapes"""
        return sum(1 for result in self.scrape_results.values() if result.status == "ready")


@dataclass
class PipelineStageResult:
    """Generic result wrapper for pipeline stages"""
    stage_name: str
    data: Any
    success: bool
    elapsed_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    retry_count: int = 0