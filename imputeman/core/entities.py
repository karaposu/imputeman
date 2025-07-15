# imputeman/core/entities.py
"""Core entity definitions for Imputeman"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
from brightdata.models import ScrapeResult

from extracthero import ExtractOp,ParseOp, FilterOp
from serpengine.schemes import SearchHit, SerpChannelOp, SerpEngineOp, UsageInfo


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
class ImputeResult:
    """Final result from the complete imputation pipeline"""
    entity: EntityToImpute
    schema: List[WhatToRetain]
    serp_result: Optional[SerpEngineOp] = None
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