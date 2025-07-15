# imputeman/models.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from serpengine.schemes import SerpEngineOp, SearchHit
from brightdata.models import ScrapeResult
from extracthero.schemes import ExtractOp, WhatToRetain
from enum import Enum
from datetime import datetime
from serpengine.schemes import SerpEngineOp 


@dataclass
class EntityToImpute:    
    name: str   
    identifier_context: Optional[str] = None   
    impute_task_purpose: Optional[str] = None


class PipelineStatus(Enum):
    """Pipeline execution status for streaming parallelization"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class StatusDetails:
    """Detailed status information for streaming parallelization"""
    current_status: PipelineStatus = PipelineStatus.INITIALIZING  # Fix: provide default value
    
    # Discovery phase
    serp_completed: bool = False
    urls_found: int = 0
    
    # Parallel execution counters
    urls_scraping: int = 0      # Currently being scraped
    urls_scraped: int = 0       # Successfully scraped
    urls_extracting: int = 0    # Currently being extracted  
    urls_extracted: int = 0     # Successfully extracted
    urls_failed: int = 0        # Failed (scrape or extract)
    
    # Progress tracking
    errors_count: int = 0
    last_update: Optional[datetime] = None
    
    def __str__(self) -> str:
        if self.current_status == PipelineStatus.INITIALIZING:
            return "🚀 Initializing pipeline..."
        elif self.current_status == PipelineStatus.RUNNING:
            if not self.serp_completed:
                return "🔍 Searching for URLs..."
            else:
                # Show parallel activities
                active_work = []
                if self.urls_scraping > 0:
                    active_work.append(f"{self.urls_scraping} scraping")
                if self.urls_extracting > 0:
                    active_work.append(f"{self.urls_extracting} extracting")
                
                completed = f"{self.urls_extracted} extracted"
                if self.urls_failed > 0:
                    completed += f", {self.urls_failed} failed"
                
                if active_work:
                    return f"⚡ {', '.join(active_work)} | ✅ {completed} of {self.urls_found}"
                else:
                    return f"🔄 Processing... | ✅ {completed} of {self.urls_found}"
        elif self.current_status == PipelineStatus.FINISHED:
            return f"✅ Finished: {self.urls_extracted}/{self.urls_found} successful extractions"
        elif self.current_status == PipelineStatus.FAILED:
            return f"❌ Failed: {self.urls_extracted}/{self.urls_found} completed, {self.errors_count} errors"
        elif self.current_status == PipelineStatus.CANCELLED:
            return f"🛑 Cancelled: {self.urls_extracted}/{self.urls_found} completed before cancellation"
        else:
            return f"📊 Status: {self.current_status.value}"


@dataclass
class PerformanceMetrics:
    """Comprehensive performance analysis"""
    # Timing
    total_elapsed_time: float = 0.0
    serp_duration: float = 0.0
    avg_scrape_time: Optional[float] = None
    avg_extraction_time: Optional[float] = None
    time_to_first_result: Optional[float] = None
    time_saved_by_streaming: float = 0.0  # vs batch processing
    
    # Throughput
    urls_found: int = 0
    successful_scrapes: int = 0
    successful_extractions: int = 0
    scrape_success_rate: float = 0.0
    extraction_success_rate: float = 0.0
    overall_success_rate: float = 0.0
    
    # Advanced metrics
    scrape_performance: Optional[Dict[str, float]] = None  # min, max, std times
    extraction_performance: Optional[Dict[str, Any]] = None  # filter/parse breakdown
    polling_analysis: Optional[Dict[str, float]] = None  # BrightData polling stats


@dataclass
class CostBreakdown:
    """Detailed cost tracking"""
    total_cost: float = 0.0
    serp_cost: float = 0.0
    scrape_cost: float = 0.0
    extraction_cost: float = 0.0
    cost_per_result: float = 0.0
    cost_per_successful_extraction: float = 0.0
    
    # Cost efficiency metrics
    most_expensive_url: Optional[str] = None
    cheapest_successful_extraction: Optional[float] = None
    cost_threshold_exceeded: bool = False


@dataclass
class ImputeOp:
    """
    Enhanced Imputeman operation result with real-time status tracking,
    comprehensive metrics, and detailed cost analysis.
    
    Combines the best of real-time monitoring with final result analysis.
    """
    # Core identification
    query: str
    schema: List[WhatToRetain]
    
    # Status tracking (NEW!)
    success: bool = False
    status: PipelineStatus = PipelineStatus.INITIALIZING
    status_details: StatusDetails = field(default_factory=StatusDetails)  # Now works properly
    
    # Execution results
    search_op: Optional[SerpEngineOp] = None
    urls: List[str] = field(default_factory=list)
    scrape_results: Dict[str, ScrapeResult] = field(default_factory=dict)
    extract_results: Dict[str, ExtractOp] = field(default_factory=dict)
    
    # Final content
    content: Optional[Dict[str, Any]] = None
    
    # Comprehensive metrics (ENHANCED!)
    performance: PerformanceMetrics = field(default_factory=PerformanceMetrics)
    costs: CostBreakdown = field(default_factory=CostBreakdown)
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    def update_status(self, new_status: PipelineStatus, details: str = ""):
        """Update pipeline status with timestamp"""
        self.status = new_status
        self.status_details.current_status = new_status
        self.status_details.last_update = datetime.now()
       
    
    def start_running(self):
        """Mark pipeline as running (after initialization)"""
        self.update_status(PipelineStatus.RUNNING, "Pipeline started")
    
    def mark_serp_completed(self, urls_found: int):
        """Mark SERP search as completed"""
        self.status_details.serp_completed = True
        self.status_details.urls_found = urls_found
      
    
    def mark_url_scraping(self, url: str):
        """Mark URL as currently being scraped"""
        self.status_details.urls_scraping += 1
       
    def mark_url_scraped(self, url: str, success: bool):
        """Mark URL as scraped (success or failure)"""
        self.status_details.urls_scraping = max(0, self.status_details.urls_scraping - 1)
        if success:
            self.status_details.urls_scraped += 1
          
        else:
            self.status_details.urls_failed += 1
          
        self.performance.successful_scrapes = self.status_details.urls_scraped
    
    def mark_url_extracting(self, url: str):
        """Mark URL as currently being extracted"""
        self.status_details.urls_extracting += 1
       
    def mark_url_extracted(self, url: str, success: bool):
        """Mark URL as extracted (success or failure)"""
        self.status_details.urls_extracting = max(0, self.status_details.urls_extracting - 1)
        if success:
            self.status_details.urls_extracted += 1
          
        else:
            self.status_details.urls_failed += 1
           
        self.performance.successful_extractions = self.status_details.urls_extracted
    
    def calculate_final_metrics(self):
        """Calculate final performance and cost metrics"""
        # Performance metrics
        self.performance.urls_found = len(self.urls)
        if self.performance.urls_found > 0:
            self.performance.scrape_success_rate = self.performance.successful_scrapes / self.performance.urls_found
            self.performance.overall_success_rate = self.performance.successful_extractions / self.performance.urls_found
        
        if self.performance.successful_scrapes > 0:
            self.performance.extraction_success_rate = self.performance.successful_extractions / self.performance.successful_scrapes
        
        # Cost metrics
        scrape_costs = [r.cost for r in self.scrape_results.values() if hasattr(r, 'cost') and r.cost]
        extract_costs = []
        for extract_op in self.extract_results.values():
            if extract_op.usage and 'total_cost' in extract_op.usage:
                extract_costs.append(extract_op.usage['total_cost'])
        
        self.costs.scrape_cost = sum(scrape_costs)
        self.costs.extraction_cost = sum(extract_costs)
        self.costs.total_cost = self.costs.scrape_cost + self.costs.extraction_cost
        
        if self.performance.successful_extractions > 0:
            self.costs.cost_per_successful_extraction = self.costs.total_cost / self.performance.successful_extractions
    
    def finalize(self, success: bool = None):
        """Mark pipeline as completed and calculate final metrics"""
        if success is not None:
            self.success = success
        else:
            self.success = self.performance.successful_extractions > 0
        
        self.status = PipelineStatus.FINISHED if self.success else PipelineStatus.FAILED
        self.completed_at = datetime.now()
        self.calculate_final_metrics()
        
        # Update final status details
        self.status_details.current_status = self.status
        if self.performance.total_elapsed_time == 0.0:
            self.performance.total_elapsed_time = (self.completed_at - self.created_at).total_seconds()
    
    def get_live_summary(self) -> str:
        """Get real-time summary for monitoring"""
        return (f"🎯 {self.query} | "
                f"{self.status_details} | "
                f"⏱️ {self.performance.total_elapsed_time:.1f}s | "
                f"💰 ${self.costs.total_cost:.3f}")

    def __str__(self) -> str:
        return self.get_live_summary()