# flows/streaming_flow.py
"""
Streaming Pipeline Flow - True cross-module parallelization
Extraction starts immediately when each scrape completes (no waiting for all scrapes)
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import logging

from ..core.entities import EntityToImpute, WhatToRetain
from ..services import ServiceRegistry
from extracthero import ExtractOp
from brightdata.models import ScrapeResult


logger = logging.getLogger(__name__)


@dataclass
class StreamingMetrics:
    """Comprehensive metrics for streaming pipeline execution"""
    
    # Phase timings
    serp_start_time: float = 0.0
    serp_end_time: float = 0.0
    pipeline_start_time: float = 0.0
    pipeline_end_time: float = 0.0
    
    # Per-URL timings
    scrape_start_times: Dict[str, float] = field(default_factory=dict)
    scrape_end_times: Dict[str, float] = field(default_factory=dict)
    extract_start_times: Dict[str, float] = field(default_factory=dict)
    extract_end_times: Dict[str, float] = field(default_factory=dict)
    
    # Results tracking
    urls_found: int = 0
    scrapes_attempted: int = 0
    scrapes_successful: int = 0
    extractions_attempted: int = 0
    extractions_successful: int = 0
    
    # Error tracking
    scrape_errors: Dict[str, str] = field(default_factory=dict)
    extract_errors: Dict[str, str] = field(default_factory=dict)
    
    # Cost tracking
    total_scrape_cost: float = 0.0
    total_extract_cost: float = 0.0
    
    def calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate derived performance metrics"""
        
        metrics = {
            'total_duration': self.pipeline_end_time - self.pipeline_start_time,
            'serp_duration': self.serp_end_time - self.serp_start_time,
            'total_cost': self.total_scrape_cost + self.total_extract_cost,
            'success_rates': {
                'scrape_success_rate': self.scrapes_successful / max(self.scrapes_attempted, 1),
                'extract_success_rate': self.extractions_successful / max(self.extractions_attempted, 1),
                'overall_success_rate': self.extractions_successful / max(self.urls_found, 1)
            }
        }
        
        # Calculate individual URL processing times
        if self.scrape_start_times and self.extract_end_times:
            url_processing_times = {}
            for url in self.scrape_start_times:
                if url in self.extract_end_times:
                    total_time = self.extract_end_times[url] - self.scrape_start_times[url]
                    url_processing_times[url] = total_time
            
            if url_processing_times:
                metrics['url_processing_times'] = url_processing_times
                metrics['avg_url_processing_time'] = sum(url_processing_times.values()) / len(url_processing_times)
                metrics['fastest_url_time'] = min(url_processing_times.values())
                metrics['slowest_url_time'] = max(url_processing_times.values())
        
        # Analyze streaming effectiveness
        if self.scrape_end_times and self.extract_start_times:
            scrape_to_extract_delays = {}
            for url in self.scrape_end_times:
                if url in self.extract_start_times:
                    delay = self.extract_start_times[url] - self.scrape_end_times[url]
                    scrape_to_extract_delays[url] = delay
            
            if scrape_to_extract_delays:
                metrics['scrape_to_extract_delays'] = scrape_to_extract_delays