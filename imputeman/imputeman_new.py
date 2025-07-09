# imputeman.py
"""
Imputeman - AI-powered context-aware data imputation pipeline
Main orchestrator class for streaming SERP → Scrape → Extract operations
with optional DigiKey pre-processing
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
import statistics
import logging

from .core.entities import EntityToImpute, WhatToRetain
from .core.config import PipelineConfig, get_development_config, get_production_config
from .services import ServiceRegistry
from extracthero.schemes import ExtractOp

# Import DigiKey functionality
try:
    from imputemodule.imputemodule import Imputemodule
    from imputemodule.models import ImputeOp
    from brightdata.auto import scrape_url_async
    DIGIKEY_AVAILABLE = True
except ImportError:
    DIGIKEY_AVAILABLE = False
    ImputeOp = None


logger = logging.getLogger(__name__)


@dataclass
class ScrapeTimingMetrics:
    """Detailed timing metrics from scrape operations"""
    url: str
    request_sent_at: Optional[datetime] = None
    snapshot_id_received_at: Optional[datetime] = None
    snapshot_polled_at: Optional[List[datetime]] = None
    data_received_at: Optional[datetime] = None
    browser_warmed_at: Optional[datetime] = None
    
    def calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate scrape performance metrics"""
        metrics = {'url': self.url}
        
        try:
            def safe_datetime(value):
                if value is None:
                    return None
                if isinstance(value, list):
                    return value[-1] if value else None
                if isinstance(value, str):
                    try:
                        return datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except:
                        return None
                return value
            
            request_sent = safe_datetime(self.request_sent_at)
            snapshot_received = safe_datetime(self.snapshot_id_received_at)
            data_received = safe_datetime(self.data_received_at)
            
            # Basic timing metrics
            if request_sent and data_received:
                metrics['total_scrape_time'] = (data_received - request_sent).total_seconds()
            
            if request_sent and snapshot_received:
                metrics['request_to_snapshot'] = (snapshot_received - request_sent).total_seconds()
            
            # Enhanced polling analysis
            if isinstance(self.snapshot_polled_at, list) and self.snapshot_polled_at:
                polling_times = [safe_datetime(poll) for poll in self.snapshot_polled_at if poll]
                polling_times = [p for p in polling_times if p is not None]
                
                if polling_times:
                    first_poll = polling_times[0]
                    last_poll = polling_times[-1]
                    
                    metrics['poll_count'] = len(polling_times)
                    
                    if len(polling_times) > 1:
                        total_polling = (last_poll - first_poll).total_seconds()
                        metrics['total_polling_time'] = total_polling
                        metrics['avg_poll_interval'] = total_polling / (len(polling_times) - 1)
                    
                    if snapshot_received and first_poll:
                        metrics['snapshot_to_first_poll'] = (first_poll - snapshot_received).total_seconds()
                    
                    if last_poll and data_received:
                        metrics['last_poll_to_data'] = (data_received - last_poll).total_seconds()
                        
        except Exception as e:
            metrics['error'] = f"Error calculating scrape metrics: {e}"
            
        return metrics


@dataclass
class ExtractionTimingMetrics:
    """Detailed timing metrics from extraction operations"""
    url: str
    filter_start_time: Optional[float] = None
    parse_start_time: Optional[float] = None
    filter_generation_requested_at: Optional[datetime] = None
    filter_generation_completed_at: Optional[datetime] = None
    parse_generation_requested_at: Optional[datetime] = None
    parse_generation_completed_at: Optional[datetime] = None
    parse_converttodict_start_at: Optional[datetime] = None
    parse_converttodict_end_at: Optional[datetime] = None
    
    def calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate extraction performance metrics"""
        metrics = {'url': self.url}
        
        try:
            # Filter timing
            if self.filter_generation_requested_at and self.filter_generation_completed_at:
                filter_duration = (self.filter_generation_completed_at - self.filter_generation_requested_at).total_seconds()
                metrics['filter_duration'] = filter_duration
            
            # Parse timing
            if self.parse_generation_requested_at and self.parse_generation_completed_at:
                parse_duration = (self.parse_generation_completed_at - self.parse_generation_requested_at).total_seconds()
                metrics['parse_duration'] = parse_duration
            
            # ConvertToDict timing (potential blocking operation)
            if self.parse_converttodict_start_at and self.parse_converttodict_end_at:
                converttodict_duration = (self.parse_converttodict_end_at - self.parse_converttodict_start_at).total_seconds()
                metrics['converttodict_duration'] = converttodict_duration
                
                # Flag if potentially blocking
                if converttodict_duration > 0.1:
                    metrics['converttodict_blocking_risk'] = 'high'
                elif converttodict_duration > 0.05:
                    metrics['converttodict_blocking_risk'] = 'medium'
                else:
                    metrics['converttodict_blocking_risk'] = 'low'
            
            # Phase coordination timing
            if isinstance(self.filter_start_time, (int, float)) and isinstance(self.parse_start_time, (int, float)):
                filter_to_parse_time = self.parse_start_time - self.filter_start_time
                metrics['filter_to_parse_time'] = filter_to_parse_time
                
        except Exception as e:
            metrics['error'] = f"Error calculating extraction metrics: {e}"
            
        return metrics


@dataclass
class DigikeyResult:
    """Result from DigiKey pre-processing phase"""
    success: bool
    query: str
    data: Any = None
    url: str = ""
    elapsed_time: float = 0.0
    error: Optional[str] = None
    hits: int = 0


@dataclass
class ImputemanResult:
    """Comprehensive result from Imputeman pipeline execution"""
    success: bool
    entity: EntityToImpute
    schema: List[WhatToRetain]
    
    # Core results
    serp_urls: List[str] = field(default_factory=list)
    extract_results: Dict[str, ExtractOp] = field(default_factory=dict)
    
    # DigiKey pre-processing results
    digikey_result: Optional[DigikeyResult] = None
    
    # Performance metrics
    total_elapsed_time: float = 0.0
    serp_duration: float = 0.0
    digikey_duration: float = 0.0
    scrape_metrics: List[ScrapeTimingMetrics] = field(default_factory=list)
    extraction_metrics: List[ExtractionTimingMetrics] = field(default_factory=list)
    
    # Cost tracking
    total_scrape_cost: float = 0.0
    total_extraction_cost: float = 0.0
    digikey_cost: float = 0.0
    
    # Success metrics
    successful_scrapes: int = 0
    successful_extractions: int = 0
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    
    @property
    def total_cost(self) -> float:
        return self.total_scrape_cost + self.total_extraction_cost + self.digikey_cost
    
    @property
    def success_rate(self) -> float:
        total_sources = len(self.serp_urls) + (1 if self.digikey_result and self.digikey_result.success else 0)
        if total_sources == 0:
            return 0.0
        return self.successful_extractions / total_sources
    
    @property
    def time_to_first_result(self) -> Optional[float]:
        """Time to first successful extraction (including DigiKey)"""
        if not self.extract_results and not (self.digikey_result and self.digikey_result.success):
            return None
        
        times = []
        
        # Include DigiKey time if successful
        if self.digikey_result and self.digikey_result.success:
            times.append(self.digikey_duration)
        
        # Include extraction times
        for result in self.extract_results.values():
            if result.success and result.elapsed_time:
                times.append(result.elapsed_time)
        
        return min(times) if times else None
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance analysis"""
        summary = {
            'execution': {
                'total_time': self.total_elapsed_time,
                'serp_time': self.serp_duration,
                'digikey_time': self.digikey_duration,
                'success_rate': self.success_rate,
                'time_to_first_result': self.time_to_first_result
            },
            'costs': {
                'total_cost': self.total_cost,
                'scrape_cost': self.total_scrape_cost,
                'extraction_cost': self.total_extraction_cost,
                'digikey_cost': self.digikey_cost,
                'cost_per_result': self.total_cost / max(self.successful_extractions + (1 if self.digikey_result and self.digikey_result.success else 0), 1)
            },
            'throughput': {
                'urls_found': len(self.serp_urls),
                'successful_scrapes': self.successful_scrapes,
                'successful_extractions': self.successful_extractions,
                'digikey_success': self.digikey_result.success if self.digikey_result else False,
                'scrape_success_rate': self.successful_scrapes / max(len(self.serp_urls), 1) if self.serp_urls else 0,
                'extraction_success_rate': self.successful_extractions / max(self.successful_scrapes, 1) if self.successful_scrapes > 0 else 0
            }
        }
        
        # Scrape performance analysis
        if self.scrape_metrics:
            scrape_times = []
            poll_counts = []
            
            for metric in self.scrape_metrics:
                perf = metric.calculate_performance_metrics()
                if 'total_scrape_time' in perf:
                    scrape_times.append(perf['total_scrape_time'])
                if 'poll_count' in perf:
                    poll_counts.append(perf['poll_count'])
            
            if scrape_times:
                summary['scrape_performance'] = {
                    'avg_scrape_time': statistics.mean(scrape_times),
                    'min_scrape_time': min(scrape_times),
                    'max_scrape_time': max(scrape_times),
                    'std_scrape_time': statistics.stdev(scrape_times) if len(scrape_times) > 1 else 0
                }
            
            if poll_counts:
                summary['polling_analysis'] = {
                    'avg_polls': statistics.mean(poll_counts),
                    'max_polls': max(poll_counts),
                    'min_polls': min(poll_counts)
                }
        
        # Extraction performance analysis
        if self.extraction_metrics:
            filter_times = []
            parse_times = []
            converttodict_times = []
            
            for metric in self.extraction_metrics:
                perf = metric.calculate_performance_metrics()
                if 'filter_duration' in perf:
                    filter_times.append(perf['filter_duration'])
                if 'parse_duration' in perf:
                    parse_times.append(perf['parse_duration'])
                if 'converttodict_duration' in perf:
                    converttodict_times.append(perf['converttodict_duration'])
            
            if filter_times or parse_times:
                summary['extraction_performance'] = {}
                
                if filter_times:
                    summary['extraction_performance']['filter'] = {
                        'avg_time': statistics.mean(filter_times),
                        'max_time': max(filter_times)
                    }
                
                if parse_times:
                    summary['extraction_performance']['parse'] = {
                        'avg_time': statistics.mean(parse_times),
                        'max_time': max(parse_times)
                    }
                
                if converttodict_times:
                    max_converttodict = max(converttodict_times)
                    summary['extraction_performance']['converttodict'] = {
                        'avg_time': statistics.mean(converttodict_times),
                        'max_time': max_converttodict,
                        'blocking_risk': 'high' if max_converttodict > 0.1 else 'medium' if max_converttodict > 0.05 else 'low'
                    }
        
        return summary


class EeImputeModule:
    """
    Embedded DigiKey scraping functionality for electronics components.
    Based on the original EeImputeModule but simplified for integration.
    """
    
    def __init__(self, bright_token: Optional[str] = None):
        self.bright_token = bright_token
        self._scrape_sem = asyncio.Semaphore(3)  # Limit concurrent DigiKey scrapes
    
    async def scrape_digikey_query_async(
        self,
        query: str,
        *,
        poll_interval: int = 8,
        poll_timeout: int = 600,
        fallback_to_browser_api: bool = False,
    ) -> DigikeyResult:
        """
        Async function to scrape data from DigiKey for a specific query.
        
        Args:
            query: The search query or part number to search for on DigiKey
            poll_interval: Interval in seconds to poll for scrape results
            poll_timeout: Timeout in seconds for the scrape operation
            fallback_to_browser_api: Whether to fallback to browser API if initial scrape fails
            
        Returns:
            DigikeyResult: The result of the DigiKey scrape operation
        """
        url = f'https://www.digikey.com/en/products/result?keywords={query}'
        start_time = time.time()
        
        try:
            if not DIGIKEY_AVAILABLE:
                return DigikeyResult(
                    success=False,
                    query=query,
                    url=url,
                    error="DigiKey modules not available",
                    elapsed_time=time.time() - start_time
                )
            
            async with self._scrape_sem:
                result = await scrape_url_async(
                    url,
                    bearer_token=self.bright_token,
                    poll_interval=poll_interval,
                    poll_timeout=poll_timeout,
                    fallback_to_browser_api=fallback_to_browser_api,
                )
            
            elapsed_time = time.time() - start_time
            
            if result and hasattr(result, 'data') and result.data:
                hits = len(result.data) if isinstance(result.data, list) else 1
                return DigikeyResult(
                    success=True,
                    query=query,
                    data=result.data,
                    url=url,
                    elapsed_time=elapsed_time,
                    hits=hits
                )
            else:
                return DigikeyResult(
                    success=False,
                    query=query,
                    url=url,
                    elapsed_time=elapsed_time,
                    error="No data returned from DigiKey"
                )
                
        except Exception as e:
            return DigikeyResult(
                success=False,
                query=query,
                url=url,
                elapsed_time=time.time() - start_time,
                error=f"DigiKey scrape failed: {str(e)}"
            )


class Imputeman:
    """
    Main Imputeman orchestrator for AI-powered data imputation pipeline.
    
    Provides streaming parallelization with optional DigiKey pre-processing:
    1. Optional DigiKey direct scrape (for electronics components)
    2. SERP → Scrape → Extract (streaming pipeline)
    
    Includes comprehensive timing analysis and performance metrics.
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or get_development_config()
        self.registry = ServiceRegistry(self.config)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize DigiKey module if available
        self.digikey_module = None
        if DIGIKEY_AVAILABLE:
            bright_token = getattr(self.config.scrape_config, 'bearer_token', None)
            self.digikey_module = EeImputeModule(bright_token)
    
    async def run(
        self,
        entity: EntityToImpute,
        schema: List[WhatToRetain],
        max_urls: int = None,
        enable_streaming: bool = True,
        capture_detailed_metrics: bool = True,
        enable_digikey_preprocessing: bool = False,
        digikey_only: bool = False
    ) -> ImputemanResult:
        """
        Execute complete Imputeman pipeline with optional DigiKey pre-processing.
        
        Args:
            entity: Entity to search and extract data for
            schema: List of fields to extract
            max_urls: Maximum URLs to process (uses config default if None)
            enable_streaming: If True, use streaming extraction (recommended)
            capture_detailed_metrics: If True, capture comprehensive timing data
            enable_digikey_preprocessing: If True, try DigiKey direct scrape first
            digikey_only: If True, only run DigiKey scrape (skip SERP pipeline)
            
        Returns:
            ImputemanResult with extracted data, metrics, and performance analysis
        """
        start_time = time.time()
        max_urls = max_urls or self.config.serp_config.top_k_results
        
        result = ImputemanResult(
            success=False,
            entity=entity,
            schema=schema
        )
        
        self.logger.info(f"🚀 Starting Imputeman pipeline for: {entity.name}")
        
        try:
            # Phase 0: Optional DigiKey Pre-processing
            if enable_digikey_preprocessing or digikey_only:
                await self._execute_digikey_preprocessing(result)
            
            # Early exit if DigiKey-only mode
            if digikey_only:
                result.total_elapsed_time = time.time() - start_time
                result.success = result.digikey_result and result.digikey_result.success
                self._log_execution_summary(result)
                return result
            
            # Phase 1: SERP - Find URLs (skip if DigiKey succeeded and we have enough data)
            skip_serp = (enable_digikey_preprocessing and 
                        result.digikey_result and 
                        result.digikey_result.success and 
                        result.digikey_result.hits > 0)
            
            if not skip_serp:
                serp_start = time.time()
                self.logger.info("🔍 Executing SERP phase...")
                
                serp_result = await self.registry.serp.search(entity.name, top_k=max_urls)
                
                result.serp_duration = time.time() - serp_start
                
                if not serp_result.success or not serp_result.links:
                    result.errors.append(f"SERP failed: {serp_result.metadata}")
                    self.logger.error(f"SERP phase failed: {serp_result.metadata}")
                    
                    # If DigiKey succeeded, still consider this a success
                    if result.digikey_result and result.digikey_result.success:
                        result.success = True
                        result.total_elapsed_time = time.time() - start_time
                        self._log_execution_summary(result)
                        return result
                    else:
                        result.total_elapsed_time = time.time() - start_time
                        return result
                
                result.serp_urls = serp_result.links[:max_urls]
                self.logger.info(f"✅ Found {len(result.serp_urls)} URLs in {result.serp_duration:.2f}s")
                
                # Phase 2: Streaming Scrape + Extract
                if enable_streaming:
                    await self._execute_streaming_pipeline(result, capture_detailed_metrics)
                else:
                    await self._execute_batch_pipeline(result, capture_detailed_metrics)
            else:
                self.logger.info("⚡ Skipping SERP phase - DigiKey preprocessing provided sufficient data")
            
            # Phase 3: Finalize results
            result.total_elapsed_time = time.time() - start_time
            result.success = (result.successful_extractions > 0 or 
                            (result.digikey_result and result.digikey_result.success))
            
            # Log summary
            self._log_execution_summary(result)
            
            return result
            
        except Exception as e:
            result.errors.append(f"Pipeline execution failed: {str(e)}")
            result.total_elapsed_time = time.time() - start_time
            self.logger.error(f"❌ Pipeline execution failed: {e}", exc_info=True)
            return result
        
        finally:
            await self.registry.close_all()
    
    async def _execute_digikey_preprocessing(self, result: ImputemanResult):
        """Execute DigiKey pre-processing phase"""
        
        if not self.digikey_module:
            result.errors.append("DigiKey module not available")
            self.logger.warning("⚠️ DigiKey preprocessing requested but module not available")
            return
        
        self.logger.info("🔧 Executing DigiKey pre-processing...")
        digikey_start = time.time()
        
        digikey_result = await self.digikey_module.scrape_digikey_query_async(
            query=result.entity.name,
            poll_interval=8,
            poll_timeout=300,
            fallback_to_browser_api=True
        )
        
        result.digikey_duration = time.time() - digikey_start
        result.digikey_result = digikey_result
        
        if digikey_result.success:
            self.logger.info(f"✅ DigiKey preprocessing successful: {digikey_result.hits} hits in {digikey_result.elapsed_time:.2f}s")
            # TODO: Add DigiKey cost tracking if available
        else:
            self.logger.warning(f"⚠️ DigiKey preprocessing failed: {digikey_result.error}")
    
    async def _execute_streaming_pipeline(self, result: ImputemanResult, capture_metrics: bool):
        """Execute streaming pipeline: extract immediately as each scrape completes"""
        
        self.logger.info("⚡ Executing streaming scrape + extract pipeline")
        
        # Start all scrapes concurrently
        scrape_tasks = {}
        for url in result.serp_urls:
            task = asyncio.create_task(self._scrape_with_metrics(url, capture_metrics))
            scrape_tasks[task] = url
        
        # Process as each scrape completes (streaming!)
        pending_tasks = set(scrape_tasks.keys())
        
        while pending_tasks:
            done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            
            for completed_task in done:
                url = scrape_tasks[completed_task]
                
                try:
                    scrape_result, scrape_metrics = await completed_task
                    
                    if capture_metrics and scrape_metrics:
                        result.scrape_metrics.append(scrape_metrics)
                    
                    # Track scrape cost
                    if scrape_result and url in scrape_result:
                        scrape_cost = getattr(scrape_result[url], 'cost', 0) or 0
                        result.total_scrape_cost += scrape_cost
                    
                    # Check if scrape successful
                    if scrape_result and any(r.status == "ready" for r in scrape_result.values()):
                        result.successful_scrapes += 1
                        self.logger.info(f"✅ Scrape completed for {url[:40]}... - Starting extraction IMMEDIATELY")
                        
                        # Immediately start extraction
                        extract_result, extract_metrics = await self._extract_with_metrics(
                            scrape_result, result.schema, url, capture_metrics
                        )
                        
                        if capture_metrics and extract_metrics:
                            result.extraction_metrics.append(extract_metrics)
                        
                        if extract_result and any(r.success for r in extract_result.values()):
                            result.successful_extractions += 1
                            result.extract_results.update(extract_result)
                            
                            # Track extraction cost
                            for extract_op in extract_result.values():
                                if extract_op.usage and 'total_cost' in extract_op.usage:
                                    result.total_extraction_cost += extract_op.usage['total_cost']
                            
                            self.logger.info(f"✅ Extraction completed for {url[:40]}...")
                        else:
                            self.logger.warning(f"⚠️ Extraction failed for {url[:40]}...")
                    else:
                        self.logger.warning(f"⚠️ Scrape failed for {url[:40]}...")
                        
                except Exception as e:
                    result.errors.append(f"Processing failed for {url}: {str(e)}")
                    self.logger.error(f"❌ Processing failed for {url[:40]}...: {e}")
    
    async def _execute_batch_pipeline(self, result: ImputemanResult, capture_metrics: bool):
        """Execute batch pipeline: scrape all, then extract all"""
        
        self.logger.info("📦 Executing batch scrape + extract pipeline")
        
        # Phase 1: Scrape all URLs
        scrape_results = await self.registry.scraper.scrape_urls(result.serp_urls)
        
        for url, scrape_result in scrape_results.items():
            if scrape_result.status == "ready":
                result.successful_scrapes += 1
                scrape_cost = getattr(scrape_result, 'cost', 0) or 0
                result.total_scrape_cost += scrape_cost
        
        # Phase 2: Extract from all successful scrapes
        if scrape_results:
            extract_results = await self.registry.extractor.extract_from_scrapes(scrape_results, result.schema)
            
            for url, extract_result in extract_results.items():
                if extract_result.success:
                    result.successful_extractions += 1
                    result.extract_results[url] = extract_result
                    
                    if extract_result.usage and 'total_cost' in extract_result.usage:
                        result.total_extraction_cost += extract_result.usage['total_cost']
    
    async def _scrape_with_metrics(self, url: str, capture_metrics: bool):
        """Scrape URL and optionally capture detailed timing metrics"""
        
        scrape_result = await self.registry.scraper.scrape_urls([url])
        
        scrape_metrics = None
        if capture_metrics and url in scrape_result:
            result_obj = scrape_result[url]
            scrape_metrics = ScrapeTimingMetrics(
                url=url,
                request_sent_at=getattr(result_obj, 'request_sent_at', None),
                snapshot_id_received_at=getattr(result_obj, 'snapshot_id_received_at', None),
                snapshot_polled_at=getattr(result_obj, 'snapshot_polled_at', None),
                data_received_at=getattr(result_obj, 'data_received_at', None),
                browser_warmed_at=getattr(result_obj, 'browser_warmed_at', None)
            )
        
        return scrape_result, scrape_metrics
    
    async def _extract_with_metrics(self, scrape_result, schema, url: str, capture_metrics: bool):
        """Extract data and optionally capture detailed timing metrics"""
        
        extract_result = await self.registry.extractor.extract_from_scrapes(scrape_result, schema)
        
        extract_metrics = None
        if capture_metrics and url in extract_result:
            result_obj = extract_result[url]
            extract_metrics = ExtractionTimingMetrics(url=url)
            
            # Capture filter timing
            if hasattr(result_obj, 'filter_op') and result_obj.filter_op:
                extract_metrics.filter_start_time = getattr(result_obj.filter_op, 'start_time', None)
                
                if hasattr(result_obj.filter_op, 'generation_result') and result_obj.filter_op.generation_result:
                    gen_result = result_obj.filter_op.generation_result
                    if hasattr(gen_result, 'timestamps') and gen_result.timestamps:
                        extract_metrics.filter_generation_requested_at = gen_result.timestamps.generation_requested_at
                        extract_metrics.filter_generation_completed_at = gen_result.timestamps.generation_completed_at
            
            # Capture parse timing
            if hasattr(result_obj, 'parse_op') and result_obj.parse_op:
                extract_metrics.parse_start_time = getattr(result_obj.parse_op, 'start_time', None)
                
                if hasattr(result_obj.parse_op, 'generation_result') and result_obj.parse_op.generation_result:
                    gen_result = result_obj.parse_op.generation_result
                    if hasattr(gen_result, 'timestamps') and gen_result.timestamps:
                        extract_metrics.parse_generation_requested_at = gen_result.timestamps.generation_requested_at
                        extract_metrics.parse_generation_completed_at = gen_result.timestamps.generation_completed_at
                        extract_metrics.parse_converttodict_start_at = gen_result.timestamps.converttodict_start_at
                        extract_metrics.parse_converttodict_end_at = gen_result.timestamps.converttodict_end_at
        
        return extract_result, extract_metrics
    
    def _log_execution_summary(self, result: ImputemanResult):
        """Log comprehensive execution summary"""
        
        perf = result.get_performance_summary()
        
        self.logger.info("🎯 Imputeman Pipeline Results:")
        self.logger.info(f"   ✅ Overall Success: {result.success}")
        self.logger.info(f"   📊 Success Rate: {result.success_rate:.1%}")
        self.logger.info(f"   ⏱️ Total Duration: {result.total_elapsed_time:.2f}s")
        self.logger.info(f"   💰 Total Cost: ${result.total_cost:.4f}")
        
        # DigiKey results
        if result.digikey_result:
            dk_status = "✅ Success" if result.digikey_result.success else "❌ Failed"
            self.logger.info(f"   🔧 DigiKey: {dk_status} ({result.digikey_result.hits} hits in {result.digikey_duration:.2f}s)")
        
        # SERP pipeline results
        if result.serp_urls:
            self.logger.info(f"   🔗 SERP Pipeline: {len(result.serp_urls)} found → {result.successful_scrapes} scraped → {result.successful_extractions} extracted")
        
        if result.time_to_first_result:
            self.logger.info(f"   ⚡ Time to First Result: {result.time_to_first_result:.2f}s")
        
        # Performance insights
        if 'extraction_performance' in perf and 'converttodict' in perf['extraction_performance']:
            converttodict_info = perf['extraction_performance']['converttodict']
            if converttodict_info['blocking_risk'] == 'high':
                self.logger.warning(f"   ⚠️ ConvertToDict Blocking Risk: {converttodict_info['blocking_risk']} (max: {converttodict_info['max_time']:.3f}s)")
        
        if result.errors:
            self.logger.warning(f"   ⚠️ Errors Encountered: {len(result.errors)}")


# Convenience functions for easy usage
async def run_imputeman(
    entity: Union[str, EntityToImpute],
    schema: List[WhatToRetain],
    config: Optional[PipelineConfig] = None,
    **kwargs
) -> ImputemanResult:
    """
    Convenience function to run Imputeman pipeline
    
    Args:
        entity: Entity name (str) or EntityToImpute object
        schema: List of WhatToRetain specifications
        config: Pipeline configuration (uses development config if None)
        **kwargs: Additional arguments for Imputeman.run()
    
    Returns:
        ImputemanResult with extracted data and metrics
    """
    if isinstance(entity, str):
        entity = EntityToImpute(name=entity)
    
    config = config or get_development_config()
    imputeman = Imputeman(config)
    
    return await imputeman.run(entity, schema, **kwargs)


async def run_imputeman_digikey_only(
    entity: Union[str, EntityToImpute],
    schema: List[WhatToRetain],
    config: Optional[PipelineConfig] = None
) -> ImputemanResult:
    """
    Convenience function to run DigiKey-only pipeline
    
    Args:
        entity: Entity name (str) or EntityToImpute object
        schema: List of WhatToRetain specifications
        config: Pipeline configuration
    
    Returns:
        ImputemanResult with DigiKey data only
    """
    return await run_imputeman(
        entity,
        schema,
        config,
        enable_digikey_preprocessing=True,
        digikey_only=True
    )


def run_imputeman_sync(
    entity: Union[str, EntityToImpute],
    schema: List[WhatToRetain],
    config: Optional[PipelineConfig] = None,
    **kwargs
) -> ImputemanResult:
    """
    Synchronous wrapper for run_imputeman
    
    Args:
        entity: Entity name (str) or EntityToImpute object
        schema: List of WhatToRetain specifications  
        config: Pipeline configuration
        **kwargs: Additional arguments for Imputeman.run()
    
    Returns:
        ImputemanResult with extracted data and metrics
    """
    return asyncio.run(run_imputeman(entity, schema, config, **kwargs))


# Example usage
async def main():
    """Example usage of Imputeman with DigiKey preprocessing"""
    
    # Define what to extract
    schema = [
        WhatToRetain(name="component_type", desc="Type of electronic component"),
        WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
        WhatToRetain(name="package_type", desc="Physical package type")
    ]
    
    # Example 1: Full pipeline with DigiKey preprocessing
    result1 = await run_imputeman(
        entity="BAV99",
        schema=schema,
        max_urls=5,
        enable_streaming=True,
        enable_digikey_preprocessing=True,  # Try DigiKey first
        capture_detailed_metrics=True
    )
    
    print("=== Full Pipeline with DigiKey ===")
    print(f"Success: {result1.success}")
    print(f"DigiKey success: {result1.digikey_result.success if result1.digikey_result else False}")
    print(f"SERP extractions: {len(result1.extract_results)}")
    print(f"Total cost: ${result1.total_cost:.4f}")
    
    # Example 2: DigiKey-only pipeline
    result2 = await run_imputeman_digikey_only(
        entity="NTJD5121NT1G",
        schema=schema
    )
    
    print("\n=== DigiKey Only ===")
    print(f"Success: {result2.success}")
    print(f"DigiKey hits: {result2.digikey_result.hits if result2.digikey_result else 0}")
    print(f"Duration: {result2.total_elapsed_time:.2f}s")
    
    # Example 3: Traditional SERP pipeline (no DigiKey)
    result3 = await run_imputeman(
        entity="LM358",
        schema=schema,
        enable_digikey_preprocessing=False,  # Skip DigiKey
        enable_streaming=True
    )
    
    print("\n=== Traditional SERP Pipeline ===")
    print(f"Success: {result3.success}")
    print(f"URLs found: {len(result3.serp_urls)}")
    print(f"Successful extractions: {result3.successful_extractions}")


if __name__ == "__main__":
    asyncio.run(main())