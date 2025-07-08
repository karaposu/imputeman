# smoke_tests/test_10_streaming_pipeline_debug.py
"""
Streaming Pipeline Debug Test - Detailed timing analysis
Analyzes timing logs from scraping and extraction to identify blocking operations

python -m imputeman.smoke_tests.test_10_streaming_pipeline_debug
"""

import asyncio
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import statistics

from ..core.entities import EntityToImpute, WhatToRetain
from ..core.config import get_development_config
from ..services import ServiceRegistry


@dataclass
class ScrapeTimingData:
    """Detailed timing data from scrape operations"""
    url: str
    request_sent_at: Optional[datetime] = None
    snapshot_id_received_at: Optional[datetime] = None
    snapshot_polled_at: Optional[datetime] = None
    data_received_at: Optional[datetime] = None
    browser_warmed_at: Optional[datetime] = None
    
    def calculate_durations(self) -> Dict[str, float]:
        """Calculate duration between timing phases"""
        durations = {}
        
        try:
            # Helper function to safely extract datetime from various formats
            def safe_datetime(value):
                if value is None:
                    return None
                if isinstance(value, list):
                    # Take the last element if it's a list (most recent timestamp)
                    value = value[-1] if value else None
                if isinstance(value, str):
                    # Try to parse string datetime
                    try:
                        from datetime import datetime
                        return datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except:
                        return None
                return value
            
            # Safely extract datetime objects
            request_sent = safe_datetime(self.request_sent_at)
            snapshot_received = safe_datetime(self.snapshot_id_received_at)
            snapshot_polled = safe_datetime(self.snapshot_polled_at)
            data_received = safe_datetime(self.data_received_at)
            browser_warmed = safe_datetime(self.browser_warmed_at)
            
            if request_sent and snapshot_received:
                durations['request_to_snapshot'] = (snapshot_received - request_sent).total_seconds()
            
            if snapshot_received and snapshot_polled:
                durations['snapshot_to_poll'] = (snapshot_polled - snapshot_received).total_seconds()
                
            if snapshot_polled and data_received:
                durations['poll_to_data'] = (data_received - snapshot_polled).total_seconds()
                
            if browser_warmed and request_sent:
                durations['browser_warm_to_request'] = (request_sent - browser_warmed).total_seconds()
                
            if request_sent and data_received:
                durations['total_scrape_time'] = (data_received - request_sent).total_seconds()
                
        except Exception as e:
            print(f"            ⚠️ Error calculating scrape durations: {e}")
            
        return durations


@dataclass
class ExtractionTimingData:
    """Detailed timing data from extraction operations"""
    url: str
    
    # Filter phase timing
    filter_start_time: Optional[float] = None
    filter_generation_requested_at: Optional[datetime] = None
    filter_generation_enqueued_at: Optional[datetime] = None
    filter_generation_dequeued_at: Optional[datetime] = None
    filter_generation_completed_at: Optional[datetime] = None
    
    # Parse phase timing
    parse_start_time: Optional[float] = None
    parse_generation_requested_at: Optional[datetime] = None
    parse_generation_enqueued_at: Optional[datetime] = None
    parse_generation_dequeued_at: Optional[datetime] = None
    parse_converttodict_start_at: Optional[datetime] = None
    parse_converttodict_end_at: Optional[datetime] = None
    parse_generation_completed_at: Optional[datetime] = None
    
    def calculate_durations(self) -> Dict[str, float]:
        """Calculate duration between timing phases"""
        durations = {}
        
        try:
            # Helper function to safely extract datetime and handle time.time() floats
            def safe_datetime(value):
                if value is None:
                    return None
                if isinstance(value, list):
                    value = value[-1] if value else None
                if isinstance(value, str):
                    try:
                        from datetime import datetime
                        return datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except:
                        return None
                return value
            
            def safe_time_diff(start, end):
                """Calculate time difference handling datetime vs time.time() formats"""
                if start is None or end is None:
                    return None
                
                # If both are datetime objects
                if hasattr(start, 'total_seconds') and hasattr(end, 'total_seconds'):
                    return (end - start).total_seconds()
                
                # If both are Unix timestamps (floats)
                if isinstance(start, (int, float)) and isinstance(end, (int, float)):
                    return end - start
                
                # Mixed types - convert datetime to timestamp
                if hasattr(start, 'timestamp') and isinstance(end, (int, float)):
                    return end - start.timestamp()
                if isinstance(start, (int, float)) and hasattr(end, 'timestamp'):
                    return end.timestamp() - start
                
                # Fallback for datetime objects
                if hasattr(start, 'total_seconds') or hasattr(end, 'total_seconds'):
                    try:
                        return (end - start).total_seconds()
                    except:
                        return None
                
                return None
            
            # Filter phase durations
            filter_req = safe_datetime(self.filter_generation_requested_at)
            filter_enq = safe_datetime(self.filter_generation_enqueued_at)
            filter_deq = safe_datetime(self.filter_generation_dequeued_at)
            filter_comp = safe_datetime(self.filter_generation_completed_at)
            
            if filter_req and filter_enq:
                diff = safe_time_diff(filter_req, filter_enq)
                if diff is not None:
                    durations['filter_request_to_enqueue'] = diff
                    
            if filter_enq and filter_deq:
                diff = safe_time_diff(filter_enq, filter_deq)
                if diff is not None:
                    durations['filter_enqueue_to_dequeue'] = diff
                    
            if filter_deq and filter_comp:
                diff = safe_time_diff(filter_deq, filter_comp)
                if diff is not None:
                    durations['filter_dequeue_to_complete'] = diff
                    
            if filter_req and filter_comp:
                diff = safe_time_diff(filter_req, filter_comp)
                if diff is not None:
                    durations['filter_total_time'] = diff
            
            # Parse phase durations
            parse_req = safe_datetime(self.parse_generation_requested_at)
            parse_enq = safe_datetime(self.parse_generation_enqueued_at)
            parse_deq = safe_datetime(self.parse_generation_dequeued_at)
            parse_conv_start = safe_datetime(self.parse_converttodict_start_at)
            parse_conv_end = safe_datetime(self.parse_converttodict_end_at)
            parse_comp = safe_datetime(self.parse_generation_completed_at)
            
            if parse_req and parse_enq:
                diff = safe_time_diff(parse_req, parse_enq)
                if diff is not None:
                    durations['parse_request_to_enqueue'] = diff
                    
            if parse_enq and parse_deq:
                diff = safe_time_diff(parse_enq, parse_deq)
                if diff is not None:
                    durations['parse_enqueue_to_dequeue'] = diff
                    
            if parse_deq and parse_conv_start:
                diff = safe_time_diff(parse_deq, parse_conv_start)
                if diff is not None:
                    durations['parse_dequeue_to_convertstart'] = diff
                    
            if parse_conv_start and parse_conv_end:
                diff = safe_time_diff(parse_conv_start, parse_conv_end)
                if diff is not None:
                    durations['parse_converttodict_duration'] = diff
                    
            if parse_conv_end and parse_comp:
                diff = safe_time_diff(parse_conv_end, parse_comp)
                if diff is not None:
                    durations['parse_convertend_to_complete'] = diff
                    
            if parse_req and parse_comp:
                diff = safe_time_diff(parse_req, parse_comp)
                if diff is not None:
                    durations['parse_total_time'] = diff
                    
            # Cross-phase timing (handle time.time() vs datetime mix)
            if self.filter_start_time and self.parse_start_time:
                # Both should be time.time() floats from ExtractHero
                if isinstance(self.filter_start_time, (int, float)) and isinstance(self.parse_start_time, (int, float)):
                    durations['filter_to_parse_gap'] = self.parse_start_time - self.filter_start_time
                    
        except Exception as e:
            print(f"            ⚠️ Error calculating extraction durations: {e}")
            
        return durations


@dataclass
class OperationTimeline:
    """Complete timeline of operations for analysis"""
    scrape_timings: List[ScrapeTimingData] = field(default_factory=list)
    extraction_timings: List[ExtractionTimingData] = field(default_factory=list)
    
    def analyze_blocking_operations(self) -> Dict[str, Any]:
        """Analyze timing data to identify potential blocking operations"""
        analysis = {
            'scrape_analysis': {},
            'extraction_analysis': {},
            'blocking_candidates': [],
            'recommendations': []
        }
        
        # Analyze scrape operations
        scrape_durations = []
        for scrape_timing in self.scrape_timings:
            durations = scrape_timing.calculate_durations()
            scrape_durations.append(durations)
        
        if scrape_durations:
            # Calculate statistics for each scrape phase
            phase_stats = {}
            for phase in ['request_to_snapshot', 'snapshot_to_poll', 'poll_to_data', 'total_scrape_time']:
                values = [d.get(phase, 0) for d in scrape_durations if phase in d]
                if values:
                    phase_stats[phase] = {
                        'min': min(values),
                        'max': max(values),
                        'avg': statistics.mean(values),
                        'std': statistics.stdev(values) if len(values) > 1 else 0
                    }
            
            analysis['scrape_analysis'] = phase_stats
            
            # Identify slow scrape phases
            for phase, stats in phase_stats.items():
                if stats['max'] > 10.0:  # More than 10 seconds
                    analysis['blocking_candidates'].append({
                        'operation': f'scrape_{phase}',
                        'max_duration': stats['max'],
                        'avg_duration': stats['avg'],
                        'concern_level': 'high' if stats['max'] > 30 else 'medium'
                    })
        
        # Analyze extraction operations
        extraction_durations = []
        for extract_timing in self.extraction_timings:
            durations = extract_timing.calculate_durations()
            extraction_durations.append(durations)
        
        if extraction_durations:
            # Calculate statistics for each extraction phase
            phase_stats = {}
            phases = [
                'filter_request_to_enqueue', 'filter_enqueue_to_dequeue', 'filter_dequeue_to_complete',
                'parse_request_to_enqueue', 'parse_enqueue_to_dequeue', 'parse_dequeue_to_convertstart',
                'parse_converttodict_duration', 'parse_convertend_to_complete',
                'filter_total_time', 'parse_total_time', 'filter_to_parse_gap'
            ]
            
            for phase in phases:
                values = [d.get(phase, 0) for d in extraction_durations if phase in d]
                if values:
                    phase_stats[phase] = {
                        'min': min(values),
                        'max': max(values),
                        'avg': statistics.mean(values),
                        'std': statistics.stdev(values) if len(values) > 1 else 0
                    }
            
            analysis['extraction_analysis'] = phase_stats
            
            # Identify potentially blocking extraction phases
            blocking_thresholds = {
                'parse_converttodict_duration': 0.1,  # converttodict should be very fast
                'filter_enqueue_to_dequeue': 1.0,     # queue waiting time
                'parse_enqueue_to_dequeue': 1.0,      # queue waiting time
                'filter_to_parse_gap': 0.1,           # gap between phases should be minimal
            }
            
            for phase, threshold in blocking_thresholds.items():
                if phase in phase_stats and phase_stats[phase]['max'] > threshold:
                    analysis['blocking_candidates'].append({
                        'operation': f'extraction_{phase}',
                        'max_duration': phase_stats[phase]['max'],
                        'avg_duration': phase_stats[phase]['avg'],
                        'threshold': threshold,
                        'concern_level': 'high' if phase_stats[phase]['max'] > threshold * 5 else 'medium'
                    })
        
        # Generate recommendations
        recommendations = []
        for candidate in analysis['blocking_candidates']:
            if 'converttodict' in candidate['operation']:
                recommendations.append({
                    'operation': candidate['operation'],
                    'recommendation': 'Consider making converttodict operation async or running in thread pool',
                    'reason': f"Duration {candidate['max_duration']:.3f}s may block event loop"
                })
            elif 'enqueue_to_dequeue' in candidate['operation']:
                recommendations.append({
                    'operation': candidate['operation'],
                    'recommendation': 'Consider increasing LLM service concurrency limits',
                    'reason': f"Queue wait time {candidate['max_duration']:.3f}s indicates resource contention"
                })
            elif 'scrape' in candidate['operation']:
                recommendations.append({
                    'operation': candidate['operation'],
                    'recommendation': 'Consider optimizing scraping timeouts or concurrency',
                    'reason': f"Long scrape duration {candidate['max_duration']:.3f}s may impact pipeline throughput"
                })
        
        analysis['recommendations'] = recommendations
        return analysis


async def test_streaming_pipeline_debug():
    """Run streaming pipeline with detailed timing analysis"""
    
    print("🔍 Streaming Pipeline Debug Test")
    print("=" * 60)
    print("🎯 Goal: Identify blocking operations in streaming pipeline")
    print("📊 Method: Detailed timing analysis of scrape and extraction phases")
    
    # Setup
    entity = EntityToImpute(
        name="BAV99",
        identifier_context="electronic component, diode"
    )
    
    schema = [
        WhatToRetain(name="component_type", desc="Type of electronic component"),
        WhatToRetain(name="voltage_rating", desc="Maximum voltage rating")
    ]
    
    config = get_development_config()
    registry = ServiceRegistry(config)
    
    timeline = OperationTimeline()
    
    try:
        # Phase 1: SERP (batch operation, not analyzed)
        print("\n1️⃣ SERP Phase: Finding URLs...")
        serp_start = time.time()
        
        serp_result = await registry.serp.search(entity.name, top_k=3)
        
        serp_duration = time.time() - serp_start
        print(f"   ✅ Found {len(serp_result.links)} URLs in {serp_duration:.2f}s")
        print(f"   📝 SERP is batch operation (not analyzed for blocking)")
        
        if not serp_result.success or len(serp_result.links) < 2:
            print("   ❌ Need at least 2 URLs for analysis")
            return False
        
        urls = serp_result.links[:3]
        
        # Phase 2: Streaming pipeline with detailed timing
        print("\n2️⃣ Streaming Pipeline with Timing Analysis...")
        
        # Start scraping tasks
        scrape_tasks = {}
        for url in urls:
            print(f"      🚀 Starting scrape for {url[:40]}...")
            task = asyncio.create_task(scrape_with_timing_analysis(registry, url))
            scrape_tasks[task] = url
        
        pending_tasks = set(scrape_tasks.keys())
        
        # Process as each scrape completes
        while pending_tasks:
            done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
            
            for completed_task in done:
                url = scrape_tasks[completed_task]
                
                try:
                    # Get scrape result with timing data
                    scrape_result, scrape_timing = await completed_task
                    timeline.scrape_timings.append(scrape_timing)
                    
                    print(f"      ✅ Scrape completed for {url[:40]}...")
                    print_scrape_timing_analysis(scrape_timing)
                    
                    # If scrape successful, start extraction with timing
                    if scrape_result and any(r.status == "ready" for r in scrape_result.values()):
                        print(f"      🔧 Starting extraction for {url[:40]}... IMMEDIATELY")
                        
                        extract_result, extract_timing = await extract_with_timing_analysis(
                            registry, scrape_result, schema, url
                        )
                        
                        timeline.extraction_timings.append(extract_timing)
                        
                        if extract_result and any(r.success for r in extract_result.values()):
                            print(f"      ✅ Extraction completed for {url[:40]}...")
                            print_extraction_timing_analysis(extract_timing)
                        else:
                            print(f"      ❌ Extraction failed for {url[:40]}...")
                    else:
                        print(f"      ⚠️ Scrape failed for {url[:40]}..., skipping extraction")
                        
                except Exception as e:
                    print(f"      ❌ Processing failed for {url[:40]}...: {e}")
        
        # Phase 3: Analyze for blocking operations
        print("\n3️⃣ Blocking Operations Analysis...")
        analysis = timeline.analyze_blocking_operations()
        print_blocking_analysis(analysis)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Debug test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await registry.close_all()


async def scrape_with_timing_analysis(registry, url: str):
    """Scrape URL and extract detailed timing data"""
    
    scrape_result = await registry.scraper.scrape_urls([url])
    
    # Extract timing data from scrape result
    scrape_timing = ScrapeTimingData(url=url)
    
    if url in scrape_result:
        result = scrape_result[url]
        # Extract timing fields from BrightData result with debugging
        try:
            scrape_timing.request_sent_at = getattr(result, 'request_sent_at', None)
            scrape_timing.snapshot_id_received_at = getattr(result, 'snapshot_id_received_at', None)
            scrape_timing.snapshot_polled_at = getattr(result, 'snapshot_polled_at', None)
            scrape_timing.data_received_at = getattr(result, 'data_received_at', None)
            scrape_timing.browser_warmed_at = getattr(result, 'browser_warmed_at', None)
            
            # Debug: Print types of timing fields to understand the data structure
            timing_fields = {
                'request_sent_at': scrape_timing.request_sent_at,
                'snapshot_id_received_at': scrape_timing.snapshot_id_received_at,
                'snapshot_polled_at': scrape_timing.snapshot_polled_at,
                'data_received_at': scrape_timing.data_received_at,
                'browser_warmed_at': scrape_timing.browser_warmed_at,
            }
            
            print(f"        🔍 Debug - Timing field types for {url[:30]}:")
            for field, value in timing_fields.items():
                print(f"           {field}: {type(value)} = {value}")
                
        except Exception as e:
            print(f"        ⚠️ Error extracting scrape timing data: {e}")
    
    return scrape_result, scrape_timing


async def extract_with_timing_analysis(registry, scrape_result, schema, url: str):
    """Extract data and capture detailed timing information"""
    
    extract_result = await registry.extractor.extract_from_scrapes(scrape_result, schema)
    
    # Extract timing data from extraction result
    extract_timing = ExtractionTimingData(url=url)
    
    if url in extract_result:
        result = extract_result[url]
        
        try:
            # Filter phase timing
            if hasattr(result, 'filter_op') and result.filter_op:
                extract_timing.filter_start_time = getattr(result.filter_op, 'start_time', None)
                
                if hasattr(result.filter_op, 'generation_result') and result.filter_op.generation_result:
                    gen_result = result.filter_op.generation_result
                    if hasattr(gen_result, 'timestamps') and gen_result.timestamps:
                        timestamps = gen_result.timestamps
                        extract_timing.filter_generation_requested_at = timestamps.generation_requested_at
                        extract_timing.filter_generation_enqueued_at = timestamps.generation_enqueued_at
                        extract_timing.filter_generation_dequeued_at = timestamps.generation_dequeued_at
                        extract_timing.filter_generation_completed_at = timestamps.generation_completed_at
            
            # Parse phase timing
            if hasattr(result, 'parse_op') and result.parse_op:
                extract_timing.parse_start_time = getattr(result.parse_op, 'start_time', None)
                
                if hasattr(result.parse_op, 'generation_result') and result.parse_op.generation_result:
                    gen_result = result.parse_op.generation_result
                    if hasattr(gen_result, 'timestamps') and gen_result.timestamps:
                        timestamps = gen_result.timestamps
                        extract_timing.parse_generation_requested_at = timestamps.generation_requested_at
                        extract_timing.parse_generation_enqueued_at = timestamps.generation_enqueued_at
                        extract_timing.parse_generation_dequeued_at = timestamps.generation_dequeued_at
                        extract_timing.parse_converttodict_start_at = timestamps.converttodict_start_at
                        extract_timing.parse_converttodict_end_at = timestamps.converttodict_end_at
                        extract_timing.parse_generation_completed_at = timestamps.generation_completed_at
            
            # Debug: Print types of timing fields
            print(f"        🔍 Debug - Extraction timing types for {url[:30]}:")
            print(f"           filter_start_time: {type(extract_timing.filter_start_time)} = {extract_timing.filter_start_time}")
            print(f"           parse_start_time: {type(extract_timing.parse_start_time)} = {extract_timing.parse_start_time}")
            if extract_timing.parse_converttodict_start_at:
                print(f"           converttodict_start: {type(extract_timing.parse_converttodict_start_at)} = {extract_timing.parse_converttodict_start_at}")
                print(f"           converttodict_end: {type(extract_timing.parse_converttodict_end_at)} = {extract_timing.parse_converttodict_end_at}")
                
        except Exception as e:
            print(f"        ⚠️ Error extracting timing data: {e}")
    
    return extract_result, extract_timing


def print_scrape_timing_analysis(timing: ScrapeTimingData):
    """Print detailed scrape timing analysis"""
    
    print(f"         📊 Scrape Timing Analysis for {timing.url[:30]}...")
    durations = timing.calculate_durations()
    
    if durations:
        for phase, duration in durations.items():
            status = "🟢" if duration < 5 else "🟡" if duration < 15 else "🔴"
            print(f"            {status} {phase}: {duration:.3f}s")
    else:
        print(f"            ⚠️ No timing data available")


def print_extraction_timing_analysis(timing: ExtractionTimingData):
    """Print detailed extraction timing analysis"""
    
    print(f"         📊 Extraction Timing Analysis for {timing.url[:30]}...")
    durations = timing.calculate_durations()
    
    # Organize by phase
    filter_durations = {k: v for k, v in durations.items() if k.startswith('filter_')}
    parse_durations = {k: v for k, v in durations.items() if k.startswith('parse_')}
    
    if filter_durations:
        print(f"            🔍 Filter Phase:")
        for phase, duration in filter_durations.items():
            status = "🟢" if duration < 1 else "🟡" if duration < 5 else "🔴"
            print(f"               {status} {phase}: {duration:.3f}s")
    
    if parse_durations:
        print(f"            🔧 Parse Phase:")
        for phase, duration in parse_durations.items():
            # Special attention to converttodict
            if 'converttodict' in phase:
                status = "🟢" if duration < 0.05 else "🟡" if duration < 0.2 else "🔴"
            else:
                status = "🟢" if duration < 1 else "🟡" if duration < 5 else "🔴"
            print(f"               {status} {phase}: {duration:.3f}s")
    
    if not durations:
        print(f"            ⚠️ No timing data available")


def print_blocking_analysis(analysis: Dict[str, Any]):
    """Print comprehensive blocking operations analysis"""
    
    print("   🔍 Blocking Operations Analysis:")
    
    # Print scrape analysis
    if analysis['scrape_analysis']:
        print("\n      📊 Scrape Operations Analysis:")
        for phase, stats in analysis['scrape_analysis'].items():
            print(f"         {phase}:")
            print(f"            Min: {stats['min']:.3f}s, Max: {stats['max']:.3f}s, Avg: {stats['avg']:.3f}s")
    
    # Print extraction analysis
    if analysis['extraction_analysis']:
        print("\n      🔧 Extraction Operations Analysis:")
        for phase, stats in analysis['extraction_analysis'].items():
            status = "🟢" if stats['max'] < 1 else "🟡" if stats['max'] < 5 else "🔴"
            print(f"         {status} {phase}:")
            print(f"            Min: {stats['min']:.3f}s, Max: {stats['max']:.3f}s, Avg: {stats['avg']:.3f}s")
    
    # Print blocking candidates
    if analysis['blocking_candidates']:
        print("\n      ⚠️ Potential Blocking Operations:")
        for candidate in analysis['blocking_candidates']:
            concern_icon = "🔴" if candidate['concern_level'] == 'high' else "🟡"
            print(f"         {concern_icon} {candidate['operation']}")
            print(f"            Max Duration: {candidate['max_duration']:.3f}s")
            print(f"            Avg Duration: {candidate['avg_duration']:.3f}s")
            print(f"            Concern Level: {candidate['concern_level']}")
    
    # Print recommendations
    if analysis['recommendations']:
        print("\n      💡 Optimization Recommendations:")
        for rec in analysis['recommendations']:
            print(f"         🎯 {rec['operation']}:")
            print(f"            Recommendation: {rec['recommendation']}")
            print(f"            Reason: {rec['reason']}")
    
    if not analysis['blocking_candidates']:
        print("\n      ✅ No significant blocking operations detected!")


async def main():
    """Run streaming pipeline debug test"""
    
    print("🔍 Streaming Pipeline Debug Test")
    print("Analyzing detailed timing logs to identify blocking operations")
    
    success = await test_streaming_pipeline_debug()
    
    print("\n" + "=" * 60)
    print("📊 STREAMING PIPELINE DEBUG RESULTS")
    print("=" * 60)
    
    if success:
        print("✅ Debug analysis completed successfully!")
        print("   📊 Check timing analysis above for potential optimizations")
        print("   💡 Focus on operations marked with 🔴 or 🟡")
        print("   🎯 Consider async conversion for blocking operations")
    else:
        print("❌ Debug analysis failed!")
        print("   🔧 Check pipeline configuration and service availability")
    
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)