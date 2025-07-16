# imputeman/db_models.py

import os
from datetime import datetime
from sqlalchemy import Column, Integer, Text, DateTime, Float, Boolean, String, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Dict, Any

Base = declarative_base()

# Schema configuration based on environment variables
RUN_MODE = os.getenv("RUN_MODE", "test")
PROD_SCHEMA = os.getenv("PROD_SCHEMA", "production")
TEST_SCHEMA = os.getenv("TEST_SCHEMA", "test")
SCHEMA_NAME = PROD_SCHEMA if RUN_MODE == "prod" else TEST_SCHEMA
TABLE_NAME = os.getenv("IMPUTE_LOG_TABLE_NAME", "impute_logs")


class ImputeLog(Base):
    """
    SQLAlchemy model that exactly mirrors ImputeOp structure.
    Each nested dataclass is stored as JSONB with the same field name.
    """
    __tablename__ = TABLE_NAME
    __table_args__ = {'schema': SCHEMA_NAME}
    
    # Primary key
    id = Column(Integer, primary_key=True)
    
    # Core identification (matches ImputeOp exactly)
    query = Column(Text, nullable=False)
    schema = Column(JSONB, nullable=True)  # List[WhatToRetain] as JSON
    
    # Status tracking (matches ImputeOp exactly)
    success = Column(Boolean, default=False)
    status = Column(String(50), nullable=False)  # PipelineStatus enum value
    status_details = Column(JSONB, nullable=True)  # StatusDetails dataclass as JSON
    
    # Execution results (matches ImputeOp exactly)
    search_op = Column(JSONB, nullable=True)  # SerpEngineOp as JSON
    urls = Column(JSONB, nullable=True)  # List[str]
    scrape_results = Column(JSONB, nullable=True)  # Dict[str, ScrapeResult] as JSON
    extract_results = Column(JSONB, nullable=True)  # Dict[str, ExtractOp] as JSON
    
    # Final content (matches ImputeOp exactly)
    content = Column(JSONB, nullable=True)  # Dict[str, Any]
    
    # Comprehensive metrics (matches ImputeOp exactly)
    performance = Column(JSONB, nullable=True)  # PerformanceMetrics dataclass as JSON
    costs = Column(JSONB, nullable=True)  # CostBreakdown dataclass as JSON
    
    # Error tracking (matches ImputeOp exactly)
    errors = Column(JSONB, nullable=True)  # List[str]
    warnings = Column(JSONB, nullable=True)  # List[str]
    
    # Metadata (matches ImputeOp exactly)
    created_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    
    # Additional fields for fast path support
    fast_path_results = Column(JSONB, nullable=True)  # Dict[str, ScrapeResult] for fast path
    
    # Database metadata (not in ImputeOp)
    db_created_at = Column(DateTime, default=func.now())
    db_updated_at = Column(DateTime, onupdate=func.now())
    
    @classmethod
    async def from_imputeOp(cls, impute_op, session=None):
        """
        Create an ImputeLog instance from an ImputeOp instance.
        Preserves the exact structure of ImputeOp.
        
        Args:
            impute_op: An instance of ImputeOp to convert
            session: Database session for saving (if provided)
            
        Returns:
            ImputeLog: The created log instance
        """
        # Helper function to convert dataclass to dict
        def dataclass_to_dict(obj):
            if hasattr(obj, '__dict__'):
                result = {}
                for key, value in obj.__dict__.items():
                    if hasattr(value, '__dict__'):  # Nested dataclass
                        result[key] = dataclass_to_dict(value)
                    elif hasattr(value, 'value'):  # Enum
                        result[key] = value.value
                    elif isinstance(value, datetime):
                        result[key] = value.isoformat()
                    elif isinstance(value, (list, dict, str, int, float, bool, type(None))):
                        result[key] = value
                    else:
                        result[key] = str(value)  # Fallback for unknown types
                return result
            return obj
        
        # Convert schema (List[WhatToRetain])
        schema_data = None
        if impute_op.schema:
            schema_data = [
                {'name': item.name, 'desc': item.desc} 
                for item in impute_op.schema
            ]
        
        # Convert StatusDetails dataclass
        status_details_data = dataclass_to_dict(impute_op.status_details)
        
        # Convert PerformanceMetrics dataclass
        performance_data = dataclass_to_dict(impute_op.performance)
        
        # Convert CostBreakdown dataclass
        costs_data = dataclass_to_dict(impute_op.costs)
        
        # Convert search_op (SerpEngineOp)
        search_op_data = None
        if impute_op.search_op:
            search_op_data = {
                'all_links': getattr(impute_op.search_op, 'all_links', []),
                'results': [
                    {
                        'url': r.url,
                        'title': getattr(r, 'title', ''),
                        'snippet': getattr(r, 'snippet', ''),
                        'channel': getattr(r, 'channel', '')
                    } for r in getattr(impute_op.search_op, 'results', [])
                ],
                'usage': {
                    'cost': impute_op.search_op.usage.cost if hasattr(impute_op.search_op, 'usage') else 0
                }
            }
        
        # Convert scrape_results (Dict[str, ScrapeResult])
        scrape_results_data = {}
        if impute_op.scrape_results:
            for url, result in impute_op.scrape_results.items():
                scrape_results_data[url] = {
                    'success': result.success,
                    'url': result.url,
                    'status': result.status,
                    'cost': getattr(result, 'cost', 0),
                    'error': getattr(result, 'error', None),
                    'snapshot_id': getattr(result, 'snapshot_id', None),
                    'fallback_used': getattr(result, 'fallback_used', False),
                    'root_domain': getattr(result, 'root_domain', None),
                    'html_char_size': getattr(result, 'html_char_size', 0),
                    'row_count': getattr(result, 'row_count', 0),
                    'field_count': getattr(result, 'field_count', 0),
                    # Store data type info but not full data
                    'data_type': type(result.data).__name__ if hasattr(result, 'data') else None,
                    'data_size': len(result.data) if hasattr(result, 'data') and result.data else 0
                }
        
        # Convert extract_results (Dict[str, ExtractOp])
        extract_results_data = {}
        if impute_op.extract_results:
            for url, extract_op in impute_op.extract_results.items():
                extract_results_data[url] = {
                    'success': extract_op.success,
                    'content': extract_op.content,  # Preserve extracted data
                    'error': getattr(extract_op, 'error', None),
                    'usage': getattr(extract_op, 'usage', {}),
                    'stage_tokens': getattr(extract_op, 'stage_tokens', {}),
                    'filter_op': dataclass_to_dict(getattr(extract_op, 'filter_op', None)),
                    'parse_op': dataclass_to_dict(getattr(extract_op, 'parse_op', None))
                }
        
        # Convert fast_path_results if present
        fast_path_results_data = None
        if hasattr(impute_op, 'fast_path_results') and impute_op.fast_path_results:
            fast_path_results_data = {}
            for url, result in impute_op.fast_path_results.items():
                fast_path_results_data[url] = {
                    'success': result.success,
                    'url': result.url,
                    'status': result.status,
                    'cost': getattr(result, 'cost', 0),
                    'error': getattr(result, 'error', None),
                    'data_type': type(result.data).__name__ if hasattr(result, 'data') else None,
                    'data_size': len(result.data) if hasattr(result, 'data') and result.data else 0
                }
        
        # Create log entry with exact ImputeOp structure
        log_entry = cls(
            # Core identification
            query=impute_op.query,
            schema=schema_data,
            
            # Status tracking
            success=impute_op.success,
            status=impute_op.status.value if hasattr(impute_op.status, 'value') else str(impute_op.status),
            status_details=status_details_data,
            
            # Execution results
            search_op=search_op_data,
            urls=impute_op.urls,
            scrape_results=scrape_results_data,
            extract_results=extract_results_data,
            
            # Final content
            content=impute_op.content,
            
            # Comprehensive metrics
            performance=performance_data,
            costs=costs_data,
            
            # Error tracking
            errors=impute_op.errors,
            warnings=impute_op.warnings,
            
            # Metadata
            created_at=impute_op.created_at,
            completed_at=impute_op.completed_at,
            
            # Fast path results
            fast_path_results=fast_path_results_data
        )
        
        # Save if session provided
        if session:
            session.add(log_entry)
            await session.commit()
            print(f"Saved ImputeLog for query: {impute_op.query}")
        
        return log_entry
    
    @classmethod
    async def to_imputeOp(cls, log_id: int, session):
        """
        Reconstruct an ImputeOp instance from a database log entry.
        
        Args:
            log_id: The ID of the log entry to retrieve
            session: Database session for querying
            
        Returns:
            ImputeOp: Reconstructed ImputeOp instance, or None if not found
        """
        from .models import ImputeOp, PipelineStatus, StatusDetails, PerformanceMetrics, CostBreakdown
        from .core.entities import WhatToRetain
        
        # Retrieve log entry
        log_entry = await session.get(cls, log_id)
        if not log_entry:
            print(f"No log entry found with ID: {log_id}")
            return None
        
        # Reconstruct schema
        schema = []
        if log_entry.schema:
            schema = [
                WhatToRetain(name=item['name'], desc=item['desc'])
                for item in log_entry.schema
            ]
        
        # Create ImputeOp instance
        impute_op = ImputeOp(query=log_entry.query, schema=schema)
        
        # Restore status fields
        impute_op.success = log_entry.success
        impute_op.status = PipelineStatus(log_entry.status)
        
        # Restore StatusDetails from JSONB
        if log_entry.status_details:
            for key, value in log_entry.status_details.items():
                if hasattr(impute_op.status_details, key):
                    if key == 'current_status':
                        setattr(impute_op.status_details, key, PipelineStatus(value))
                    elif key == 'last_update' and value:
                        setattr(impute_op.status_details, key, datetime.fromisoformat(value))
                    else:
                        setattr(impute_op.status_details, key, value)
        
        # Restore PerformanceMetrics from JSONB
        if log_entry.performance:
            for key, value in log_entry.performance.items():
                if hasattr(impute_op.performance, key):
                    setattr(impute_op.performance, key, value)
        
        # Restore CostBreakdown from JSONB
        if log_entry.costs:
            for key, value in log_entry.costs.items():
                if hasattr(impute_op.costs, key):
                    setattr(impute_op.costs, key, value)
        
        # Restore execution results
        impute_op.urls = log_entry.urls or []
        
        # Restore search_op (partial reconstruction)
        if log_entry.search_op:
            search_op = type('ReconstructedSerpEngineOp', (), {
                'all_links': log_entry.search_op.get('all_links', []),
                'results': [
                    type('ReconstructedSerpResult', (), result)() 
                    for result in log_entry.search_op.get('results', [])
                ],
                'usage': type('ReconstructedUsage', (), log_entry.search_op.get('usage', {}))()
            })()
            impute_op.search_op = search_op
        
        # Restore scrape_results
        if log_entry.scrape_results:
            impute_op.scrape_results = {}
            for url, data in log_entry.scrape_results.items():
                scrape_result = type('ReconstructedScrapeResult', (), data)()
                scrape_result.data = None  # Data not stored
                impute_op.scrape_results[url] = scrape_result
        
        # Restore extract_results (with full content)
        if log_entry.extract_results:
            impute_op.extract_results = {}
            for url, data in log_entry.extract_results.items():
                extract_op = type('ReconstructedExtractOp', (), {
                    'success': data.get('success', False),
                    'content': data.get('content'),
                    'error': data.get('error'),
                    'usage': data.get('usage', {}),
                    'stage_tokens': data.get('stage_tokens', {})
                })()
                impute_op.extract_results[url] = extract_op
        
        # Restore fast_path_results if present
        if log_entry.fast_path_results:
            impute_op.fast_path_results = {}
            for url, data in log_entry.fast_path_results.items():
                fast_path_result = type('ReconstructedScrapeResult', (), data)()
                fast_path_result.data = None  # Data not stored
                impute_op.fast_path_results[url] = fast_path_result
        
        # Restore final content
        impute_op.content = log_entry.content
        
        # Restore error tracking
        impute_op.errors = log_entry.errors or []
        impute_op.warnings = log_entry.warnings or []
        
        # Restore metadata
        impute_op.created_at = log_entry.created_at
        impute_op.completed_at = log_entry.completed_at
        
        print(f"Reconstructed ImputeOp from log ID: {log_id}")
        return impute_op
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary for API responses"""
        return {
            'id': self.id,
            'query': self.query,
            'status': self.status,
            'success': self.success,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'status_details': self.status_details,
            'performance': self.performance,
            'costs': self.costs,
            'errors': self.errors,
            'warnings': self.warnings,
            'urls': self.urls,
            'content': self.content,
            'extract_results': self.extract_results
        }