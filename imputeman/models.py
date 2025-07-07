# imputeman/models.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from serpengine.schemes import SerpEngineOp, SearchHit
from brightdata.models import ScrapeResult
from extracthero.schemes import ExtractOp, WhatToRetain



@dataclass
class EntityToImpute:    
    name: str   
    identifier_context: Optional[str] = None   
    impute_task_purpose: Optional[str] = None


@dataclass
class ImputeOp:
    """
    Encapsulates one end-to-end run of Imputeman:
      - the original query & schema
      - the raw SERP & link list
      - per-URL scrape & extract results
      - summary metrics
    """
    query: str
    schema: List[WhatToRetain]
    search_op: SerpEngineOp
    links: List[str]
    scrape_results_dict: Dict[str, ScrapeResult]
    extract_ops: Dict[str, ExtractOp]
    hits: int
    extracted: int
    elapsed: float
    results: Dict[str, Any]

