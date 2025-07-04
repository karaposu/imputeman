# imputeman/models.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict

from serpengine.schemes import SerpEngineOp, SearchHit
from brightdata.models import ScrapeResult
from extracthero.schemes import ExtractOp, ItemToExtract

@dataclass
class ImputeOp:
    """Holds the full end-to-end result of one Imputeman run."""
    query: str
    what_to_extract: List[ItemToExtract]
    serp_op: SerpEngineOp
    all_links: List[str]
    crawl_ops: Dict[str, ScrapeResult]
    extract_ops: Dict[str, ExtractOp]
    elapsed: float
