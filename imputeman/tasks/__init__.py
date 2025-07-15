# imputeman/tasks/__init__.py
"""Prefect tasks for Imputeman pipeline"""

from .serp_tasks import search_serp_task, extract_urls_from_serp_task
from .scrape_tasks import scrape_urls_task, analyze_scrape_costs_task
from .extract_tasks import extract_data_task

__all__ = [
    # SERP tasks
    "search_serp_task",
    "extract_urls_from_serp_task",
    
    # Scraping tasks
    "scrape_urls_task",
    "analyze_scrape_costs_task",
    
    # Extraction tasks
    "extract_data_task",
]