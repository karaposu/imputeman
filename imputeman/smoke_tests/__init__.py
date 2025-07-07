# imputeman/smoke_tests/__init__.py
"""
Smoke tests for Imputeman - incremental testing of system components

Quick Start:
    python -m imputeman.smoke_tests quick    # Run essential tests
    python -m imputeman.smoke_tests           # Run all tests
    python -m imputeman.smoke_tests 2         # Run configuration test only

Individual tests:
    python -m imputeman.smoke_tests.test_2_configuration_system
    python -m imputeman.smoke_tests.test_3_entities_and_schema  
    python -m imputeman.smoke_tests.test_4_individual_services
    python -m imputeman.smoke_tests.test_1_run_imputeman_without_prefect
    python -m imputeman.smoke_tests.test_5_prefect_integration

All commands should be run from the project root directory.
"""

__version__ = "1.0.0"

# Make test runner easily accessible
from .run_all_tests import SmokeTestRunner

__all__ = ["SmokeTestRunner"]