"""
pydantic-bq: Pydantic-based BigQuery client.

A simple library for interacting with BigQuery using Pydantic models
for schema definition and data validation.
"""

from .client import BQTable, BQView, DatasetClient, create_client
from .schema import BQBaseModel
from .settings import Settings, settings
from .types import T, to_str

__version__ = '0.1.0'

__all__ = [
    'BQBaseModel',
    'BQTable',
    'BQView',
    'DatasetClient',
    'Settings',
    'T',
    'create_client',
    'settings',
    'to_str',
]
