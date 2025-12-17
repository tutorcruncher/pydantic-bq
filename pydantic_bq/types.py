"""BigQuery type definitions and utilities."""

import logging
from decimal import Decimal
from enum import Enum

logger = logging.getLogger('pydantic_bq')


class T(str, Enum):
    """BigQuery field types."""

    INT = 'INTEGER'
    NUM = 'NUMERIC'
    FLOAT = 'FLOAT'
    STR = 'STRING'
    TS = 'TIMESTAMP'
    DATE = 'DATE'
    BOOL = 'BOOLEAN'


def to_str(v) -> str:
    """Convert a value to string for BigQuery."""
    if isinstance(v, Decimal):
        return f'{v:0.2f}'
    elif isinstance(v, bool):
        return str(v).lower()
    else:
        return str(v)
