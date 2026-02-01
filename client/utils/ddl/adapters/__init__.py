"""
Target database adapters for DDL generation.
"""

from .base_adapter import BaseTargetAdapter
from .mysql_adapter import MySQLTargetAdapter
from .postgres_adapter import PostgreSQLTargetAdapter

__all__ = [
    'BaseTargetAdapter',
    'MySQLTargetAdapter',
    'PostgreSQLTargetAdapter',
]