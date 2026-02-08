"""
Unified DDL (Data Definition Language) handling for CDC replication.

Supports:
- MySQL sources (full DDL via Kafka schema topic)
- SQL Server sources (tableChanges metadata)
- PostgreSQL sources (event trigger-based DDL capture via Kafka)

Targets:
- MySQL
- PostgreSQL
"""

from .base_processor import DDLOperation, DDLOperationType, BaseDDLProcessor
from .type_maps import get_type_map, map_type, MYSQL_TYPE_MAP, MSSQL_TYPE_MAP, POSTGRESQL_TYPE_MAP
from .kafka_processor import KafkaDDLProcessor
from .postgres_kafka_processor import PostgreSQLKafkaDDLProcessor
from .schema_sync import PostgreSQLSchemaSyncService
from .adapters import MySQLTargetAdapter, PostgreSQLTargetAdapter, BaseTargetAdapter

__all__ = [
    'DDLOperation',
    'DDLOperationType',
    'BaseDDLProcessor',
    'BaseTargetAdapter',
    'KafkaDDLProcessor',
    'PostgreSQLKafkaDDLProcessor',
    'PostgreSQLSchemaSyncService',
    'MySQLTargetAdapter',
    'PostgreSQLTargetAdapter',
    'get_type_map',
    'map_type',
    'MYSQL_TYPE_MAP',
    'MSSQL_TYPE_MAP',
    'POSTGRESQL_TYPE_MAP',
]