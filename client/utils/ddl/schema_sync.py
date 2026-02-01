"""
PostgreSQL Schema Sync Service.

Since PostgreSQL logical decoding doesn't emit DDL events,
this service periodically compares schemas and applies changes.

Flow:
1. Get list of replicated tables from ReplicationConfig
2. For each table, compare source and target schemas
3. Generate and apply DDL operations for differences
"""

import logging
from typing import Dict, List, Optional, Tuple, Set

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from .base_processor import BaseDDLProcessor, DDLOperation, DDLOperationType
from client.models.replication import ReplicationConfig

logger = logging.getLogger(__name__)


class PostgreSQLSchemaSyncService(BaseDDLProcessor):
    """
    Periodic schema synchronization for PostgreSQL sources.

    Since PostgreSQL's logical decoding doesn't support DDL events,
    this service periodically compares source and target schemas
    and applies any differences.
    """

    def __init__(
        self,
        replication_config: ReplicationConfig,
        target_engine: Engine,
        source_engine: Optional[Engine] = None,
        auto_execute_destructive: bool = False
    ):
        """
        Initialize PostgreSQL Schema Sync Service.

        Args:
            replication_config: ReplicationConfig instance
            target_engine: SQLAlchemy engine for target database
            source_engine: SQLAlchemy engine for source database (optional, will be created if not provided)
            auto_execute_destructive: Whether to auto-execute destructive operations
        """
        super().__init__(replication_config, target_engine, auto_execute_destructive)

        # Source database connection
        self.source_db = replication_config.client_database

        if source_engine:
            self.source_engine = source_engine
            self._owns_source_engine = False
        else:
            from client.utils.database_utils import get_database_engine
            self.source_engine = get_database_engine(self.source_db)
            self._owns_source_engine = True

        self._source_inspector = None

        # Get target database info
        target_db = self.source_db.client.client_databases.filter(is_target=True).first()
        self.target_db_type = target_db.db_type.lower() if target_db else 'postgresql'

        # Get source schema (default to 'public' for PostgreSQL)
        self.source_schema = getattr(self.source_db, 'schema', 'public') or 'public'

        # Get target schema
        self.target_schema = 'public' if self.target_db_type == 'postgresql' else None

        logger.info(f"PostgreSQL Schema Sync Service initialized")
        logger.info(f"   Source: {self.source_db.connection_name} (schema: {self.source_schema})")
        logger.info(f"   Target type: {self.target_db_type}")

    @property
    def source_inspector(self):
        """Lazy initialization of source SQLAlchemy inspector."""
        if self._source_inspector is None:
            self._source_inspector = inspect(self.source_engine)
        return self._source_inspector

    def process(self, timeout_sec: int = 30) -> Tuple[int, int]:
        """
        Run schema synchronization.

        Compares source and target schemas for all replicated tables
        and applies any differences.

        Args:
            timeout_sec: Not used (for interface compatibility)

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        processed = 0
        errors = 0

        # Get replicated tables from TableMapping
        table_mappings = self.replication_config.table_mappings.filter(is_enabled=True)

        if not table_mappings.exists():
            logger.debug("No enabled table mappings found")
            return 0, 0

        for table_mapping in table_mappings:
            source_table = table_mapping.source_table
            target_table = table_mapping.target_table
            source_schema = table_mapping.source_schema or self.source_schema

            try:
                # Compare schemas
                operations = self._compare_table_schemas(
                    source_table=source_table,
                    target_table=target_table,
                    source_schema=source_schema
                )

                if not operations:
                    logger.debug(f"Table {source_table} is in sync")
                    continue

                logger.info(f"Found {len(operations)} schema changes for {source_table}")

                # Apply operations
                for op in operations:
                    success, error = self.execute_operation(op)
                    if success:
                        processed += 1
                        logger.info(f"   Applied: {op.operation_type.value} on {op.table_name}")
                    else:
                        errors += 1
                        logger.error(f"   Failed: {op.operation_type.value} on {op.table_name}: {error}")

            except Exception as e:
                logger.error(f"Error syncing table {source_table}: {e}", exc_info=True)
                errors += 1

        if processed > 0 or errors > 0:
            logger.info(f"Schema sync complete: {processed} changes applied, {errors} errors")

        return processed, errors

    def _compare_table_schemas(
        self,
        source_table: str,
        target_table: str,
        source_schema: str = 'public'
    ) -> List[DDLOperation]:
        """
        Compare source and target table schemas.

        Returns list of DDL operations to sync target with source.
        """
        operations = []

        # Check if source table exists
        try:
            source_tables = self.source_inspector.get_table_names(schema=source_schema)
            if source_table not in source_tables:
                logger.warning(f"Source table {source_schema}.{source_table} not found")
                return []
        except Exception as e:
            logger.error(f"Error checking source table existence: {e}")
            return []

        # Check if target table exists
        target_exists = self.target_adapter.table_exists(target_table)

        if not target_exists:
            # Target table doesn't exist - create it
            logger.info(f"Target table {target_table} doesn't exist, will create")
            return [self._create_table_operation(source_table, target_table, source_schema)]

        # Get column definitions
        source_columns = self._get_source_columns(source_table, source_schema)
        target_columns = self._get_target_columns(target_table)

        source_col_map = {c['name']: c for c in source_columns}
        target_col_map = {c['name']: c for c in target_columns}

        # Detect added columns (in source but not in target)
        for col_name, col_info in source_col_map.items():
            if col_name not in target_col_map:
                logger.debug(f"Column {col_name} exists in source but not target")
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.ADD_COLUMN,
                    table_name=target_table,
                    details={'column': self._convert_column_info(col_info)},
                    is_destructive=False
                ))

        # Detect dropped columns (in target but not in source)
        for col_name in target_col_map:
            if col_name not in source_col_map:
                logger.debug(f"Column {col_name} exists in target but not source")
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.DROP_COLUMN,
                    table_name=target_table,
                    details={'column_name': col_name},
                    is_destructive=True
                ))

        # Detect type changes
        for col_name, source_col in source_col_map.items():
            if col_name in target_col_map:
                target_col = target_col_map[col_name]
                if self._types_differ(source_col, target_col):
                    logger.debug(f"Column {col_name} type differs: {source_col.get('type')} vs {target_col.get('type')}")
                    operations.append(DDLOperation(
                        operation_type=DDLOperationType.MODIFY_COLUMN,
                        table_name=target_table,
                        details={
                            'column': self._convert_column_info(source_col),
                            'old_type': str(target_col.get('type', ''))
                        },
                        is_destructive=False
                    ))

        return operations

    def _create_table_operation(
        self,
        source_table: str,
        target_table: str,
        source_schema: str
    ) -> DDLOperation:
        """Create DDL operation for new table."""
        # Get full schema from source
        columns = self._get_source_columns(source_table, source_schema)
        primary_keys = self._get_source_primary_keys(source_table, source_schema)

        converted_columns = [self._convert_column_info(col) for col in columns]

        return DDLOperation(
            operation_type=DDLOperationType.CREATE_TABLE,
            table_name=target_table,
            details={
                'columns': converted_columns,
                'primary_keys': primary_keys
            },
            is_destructive=False
        )

    def _get_source_columns(self, table_name: str, schema: str) -> List[Dict]:
        """Get column definitions from source database."""
        try:
            return self.source_inspector.get_columns(table_name, schema=schema)
        except Exception as e:
            logger.error(f"Error getting source columns for {schema}.{table_name}: {e}")
            return []

    def _get_source_primary_keys(self, table_name: str, schema: str) -> List[str]:
        """Get primary key columns from source database."""
        try:
            pk_constraint = self.source_inspector.get_pk_constraint(table_name, schema=schema)
            return pk_constraint.get('constrained_columns', []) if pk_constraint else []
        except Exception as e:
            logger.error(f"Error getting source primary keys for {schema}.{table_name}: {e}")
            return []

    def _get_target_columns(self, table_name: str) -> List[Dict]:
        """Get column definitions from target database."""
        return self.target_adapter.get_table_columns(table_name)

    def _convert_column_info(self, col: Dict) -> Dict:
        """
        Convert SQLAlchemy column info to Debezium-style format.

        SQLAlchemy Inspector returns:
        {
            'name': 'column_name',
            'type': INTEGER(),
            'nullable': True,
            'default': None,
            'autoincrement': False,
            ...
        }

        We convert to Debezium format:
        {
            'name': 'column_name',
            'typeName': 'INTEGER',
            'length': None,
            'scale': None,
            'optional': True,
            'autoIncremented': False,
            'sourceDbType': 'postgresql'
        }
        """
        col_type = col.get('type')
        type_str = str(col_type).upper() if col_type else 'VARCHAR'

        # Extract type name and parameters
        type_name = type_str.split('(')[0].strip()

        # Get length/precision
        length = getattr(col_type, 'length', None)
        if length is None:
            length = getattr(col_type, 'precision', None)

        # Get scale
        scale = getattr(col_type, 'scale', None)

        return {
            'name': col['name'],
            'typeName': type_name,
            'length': length,
            'scale': scale,
            'optional': col.get('nullable', True),
            'autoIncremented': col.get('autoincrement', False),
            'sourceDbType': 'postgresql',
            'default': col.get('default')
        }

    def _types_differ(self, source_col: Dict, target_col: Dict) -> bool:
        """
        Check if column types differ significantly.

        Normalizes type names to handle equivalent types.
        """
        source_type = str(source_col.get('type', '')).upper()
        target_type = str(target_col.get('type', '')).upper()

        # Normalize type names for comparison
        source_base = source_type.split('(')[0].strip()
        target_base = target_type.split('(')[0].strip()

        # Map equivalent types
        equivalents = {
            'INTEGER': {'INT', 'INT4', 'INTEGER'},
            'BIGINT': {'INT8', 'BIGINT'},
            'SMALLINT': {'INT2', 'SMALLINT'},
            'TEXT': {'TEXT', 'VARCHAR'},  # Treat unbounded VARCHAR as TEXT
            'CHARACTER VARYING': {'VARCHAR', 'CHARACTER VARYING'},
            'TIMESTAMP WITHOUT TIME ZONE': {'TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE'},
            'TIMESTAMP WITH TIME ZONE': {'TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE'},
            'DOUBLE PRECISION': {'FLOAT8', 'DOUBLE PRECISION', 'DOUBLE'},
            'REAL': {'FLOAT4', 'REAL', 'FLOAT'},
            'BOOLEAN': {'BOOL', 'BOOLEAN'},
            'NUMERIC': {'DECIMAL', 'NUMERIC'},
        }

        # Check if types are equivalent
        for canonical, aliases in equivalents.items():
            source_match = source_base in aliases or source_base == canonical
            target_match = target_base in aliases or target_base == canonical
            if source_match and target_match:
                # Types are equivalent, check if parameters differ for VARCHAR/NUMERIC
                if canonical in ('CHARACTER VARYING', 'NUMERIC'):
                    source_len = getattr(source_col.get('type'), 'length', None)
                    target_len = getattr(target_col.get('type'), 'length', None)
                    if source_len != target_len:
                        return True
                return False

        # Direct comparison
        return source_base != target_base

    def close(self):
        """Clean up resources."""
        if self._owns_source_engine and self.source_engine:
            try:
                self.source_engine.dispose()
                logger.info("PostgreSQL Schema Sync Service closed")
            except Exception as e:
                logger.error(f"Error closing source engine: {e}")