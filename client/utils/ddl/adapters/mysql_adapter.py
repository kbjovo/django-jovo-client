"""
MySQL target adapter for DDL generation.

Handles MySQL-specific DDL syntax and type mappings.
"""

import logging
from typing import Tuple, Optional, List, Dict, TYPE_CHECKING

from .base_adapter import BaseTargetAdapter
from ..type_maps import (
    get_mysql_to_postgres_type,
    get_postgres_to_mysql_type,
    get_mssql_to_mysql_type,
)

if TYPE_CHECKING:
    from ..base_processor import DDLOperation

logger = logging.getLogger(__name__)


class MySQLTargetAdapter(BaseTargetAdapter):
    """MySQL-specific DDL generation and execution."""

    @property
    def db_type(self) -> str:
        return 'mysql'

    def _get_default_schema(self) -> Optional[str]:
        # MySQL doesn't use schemas in the same way - use database name
        return None

    def execute(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Execute DDL operation for MySQL target."""
        from ..base_processor import DDLOperationType

        handlers = {
            DDLOperationType.CREATE_TABLE: self._handle_create_table,
            DDLOperationType.DROP_TABLE: self._handle_drop_table,
            DDLOperationType.ADD_COLUMN: self._handle_add_column,
            DDLOperationType.DROP_COLUMN: self._handle_drop_column,
            DDLOperationType.MODIFY_COLUMN: self._handle_modify_column,
            DDLOperationType.RENAME_COLUMN: self._handle_rename_column,
            DDLOperationType.ADD_INDEX: self._handle_add_index,
            DDLOperationType.DROP_INDEX: self._handle_drop_index,
            DDLOperationType.ADD_PRIMARY_KEY: self._handle_add_primary_key,
            DDLOperationType.DROP_PRIMARY_KEY: self._handle_drop_primary_key,
        }

        handler = handlers.get(operation.operation_type)
        if not handler:
            return False, f"Unsupported operation: {operation.operation_type}"

        return handler(operation)

    def _handle_create_table(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle CREATE TABLE operation."""
        sql = self.generate_create_table(
            operation.table_name,
            operation.details.get('columns', []),
            operation.details.get('primary_keys', [])
        )
        logger.info(f"Creating table {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_drop_table(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle DROP TABLE operation."""
        sql = f"DROP TABLE IF EXISTS `{operation.table_name}`"
        logger.info(f"Dropping table {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_add_column(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle ADD COLUMN operation."""
        sql = self.generate_add_column(
            operation.table_name,
            operation.details.get('column', {})
        )
        logger.info(f"Adding column to {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_drop_column(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle DROP COLUMN operation."""
        sql = self.generate_drop_column(
            operation.table_name,
            operation.details.get('column_name')
        )
        logger.info(f"Dropping column from {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_modify_column(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle MODIFY COLUMN operation."""
        sql = self.generate_modify_column(
            operation.table_name,
            operation.details.get('column', {}),
            operation.details.get('old_type', '')
        )
        logger.info(f"Modifying column in {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_rename_column(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle RENAME COLUMN operation."""
        details = operation.details
        old_name = details.get('old_name')
        new_name = details.get('new_name')
        column_type = details.get('column_type', 'VARCHAR(255)')

        # Check if source column exists (skip if already renamed or doesn't exist)
        actual_type = self._get_column_type_from_db(operation.table_name, old_name)
        if not actual_type:
            # Source column doesn't exist - either already renamed or stale DDL event
            if self._get_column_type_from_db(operation.table_name, new_name):
                logger.info(f"Column {old_name} already renamed to {new_name}, skipping")
            else:
                logger.info(f"Column {old_name} not found (stale DDL event), skipping")
            return True, None  # Skip gracefully, don't fail

        # Use actual type from DB (handles ENUM/SET with values)
        column_type = actual_type

        sql = self.generate_rename_column(
            operation.table_name,
            old_name,
            new_name,
            column_type
        )
        logger.info(f"Renaming column in {operation.table_name}")
        return self.execute_sql(sql)

    def _get_column_type_from_db(self, table_name: str, column_name: str) -> Optional[str]:
        """Query actual column type from database (needed for ENUM/SET with values)."""
        try:
            from sqlalchemy import text
            sql = text("""
                SELECT COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = :table_name
                AND COLUMN_NAME = :column_name
            """)
            with self.engine.connect() as conn:
                result = conn.execute(sql, {'table_name': table_name, 'column_name': column_name})
                row = result.fetchone()
                if row:
                    return row[0]
        except Exception as e:
            logger.warning(f"Could not get column type from DB: {e}")
        return None

    def _handle_add_index(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle ADD INDEX operation."""
        details = operation.details
        sql = self.generate_add_index(
            operation.table_name,
            details.get('index_name'),
            details.get('columns', []),
            details.get('unique', False)
        )
        logger.info(f"Adding index to {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_drop_index(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle DROP INDEX operation."""
        sql = self.generate_drop_index(
            operation.table_name,
            operation.details.get('index_name')
        )
        logger.info(f"Dropping index from {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_add_primary_key(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle ADD PRIMARY KEY operation."""
        columns = operation.details.get('columns', [])
        pk_cols = ', '.join(f"`{col}`" for col in columns)
        sql = f"ALTER TABLE `{operation.table_name}` ADD PRIMARY KEY ({pk_cols})"
        logger.info(f"Adding primary key to {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_drop_primary_key(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle DROP PRIMARY KEY operation."""
        sql = f"ALTER TABLE `{operation.table_name}` DROP PRIMARY KEY"
        logger.info(f"Dropping primary key from {operation.table_name}")
        return self.execute_sql(sql)

    def generate_create_table(
        self,
        table_name: str,
        columns: List[Dict],
        primary_keys: List[str]
    ) -> str:
        """Generate CREATE TABLE statement for MySQL."""
        col_defs = []
        for col in columns:
            col_def = self._column_to_sql(col, col.get('name', '') in primary_keys)
            col_defs.append(col_def)

        if primary_keys:
            pk_cols = ', '.join(f"`{pk}`" for pk in primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ',\n  '.join(col_defs)
        return f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  {columns_sql}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

    def generate_add_column(self, table_name: str, column: Dict) -> str:
        """Generate ADD COLUMN statement for MySQL."""
        col_def = self._column_to_sql(column, False)
        return f"ALTER TABLE `{table_name}` ADD COLUMN {col_def}"

    def generate_modify_column(
        self,
        table_name: str,
        column: Dict,
        old_type: str
    ) -> str:
        """Generate MODIFY COLUMN statement for MySQL."""
        col_def = self._column_to_sql(column, False)
        return f"ALTER TABLE `{table_name}` MODIFY COLUMN {col_def}"

    def generate_drop_column(self, table_name: str, column_name: str) -> str:
        """Generate DROP COLUMN statement for MySQL."""
        return f"ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`"

    def generate_rename_column(
        self,
        table_name: str,
        old_name: str,
        new_name: str,
        column_type: str
    ) -> str:
        """
        Generate RENAME COLUMN statement for MySQL.

        Note: MySQL 8.0+ supports RENAME COLUMN, but for compatibility
        we use CHANGE COLUMN which requires the column type.
        """
        # MySQL CHANGE syntax requires the full column definition
        return f"ALTER TABLE `{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` {column_type}"

    def generate_add_index(
        self,
        table_name: str,
        index_name: str,
        columns: List[str],
        unique: bool = False
    ) -> str:
        """Generate CREATE INDEX statement for MySQL."""
        unique_str = 'UNIQUE ' if unique else ''
        cols_str = ', '.join(f"`{col}`" for col in columns)
        return f"CREATE {unique_str}INDEX `{index_name}` ON `{table_name}` ({cols_str})"

    def generate_drop_index(self, table_name: str, index_name: str) -> str:
        """Generate DROP INDEX statement for MySQL."""
        return f"DROP INDEX `{index_name}` ON `{table_name}`"

    def _column_to_sql(self, column: Dict, is_pk: bool) -> str:
        """
        Convert column definition to MySQL DDL.

        Args:
            column: Column definition dict with keys:
                - name: Column name
                - typeName: Type name (e.g., 'VARCHAR', 'INT')
                - length: Length/precision (optional)
                - scale: Scale for numeric types (optional)
                - optional: Whether column is nullable
                - autoIncremented: Whether column auto-increments
            is_pk: Whether this column is part of primary key

        Returns:
            MySQL column definition string
        """
        name = column.get('name', 'unknown')
        col_type = self._map_to_mysql_type(column)
        nullable = 'NOT NULL' if is_pk or not column.get('optional', True) else 'NULL'
        auto_inc = 'AUTO_INCREMENT' if column.get('autoIncremented', False) else ''
        default = ''

        # Handle default values
        default_value = column.get('defaultValue') or column.get('default')
        if default_value is not None and not column.get('autoIncremented', False):
            if isinstance(default_value, str):
                default = f"DEFAULT '{default_value}'"
            elif isinstance(default_value, bool):
                default = f"DEFAULT {1 if default_value else 0}"
            else:
                default = f"DEFAULT {default_value}"

        parts = [f"`{name}`", col_type, nullable]
        if default:
            parts.append(default)
        if auto_inc:
            parts.append(auto_inc)

        return ' '.join(filter(None, parts))

    def _map_to_mysql_type(self, column: Dict) -> str:
        """
        Map column to MySQL type string.

        Handles type mapping from various source databases.
        """
        type_name = column.get('typeName', 'VARCHAR').upper()
        length = column.get('length')
        scale = column.get('scale')
        source_db = column.get('sourceDbType', 'mysql').lower()

        # If source is PostgreSQL, convert PG -> MySQL
        if source_db in ('postgresql', 'postgres'):
            return get_postgres_to_mysql_type(type_name, length, scale)

        # If source is SQL Server, convert MSSQL -> MySQL
        if source_db in ('mssql', 'sqlserver'):
            return get_mssql_to_mysql_type(type_name, length, scale)

        # Source is MySQL - direct mapping with parameters
        if type_name in ('VARCHAR', 'CHAR'):
            return f"{type_name}({length or 255})"
        elif type_name in ('DECIMAL', 'NUMERIC'):
            if length and scale:
                return f"DECIMAL({length},{scale})"
            elif length:
                return f"DECIMAL({length})"
            return "DECIMAL(10,2)"
        elif type_name in ('TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYTEXT'):
            return type_name
        elif type_name == 'JSON':
            return 'JSON'
        elif type_name == 'ENUM':
            # ENUM values should be in column.get('enumValues', [])
            enum_values = column.get('enumValues', [])
            if enum_values:
                values_str = ', '.join(f"'{v}'" for v in enum_values)
                return f"ENUM({values_str})"
            return 'VARCHAR(255)'
        elif type_name == 'BINARY' and length:
            return f"BINARY({length})"
        elif type_name == 'VARBINARY' and length:
            return f"VARBINARY({length})"
        else:
            return type_name