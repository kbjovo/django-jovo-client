"""
PostgreSQL target adapter for DDL generation.

Handles PostgreSQL-specific DDL syntax and type mappings.
"""

import logging
from typing import Tuple, Optional, List, Dict, TYPE_CHECKING

from .base_adapter import BaseTargetAdapter
from ..type_maps import (
    get_mysql_to_postgres_type,
    get_mssql_to_postgres_type,
)

if TYPE_CHECKING:
    from ..base_processor import DDLOperation

logger = logging.getLogger(__name__)


class PostgreSQLTargetAdapter(BaseTargetAdapter):
    """PostgreSQL-specific DDL generation and execution."""

    @property
    def db_type(self) -> str:
        return 'postgresql'

    def _get_default_schema(self) -> Optional[str]:
        return 'public'

    def _quote_identifier(self, name: str) -> str:
        """Quote identifier for PostgreSQL (uses double quotes)."""
        return f'"{name}"'

    def _full_table_name(self, table_name: str) -> str:
        """Get fully qualified table name with schema."""
        if self.schema:
            return f'"{self.schema}"."{table_name}"'
        return f'"{table_name}"'

    def execute(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Execute DDL operation for PostgreSQL target."""
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
        full_table = self._full_table_name(operation.table_name)
        sql = f"DROP TABLE IF EXISTS {full_table} CASCADE"
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
        """
        Handle MODIFY COLUMN operation.

        PostgreSQL requires separate statements for:
        1. Type change (ALTER COLUMN ... TYPE)
        2. Nullability change (ALTER COLUMN ... SET/DROP NOT NULL)
        3. Default change (ALTER COLUMN ... SET/DROP DEFAULT)
        """
        column = operation.details.get('column', {})
        old_type = operation.details.get('old_type', '')
        statements = self._generate_modify_statements(
            operation.table_name,
            column,
            old_type
        )
        logger.info(f"Modifying column in {operation.table_name}")
        return self.execute_multiple_sql(statements)

    def _handle_rename_column(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle RENAME COLUMN operation."""
        details = operation.details
        old_name = details.get('old_name')
        new_name = details.get('new_name')

        # Check if source column exists (skip if already renamed or stale DDL event)
        existing_columns = {c['name'] for c in self.get_table_columns(operation.table_name)}
        if old_name not in existing_columns:
            if new_name in existing_columns:
                logger.info(f"Column {old_name} already renamed to {new_name}, skipping")
            else:
                logger.info(f"Column {old_name} not found (stale DDL event), skipping")
            return True, None

        sql = self.generate_rename_column(
            operation.table_name,
            old_name,
            new_name,
            details.get('column_type', '')  # Not needed for PostgreSQL
        )
        logger.info(f"Renaming column in {operation.table_name}")
        return self.execute_sql(sql)

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
        pk_cols = ', '.join(f'"{col}"' for col in columns)
        full_table = self._full_table_name(operation.table_name)
        sql = f"ALTER TABLE {full_table} ADD PRIMARY KEY ({pk_cols})"
        logger.info(f"Adding primary key to {operation.table_name}")
        return self.execute_sql(sql)

    def _handle_drop_primary_key(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """Handle DROP PRIMARY KEY operation."""
        full_table = self._full_table_name(operation.table_name)
        # PostgreSQL requires constraint name; default is {table}_pkey
        constraint_name = f"{operation.table_name}_pkey"
        sql = f'ALTER TABLE {full_table} DROP CONSTRAINT IF EXISTS "{constraint_name}"'
        logger.info(f"Dropping primary key from {operation.table_name}")
        return self.execute_sql(sql)

    def generate_create_table(
        self,
        table_name: str,
        columns: List[Dict],
        primary_keys: List[str]
    ) -> str:
        """Generate CREATE TABLE statement for PostgreSQL."""
        col_defs = []
        for col in columns:
            col_def = self._column_to_sql(col, col.get('name', '') in primary_keys)
            col_defs.append(col_def)

        if primary_keys:
            pk_cols = ', '.join(f'"{pk}"' for pk in primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ',\n  '.join(col_defs)
        full_table = self._full_table_name(table_name)
        return f"CREATE TABLE IF NOT EXISTS {full_table} (\n  {columns_sql}\n)"

    def generate_add_column(self, table_name: str, column: Dict) -> str:
        """Generate ADD COLUMN statement for PostgreSQL."""
        col_def = self._column_to_sql(column, False)
        full_table = self._full_table_name(table_name)
        return f"ALTER TABLE {full_table} ADD COLUMN {col_def}"

    def generate_modify_column(
        self,
        table_name: str,
        column: Dict,
        old_type: str
    ) -> str:
        """
        Generate ALTER COLUMN TYPE statement for PostgreSQL.

        Note: PostgreSQL requires separate statements for type, nullability,
        and default changes. This method only handles type change.
        Use _generate_modify_statements() for complete handling.
        """
        statements = self._generate_modify_statements(table_name, column, old_type)
        # Return first statement (type change) for backwards compatibility
        return statements[0] if statements else ''

    def _generate_modify_statements(
        self,
        table_name: str,
        column: Dict,
        old_type: str
    ) -> List[str]:
        """
        Generate all ALTER statements needed to modify a column.

        PostgreSQL requires separate statements for:
        1. Type change
        2. Nullability change
        3. Default change
        """
        statements = []
        name = column.get('name', 'unknown')
        new_type = self._map_to_pg_type(column)
        full_table = self._full_table_name(table_name)

        # Type change with USING clause if needed
        using_clause = ''
        if self._needs_using_clause(old_type, new_type):
            using_clause = f' USING "{name}"::{new_type}'

        statements.append(
            f'ALTER TABLE {full_table} ALTER COLUMN "{name}" TYPE {new_type}{using_clause}'
        )

        # Nullability change
        is_nullable = column.get('optional', True)
        if not is_nullable:
            statements.append(
                f'ALTER TABLE {full_table} ALTER COLUMN "{name}" SET NOT NULL'
            )

        return statements

    def generate_drop_column(self, table_name: str, column_name: str) -> str:
        """Generate DROP COLUMN statement for PostgreSQL."""
        full_table = self._full_table_name(table_name)
        return f'ALTER TABLE {full_table} DROP COLUMN IF EXISTS "{column_name}"'

    def generate_rename_column(
        self,
        table_name: str,
        old_name: str,
        new_name: str,
        column_type: str
    ) -> str:
        """Generate RENAME COLUMN statement for PostgreSQL."""
        full_table = self._full_table_name(table_name)
        return f'ALTER TABLE {full_table} RENAME COLUMN "{old_name}" TO "{new_name}"'

    def generate_add_index(
        self,
        table_name: str,
        index_name: str,
        columns: List[str],
        unique: bool = False
    ) -> str:
        """Generate CREATE INDEX statement for PostgreSQL."""
        unique_str = 'UNIQUE ' if unique else ''
        cols_str = ', '.join(f'"{col}"' for col in columns)
        full_table = self._full_table_name(table_name)
        return f"CREATE {unique_str}INDEX IF NOT EXISTS {index_name} ON {full_table} ({cols_str})"

    def generate_drop_index(self, table_name: str, index_name: str) -> str:
        """Generate DROP INDEX statement for PostgreSQL."""
        # In PostgreSQL, indexes are schema-qualified
        if self.schema:
            return f'DROP INDEX IF EXISTS "{self.schema}"."{index_name}"'
        return f'DROP INDEX IF EXISTS "{index_name}"'

    def _column_to_sql(self, column: Dict, is_pk: bool) -> str:
        """
        Convert column definition to PostgreSQL DDL.

        Args:
            column: Column definition dict
            is_pk: Whether this column is part of primary key

        Returns:
            PostgreSQL column definition string
        """
        name = column.get('name', 'unknown')
        col_type = self._map_to_pg_type(column)
        nullable = 'NOT NULL' if is_pk or not column.get('optional', True) else ''

        # PostgreSQL uses SERIAL/BIGSERIAL for auto-increment
        if column.get('autoIncremented', False):
            if 'BIGINT' in col_type.upper() or 'INT8' in col_type.upper():
                col_type = 'BIGSERIAL'
            else:
                col_type = 'SERIAL'
            # SERIAL implies NOT NULL
            nullable = ''

        # Handle default values
        default = ''
        default_value = column.get('defaultValue') or column.get('default')
        if default_value is not None and not column.get('autoIncremented', False):
            if isinstance(default_value, str):
                default = f"DEFAULT '{default_value}'"
            elif isinstance(default_value, bool):
                default = f"DEFAULT {'TRUE' if default_value else 'FALSE'}"
            else:
                default = f"DEFAULT {default_value}"

        parts = [f'"{name}"', col_type]
        if nullable:
            parts.append(nullable)
        if default:
            parts.append(default)

        return ' '.join(filter(None, parts))

    def _map_to_pg_type(self, column: Dict) -> str:
        """
        Map column to PostgreSQL type string.

        Handles type mapping from various source databases.
        """
        type_name = column.get('typeName', 'VARCHAR')
        length = column.get('length')
        scale = column.get('scale')
        source_db = column.get('sourceDbType', 'postgresql').lower()

        # If source is MySQL, convert MySQL -> PostgreSQL
        if source_db == 'mysql':
            return get_mysql_to_postgres_type(type_name, length, scale)

        # If source is SQL Server, convert MSSQL -> PostgreSQL
        if source_db in ('mssql', 'sqlserver'):
            return get_mssql_to_postgres_type(type_name, length, scale)

        # Source is PostgreSQL - direct mapping with parameters
        type_upper = type_name.upper()

        # PostgreSQL type mappings
        pg_type_mappings = {
            'TINYINT': 'SMALLINT',
            'MEDIUMINT': 'INTEGER',
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'FLOAT': 'REAL',
            'DOUBLE': 'DOUBLE PRECISION',
            'DATETIME': 'TIMESTAMP',
            'TINYTEXT': 'TEXT',
            'MEDIUMTEXT': 'TEXT',
            'LONGTEXT': 'TEXT',
            'BLOB': 'BYTEA',
            'TINYBLOB': 'BYTEA',
            'MEDIUMBLOB': 'BYTEA',
            'LONGBLOB': 'BYTEA',
            'BINARY': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'JSON': 'JSONB',
            'YEAR': 'SMALLINT',
        }

        pg_type = pg_type_mappings.get(type_upper, type_upper)

        # Add parameters where applicable
        if pg_type in ('VARCHAR', 'CHAR', 'CHARACTER VARYING') and length:
            return f"VARCHAR({length})"
        elif pg_type in ('DECIMAL', 'NUMERIC') and length:
            if scale:
                return f"NUMERIC({length},{scale})"
            return f"NUMERIC({length})"
        elif pg_type == 'VARCHAR' and not length:
            return 'TEXT'  # PostgreSQL prefers TEXT over unbounded VARCHAR

        return pg_type

    def _needs_using_clause(self, old_type: str, new_type: str) -> bool:
        """
        Check if type conversion needs USING clause.

        PostgreSQL requires explicit casting for incompatible type conversions.
        """
        if not old_type:
            return False

        incompatible_pairs = [
            ('VARCHAR', 'INTEGER'), ('VARCHAR', 'NUMERIC'), ('VARCHAR', 'BIGINT'),
            ('VARCHAR', 'SMALLINT'), ('VARCHAR', 'BOOLEAN'), ('VARCHAR', 'DATE'),
            ('TEXT', 'INTEGER'), ('TEXT', 'NUMERIC'), ('TEXT', 'BIGINT'),
            ('TEXT', 'SMALLINT'), ('TEXT', 'BOOLEAN'), ('TEXT', 'DATE'),
            ('INTEGER', 'VARCHAR'), ('INTEGER', 'TEXT'),
            ('NUMERIC', 'VARCHAR'), ('NUMERIC', 'TEXT'),
            ('BIGINT', 'VARCHAR'), ('BIGINT', 'TEXT'),
            ('BOOLEAN', 'VARCHAR'), ('BOOLEAN', 'TEXT'),
            ('TIMESTAMP', 'DATE'), ('DATE', 'TIMESTAMP'),
        ]

        old = old_type.upper().split('(')[0].strip()
        new = new_type.upper().split('(')[0].strip()

        # Check both directions
        return (old, new) in incompatible_pairs or (new, old) in incompatible_pairs