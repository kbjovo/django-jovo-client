"""
Base adapter for target database DDL generation.

Provides abstract interface for database-specific DDL generation.
"""

import logging
from abc import ABC, abstractmethod
from typing import Tuple, Optional, List, Dict, Any, TYPE_CHECKING

from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from ..base_processor import DDLOperation

logger = logging.getLogger(__name__)


class BaseTargetAdapter(ABC):
    """
    Abstract base class for target database DDL generation.

    Implementations handle MySQL and PostgreSQL specific DDL syntax.
    """

    def __init__(self, engine: Engine, schema: Optional[str] = None):
        """
        Initialize the target adapter.

        Args:
            engine: SQLAlchemy engine for target database
            schema: Schema name (optional, defaults to database-specific default)
        """
        self.engine = engine
        self.schema = schema or self._get_default_schema()
        self.metadata = MetaData(schema=self.schema) if self.schema else MetaData()
        self._inspector = None

    @property
    def inspector(self):
        """Lazy initialization of SQLAlchemy inspector."""
        if self._inspector is None:
            self._inspector = inspect(self.engine)
        return self._inspector

    def clear_inspector_cache(self):
        """Clear the inspector cache to get fresh metadata."""
        self._inspector = None

    @property
    @abstractmethod
    def db_type(self) -> str:
        """Return database type identifier."""
        pass

    @abstractmethod
    def _get_default_schema(self) -> Optional[str]:
        """Get default schema for this database type."""
        pass

    @abstractmethod
    def execute(self, operation: 'DDLOperation') -> Tuple[bool, Optional[str]]:
        """
        Execute a DDL operation.

        Args:
            operation: DDL operation to execute

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        pass

    @abstractmethod
    def generate_create_table(
        self,
        table_name: str,
        columns: List[Dict],
        primary_keys: List[str]
    ) -> str:
        """
        Generate CREATE TABLE statement.

        Args:
            table_name: Name of the table
            columns: List of column definitions
            primary_keys: List of primary key column names

        Returns:
            CREATE TABLE SQL statement
        """
        pass

    @abstractmethod
    def generate_add_column(self, table_name: str, column: Dict) -> str:
        """
        Generate ADD COLUMN statement.

        Args:
            table_name: Name of the table
            column: Column definition

        Returns:
            ALTER TABLE ADD COLUMN SQL statement
        """
        pass

    @abstractmethod
    def generate_modify_column(
        self,
        table_name: str,
        column: Dict,
        old_type: str
    ) -> str:
        """
        Generate MODIFY/ALTER COLUMN statement.

        Args:
            table_name: Name of the table
            column: New column definition
            old_type: Old column type (for type conversion handling)

        Returns:
            ALTER TABLE MODIFY/ALTER COLUMN SQL statement
        """
        pass

    @abstractmethod
    def generate_drop_column(self, table_name: str, column_name: str) -> str:
        """
        Generate DROP COLUMN statement.

        Args:
            table_name: Name of the table
            column_name: Name of the column to drop

        Returns:
            ALTER TABLE DROP COLUMN SQL statement
        """
        pass

    def generate_rename_column(
        self,
        table_name: str,
        old_name: str,
        new_name: str,
        column_type: str
    ) -> str:
        """
        Generate RENAME COLUMN statement.

        Args:
            table_name: Name of the table
            old_name: Current column name
            new_name: New column name
            column_type: Column type (needed for some databases)

        Returns:
            ALTER TABLE RENAME COLUMN SQL statement
        """
        raise NotImplementedError("Subclass must implement generate_rename_column")

    def generate_add_index(
        self,
        table_name: str,
        index_name: str,
        columns: List[str],
        unique: bool = False
    ) -> str:
        """
        Generate CREATE INDEX statement.

        Args:
            table_name: Name of the table
            index_name: Name of the index
            columns: List of column names
            unique: Whether the index is unique

        Returns:
            CREATE INDEX SQL statement
        """
        raise NotImplementedError("Subclass must implement generate_add_index")

    def generate_drop_index(self, table_name: str, index_name: str) -> str:
        """
        Generate DROP INDEX statement.

        Args:
            table_name: Name of the table
            index_name: Name of the index

        Returns:
            DROP INDEX SQL statement
        """
        raise NotImplementedError("Subclass must implement generate_drop_index")

    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in target database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists
        """
        try:
            tables = self.inspector.get_table_names(schema=self.schema)
            return table_name in tables
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def get_table_columns(self, table_name: str) -> List[Dict]:
        """
        Get columns from existing table.

        Args:
            table_name: Name of the table

        Returns:
            List of column definitions
        """
        try:
            return self.inspector.get_columns(table_name, schema=self.schema)
        except Exception as e:
            logger.error(f"Error getting table columns: {e}")
            return []

    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns from existing table.

        Args:
            table_name: Name of the table

        Returns:
            List of primary key column names
        """
        try:
            pk_constraint = self.inspector.get_pk_constraint(table_name, schema=self.schema)
            return pk_constraint.get('constrained_columns', []) if pk_constraint else []
        except Exception as e:
            logger.error(f"Error getting primary keys: {e}")
            return []

    def execute_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Execute raw SQL statement.

        Args:
            sql: SQL statement to execute

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info(f"   Executed: {sql[:100]}{'...' if len(sql) > 100 else ''}")
            # Clear inspector cache after DDL execution to get fresh metadata
            self.clear_inspector_cache()
            return True, None
        except Exception as e:
            error_msg = str(e)
            logger.error(f"   Failed to execute SQL: {error_msg}")
            return False, error_msg

    def execute_multiple_sql(self, statements: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Execute multiple SQL statements in a transaction.

        Args:
            statements: List of SQL statements to execute

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            with self.engine.begin() as conn:
                for sql in statements:
                    conn.execute(text(sql))
                    logger.info(f"   Executed: {sql[:100]}{'...' if len(sql) > 100 else ''}")
            # Clear inspector cache after DDL execution to get fresh metadata
            self.clear_inspector_cache()
            return True, None
        except Exception as e:
            error_msg = str(e)
            logger.error(f"   Failed to execute SQL statements: {error_msg}")
            return False, error_msg