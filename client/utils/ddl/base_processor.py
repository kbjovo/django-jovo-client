"""
Base DDL Processor for unified DDL handling.

Provides:
- DDLOperation class for representing DDL operations
- BaseDDLProcessor abstract class for all DDL processors
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum

from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from client.models.replication import ReplicationConfig

logger = logging.getLogger(__name__)


class DDLOperationType(str, Enum):
    """DDL operation types."""
    CREATE_TABLE = 'CREATE_TABLE'
    DROP_TABLE = 'DROP_TABLE'
    ADD_COLUMN = 'ADD_COLUMN'
    DROP_COLUMN = 'DROP_COLUMN'
    MODIFY_COLUMN = 'MODIFY_COLUMN'
    RENAME_COLUMN = 'RENAME_COLUMN'
    ADD_INDEX = 'ADD_INDEX'
    DROP_INDEX = 'DROP_INDEX'
    ADD_PRIMARY_KEY = 'ADD_PRIMARY_KEY'
    DROP_PRIMARY_KEY = 'DROP_PRIMARY_KEY'
    ADD_CONSTRAINT = 'ADD_CONSTRAINT'
    DROP_CONSTRAINT = 'DROP_CONSTRAINT'
    RENAME_TABLE = 'RENAME_TABLE'


@dataclass
class DDLOperation:
    """
    Represents a DDL operation to be executed.

    Attributes:
        operation_type: Type of DDL operation
        table_name: Name of the table being modified
        details: Operation-specific details (columns, constraints, etc.)
        is_destructive: Whether the operation may cause data loss
        source_ddl: Original DDL statement (if available from source)
        schema: Schema/database name (optional)
    """
    operation_type: DDLOperationType
    table_name: str
    details: Dict[str, Any] = field(default_factory=dict)
    is_destructive: bool = False
    source_ddl: Optional[str] = None
    schema: Optional[str] = None

    # Class-level constants for operation types (backwards compatibility)
    CREATE_TABLE = DDLOperationType.CREATE_TABLE
    DROP_TABLE = DDLOperationType.DROP_TABLE
    ADD_COLUMN = DDLOperationType.ADD_COLUMN
    DROP_COLUMN = DDLOperationType.DROP_COLUMN
    MODIFY_COLUMN = DDLOperationType.MODIFY_COLUMN
    RENAME_COLUMN = DDLOperationType.RENAME_COLUMN
    ADD_INDEX = DDLOperationType.ADD_INDEX
    DROP_INDEX = DDLOperationType.DROP_INDEX
    ADD_PRIMARY_KEY = DDLOperationType.ADD_PRIMARY_KEY
    DROP_PRIMARY_KEY = DDLOperationType.DROP_PRIMARY_KEY
    ADD_CONSTRAINT = DDLOperationType.ADD_CONSTRAINT
    DROP_CONSTRAINT = DDLOperationType.DROP_CONSTRAINT
    RENAME_TABLE = DDLOperationType.RENAME_TABLE

    def __str__(self) -> str:
        return f"DDLOperation({self.operation_type.value}, {self.table_name})"


class BaseDDLProcessor(ABC):
    """
    Abstract base class for DDL processors.

    Provides unified interface for:
    - MySQL/MSSQL Kafka-based DDL processing
    - PostgreSQL schema sync service

    Subclasses must implement:
    - process(): Main processing loop
    - close(): Cleanup resources
    """

    def __init__(
        self,
        replication_config: 'ReplicationConfig',
        target_engine: Engine,
        auto_execute_destructive: bool = False
    ):
        """
        Initialize the DDL processor.

        Args:
            replication_config: ReplicationConfig instance with source/target info
            target_engine: SQLAlchemy engine for target database
            auto_execute_destructive: Whether to auto-execute destructive operations
        """
        self.replication_config = replication_config
        self.target_engine = target_engine
        self.auto_execute_destructive = auto_execute_destructive

        # Get target adapter
        self._target_adapter = None  # Lazy initialization

        logger.info(f"BaseDDLProcessor initialized for config {replication_config.id}")

    @property
    def target_adapter(self):
        """Lazy initialization of target adapter."""
        if self._target_adapter is None:
            self._target_adapter = self._get_target_adapter()
        return self._target_adapter

    def _get_target_adapter(self):
        """
        Get appropriate adapter based on target database type.

        Returns:
            Target adapter instance (MySQLTargetAdapter or PostgreSQLTargetAdapter)
        """
        from .adapters import MySQLTargetAdapter, PostgreSQLTargetAdapter

        # Get target database from client
        target_db = self.replication_config.client_database.client.client_databases.filter(
            is_target=True
        ).first()

        if not target_db:
            raise ValueError(f"No target database found for config {self.replication_config.id}")

        target_type = target_db.db_type.lower()

        if target_type == 'mysql':
            return MySQLTargetAdapter(self.target_engine)
        elif target_type in ('postgresql', 'postgres'):
            # Get schema from target database or default to 'public'
            schema = getattr(target_db, 'schema', 'public') or 'public'
            return PostgreSQLTargetAdapter(self.target_engine, schema=schema)
        else:
            raise ValueError(f"Unsupported target database type: {target_type}")

    @abstractmethod
    def process(self, timeout_sec: int = 30) -> Tuple[int, int]:
        """
        Process DDL changes.

        Args:
            timeout_sec: Processing timeout in seconds

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        pass

    @abstractmethod
    def close(self):
        """Clean up resources."""
        pass

    def execute_operation(self, operation: DDLOperation) -> Tuple[bool, Optional[str]]:
        """
        Execute a DDL operation using the target adapter.

        Args:
            operation: DDL operation to execute

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        # Check if destructive operation requires approval
        if operation.is_destructive and not self.auto_execute_destructive:
            self._log_pending_operation(operation)
            logger.warning(
                f"Destructive operation pending approval: "
                f"{operation.operation_type.value} on {operation.table_name}"
            )
            return False, "Destructive operation requires manual approval"

        try:
            logger.info(f"Executing DDL: {operation}")
            success, error = self.target_adapter.execute(operation)
            self._log_audit(operation, 'success' if success else 'failed', error)
            return success, error
        except Exception as e:
            error_msg = str(e)
            logger.error(f"DDL execution failed: {error_msg}", exc_info=True)
            self._log_audit(operation, 'failed', error_msg)
            return False, error_msg

    def _log_audit(
        self,
        operation: DDLOperation,
        status: str,
        error_message: Optional[str] = None
    ):
        """
        Log DDL operation.

        Note: Database audit table (ddl_audit_log) is disabled.
        Operations are logged via Python logger instead.

        Args:
            operation: The DDL operation that was executed
            status: Status of the operation ('success', 'failed', 'pending_approval')
            error_message: Error message if operation failed
        """
        # Log to Python logger instead of database table
        log_msg = f"DDL {status}: {operation.operation_type.value} on {operation.table_name}"
        if error_message:
            log_msg += f" - {error_message}"

        if status == 'success':
            logger.info(log_msg)
        elif status == 'failed':
            logger.error(log_msg)
        else:
            logger.warning(log_msg)

    def _log_pending_operation(self, operation: DDLOperation):
        """
        Log operation that requires manual approval.

        Args:
            operation: The DDL operation pending approval
        """
        logger.warning(
            f"Destructive DDL operation pending approval: "
            f"{operation.operation_type.value} on {operation.table_name}"
        )
        self._log_audit(operation, 'pending_approval')

    def _get_source_db_type(self) -> str:
        """Get source database type."""
        return self.replication_config.client_database.db_type.lower()

    def _get_target_db_type(self) -> str:
        """Get target database type."""
        target_db = self.replication_config.client_database.client.client_databases.filter(
            is_target=True
        ).first()
        return target_db.db_type.lower() if target_db else 'unknown'