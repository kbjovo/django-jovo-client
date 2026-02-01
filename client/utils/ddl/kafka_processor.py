"""
Kafka-based DDL Processor for MySQL and SQL Server sources.

Consumes Debezium schema history topic and applies DDL changes to target database.

Supports:
- MySQL: Full DDL with SQL statements in 'ddl' field
- SQL Server: Schema metadata in 'tableChanges' (ddl field is null)
"""

import json
import logging
import re
from typing import Dict, List, Optional, Tuple, Any

from confluent_kafka import Consumer, KafkaException, KafkaError
from sqlalchemy.engine import Engine

from .base_processor import BaseDDLProcessor, DDLOperation, DDLOperationType
from .type_maps import get_type_map
from client.models.replication import ReplicationConfig
from jovoclient.utils.debezium.schema_registry_utils import delete_table_schemas

logger = logging.getLogger(__name__)


class KafkaDDLProcessor(BaseDDLProcessor):
    """
    Process DDL changes from Kafka schema history topic.

    Supports:
    - MySQL: Full DDL with SQL statements
    - SQL Server: Schema metadata (tableChanges) without raw DDL
    """

    def __init__(
        self,
        replication_config: ReplicationConfig,
        target_engine: Engine,
        bootstrap_servers: str,
        auto_execute_destructive: bool = False,
        consumer_group: Optional[str] = None
    ):
        """
        Initialize Kafka DDL Processor.

        Args:
            replication_config: ReplicationConfig instance
            target_engine: SQLAlchemy engine for target database
            bootstrap_servers: Kafka bootstrap servers
            auto_execute_destructive: Whether to auto-execute destructive operations
            consumer_group: Kafka consumer group ID (optional)
        """
        super().__init__(replication_config, target_engine, auto_execute_destructive)

        self.bootstrap_servers = bootstrap_servers
        self.source_db_type = replication_config.client_database.db_type.lower()

        # Build schema history topic name
        # Format: schema-history.client_{client_id}_db_{db_id}_v_{version}
        client = replication_config.client_database.client
        db_config = replication_config.client_database
        version = replication_config.connector_version or 0

        self.schema_topic = f"schema-history.client_{client.id}_db_{db_config.id}_v_{version}"

        # Initialize Kafka consumer
        group_id = consumer_group or f'ddl-processor-config-{replication_config.id}'
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000,
        })
        self.consumer.subscribe([self.schema_topic])

        # Cache for table schemas (used for detecting changes)
        self.schema_cache: Dict[str, Dict] = {}

        # Track processed tables to avoid duplicate processing
        self._processed_tables: set = set()

        logger.info(f"KafkaDDLProcessor initialized for {self.source_db_type}")
        logger.info(f"   Schema topic: {self.schema_topic}")
        logger.info(f"   Consumer group: {group_id}")

    def process(self, timeout_sec: int = 30, max_messages: int = 100) -> Tuple[int, int]:
        """
        Process DDL messages from Kafka.

        Args:
            timeout_sec: Timeout for polling messages
            max_messages: Maximum messages to process per call

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        processed = 0
        errors = 0

        try:
            messages_read = 0
            while messages_read < max_messages:
                msg = self.consumer.poll(timeout=timeout_sec if messages_read == 0 else 1.0)

                if msg is None:
                    # No more messages
                    break

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition
                        break
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                messages_read += 1

                try:
                    value = msg.value()
                    if value is None:
                        continue

                    message = json.loads(value.decode('utf-8'))
                    success = self._process_message(message)

                    if success:
                        processed += 1
                    else:
                        errors += 1

                    # Commit after each message
                    self.consumer.commit(msg)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse DDL message: {e}")
                    errors += 1
                except Exception as e:
                    logger.error(f"Error processing DDL message: {e}", exc_info=True)
                    errors += 1

        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")

        if processed > 0 or errors > 0:
            logger.info(f"DDL processing complete: {processed} processed, {errors} errors")

        return processed, errors

    def _process_message(self, message: Dict) -> bool:
        """
        Process a single schema change message.

        Debezium schema history message format:
        {
            "source": {...},
            "position": {...},
            "databaseName": "mydb",
            "ddl": "ALTER TABLE ...",  // MySQL only, null for MSSQL
            "tableChanges": [
                {
                    "type": "ALTER",
                    "id": "\"mydb\".\"mytable\"",
                    "table": {
                        "columns": [...],
                        "primaryKeyColumnNames": [...]
                    }
                }
            ]
        }
        """
        ddl = message.get('ddl')
        table_changes = message.get('tableChanges', [])
        database_name = message.get('databaseName', '')

        # Log incoming DDL for debugging
        if ddl:
            logger.info(f"Processing DDL message: {ddl[:80]}...")

        # Skip non-DDL messages
        if not ddl and not table_changes:
            return True

        # Skip system database changes
        if database_name.lower() in ('mysql', 'information_schema', 'performance_schema', 'sys'):
            return True

        if self.source_db_type == 'mysql' and ddl:
            # MySQL: Use raw DDL
            return self._process_mysql_ddl(ddl, table_changes)
        elif self.source_db_type in ('mssql', 'sqlserver'):
            # SQL Server: Use tableChanges metadata
            return self._process_mssql_schema_change(table_changes)
        else:
            logger.warning(f"Unsupported source type for Kafka DDL: {self.source_db_type}")
            return False

    def _process_mysql_ddl(self, ddl: str, table_changes: List[Dict]) -> bool:
        """
        Process MySQL DDL from raw SQL statement.

        Uses combination of DDL parsing and tableChanges for accurate processing.
        """
        ddl_upper = ddl.upper().strip()

        # Skip DDL that sink connector handles or is not relevant
        # CREATE TABLE - handled by sink connector (auto.create=true)
        # DROP TABLE - dangerous, let sink connector handle gracefully
        skip_patterns = [
            'CREATE TABLE',  # Sink connector creates tables automatically
            'DROP TABLE',    # Dangerous - skip unless explicitly needed
            'CREATE DATABASE', 'DROP DATABASE', 'USE ',
            'CREATE USER', 'DROP USER', 'GRANT ', 'REVOKE ',
            'CREATE PROCEDURE', 'DROP PROCEDURE',
            'CREATE FUNCTION', 'DROP FUNCTION',
            'CREATE TRIGGER', 'DROP TRIGGER',
            'CREATE VIEW', 'DROP VIEW',
            'SET ', 'FLUSH ', 'ANALYZE ', 'OPTIMIZE ',
            'TRUNCATE',  # Data operation, not schema
        ]

        for pattern in skip_patterns:
            if ddl_upper.startswith(pattern):
                logger.debug(f"Skipping non-table DDL: {ddl[:50]}...")
                return True

        # Parse DDL type
        if ddl_upper.startswith('CREATE TABLE'):
            return self._handle_create_from_changes(table_changes)
        elif ddl_upper.startswith('ALTER TABLE'):
            return self._handle_alter_table(ddl, table_changes)
        elif ddl_upper.startswith('DROP TABLE'):
            return self._handle_drop_table(ddl)
        elif ddl_upper.startswith('RENAME TABLE'):
            return self._handle_rename_table(ddl)
        elif ddl_upper.startswith('TRUNCATE'):
            # Truncate doesn't change schema, but log it
            logger.info(f"TRUNCATE detected (no schema change): {ddl[:50]}...")
            return True
        else:
            logger.debug(f"Ignoring DDL: {ddl[:50]}...")
            return True

    def _process_mssql_schema_change(self, table_changes: List[Dict]) -> bool:
        """
        Process SQL Server schema changes from tableChanges metadata.

        SQL Server doesn't provide raw DDL, so we reconstruct from metadata.
        """
        if not table_changes:
            return True

        for change in table_changes:
            change_type = change.get('type', '').upper()
            table_info = change.get('table', {})
            table_id = change.get('id', '')

            if not table_info:
                continue

            table_name = self._extract_table_name(table_id)

            if change_type == 'CREATE':
                # Skip CREATE - sink connector handles table creation (auto.create=true)
                logger.debug(f"Skipping CREATE TABLE for {table_name} (sink connector handles this)")
                continue
            elif change_type == 'ALTER':
                success = self._handle_alter_from_metadata(table_name, table_info)
            elif change_type == 'DROP':
                # Skip DROP - dangerous, let sink connector handle gracefully
                logger.debug(f"Skipping DROP TABLE for {table_name}")
                continue
            else:
                logger.debug(f"Unknown change type: {change_type}")
                continue

            if not success:
                return False

        return True

    def _handle_create_from_changes(self, table_changes: List[Dict]) -> bool:
        """Handle CREATE TABLE from tableChanges metadata."""
        for change in table_changes:
            if change.get('type', '').upper() != 'CREATE':
                continue

            table_info = change.get('table', {})
            if not table_info:
                continue

            table_id = change.get('id', '')
            table_name = self._extract_table_name(table_id)

            return self._handle_create_from_metadata(table_name, table_info)

        return True

    def _handle_create_from_metadata(self, table_name: str, table_info: Dict) -> bool:
        """Create table from metadata."""
        columns = table_info.get('columns', [])
        pk_columns = table_info.get('primaryKeyColumnNames', [])

        # Add source db type to columns for proper type mapping
        for col in columns:
            col['sourceDbType'] = self.source_db_type

        operation = DDLOperation(
            operation_type=DDLOperationType.CREATE_TABLE,
            table_name=self._get_target_table_name(table_name),
            details={
                'columns': columns,
                'primary_keys': pk_columns
            },
            is_destructive=False
        )

        success, error = self.execute_operation(operation)
        if success:
            # Cache schema
            self.schema_cache[table_name] = table_info

        return success

    def _handle_alter_table(self, ddl: str, table_changes: List[Dict]) -> bool:
        """Handle ALTER TABLE from DDL and tableChanges."""
        logger.info(f"Handling ALTER TABLE, tableChanges count: {len(table_changes)}")

        # Extract table name from tableChanges (more reliable than DDL parsing)
        source_table_name = None
        for change in table_changes:
            if change.get('type', '').upper() == 'ALTER':
                table_id = change.get('id', '')
                if table_id:
                    source_table_name = self._extract_table_name(table_id)
                    logger.info(f"Found ALTER change for table: {source_table_name}")
                    break

        if not source_table_name:
            # Fallback to DDL parsing if tableChanges not available
            match = re.search(r'ALTER\s+TABLE\s+(?:`?[\w]+`?\.)?`?(\w+)`?', ddl, re.IGNORECASE)
            if not match:
                return True
            source_table_name = match.group(1)

        target_table_name = self._get_target_table_name(source_table_name)

        # Check if table exists in target database
        # If not, skip ALTER operations - sink connector will create it with auto.create=true
        if not self.target_adapter.table_exists(target_table_name):
            logger.info(
                f"Skipping ALTER for {target_table_name} - table doesn't exist yet "
                "(sink connector will create it)"
            )
            # Update cache with new schema so we don't try to process again
            for change in table_changes:
                if change.get('type', '').upper() in ('ALTER', 'CREATE'):
                    self.schema_cache[source_table_name] = change.get('table', {})
                    break
            return True

        # Get new schema from tableChanges
        new_schema = None
        for change in table_changes:
            if change.get('type', '').upper() == 'ALTER':
                new_schema = change.get('table', {})
                break

        if not new_schema:
            # No schema change info, try to parse DDL directly
            logger.info(f"No tableChanges schema, falling back to DDL parsing")
            return self._handle_alter_from_ddl(ddl, source_table_name, target_table_name)

        # Query target table's actual current columns instead of using cache
        # This allows position-based rename detection
        target_columns = self.target_adapter.get_table_columns(target_table_name)
        source_columns = new_schema.get('columns', [])

        # Log columns for debugging (info level for troubleshooting)
        target_col_names = [c.get('name') for c in target_columns]
        source_col_names = [c.get('name') for c in source_columns]
        logger.info(f"Schema comparison for {target_table_name}:")
        logger.info(f"  Target columns ({len(target_columns)}): {target_col_names}")
        logger.info(f"  Source columns ({len(source_columns)}): {source_col_names}")

        # Detect renames by comparing positions
        operations = self._detect_schema_changes_by_position(
            target_table_name, target_columns, source_columns
        )

        if operations:
            logger.info(f"Detected {len(operations)} schema changes for {target_table_name}: "
                       f"{[str(op) for op in operations]}")
        else:
            logger.info(f"No schema changes detected for {target_table_name}")

        # Check if any operations would cause schema compatibility issues
        # Column renames, drops, or significant type changes are breaking changes in Avro
        has_breaking_changes = any(
            op.operation_type in (
                DDLOperationType.RENAME_COLUMN,
                DDLOperationType.DROP_COLUMN,
            )
            for op in operations
        )

        if has_breaking_changes:
            logger.info(
                f"Breaking schema changes detected for {source_table_name}. "
                "Deleting schema subjects to prevent compatibility issues."
            )
            self._delete_schema_for_table(source_table_name)

        for op in operations:
            success, error = self.execute_operation(op)
            if not success:
                logger.error(f"Failed ALTER operation on {target_table_name}: {error}")
                return False

        # Update cache
        self.schema_cache[source_table_name] = new_schema
        return True

    def _handle_alter_from_ddl(self, ddl: str, source_table: str, target_table: str) -> bool:
        """Parse ALTER TABLE DDL directly when tableChanges not available."""
        # Check if table exists in target database
        if not self.target_adapter.table_exists(target_table):
            logger.info(
                f"Skipping ALTER for {target_table} - table doesn't exist yet "
                "(sink connector will create it)"
            )
            return True

        ddl_upper = ddl.upper()

        # ADD COLUMN
        add_match = re.search(
            r'ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+(\w+(?:\([^)]+\))?)',
            ddl, re.IGNORECASE
        )
        if add_match:
            col_name = add_match.group(1)
            col_type = add_match.group(2)
            operation = DDLOperation(
                operation_type=DDLOperationType.ADD_COLUMN,
                table_name=target_table,
                details={
                    'column': {
                        'name': col_name,
                        'typeName': col_type.split('(')[0].upper(),
                        'length': self._extract_length(col_type),
                        'optional': 'NOT NULL' not in ddl_upper,
                        'sourceDbType': self.source_db_type
                    }
                },
                is_destructive=False,
                source_ddl=ddl
            )
            success, _ = self.execute_operation(operation)
            return success

        # DROP COLUMN - handle both "DROP COLUMN col" and "DROP col"
        drop_match = re.search(r'DROP\s+(?:COLUMN\s+)?`?(\w+)`?', ddl, re.IGNORECASE)
        if drop_match and ('DROP COLUMN' in ddl_upper or 'DROP `' in ddl_upper or re.search(r'DROP\s+\w', ddl_upper)):
            col_name = drop_match.group(1)
            # Skip if it looks like DROP INDEX/KEY/PRIMARY
            if col_name.upper() in ('INDEX', 'KEY', 'PRIMARY', 'FOREIGN', 'CONSTRAINT'):
                pass  # Not a column drop
            else:
                operation = DDLOperation(
                    operation_type=DDLOperationType.DROP_COLUMN,
                    table_name=target_table,
                    details={'column_name': col_name},
                    is_destructive=True,
                    source_ddl=ddl
                )
                success, _ = self.execute_operation(operation)
                return success

        # MODIFY/CHANGE COLUMN
        modify_match = re.search(
            r'(?:MODIFY|CHANGE)\s+(?:COLUMN\s+)?`?(\w+)`?\s+(?:`?(\w+)`?\s+)?(\w+(?:\([^)]+\))?)',
            ddl, re.IGNORECASE
        )
        if modify_match:
            old_name = modify_match.group(1)
            new_name = modify_match.group(2) or old_name
            col_type = modify_match.group(3)

            if old_name != new_name:
                # Column rename detected - this is a breaking schema change in Avro
                # Delete schema subjects from Schema Registry to allow new schema registration
                logger.info(
                    f"Column rename detected: {old_name} -> {new_name} in {source_table}. "
                    "Deleting schema subjects to prevent compatibility issues."
                )
                self._delete_schema_for_table(source_table)

                # Rename column
                operation = DDLOperation(
                    operation_type=DDLOperationType.RENAME_COLUMN,
                    table_name=target_table,
                    details={
                        'old_name': old_name,
                        'new_name': new_name,
                        'column_type': col_type
                    },
                    is_destructive=False,
                    source_ddl=ddl
                )
            else:
                # Modify column type
                operation = DDLOperation(
                    operation_type=DDLOperationType.MODIFY_COLUMN,
                    table_name=target_table,
                    details={
                        'column': {
                            'name': old_name,
                            'typeName': col_type.split('(')[0].upper(),
                            'length': self._extract_length(col_type),
                            'optional': 'NOT NULL' not in ddl_upper,
                            'sourceDbType': self.source_db_type
                        },
                        'old_type': ''
                    },
                    is_destructive=False,
                    source_ddl=ddl
                )
            success, _ = self.execute_operation(operation)
            return success

        # ADD INDEX
        index_match = re.search(
            r'ADD\s+(UNIQUE\s+)?(?:INDEX|KEY)\s+`?(\w+)`?\s*\(([^)]+)\)',
            ddl, re.IGNORECASE
        )
        if index_match:
            unique = bool(index_match.group(1))
            index_name = index_match.group(2)
            columns = [c.strip().strip('`') for c in index_match.group(3).split(',')]
            operation = DDLOperation(
                operation_type=DDLOperationType.ADD_INDEX,
                table_name=target_table,
                details={
                    'index_name': index_name,
                    'columns': columns,
                    'unique': unique
                },
                is_destructive=False,
                source_ddl=ddl
            )
            success, _ = self.execute_operation(operation)
            return success

        # DROP INDEX
        drop_idx_match = re.search(r'DROP\s+(?:INDEX|KEY)\s+`?(\w+)`?', ddl, re.IGNORECASE)
        if drop_idx_match:
            index_name = drop_idx_match.group(1)
            operation = DDLOperation(
                operation_type=DDLOperationType.DROP_INDEX,
                table_name=target_table,
                details={'index_name': index_name},
                is_destructive=False,
                source_ddl=ddl
            )
            success, _ = self.execute_operation(operation)
            return success

        logger.debug(f"Unhandled ALTER TABLE DDL: {ddl[:100]}...")
        return True

    def _handle_alter_from_metadata(self, table_name: str, table_info: Dict) -> bool:
        """Handle ALTER TABLE from metadata only (SQL Server)."""
        target_table = self._get_target_table_name(table_name)

        # Check if table exists in target database
        # If not, skip ALTER operations - sink connector will create it with auto.create=true
        if not self.target_adapter.table_exists(target_table):
            logger.info(
                f"Skipping ALTER for {target_table} - table doesn't exist yet "
                "(sink connector will create it)"
            )
            # Update cache with new schema so we don't try to process again
            self.schema_cache[table_name] = table_info
            return True

        # Query actual target columns instead of using cache (more reliable)
        # This handles cases where cache is empty (first run/restart) or stale
        target_columns = self.target_adapter.get_table_columns(target_table)
        source_columns = table_info.get('columns', [])

        # Log columns for debugging (info level for troubleshooting)
        target_col_names = [c.get('name') for c in target_columns]
        source_col_names = [c.get('name') for c in source_columns]
        logger.info(f"Schema comparison for {target_table} (MSSQL):")
        logger.info(f"  Target columns ({len(target_columns)}): {target_col_names}")
        logger.info(f"  Source columns ({len(source_columns)}): {source_col_names}")

        # Use position-based detection (same as MySQL) for accurate change detection
        operations = self._detect_schema_changes_by_position(
            target_table, target_columns, source_columns
        )

        if operations:
            logger.info(f"Detected {len(operations)} schema changes for {target_table}: "
                       f"{[str(op) for op in operations]}")
        else:
            logger.info(f"No schema changes detected for {target_table}")

        # Check if any operations would cause schema compatibility issues
        has_breaking_changes = any(
            op.operation_type in (
                DDLOperationType.RENAME_COLUMN,
                DDLOperationType.DROP_COLUMN,
            )
            for op in operations
        )

        if has_breaking_changes:
            logger.info(
                f"Breaking schema changes detected for {table_name}. "
                "Deleting schema subjects to prevent compatibility issues."
            )
            self._delete_schema_for_table(table_name)

        for op in operations:
            success, error = self.execute_operation(op)
            if not success:
                logger.error(f"Failed ALTER operation on {target_table}: {error}")
                return False

        self.schema_cache[table_name] = table_info
        return True

    def _handle_drop_table(self, ddl: str) -> bool:
        """Handle DROP TABLE."""
        match = re.search(
            r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?',
            ddl, re.IGNORECASE
        )
        if not match:
            return True

        source_table = match.group(1)
        target_table = self._get_target_table_name(source_table)

        operation = DDLOperation(
            operation_type=DDLOperationType.DROP_TABLE,
            table_name=target_table,
            details={},
            is_destructive=True,
            source_ddl=ddl
        )

        success, error = self.execute_operation(operation)

        # Remove from cache
        if source_table in self.schema_cache:
            del self.schema_cache[source_table]

        return success

    def _handle_rename_table(self, ddl: str) -> bool:
        """Handle RENAME TABLE."""
        # MySQL: RENAME TABLE old_name TO new_name
        match = re.search(
            r'RENAME\s+TABLE\s+`?(\w+)`?\s+TO\s+`?(\w+)`?',
            ddl, re.IGNORECASE
        )
        if not match:
            return True

        old_name = match.group(1)
        new_name = match.group(2)

        operation = DDLOperation(
            operation_type=DDLOperationType.RENAME_TABLE,
            table_name=self._get_target_table_name(old_name),
            details={'new_name': self._get_target_table_name(new_name)},
            is_destructive=False,
            source_ddl=ddl
        )

        # Note: RENAME_TABLE not implemented in adapters yet
        logger.warning(f"RENAME TABLE not fully supported: {ddl}")
        return True

    def _detect_schema_changes(
        self,
        table_name: str,
        old_schema: Dict,
        new_schema: Dict
    ) -> List[DDLOperation]:
        """
        Detect schema changes between old and new schema.

        Returns list of DDL operations to apply.
        """
        operations = []

        old_columns = {c['name']: c for c in old_schema.get('columns', [])}
        new_columns = {c['name']: c for c in new_schema.get('columns', [])}

        # Add source db type to new columns
        for col in new_columns.values():
            col['sourceDbType'] = self.source_db_type

        # Detect added columns
        for col_name, col_info in new_columns.items():
            if col_name not in old_columns:
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.ADD_COLUMN,
                    table_name=table_name,
                    details={'column': col_info},
                    is_destructive=False
                ))

        # Detect dropped columns
        for col_name in old_columns:
            if col_name not in new_columns:
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.DROP_COLUMN,
                    table_name=table_name,
                    details={'column_name': col_name},
                    is_destructive=True
                ))

        # Detect modified columns
        for col_name, new_col in new_columns.items():
            if col_name in old_columns:
                old_col = old_columns[col_name]
                if self._column_changed(old_col, new_col):
                    operations.append(DDLOperation(
                        operation_type=DDLOperationType.MODIFY_COLUMN,
                        table_name=table_name,
                        details={
                            'column': new_col,
                            'old_type': old_col.get('typeName', '')
                        },
                        is_destructive=False
                    ))

        return operations

    def _column_changed(self, old_col: Dict, new_col: Dict) -> bool:
        """Check if column definition changed."""
        return (
            old_col.get('typeName') != new_col.get('typeName') or
            old_col.get('length') != new_col.get('length') or
            old_col.get('scale') != new_col.get('scale') or
            old_col.get('optional') != new_col.get('optional')
        )

    def _detect_schema_changes_by_position(
        self,
        table_name: str,
        target_columns: List[Dict],
        source_columns: List[Dict]
    ) -> List[DDLOperation]:
        """
        Detect schema changes by comparing actual target and source columns by position.
        Detects renames when same position has different name but similar type.
        """
        operations = []

        target_by_name = {c['name']: c for c in target_columns}
        source_by_name = {c['name']: c for c in source_columns}

        for col in source_columns:
            col['sourceDbType'] = self.source_db_type

        matched_target = set()
        matched_source = set()

        # Detect renames by position
        for pos, source_col in enumerate(source_columns):
            source_name = source_col['name']

            if source_name in target_by_name:
                matched_target.add(source_name)
                matched_source.add(source_name)
                continue

            if pos < len(target_columns):
                target_col = target_columns[pos]
                target_name = target_col['name']

                if target_name not in source_by_name and target_name not in matched_target:
                    source_type = source_col.get('typeName', '').upper()
                    target_type = str(target_col.get('type', '')).upper().split('(')[0]

                    if self._types_are_similar(source_type, target_type):
                        logger.info(f"Column rename detected: {target_name} -> {source_name}")
                        operations.append(DDLOperation(
                            operation_type=DDLOperationType.RENAME_COLUMN,
                            table_name=table_name,
                            details={
                                'old_name': target_name,
                                'new_name': source_name,
                                'column_type': source_col.get('typeName', ''),
                                'column': source_col
                            },
                            is_destructive=False
                        ))
                        matched_target.add(target_name)
                        matched_source.add(source_name)
                        continue

        # Detect added columns
        for source_col in source_columns:
            if source_col['name'] not in matched_source:
                logger.info(f"Column add detected: {source_col['name']} not in target")
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.ADD_COLUMN,
                    table_name=table_name,
                    details={'column': source_col},
                    is_destructive=False
                ))

        # Detect dropped columns
        for target_col in target_columns:
            if target_col['name'] not in matched_target:
                logger.info(f"Column drop detected: {target_col['name']} not in source")
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.DROP_COLUMN,
                    table_name=table_name,
                    details={'column_name': target_col['name']},
                    is_destructive=True
                ))

        return operations

    def _types_are_similar(self, source_type: str, target_type: str) -> bool:
        """Check if two column types are similar enough to consider a rename."""
        if source_type.upper() == target_type.upper():
            return True

        equivalents = [
            {'INT', 'INTEGER', 'INT4'},
            {'BIGINT', 'INT8'},
            {'SMALLINT', 'INT2'},
            {'VARCHAR', 'CHARACTER VARYING', 'TEXT'},
            {'TIMESTAMP', 'DATETIME'},
            {'BOOL', 'BOOLEAN', 'TINYINT'},
            {'FLOAT', 'REAL', 'FLOAT4'},
            {'DOUBLE', 'DOUBLE PRECISION', 'FLOAT8'},
            {'NUMERIC', 'DECIMAL'},
        ]

        for group in equivalents:
            if source_type.upper() in group and target_type.upper() in group:
                return True

        return False

    def _extract_table_name(self, table_id: str) -> str:
        """
        Extract table name from Debezium table ID.

        Format: "database"."schema"."table" or "database"."table"
        """
        parts = table_id.replace('"', '').replace('`', '').split('.')
        return parts[-1]

    def _get_target_table_name(self, source_table: str) -> str:
        """
        Get target table name from source table name.

        Uses the same naming convention as sink connector transforms.
        Format: {database}_{table} or {schema}_{table}
        """
        db_config = self.replication_config.client_database

        if self.source_db_type == 'mysql':
            # MySQL: database_table
            return f"{db_config.database_name}_{source_table}"
        else:
            # MSSQL/PostgreSQL: schema_table (default schema is dbo/public)
            schema = getattr(db_config, 'schema', 'dbo') or 'dbo'
            return f"{schema}_{source_table}"

    def _extract_length(self, type_str: str) -> Optional[int]:
        """Extract length from type string like VARCHAR(255)."""
        match = re.search(r'\((\d+)', type_str)
        return int(match.group(1)) if match else None

    def _delete_schema_for_table(self, source_table: str) -> bool:
        """
        Delete Schema Registry subjects for a table.

        This is needed when a breaking schema change occurs (e.g., column rename)
        that would cause Avro schema compatibility issues.

        Args:
            source_table: Source table name (without database prefix)

        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            # Get topic prefix (used in schema subject naming)
            # Format: client_{client_id}_db_{db_id}_v_{version}
            db_config = self.replication_config.client_database
            client = db_config.client
            version = self.replication_config.connector_version or 0
            topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{version}"

            # Schema subject format: {topic_prefix}.{database}.{table}
            # For MySQL: topic_prefix.database_name.table_name
            full_table_name = f"{db_config.database_name}.{source_table}"

            logger.info(
                f"Deleting schema subjects for {full_table_name} due to breaking schema change"
            )

            result = delete_table_schemas(topic_prefix, full_table_name, permanent=True)

            if result.get('key') and result.get('value'):
                logger.info(f"Successfully deleted schema subjects for {full_table_name}")
                return True
            else:
                logger.warning(f"Schema deletion result for {full_table_name}: {result}")
                return False

        except Exception as e:
            logger.error(f"Failed to delete schema subjects: {e}")
            return False

    def close(self):
        """Close Kafka consumer."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("KafkaDDLProcessor closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")