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
import time
from typing import Dict, List, Optional, Tuple, Any

from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
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

        # Cache table name prefix (stable for the processor's lifetime, avoids repeated
        # attribute chain traversal on every _get_target_table_name() call)
        if self.source_db_type == 'mysql':
            self._table_prefix = f"{db_config.database_name}_"
        else:
            schema = getattr(db_config, 'schema', 'dbo') or 'dbo'
            self._table_prefix = f"{schema}_"

        # Build source_table → target_table lookup from TableMapping.
        # When a user assigns a custom target name, the sink connector routes that
        # topic directly to the custom name (not the default {prefix}{source} name).
        # We must use the same name here so table_exists() checks the right table.
        from client.models.replication import TableMapping
        self._table_name_map: Dict[str, str] = {
            m.source_table: m.target_table
            for m in replication_config.table_mappings.filter(is_enabled=True)
            if m.target_table and m.target_table != f"{self._table_prefix}{m.source_table}"
        }
        if self._table_name_map:
            logger.info(f"   Custom table name mappings: {self._table_name_map}")

        # Initialize Kafka consumer (schema history topic)
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

        # Truncate watcher: subscribes to change topics for MySQL/Oracle to detect
        # op="t" events emitted when TRUNCATE TABLE runs on the source.
        # Requires connector config: skipped.operations="" (not the default "t").
        self.truncate_consumer: Optional[Any] = None
        self._truncate_deserializer: Optional[Any] = None
        self._admin_client: Optional[AdminClient] = None
        self._sink_consumer_group: Optional[str] = None
        # op="t" events whose sink hasn't caught up yet — retried each poll, so a
        # truncate is applied only once the sink has consumed past it (ordered like DML).
        self._truncate_pending: List[Dict] = []
        # Change-topic subscription state, refreshed so newly-added tables are watched.
        self._truncate_topic_prefix: Optional[str] = None
        self._watched_topics: set = set()
        self._last_topic_refresh: float = 0.0
        if self.source_db_type in ('mysql', 'oracle'):
            self._init_truncate_watcher(replication_config, bootstrap_servers, group_id)

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

        Uses consume() to batch-fetch all available messages in one call, then commits
        a single offset at the end of the batch. This is far more efficient than the
        previous approach of poll()+commit() in a tight loop, which caused N round-trips
        to Kafka brokers for N messages.

        Args:
            timeout_sec: Total time budget (seconds) to wait for messages
            max_messages: Maximum messages to process per call

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        processed = 0
        errors = 0
        last_msg = None

        try:
            # Batch-fetch up to max_messages within timeout_sec in a single call.
            # Returns immediately when the partition is at EOF.
            messages = self.consumer.consume(num_messages=max_messages, timeout=timeout_sec)

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    value = msg.value()
                    if value is not None:
                        message = json.loads(value.decode('utf-8'))
                        success = self._process_message(message)
                        if success:
                            processed += 1
                        else:
                            errors += 1

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse DDL message: {e}")
                    errors += 1
                except Exception as e:
                    logger.error(f"Error processing DDL message: {e}", exc_info=True)
                    errors += 1

                last_msg = msg

            # Single commit for the entire batch (replaces N per-message commits)
            if last_msg is not None:
                self.consumer.commit(last_msg)

        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")

        if errors > 0:
            logger.warning(f"DDL batch complete: {processed} processed, {errors} errors")

        # Poll change topics for TRUNCATE events (MySQL/Oracle only)
        self.poll_truncate_events(timeout_sec=2.0)

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

        if ddl:
            logger.debug(f"Processing DDL message: {ddl[:80]}...")

        # Skip non-DDL messages
        if not ddl and not table_changes:
            return True

        # Skip system database changes
        if database_name.lower() in ('mysql', 'information_schema', 'performance_schema', 'sys'):
            return True

        if self.source_db_type == 'mysql':
            if ddl:
                return self._process_mysql_ddl(ddl, table_changes)
            # MySQL message without raw DDL (e.g. initial schema metadata) — skip gracefully
            return True
        elif self.source_db_type == 'oracle':
            if ddl:
                return self._process_oracle_ddl(ddl)
            return True
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
        ]

        for pattern in skip_patterns:
            if ddl_upper.startswith(pattern):
                logger.debug(f"Skipping non-table DDL: {ddl[:50]}...")
                return True

        # Parse DDL type — CREATE TABLE, DROP TABLE etc. are caught by skip_patterns above.
        if ddl_upper.startswith('ALTER TABLE'):
            return self._handle_alter_table(ddl, table_changes)
        elif ddl_upper.startswith('RENAME TABLE'):
            return self._handle_rename_table(ddl)
        elif ddl_upper.startswith('TRUNCATE'):
            # TRUNCATE is detected exclusively via the op="t" change-topic watcher
            # (poll_truncate_events). Handling it here too would double-fire the
            # resync, and the schema-history consumer reads from 'earliest' — so on
            # a cold start it would replay every historical TRUNCATE and wipe the
            # target spuriously. Ignore it on this path.
            logger.debug("Ignoring TRUNCATE in schema-history (handled by op=t watcher)")
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

        # Extract explicit renames from MySQL DDL (CHANGE COLUMN / RENAME COLUMN)
        # This is much more reliable than position-based heuristic detection
        known_renames = self._extract_renames_from_ddl(ddl) if ddl else {}
        if known_renames:
            logger.info(f"Explicit renames parsed from DDL: {known_renames}")

        # Detect schema changes, using known renames from DDL when available
        operations = self._detect_schema_changes_by_position(
            target_table_name, target_columns, source_columns,
            known_renames=known_renames
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
        """
        Parse ALTER TABLE DDL directly when tableChanges metadata is not available.

        Fallback path: only reached when Debezium does not include tableChanges
        metadata (rare with MySQL). Splits multi-operation ALTERs (e.g.
        ADD COLUMN x, DROP COLUMN y) into individual clauses and applies each one,
        so trailing operations are no longer silently ignored. The primary path
        (_handle_alter_table with tableChanges) is still preferred when available.
        """
        # Check if table exists in target database
        if not self.target_adapter.table_exists(target_table):
            logger.info(
                f"Skipping ALTER for {target_table} - table doesn't exist yet "
                "(sink connector will create it)"
            )
            return True

        clauses = self._split_alter_operations(ddl)

        all_success = True
        handled_any = False
        for clause in clauses:
            operation = self._parse_alter_clause(clause, target_table, source_table)
            if operation is None:
                logger.debug(f"Unhandled ALTER clause skipped: {clause[:80]}")
                continue
            handled_any = True
            success, error = self.execute_operation(operation)
            if not success:
                logger.error(
                    f"Failed ALTER clause on {target_table}: {clause[:80]} ({error})"
                )
                all_success = False

        if not handled_any:
            logger.debug(f"Unhandled ALTER TABLE DDL: {ddl[:100]}...")
        return all_success

    def _split_alter_operations(self, ddl: str) -> List[str]:
        """
        Split a (possibly multi-operation) ALTER TABLE statement into individual
        operation clauses.

        Splits on top-level commas only, tracking parenthesis depth so commas
        inside type definitions (e.g. ``DECIMAL(10,2)``) or index column lists
        (e.g. ``(a, b)``) do not split a clause. The leading
        ``ALTER TABLE <table>`` prefix is stripped.

        Example::

            ALTER TABLE `t` ADD COLUMN a INT, DROP COLUMN b, MODIFY c DECIMAL(10,2)
            -> ['ADD COLUMN a INT', 'DROP COLUMN b', 'MODIFY c DECIMAL(10,2)']
        """
        prefix = re.match(
            r'\s*ALTER\s+TABLE\s+(?:`?\w+`?\.)?`?\w+`?\s+', ddl, re.IGNORECASE
        )
        body = ddl[prefix.end():] if prefix else ddl

        clauses: List[str] = []
        depth = 0
        current: List[str] = []
        for ch in body:
            if ch == '(':
                depth += 1
                current.append(ch)
            elif ch == ')':
                depth = max(0, depth - 1)
                current.append(ch)
            elif ch == ',' and depth == 0:
                clause = ''.join(current).strip().rstrip(';').strip()
                if clause:
                    clauses.append(clause)
                current = []
            else:
                current.append(ch)

        clause = ''.join(current).strip().rstrip(';').strip()
        if clause:
            clauses.append(clause)
        return clauses

    def _parse_alter_clause(
        self, clause: str, target_table: str, source_table: str
    ) -> Optional[DDLOperation]:
        """
        Parse a single ALTER TABLE operation clause into a DDLOperation.

        Returns None if the clause is not a recognised operation. Index
        operations are matched before column operations so an ``ADD ... INDEX``
        clause is not misparsed as ``ADD COLUMN``.
        """
        clause_upper = clause.upper()

        # ADD INDEX / KEY (before ADD COLUMN to avoid misparsing the index name)
        index_match = re.search(
            r'ADD\s+(UNIQUE\s+)?(?:INDEX|KEY)\s+`?(\w+)`?\s*\(([^)]+)\)',
            clause, re.IGNORECASE
        )
        if index_match:
            unique = bool(index_match.group(1))
            index_name = index_match.group(2)
            columns = [c.strip().strip('`') for c in index_match.group(3).split(',')]
            return DDLOperation(
                operation_type=DDLOperationType.ADD_INDEX,
                table_name=target_table,
                details={
                    'index_name': index_name,
                    'columns': columns,
                    'unique': unique
                },
                is_destructive=False,
                source_ddl=clause
            )

        # DROP INDEX / KEY (before DROP COLUMN)
        drop_idx_match = re.search(r'DROP\s+(?:INDEX|KEY)\s+`?(\w+)`?', clause, re.IGNORECASE)
        if drop_idx_match:
            return DDLOperation(
                operation_type=DDLOperationType.DROP_INDEX,
                table_name=target_table,
                details={'index_name': drop_idx_match.group(1)},
                is_destructive=False,
                source_ddl=clause
            )

        # ADD COLUMN
        add_match = re.search(
            r'ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+(\w+(?:\([^)]+\))?)',
            clause, re.IGNORECASE
        )
        if add_match:
            col_name = add_match.group(1)
            col_type = add_match.group(2)
            return DDLOperation(
                operation_type=DDLOperationType.ADD_COLUMN,
                table_name=target_table,
                details={
                    'column': {
                        'name': col_name,
                        'typeName': col_type.split('(')[0].upper(),
                        'length': self._extract_length(col_type),
                        'optional': 'NOT NULL' not in clause_upper,
                        'sourceDbType': self.source_db_type
                    }
                },
                is_destructive=False,
                source_ddl=clause
            )

        # DROP COLUMN - handle both "DROP COLUMN col" and "DROP col"
        drop_match = re.search(r'DROP\s+(?:COLUMN\s+)?`?(\w+)`?', clause, re.IGNORECASE)
        if drop_match:
            col_name = drop_match.group(1)
            # Skip if it looks like DROP PRIMARY/FOREIGN/CONSTRAINT (index/key handled above)
            if col_name.upper() not in ('INDEX', 'KEY', 'PRIMARY', 'FOREIGN', 'CONSTRAINT'):
                return DDLOperation(
                    operation_type=DDLOperationType.DROP_COLUMN,
                    table_name=target_table,
                    details={'column_name': col_name},
                    is_destructive=True,
                    source_ddl=clause
                )

        # MODIFY/CHANGE COLUMN
        modify_match = re.search(
            r'(?:MODIFY|CHANGE)\s+(?:COLUMN\s+)?`?(\w+)`?\s+(?:`?(\w+)`?\s+)?(\w+(?:\([^)]+\))?)',
            clause, re.IGNORECASE
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

                return DDLOperation(
                    operation_type=DDLOperationType.RENAME_COLUMN,
                    table_name=target_table,
                    details={
                        'old_name': old_name,
                        'new_name': new_name,
                        'column_type': col_type
                    },
                    is_destructive=False,
                    source_ddl=clause
                )

            # Modify column type
            return DDLOperation(
                operation_type=DDLOperationType.MODIFY_COLUMN,
                table_name=target_table,
                details={
                    'column': {
                        'name': old_name,
                        'typeName': col_type.split('(')[0].upper(),
                        'length': self._extract_length(col_type),
                        'optional': 'NOT NULL' not in clause_upper,
                        'sourceDbType': self.source_db_type
                    },
                    'old_type': ''
                },
                is_destructive=False,
                source_ddl=clause
            )

        return None

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

    def _process_oracle_ddl(self, ddl: str) -> bool:
        """
        Process Oracle DDL captured by LogMiner from the schema history topic.

        Oracle TRUNCATE does not appear in the schema history topic (it doesn't
        change the schema structure), so it is detected via op="t" on the change
        topic instead (requires skipped.operations="none" on the connector).
        All other schema changes (ALTER, CREATE, DROP) are handled by the sink
        connector's auto.create / auto.evolve settings.
        """
        logger.debug(f"Ignoring Oracle DDL: {ddl[:60]}...")
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
        """
        Handle RENAME TABLE on the target so it tracks the source rename instead of
        leaving a stale table behind under the old name.

        MySQL allows multiple renames in one statement:
            RENAME TABLE a TO b, c TO d
        Each (old -> new) pair is applied to the corresponding target table.

        Guards:
          - If the old target table doesn't exist, the rename is skipped (the sink
            will create the new table from its topic via auto.create).
          - If the new target table already exists, the rename is skipped to avoid
            clobbering existing data.

        Note: Debezium only keeps capturing the renamed table if the *new* name is in
        table.include.list. A source rename to a name outside the selected set stops
        replication for that table until it is re-selected — that reconfiguration is
        handled by the add/remove-table flow, not here.
        """
        # Each pair: [schema.]old TO [schema.]new  (back-ticked or bare)
        pairs = re.findall(
            r'(?:[`"]?\w+[`"]?\.)?[`"]?(\w+)[`"]?\s+TO\s+(?:[`"]?\w+[`"]?\.)?[`"]?(\w+)[`"]?',
            ddl, re.IGNORECASE
        )
        if not pairs:
            logger.warning(f"Could not parse RENAME TABLE: {ddl[:80]}")
            return True

        all_ok = True
        for old_src, new_src in pairs:
            old_target = self._get_target_table_name(old_src)
            new_target = self._get_target_table_name(new_src)

            if not self.target_adapter.table_exists(old_target):
                logger.info(
                    f"RENAME: target '{old_target}' missing — sink will create "
                    f"'{new_target}' from its topic, skipping rename"
                )
                continue
            if self.target_adapter.table_exists(new_target):
                logger.warning(
                    f"RENAME: target '{new_target}' already exists — skipping rename "
                    f"of '{old_target}' to avoid clobbering data"
                )
                continue

            operation = DDLOperation(
                operation_type=DDLOperationType.RENAME_TABLE,
                table_name=old_target,
                details={'new_name': new_target},
                is_destructive=False,
                source_ddl=ddl
            )
            success, error = self.execute_operation(operation)
            if success:
                logger.info(f"RENAME: target '{old_target}' -> '{new_target}'")
                # Keep the schema cache coherent under the new source name.
                if old_src in self.schema_cache:
                    self.schema_cache[new_src] = self.schema_cache.pop(old_src)
            else:
                logger.error(f"RENAME failed '{old_target}' -> '{new_target}': {error}")
                all_ok = False

        return all_ok

    def _detect_schema_changes_by_position(
        self,
        table_name: str,
        target_columns: List[Dict],
        source_columns: List[Dict],
        known_renames: Optional[Dict[str, str]] = None
    ) -> List[DDLOperation]:
        """
        Detect schema changes by comparing actual target and source columns by position.

        Uses known_renames (parsed from DDL) first for reliable rename detection,
        then falls back to position-based heuristic for remaining columns.
        """
        operations = []
        known_renames = known_renames or {}

        target_by_name = {c['name']: c for c in target_columns}
        source_by_name = {c['name']: c for c in source_columns}

        for col in source_columns:
            col['sourceDbType'] = self.source_db_type

        matched_target = set()
        matched_source = set()

        # First: handle known renames from DDL parsing (most reliable)
        # This prevents the position-based heuristic from misdetecting renames
        # as DROP + ADD which causes data loss
        for old_name, new_name in known_renames.items():
            if old_name in target_by_name and new_name in source_by_name:
                source_col = source_by_name[new_name]
                logger.info(f"Column rename from DDL: {old_name} -> {new_name}")
                operations.append(DDLOperation(
                    operation_type=DDLOperationType.RENAME_COLUMN,
                    table_name=table_name,
                    details={
                        'old_name': old_name,
                        'new_name': new_name,
                        'column_type': source_col.get('typeName', ''),
                        'column': source_col
                    },
                    is_destructive=False
                ))
                matched_target.add(old_name)
                matched_source.add(new_name)

        # Second: detect renames by position (fallback for MSSQL or missing DDL info)
        for pos, source_col in enumerate(source_columns):
            source_name = source_col['name']

            if source_name in matched_source:
                continue

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
                        logger.info(f"Column rename detected by position: {target_name} -> {source_name}")
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

    def _extract_renames_from_ddl(self, ddl: str) -> Dict[str, str]:
        """
        Extract column renames from MySQL DDL.

        MySQL CHANGE COLUMN syntax: CHANGE [COLUMN] `old_name` `new_name` column_definition
        MySQL 8.0+ RENAME COLUMN syntax: RENAME COLUMN `old_name` TO `new_name`

        Returns:
            Dict mapping old_name -> new_name for each rename detected
        """
        renames = {}

        # MySQL CHANGE COLUMN: CHANGE [COLUMN] `old` `new` type
        for match in re.finditer(
            r'CHANGE\s+(?:COLUMN\s+)?`?(\w+)`?\s+`?(\w+)`?\s+\w+',
            ddl, re.IGNORECASE
        ):
            old_name = match.group(1)
            new_name = match.group(2)
            if old_name != new_name:
                renames[old_name] = new_name

        # MySQL 8.0+ RENAME COLUMN: RENAME COLUMN `old` TO `new`
        for match in re.finditer(
            r'RENAME\s+COLUMN\s+`?(\w+)`?\s+TO\s+`?(\w+)`?',
            ddl, re.IGNORECASE
        ):
            old_name = match.group(1)
            new_name = match.group(2)
            if old_name != new_name:
                renames[old_name] = new_name

        return renames

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

        Uses custom TableMapping.target_table when set (matches sink connector routing).
        Falls back to the default {database}_{table} / {schema}_{table} convention.
        """
        if source_table in self._table_name_map:
            return self._table_name_map[source_table]
        return f"{self._table_prefix}{source_table}"

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

    def _init_truncate_watcher(self, replication_config, bootstrap_servers: str, schema_group_id: str) -> None:
        """
        Set up a second Kafka consumer that watches change topics for op="t" events.

        MySQL (and Oracle) emit TRUNCATE as op="t" to the change topic when
        skipped.operations="none" is configured on the source connector. This is the
        single source of truth for truncate detection — the schema-history path
        deliberately ignores TRUNCATE to avoid double-firing the resync.

        A detected truncate is only applied once the sink connector has consumed
        past it (see poll_truncate_events / _sink_caught_up), so the target is never
        truncated ahead of in-flight DML and a paused sink defers it like any other
        message.

        Uses a separate consumer group so it doesn't interfere with the sink.
        """
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from django.conf import settings as django_settings

            schema_registry_url = django_settings.DEBEZIUM_CONFIG.get(
                'SCHEMA_REGISTRY_URL', 'http://schema-registry:8081'
            )
            sr_client = SchemaRegistryClient({'url': schema_registry_url})
            # Use generic Avro deserializer — returns a dict
            self._truncate_deserializer = AvroDeserializer(sr_client)

            # AdminClient + sink consumer group are used to check whether the sink
            # has consumed past a truncate before we apply it to the target.
            # Kafka Connect names a sink's consumer group "connect-<connector_name>".
            self._admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
            sink_name = getattr(replication_config, 'sink_connector_name', None)
            self._sink_consumer_group = f"connect-{sink_name}" if sink_name else None

            # Topic prefix is stable for the processor's lifetime; cache it so the
            # change-topic list can be rebuilt cheaply when tables are added/removed.
            db_config = replication_config.client_database
            self._truncate_topic_prefix = replication_config.kafka_topic_prefix or (
                f"client_{db_config.client_id}_db_{db_config.id}_v_{replication_config.connector_version or 0}"
            )
            change_topics = self._build_change_topics()
            if not change_topics:
                return

            self.truncate_consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': f'{schema_group_id}-truncate-watcher',
                'auto.offset.reset': 'latest',  # only care about new events
                'enable.auto.commit': False,
                'session.timeout.ms': 30000,
                'max.poll.interval.ms': 300000,
            })
            self._watched_topics = set(change_topics)
            self._last_topic_refresh = time.monotonic()
            self.truncate_consumer.subscribe(sorted(self._watched_topics))
            logger.info(f"Truncate watcher initialized — watching {len(change_topics)} change topic(s)")

        except Exception as e:
            logger.warning(f"Could not init truncate watcher (non-fatal): {e}")
            self.truncate_consumer = None
            self._truncate_deserializer = None

    # How often the watcher re-queries the enabled table set to pick up newly
    # added tables without a processor restart (a re-subscribe triggers a rebalance,
    # so it is throttled rather than run every poll).
    _TOPIC_REFRESH_INTERVAL_SEC = 30

    def _build_change_topics(self) -> List[str]:
        """Build the change-topic list for all currently-enabled tracked tables."""
        rc = self.replication_config
        db_config = rc.client_database
        prefix = self._truncate_topic_prefix
        mappings = rc.table_mappings.filter(is_enabled=True)
        if self.source_db_type == 'mysql':
            db_name = db_config.database_name
            return [f"{prefix}.{db_name}.{m.source_table}" for m in mappings]
        # oracle
        return [
            f"{prefix}.{m.source_schema or db_config.username.upper()}.{m.source_table}"
            for m in mappings
        ]

    def _refresh_truncate_subscription(self) -> None:
        """
        Re-subscribe the watcher when the enabled table set has changed, so a table
        added after the processor started gets its TRUNCATEs watched without a
        restart. Throttled to _TOPIC_REFRESH_INTERVAL_SEC; only re-subscribes when
        the desired topic set actually differs (a re-subscribe forces a rebalance).
        """
        if not self.truncate_consumer:
            return
        now = time.monotonic()
        if now - self._last_topic_refresh < self._TOPIC_REFRESH_INTERVAL_SEC:
            return
        self._last_topic_refresh = now
        try:
            desired = set(self._build_change_topics())
            if desired and desired != self._watched_topics:
                added = sorted(desired - self._watched_topics)
                removed = sorted(self._watched_topics - desired)
                self.truncate_consumer.subscribe(sorted(desired))
                self._watched_topics = desired
                logger.info(
                    f"Truncate watcher subscription updated "
                    f"(added={added}, removed={removed})"
                )
        except Exception as e:
            logger.debug(f"Truncate subscription refresh failed: {e}")

    def poll_truncate_events(self, timeout_sec: float = 2.0) -> int:
        """
        Poll change topics for op="t" (TRUNCATE) events and trigger a resync, but
        only once the sink connector has consumed past the truncate event.

        Ordering guarantee: a TRUNCATE re-snapshots the target out-of-band, so it
        must not run ahead of DML the sink hasn't applied yet. We therefore defer any
        op="t" whose offset the sink hasn't committed past, and retry it on each poll.
        A paused sink never advances its committed offset, so its truncates stay
        deferred until it resumes — exactly how buffered DML behaves.

        Un-applied truncates are kept in memory and the consumer's committed offset
        is held at the earliest un-applied truncate, so a restart re-reads them.

        Returns number of truncate events applied this call.
        """
        if not self.truncate_consumer or not self._truncate_deserializer:
            return 0

        # Pick up tables added since the watcher started (throttled internally).
        self._refresh_truncate_subscription()

        handled = 0
        try:
            # 1) Retry truncates deferred on a previous poll (sink may have caught up).
            handled += self._retry_pending_truncates()

            # 2) Consume new change-topic messages and dispatch op="t" events.
            msgs = self.truncate_consumer.consume(num_messages=50, timeout=timeout_sec)
            for msg in msgs:
                if msg.error() or msg.value() is None:
                    continue  # error / tombstone

                try:
                    record = self._truncate_deserializer(msg.value(), None)
                    if not record:
                        continue
                    op = record.get('op') or record.get('payload', {}).get('op')
                    if op != 't':
                        continue
                except Exception as e:
                    logger.debug(f"Could not decode change topic message: {e}")
                    continue

                topic = msg.topic()
                table_name = topic.rsplit('.', 1)[-1]  # prefix.db.table -> table
                entry = {
                    'table': table_name,
                    'topic': topic,
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                }

                if self._sink_caught_up(topic, msg.partition(), msg.offset()):
                    if self._handle_truncate_for_table(table_name):
                        logger.info(
                            f"op=t on '{topic}' (offset {msg.offset()}) — sink caught up, "
                            f"resynced '{table_name}'"
                        )
                        handled += 1
                    else:
                        logger.warning(
                            f"op=t on '{topic}' (offset {msg.offset()}) — resync of "
                            f"'{table_name}' failed, will retry"
                        )
                        self._truncate_pending.append(entry)
                else:
                    logger.info(
                        f"op=t on '{topic}' (offset {msg.offset()}) — sink not caught up, "
                        f"deferring resync of '{table_name}'"
                    )
                    self._truncate_pending.append(entry)

            # 3) Commit a watermark that never advances past an un-applied truncate.
            self._commit_truncate_watermark()

        except Exception as e:
            logger.error(f"Truncate watcher poll error: {e}")

        return handled

    def _retry_pending_truncates(self) -> int:
        """
        Re-evaluate deferred op="t" events. Apply those the sink has now passed; keep
        any still behind the sink, or whose resync failed (e.g. the snapshot signal
        didn't go through), so they are retried on the next poll.
        """
        if not self._truncate_pending:
            return 0

        still_pending: List[Dict] = []
        handled = 0
        for entry in self._truncate_pending:
            if not self._sink_caught_up(entry['topic'], entry['partition'], entry['offset']):
                still_pending.append(entry)
                continue
            if self._handle_truncate_for_table(entry['table']):
                logger.info(
                    f"Deferred op=t for '{entry['table']}' (offset {entry['offset']}) — "
                    f"sink caught up, resynced"
                )
                handled += 1
            else:
                logger.warning(
                    f"Deferred op=t for '{entry['table']}' — resync failed, will retry"
                )
                still_pending.append(entry)
        self._truncate_pending = still_pending
        return handled

    def _sink_caught_up(self, topic: str, partition: int, offset: int) -> bool:
        """
        Return True if the sink connector has consumed past *offset* on (topic,
        partition) — i.e. it has applied every DML record up to and including the
        truncate event, so truncating the target now won't drop in-flight rows.

        The sink's committed offset is the NEXT offset it will read, so
        committed > offset means the truncate event itself has been consumed.

        Degrades safely: if the sink group/offset can't be read, returns True so a
        truncate is never stuck forever — the pause check in
        _handle_truncate_for_table still guards the explicit paused case.
        """
        if not self._admin_client or not self._sink_consumer_group:
            return True
        try:
            from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

            req = ConsumerGroupTopicPartitions(
                self._sink_consumer_group, [TopicPartition(topic, partition)]
            )
            futures = self._admin_client.list_consumer_group_offsets([req])
            result = futures[self._sink_consumer_group].result(timeout=10)
            for tp in result.topic_partitions:
                if tp.topic == topic and tp.partition == partition:
                    committed = tp.offset
                    if committed is None or committed < 0:
                        return False  # sink hasn't committed here yet
                    return committed > offset
            return False  # no committed offset for this partition yet
        except Exception as e:
            logger.warning(
                f"Could not read sink offset for {topic}[{partition}] "
                f"(group={self._sink_consumer_group}): {e} — proceeding"
            )
            return True

    def _commit_truncate_watermark(self) -> None:
        """
        Commit the truncate watcher's offsets, holding each partition at the earliest
        un-applied truncate so a restart re-reads deferred truncates rather than
        skipping them (the consumer starts from 'latest').
        """
        try:
            assignment = self.truncate_consumer.assignment()
            if not assignment:
                return
            positions = self.truncate_consumer.position(assignment)

            # Earliest still-deferred truncate offset per (topic, partition).
            pending_min: Dict[Tuple[str, int], int] = {}
            for p in self._truncate_pending:
                key = (p['topic'], p['partition'])
                pending_min[key] = min(pending_min.get(key, p['offset']), p['offset'])

            commit_list = []
            for tp in positions:
                key = (tp.topic, tp.partition)
                if key in pending_min:
                    tp.offset = pending_min[key]  # resume here on restart
                if tp.offset is not None and tp.offset >= 0:
                    commit_list.append(tp)
            if commit_list:
                self.truncate_consumer.commit(offsets=commit_list, asynchronous=False)
        except Exception as e:
            logger.debug(f"Truncate watermark commit failed: {e}")

    def close(self):
        """Close Kafka consumers."""
        if self.truncate_consumer:
            try:
                self.truncate_consumer.close()
            except Exception as e:
                logger.error(f"Error closing truncate watcher: {e}")
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("KafkaDDLProcessor closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")