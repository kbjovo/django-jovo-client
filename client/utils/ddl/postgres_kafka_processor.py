"""
Kafka-based DDL Processor for PostgreSQL sources.

Consumes DDL events from the ddl_capture.ddl_events table replicated via Debezium.

Since PostgreSQL logical decoding doesn't emit DDL events natively, we use:
1. Event triggers that capture DDL to ddl_capture.ddl_events table
2. Debezium replicates this table to Kafka
3. This processor consumes those events and applies them to target
"""

import json
import logging
import re
from typing import Dict, List, Optional, Tuple, Any

from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from sqlalchemy.engine import Engine

from django.conf import settings

from .base_processor import BaseDDLProcessor, DDLOperation, DDLOperationType
from client.models.replication import ReplicationConfig

logger = logging.getLogger(__name__)


class PostgreSQLKafkaDDLProcessor(BaseDDLProcessor):
    """
    Process DDL changes from PostgreSQL source via Kafka.

    Consumes the ddl_capture.ddl_events table topic replicated by Debezium.
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
        Initialize PostgreSQL Kafka DDL Processor.

        Args:
            replication_config: ReplicationConfig instance
            target_engine: SQLAlchemy engine for target database
            bootstrap_servers: Kafka bootstrap servers
            auto_execute_destructive: Whether to auto-execute destructive operations
            consumer_group: Kafka consumer group ID (optional)
        """
        super().__init__(replication_config, target_engine, auto_execute_destructive)

        self.bootstrap_servers = bootstrap_servers
        self.source_db_type = 'postgresql'

        # Build DDL events topic name
        # Format: {topic_prefix}.{database}.ddl_events
        # NOTE: The PostgreSQL connector uses RegexRouter transform that replaces
        # schema name with database name in topics, so ddl_capture.ddl_events
        # becomes {database}.ddl_events in Kafka
        client = replication_config.client_database.client
        db_config = replication_config.client_database
        version = replication_config.connector_version or 0

        topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{version}"
        self.ddl_topic = f"{topic_prefix}.{db_config.database_name}.ddl_events"

        # Initialize Schema Registry client for Avro deserialization
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )
        self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        self.avro_deserializer = AvroDeserializer(self.schema_registry_client)

        # Initialize Kafka consumer
        group_id = consumer_group or f'pg-ddl-processor-config-{replication_config.id}'
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000,
        })
        self.consumer.subscribe([self.ddl_topic])

        # Track processed event IDs to avoid duplicates
        self._processed_events: set = set()

        # Source database info for table name mapping
        self.source_schema = getattr(db_config, 'schema', 'public') or 'public'
        self.source_database = db_config.database_name

        logger.info(f"PostgreSQLKafkaDDLProcessor initialized")
        logger.info(f"   DDL topic: {self.ddl_topic}")
        logger.info(f"   Consumer group: {group_id}")
        logger.info(f"   Source schema: {self.source_schema}")

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
                    break

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Topic doesn't exist yet - this is normal if no DDL events have been captured
                        # The topic will be created when the first DDL event is inserted
                        logger.debug(
                            f"DDL topic {self.ddl_topic} not available yet. "
                            "Waiting for first DDL event to be captured..."
                        )
                        break
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                messages_read += 1

                try:
                    value = msg.value()
                    if value is None:
                        # Tombstone message (delete) - skip
                        continue

                    # Deserialize Avro message using Schema Registry
                    message = self.avro_deserializer(value, None)
                    if message is None:
                        logger.debug("Received null message after Avro deserialization")
                        continue

                    success = self._process_message(message)

                    if success:
                        processed += 1
                    else:
                        errors += 1

                    # Commit after each message
                    self.consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Error processing DDL message: {e}", exc_info=True)
                    errors += 1

        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")

        if processed > 0 or errors > 0:
            logger.info(f"PostgreSQL DDL processing complete: {processed} processed, {errors} errors")

        return processed, errors

    def _process_message(self, message: Dict) -> bool:
        """
        Process a single DDL event message from ddl_capture.ddl_events.

        Debezium CDC message format (envelope):
        {
            "before": null,
            "after": {
                "id": 1,
                "event_id": "uuid",
                "event_time": "2024-01-01T00:00:00Z",
                "event_type": "ALTER TABLE",
                "object_type": "table",
                "schema_name": "public",
                "object_name": "users",
                "ddl_command": "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
                "object_identity": "public.users",
                "processed": false
            },
            "source": {...},
            "op": "c"  // c=create, u=update, d=delete
        }
        """
        # Handle Debezium envelope format
        after = message.get('after')
        if not after:
            # Delete or no data
            return True

        # Extract DDL event fields
        event_id = after.get('event_id')
        event_type = after.get('event_type', '')
        object_type = after.get('object_type', '')
        schema_name = after.get('schema_name', 'public')
        object_name = after.get('object_name', '')
        ddl_command = after.get('ddl_command', '')
        processed = after.get('processed', False)

        # Skip already processed events
        if processed:
            logger.debug(f"Skipping already processed DDL event: {event_id}")
            return True

        # Skip duplicate events (same event_id)
        if event_id in self._processed_events:
            logger.debug(f"Skipping duplicate DDL event: {event_id}")
            return True

        # Skip DDL on system schemas
        if schema_name in ('pg_catalog', 'information_schema', 'pg_toast', 'ddl_capture'):
            return True

        logger.info(f"Processing PostgreSQL DDL: {event_type} {object_type} {schema_name}.{object_name}")
        logger.debug(f"DDL command: {ddl_command}")

        # Process based on event type
        success = self._process_ddl_event(
            event_type=event_type,
            object_type=object_type,
            schema_name=schema_name,
            object_name=object_name,
            ddl_command=ddl_command
        )

        if success:
            self._processed_events.add(event_id)
            # Mark as processed in source (optional - via separate update)
            self._mark_event_processed(event_id)

        return success

    def _process_ddl_event(
        self,
        event_type: str,
        object_type: str,
        schema_name: str,
        object_name: str,
        ddl_command: str
    ) -> bool:
        """
        Process a DDL event and apply to target.

        Args:
            event_type: DDL event type (CREATE TABLE, ALTER TABLE, DROP TABLE)
            object_type: Object type (table, index, etc.)
            schema_name: Source schema name
            object_name: Source object name/identity
            ddl_command: Full DDL command text

        Returns:
            True if processed successfully
        """
        event_upper = event_type.upper()

        # Extract table name from object_name based on object_type
        # For "table column" events, object_name is schema.table.column
        # For "table" events, object_name is schema.table or just table
        table_name = self._extract_table_name(object_name, object_type)
        target_table = self._get_target_table_name(table_name, schema_name)

        # Skip CREATE TABLE - sink connector handles this with auto.create=true
        if event_upper == 'CREATE TABLE':
            logger.debug(f"Skipping CREATE TABLE for {table_name} (sink connector handles this)")
            return True

        # Skip DROP TABLE - dangerous, let user handle manually
        if event_upper == 'DROP TABLE':
            logger.warning(f"Skipping DROP TABLE for {table_name} (requires manual approval)")
            return True

        # Handle ALTER TABLE
        if event_upper == 'ALTER TABLE':
            return self._handle_alter_table(target_table, ddl_command, schema_name, table_name)

        # Handle CREATE INDEX
        if event_upper == 'CREATE INDEX':
            return self._handle_create_index(target_table, ddl_command)

        # Handle DROP INDEX (via sql_drop event trigger)
        if object_type == 'index' and 'DROP' in event_upper:
            return self._handle_drop_index(target_table, object_name, ddl_command)

        # Handle TABLE_REWRITE (type changes)
        if event_upper == 'TABLE_REWRITE':
            logger.info(f"Table rewrite detected for {table_name}, may need schema refresh")
            return True

        logger.debug(f"Ignoring DDL event: {event_type} {object_type}")
        return True

    def _handle_alter_table(
        self,
        target_table: str,
        ddl_command: str,
        schema_name: str,
        source_table: str
    ) -> bool:
        """
        Handle ALTER TABLE DDL.

        Parses PostgreSQL ALTER TABLE syntax and applies to target.
        """
        ddl_upper = ddl_command.upper()

        # Check if target table exists
        if not self.target_adapter.table_exists(target_table):
            logger.info(
                f"Skipping ALTER for {target_table} - table doesn't exist yet "
                "(sink connector will create it)"
            )
            return True

        # ADD COLUMN
        # PostgreSQL: ALTER TABLE table_name ADD COLUMN column_name data_type
        add_match = re.search(
            r'ADD\s+(?:COLUMN\s+)?(?:")?(\w+)(?:")?\s+(\w+(?:\([^)]+\))?)',
            ddl_command, re.IGNORECASE
        )
        if add_match and 'ADD' in ddl_upper and 'COLUMN' in ddl_upper:
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
                        'sourceDbType': 'postgresql'
                    }
                },
                is_destructive=False,
                source_ddl=ddl_command
            )
            success, error = self.execute_operation(operation)
            if error:
                logger.error(f"ADD COLUMN failed: {error}")
            return success

        # DROP COLUMN
        # PostgreSQL: ALTER TABLE table_name DROP COLUMN column_name
        drop_match = re.search(
            r'DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(?:")?(\w+)(?:")?',
            ddl_command, re.IGNORECASE
        )
        if drop_match:
            col_name = drop_match.group(1)

            # Delete schema from registry for breaking change
            self._delete_schema_for_table(source_table, schema_name)

            operation = DDLOperation(
                operation_type=DDLOperationType.DROP_COLUMN,
                table_name=target_table,
                details={'column_name': col_name},
                is_destructive=True,
                source_ddl=ddl_command
            )
            success, error = self.execute_operation(operation)
            if error:
                logger.error(f"DROP COLUMN failed: {error}")
            return success

        # RENAME COLUMN
        # PostgreSQL: ALTER TABLE table_name RENAME COLUMN old_name TO new_name
        rename_match = re.search(
            r'RENAME\s+COLUMN\s+(?:")?(\w+)(?:")?\s+TO\s+(?:")?(\w+)(?:")?',
            ddl_command, re.IGNORECASE
        )
        if rename_match:
            old_name = rename_match.group(1)
            new_name = rename_match.group(2)

            # Delete schema from registry for breaking change
            self._delete_schema_for_table(source_table, schema_name)

            operation = DDLOperation(
                operation_type=DDLOperationType.RENAME_COLUMN,
                table_name=target_table,
                details={
                    'old_name': old_name,
                    'new_name': new_name,
                    'column_type': ''  # PostgreSQL RENAME doesn't require type
                },
                is_destructive=False,
                source_ddl=ddl_command
            )
            success, error = self.execute_operation(operation)
            if error:
                logger.error(f"RENAME COLUMN failed: {error}")
            return success

        # ALTER COLUMN TYPE
        # PostgreSQL: ALTER TABLE table_name ALTER COLUMN column_name TYPE new_type
        type_match = re.search(
            r'ALTER\s+COLUMN\s+(?:")?(\w+)(?:")?\s+(?:SET\s+DATA\s+)?TYPE\s+(\w+(?:\([^)]+\))?)',
            ddl_command, re.IGNORECASE
        )
        if type_match:
            col_name = type_match.group(1)
            new_type = type_match.group(2)

            operation = DDLOperation(
                operation_type=DDLOperationType.MODIFY_COLUMN,
                table_name=target_table,
                details={
                    'column': {
                        'name': col_name,
                        'typeName': new_type.split('(')[0].upper(),
                        'length': self._extract_length(new_type),
                        'optional': True,
                        'sourceDbType': 'postgresql'
                    },
                    'old_type': ''
                },
                is_destructive=False,
                source_ddl=ddl_command
            )
            success, error = self.execute_operation(operation)
            if error:
                logger.error(f"MODIFY COLUMN failed: {error}")
            return success

        # ALTER COLUMN SET/DROP NOT NULL
        null_match = re.search(
            r'ALTER\s+COLUMN\s+(?:")?(\w+)(?:")?\s+(SET|DROP)\s+NOT\s+NULL',
            ddl_command, re.IGNORECASE
        )
        if null_match:
            col_name = null_match.group(1)
            action = null_match.group(2).upper()
            logger.info(f"Nullability change for {col_name}: {action} NOT NULL")
            # Nullability changes typically don't need target-side DDL
            # as data will still flow correctly
            return True

        # ALTER COLUMN SET DEFAULT / DROP DEFAULT
        default_match = re.search(
            r'ALTER\s+COLUMN\s+(?:")?(\w+)(?:")?\s+(SET\s+DEFAULT|DROP\s+DEFAULT)',
            ddl_command, re.IGNORECASE
        )
        if default_match:
            col_name = default_match.group(1)
            logger.info(f"Default change for {col_name}")
            # Default changes don't affect replication
            return True

        logger.debug(f"Unhandled ALTER TABLE DDL: {ddl_command[:100]}...")
        return True

    def _handle_create_index(self, target_table: str, ddl_command: str) -> bool:
        """Handle CREATE INDEX DDL."""
        # PostgreSQL: CREATE [UNIQUE] INDEX index_name ON table_name (columns)
        match = re.search(
            r'CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:")?(\w+)(?:")?\s+ON\s+(?:\w+\.)?(?:")?(\w+)(?:")?\s*\(([^)]+)\)',
            ddl_command, re.IGNORECASE
        )
        if not match:
            logger.debug(f"Could not parse CREATE INDEX: {ddl_command[:100]}")
            return True

        unique = bool(match.group(1))
        index_name = match.group(2)
        columns = [c.strip().strip('"') for c in match.group(4).split(',')]

        operation = DDLOperation(
            operation_type=DDLOperationType.ADD_INDEX,
            table_name=target_table,
            details={
                'index_name': index_name,
                'columns': columns,
                'unique': unique
            },
            is_destructive=False,
            source_ddl=ddl_command
        )
        success, error = self.execute_operation(operation)
        if error:
            logger.error(f"CREATE INDEX failed: {error}")
        return success

    def _handle_drop_index(self, target_table: str, object_name: str, ddl_command: str) -> bool:
        """Handle DROP INDEX DDL."""
        # Extract index name from object_name or DDL
        index_name = object_name.split('.')[-1] if '.' in object_name else object_name

        operation = DDLOperation(
            operation_type=DDLOperationType.DROP_INDEX,
            table_name=target_table,
            details={'index_name': index_name},
            is_destructive=False,
            source_ddl=ddl_command
        )
        success, error = self.execute_operation(operation)
        if error:
            logger.error(f"DROP INDEX failed: {error}")
        return success

    def _extract_table_name(self, object_name: str, object_type: str = 'table') -> str:
        """
        Extract table name from object identity.

        object_name format depends on object_type:
        - "table": schema.table or just table (e.g., "public.users", "users")
        - "table column": schema.table.column (e.g., "public.users.email")
        - "table constraint": schema.table.constraint_name

        Args:
            object_name: Object identity string
            object_type: Type of object (table, table column, index, etc.)

        Returns:
            Extracted table name
        """
        # Remove quotes
        cleaned = object_name.replace('"', '').replace("'", '')
        parts = cleaned.split('.')

        # For column/constraint events, object_name is schema.table.column/constraint
        # So table name is second-to-last part
        if object_type in ('table column', 'table constraint') and len(parts) >= 2:
            return parts[-2]

        # For table/index events, table name is the last part
        return parts[-1] if parts else object_name

    def _get_target_table_name(self, source_table: str, schema_name: str = 'public') -> str:
        """
        Get target table name from source table name.

        First looks up the target table from TableMapping (most accurate).
        Falls back to computed name if not found.
        """
        # Look up from TableMapping first (this is the actual target table name)
        table_mapping = self.replication_config.table_mappings.filter(
            source_table=source_table,
            is_enabled=True
        ).first()

        if table_mapping and table_mapping.target_table:
            return table_mapping.target_table

        # Fallback: compute using schema_table convention
        return f"{schema_name}_{source_table}"

    def _extract_length(self, type_str: str) -> Optional[int]:
        """Extract length from type string like VARCHAR(255)."""
        match = re.search(r'\((\d+)', type_str)
        return int(match.group(1)) if match else None

    def _mark_event_processed(self, event_id: str):
        """
        Mark DDL event as processed in source database.

        This is done via direct connection to source, not via Kafka.
        Optional: Can be implemented to update ddl_capture.ddl_events.processed = true
        """
        # Optional: Update source database to mark event as processed
        # This prevents reprocessing on consumer restart
        # For now, we rely on Kafka consumer offsets
        pass

    def _delete_schema_for_table(self, source_table: str, schema_name: str) -> bool:
        """
        Delete Schema Registry subjects for a table.

        This is needed when a breaking schema change occurs (e.g., column rename/drop)
        that would cause Avro schema compatibility issues.
        """
        try:
            from jovoclient.utils.debezium.schema_registry_utils import delete_table_schemas

            db_config = self.replication_config.client_database
            client = db_config.client
            version = self.replication_config.connector_version or 0
            topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{version}"

            # Schema subject format: {topic_prefix}.{schema}.{table}
            full_table_name = f"{schema_name}.{source_table}"

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
                logger.info("PostgreSQLKafkaDDLProcessor closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")