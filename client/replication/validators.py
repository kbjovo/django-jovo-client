"""
Validation logic for replication operations.

Provides pre-flight checks to ensure replication can start successfully.
"""

import logging
from typing import Dict, List, Tuple

from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

logger = logging.getLogger(__name__)


class ReplicationValidator:
    """
    Validates prerequisites for replication operations.
    All validation methods return (bool, str) - (is_valid, error_message)
    """

    def __init__(self, replication_config):
        self.config = replication_config
        self.errors = []

    def validate_all(self, skip_topic_check: bool = True) -> Tuple[bool, List[str]]:
        """
        Run all validations and return consolidated result.

        Args:
            skip_topic_check: If True, skip Kafka topic validation (default: True for Option A)
                             Topics will be created automatically by Debezium connector

        Returns:
            (is_valid, [error_messages])
        """
        logger.info(f"[{self.config.connector_name}] Running pre-flight validation...")

        validations = [
            self._validate_database_config(),
            self._validate_table_mappings(),
            self._validate_primary_keys(),  # Ensures all tables have PKs to avoid record-key errors
            self._validate_target_database(),
            self._validate_no_topic_conflicts(),
        ]

        # Skip topic validation for Option A (Debezium creates topics automatically)
        if not skip_topic_check:
            logger.debug(f"[{self.config.connector_name}] Checking Kafka topics...")
            validations.append(self._validate_kafka_topics())
        else:
            logger.debug(f"[{self.config.connector_name}] Skipping Kafka topic validation (topics will be auto-created)")

        # Collect all errors
        errors = []
        for is_valid, error_msg in validations:
            if not is_valid:
                errors.append(error_msg)

        if errors:
            logger.error(f"[{self.config.connector_name}] Validation failed: {errors}")
            return False, errors

        logger.info(f"[{self.config.connector_name}] ✓ All validations passed")
        return True, []

    def _validate_database_config(self) -> Tuple[bool, str]:
        """Validate source database configuration."""
        try:
            db_config = self.config.client_database

            if not db_config:
                return False, "Source database not configured"

            if not db_config.host or not db_config.port:
                return False, f"Invalid database connection: host={db_config.host}, port={db_config.port}"

            if db_config.connection_status != 'success':
                return False, f"Source database connection failed: {db_config.connection_status}"

            logger.debug(f"[{self.config.connector_name}] ✓ Database config valid")
            return True, ""

        except Exception as e:
            return False, f"Database config validation error: {str(e)}"

    def _validate_table_mappings(self) -> Tuple[bool, str]:
        """Validate at least one table is configured and enabled."""
        try:
            enabled_tables = self.config.table_mappings.filter(is_enabled=True)

            if not enabled_tables.exists():
                return False, "No tables enabled for replication"

            table_count = enabled_tables.count()
            logger.debug(f"[{self.config.connector_name}] ✓ {table_count} tables enabled")
            return True, ""

        except Exception as e:
            return False, f"Table mapping validation error: {str(e)}"

    def _validate_primary_keys(self) -> Tuple[bool, str]:
        """
        Validate all enabled tables have primary keys defined.

        Tables without primary keys cause 'record-key' errors because:
        - Debezium cannot generate proper message keys
        - JDBC sink connector cannot perform upserts
        - Schema registry may have conflicts

        Returns:
            (is_valid, error_message)
        """
        try:
            enabled_tables = self.config.table_mappings.filter(is_enabled=True)
            tables_without_pk = []

            for table_mapping in enabled_tables:
                # Check if any column is marked as primary key
                pk_columns = table_mapping.column_mappings.filter(
                    is_primary_key=True,
                    is_enabled=True
                )

                if not pk_columns.exists():
                    tables_without_pk.append(table_mapping.source_table)

            if tables_without_pk:
                error_msg = (
                    f"The following {len(tables_without_pk)} table(s) have no primary key defined:\n"
                    + "\n".join([f"  • {t}" for t in tables_without_pk[:10]])  # Limit to first 10
                )
                if len(tables_without_pk) > 10:
                    error_msg += f"\n  ... and {len(tables_without_pk) - 10} more"

                error_msg += (
                    "\n\nTables without primary keys can cause 'record-key' errors. "
                    "Please ensure all tables have at least one primary key column marked."
                )

                logger.warning(f"[{self.config.connector_name}] Tables without PKs: {tables_without_pk}")
                return False, error_msg

            logger.debug(f"[{self.config.connector_name}] ✓ All {enabled_tables.count()} tables have primary keys")
            return True, ""

        except Exception as e:
            logger.warning(f"[{self.config.connector_name}] Could not validate PKs: {e}")
            # Don't fail validation if check fails
            return True, ""


    def _validate_no_topic_conflicts(self) -> Tuple[bool, str]:
        """
        Validate that no other active replication is consuming from the same topics.
        
        This prevents the scenario where multiple ReplicationConfigs try to consume
        from the same Kafka topics, which causes:
        - Duplicate message processing
        - Consumer group conflicts
        - Unpredictable behavior
        
        Returns:
            (is_valid, error_message)
        """
        try:
            from client.models import ReplicationConfig
            
            # Build topic prefix for this config
            # Format: client_{client_id}_db_{database_id}
            client_id = self.config.client_database.client.id
            database_id = self.config.client_database.id
            this_topic_prefix = f"client_{client_id}_db_{database_id}"
            
            # Also check the saved kafka_topic_prefix (may be different if manually set)
            saved_topic_prefix = self.config.kafka_topic_prefix
            
            logger.debug(
                f"[{self.config.connector_name}] Checking for conflicts with "
                f"topic prefix: {this_topic_prefix} or {saved_topic_prefix}"
            )
            
            # Find other active replications using the same topic prefix
            conflicting_configs = ReplicationConfig.objects.filter(
                is_active=True,
                status='active'
            ).exclude(id=self.config.id)
            
            # Check each potential conflict
            conflicts = []
            for other_config in conflicting_configs:
                # Check if they share the same database (which means same topics)
                if other_config.client_database.id == self.config.client_database.id:
                    conflicts.append({
                        'id': other_config.id,
                        'connector_name': other_config.connector_name,
                        'topic_prefix': other_config.kafka_topic_prefix,
                        'reason': 'Same source database'
                    })
                # Also check by topic prefix
                elif other_config.kafka_topic_prefix == this_topic_prefix:
                    conflicts.append({
                        'id': other_config.id,
                        'connector_name': other_config.connector_name,
                        'topic_prefix': other_config.kafka_topic_prefix,
                        'reason': 'Same topic prefix'
                    })
                elif other_config.kafka_topic_prefix == saved_topic_prefix:
                    conflicts.append({
                        'id': other_config.id,
                        'connector_name': other_config.connector_name,
                        'topic_prefix': other_config.kafka_topic_prefix,
                        'reason': 'Same saved topic prefix'
                    })
            
            if conflicts:
                # Build detailed error message
                conflict_details = []
                for conflict in conflicts:
                    conflict_details.append(
                        f"  • ReplicationConfig ID {conflict['id']} "
                        f"(Connector: {conflict['connector_name']}, "
                        f"Reason: {conflict['reason']})"
                    )
                
                error_msg = (
                    f"Cannot start replication: {len(conflicts)} active replication(s) "
                    f"already consuming from these topics:\n"
                    + "\n".join(conflict_details) +
                    f"\n\nTo fix this:\n"
                    f"  1. Stop or delete the conflicting replication(s), OR\n"
                    f"  2. Use a different source database for this replication\n\n"
                    f"Note: Each topic can only be consumed by one replication at a time."
                )
                
                logger.error(
                    f"[{self.config.connector_name}] Topic conflict detected: "
                    f"{len(conflicts)} conflicting replication(s)"
                )
                
                return False, error_msg
            
            logger.debug(f"[{self.config.connector_name}] ✓ No topic conflicts detected")
            return True, ""

        except Exception as e:
            logger.error(f"[{self.config.connector_name}] Error checking for topic conflicts: {e}")
            # Don't fail validation if check fails - log warning and continue
            logger.warning(
                f"[{self.config.connector_name}] Could not verify topic conflicts, "
                f"proceeding anyway. Error: {str(e)}"
            )
            return True, ""


    def _validate_kafka_topics(self) -> Tuple[bool, str]:
        """Validate all required Kafka topics exist."""
        try:
            topic_manager = KafkaTopicManager()
            enabled_tables = self.config.table_mappings.filter(is_enabled=True)

            missing_topics = []
            for table_mapping in enabled_tables:
                # Topic format: {topic_prefix}.{database}.{table}
                topic_name = f"{self.config.kafka_topic_prefix}.{self.config.client_database.database_name}.{table_mapping.source_table}"

                if not topic_manager.topic_exists(topic_name):
                    missing_topics.append(topic_name)

            if missing_topics:
                return False, f"Missing Kafka topics: {', '.join(missing_topics)}"

            logger.debug(f"[{self.config.connector_name}] ✓ All Kafka topics exist")
            return True, ""

        except Exception as e:
            logger.warning(f"[{self.config.connector_name}] Could not validate Kafka topics: {e}")
            # Don't fail validation if Kafka is temporarily unavailable
            return True, ""

    def _validate_target_database(self) -> Tuple[bool, str]:
        """Validate target database is accessible."""
        try:
            from client.models import ClientDatabase

            target_db = ClientDatabase.objects.filter(is_target=True).first()

            if not target_db:
                return False, "No target database configured"

            if target_db.connection_status != 'success':
                return False, f"Target database connection failed: {target_db.connection_status}"

            logger.debug(f"[{self.config.connector_name}] ✓ Target database accessible")
            return True, ""

        except Exception as e:
            return False, f"Target database validation error: {str(e)}"

    def validate_connector_exists(self) -> Tuple[bool, str]:
        """Check if Debezium connector exists in Kafka Connect."""
        try:
            manager = DebeziumConnectorManager()
            status = manager.get_connector_status(self.config.connector_name)

            if status:
                logger.debug(f"[{self.config.connector_name}] ✓ Connector exists")
                return True, ""
            else:
                return False, f"Connector '{self.config.connector_name}' does not exist"

        except Exception as e:
            return False, f"Connector existence check failed: {str(e)}"

    def validate_connector_running(self) -> Tuple[bool, str]:
        """Check if Debezium connector is in RUNNING state."""
        try:
            manager = DebeziumConnectorManager()
            status_data = manager.get_connector_status(self.config.connector_name)

            if not status_data:
                return False, f"Connector '{self.config.connector_name}' does not exist"

            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')

            if connector_state == 'RUNNING':
                logger.debug(f"[{self.config.connector_name}] ✓ Connector RUNNING")
                return True, ""
            else:
                return False, f"Connector state is '{connector_state}', expected 'RUNNING'"

        except Exception as e:
            return False, f"Connector status check failed: {str(e)}"
        
        