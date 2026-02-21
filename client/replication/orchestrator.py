import logging
from typing import Dict, Any, Tuple, Optional, Set
from django.utils import timezone
from django.conf import settings

from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.jolokia_client import JolokiaClient
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager, format_topic_name
from jovoclient.utils.debezium.connector_templates import get_connector_config_for_database
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from client.utils.table_creator import drop_tables_for_mappings, truncate_tables_for_mappings
from .validators import ReplicationValidator
from sqlalchemy import text

logger = logging.getLogger(__name__)


class ReplicationOrchestrator:
    """
    Orchestrates all replication operations.

    This is the single source of truth for managing CDC replication.
    It ensures connector and consumer are always in sync.
    """

    def __init__(self, replication_config):
        """
        Initialize orchestrator for a specific replication config.

        Args:
            replication_config: ReplicationConfig model instance
        """
        self.config = replication_config
        self.validator = ReplicationValidator(replication_config)
        self.connector_manager = DebeziumConnectorManager()
        self.topic_manager = KafkaTopicManager()
        self.jolokia = JolokiaClient()

    # ==========================================
    # Utility Methods
    # ==========================================

    def _update_status(self, status: str, message: Optional[str] = None):
        """Update ReplicationConfig status."""
        self.config.status = status
        if message:
            self.config.last_error_message = message if status == 'error' else ''
        self.config.save()

    def _log_info(self, message: str):
        """Log info message."""
        logger.info(f"[{self.config.connector_name}] {message}")

    def _log_warning(self, message: str):
        """Log warning message."""
        logger.warning(f"[{self.config.connector_name}] {message}")

    def _log_error(self, message: str):
        """Log error message."""
        logger.error(f"[{self.config.connector_name}] {message}") 
    # ==========================================

    def start_replication(self, skip_topic_conflict_check: bool = False) -> Tuple[bool, str]:
        """
        Start complete replication (connector + consumer).

        FLOW:
        1. Validate prerequisites (database connectivity, binlog config, permissions)
        2. Determine connector version and update kafka_topic_prefix
           (CRITICAL: must happen BEFORE creating topics)
        3. Create Kafka topics (using correct topic prefix with version)
        4. Ensure sink connector is ready (create/update/reuse with topics.regex)
        5. Create fresh source connector (uses pre-set version)

        Args:
            skip_topic_conflict_check: If True, skip topic conflict validation (used for batch mode)

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STARTING CDC REPLICATION")
        self._log_info("=" * 60)

        try:
            # ========================================
            # STEP 1: Validate Prerequisites
            # ========================================
            self._log_info("STEP 1/5: Validating prerequisites...")
            self._log_info("  → Checking database connectivity")
            self._log_info("  → Verifying binlog configuration")
            self._log_info("  → Validating user permissions")

            is_valid, errors = self.validator.validate_all(skip_topic_conflict_check=skip_topic_conflict_check)
            if not is_valid:
                error_msg = f"Validation failed: {'; '.join(errors)}"
                self._log_error(error_msg)
                self._update_status('error', error_msg)
                return False, error_msg

            self._log_info("✓ All prerequisites validated successfully")
            self._log_info("")

            # ========================================
            # STEP 2: Determine Connector Version and Update Topic Prefix
            # ========================================
            # CRITICAL: Must happen BEFORE creating topics so all components use the same version
            self._log_info("STEP 2/5: Determining connector version...")

            from client.models import ConnectorHistory
            db_config = self.config.client_database
            client = db_config.client

            next_version = ConnectorHistory.get_next_version(
                client_id=client.id,
                database_id=db_config.id,
                connector_type='source'
            )

            # Update version and topic prefix BEFORE creating topics
            self.config.connector_version = next_version
            self.config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{next_version}"
            self.config.save()

            self._log_info(f"✓ Connector version: {next_version}")
            self._log_info(f"✓ Topic prefix: {self.config.kafka_topic_prefix}")
            self._log_info("")

            # ========================================
            # STEP 3: Create Kafka Topics Explicitly
            # ========================================
            self._log_info("STEP 3/5: Creating Kafka topics explicitly...")

            success, message = self.create_topics()
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ {message}")
            self._log_info("")

            # Note: Target tables are auto-created by sink connector (schema.evolution=basic)
            # No manual table creation needed

            # ========================================
            # STEP 4: Ensure Sink Connector Ready (Efficient)
            # ========================================
            self._log_info("STEP 4/5: Ensuring sink connector is ready...")
            self._log_info("  → Checking if sink connector exists")
            self._log_info("  → Will reuse if compatible, update if needed, or create if missing")

            success, message = self._ensure_sink_connector_ready()
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ {message}")
            self._log_info("")

            # ========================================
            # STEP 5: Create Fresh Source Connector
            # ========================================
            self._log_info("STEP 5/5: Creating fresh source connector...")
            self._log_info("  → Snapshot mode: 'initial' (full snapshot + CDC streaming)")
            self._log_info("  → Source: {}.{}".format(
                self.config.client_database.host,
                self.config.client_database.database_name
            ))

            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True)
            )
            self._log_info(f"  → Tables: {', '.join(enabled_tables)}")

            # Create connector (version already set in Step 2)
            success, message = self._ensure_connector_running(snapshot_mode='initial')
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ Source connector created: {self.config.connector_name}")
            self._log_info("")

            # ========================================
            # Mark as Active
            # ========================================
            self._update_status('active', 'Replication active')
            self.config.is_active = True
            self.config.save()

            # ========================================
            # Start Continuous DDL Consumer
            # ========================================
            source_type = self.config.client_database.db_type.lower()
            if source_type in ('mysql', 'mssql', 'sqlserver'):
                from client.tasks import start_continuous_ddl_consumer
                start_continuous_ddl_consumer.delay(self.config.id)
                self._log_info("✓ Started continuous DDL consumer for real-time schema sync")
            elif source_type in ('postgresql', 'postgres'):
                # PostgreSQL DDL consumer requires ddl_capture.ddl_events table
                # to be included in Debezium table.include.list
                from client.tasks import start_continuous_ddl_consumer
                start_continuous_ddl_consumer.delay(self.config.id)
                self._log_info("✓ Started PostgreSQL DDL consumer (ddl_capture.ddl_events topic)")

            # ========================================
            # Success Summary
            # ========================================
            self._log_info("=" * 60)
            self._log_info("✓ CDC REPLICATION STARTED SUCCESSFULLY")
            self._log_info("=" * 60)
            self._log_info("Status: ACTIVE")
            self._log_info(f"Source Connector: {self.config.connector_name}")
            self._log_info(f"Sink Connector: {self.config.sink_connector_name}")
            self._log_info(f"Topics: {self.config.kafka_topic_prefix}.*")
            self._log_info("")
            self._log_info("Data flow:")
            self._log_info(f"  Source DB → Debezium → Kafka → JDBC Sink → Target DB")
            self._log_info("=" * 60)

            return True, "Replication started successfully"

        except Exception as e:
            error_msg = f"Failed to start replication: {str(e)}"
            self._log_error(error_msg)
            self._update_status('error', error_msg)
            return False, error_msg

    def stop_replication(self) -> Tuple[bool, str]:
        """
        Stop replication (pause source connector only).

        NOTE: Sink connector is NOT paused because it may be shared by
        multiple source connectors. Other active sources should continue
        feeding into the sink.

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STOPPING REPLICATION")
        self._log_info("=" * 60)

        try:
            # Update status
            self._update_status('paused', 'Stopping replication...')
            self.config.is_active = False
            self.config.connector_state = 'PAUSED'
            self.config.save()

            errors = []

            # Pause source connector only
            if self.config.connector_name:
                try:
                    success, error = self.connector_manager.pause_connector(self.config.connector_name)
                    if success:
                        self._log_info(f"✓ Paused source: {self.config.connector_name}")
                    else:
                        errors.append(f"Source connector: {error}")
                except Exception as e:
                    errors.append(f"Source connector: {str(e)}")

            # NOTE: Sink connector is intentionally NOT paused
            # It's shared by all source connectors for this client
            # Other active sources should continue feeding into the sink
            self._log_info("ℹ️ Sink connector not paused (shared by multiple sources)")

            # Stop continuous DDL consumer
            from client.tasks import stop_continuous_ddl_consumer
            stop_continuous_ddl_consumer.delay(self.config.id)
            self._log_info("✓ Stopped continuous DDL consumer")

            if errors:
                error_msg = "; ".join(errors)
                self._log_warning(f"⚠️ Could not pause source connector: {error_msg}")
                return False, error_msg

            self._log_info("=" * 60)
            self._log_info("✓ REPLICATION STOPPED SUCCESSFULLY")
            self._log_info("=" * 60)

            return True, "Replication stopped successfully"

        except Exception as e:
            error_msg = f"Failed to stop replication: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def restart_replication(self) -> Tuple[bool, str]:
        """
        Restart replication (stop + start).

        Returns:
            (success, message)
        """
        self._log_info("Restarting replication...")

        # Stop first
        success, message = self.stop_replication()
        if not success:
            return False, f"Failed to stop: {message}"

        # Wait a moment
        import time
        time.sleep(2)

        # Start again
        success, message = self.start_replication()
        if not success:
            return False, f"Failed to start: {message}"

        return True, "Replication restarted successfully"

    def recreate_source_connector(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Recreate the source connector WITHOUT changing version, topics, or sink.

        Use this when a source connector is FAILED/stuck and you want a fresh
        connector that resumes from the existing Kafka offsets.

        FLOW:
        1. Delete old source connector from Kafka Connect
        2. Mark old connector as deleted in ConnectorHistory
        3. Keep same version + topic prefix (NO version bump)
        4. Create new source connector with same config
        5. Leave sink connector untouched
        6. Restart DDL consumer

        Args:
            snapshot_mode: Debezium snapshot mode. Defaults to 'when_needed'
                - 'when_needed': Resume from offset, only snapshot if no offset found
                - 'initial': Full re-snapshot of all data (use if data is out of sync)

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("RECREATING SOURCE CONNECTOR")
        self._log_info(f"  Snapshot mode: {snapshot_mode}")
        self._log_info(f"  Version: {self.config.connector_version} (unchanged)")
        self._log_info(f"  Topic prefix: {self.config.kafka_topic_prefix} (unchanged)")
        self._log_info("=" * 60)

        try:
            # Validate we have an existing config with version
            if not self.config.connector_version or self.config.connector_version == 0:
                return False, "No connector version set. Use start_replication() for first-time setup."

            if not self.config.kafka_topic_prefix:
                return False, "No topic prefix set. Use start_replication() for first-time setup."

            old_connector_name = self.config.connector_name

            # ========================================
            # STEP 1: Stop DDL consumer
            # ========================================
            self._log_info("STEP 1/5: Stopping DDL consumer...")
            try:
                from client.tasks import stop_continuous_ddl_consumer
                stop_continuous_ddl_consumer.delay(self.config.id)
                self._log_info("  DDL consumer stop requested")
            except Exception as e:
                self._log_warning(f"  Could not stop DDL consumer: {e}")

            # ========================================
            # STEP 2: Delete old source connector from Kafka Connect
            # ========================================
            self._log_info("STEP 2/5: Deleting old source connector...")

            if old_connector_name:
                try:
                    success, error = self.connector_manager.delete_connector(
                        old_connector_name,
                        delete_topics=False  # Keep topics!
                    )
                    if success:
                        # Mark as deleted in history
                        from client.models import ConnectorHistory
                        ConnectorHistory.mark_connector_deleted(
                            connector_name=old_connector_name,
                            notes="Deleted for recreate (same version)"
                        )
                        self._log_info(f"  Deleted: {old_connector_name}")
                    else:
                        # Connector might already be gone — that's OK
                        self._log_warning(f"  Could not delete (may already be gone): {error}")
                except Exception as e:
                    self._log_warning(f"  Delete error (continuing anyway): {e}")
            else:
                self._log_info("  No existing connector to delete")

            # Wait for Kafka Connect to clean up
            import time
            time.sleep(3)

            # ========================================
            # STEP 3: Create new source connector (same version)
            # ========================================
            self._log_info("STEP 3/5: Creating new source connector...")
            self._log_info(f"  Version: {self.config.connector_version} (same)")
            self._log_info(f"  Topic prefix: {self.config.kafka_topic_prefix} (same)")
            self._log_info(f"  Snapshot mode: {snapshot_mode}")

            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True)
                .values_list('source_table', flat=True)
            )

            if not enabled_tables:
                return False, "No tables enabled for replication"

            self._log_info(f"  Tables: {', '.join(enabled_tables)}")

            # Generate connector config
            from jovoclient.utils.debezium.connector_templates import (
                get_connector_config_for_database,
                generate_connector_name,
            )

            db_config = self.config.client_database
            client = db_config.client

            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            schema_registry = settings.DEBEZIUM_CONFIG.get(
                'SCHEMA_REGISTRY_URL',
                'http://schema-registry:8081'
            )

            config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=enabled_tables,
                kafka_bootstrap_servers=kafka_bootstrap,
                schema_registry_url=schema_registry,
                snapshot_mode=snapshot_mode,
            )

            # Use same versioned connector name
            versioned_connector_name = generate_connector_name(
                client,
                db_config,
                version=self.config.connector_version
            )

            self.config.connector_name = versioned_connector_name
            self.config.save()

            # Create connector
            success, error = self.connector_manager.create_connector(
                connector_name=versioned_connector_name,
                config=config
            )

            if not success:
                self._update_status('error', f"Failed to create connector: {error}")
                return False, f"Failed to create connector: {error}"

            # Record in history
            from client.models import ConnectorHistory
            ConnectorHistory.record_connector_creation(
                replication_config=self.config,
                connector_name=versioned_connector_name,
                connector_version=self.config.connector_version,
                connector_type='source'
            )

            self._log_info(f"  Created: {versioned_connector_name}")

            # ========================================
            # STEP 4: Handle batch mode (pause after creation)
            # ========================================
            is_batch = self.config.processing_mode == 'batch'

            if is_batch:
                self._log_info("STEP 4/6: Batch mode — pausing connector...")

                # Wait for connector to initialize before pausing
                import time as _time
                _time.sleep(3)

                success_pause, error_pause = self.connector_manager.pause_connector(
                    versioned_connector_name
                )
                if success_pause:
                    self._log_info("  Connector paused (batch scheduler will resume)")
                    self.config.connector_state = 'PAUSED'
                else:
                    self._log_warning(f"  Could not pause: {error_pause}")
                    self.config.connector_state = 'RUNNING'

                self._update_status('active')
                self.config.is_active = True
                self.config.save()
            else:
                self._log_info("STEP 4/6: Updating status (CDC mode)...")
                self._update_status('active')
                self.config.is_active = True
                self.config.connector_state = 'RUNNING'
                self.config.save()

            # ========================================
            # STEP 5: Restart DDL consumer
            # ========================================
            self._log_info("STEP 5/6: Restarting DDL consumer...")

            source_type = self.config.client_database.db_type.lower()
            if source_type in ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres'):
                try:
                    from client.tasks import start_continuous_ddl_consumer
                    start_continuous_ddl_consumer.delay(self.config.id)
                    self._log_info("  DDL consumer started")
                except Exception as e:
                    self._log_warning(f"  Could not start DDL consumer: {e}")

            # ========================================
            # STEP 6: Verify batch schedule still exists
            # ========================================
            if is_batch:
                self._log_info("STEP 6/6: Verifying batch schedule...")
                try:
                    from django_celery_beat.models import PeriodicTask
                    task_name = f"batch_sync_config_{self.config.id}"
                    if PeriodicTask.objects.filter(name=task_name).exists():
                        self._log_info(f"  Batch schedule OK: {task_name}")
                    else:
                        self._log_warning("  Batch schedule missing — recreating...")
                        self.setup_batch_schedule()
                except Exception as e:
                    self._log_warning(f"  Could not verify batch schedule: {e}")
            else:
                self._log_info("STEP 6/6: Skipped (CDC mode, no batch schedule)")

            # ========================================
            # Done
            # ========================================
            final_state = 'PAUSED' if is_batch else 'RUNNING'
            self._log_info("=" * 60)
            self._log_info("SOURCE CONNECTOR RECREATED SUCCESSFULLY")
            self._log_info(f"  Connector: {versioned_connector_name}")
            self._log_info(f"  Version: {self.config.connector_version} (unchanged)")
            self._log_info(f"  Topics: {self.config.kafka_topic_prefix}.* (unchanged)")
            self._log_info(f"  Sink: {self.config.sink_connector_name} (untouched)")
            self._log_info(f"  Snapshot mode: {snapshot_mode}")
            self._log_info(f"  State: {final_state}")
            if is_batch:
                self._log_info(f"  Batch interval: {self.config.batch_interval}")
                self._log_info(f"  Next sync controlled by Celery Beat scheduler")
            self._log_info("=" * 60)

            mode_label = "batch" if is_batch else "CDC"
            return True, f"Source connector recreated ({snapshot_mode} snapshot, {mode_label} mode, {final_state})"

        except Exception as e:
            error_msg = f"Failed to recreate source connector: {str(e)}"
            self._log_error(error_msg)
            self._update_status('error', error_msg)
            return False, error_msg

    def delete_replication(self, delete_topics: bool = False, drop_tables: bool = False, truncate_tables: bool = False) -> Tuple[bool, str]:
        """
        Delete replication completely.

        Args:
            delete_topics: If True, permanently delete Kafka topics
            drop_tables: If True, drop target database tables
            truncate_tables: If True, truncate target database tables (data cleared, structure kept)

        Returns:
            (success, message)
        """
        self._log_info("=" * 80)
        self._log_info("DELETING REPLICATION")
        self._log_info("=" * 80)

        config_id = self.config.id
        source_connector_name = self.config.connector_name
        sink_connector_name = self.config.sink_connector_name
        
        try:
            # Mark as inactive
            self.config.is_active = False
            self.config.status = 'stopping'
            self.config.save()

            # Delete source connector
            if source_connector_name:
                try:
                    success, error = self.connector_manager.delete_connector(
                        source_connector_name,
                        delete_topics=False
                    )
                    if success:
                        # Mark as deleted in history
                        from client.models import ConnectorHistory
                        ConnectorHistory.mark_connector_deleted(
                            connector_name=source_connector_name,
                            notes="Deleted via orchestrator"
                        )
                        self._log_info(f"✓ Deleted source: {source_connector_name}")
                    else:
                        self._log_warning(f"⚠️ Source deletion failed: {error}")
                except Exception as e:
                    self._log_warning(f"⚠️ Source deletion error: {e}")

            # Handle sink connector (shared by multiple source connectors)
            # Check if there are other active source connectors for this client
            if sink_connector_name:
                from client.models import ReplicationConfig
                from client.models.database import ClientDatabase

                client = self.config.client_database.client
                target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

                # Get remaining configs (excluding the one being deleted)
                remaining_configs = ReplicationConfig.objects.filter(
                    client_database__client=client,
                    status__in=['configured', 'active', 'paused', 'error'],
                ).exclude(pk=config_id)

                if remaining_configs.exists() and target_db:
                    # Other source connectors exist - update sink with remaining topics
                    self._log_info("ℹ️ Other source connectors exist - updating sink connector")

                    remaining_topics = set()
                    for config in remaining_configs:
                        db_config = config.client_database
                        topic_prefix = config.kafka_topic_prefix

                        for tm in config.table_mappings.filter(is_enabled=True):
                            topic = format_topic_name(
                                db_config=db_config,
                                table_name=tm.source_table,
                                topic_prefix=topic_prefix,
                                schema=tm.source_schema
                            )
                            remaining_topics.add(topic)

                    if remaining_topics:
                        try:
                            success, message = self._update_sink_connector(
                                sink_connector_name,
                                remaining_topics,
                                target_db
                            )
                            if success:
                                self._log_info(f"✓ Updated sink with {len(remaining_topics)} remaining topics")
                            else:
                                self._log_warning(f"⚠️ Failed to update sink: {message}")
                        except Exception as e:
                            self._log_warning(f"⚠️ Sink update error: {e}")
                    else:
                        # No remaining topics - delete sink
                        self._log_info("ℹ️ No remaining topics - deleting sink connector")
                        self._delete_sink_connector(sink_connector_name)
                else:
                    # No other active source connectors - delete sink
                    self._log_info("ℹ️ No other source connectors - deleting sink connector")
                    self._delete_sink_connector(sink_connector_name)

            # Optionally delete Kafka topics
            if delete_topics:
                self._log_info("Deleting Kafka topics...")
                success, message = self.delete_topics()
                if success:
                    self._log_info(f"✓ {message}")
                else:
                    self._log_warning(f"⚠️ {message}")

            # Optionally drop target database tables
            if drop_tables:
                self._log_info("Dropping target database tables...")
                from client.models.database import ClientDatabase
                client = self.config.client_database.client
                target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

                if target_db:
                    enabled_tables = self.config.table_mappings.filter(is_enabled=True)
                    success, message = drop_tables_for_mappings(target_db, enabled_tables)
                    if success:
                        self._log_info(f"✓ {message}")
                    else:
                        self._log_warning(f"⚠️ {message}")
                else:
                    self._log_warning("⚠️ No target database configured")

            # Optionally truncate target database tables (clear data, keep structure)
            if truncate_tables and not drop_tables:
                self._log_info("Truncating target database tables...")
                from client.models.database import ClientDatabase
                client = self.config.client_database.client
                target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

                if target_db:
                    enabled_tables = self.config.table_mappings.filter(is_enabled=True)
                    success, message = truncate_tables_for_mappings(target_db, enabled_tables)
                    if success:
                        self._log_info(f"✓ {message}")
                    else:
                        self._log_warning(f"⚠️ {message}")
                else:
                    self._log_warning("⚠️ No target database configured")

            # Delete config
            from client.models import ReplicationConfig
            ReplicationConfig.objects.filter(pk=config_id).delete()
            self._log_info(f"✓ Deleted config (id={config_id})")

            self._log_info("=" * 80)
            self._log_info("✓ REPLICATION DELETED")
            self._log_info("=" * 80)

            return True, "Replication deleted successfully"

        except Exception as e:
            error_msg = f"Failed to delete: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Efficient Sink Connector Management
    # ==========================================

    def _ensure_sink_connector_ready(self) -> Tuple[bool, str]:
        """
        Efficiently ensure sink connector is ready.

        Strategy:
        1. Check if sink connector exists
        2. If exists:
           - Compare current tables with configured tables
           - If same: reuse (do nothing)
           - If different: update config
        3. If doesn't exist: create new

        Returns:
            (success, message)
        """
        try:
            # Get client's target database
            from client.models.database import ClientDatabase
            client = self.config.client_database.client
            target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

            if not target_db:
                return False, "No target database configured"

            # Generate sink connector name using consistent naming convention
            # IMPORTANT: Must match ClientDatabase.get_sink_connector_name() for consistency
            # Format: client_{client_id}_sink (shared by ALL source connectors for this client)
            sink_connector_name = f"client_{client.id}_sink"

            # Get topics from ALL active source connectors for this client
            # This enables multiple source connectors to feed into one shared sink
            current_topics = self._get_all_kafka_topics_for_client()

            if not current_topics:
                return False, "No tables enabled for replication"

            # Check if sink connector exists
            exists, status_data = self.connector_manager.get_connector_status(sink_connector_name)

            if exists:
                # Get existing config
                existing_config = self.connector_manager.get_connector_config(sink_connector_name)

                if existing_config:
                    # Compare topics
                    existing_topics = self._parse_topics_from_config(existing_config)

                    if existing_topics == current_topics:
                        # Perfect match - reuse as is
                        self._log_info("✓ Reusing existing sink connector")
                        self.config.sink_connector_name = sink_connector_name
                        self.config.sink_connector_state = 'RUNNING'
                        self.config.save()
                        return True, "Sink connector reused (config unchanged)"
                    else:
                        # Tables changed - update config
                        self._log_info(f"Updating sink connector topics ({len(current_topics)} tables)")
                        return self._update_sink_connector(sink_connector_name, current_topics, target_db)
                else:
                    # Couldn't get config - recreate
                    self._log_warning("Unable to retrieve sink config - recreating")
                    self.connector_manager.delete_connector(sink_connector_name, delete_topics=False)
                    import time
                    time.sleep(2)
                    return self._create_sink_connector(sink_connector_name, current_topics, target_db)
            else:
                # Doesn't exist - create new
                return self._create_sink_connector(sink_connector_name, current_topics, target_db)

        except Exception as e:
            error_msg = f"Failed to ensure sink connector ready: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _get_kafka_topics_for_config(self) -> Set[str]:
        """
        Get set of Kafka topics for currently enabled tables.

        Uses unified format_topic_name() for consistent topic naming.

        Returns:
            Set of topic names
        """
        topics = set()
        db_config = self.config.client_database
        topic_prefix = self.config.kafka_topic_prefix

        for table_mapping in self.config.table_mappings.filter(is_enabled=True):
            topic = format_topic_name(
                db_config=db_config,
                table_name=table_mapping.source_table,
                topic_prefix=topic_prefix,
                schema=table_mapping.source_schema
            )
            topics.add(topic)

        return topics

    def _get_all_kafka_topics_for_client(self) -> Set[str]:
        """
        Get topics from ALL active ReplicationConfigs for this client.

        Used for sink connector to subscribe to all source connectors,
        enabling multiple source connectors to feed into a shared sink.

        Returns:
            Set of topic names from all active configs
        """
        from client.models import ReplicationConfig

        client = self.config.client_database.client
        topics = set()

        # Get all active/configured configs for this client (any source DB)
        active_configs = ReplicationConfig.objects.filter(
            client_database__client=client,
            status__in=['configured', 'active'],
        ).exclude(
            pk=self.config.pk  # Exclude current config, we'll add it separately
        )

        # Add topics from other active configs
        for config in active_configs:
            db_config = config.client_database
            topic_prefix = config.kafka_topic_prefix

            for tm in config.table_mappings.filter(is_enabled=True):
                topic = format_topic_name(
                    db_config=db_config,
                    table_name=tm.source_table,
                    topic_prefix=topic_prefix,
                    schema=tm.source_schema
                )
                topics.add(topic)

        # Add topics from current config
        topics.update(self._get_kafka_topics_for_config())

        self._log_info(f"Aggregated {len(topics)} topics from {active_configs.count() + 1} source connector(s)")

        return topics

    def _parse_topics_from_config(self, config: Dict[str, Any]) -> Set[str]:
        """
        Parse topics from existing sink connector config.

        Args:
            config: Sink connector configuration dict

        Returns:
            Set of topic names
        """
        topics_str = config.get('topics', '')
        if topics_str:
            return set(t.strip() for t in topics_str.split(',') if t.strip())
        return set()

    def _update_sink_connector(
        self,
        sink_connector_name: str,
        topics: Set[str],
        target_db
    ) -> Tuple[bool, str]:
        """
        Update existing sink connector to use topics.regex for auto-subscription.

        Args:
            sink_connector_name: Name of sink connector
            topics: Set of Kafka topics (used for logging only)
            target_db: Target database instance

        Returns:
            (success, message)
        """
        try:
            # Get primary key fields
            primary_key_fields = self._get_primary_key_fields()

            # Generate new config with topics.regex
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )

            # Let sink_connector_templates.py handle topics.regex
            # (excludes ddl_events and debezium_signal tables)
            custom_config = {
                'name': sink_connector_name,
            }

            new_config = get_sink_connector_config_for_database(
                db_config=target_db,
                topics=None,  # Use regex instead of explicit topics
                kafka_bootstrap_servers=kafka_bootstrap,
                primary_key_fields=primary_key_fields,
                delete_enabled=True,
                custom_config=custom_config,
                replication_config=self.config,
            )

            # Update connector config
            success, error = self.connector_manager.update_connector_config(
                sink_connector_name,
                new_config
            )

            if success:
                self.config.sink_connector_name = sink_connector_name
                self.config.sink_connector_state = 'RUNNING'
                self.config.save()
                return True, f"Sink connector updated (topics.regex)"
            else:
                return False, f"Failed to update sink connector: {error}"

        except Exception as e:
            error_msg = f"Failed to update sink connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _create_sink_connector(
        self,
        sink_connector_name: str,
        topics: Set[str],
        target_db
    ) -> Tuple[bool, str]:
        """
        Create new sink connector using topics.regex for auto-subscription.

        Args:
            sink_connector_name: Name for sink connector
            topics: Set of Kafka topics (used for logging only)
            target_db: Target database instance

        Returns:
            (success, message)
        """
        try:
            # Get primary key fields
            primary_key_fields = self._get_primary_key_fields()

            # Get Kafka bootstrap servers
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )

            # Let sink_connector_templates.py handle topics.regex
            # (excludes ddl_events and debezium_signal tables)
            custom_config = {
                'name': sink_connector_name,
            }

            sink_config = get_sink_connector_config_for_database(
                db_config=target_db,
                topics=None,  # Use regex instead of explicit topics
                kafka_bootstrap_servers=kafka_bootstrap,
                primary_key_fields=primary_key_fields,
                delete_enabled=True,
                custom_config=custom_config,
                replication_config=self.config,
            )

            # Create connector
            success, error = self.connector_manager.create_connector(
                connector_name=sink_connector_name,
                config=sink_config
            )

            if success:
                # Record sink connector creation in history
                from client.models import ConnectorHistory
                ConnectorHistory.record_connector_creation(
                    replication_config=self.config,
                    connector_name=sink_connector_name,
                    connector_version=1,  # Sink connectors don't use versions
                    connector_type='sink'
                )

                # Log success with configuration details
                self._log_info(f"✓ Sink connector created successfully")
                self._log_info(f"  Connector: {sink_connector_name}")
                self._log_info(f"  Target: {target_db.db_type} ({target_db.host}:{target_db.port})")
                self._log_info(f"  Database: {target_db.database_name}")
                self._log_info(f"  Primary key fields: {primary_key_fields or 'auto-detected'}")
                self._log_info(f"  Kafka bootstrap: {kafka_bootstrap}")

                self.config.sink_connector_name = sink_connector_name
                self.config.sink_connector_state = 'RUNNING'
                self.config.save()

                return True, f"Sink connector '{sink_connector_name}' created successfully"
            else:
                return False, f"Failed to create sink connector: {error}"

        except Exception as e:
            error_msg = f"Failed to create sink connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _get_primary_key_fields(self) -> Optional[str]:
        """
        Extract primary key fields from enabled tables.

        Note: With primary.key.mode=record_key (default), JDBC sink connector
        auto-extracts PKs from Kafka message keys. This returns None to let
        the sink connector handle PKs automatically per table.

        Returns:
            None - let sink connector auto-detect PKs from record keys
        """
        # With record_key mode, PKs are extracted from Kafka message keys
        # which Debezium sets correctly per table. No need to specify here.
        return None

    def _delete_sink_connector(self, sink_connector_name: str) -> bool:
        """
        Delete sink connector and mark in history.

        Args:
            sink_connector_name: Name of the sink connector to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            success, error = self.connector_manager.delete_connector(
                sink_connector_name,
                delete_topics=False
            )
            if success:
                from client.models import ConnectorHistory
                ConnectorHistory.mark_connector_deleted(
                    connector_name=sink_connector_name,
                    notes="Deleted via orchestrator (no remaining sources)"
                )
                self._log_info(f"✓ Deleted sink: {sink_connector_name}")
                return True
            else:
                self._log_warning(f"⚠️ Sink deletion failed: {error}")
                return False
        except Exception as e:
            self._log_warning(f"⚠️ Sink deletion error: {e}")
            return False

    # ==========================================
    # Source Connector Management (Existing)
    # ==========================================

    def _ensure_connector_running(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Ensure Debezium source connector exists and is running.

        Creates new connector with incremented version.

        Args:
            snapshot_mode: Debezium snapshot mode

        Returns:
            (success, message)
        """
        # Generate versioned connector name
        from jovoclient.utils.debezium.connector_templates import generate_connector_name
        db_config = self.config.client_database
        client = db_config.client
        
        versioned_connector_name = generate_connector_name(
            client,
            db_config,
            version=self.config.connector_version
        )

        # Create connector (version will be incremented in _create_connector)
        # Note: Old connectors are NOT deleted - each version runs independently
        # The database.server.name includes version to prevent JMX MBean conflicts
        return self._create_connector(snapshot_mode=snapshot_mode)

    def _create_connector(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Create new Debezium source connector.

        NOTE: connector_version and kafka_topic_prefix should be set BEFORE calling this method
        (typically in start_replication). This method uses the existing version to ensure
        consistency between topics, sink connector, and source connector.

        Returns:
            (success, message)
        """
        try:
            from client.models import ConnectorHistory
            db_config = self.config.client_database
            client = db_config.client

            # Use existing version (should be set by caller, e.g., start_replication)
            # Only get next version if not already set
            if not self.config.connector_version or self.config.connector_version == 0:
                next_version = ConnectorHistory.get_next_version(
                    client_id=client.id,
                    database_id=db_config.id,
                    connector_type='source'
                )
                self.config.connector_version = next_version
                self.config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{next_version}"
                self.config.save()
            else:
                # Version already set by caller - use it
                next_version = self.config.connector_version

            # Generate configuration
            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True)
            )

            if not enabled_tables:
                return False, "No tables enabled for replication"

            max_tables = settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25)
            if len(enabled_tables) > max_tables:
                return False, f"Too many tables ({len(enabled_tables)}). Maximum {max_tables} tables per connector to prevent worker crashes."

            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            schema_registry = settings.DEBEZIUM_CONFIG.get(
                'SCHEMA_REGISTRY_URL',
                'http://schema-registry:8081'
            )

            config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=enabled_tables,
                kafka_bootstrap_servers=kafka_bootstrap,
                schema_registry_url=schema_registry,
                snapshot_mode=snapshot_mode,
            )

            # Generate versioned connector name
            from jovoclient.utils.debezium.connector_templates import generate_connector_name
            versioned_connector_name = generate_connector_name(
                client,
                db_config,
                version=self.config.connector_version
            )

            # Update config
            self.config.connector_name = versioned_connector_name
            self.config.save()

            # Create connector
            success, error = self.connector_manager.create_connector(
                connector_name=versioned_connector_name,
                config=config
            )

            if success:
                # Record connector creation in history
                ConnectorHistory.record_connector_creation(
                    replication_config=self.config,
                    connector_name=versioned_connector_name,
                    connector_version=self.config.connector_version,
                    connector_type='source'
                )

                # Log success with configuration details
                self._log_info(f"✓ Source connector created successfully")
                self._log_info(f"  Connector: {versioned_connector_name}")
                self._log_info(f"  Version: {self.config.connector_version}")
                self._log_info(f"  Snapshot mode: {snapshot_mode}")
                self._log_info(f"  Tables: {', '.join(enabled_tables)}")
                self._log_info(f"  Kafka bootstrap: {kafka_bootstrap}")
                self._log_info(f"  Topic prefix: {self.config.kafka_topic_prefix}")

                return True, "Connector created successfully"
            else:
                return False, f"Failed to create connector: {error}"

        except Exception as e:
            error_msg = f"Failed to create connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Topic Management (Delegated)
    # ==========================================

    def create_topics(self) -> Tuple[bool, str]:
        """Create Kafka topics for replication."""
        try:
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_BOOTSTRAP_SERVERS',
                'localhost:9092,localhost:9094,localhost:9096'
            )
            topic_manager = KafkaTopicManager(bootstrap_servers=kafka_bootstrap)
            return topic_manager.create_topics_for_config(self.config)
        except Exception as e:
            error_msg = f"Failed to create topics: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def delete_topics(self) -> Tuple[bool, str]:
        """Delete Kafka topics for replication."""
        try:
            kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_BOOTSTRAP_SERVERS',
                'localhost:9092,localhost:9094,localhost:9096'
            )
            topic_manager = KafkaTopicManager(bootstrap_servers=kafka_bootstrap)
            topic_prefix = self.config.kafka_topic_prefix
            return topic_manager.delete_topics_by_prefix(topic_prefix)
        except Exception as e:
            error_msg = f"Failed to delete topics: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Table Management (Add/Remove)
    # ==========================================

    def remove_tables(self, table_names: list) -> Tuple[bool, str]:
        """
        Remove tables from a connector with full cleanup.

        This will:
        1. Delete Kafka topics for the removed tables
        2. Truncate target tables in target database
        3. Disable table mappings in database
        4. Update source connector config
        5. Restart source connector
        6. Restart sink connector

        Args:
            table_names: List of source table names to remove

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("REMOVING TABLES FROM CONNECTOR")
        self._log_info(f"Tables to remove: {', '.join(table_names)}")
        self._log_info("=" * 60)

        try:
            db_config = self.config.client_database
            client = db_config.client

            # Get table mappings for the tables being removed
            table_mappings_to_remove = self.config.table_mappings.filter(
                source_table__in=table_names,
                is_enabled=True
            )

            if not table_mappings_to_remove.exists():
                return False, "No matching enabled tables found to remove"

            # ========================================
            # STEP 1: Delete Kafka topics for removed tables
            # ========================================
            self._log_info("STEP 1/5: Deleting Kafka topics for removed tables...")

            topic_prefix = self.config.kafka_topic_prefix
            topics_to_delete = [
                format_topic_name(
                    db_config=db_config,
                    table_name=tm.source_table,
                    topic_prefix=topic_prefix,
                    schema=tm.source_schema
                )
                for tm in table_mappings_to_remove
            ]

            deleted_topics = []
            for topic in topics_to_delete:
                try:
                    success, error = self.topic_manager.delete_topic(topic)
                    if success:
                        deleted_topics.append(topic)
                        self._log_info(f"  ✓ Deleted topic: {topic}")
                    else:
                        self._log_warning(f"  ⚠️ Failed to delete topic {topic}: {error}")
                except Exception as e:
                    self._log_warning(f"  ⚠️ Error deleting topic {topic}: {e}")

            self._log_info(f"✓ Deleted {len(deleted_topics)}/{len(topics_to_delete)} topics")

            # ========================================
            # STEP 2: Truncate target tables
            # ========================================
            self._log_info("STEP 2/5: Truncating target database tables...")

            from client.models.database import ClientDatabase
            target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

            if target_db:
                success, message = truncate_tables_for_mappings(target_db, table_mappings_to_remove)
                if success:
                    self._log_info(f"✓ {message}")
                else:
                    self._log_warning(f"⚠️ {message}")
            else:
                self._log_warning("⚠️ No target database configured - skipping table truncate")

            # ========================================
            # STEP 3: Disable table mappings in database
            # ========================================
            self._log_info("STEP 3/5: Disabling table mappings...")

            disabled_count = table_mappings_to_remove.update(is_enabled=False)
            self._log_info(f"✓ Disabled {disabled_count} table mappings")

            # Check if any tables remain
            remaining_tables = list(
                self.config.table_mappings.filter(is_enabled=True)
                .values_list('source_table', flat=True)
            )

            if not remaining_tables:
                return False, "Cannot remove all tables. Delete the connector instead."

            # ========================================
            # STEP 4: Update and restart source connector
            # ========================================
            self._log_info("STEP 4/6: Updating source connector configuration...")

            # For batch mode: check if connector is paused and resume it first
            was_paused = self._resume_if_paused()

            source_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=remaining_tables
            )

            success, error = self.connector_manager.update_connector_config(
                self.config.connector_name,
                source_config
            )
            if not success:
                # Re-pause if we resumed it
                self._re_pause_if_batch_mode(was_paused)
                return False, f"Failed to update source connector: {error}"

            self._log_info(f"✓ Source connector updated with {len(remaining_tables)} tables")

            # Restart source connector
            success, error = self.connector_manager.restart_connector(self.config.connector_name)
            if success:
                self._log_info(f"✓ Source connector restarted")
            else:
                self._log_warning(f"⚠️ Source connector restart failed: {error}")

            # ========================================
            # STEP 5: Restart sink connector
            # ========================================
            self._log_info("STEP 5/6: Restarting sink connector...")

            if self.config.sink_connector_name:
                success, error = self.connector_manager.restart_connector(
                    self.config.sink_connector_name
                )
                if success:
                    self._log_info(f"✓ Sink connector restarted")
                else:
                    self._log_warning(f"⚠️ Sink connector restart failed: {error}")

            # ========================================
            # STEP 6: Re-pause for batch mode (if applicable)
            # ========================================
            if was_paused:
                self._log_info("STEP 6/6: Re-pausing connector for batch mode...")
                self._re_pause_if_batch_mode(was_paused)
            else:
                self._log_info("STEP 6/6: Skipped (connector was not paused)")

            self._log_info("=" * 60)
            self._log_info("✓ TABLES REMOVED SUCCESSFULLY")
            self._log_info("=" * 60)

            return True, f"Removed {len(table_names)} tables (topics deleted, target tables dropped)"

        except Exception as e:
            error_msg = f"Failed to remove tables: {str(e)}"
            self._log_error(error_msg)
            # Ensure we re-pause on error if we resumed
            if 'was_paused' in locals() and was_paused:
                self._re_pause_if_batch_mode(was_paused)
            return False, error_msg

    def add_tables(self, table_names: list, target_table_names: dict = None) -> Tuple[bool, str]:
        """
        Add tables to a connector with incremental snapshot.

        This will:
        1. Create/re-enable table mappings (all columns enabled)
        2. Create Kafka topics for new tables
        3. Update source connector config
        4. Send incremental snapshot signal (db-based or Kafka-based)
        5. Restart sink connector to pick up new topics

        Args:
            table_names: List of source table names to add

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("ADDING TABLES TO CONNECTOR")
        self._log_info(f"Tables to add: {', '.join(table_names)}")
        self._log_info("=" * 60)

        db_config = self.config.client_database
        client = db_config.client
        added_tables = []
        failed_tables = []

        try:
            # ========================================
            # STEP 1: Create/re-enable table mappings
            # ========================================
            self._log_info("STEP 1/6: Creating table mappings...")

            from client.models.replication import TableMapping
            from client.utils.database_utils import get_table_schema

            for table_name in table_names:
                try:
                    schema = get_table_schema(db_config, table_name)
                    columns = schema.get('columns', [])
                    primary_keys = schema.get('primary_keys', [])

                    for col in columns:
                        col['primary_key'] = col['name'] in primary_keys

                    # Parse schema and table name
                    if (db_config.db_type in ['mssql', 'oracle']) and '.' in table_name:
                        source_schema, actual_table_name = table_name.split('.', 1)
                    else:
                        source_schema = schema.get('schema', '')
                        actual_table_name = table_name

                    # Build target table name — use custom name from caller if provided,
                    # otherwise compute the default to match sink connector transform
                    # ($1_$2: {schema}_{table}).
                    custom_name = (target_table_names or {}).get(table_name, '')
                    if custom_name:
                        target_table_name = custom_name
                    elif db_config.db_type == 'mysql':
                        target_table_name = f"{db_config.database_name}_{actual_table_name}"
                    elif db_config.db_type == 'postgresql':
                        pg_schema = source_schema or 'public'
                        target_table_name = f"{pg_schema}_{actual_table_name}"
                    elif db_config.db_type == 'mssql':
                        mssql_schema = source_schema or 'dbo'
                        target_table_name = f"{mssql_schema}_{actual_table_name}"
                    elif db_config.db_type == 'oracle':
                        oracle_schema = source_schema or db_config.username.upper()
                        target_table_name = f"{oracle_schema}_{actual_table_name}"
                    else:
                        target_table_name = actual_table_name

                    self._log_info(f"  → Target table name: {target_table_name}")

                    # Check for existing disabled mapping
                    existing_mapping = TableMapping.objects.filter(
                        replication_config=self.config,
                        source_table=actual_table_name,
                        is_enabled=False
                    ).first()

                    if existing_mapping:
                        existing_mapping.is_enabled = True
                        existing_mapping.source_schema = source_schema
                        existing_mapping.target_table = target_table_name
                        existing_mapping.save()
                        table_mapping = existing_mapping
                        self._log_info(f"  ✓ Re-enabled mapping: {table_name} → {target_table_name}")
                    else:
                        table_mapping = TableMapping.objects.create(
                            replication_config=self.config,
                            source_table=actual_table_name,
                            target_table=target_table_name,
                            source_schema=source_schema,
                            is_enabled=True,
                        )
                        self._log_info(f"  ✓ Created mapping: {table_name} → {target_table_name}")

                    added_tables.append(table_name)

                except Exception as e:
                    self._log_warning(f"  ⚠️ Failed to create mapping for {table_name}: {e}")
                    failed_tables.append(table_name)

            if not added_tables:
                return False, "No tables could be added"

            self._log_info(f"✓ Created mappings for {len(added_tables)} tables")

            # ========================================
            # STEP 2: Create Kafka topics
            # ========================================
            self._log_info("STEP 2/6: Creating Kafka topics...")

            success, message = self.create_topics()
            if success:
                self._log_info(f"✓ {message}")
            else:
                self._log_warning(f"⚠️ Topic creation warning: {message}")

            # Note: Target tables are auto-created by sink connector (schema.evolution=basic)

            # ========================================
            # STEP 3: Update source connector config
            # ========================================
            self._log_info("STEP 3/6: Updating source connector...")

            # For batch mode: check if connector is paused and resume it first
            was_paused = self._resume_if_paused()

            all_tables = list(
                self.config.table_mappings.filter(is_enabled=True)
                .values_list('source_table', flat=True)
            )

            source_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=all_tables
            )

            success, error = self.connector_manager.update_connector_config(
                self.config.connector_name,
                source_config
            )
            if not success:
                # Re-pause if we resumed it
                self._re_pause_if_batch_mode(was_paused)
                return False, f"Failed to update source connector: {error}"

            self._log_info(f"✓ Source connector config updated with {len(all_tables)} tables")

            # CRITICAL: Restart connector BEFORE sending signal
            # The connector needs to restart and refresh schema from database
            # before it can process incremental snapshot signals for new tables
            import time

            self._log_info("  → Restarting connector to apply new config...")
            self.connector_manager.restart_connector(self.config.connector_name)

            # Wait for connector to restart and refresh schema
            # Schema refresh can take several seconds depending on database size
            self._log_info("  → Waiting for connector to restart and refresh schema...")
            time.sleep(3)  # Initial wait for restart

            # Poll for RUNNING state with longer timeout
            for i in range(20):  # Up to 10 seconds
                time.sleep(0.5)
                exists, status_data = self.connector_manager.get_connector_status(self.config.connector_name)
                if exists and status_data:
                    state = status_data.get('connector', {}).get('state', '')
                    if state == 'RUNNING':
                        # Additional wait for schema refresh after RUNNING
                        self._log_info("  → Connector RUNNING, waiting for schema refresh...")
                        time.sleep(3)  # Extra time for schema history to be read
                        self._log_info("✓ Connector ready with updated schema")
                        break
                    elif state == 'FAILED':
                        return False, f"Connector failed after config update"

            # ========================================
            # STEP 4: Send incremental snapshot signal
            # ========================================
            self._log_info("STEP 4/6: Sending incremental snapshot signal...")

            from jovoclient.utils.kafka.signal import send_incremental_snapshot_signal

            signal_id, method = send_incremental_snapshot_signal(
                database=db_config,
                replication_config=self.config,
                tables=added_tables
            )

            self._log_info(f"✓ Signal sent via {method} - ID: {signal_id}")

            # Verify incremental snapshot started via Jolokia
            self._verify_incremental_snapshot_started(signal_id)

            # ========================================
            # STEP 5: Restart sink connector
            # ========================================
            self._log_info("STEP 5/6: Restarting sink connector...")

            if self.config.sink_connector_name:
                # Update sink connector config so any new custom table rename transforms
                # (for tables with non-default target names) are applied, then restart
                # to pick up the new topics via topics.regex auto-subscription.
                target_db = client.get_target_database()
                if target_db:
                    self._update_sink_connector(
                        self.config.sink_connector_name,
                        set(),  # topics unused — sink uses topics.regex
                        target_db
                    )
                    self._log_info("✓ Sink connector config updated with new table transforms")
                else:
                    self.connector_manager.restart_connector(self.config.sink_connector_name)
                    self._log_info("✓ Sink connector restarted (uses topics.regex for auto-subscription)")

            # ========================================
            # STEP 5.5: Add foreign keys to new target tables
            # ========================================
            self._log_info("STEP 5.5/6: Adding foreign keys to new target tables...")
            from client.utils.table_creator import add_foreign_keys_to_target
            try:
                # Wait a moment for sink to create the tables
                import time
                time.sleep(5)
                created, skipped, errors = add_foreign_keys_to_target(self.config, specific_tables=added_tables)
                self._log_info(f"✓ Foreign keys: {created} created, {skipped} skipped")
                if errors:
                    self._log_warning(f"⚠️ FK errors: {errors}")
            except Exception as e:
                self._log_warning(f"⚠️ Could not add foreign keys: {e}")

            # ========================================
            # STEP 6: Handle batch mode state
            # ========================================
            # NOTE: For add_tables, we do NOT re-pause immediately because the
            # incremental snapshot needs to run. The connector must stay RUNNING
            # until the snapshot completes. The batch scheduler will pause it
            # on the next scheduled cycle.
            if was_paused:
                self._log_info("STEP 6/6: Batch mode - connector will stay RUNNING for snapshot")
                self._log_info("  → Incremental snapshot is in progress")
                self._log_info("  → Connector will be paused on next batch schedule")
                # Update state to reflect it's now running
                self.config.connector_state = 'RUNNING'
                self.config.save()
            else:
                self._log_info("STEP 6/6: Skipped (connector was not paused)")

            self._log_info("=" * 60)
            self._log_info("✓ TABLES ADDED SUCCESSFULLY")
            self._log_info("=" * 60)

            result_msg = f"Added {len(added_tables)} tables via {method} signal (ID: {signal_id})"
            if was_paused:
                result_msg += " | Connector running for snapshot (will pause on next schedule)"
            if failed_tables:
                result_msg += f" | {len(failed_tables)} failed: {', '.join(failed_tables)}"

            return True, result_msg

        except Exception as e:
            error_msg = f"Failed to add tables: {str(e)}"
            self._log_error(error_msg)
            # On error, re-pause if we resumed (snapshot didn't start)
            if 'was_paused' in locals() and was_paused:
                self._re_pause_if_batch_mode(was_paused)
            return False, error_msg

    # ==========================================
    # Jolokia Snapshot Verification
    # ==========================================

    def _verify_incremental_snapshot_started(
        self, signal_id: str, grace_seconds: int = 10, polls: int = 3
    ):
        """Best-effort check that an incremental snapshot actually started.

        Polls Jolokia a few times after the signal is sent. Logs a
        confirmation or warning but never fails the overall operation —
        the signal may simply be delayed or Jolokia may be unavailable.
        """
        import time

        db_type = self.config.client_database.db_type
        topic_prefix = self.config.kafka_topic_prefix

        # Give Debezium a moment to pick up the signal
        time.sleep(grace_seconds)

        for attempt in range(polls):
            progress = self.jolokia.get_incremental_snapshot_progress(
                db_type, topic_prefix
            )
            if progress and progress.get('running'):
                tables = progress.get('total_tables', '?')
                self._log_info(
                    f"  Incremental snapshot confirmed running "
                    f"(signal {signal_id}): {tables} tables queued"
                )
                return

            if attempt < polls - 1:
                time.sleep(5)

        self._log_warning(
            f"  Could not confirm incremental snapshot via Jolokia "
            f"(signal {signal_id}) — may still be starting"
        )

    # ==========================================
    # Status and Health
    # ==========================================

    def get_unified_status(self) -> Dict[str, Any]:
        """Get comprehensive status of replication."""
        connector_status = self._get_connector_status()
        sink_status = self._get_sink_connector_status()
        overall_health = self._calculate_overall_health(connector_status, sink_status)
        snapshot_progress = self._get_snapshot_progress()

        return {
            'overall': overall_health,
            'source_connector': connector_status,
            'sink_connector': sink_status,
            'snapshot': snapshot_progress,
            'config': {
                'status': self.config.status,
                'is_active': self.config.is_active,
                'connector_name': self.config.connector_name,
                'sink_connector_name': self.config.sink_connector_name,
                'kafka_topic_prefix': self.config.kafka_topic_prefix,
            },
            'statistics': {
                'total_rows_synced': sum(
                    tm.total_rows_synced or 0
                    for tm in self.config.table_mappings.all()
                ),
                'tables_enabled': self.config.table_mappings.filter(is_enabled=True).count(),
                'last_sync_at': self.config.last_sync_at.isoformat() if self.config.last_sync_at else None,
            },
            'batch_schedule': self._get_batch_schedule() if self.config.processing_mode == 'batch' else None,
            'timestamp': timezone.now().isoformat(),
        }

    def _get_snapshot_progress(self) -> Optional[Dict[str, Any]]:
        """Get current snapshot progress via Jolokia.

        Returns a normalised progress dict if a snapshot (initial or
        incremental) is active, or None if nothing is running / Jolokia
        is unreachable.
        """
        try:
            db_type = self.config.client_database.db_type
            topic_prefix = self.config.kafka_topic_prefix
            return self.jolokia.get_active_snapshot_progress(db_type, topic_prefix)
        except Exception:
            return None

    def _get_batch_schedule(self) -> Dict[str, Any]:
        """Get batch schedule info for the monitor page."""
        schedule_active = False
        if self.config.batch_celery_task_name:
            try:
                from django_celery_beat.models import PeriodicTask
                schedule_active = PeriodicTask.objects.filter(
                    name=self.config.batch_celery_task_name,
                    enabled=True,
                ).exists()
            except (ImportError, Exception):
                pass

        is_on_schedule = schedule_active and self.config.next_batch_run is not None

        return {
            'next_batch_run': self.config.next_batch_run.isoformat() if self.config.next_batch_run else None,
            'last_batch_run': self.config.last_batch_run.isoformat() if self.config.last_batch_run else None,
            'batch_interval': self.config.batch_interval,
            'batch_interval_display': self.config.get_batch_interval_display() if self.config.batch_interval else None,
            'batch_max_catchup_minutes': self.config.batch_max_catchup_minutes,
            'celery_task_name': self.config.batch_celery_task_name,
            'schedule_active': schedule_active,
            'is_on_schedule': is_on_schedule,
        }

    def _get_connector_status(self) -> Dict[str, Any]:
        """Get Debezium source connector status."""
        try:
            result = self.connector_manager.get_connector_status(self.config.connector_name)
            
            if isinstance(result, tuple):
                success, status_data = result
                if not success or not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}
            else:
                status_data = result
                if not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}

            state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = status_data.get('tasks', [])
            has_failed_task = any(t.get('state') == 'FAILED' for t in tasks)
            return {
                'state': state,
                'healthy': state in ('RUNNING', 'PAUSED') and not has_failed_task,
                'tasks': tasks,
            }
        except Exception as e:
            return {'state': 'ERROR', 'healthy': False, 'message': str(e)}

    def _get_sink_connector_status(self) -> Dict[str, Any]:
        """Get JDBC Sink connector status."""
        try:
            if not self.config.sink_connector_name:
                return {'state': 'NOT_CONFIGURED', 'healthy': False}

            result = self.connector_manager.get_connector_status(self.config.sink_connector_name)

            if isinstance(result, tuple):
                success, status_data = result
                if not success or not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}
            else:
                status_data = result
                if not status_data:
                    return {'state': 'NOT_FOUND', 'healthy': False}

            state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = status_data.get('tasks', [])
            has_failed_task = any(t.get('state') == 'FAILED' for t in tasks)
            return {
                'state': state,
                'healthy': state in ('RUNNING', 'PAUSED') and not has_failed_task,
                'tasks': tasks,
            }
        except Exception as e:
            return {'state': 'ERROR', 'healthy': False, 'message': str(e)}

    def _calculate_overall_health(self, connector_status: Dict, sink_status: Dict) -> str:
        """Calculate overall health: 'healthy' or 'unhealthy'."""
        connector_healthy = connector_status.get('healthy', False)
        sink_healthy = sink_status.get('healthy', False)

        if connector_healthy and sink_healthy:
            return 'healthy'
        return 'unhealthy'

    # ==========================================
    # Batch Connector State Helpers
    # ==========================================

    def _is_connector_paused(self) -> bool:
        """
        Check if the source connector is currently paused.

        Returns:
            True if connector is paused, False otherwise
        """
        if not self.config.connector_name:
            return False

        exists, status_data = self.connector_manager.get_connector_status(
            self.config.connector_name
        )
        if exists and status_data:
            state = status_data.get('connector', {}).get('state', '')
            return state == 'PAUSED'
        return False

    def _resume_if_paused(self) -> bool:
        """
        Resume connector if it's paused (for batch mode operations).

        Returns:
            True if connector was paused and resumed, False if it was already running
        """
        if self._is_connector_paused():
            self._log_info("  → Connector is PAUSED, resuming for config update...")
            success, _ = self.connector_manager.resume_connector(self.config.connector_name)
            if success:
                # Wait for connector to resume
                import time
                time.sleep(2)
                self._log_info("  ✓ Connector resumed")
                return True
            else:
                self._log_warning("  ⚠️ Failed to resume paused connector")
        return False

    def _re_pause_if_batch_mode(self, was_paused: bool) -> None:
        """
        Re-pause connector if it's in batch mode and was previously paused.

        Args:
            was_paused: Whether the connector was paused before the operation
        """
        if was_paused and self.config.processing_mode == 'batch':
            self._log_info("  → Re-pausing connector for batch mode...")
            success, _ = self.connector_manager.pause_connector(self.config.connector_name)
            if success:
                self.config.connector_state = 'PAUSED'
                self.config.save()
                self._log_info("  ✓ Connector re-paused for batch mode")
            else:
                self._log_warning("  ⚠️ Failed to re-pause connector")

    # ==========================================
    # Batch Processing Methods
    # ==========================================

    def resume_connector(self) -> Tuple[bool, str]:
        """
        Resume a paused source connector.
        Used by batch processing to start a sync window.

        Returns:
            (success, message)
        """
        self._log_info("Resuming source connector...")

        if not self.config.connector_name:
            return False, "No connector configured"

        try:
            success, error = self.connector_manager.resume_connector(self.config.connector_name)

            if success:
                self.config.connector_state = 'RUNNING'
                self.config.save()
                self._log_info(f"✓ Connector resumed: {self.config.connector_name}")
                return True, "Connector resumed successfully"
            else:
                return False, f"Failed to resume connector: {error}"

        except Exception as e:
            error_msg = f"Error resuming connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def pause_connector(self) -> Tuple[bool, str]:
        """
        Pause a running source connector.
        Used by batch processing to end a sync window.

        Returns:
            (success, message)
        """
        self._log_info("Pausing source connector...")

        if not self.config.connector_name:
            return False, "No connector configured"

        try:
            success, error = self.connector_manager.pause_connector(self.config.connector_name)

            if success:
                self.config.connector_state = 'PAUSED'
                self.config.save()
                self._log_info(f"✓ Connector paused: {self.config.connector_name}")
                return True, "Connector paused successfully"
            else:
                return False, f"Failed to pause connector: {error}"

        except Exception as e:
            error_msg = f"Error pausing connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def restart_failed_tasks(self, connector_name: str = None) -> Tuple[bool, str]:
        """
        Restart only FAILED tasks for a connector.

        Args:
            connector_name: Connector to target. Defaults to source connector.

        Returns:
            (success, message)
        """
        connector_name = connector_name or self.config.connector_name
        if not connector_name:
            return False, "No connector configured"

        try:
            exists, status_data = self.connector_manager.get_connector_status(
                connector_name
            )
            if not exists or not status_data:
                return False, "Connector not found"

            tasks = status_data.get('tasks', [])
            failed_tasks = [t for t in tasks if t.get('state') == 'FAILED']

            if not failed_tasks:
                return True, "No failed tasks to restart"

            restarted = 0
            errors = []
            for task in failed_tasks:
                task_id = task.get('id', 0)
                success, error = self.connector_manager.restart_task(
                    connector_name, task_id
                )
                if success:
                    restarted += 1
                else:
                    errors.append(f"Task {task_id}: {error}")

            if errors:
                return False, f"Restarted {restarted}/{len(failed_tasks)} tasks. Errors: {'; '.join(errors)}"

            self._log_info(f"✓ Restarted {restarted} failed task(s) for {connector_name}")
            return True, f"Restarted {restarted} failed task(s)"

        except Exception as e:
            error_msg = f"Error restarting failed tasks: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def restart_all_tasks(self, connector_name: str = None) -> Tuple[bool, str]:
        """
        Restart all tasks for a connector.

        Args:
            connector_name: Connector to target. Defaults to source connector.

        Returns:
            (success, message)
        """
        connector_name = connector_name or self.config.connector_name
        if not connector_name:
            return False, "No connector configured"

        try:
            exists, status_data = self.connector_manager.get_connector_status(
                connector_name
            )
            if not exists or not status_data:
                return False, "Connector not found"

            tasks = status_data.get('tasks', [])
            if not tasks:
                return True, "No tasks to restart"

            restarted = 0
            errors = []
            for task in tasks:
                task_id = task.get('id', 0)
                success, error = self.connector_manager.restart_task(
                    connector_name, task_id
                )
                if success:
                    restarted += 1
                else:
                    errors.append(f"Task {task_id}: {error}")

            if errors:
                return False, f"Restarted {restarted}/{len(tasks)} tasks. Errors: {'; '.join(errors)}"

            self._log_info(f"✓ Restarted {restarted} task(s) for {connector_name}")
            return True, f"Restarted {restarted} task(s)"

        except Exception as e:
            error_msg = f"Error restarting tasks: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def sync_with_schedule(self) -> Tuple[bool, str]:
        """
        Re-establish batch schedule for a connector that is out of sync.

        If the connector is currently RUNNING (manually resumed), pause it first,
        then set up the Celery Beat periodic task so it returns to its normal cycle.

        Returns:
            (success, message) - returns early if already on schedule.
        """
        if self.config.processing_mode != 'batch':
            return False, "Only batch connectors can be synced with schedule"

        if not self.config.batch_interval:
            return False, "No batch interval configured"

        try:
            # Check if already on schedule
            schedule_info = self._get_batch_schedule()
            if schedule_info['is_on_schedule']:
                # Also check connector is actually paused (expected state between runs)
                exists, status_data = self.connector_manager.get_connector_status(
                    self.config.connector_name
                )
                if exists and status_data:
                    state = status_data.get('connector', {}).get('state', '')
                    if state == 'PAUSED':
                        return True, "Connector is already on schedule"

            # If connector is running, pause it first
            exists, status_data = self.connector_manager.get_connector_status(
                self.config.connector_name
            )
            if exists and status_data:
                state = status_data.get('connector', {}).get('state', '')
                if state == 'RUNNING':
                    self._log_info("Pausing connector before re-establishing schedule...")
                    self.pause_connector()

            # Re-establish the batch schedule
            success, message = self.setup_batch_schedule()
            if success:
                self._log_info("✓ Connector synced back to schedule")
                return True, "Connector synced back to schedule"
            return False, message

        except ImportError:
            return False, "django-celery-beat is not installed"
        except Exception as e:
            error_msg = f"Error syncing with schedule: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _get_batch_interval_seconds(self) -> int:
        """
        Convert batch interval choice to seconds.

        Returns:
            Interval in seconds
        """
        interval_map = {
            '5m': 5 * 60,         # 5 minutes
            '15m': 15 * 60,       # 15 minutes
            '30m': 30 * 60,       # 30 minutes
            '1h': 60 * 60,        # 1 hour
            '2h': 2 * 60 * 60,    # 2 hours
            '6h': 6 * 60 * 60,    # 6 hours
            '12h': 12 * 60 * 60,  # 12 hours
            '24h': 24 * 60 * 60,  # 24 hours
        }
        return interval_map.get(self.config.batch_interval, 2 * 60 * 60)

    def setup_batch_schedule(self) -> Tuple[bool, str]:
        """
        Create or update Celery Beat periodic task for batch processing.

        Uses django-celery-beat's IntervalSchedule and PeriodicTask models.

        Returns:
            (success, message)
        """
        if self.config.processing_mode != 'batch':
            return False, "Connector is not in batch processing mode"

        if not self.config.batch_interval:
            return False, "No batch interval configured"

        try:
            from django_celery_beat.models import PeriodicTask, IntervalSchedule
            import json

            # Get interval in seconds
            interval_seconds = self._get_batch_interval_seconds()

            # Create or get interval schedule
            schedule, _ = IntervalSchedule.objects.get_or_create(
                every=interval_seconds,
                period=IntervalSchedule.SECONDS,
            )

            # Generate unique task name
            task_name = f"batch_sync_config_{self.config.id}"

            # Create or update periodic task
            periodic_task, created = PeriodicTask.objects.update_or_create(
                name=task_name,
                defaults={
                    'interval': schedule,
                    'task': 'client.tasks.run_batch_sync',
                    'args': json.dumps([self.config.id]),
                    'enabled': True,
                }
            )

            # Calculate next run time
            self.config.batch_celery_task_name = task_name
            self.config.next_batch_run = timezone.now() + timezone.timedelta(seconds=interval_seconds)
            self.config.save()

            action = "created" if created else "updated"
            self._log_info(f"✓ Batch schedule {action}: {task_name} (every {self.config.batch_interval})")

            return True, f"Batch schedule {action} successfully"

        except ImportError:
            error_msg = "django-celery-beat is not installed. Run: pip install django-celery-beat"
            self._log_error(error_msg)
            return False, error_msg

        except Exception as e:
            error_msg = f"Failed to setup batch schedule: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def remove_batch_schedule(self) -> Tuple[bool, str]:
        """
        Remove Celery Beat periodic task for batch processing.

        Returns:
            (success, message)
        """
        if not self.config.batch_celery_task_name:
            return True, "No batch schedule to remove"

        try:
            from django_celery_beat.models import PeriodicTask

            # Delete periodic task
            deleted_count, _ = PeriodicTask.objects.filter(
                name=self.config.batch_celery_task_name
            ).delete()

            # Clear all batch schedule fields
            self.config.batch_celery_task_name = None
            self.config.next_batch_run = None
            self.config.last_batch_run = None
            self.config.save()

            if deleted_count > 0:
                self._log_info(f"✓ Batch schedule removed")
                return True, "Batch schedule removed successfully"
            else:
                return True, "No batch schedule found (already removed)"

        except ImportError:
            return True, "django-celery-beat not installed, no schedule to remove"

        except Exception as e:
            error_msg = f"Failed to remove batch schedule: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def update_batch_interval(self, new_interval: str) -> Tuple[bool, str]:
        """
        Update the batch interval and reschedule.

        Args:
            new_interval: New interval choice ('30m', '2h', '6h', '12h', '24h')

        Returns:
            (success, message)
        """
        valid_intervals = ['5m', '15m', '30m', '1h', '2h', '6h', '12h', '24h']
        if new_interval not in valid_intervals:
            return False, f"Invalid interval. Must be one of: {', '.join(valid_intervals)}"

        self.config.batch_interval = new_interval
        self.config.save()

        # Reschedule with new interval
        return self.setup_batch_schedule()

    def _wait_for_snapshot_completion(self, max_wait_minutes: int = 60) -> Tuple[bool, str]:
        """
        Wait for initial snapshot to complete before transitioning to streaming.

        Uses Jolokia JMX metrics for real progress tracking when available,
        with a fallback to the Kafka Connect REST API.

        Args:
            max_wait_minutes: Maximum time to wait for snapshot (default 60 min)

        Returns:
            (success, message)
        """
        import time

        self._log_info(f"Waiting for initial snapshot to complete (max {max_wait_minutes} min)...")

        max_wait_seconds = max_wait_minutes * 60
        poll_interval = 10  # Check every 10 seconds
        elapsed = 0
        jolokia_available = True  # Assume available; disable on first failure
        db_type = self.config.client_database.db_type
        topic_prefix = self.config.kafka_topic_prefix

        while elapsed < max_wait_seconds:
            # ---------------------------------------------------
            # 1. Check connector health via REST API
            # ---------------------------------------------------
            try:
                exists, status_data = self.connector_manager.get_connector_status(
                    self.config.connector_name
                )

                if not exists:
                    return False, "Connector not found"

                if status_data:
                    connector_state = status_data.get('connector', {}).get('state', '')

                    if connector_state == 'FAILED':
                        return False, "Connector failed during snapshot"

                    tasks = status_data.get('tasks', [])
                    if tasks:
                        task_state = tasks[0].get('state', '')
                        if task_state == 'FAILED':
                            trace = tasks[0].get('trace', 'Unknown error')
                            return False, f"Connector task failed: {trace[:200]}"

            except Exception as e:
                self._log_warning(f"  Error checking connector status: {e}")

            # ---------------------------------------------------
            # 2. Check snapshot progress via Jolokia
            # ---------------------------------------------------
            if jolokia_available:
                progress = self.jolokia.get_snapshot_progress(db_type, topic_prefix)

                if progress is None and elapsed == 0:
                    # First poll – MBean may not be registered yet; that's normal.
                    self._log_info("  Waiting for snapshot MBean to register...")

                elif progress is None and elapsed > 0:
                    # Jolokia was working but now returned None – disable.
                    self._log_warning(
                        "Jolokia unavailable, falling back to REST API status checks"
                    )
                    jolokia_available = False

                elif progress is not None:
                    if progress['aborted']:
                        return False, "Snapshot was aborted"

                    if progress['completed'] and not progress['running']:
                        total = progress['total_tables']
                        rows = progress['total_rows_scanned']
                        dur = progress['duration_seconds']
                        msg = (
                            f"Snapshot complete: {total} tables, "
                            f"{rows:,} rows in {dur}s"
                        )
                        self._log_info(f"  {msg}")
                        return True, msg

                    if progress['running']:
                        done = progress['completed_tables']
                        total = progress['total_tables']
                        rows = progress['total_rows_scanned']
                        cur = progress['current_table'] or '...'
                        self._log_info(
                            f"  Snapshot in progress: {done}/{total} tables, "
                            f"{rows:,} rows scanned, "
                            f"current: {cur} ({int(elapsed)}s elapsed)"
                        )

                    # Keep polling – snapshot still running.
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                    continue

            # ---------------------------------------------------
            # 3. Fallback: REST-only heuristic (Jolokia unavailable)
            # ---------------------------------------------------
            # Streaming MBean appearing means the snapshot finished and
            # Debezium transitioned to CDC streaming.
            if not jolokia_available:
                streaming = self.jolokia.get_streaming_metrics(db_type, topic_prefix)
                if streaming is not None:
                    self._log_info("  Streaming MBean detected – snapshot complete")
                    return True, "Snapshot complete (streaming MBean detected)"

                self._log_info(f"  Snapshot in progress... ({int(elapsed)}s elapsed)")

            time.sleep(poll_interval)
            elapsed += poll_interval

        return False, f"Timeout waiting for snapshot after {max_wait_minutes} minutes"

    def start_batch_replication(self) -> Tuple[bool, str]:
        """
        Start replication in batch mode.

        Flow:
        1. Create connector (starts with initial snapshot per config)
        2. Wait for initial snapshot to complete
        3. Pause connector for scheduled batch syncs
        4. Setup batch schedule via Celery Beat

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STARTING BATCH REPLICATION")
        self._log_info(f"Interval: {self.config.batch_interval}")
        self._log_info("=" * 60)

        try:
            # Step 1: Create connectors (source + sink)
            # Skip topic conflict check for batch mode - multiple batch replications 
            # can use same source database without conflicts
            self._log_info("Step 1/4: Creating connectors...")
            success, message = self.start_replication(skip_topic_conflict_check=True)

            if not success:
                return False, message

            # Step 2: Wait for initial snapshot to complete
            self._log_info("Step 2/4: Waiting for initial snapshot...")

            # Determine wait time based on snapshot mode
            if self.config.snapshot_mode == 'never':
                self._log_info("  Snapshot mode is 'never', skipping wait")
            else:
                success, message = self._wait_for_snapshot_completion(max_wait_minutes=60)
                if not success:
                    self._log_warning(f"Snapshot wait issue: {message}")
                    # Continue anyway - connector might still be usable

            # Step 2.5: Add foreign keys to target tables
            self._log_info("Step 2.5/4: Adding foreign keys to target tables...")
            from client.utils.table_creator import add_foreign_keys_to_target
            try:
                created, skipped, errors = add_foreign_keys_to_target(self.config)
                self._log_info(f"✓ Foreign keys: {created} created, {skipped} skipped")
                if errors:
                    self._log_warning(f"⚠️ FK errors: {errors}")
            except Exception as e:
                self._log_warning(f"⚠️ Could not add foreign keys: {e}")

            # Step 3: Pause source connector (batch mode waits between syncs)
            self._log_info("Step 3/4: Pausing connector for batch scheduling...")
            success, message = self.pause_connector()
            if not success:
                self._log_warning(f"Could not pause connector: {message}")
                # Retry once after a short delay
                import time
                time.sleep(3)
                success, message = self.pause_connector()
                if not success:
                    self._log_warning(f"Retry pause failed: {message}")

            # Update status for batch mode
            self.config.status = 'active'
            self.config.is_active = True
            self.config.connector_state = 'PAUSED'
            self.config.save()

            # Step 4: Setup batch schedule via Celery Beat
            self._log_info("Step 4/4: Setting up batch schedule...")
            success, message = self.setup_batch_schedule()
            if not success:
                self._log_warning(f"Could not setup batch schedule: {message}")
                return False, message

            self._log_info("=" * 60)
            self._log_info("✓ BATCH REPLICATION STARTED")
            self._log_info(f"Connector: {self.config.connector_name} (PAUSED)")
            self._log_info(f"Initial snapshot: Complete")
            self._log_info(f"Next sync: {self.config.next_batch_run}")
            self._log_info("=" * 60)

            return True, "Batch replication started successfully"

        except Exception as e:
            error_msg = f"Failed to start batch replication: {str(e)}"
            self._log_error(error_msg)
            self._update_status('error', error_msg)
            return False, error_msg