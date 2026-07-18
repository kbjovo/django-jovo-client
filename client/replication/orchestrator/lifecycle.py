"""LifecycleMixin

Start / stop / restart / delete of a replication.

Split from the original monolithic ReplicationOrchestrator.
"""

import logging
from typing import Dict, Any, Tuple, Optional, Set
from django.utils import timezone
from django.conf import settings

from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.jolokia_client import JolokiaClient
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager, format_topic_name
from jovoclient.utils.debezium.schema_registry_utils import delete_schema_subject, set_compatibility_mode
from jovoclient.utils.debezium.connector_templates import get_connector_config_for_database
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from client.utils.table_creator import drop_tables_for_mappings, truncate_tables_for_mappings
from ..validators import ReplicationValidator
from sqlalchemy import text

logger = logging.getLogger("client.replication.orchestrator")


class LifecycleMixin:
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

            # Stop any running DDL consumer immediately so it is restarted with the
            # new version after the connector is created.  The consumer holds the old
            # topic name (client_..._v_{prev}) in memory; releasing the lock here
            # causes the watchdog (ensure_continuous_ddl_consumers) to spawn a fresh
            # consumer that picks up the updated connector_version from the DB.
            if db_config.db_type.lower() in ('postgresql', 'postgres'):
                try:
                    from client.tasks import _release_lock, _clear_consumer_pending
                    _release_lock(self.config.id)
                    _clear_consumer_pending(self.config.id)
                    self._log_info("✓ Released old DDL consumer lock (will restart with new version)")
                except Exception as e:
                    self._log_warning(f"⚠️ Could not release DDL consumer lock: {e}")

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
            # For PostgreSQL: set up DDL capture infrastructure only when user enabled it
            if db_config.db_type.lower() in ('postgresql', 'postgres') and self.config.enable_ddl_sync:
                try:
                    from client.utils.ddl.postgres_setup import check_postgresql_privileges, setup_postgresql_ddl_capture
                    privs = check_postgresql_privileges(db_config)
                    if privs.get('error'):
                        self._log_warning(f"⚠️ DDL privilege check failed: {privs['error']}")
                    elif privs.get('ddl_triggers_found', 0) >= 2 or privs.get('any_ddl_triggers', 0) >= 2:
                        self._log_info("✓ DDL capture triggers already present — skipping setup")
                    elif privs.get('can_setup_ddl'):
                        ok, msg = setup_postgresql_ddl_capture(db_config)
                        if ok:
                            self._log_info("✓ PostgreSQL DDL capture infrastructure created")
                        else:
                            self._log_warning(f"⚠️ DDL capture setup failed: {msg}")
                    else:
                        self._log_warning("⚠️ DDL sync enabled but user lacks CREATE EVENT TRIGGER — skipping")
                except Exception as e:
                    self._log_warning(f"⚠️ DDL capture setup error: {e}")
            elif db_config.db_type.lower() in ('postgresql', 'postgres'):
                self._log_info("ℹ️ DDL sync disabled for this connector")

            # For Oracle: ensure signal table exists BEFORE connector starts
            if db_config.db_type.lower() == 'oracle':
                try:
                    from client.utils.ddl.oracle_setup import create_signal_table
                    created, msg = create_signal_table(db_config)
                    if created:
                        self._log_info(f"✓ Oracle signal table created: {msg}")
                    elif "already exists" in msg:
                        self._log_info("✓ Oracle signal table already exists")
                    else:
                        self._log_warning(f"⚠️ Could not create Oracle signal table: {msg}")
                except Exception as e:
                    self._log_warning(f"⚠️ Oracle signal table setup error: {e}")

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
            # Ensure DDL Supervisor is running
            # ========================================
            # The supervisor manages DDL consumers for ALL configs in one Celery task
            # via threads. Calling ensure_continuous_ddl_consumers starts the supervisor
            # if not already running; it will pick up this new config within its next
            # config-refresh cycle (≤30 s). This avoids the per-config task storm that
            # resulted from calling start_continuous_ddl_consumer directly.
            try:
                from client.tasks import ensure_continuous_ddl_consumers
                ensure_continuous_ddl_consumers.delay()
                self._log_info("✓ DDL supervisor signalled (picks up new config within 30 s)")
            except Exception as e:
                self._log_warning(f"⚠️ Could not signal DDL supervisor: {e}")

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
                        # Drop the PostgreSQL replication slot and publication now that
                        # the connector is gone and the slot is no longer active.
                        self._drop_postgresql_slot()
                    else:
                        self._log_warning(f"⚠️ Source deletion failed: {error}")
                except Exception as e:
                    self._log_warning(f"⚠️ Source deletion error: {e}")

            # Sink connector is dedicated to THIS source connector (one sink per source DB).
            # Delete it unless another active config still uses the same source database
            # (which would share the same per-source sink name).
            if sink_connector_name:
                from client.models import ReplicationConfig

                siblings = ReplicationConfig.objects.filter(
                    client_database=self.config.client_database,
                    status__in=['configured', 'active', 'paused', 'error'],
                ).exclude(pk=config_id)

                if siblings.exists():
                    self._log_info("ℹ️ Another config still uses this source DB — keeping its sink connector")
                else:
                    self._log_info("ℹ️ Deleting dedicated sink connector")
                    self._delete_sink_connector(sink_connector_name)

            # Optionally delete Kafka topics and their schema registry subjects
            if delete_topics:
                self._log_info("Deleting Kafka topics...")
                success, message = self.delete_topics()
                if success:
                    self._log_info(f"✓ {message}")
                else:
                    self._log_warning(f"⚠️ {message}")

                self._log_info("Deleting Schema Registry subjects...")
                topic_prefix = self.config.kafka_topic_prefix
                db_config = self.config.client_database
                for tm in self.config.table_mappings.all():
                    topic = format_topic_name(
                        db_config=db_config,
                        table_name=tm.source_table,
                        topic_prefix=topic_prefix,
                        schema=tm.source_schema,
                    )
                    try:
                        delete_schema_subject(f"{topic}-key")
                        delete_schema_subject(f"{topic}-value")
                        self._log_info(f"  ✓ Deleted schema subjects for: {topic}")
                    except Exception as e:
                        self._log_warning(f"  ⚠️ Error deleting schema subjects for {topic}: {e}")

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
