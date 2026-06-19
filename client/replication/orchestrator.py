import logging
from typing import Dict, Any, Tuple, Optional, Set
from django.utils import timezone
from django.conf import settings

from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.jolokia_client import JolokiaClient
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager, format_topic_name
from jovoclient.utils.debezium.schema_registry_utils import delete_schema_subject
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

    # ==========================================
    # PostgreSQL Slot / Publication Cleanup
    # ==========================================

    def _drop_postgresql_slot(self) -> None:
        """
        Drop the PostgreSQL replication slot and publication for this connector version.

        Called after the Kafka Connect source connector has been deleted so the slot
        is no longer active.  Silently skips if the source DB is not PostgreSQL or if
        the slot/publication don't exist.
        """
        db_config = self.config.client_database
        if db_config.db_type.lower() != 'postgresql':
            return

        import re
        match = re.search(r'_v_(\d+)$', self.config.kafka_topic_prefix or '')
        version = int(match.group(1)) if match else 0

        client_id = db_config.client_id
        db_id = db_config.id

        slot_name = f"debezium_{client_id}_{db_id}_v_{version}"
        slot_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in slot_name)[:63]

        pub_name = f"debezium_pub_{client_id}_{db_id}_v_{version}"
        pub_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in pub_name)[:63]

        try:
            from sqlalchemy import create_engine
            engine = create_engine(db_config.get_connection_url(), pool_pre_ping=True)
            with engine.connect() as conn:
                # If the Kafka Connect worker hasn't fully released the slot yet,
                # terminate the WAL sender backend holding it so the drop succeeds immediately.
                conn.execute(
                    text(
                        "SELECT pg_terminate_backend(active_pid) "
                        "FROM pg_replication_slots "
                        "WHERE slot_name = :slot AND active_pid IS NOT NULL"
                    ),
                    {"slot": slot_name},
                )

                result = conn.execute(
                    text(
                        "SELECT pg_drop_replication_slot(slot_name) "
                        "FROM pg_replication_slots "
                        "WHERE slot_name = :slot"
                    ),
                    {"slot": slot_name},
                )
                if result.rowcount:
                    self._log_info(f"✓ Dropped replication slot: {slot_name}")
                else:
                    self._log_info(f"ℹ️ Replication slot not found (already gone): {slot_name}")

                conn.execute(text(f'DROP PUBLICATION IF EXISTS "{pub_name}"'))
                conn.commit()
                self._log_info(f"✓ Dropped publication: {pub_name}")

        except Exception as e:
            self._log_warning(f"⚠️ Could not drop PostgreSQL slot/publication ({slot_name}): {e}")

    def _ensure_postgres_publication_tables(self, db_config, new_tables: list) -> None:
        """
        Add newly requested tables to the existing PostgreSQL publication.

        publication.autocreate.mode = "filtered" creates the publication on first
        start but never alters it afterward.  When tables are added via add_tables()
        we must run ALTER PUBLICATION ... ADD TABLE so Debezium's WAL stream covers
        the new tables before the connector is restarted.

        Silently warns (does not raise) if the publication doesn't exist yet — in
        that case Debezium will create it with the correct table list on restart.
        """
        import re
        match = re.search(r'_v_(\d+)$', self.config.kafka_topic_prefix or '')
        version = int(match.group(1)) if match else 0

        pub_name = f"debezium_pub_{db_config.client_id}_{db_config.id}_v_{version}"
        pub_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in pub_name)[:63]

        # Resolve schema — use source_schema from the first mapping that has one,
        # fall back to 'public'.
        schema_name = 'public'
        for t in new_tables:
            mapping = self.config.table_mappings.filter(source_table=t, is_enabled=True).first()
            if mapping and mapping.source_schema:
                schema_name = mapping.source_schema
                break

        try:
            from client.utils.database_utils import get_database_engine
            engine = get_database_engine(db_config)
            with engine.connect() as conn:
                # Check the publication exists before trying to alter it
                exists = conn.execute(
                    text("SELECT 1 FROM pg_publication WHERE pubname = :pub"),
                    {'pub': pub_name}
                ).fetchone()

            if not exists:
                self._log_info(
                    f"  ℹ️ Publication '{pub_name}' not found yet — "
                    f"Debezium will create it with all tables on next start"
                )
                engine.dispose()
                return

            # Build the table list: schema.table for each new table
            table_list = ', '.join(
                f'"{schema_name}"."{t}"' for t in new_tables
            )
            alter_sql = f'ALTER PUBLICATION "{pub_name}" ADD TABLE {table_list}'
            with engine.begin() as conn:
                conn.execute(text(alter_sql))

            self._log_info(
                f"  ✓ Added {len(new_tables)} table(s) to publication '{pub_name}': "
                f"{', '.join(new_tables)}"
            )
            engine.dispose()

        except Exception as e:
            self._log_warning(
                f"⚠️ Could not update PostgreSQL publication '{pub_name}': {e}. "
                f"Debezium may not replicate the new tables until the publication is updated manually."
            )

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

            # Generate sink connector name using consistent naming convention.
            # IMPORTANT: Must match ClientDatabase.get_sink_connector_name() for consistency.
            # Format: client_{client_id}_db_{src_db_id}_sink (one sink per source connector).
            sink_connector_name = self.config.client_database.get_sink_connector_name()

            # Topics for THIS source connector only (one sink per source connector).
            current_topics = self._get_kafka_topics_for_config()

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
            kafka_bootstrap = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']

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
            kafka_bootstrap = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']

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
            enabled_mappings_qs = self.config.table_mappings.filter(is_enabled=True)
            if db_config.db_type == 'oracle':
                enabled_tables = [
                    f"{tm.source_schema}.{tm.source_table}" if tm.source_schema else tm.source_table
                    for tm in enabled_mappings_qs
                ]
            else:
                enabled_tables = list(enabled_mappings_qs.values_list('source_table', flat=True))

            if not enabled_tables:
                return False, "No tables enabled for replication"

            max_tables = settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25)
            if len(enabled_tables) > max_tables:
                return False, f"Too many tables ({len(enabled_tables)}). Maximum {max_tables} tables per connector to prevent worker crashes."

            kafka_bootstrap = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
            schema_registry = settings.DEBEZIUM_CONFIG.get(
                'SCHEMA_REGISTRY_INTERNAL_URL',
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

            # Use the user's custom name if already set; otherwise generate the default.
            # IMPORTANT: do NOT overwrite a custom name that was saved during connector_add.
            from jovoclient.utils.debezium.connector_templates import generate_connector_name
            if not self.config.connector_name:
                self.config.connector_name = generate_connector_name(
                    client,
                    db_config,
                    version=self.config.connector_version
                )
                self.config.save()

            versioned_connector_name = self.config.connector_name

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
    # Table Management (Add/Remove/Resync)
    # ==========================================

    def resync_table(self, table_name: str) -> Tuple[bool, str]:
        """
        Manually resync a single table after a source TRUNCATE + repopulate.

        Use this for PostgreSQL and SQL Server sources where TRUNCATE events are
        not automatically detected by the DDL processor (MySQL and Oracle handle
        this automatically via the schema history topic).

        Steps:
          1. Truncate target table (clears stale rows, keeps schema).
          2. Send incremental snapshot signal so Debezium re-reads the source.

        Args:
            table_name: Source table name (unqualified, e.g. 'orders')

        Returns:
            Tuple[bool, str]: (success, message)
        """
        from client.utils.table_creator import truncate_tables_for_mappings
        from jovoclient.utils.kafka.signal import send_incremental_snapshot_signal

        table_mapping = self.config.table_mappings.filter(
            source_table=table_name,
            is_enabled=True
        ).first()
        if not table_mapping:
            return False, f"No enabled mapping found for table '{table_name}'"

        target_db = self.config.client_database.client.client_databases.filter(
            is_target=True
        ).first()
        if not target_db:
            return False, "No target database configured"

        # Step 1: truncate target
        self._log_info(f"Resync '{table_name}': truncating target table '{table_mapping.target_table}'")
        ok, msg = truncate_tables_for_mappings(target_db, [table_mapping])
        if not ok:
            return False, f"Failed to truncate target table: {msg}"

        # Step 2: incremental snapshot signal
        self._log_info(f"Resync '{table_name}': sending incremental snapshot signal")
        try:
            signal_id, method = send_incremental_snapshot_signal(
                self.config.client_database,
                self.config,
                [table_name]
            )
        except Exception as e:
            return False, f"Target truncated but snapshot signal failed: {e}"

        return True, f"Resync triggered for '{table_name}' (signal {signal_id} via {method})"

    def remove_tables(self, table_names: list, target_action: str = 'truncate', allow_empty: bool = False) -> Tuple[bool, str, list]:
        """
        Remove tables from a connector with full cleanup.

        This will:
        1. Delete Kafka topics for the removed tables
        2. Truncate or drop target tables in target database
        3. Disable table mappings in database
        4. Update source connector config  (skipped when allow_empty=True and no tables remain)
        5. Restart source connector        (skipped when allow_empty=True and no tables remain)
        6. Restart sink connector

        Args:
            table_names: List of source table names to remove
            target_action: 'truncate' (keep structure, clear data) or 'drop' (remove table entirely)

        Returns:
            (success, message, steps) where steps is a list of
            {'id': str, 'status': 'ok'|'warn'|'error', 'msg': str} dicts
        """
        self._log_info("=" * 60)
        self._log_info("REMOVING TABLES FROM CONNECTOR")
        self._log_info(f"Tables to remove: {', '.join(table_names)}")
        self._log_info("=" * 60)

        steps = []

        try:
            db_config = self.config.client_database
            client = db_config.client

            # Get table mappings for the tables being removed
            table_mappings_to_remove = self.config.table_mappings.filter(
                source_table__in=table_names,
                is_enabled=True
            )

            if not table_mappings_to_remove.exists():
                return False, "No matching enabled tables found to remove", steps

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
                    topic_ok, topic_err = self.topic_manager.delete_topic(topic)
                    if topic_ok:
                        deleted_topics.append(topic)
                        self._log_info(f"  ✓ Deleted topic: {topic}")
                    else:
                        self._log_warning(f"  ⚠️ Failed to delete topic {topic}: {topic_err}")
                except Exception as e:
                    self._log_warning(f"  ⚠️ Error deleting topic {topic}: {e}")

                # Delete schema registry subjects regardless of topic deletion outcome
                try:
                    delete_schema_subject(f"{topic}-key")
                    delete_schema_subject(f"{topic}-value")
                    self._log_info(f"  ✓ Deleted schema subjects for: {topic}")
                except Exception as e:
                    self._log_warning(f"  ⚠️ Error deleting schema subjects for {topic}: {e}")

            self._log_info(f"✓ Deleted {len(deleted_topics)}/{len(topics_to_delete)} topics")
            steps.append({
                'id': 'topics',
                'status': 'ok' if len(deleted_topics) == len(topics_to_delete) else 'warn',
                'msg': f"{len(deleted_topics)}/{len(topics_to_delete)} Kafka topic(s) deleted",
            })

            # ========================================
            # STEP 2: Truncate or drop target tables
            # ========================================
            action_word = 'dropped' if target_action == 'drop' else 'truncated'
            self._log_info(f"STEP 2/5: {action_word.capitalize()} target database tables...")

            from client.models.database import ClientDatabase
            target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

            if target_db:
                if target_action == 'drop':
                    from client.utils.table_creator import drop_tables_for_mappings
                    tbl_ok, tbl_msg = drop_tables_for_mappings(target_db, table_mappings_to_remove)
                else:
                    tbl_ok, tbl_msg = truncate_tables_for_mappings(target_db, table_mappings_to_remove)

                if tbl_ok:
                    self._log_info(f"✓ {tbl_msg}")
                else:
                    self._log_warning(f"⚠️ {tbl_msg}")
                steps.append({
                    'id': 'target_tbl',
                    'status': 'ok' if tbl_ok else 'warn',
                    'msg': f"Target table(s) {action_word}" + (f" — {tbl_msg}" if not tbl_ok else ""),
                })
            else:
                self._log_warning("⚠️ No target database configured - skipping table truncate")
                steps.append({
                    'id': 'target_tbl',
                    'status': 'warn',
                    'msg': "No target database configured — skipped",
                })

            # ========================================
            # STEP 3: Disable table mappings in database
            # ========================================
            self._log_info("STEP 3/5: Disabling table mappings...")

            disabled_count = table_mappings_to_remove.update(is_enabled=False)
            self._log_info(f"✓ Disabled {disabled_count} table mappings")
            steps.append({
                'id': 'mappings',
                'status': 'ok',
                'msg': f"{disabled_count} table mapping(s) disabled",
            })

            # Check if any tables remain
            remaining_mappings = self.config.table_mappings.filter(is_enabled=True)
            if db_config.db_type == 'oracle':
                remaining_tables = [
                    f"{tm.source_schema}.{tm.source_table}" if tm.source_schema else tm.source_table
                    for tm in remaining_mappings
                ]
            else:
                remaining_tables = list(remaining_mappings.values_list('source_table', flat=True))

            if not remaining_tables:
                if not allow_empty:
                    return False, "Cannot remove all tables. Delete the connector instead.", steps
                # Caller intends to add tables immediately after (e.g. simultaneous add+remove).
                # Source connector update is deferred to add_tables (avoids empty table.include.list).
                # BUT we must still cycle the sink NOW — stale consumer group offsets for the
                # deleted topic-partitions cause an endless rebalance loop if not cleared here.
                self._log_info("No remaining tables — cycling sink to clear stale offsets, deferring source update to add_tables...")
                if self.config.sink_connector_name:
                    stop_ok, stop_err = self.connector_manager.stop_connector(self.config.sink_connector_name)
                    if stop_ok:
                        self._log_info("✓ Sink connector stopped")
                    else:
                        self._log_warning(f"⚠️ Sink stop failed: {stop_err}")

                    offsets_ok, offsets_err = self.connector_manager.delete_connector_offsets(self.config.sink_connector_name)
                    if offsets_ok:
                        self._log_info("✓ Sink consumer group offsets cleared")
                    else:
                        self._log_warning(f"⚠️ Could not clear sink offsets: {offsets_err}")

                    resume_ok, resume_err = self.connector_manager.resume_connector(self.config.sink_connector_name)
                    if resume_ok:
                        self._log_info("✓ Sink connector resumed")
                    else:
                        self._log_warning(f"⚠️ Sink resume failed: {resume_err}")

                    steps.append({
                        'id': 'sink_restart',
                        'status': 'ok' if (stop_ok and resume_ok) else 'warn',
                        'msg': 'Sink cycled (stop → clear offsets → resume)' if (stop_ok and resume_ok) else f"Sink cycle warning: {stop_err or resume_err}",
                    })

                action_past = 'dropped' if target_action == 'drop' else 'truncated'
                return True, f"Removed {len(table_names)} table(s) (topics deleted, target tables {action_past}) — source connector update deferred", steps

            # ========================================
            # STEP 4: Update and restart source connector
            # ========================================
            self._log_info("STEP 4/6: Updating source connector configuration...")

            # Ensure the connector is running before reconfiguring (resume if a user
            # manually paused it).
            self._resume_if_paused()

            source_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=remaining_tables
            )

            cfg_ok, cfg_err = self.connector_manager.update_connector_config(
                self.config.connector_name,
                source_config
            )
            if not cfg_ok:
                steps.append({
                    'id': 'src_config',
                    'status': 'error',
                    'msg': f"Failed to update source connector: {cfg_err}",
                })
                return False, f"Failed to update source connector: {cfg_err}", steps

            self._log_info(f"✓ Source connector updated with {len(remaining_tables)} tables")
            steps.append({
                'id': 'src_config',
                'status': 'ok',
                'msg': f"Source connector updated ({len(remaining_tables)} table(s) remaining)",
            })

            # Restart source connector
            src_restart_ok, src_restart_err = self.connector_manager.restart_connector(self.config.connector_name)
            if src_restart_ok:
                self._log_info(f"✓ Source connector restarted")
            else:
                self._log_warning(f"⚠️ Source connector restart failed: {src_restart_err}")
            steps.append({
                'id': 'src_restart',
                'status': 'ok' if src_restart_ok else 'warn',
                'msg': 'Source connector restarted' if src_restart_ok else f"Source connector restart warning: {src_restart_err}",
            })

            # ========================================
            # STEP 5: Cycle sink connector (stop → resume)
            # ========================================
            # A plain restart leaves the consumer as a group member, so it can
            # still try to commit in-flight offsets for the now-deleted topic
            # partitions. stop() evicts the consumer from the group entirely;
            # on resume Kafka re-assigns only partitions for topics that exist.
            self._log_info("STEP 5/6: Cycling sink connector (stop → resume)...")

            if self.config.sink_connector_name:
                # Stop (not pause) so the consumer leaves the group entirely
                stop_ok, stop_err = self.connector_manager.stop_connector(
                    self.config.sink_connector_name
                )
                if stop_ok:
                    self._log_info(f"✓ Sink connector stopped")
                else:
                    self._log_warning(f"⚠️ Sink connector stop failed: {stop_err}")

                # Purge stale consumer group offsets for the deleted topic-partitions.
                # Without this, the consumer re-inherits the deleted partition's offset
                # record on every rejoin and enters an endless rebalance loop.
                offsets_ok, offsets_err = self.connector_manager.delete_connector_offsets(
                    self.config.sink_connector_name
                )
                if offsets_ok:
                    self._log_info(f"✓ Sink consumer group offsets cleared")
                else:
                    self._log_warning(f"⚠️ Could not clear sink offsets: {offsets_err}")

                resume_ok, resume_err = self.connector_manager.resume_connector(
                    self.config.sink_connector_name
                )
                sink_ok = stop_ok and resume_ok
                if resume_ok:
                    self._log_info(f"✓ Sink connector resumed")
                else:
                    self._log_warning(f"⚠️ Sink connector resume failed: {resume_err}")

                steps.append({
                    'id': 'sink_restart',
                    'status': 'ok' if sink_ok else 'warn',
                    'msg': 'Sink connector cycled (stop → clear offsets → resume)' if sink_ok else f"Sink connector cycle warning: {stop_err or resume_err}",
                })

            self._log_info("=" * 60)
            self._log_info("✓ TABLES REMOVED SUCCESSFULLY")
            self._log_info("=" * 60)

            action_past = 'dropped' if target_action == 'drop' else 'truncated'
            return True, f"Removed {len(table_names)} table(s) (topics deleted, target tables {action_past})", steps

        except Exception as e:
            error_msg = f"Failed to remove tables: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg, steps

    def add_tables(self, table_names: list, target_table_names: dict = None) -> Tuple[bool, str, list]:
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

        steps = []
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
                    row_count = schema.get('row_count', -1)

                    for col in columns:
                        col['primary_key'] = col['name'] in primary_keys

                    # Parse schema and table name
                    if (db_config.db_type in ['mssql', 'oracle']) and '.' in table_name:
                        source_schema, actual_table_name = table_name.split('.', 1)
                    elif db_config.db_type == 'postgresql':
                        source_schema = 'public'
                        actual_table_name = table_name
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
                        target_table_name = f"{db_config.database_name}_{actual_table_name}"
                    elif db_config.db_type == 'mssql':
                        target_table_name = f"{db_config.database_name}_{actual_table_name}"
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
                return False, "No tables could be added", steps

            self._log_info(f"✓ Created mappings for {len(added_tables)} tables")
            steps.append({
                'id': 'mappings',
                'status': 'ok' if not failed_tables else 'warn',
                'msg': (
                    f"{len(added_tables)} table mapping(s) created"
                    + (f" ({len(failed_tables)} failed: {', '.join(failed_tables)})" if failed_tables else "")
                ),
            })

            # ========================================
            # STEP 2: Create Kafka topics
            # ========================================
            self._log_info("STEP 2/6: Creating Kafka topics...")

            topics_ok, topics_msg = self.create_topics()
            if topics_ok:
                self._log_info(f"✓ {topics_msg}")
            else:
                self._log_warning(f"⚠️ Topic creation warning: {topics_msg}")
            steps.append({
                'id': 'topics',
                'status': 'ok' if topics_ok else 'warn',
                'msg': topics_msg or "Kafka topics created",
            })

            # ========================================
            # STEP 3: Update source connector config
            # ========================================
            self._log_info("STEP 3/6: Updating source connector...")

            # Ensure the connector is running before reconfiguring (resume if a user
            # manually paused it).
            self._resume_if_paused()

            enabled_mappings = self.config.table_mappings.filter(is_enabled=True)
            if db_config.db_type == 'oracle':
                all_tables = [
                    f"{tm.source_schema}.{tm.source_table}" if tm.source_schema else tm.source_table
                    for tm in enabled_mappings
                ]
            else:
                all_tables = list(enabled_mappings.values_list('source_table', flat=True))

            # Guard: never send an empty table list to the connector template.
            # An empty tables_whitelist causes the template to omit table.include.list
            # entirely, making Debezium capture every table in the database.
            if not all_tables:
                steps.append({
                    'id': 'src_config',
                    'status': 'error',
                    'msg': "No enabled table mappings found — cannot update connector with empty table list",
                })
                return False, "No enabled table mappings found — cannot update connector with empty table list", steps

            source_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=all_tables
            )

            add_cfg_ok, add_cfg_err = self.connector_manager.update_connector_config(
                self.config.connector_name,
                source_config
            )
            if not add_cfg_ok:
                steps.append({
                    'id': 'src_config',
                    'status': 'error',
                    'msg': f"Failed to update source connector: {add_cfg_err}",
                })
                return False, f"Failed to update source connector: {add_cfg_err}", steps

            self._log_info(f"✓ Source connector config updated with {len(all_tables)} tables")

            # ── PostgreSQL only: update publication ──────────────────────────
            # publication.autocreate.mode = "filtered" creates the publication on
            # first run but never modifies it afterward.  New tables must be added
            # explicitly with ALTER PUBLICATION ... ADD TABLE before the connector
            # restarts, otherwise Debezium never replicates them.
            if db_config.db_type == 'postgresql':
                self._ensure_postgres_publication_tables(db_config, added_tables)

            # Restart the connector so it reloads its table.include.list and
            # replays the schema history from Kafka for the new table(s).
            # We must NOT send the snapshot signal until schema replay is done —
            # doing so causes a NullPointerException in Debezium because the
            # in-memory schema cache doesn't yet know about the new table.
            self._log_info("  → Restarting connector to reload schema history...")
            self.connector_manager.restart_connector(self.config.connector_name)

            # Wait until the streaming MBean reports Connected=True.
            # That is the authoritative signal that schema history replay is
            # complete and the binlog connection is live — no hardcoded sleeps.
            self._log_info("  → Waiting for connector to finish schema history replay...")
            connector_ready = self._wait_for_connector_streaming(timeout=60)
            if connector_ready:
                self._log_info("✓ Connector streaming — schema ready for snapshot signal")
            else:
                # Check if at least RUNNING before giving up
                exists, status_data = self.connector_manager.get_connector_status(self.config.connector_name)
                state = (status_data or {}).get('connector', {}).get('state', '') if exists else ''
                if state == 'FAILED':
                    steps.append({
                        'id': 'src_config',
                        'status': 'error',
                        'msg': "Connector failed after config update",
                    })
                    return False, "Connector failed after config update", steps
                self._log_warning("⚠️ Streaming MBean not yet Connected — proceeding anyway")

            steps.append({
                'id': 'src_config',
                'status': 'ok' if connector_ready else 'warn',
                'msg': (
                    f"Source connector updated ({len(all_tables)} table(s)), schema ready"
                    if connector_ready else
                    f"Source connector updated ({len(all_tables)} table(s)) — schema readiness timed out"
                ),
            })

            # ========================================
            # STEP 4: Update sink connector BEFORE snapshot signal
            # ========================================
            # CRITICAL ORDER: sink config must be updated (with custom table rename
            # transforms) BEFORE the snapshot signal is sent.  The sink's topics.regex
            # already subscribes to the new topic, so snapshot records start arriving
            # the moment the signal fires.  If the sink still has the old config at that
            # point, extractTableName routes every record to the wrong table (e.g.
            # kbbio_busy_acc_greenera instead of kbbio_busy_acc_greenera_tst) and the
            # custom target table is never created.
            self._log_info("STEP 4/6: Updating sink connector (before snapshot signal)...")

            if self.config.sink_connector_name:
                target_db = client.get_target_database()
                if target_db:
                    sink_ok, sink_err = self._update_sink_connector(
                        self.config.sink_connector_name,
                        set(),  # topics unused — sink uses topics.regex
                        target_db
                    )
                    if sink_ok:
                        self._log_info("✓ Sink connector config updated with new table transforms")
                        steps.append({
                            'id': 'sink',
                            'status': 'ok',
                            'msg': "Sink connector updated with new table transforms",
                        })
                    else:
                        self._log_warning(f"⚠️ Sink connector update failed: {sink_err}")
                        steps.append({
                            'id': 'sink',
                            'status': 'warn',
                            'msg': f"Sink connector update failed: {sink_err}",
                        })
                else:
                    self.connector_manager.restart_connector(self.config.sink_connector_name)
                    self._log_info("✓ Sink connector restarted (uses topics.regex for auto-subscription)")
                    steps.append({
                        'id': 'sink',
                        'status': 'ok',
                        'msg': "Sink connector restarted",
                    })

            # ========================================
            # STEP 5: Send incremental snapshot signal
            # ========================================
            self._log_info("STEP 5/6: Sending incremental snapshot signal...")

            from jovoclient.utils.kafka.signal import send_incremental_snapshot_signal

            signal_id, method = send_incremental_snapshot_signal(
                database=db_config,
                replication_config=self.config,
                tables=added_tables
            )

            self._log_info(f"✓ Signal sent via {method} - ID: {signal_id}")
            steps.append({
                'id': 'signal_sent',
                'status': 'ok',
                'msg': f"Snapshot signal sent via {method} (ID: {signal_id})",
            })

            # Verify incremental snapshot started via Jolokia.
            # grace_seconds is reduced from 10→3 because we already waited for
            # the connector to be streaming before sending the signal.
            recv_result = self._verify_incremental_snapshot_started(signal_id, grace_seconds=3)
            steps.append({
                'id': 'signal_recv',
                'status': 'ok' if recv_result['confirmed'] else 'warn',
                'msg': recv_result['msg'],
            })

            # ========================================
            # STEP 5.5: Add foreign keys + indexes to new target tables
            # ========================================
            self._log_info("STEP 5.5/6: Adding foreign keys and indexes to new target tables...")
            from client.utils.table_creator import add_foreign_keys_to_target, add_indexes_to_target
            try:
                # Wait a moment for sink to create the tables
                import time
                time.sleep(5)
                created, skipped, errors = add_foreign_keys_to_target(self.config, specific_tables=added_tables)
                self._log_info(f"✓ Foreign keys: {created} created, {skipped} skipped")
                if errors:
                    self._log_warning(f"⚠️ FK errors: {errors}")
                idx_created, idx_skipped, idx_errors = add_indexes_to_target(self.config, specific_tables=added_tables)
                self._log_info(f"✓ Indexes: {idx_created} created, {idx_skipped} skipped")
                if idx_errors:
                    self._log_warning(f"⚠️ Index errors: {idx_errors}")
                steps.append({
                    'id': 'fk',
                    'status': 'ok' if not errors and not idx_errors else 'warn',
                    'msg': (
                        f"Foreign keys: {created} created, {skipped} skipped"
                        + (f" ({len(errors)} error(s))" if errors else "")
                        + f" | Indexes: {idx_created} created, {idx_skipped} skipped"
                        + (f" ({len(idx_errors)} error(s))" if idx_errors else "")
                    ),
                })
            except Exception as e:
                self._log_warning(f"⚠️ Could not add foreign keys: {e}")
                steps.append({
                    'id': 'fk',
                    'status': 'warn',
                    'msg': f"Foreign key setup skipped: {e}",
                })

            # The source connector stays RUNNING so the incremental snapshot can proceed.
            self.config.connector_state = 'RUNNING'
            self.config.save()

            self._log_info("=" * 60)
            self._log_info("✓ TABLES ADDED SUCCESSFULLY")
            self._log_info("=" * 60)

            result_msg = f"Added {len(added_tables)} table(s) via {method} signal (ID: {signal_id})"
            if failed_tables:
                result_msg += f" | {len(failed_tables)} failed: {', '.join(failed_tables)}"

            return True, result_msg, steps

        except Exception as e:
            error_msg = f"Failed to add tables: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg, steps

    # ==========================================
    # Jolokia Snapshot Verification
    # ==========================================

    def _wait_for_connector_streaming(self, timeout: int = 60) -> bool:
        """
        Wait until the source connector's streaming MBean reports Connected=True.

        For MySQL this is the reliable indicator that schema history replay from
        the Kafka schema-history topic is complete and the connector is actively
        reading the binlog.  Sending an incremental snapshot signal before this
        point causes a NullPointerException in Debezium because the in-memory
        schema cache doesn't yet contain the newly-added table.

        For other db types (PostgreSQL, MSSQL, Oracle) the streaming MBean also
        becomes available once the connector has fully started, so this check is
        safe to use universally.

        Returns True if Connected within timeout, False if timed out.
        """
        import time
        db_type = self.config.client_database.db_type.lower()
        topic_prefix = self.config.kafka_topic_prefix

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            raw = self.jolokia.get_streaming_metrics(db_type, topic_prefix)
            if raw and raw.get('Connected'):
                return True
            time.sleep(1)
        return False

    def _verify_incremental_snapshot_started(
        self, signal_id: str, grace_seconds: int = 10, polls: int = 3
    ) -> dict:
        """Best-effort check that an incremental snapshot actually started.

        Polls Jolokia a few times after the signal is sent. Logs a
        confirmation or warning but never fails the overall operation —
        the signal may simply be delayed or Jolokia may be unavailable.

        Returns a dict: {'confirmed': bool, 'tables': int|None, 'msg': str}
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
            if progress and (progress.get('running') or progress.get('completed')):
                tables = progress.get('total_tables', '?')
                if progress.get('completed') and not progress.get('running'):
                    msg = f"Signal received — snapshot completed ({tables} table(s))"
                    self._log_info(
                        f"  Incremental snapshot confirmed completed fast "
                        f"(signal {signal_id}): {tables} tables"
                    )
                else:
                    msg = f"Signal received — {tables} table(s) queued for snapshot"
                    self._log_info(
                        f"  Incremental snapshot confirmed running "
                        f"(signal {signal_id}): {tables} tables queued"
                    )
                return {'confirmed': True, 'tables': tables, 'msg': msg}

            if attempt < polls - 1:
                time.sleep(5)

        msg = "Could not confirm snapshot via Jolokia — may still be starting"
        self._log_warning(
            f"  Could not confirm incremental snapshot via Jolokia "
            f"(signal {signal_id}) — may still be starting"
        )
        return {'confirmed': False, 'tables': None, 'msg': msg}

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
                'tables_enabled': self.config.table_mappings.filter(is_enabled=True).count(),
            },
            'timestamp': timezone.now().isoformat(),
        }

    def _get_snapshot_progress(self) -> Optional[Dict[str, Any]]:
        """Get current snapshot progress via Jolokia.

        Returns a normalised progress dict if a snapshot (initial or
        incremental) is active, or None if nothing is running / Jolokia
        is unreachable.
        """
        try:
            db_config = self.config.client_database
            db_type = db_config.db_type
            topic_prefix = self.config.kafka_topic_prefix

            # SQL Server registers JMX MBeans under database.server.name
            # (format: "sqlserver_{client_id}_{db_id}_v_{version}"), which is
            # intentionally different from topic.prefix ("client_{id}_db_{id}_v_{version}").
            # Try the database.server.name format first, fall back to topic.prefix
            # to handle all Debezium versions.
            if db_type == 'mssql' and self.config.connector_version:
                server_name = (
                    f"sqlserver_{db_config.client.id}_{db_config.id}"
                    f"_v_{self.config.connector_version}"
                )
                result = self.jolokia.get_active_snapshot_progress(db_type, server_name)
                if result is not None:
                    return result

            result = self.jolokia.get_active_snapshot_progress(db_type, topic_prefix)
            if result is not None:
                return result

            # Jolokia returned nothing for all MBean contexts.  This happens for
            # SQL Server (and occasionally other db types) when a fast snapshot
            # finishes before the first poll and there is a brief gap before the
            # streaming MBean registers.  As a last resort, check the Kafka
            # Connect REST status: if the connector task is RUNNING and the
            # snapshot_mode requires a snapshot, we can safely infer that the
            # snapshot completed successfully.
            if self.config.snapshot_mode and self.config.snapshot_mode != 'never':
                connector_state = self._get_connector_status()
                if connector_state.get('state') == 'RUNNING' and connector_state.get('healthy'):
                    return {
                        'type': 'initial',
                        'running': False,
                        'completed': True,
                        'aborted': False,
                        'total_tables': 0,
                        'remaining_tables': 0,
                        'completed_tables': 0,
                        'rows_scanned': {},
                        'total_rows_scanned': 0,
                        'duration_seconds': 0,
                        'current_table': None,
                    }

            return None
        except Exception:
            return None

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
        Resume the source connector if a user has manually paused it, so reconfigure
        operations (add/remove tables, snapshots) can proceed.

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

    def resume_connector(self) -> Tuple[bool, str]:
        """
        Resume a paused source connector (manual pause/resume control).

        Connectors stream continuously; this simply lets a user resume one they
        previously paused (also used for add-tables snapshots, sink offset cycling,
        and manual recovery).

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
                self._apply_pending_truncates()
                return True, "Connector resumed successfully"
            else:
                return False, f"Failed to resume connector: {error}"

        except Exception as e:
            error_msg = f"Error resuming connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _apply_pending_truncates(self) -> None:
        """
        Apply any TRUNCATE+resync operations that were deferred while a connector
        was paused.  Called by resume_connector() and resume_sink_connector().

        A deferred TRUNCATE truncates the target table directly and fires a
        re-snapshot that flows through source → Kafka → sink, so it must only run
        once BOTH the source and sink connectors are active.  If either is still
        paused the queue is left intact and drained on the next resume.
        """
        from client.models.replication import ReplicationConfig
        cfg = ReplicationConfig.objects.only(
            'pending_truncates', 'connector_state', 'sink_connector_state'
        ).get(pk=self.config.pk)
        pending = list(cfg.pending_truncates or [])
        if not pending:
            return

        if cfg.connector_state == 'PAUSED' or cfg.sink_connector_state == 'PAUSED':
            self._log_info(
                f"Deferred TRUNCATE(s) kept queued — pipeline still paused "
                f"(source={cfg.connector_state}, sink={cfg.sink_connector_state}): {pending}"
            )
            return

        self._log_info(f"Applying {len(pending)} deferred TRUNCATE(s) after resume: {pending}")

        from client.utils.ddl.base_processor import _apply_truncate_and_snapshot
        applied = []
        for table_name in pending:
            try:
                ok = _apply_truncate_and_snapshot(self.config, table_name)
                if ok:
                    applied.append(table_name)
                else:
                    self._log_warning(f"  ✗ Deferred TRUNCATE failed for '{table_name}' — will retry on next resume")
            except Exception as e:
                self._log_error(f"  ✗ Deferred TRUNCATE error for '{table_name}': {e}")

        remaining = [t for t in pending if t not in applied]
        ReplicationConfig.objects.filter(pk=self.config.pk).update(pending_truncates=remaining)
        self.config.pending_truncates = remaining

        if applied:
            self._log_info(f"  ✓ Deferred TRUNCATEs applied: {applied}")

    def pause_connector(self) -> Tuple[bool, str]:
        """
        Pause a running source connector (manual pause/resume control).

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

    def pause_sink_connector(self) -> Tuple[bool, str]:
        """
        Pause the sink connector and record sink_connector_state='PAUSED'.

        While paused, DML buffers in Kafka and any source TRUNCATE is deferred
        into pending_truncates (see base_processor._handle_truncate_for_table),
        so the target is never truncated out from under the paused pipeline.

        Returns:
            (success, message)
        """
        self._log_info("Pausing sink connector...")

        if not self.config.sink_connector_name:
            return False, "No sink connector configured"

        try:
            success, error = self.connector_manager.pause_connector(self.config.sink_connector_name)

            if success:
                self.config.sink_connector_state = 'PAUSED'
                self.config.save(update_fields=['sink_connector_state'])
                self._log_info(f"✓ Sink connector paused: {self.config.sink_connector_name}")
                return True, "Sink connector paused"
            else:
                return False, f"Failed to pause sink connector: {error}"

        except Exception as e:
            error_msg = f"Error pausing sink connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def resume_sink_connector(self) -> Tuple[bool, str]:
        """
        Resume the sink connector, record sink_connector_state='RUNNING' and drain
        any TRUNCATEs that were deferred while the pipeline was paused.

        Returns:
            (success, message)
        """
        self._log_info("Resuming sink connector...")

        if not self.config.sink_connector_name:
            return False, "No sink connector configured"

        try:
            success, error = self.connector_manager.resume_connector(self.config.sink_connector_name)

            if success:
                self.config.sink_connector_state = 'RUNNING'
                self.config.save(update_fields=['sink_connector_state'])
                self._log_info(f"✓ Sink connector resumed: {self.config.sink_connector_name}")
                self._apply_pending_truncates()
                return True, "Sink connector resumed"
            else:
                return False, f"Failed to resume sink connector: {error}"

        except Exception as e:
            error_msg = f"Error resuming sink connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def recover_binlog_offset(self) -> Tuple[bool, str]:
        """
        Recover a MySQL connector stuck on a purged binlog position.

        The connector's saved offset points to a binlog file that no longer
        exists on the server (binlog was rotated/purged). This temporarily
        sets snapshot.mode=schema_only_recovery so Debezium skips the missing
        position and resumes from the earliest available binlog, then reverts
        the setting so future restarts behave normally.

        Steps:
          1. Fetch current config from Kafka Connect
          2. Swap snapshot.mode to schema_only_recovery
          3. Restart failed tasks
          4. Poll until task is RUNNING (max 20 s)
          5. Revert snapshot.mode to original value
        """
        import time
        connector_name = self.config.connector_name
        if not connector_name:
            return False, "No connector configured"

        try:
            # Step 1 — read current config
            current_config = self.connector_manager.get_connector_config(connector_name)
            if not current_config:
                return False, "Could not read connector config from Kafka Connect"

            original_snapshot_mode = current_config.get('snapshot.mode', 'initial')

            # Step 2 — patch snapshot.mode
            recovery_config = dict(current_config)
            recovery_config['snapshot.mode'] = 'recovery'
            ok, err = self.connector_manager.update_connector_config(connector_name, recovery_config)
            if not ok:
                return False, f"Failed to update connector config: {err}"
            self._log_info("Set snapshot.mode=schema_only_recovery for binlog recovery")

            # Step 3 — restart failed tasks
            _, status_data = self.connector_manager.get_connector_status(connector_name)
            failed_tasks = [
                t for t in (status_data or {}).get('tasks', [])
                if t.get('state') == 'FAILED'
            ]
            if not failed_tasks:
                failed_tasks = (status_data or {}).get('tasks', [])  # restart all if none explicitly failed

            for task in failed_tasks:
                self.connector_manager.restart_task(connector_name, task.get('id', 0))

            # Step 4 — poll until running (max 20 s)
            recovered = False
            for _ in range(20):
                time.sleep(1)
                _, status_data = self.connector_manager.get_connector_status(connector_name)
                tasks = (status_data or {}).get('tasks', [])
                if tasks and all(t.get('state') == 'RUNNING' for t in tasks):
                    recovered = True
                    break

            # Step 5 — revert snapshot.mode regardless of outcome
            revert_config = dict(recovery_config)
            revert_config['snapshot.mode'] = original_snapshot_mode
            self.connector_manager.update_connector_config(connector_name, revert_config)
            self._log_info(f"Reverted snapshot.mode back to '{original_snapshot_mode}'")

            if recovered:
                self._log_info("✓ Binlog offset recovery successful")
                return True, "Connector recovered successfully. Binlog offset has been reset."

            return True, "Recovery initiated. Tasks were restarted — check status in a few seconds."

        except Exception as e:
            error_msg = f"Binlog offset recovery failed: {str(e)}"
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
        db_config = self.config.client_database
        db_type = db_config.db_type
        topic_prefix = self.config.kafka_topic_prefix

        # SQL Server registers JMX MBeans under database.server.name, not topic.prefix.
        # Use the server_name format for Jolokia queries; fall back to topic_prefix.
        if db_type == 'mssql' and self.config.connector_version:
            topic_prefix = (
                f"sqlserver_{db_config.client.id}_{db_config.id}"
                f"_v_{self.config.connector_version}"
            )

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

