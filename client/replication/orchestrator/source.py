"""SourceConnectorMixin

Debezium source connector, snapshots, PG slot/publication.

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


class SourceConnectorMixin:
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

            # Disable Schema Registry compatibility checks (global NONE) before the source
            # starts registering schemas. This lets breaking DDL (rename/drop column, type
            # change) register a new schema version without us ever deleting the old one —
            # deleting a schema orphans messages still retained on the topic that reference
            # its id, making the sink fail with 40403 "Schema <id> not found".
            if not set_compatibility_mode(None, "NONE"):
                self._log_warning(
                    "Could not set Schema Registry compatibility to NONE; breaking DDL "
                    "changes may be rejected when the source registers a new schema"
                )

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
