"""
Replication Orchestrator - Main entry point for all replication operations.

Manages the complete lifecycle of CDC replication:
- Starting/stopping replication (connector + consumer as a unit)
- Validating prerequisites
- Monitoring health
- Providing unified status
"""

import logging
from typing import Dict, Any, Tuple, Optional
from django.utils import timezone

from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.kafka_topic_manager import KafkaTopicManager
from client.utils.connector_templates import get_connector_config_for_database
from .validators import ReplicationValidator

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

    # ==========================================
    # Main Operations
    # ==========================================

    def start_replication(self, force_resync: bool = False) -> Tuple[bool, str]:
        """
        Start complete replication (connector + consumer).

        NEW SIMPLIFIED FLOW (Option A):
        1. Validate prerequisites
        2. Perform initial SQL copy (if needed)
        3. Start Debezium connector (CDC-only mode)
        4. Start consumer with fresh group ID

        Args:
            force_resync: If True, truncate target tables and reload all data

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STARTING REPLICATION (SIMPLIFIED)")
        self._log_info("=" * 60)

        try:
            # STEP 1: Validate prerequisites
            self._log_info("STEP 1/4: Validating prerequisites...")
            is_valid, errors = self.validator.validate_all()
            if not is_valid:
                error_msg = f"Validation failed: {'; '.join(errors)}"
                self._log_error(error_msg)
                self._update_status('error', error_msg)
                return False, error_msg

            self._log_info("✓ All prerequisites validated")

            # STEP 2: Initial data load via SQL copy
            self._log_info("STEP 2/4: Performing initial data sync...")
            success, message = self._perform_initial_sync(force_resync=force_resync)
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info("✓ Initial data sync completed")

            # STEP 3: Ensure connector exists and is running (CDC-only mode)
            # Use 'schema_only' to snapshot schema (not data) and establish binlog position
            self._log_info("STEP 3/4: Starting Debezium connector (CDC-only)...")
            success, message = self._ensure_connector_running(snapshot_mode='schema_only')
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ Connector is running: {self.config.connector_name}")

            # Mark as active BEFORE starting consumer (consumer task checks this)
            self._update_status('active', 'Replication active')
            self.config.is_active = True
            self.config.save()

            # STEP 4: Start consumer with fresh group ID
            self._log_info("STEP 4/4: Starting consumer with fresh group ID...")
            success, message = self._start_consumer_with_fresh_group()
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info("=" * 60)
            self._log_info("✓ REPLICATION STARTED SUCCESSFULLY")
            self._log_info("=" * 60)

            return True, "Replication started successfully"

        except Exception as e:
            error_msg = f"Failed to start replication: {str(e)}"
            self._log_error(error_msg)
            self._update_status('error', error_msg)
            return False, error_msg

    def stop_replication(self) -> Tuple[bool, str]:
        """
        Stop replication (consumer + optionally connector).

        Args:
            stop_connector: Whether to also stop the Debezium connector

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("STOPPING REPLICATION")
        self._log_info("=" * 60)

        try:
            # STEP 1: Update status
            self._log_info("STEP 1/3: Updating status to 'stopping'...")
            self._update_status('paused', 'Stopping consumer...')
            self.config.is_active = False
            self.config.save()

            # STEP 2: Consumer will be stopped by Celery task revocation
            self._log_info("STEP 2/3: Consumer will be stopped by task manager...")

            # STEP 3: Optionally pause connector
            self._log_info("STEP 3/3: Pausing Debezium connector...")
            try:
                self.connector_manager.pause_connector(self.config.connector_name)
                self._log_info(f"✓ Connector paused: {self.config.connector_name}")
            except Exception as e:
                self._log_warning(f"Could not pause connector: {e}")

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

    def delete_replication(self, delete_topics: bool = False) -> Tuple[bool, str]:
        """
        Delete replication completely (connector + topics).

        Args:
            delete_topics: Whether to also delete Kafka topics

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("DELETING REPLICATION")
        self._log_info("=" * 60)

        try:
            # STEP 1: Stop replication
            self._log_info("STEP 1/3: Stopping replication...")
            self.stop_replication()

            # STEP 2: Delete connector
            self._log_info("STEP 2/3: Deleting Debezium connector...")
            try:
                self.connector_manager.delete_connector(
                    self.config.connector_name,
                    delete_topics=delete_topics
                )
                self._log_info(f"✓ Connector deleted: {self.config.connector_name}")
            except Exception as e:
                self._log_warning(f"Could not delete connector: {e}")

            # STEP 3: Update status
            self._log_info("STEP 3/3: Updating configuration...")
            self.config.status = 'configured'
            self.config.is_active = False
            self.config.connector_state = 'DELETED'
            self.config.save()

            self._log_info("=" * 60)
            self._log_info("✓ REPLICATION DELETED SUCCESSFULLY")
            self._log_info("=" * 60)

            return True, "Replication deleted successfully"

        except Exception as e:
            error_msg = f"Failed to delete replication: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    # ==========================================
    # Status and Health
    # ==========================================

    def get_unified_status(self) -> Dict[str, Any]:
        """
        Get comprehensive status of replication.

        Returns a detailed status object showing:
        - Connector state (RUNNING/FAILED/PAUSED)
        - Consumer state (RUNNING/STOPPED/ERROR)
        - Last sync info
        - Statistics
        - Health check results

        Returns:
            Dictionary with complete status information
        """
        # Get connector status
        connector_status = self._get_connector_status()

        # Get consumer status
        consumer_status = self._get_consumer_status()

        # Calculate overall health
        overall_health = self._calculate_overall_health(connector_status, consumer_status)

        return {
            'overall': overall_health,
            'connector': connector_status,
            'consumer': consumer_status,
            'config': {
                'status': self.config.status,
                'is_active': self.config.is_active,
                'connector_name': self.config.connector_name,
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
            'timestamp': timezone.now().isoformat(),
        }

    def _get_connector_status(self) -> Dict[str, Any]:
        """Get Debezium connector status from Kafka Connect."""
        try:
            status_data = self.connector_manager.get_connector_status(self.config.connector_name)

            if not status_data:
                return {
                    'state': 'NOT_FOUND',
                    'healthy': False,
                    'message': 'Connector does not exist',
                }

            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = status_data.get('tasks', [])

            return {
                'state': connector_state,
                'healthy': connector_state == 'RUNNING',
                'tasks': tasks,
                'message': f"Connector is {connector_state}",
            }

        except Exception as e:
            return {
                'state': 'ERROR',
                'healthy': False,
                'message': f"Failed to get connector status: {str(e)}",
            }

    def _get_consumer_status(self) -> Dict[str, Any]:
        """Get consumer status from ReplicationConfig."""
        consumer_state = getattr(self.config, 'consumer_state', 'UNKNOWN')
        last_heartbeat = getattr(self.config, 'consumer_last_heartbeat', None)

        # Check if heartbeat is recent (within last 2 minutes)
        heartbeat_recent = False
        if last_heartbeat:
            time_diff = timezone.now() - last_heartbeat
            heartbeat_recent = time_diff.total_seconds() < 120

        is_healthy = (
            consumer_state == 'RUNNING' and
            heartbeat_recent and
            self.config.is_active
        )

        return {
            'state': consumer_state,
            'healthy': is_healthy,
            'last_heartbeat': last_heartbeat.isoformat() if last_heartbeat else None,
            'heartbeat_recent': heartbeat_recent,
            'message': self._get_consumer_message(consumer_state, heartbeat_recent),
        }

    def _get_consumer_message(self, state: str, heartbeat_recent: bool) -> str:
        """Get human-readable consumer status message."""
        if state == 'RUNNING' and heartbeat_recent:
            return "Consumer is healthy and processing messages"
        elif state == 'RUNNING' and not heartbeat_recent:
            return "Consumer may be stuck (no recent heartbeat)"
        elif state == 'STOPPED':
            return "Consumer is stopped"
        elif state == 'ERROR':
            return "Consumer encountered an error"
        else:
            return f"Consumer state: {state}"

    def _calculate_overall_health(self, connector_status: Dict, consumer_status: Dict) -> str:
        """
        Calculate overall health status.

        Returns:
            'healthy', 'degraded', or 'failed'
        """
        connector_healthy = connector_status.get('healthy', False)
        consumer_healthy = consumer_status.get('healthy', False)

        if connector_healthy and consumer_healthy:
            return 'healthy'
        elif connector_healthy or consumer_healthy:
            return 'degraded'
        else:
            return 'failed'

    # ==========================================
    # Internal Helpers
    # ==========================================

    def _ensure_connector_running(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Ensure Debezium connector exists and is running.

        Creates connector if it doesn't exist.
        Resumes if paused.
        Restarts if failed.

        Args:
            snapshot_mode: Debezium snapshot mode ('never', 'initial', 'when_needed', etc.)

        Returns:
            (success, message)
        """
        # Delete old connector if exists (for fresh start)
        try:
            self.connector_manager.delete_connector(self.config.connector_name)
            self._log_info("Deleted old connector for fresh start")
        except:
            pass  # Connector didn't exist, that's fine

        # Create fresh connector
        self._log_info(f"Creating connector with snapshot_mode={snapshot_mode}...")
        return self._create_connector(snapshot_mode=snapshot_mode)

    def _create_connector(self, snapshot_mode: str = 'when_needed') -> Tuple[bool, str]:
        """
        Create new Debezium connector.

        Returns:
            (success, message)
        """
        try:
            self._log_info("Creating Debezium connector...")

            # Generate connector configuration
            db_config = self.config.client_database
            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True)
            )

            if not enabled_tables:
                return False, "No tables enabled for replication"

            config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=enabled_tables,
                kafka_bootstrap_servers='kafka:29092',
                schema_registry_url='http://schema-registry:8081',
                snapshot_mode=snapshot_mode,
            )

            # Create connector
            success, error = self.connector_manager.create_connector(
                connector_name=self.config.connector_name,
                config=config
            )

            if success:
                self._log_info(f"✓ Connector created: {self.config.connector_name}")
                return True, "Connector created successfully"
            else:
                return False, f"Failed to create connector: {error}"

        except Exception as e:
            error_msg = f"Failed to create connector: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _update_status(self, status: str, message: Optional[str] = None):
        """Update ReplicationConfig status."""
        self.config.status = status
        if message:
            self.config.last_error_message = message if status == 'error' else ''
        self.config.save()

    def _log_info(self, message: str):
        """Log info message with structured format."""
        logger.info(f"[{self.config.connector_name}] {message}")

    def _log_warning(self, message: str):
        """Log warning message with structured format."""
        logger.warning(f"[{self.config.connector_name}] {message}")

    def _log_error(self, message: str):
        """Log error message with structured format."""
        logger.error(f"[{self.config.connector_name}] {message}")

    # ==========================================
    # NEW: Initial Data Sync Methods
    # ==========================================

    def _perform_initial_sync(self, force_resync: bool = False) -> Tuple[bool, str]:
        """
        Perform initial data sync via direct SQL copy.

        For each enabled table:
        1. Check if target has data (skip if has data and not force_resync)
        2. Optionally truncate if force_resync=True
        3. Copy data via INSERT INTO target SELECT * FROM source
        4. Track progress

        Args:
            force_resync: If True, truncate and reload all data

        Returns:
            (success, message)
        """
        try:
            from sqlalchemy import create_engine, text, inspect
            from client.utils.database_utils import get_database_engine

            # Get source and target engines
            source_engine = get_database_engine(self.config.client_database)
            client = self.config.client_database.client
            target_db = client.get_target_database()

            if not target_db:
                return False, "No target database configured"

            target_engine = get_database_engine(target_db)

            # Get enabled tables
            enabled_mappings = self.config.table_mappings.filter(is_enabled=True)

            if not enabled_mappings.exists():
                self._log_warning("No tables enabled for replication")
                return True, "No tables to sync (skipped)"

            total_tables = enabled_mappings.count()
            synced_tables = 0
            total_rows = 0

            for table_mapping in enabled_mappings:
                self._log_info(f"Syncing table {table_mapping.source_table}...")

                # Check if this is a large table (determine sync vs async)
                source_row_count = self._get_table_row_count(
                    source_engine,
                    self.config.client_database.database_name,
                    table_mapping.source_table
                )

                self._log_info(f"Source table has {source_row_count} rows")

                # Check if target already has data
                target_row_count = self._get_table_row_count(
                    target_engine,
                    target_db.database_name,
                    table_mapping.target_table
                )

                # Skip if target has data and not force_resync
                if target_row_count > 0 and not force_resync:
                    self._log_info(f"Target table already has {target_row_count} rows, skipping")
                    synced_tables += 1
                    continue

                # Decide: sync or async based on size
                if source_row_count > 100000:
                    # Large table - use background task
                    self._log_info(f"Large table ({source_row_count} rows), syncing in background...")
                    success, rows = self._copy_table_async(
                        source_engine, target_engine,
                        table_mapping, force_resync
                    )
                else:
                    # Small table - sync inline
                    success, rows = self._copy_table_sync(
                        source_engine, target_engine,
                        table_mapping, force_resync
                    )

                if not success:
                    return False, f"Failed to sync table {table_mapping.source_table}"

                # Update stats
                table_mapping.total_rows_synced = rows
                table_mapping.save()

                total_rows += rows
                synced_tables += 1
                self._log_info(f"✓ Synced {rows} rows to {table_mapping.target_table}")

            # Close engines
            source_engine.dispose()
            target_engine.dispose()

            self._log_info(f"✓ Initial sync completed: {synced_tables}/{total_tables} tables, {total_rows} rows")
            return True, f"Synced {synced_tables} tables ({total_rows} rows)"

        except Exception as e:
            error_msg = f"Initial sync failed: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg

    def _get_table_row_count(self, engine, database_name: str, table_name: str) -> int:
        """Get row count for a table."""
        from sqlalchemy import text

        try:
            with engine.connect() as conn:
                # Handle different database types
                if 'mysql' in engine.dialect.name or 'mariadb' in engine.dialect.name:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{database_name}`.`{table_name}`"))
                elif 'postgresql' in engine.dialect.name:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                elif 'oracle' in engine.dialect.name:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                else:
                    # Fallback
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))

                return result.scalar() or 0
        except Exception as e:
            self._log_warning(f"Could not get row count for {table_name}: {e}")
            return 0

    def _copy_table_sync(self, source_engine, target_engine, table_mapping, force_resync: bool) -> Tuple[bool, int]:
        """
        Copy table data synchronously via direct SQL.
        Works for MySQL, PostgreSQL, Oracle.
        """
        import traceback
        from sqlalchemy import inspect

        source_db = self.config.client_database.database_name
        target_db = self.config.client_database.client.get_target_database().database_name
        source_table = table_mapping.source_table
        target_table = table_mapping.target_table

        try:
            # Verify target table exists
            self._log_info(f"Checking if target table {target_table} exists...")
            inspector = inspect(target_engine)
            target_tables = inspector.get_table_names(schema=target_db if 'mysql' in target_engine.dialect.name else None)

            if target_table not in target_tables:
                error_msg = f"Target table '{target_table}' does not exist in database '{target_db}'. Please create it first."
                self._log_error(error_msg)
                return False, 0

            self._log_info(f"✓ Target table {target_table} exists")
        except Exception as e:
            self._log_warning(f"Could not verify target table existence: {e}")
            # Continue anyway - might work

        try:
            # Truncate if force_resync
            if force_resync:
                self._log_info(f"Truncating target table {target_table}...")
                try:
                    with target_engine.connect() as conn:
                        if 'mysql' in target_engine.dialect.name:
                            conn.execute(text(f"TRUNCATE TABLE `{target_db}`.`{target_table}`"))
                        else:
                            conn.execute(text(f"TRUNCATE TABLE {target_table}"))
                        conn.commit()
                except Exception as e:
                    self._log_error(f"Failed to truncate target table {target_table}: {e}")
                    raise

            # Copy data - database-agnostic approach
            # We'll use pandas for simplicity and database compatibility
            import pandas as pd

            # Read from source
            self._log_info(f"Reading data from source: {source_db}.{source_table}")
            try:
                if 'mysql' in source_engine.dialect.name:
                    query = f"SELECT * FROM `{source_db}`.`{source_table}`"
                else:
                    query = f"SELECT * FROM {source_table}"

                df = pd.read_sql(query, source_engine)
                rows_copied = len(df)
                self._log_info(f"Read {rows_copied} rows from source")
            except Exception as e:
                self._log_error(f"Failed to read from source table {source_table}: {e}")
                raise

            if rows_copied == 0:
                self._log_info("Source table is empty, nothing to copy")
                return True, 0

            # Write to target
            self._log_info(f"Writing {rows_copied} rows to target: {target_db}.{target_table}")
            try:
                df.to_sql(
                    name=target_table,
                    con=target_engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )
                self._log_info(f"Successfully wrote {rows_copied} rows to target")
            except Exception as e:
                self._log_error(f"Failed to write to target table {target_table}: {e}")
                self._log_error(f"Target table schema may not match source. Check table structure.")
                raise

            return True, rows_copied

        except Exception as e:
            self._log_error(f"=" * 60)
            self._log_error(f"TABLE COPY FAILED: {source_table} → {target_table}")
            self._log_error(f"Error: {str(e)}")
            self._log_error(f"Traceback:\n{traceback.format_exc()}")
            self._log_error(f"=" * 60)
            return False, 0

    def _copy_table_async(self, source_engine, target_engine, table_mapping, force_resync: bool) -> Tuple[bool, int]:
        """
        Copy table data asynchronously for large tables.
        Queues a background Celery task and returns immediately.
        """
        # For now, fall back to sync (we can implement async later if needed)
        self._log_warning("Async copy not yet implemented, using sync copy")
        return self._copy_table_sync(source_engine, target_engine, table_mapping, force_resync)

    def _start_consumer_with_fresh_group(self) -> Tuple[bool, str]:
        """
        Start consumer with timestamp-based group ID.
        This ensures no offset conflicts - always starts fresh.
        """
        try:
            import time
            from client.tasks import start_kafka_consumer

            # Generate fresh consumer group ID
            timestamp = int(time.time())
            client_id = self.config.client_database.client.id
            config_id = self.config.id
            consumer_group = f"cdc_consumer_{client_id}_{config_id}_{timestamp}"

            self._log_info(f"Starting consumer with group: {consumer_group}")

            # Queue consumer task with custom group ID
            result = start_kafka_consumer.apply_async(
                args=[self.config.id],
                kwargs={'consumer_group_override': consumer_group}
            )

            # Update config
            self.config.consumer_task_id = result.id
            self.config.consumer_state = 'STARTING'
            self.config.save()

            self._log_info(f"✓ Consumer task queued: {result.id}")
            return True, f"Consumer started with group {consumer_group}"

        except Exception as e:
            error_msg = f"Failed to start consumer: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg