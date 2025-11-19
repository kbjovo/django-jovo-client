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

    # ==========================================
    # Main Operations
    # ==========================================

    def start_replication(self, force_resync: bool = False) -> Tuple[bool, str]:
        """
        Start complete replication (connector + consumer).

        SIMPLIFIED FLOW:
        1. Validate prerequisites (database connectivity, permissions, binlog settings)
        2. Start Debezium connector with 'initial' snapshot mode
           - Debezium performs initial snapshot → Kafka topics (auto-created)
           - Then streams real-time CDC events
        3. Start consumer to process messages from Kafka → Target database

        Args:
            force_resync: If True, restart connector to re-snapshot all data

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
            self._log_info("STEP 1/3: Validating prerequisites...")
            self._log_info("  → Checking database connectivity")
            self._log_info("  → Verifying MySQL binlog configuration")
            self._log_info("  → Validating user permissions")

            is_valid, errors = self.validator.validate_all()
            if not is_valid:
                error_msg = f"Validation failed: {'; '.join(errors)}"
                self._log_error(error_msg)
                self._update_status('error', error_msg)
                return False, error_msg

            self._log_info("✓ All prerequisites validated successfully")
            self._log_info("")

            # ========================================
            # STEP 2: Start Debezium Connector
            # ========================================
            self._log_info("STEP 2/3: Starting Debezium connector...")
            self._log_info("  → Snapshot mode: 'initial' (full snapshot + CDC streaming)")
            self._log_info("  → Source: {}.{}".format(
                self.config.client_database.host,
                self.config.client_database.database_name
            ))

            enabled_tables = list(
                self.config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True)
            )
            self._log_info(f"  → Tables: {', '.join(enabled_tables)}")

            # Delete old connector if force_resync
            if force_resync:
                self._log_info("  → Force resync enabled: deleting old connector...")
                try:
                    self.connector_manager.delete_connector(self.config.connector_name)
                    self._log_info("  ✓ Old connector deleted")
                except:
                    self._log_info("  → No old connector to delete")

            # Start connector with initial snapshot
            success, message = self._ensure_connector_running(snapshot_mode='initial')
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ Debezium connector started: {self.config.connector_name}")
            self._log_info("  → Initial snapshot in progress (Debezium → Kafka)")
            self._log_info("  → Kafka topics will be auto-created")
            self._log_info("  → Binlog streaming will begin after snapshot completes")
            self._log_info("")

            # ========================================
            # STEP 3: Start Consumer
            # ========================================
            self._log_info("STEP 3/3: Starting Kafka consumer...")
            self._log_info("  → Consumer will read from Kafka topics")
            self._log_info("  → Data flow: Kafka → Consumer → Target database")

            # Mark as active BEFORE starting consumer (consumer task checks this)
            self._update_status('active', 'Consumer starting...')
            self.config.is_active = True
            self.config.save()

            success, message = self._start_consumer_with_fresh_group()
            if not success:
                self._update_status('error', message)
                return False, message

            self._log_info(f"✓ Consumer started successfully")
            self._log_info("")

            # ========================================
            # Success Summary
            # ========================================
            self._log_info("=" * 60)
            self._log_info("✓ CDC REPLICATION STARTED SUCCESSFULLY")
            self._log_info("=" * 60)
            self._log_info("Status: ACTIVE")
            self._log_info(f"Connector: {self.config.connector_name}")
            self._log_info(f"Topics: {self.config.kafka_topic_prefix}.*")
            self._log_info(f"Consumer Group: cdc_consumer_{self.config.client_database.client.id}_{self.config.id}")
            self._log_info("")
            self._log_info("Next steps:")
            self._log_info("  1. Monitor initial snapshot progress in Debezium logs")
            self._log_info("  2. Check consumer is processing messages")
            self._log_info("  3. Verify data appearing in target database")
            self._log_info("=" * 60)

            self._update_status('active', 'Replication active')
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
            self._log_info("Connector didn't exist")
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