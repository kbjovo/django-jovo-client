"""StatusMixin

Unified status, health calc, snapshot progress, recoverability.

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


class StatusMixin:
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


    def _status_for_connector(self, name: str) -> Dict[str, Any]:
        """
        Status dict for one connector.

        A timeout / connection error is reported as UNREACHABLE (unknown), never
        as NOT_FOUND: under snapshot load Kafka Connect can be slow to answer, and
        treating that as "connector missing" flipped configs to 'error', sent
        "Connector Missing" emails and stopped their DDL threads.
        """
        try:
            timeout = int(getattr(settings, 'CONNECTOR_STATUS_TIMEOUT', 8))
            outcome, status_data = self.connector_manager.get_connector_status_detailed(name, timeout=timeout)
            if outcome == 'not_found':
                return {'state': 'NOT_FOUND', 'healthy': False}
            if outcome != 'ok' or not status_data:
                return {'state': 'UNREACHABLE', 'healthy': False, 'unknown': True}

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


    def _get_connector_status(self) -> Dict[str, Any]:
        """Get Debezium source connector status."""
        return self._status_for_connector(self.config.connector_name)


    def _get_sink_connector_status(self) -> Dict[str, Any]:
        """Get JDBC Sink connector status."""
        if not self.config.sink_connector_name:
            return {'state': 'NOT_CONFIGURED', 'healthy': False}
        return self._status_for_connector(self.config.sink_connector_name)


    def _calculate_overall_health(self, connector_status: Dict, sink_status: Dict) -> str:
        """Calculate overall health: 'healthy', 'degraded', 'unhealthy' or 'unknown'.

        'degraded' means at least one side is unhealthy but in a state the health
        monitor can auto-recover (PAUSED, FAILED, or RUNNING with a FAILED task —
        e.g. the source database went unresponsive). 'unhealthy' means a
        non-recoverable state (NOT_FOUND, ERROR, NOT_CONFIGURED, UNKNOWN) that
        needs manual intervention.
        """
        connector_healthy = connector_status.get('healthy', False)
        sink_healthy = sink_status.get('healthy', False)

        if connector_healthy and sink_healthy:
            return 'healthy'

        # Kafka Connect didn't answer for one side: we genuinely don't know.
        if connector_status.get('unknown') or sink_status.get('unknown'):
            return 'unknown'

        if self._is_recoverable(connector_status) and self._is_recoverable(sink_status):
            return 'degraded'
        return 'unhealthy'


    @staticmethod
    def _is_recoverable(status: Dict) -> bool:
        """Whether a connector status can be auto-recovered by a resume/restart.

        Healthy sides are trivially recoverable (nothing to do). An unhealthy side
        is recoverable when it is PAUSED, FAILED, or RUNNING with a FAILED task
        (the typical shape of a source/sink DB outage — the connector stays RUNNING
        while its task fails). NOT_FOUND / ERROR / NOT_CONFIGURED / UNKNOWN are not.
        """
        if status.get('healthy'):
            return True
        if status.get('state') in ('PAUSED', 'FAILED'):
            return True
        return any(t.get('state') == 'FAILED' for t in status.get('tasks', []))


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
