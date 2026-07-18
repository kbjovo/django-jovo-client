"""ControlMixin

Pause/resume, binlog-offset recovery, task restarts.

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


class ControlMixin:
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
