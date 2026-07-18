"""SinkConnectorMixin

JDBC sink connector create/update/delete + pause/resume.

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


class SinkConnectorMixin:
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
