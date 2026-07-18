"""OrchestratorBase

Shared __init__ state and structured-logging helpers.

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


class OrchestratorBase:
    """Orchestrates all replication operations.

This is the single source of truth for managing CDC replication.
It ensures connector and consumer are always in sync."""

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
