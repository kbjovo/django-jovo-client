"""TopicsMixin

Kafka topic creation/deletion and topic-name resolution.

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


class TopicsMixin:
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
