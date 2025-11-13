"""
Kafka Topic Management Utilities

Handles creation, deletion, and configuration of Kafka topics
with settings from Django configuration
"""

import logging
from typing import List, Dict, Optional
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from django.conf import settings

logger = logging.getLogger(__name__)


class KafkaTopicManager:
    """Manage Kafka topics with configuration from Django settings"""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """
        Initialize Kafka Topic Manager

        Args:
            bootstrap_servers: Kafka bootstrap servers (defaults to settings)
        """
        self.bootstrap_servers = bootstrap_servers or settings.DEBEZIUM_CONFIG['KAFKA_BOOTSTRAP_SERVERS']
        self.admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        self.config = settings.KAFKA_TOPIC_CONFIG

    def list_topics(self, prefix: Optional[str] = None) -> List[str]:
        """
        List all topics in Kafka cluster

        Args:
            prefix: Optional prefix to filter topics

        Returns:
            List of topic names
        """
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topics = list(metadata.topics.keys())

            if prefix:
                topics = [t for t in topics if t.startswith(prefix)]

            logger.info(f"Found {len(topics)} topics" + (f" with prefix '{prefix}'" if prefix else ""))
            return topics

        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            raise

    def topic_exists(self, topic_name: str) -> bool:
        """
        Check if a topic exists

        Args:
            topic_name: Name of the topic

        Returns:
            True if topic exists, False otherwise
        """
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            return topic_name in metadata.topics
        except Exception as e:
            logger.error(f"Failed to check topic existence: {e}")
            return False

    def create_topic(
        self,
        topic_name: str,
        partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        config_overrides: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create a Kafka topic with configuration from settings

        Args:
            topic_name: Name of the topic to create
            partitions: Number of partitions (defaults to settings)
            replication_factor: Replication factor (defaults to settings)
            config_overrides: Optional config overrides

        Returns:
            True if created successfully, False otherwise
        """
        try:
            # Check if topic already exists
            if self.topic_exists(topic_name):
                logger.warning(f"Topic '{topic_name}' already exists")
                return False

            # Get configuration from settings or use defaults
            num_partitions = partitions or self.config['PARTITIONS']
            replica_factor = replication_factor or self.config['REPLICATION_FACTOR']

            # Build topic configuration
            topic_config = {
                'retention.ms': str(self.config['RETENTION_MS']),
                'retention.bytes': str(self.config['RETENTION_BYTES']),
                'cleanup.policy': self.config['CLEANUP_POLICY'],
                'compression.type': self.config['COMPRESSION_TYPE'],
                'min.insync.replicas': str(self.config['MIN_INSYNC_REPLICAS']),
                'segment.bytes': str(self.config['SEGMENT_BYTES']),
                'segment.ms': str(self.config['SEGMENT_MS']),
            }

            # Apply any overrides
            if config_overrides:
                topic_config.update(config_overrides)

            # Create NewTopic object
            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replica_factor,
                config=topic_config
            )
            
            # Create topic
            futures = self.admin_client.create_topics([new_topic])

            # Wait for operation to complete
            for topic, future in futures.items():
                try:
                    future.result()  # Block until topic is created
                    logger.info(f"✅ Created topic '{topic}' with {num_partitions} partition(s), "
                              f"replication factor {replica_factor}")
                    logger.debug(f"Topic config: {topic_config}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
                    return False

        except Exception as e:
            logger.error(f"Error creating topic '{topic_name}': {e}")
            return False

    def create_topics_bulk(
        self,
        topic_names: List[str],
        partitions: Optional[int] = None,
        replication_factor: Optional[int] = None
    ) -> Dict[str, bool]:
        """
        Create multiple topics at once

        Args:
            topic_names: List of topic names to create
            partitions: Number of partitions (defaults to settings)
            replication_factor: Replication factor (defaults to settings)

        Returns:
            Dict mapping topic name to success status
        """
        results = {}

        for topic_name in topic_names:
            success = self.create_topic(topic_name, partitions, replication_factor)
            results[topic_name] = success

        successful = sum(1 for v in results.values() if v)
        logger.info(f"Created {successful}/{len(topic_names)} topics successfully")

        return results

    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a Kafka topic

        Args:
            topic_name: Name of the topic to delete

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            # Check if topic exists
            if not self.topic_exists(topic_name):
                logger.warning(f"Topic '{topic_name}' does not exist")
                return False

            # Delete topic
            futures = self.admin_client.delete_topics([topic_name])

            # Wait for operation to complete
            for topic, future in futures.items():
                try:
                    future.result()  # Block until topic is deleted
                    logger.info(f"🗑️  Deleted topic '{topic}'")
                    return True
                except Exception as e:
                    logger.error(f"Failed to delete topic '{topic}': {e}")
                    return False

        except Exception as e:
            logger.error(f"Error deleting topic '{topic_name}': {e}")
            return False

    def delete_topics_by_prefix(self, prefix: str, exclude_internal: bool = True) -> Dict[str, bool]:
        """
        Delete all topics matching a prefix (used when deleting a connector)

        Args:
            prefix: Topic prefix to match (e.g., 'client_2_db_5')
            exclude_internal: Skip internal topics (starting with '_' or 'schema-changes')

        Returns:
            Dict mapping topic name to deletion success status
        """
        try:
            # List all topics with the prefix
            topics_to_delete = self.list_topics(prefix=prefix)

            if exclude_internal:
                # Exclude internal Kafka topics and schema history topics
                topics_to_delete = [
                    t for t in topics_to_delete
                    if not t.startswith('_') and not t.startswith('schema-changes')
                ]

            if not topics_to_delete:
                logger.info(f"No topics found with prefix '{prefix}'")
                return {}

            logger.info(f"Deleting {len(topics_to_delete)} topics with prefix '{prefix}': {topics_to_delete}")

            # Delete topics
            results = {}
            futures = self.admin_client.delete_topics(topics_to_delete, operation_timeout=30)

            # Wait for all deletions to complete
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"✅ Deleted topic '{topic}'")
                    results[topic] = True
                except Exception as e:
                    logger.error(f"❌ Failed to delete topic '{topic}': {e}")
                    results[topic] = False

            successful = sum(1 for v in results.values() if v)
            logger.info(f"Deleted {successful}/{len(topics_to_delete)} topics successfully")

            return results

        except Exception as e:
            logger.error(f"Error deleting topics with prefix '{prefix}': {e}")
            return {}

    def get_topic_config(self, topic_name: str) -> Optional[Dict[str, str]]:
        """
        Get configuration for a topic

        Args:
            topic_name: Name of the topic

        Returns:
            Dictionary of topic configuration or None if failed
        """
        try:
            resource = ConfigResource(ResourceType.TOPIC, topic_name)
            futures = self.admin_client.describe_configs([resource])

            for res, future in futures.items():
                try:
                    configs = future.result()
                    config_dict = {
                        name: entry.value
                        for name, entry in configs.items()
                    }
                    return config_dict
                except Exception as e:
                    logger.error(f"Failed to get config for topic '{topic_name}': {e}")
                    return None

        except Exception as e:
            logger.error(f"Error getting topic config: {e}")
            return None

    def create_cdc_topics_for_tables(
        self,
        server_name: str,
        database: str,
        table_names: List[str]
    ) -> Dict[str, bool]:
        """
        Create CDC topics for a list of database tables

        Topic naming follows Debezium pattern: {server_name}.{database}.{table}

        Args:
            server_name: Debezium server name (topic prefix)
            database: Database name
            table_names: List of table names

        Returns:
            Dict mapping topic name to success status
        """
        # Generate topic names following Debezium pattern
        topic_names = [f"{server_name}.{database}.{table}" for table in table_names]

        logger.info(f"Creating {len(topic_names)} CDC topics for {server_name}.{database}")

        return self.create_topics_bulk(topic_names)

    def get_summary(self) -> Dict:
        """
        Get summary of Kafka topic configuration

        Returns:
            Dictionary with configuration summary
        """
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'default_config': {
                'partitions': self.config['PARTITIONS'],
                'replication_factor': self.config['REPLICATION_FACTOR'],
                'retention_ms': self.config['RETENTION_MS'],
                'retention_days': self.config['RETENTION_MS'] / (1000 * 60 * 60 * 24),
                'retention_bytes': self.config['RETENTION_BYTES'],
                'cleanup_policy': self.config['CLEANUP_POLICY'],
                'compression_type': self.config['COMPRESSION_TYPE'],
                'min_insync_replicas': self.config['MIN_INSYNC_REPLICAS'],
            },
            'total_topics': len(self.list_topics()),
        }