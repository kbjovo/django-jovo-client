"""
Kafka Topic Management Utilities

Handles creation, deletion, and configuration of Kafka topics
with settings from Django configuration
"""

import logging
from typing import List, Dict, Optional, Tuple
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


    def delete_consumer_group(self, group_id: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a Kafka consumer group.
        
        Args:
            group_id: Consumer group ID to delete
            
        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            from confluent_kafka.admin import AdminClient
            
            admin_client = AdminClient({
                "bootstrap.servers": self.bootstrap_servers
            })
            
            # Delete consumer group
            fs = admin_client.delete_consumer_groups([group_id], request_timeout=30)
            
            # Wait for deletion to complete
            for group_id, future in fs.items():
                try:
                    future.result()  # Wait for operation to complete
                    logger.info(f"✅ Deleted consumer group: {group_id}")
                    return True, None
                except Exception as e:
                    error_msg = f"Failed to delete consumer group {group_id}: {str(e)}"
                    logger.error(error_msg)
                    return False, error_msg
                    
        except Exception as e:
            error_msg = f"Error deleting consumer group: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

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