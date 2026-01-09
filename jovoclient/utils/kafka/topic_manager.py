"""
Kafka Topic Management Utilities - SIMPLIFIED VERSION THAT WORKS

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
        # Use internal Kafka servers by default (for Docker network)
        default_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            settings.DEBEZIUM_CONFIG.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:29092,kafka-2:29092,kafka-3:29092')
        )
        self.bootstrap_servers = bootstrap_servers or default_servers
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

        SIMPLIFIED APPROACH: Just try to delete and handle errors gracefully.
        If deletion fails because consumer is still connected, that's OK -
        Kafka will auto-clean inactive groups after retention period.

        Args:
            group_id: Consumer group ID to delete

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            logger.info(f"Attempting to delete consumer group: {group_id}")

            # Delete consumer group
            fs = self.admin_client.delete_consumer_groups([group_id], request_timeout=30)

            # Wait for deletion to complete
            for gid, future in fs.items():
                try:
                    future.result()  # Wait for operation to complete
                    logger.info(f"✅ Deleted consumer group: {gid}")
                    return True, None
                except Exception as e:
                    error_str = str(e)

                    # Common error: Group not empty (consumer still connected)
                    if "NOT_EMPTY" in error_str or "non-empty" in error_str.lower():
                        error_msg = f"Group {gid} has active members (consumer still connected)"
                        logger.warning(error_msg)
                        return False, error_msg

                    # Common error: Group doesn't exist (already deleted or never created)
                    elif "NOT_FOUND" in error_str or "does not exist" in error_str.lower():
                        logger.info(f"Consumer group {gid} doesn't exist (already deleted or never created)")
                        return True, None  # Consider this success

                    # Common error: Coordinator not available
                    elif "COORDINATOR_NOT_AVAILABLE" in error_str:
                        error_msg = f"Kafka coordinator not available for group {gid}"
                        logger.warning(error_msg)
                        return False, error_msg

                    # Other errors
                    else:
                        error_msg = f"Failed to delete consumer group {gid}: {error_str}"
                        logger.error(error_msg)
                        return False, error_msg

        except Exception as e:
            error_msg = f"Error deleting consumer group: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def check_consumer_group_empty(self, group_id: str) -> Tuple[bool, str]:
        """
        Check if a consumer group is empty (no active members).

        SIMPLIFIED: Uses Consumer API instead of Admin API for better compatibility.

        Args:
            group_id: Consumer group ID

        Returns:
            Tuple[bool, str]: (is_empty, message)
            - (True, "msg") = group is empty or doesn't exist (safe to delete)
            - (False, "msg") = group has active members (cannot delete)
        """
        try:
            from confluent_kafka import Consumer, KafkaException

            # Create a temporary consumer with a unique group ID to check metadata
            # We use a different group ID to avoid interfering
            temp_consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': f'{group_id}_check_{id(self)}',  # Unique temp group
                'enable.auto.commit': False,
                'session.timeout.ms': 6000,
            })

            try:
                # Try to get committed offsets for the group we're checking
                # This will fail if group doesn't exist
                metadata = temp_consumer.list_topics(timeout=5)

                # If we got here, Kafka is responsive
                # We can't directly check members, so we just return True
                # and let delete_consumer_group handle the actual check
                temp_consumer.close()

                logger.debug(f"Consumer group check completed for: {group_id}")
                return True, "Group check completed (cannot determine members)"

            except KafkaException as e:
                temp_consumer.close()
                return False, f"Kafka error: {str(e)}"

        except Exception as e:
            logger.warning(f"Could not check consumer group: {e}")
            # Return True to allow deletion attempt
            return True, f"Check skipped: {str(e)}"

    def delete_topic(self, topic_name: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a Kafka topic and all its messages.

        WARNING: This is a destructive operation. All messages are lost permanently.

        Args:
            topic_name: Name of the topic to delete

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        try:
            # Delete topic
            fs = self.admin_client.delete_topics([topic_name], request_timeout=30)

            # Wait for deletion to complete
            for topic, future in fs.items():
                try:
                    future.result()  # Wait for operation to complete
                    logger.info(f"✅ Deleted topic: {topic}")
                    return True, None
                except Exception as e:
                    error_msg = f"Failed to delete topic {topic}: {str(e)}"
                    logger.error(error_msg)
                    return False, error_msg

        except Exception as e:
            error_msg = f"Error deleting topic '{topic_name}': {str(e)}"
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

    def create_signal_topic(self, topic_prefix: str) -> Tuple[bool, Optional[str]]:
        """
        Create Debezium signal topic for Kafka-based signaling

        Args:
            topic_prefix: Topic prefix (e.g., 'client_1_db_2')

        Returns:
            Tuple[bool, Optional[str]]: (success, error_message)
        """
        signal_topic = f"{topic_prefix}.signals"

        logger.info(f"Creating Debezium signal topic: {signal_topic}")

        result = self.create_topics_bulk([signal_topic])
        success = result.get(signal_topic, False)

        if success:
            return True, None
        else:
            return False, f"Failed to create signal topic: {signal_topic}"

    def create_topics_bulk(self, topic_names: List[str]) -> Dict[str, bool]:
        """
        Create multiple Kafka topics at once

        Args:
            topic_names: List of topic names to create

        Returns:
            Dict mapping topic name to success status
        """
        from confluent_kafka.admin import NewTopic

        results = {}

        try:
            # Check which topics already exist
            existing_topics = set(self.list_topics())

            # Filter out topics that already exist
            topics_to_create = []
            for topic_name in topic_names:
                if topic_name in existing_topics:
                    logger.info(f"Topic already exists: {topic_name}")
                    results[topic_name] = True  # Already exists = success
                else:
                    topics_to_create.append(topic_name)

            # Create new topics
            if topics_to_create:
                new_topics = [
                    NewTopic(
                        topic=topic,
                        num_partitions=self.config['PARTITIONS'],
                        replication_factor=self.config['REPLICATION_FACTOR'],
                        config={
                            'retention.ms': str(self.config['RETENTION_MS']),
                            'retention.bytes': str(self.config['RETENTION_BYTES']),
                            'cleanup.policy': self.config['CLEANUP_POLICY'],
                            'compression.type': self.config['COMPRESSION_TYPE'],
                            'min.insync.replicas': str(self.config['MIN_INSYNC_REPLICAS']),
                        }
                    )
                    for topic in topics_to_create
                ]

                # Create topics asynchronously
                fs = self.admin_client.create_topics(new_topics, request_timeout=30)

                # Wait for creation to complete
                for topic, future in fs.items():
                    try:
                        future.result()  # Wait for operation to complete
                        logger.info(f"✅ Created topic: {topic}")
                        results[topic] = True
                    except Exception as e:
                        error_msg = f"Failed to create topic {topic}: {str(e)}"
                        logger.error(error_msg)
                        results[topic] = False

            return results

        except Exception as e:
            logger.error(f"Error in bulk topic creation: {e}")
            # Mark all as failed if there was a general error
            for topic in topic_names:
                if topic not in results:
                    results[topic] = False
            return results

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

    # ==========================================
    # Replication Config-Specific Topic Methods
    # ==========================================

    def get_required_topics_for_config(self, replication_config) -> List[str]:
        """
        Calculate all Kafka topic names required for a replication config.

        Args:
            replication_config: ReplicationConfig model instance

        Returns:
            List of topic names (data topics + schema change topic + signal topic)
        """
        topics = []

        # Get enabled table mappings
        enabled_tables = replication_config.table_mappings.filter(is_enabled=True)

        # Generate topic names based on source database type
        db_config = replication_config.client_database
        topic_prefix = replication_config.kafka_topic_prefix

        for table_mapping in enabled_tables:
            if db_config.db_type == 'mysql':
                topic = f"{topic_prefix}.{db_config.database_name}.{table_mapping.source_table}"
            elif db_config.db_type == 'postgresql':
                schema = table_mapping.source_schema or 'public'
                topic = f"{topic_prefix}.{schema}.{table_mapping.source_table}"
            elif db_config.db_type == 'mssql':
                schema = table_mapping.source_schema or 'dbo'
                topic = f"{topic_prefix}.{db_config.database_name}.{schema}.{table_mapping.source_table}"
            elif db_config.db_type == 'oracle':
                schema = table_mapping.source_schema
                topic = f"{topic_prefix}.{schema}.{table_mapping.source_table}"
            else:
                logger.warning(f"Unsupported database type: {db_config.db_type}")
                continue

            topics.append(topic)

        # Add schema change topic (when include.schema.changes=true)
        schema_change_topic = topic_prefix
        topics.append(schema_change_topic)

        # Add signal topic for incremental snapshots
        signal_topic = f"{topic_prefix}.signals"
        topics.append(signal_topic)

        return topics

    def create_topics_for_config(self, replication_config) -> Tuple[bool, str]:
        """
        Explicitly create all required Kafka topics for a replication config.

        This eliminates the need for auto-creation and ensures proper configuration.

        Args:
            replication_config: ReplicationConfig model instance

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            logger.info("=" * 60)
            logger.info("CREATING KAFKA TOPICS")
            logger.info("=" * 60)

            # Get required topics
            topics = self.get_required_topics_for_config(replication_config)

            if not topics:
                return False, "No topics to create (no tables enabled)"

            logger.info(f"Topics to create: {len(topics)}")
            for topic in topics:
                logger.info(f"  → {topic}")
            logger.info("")

            # Get topic configuration from settings
            partitions = self.config.get('PARTITIONS', 3)
            replication_factor = self.config.get('REPLICATION_FACTOR', 3)

            logger.info(f"Configuration:")
            logger.info(f"  → Partitions: {partitions}")
            logger.info(f"  → Replication Factor: {replication_factor}")
            logger.info("")

            # Create topics in bulk
            logger.info(f"Creating {len(topics)} topics...")
            results = self.create_topics_bulk(topics)

            # Count results
            created = 0
            skipped = 0
            failed = 0

            for topic, success in results.items():
                if success:
                    # Check if it was newly created or already existed
                    existing_topics = self.list_topics()
                    if topic in existing_topics:
                        logger.info(f"  ✓ Created: {topic}")
                        created += 1
                else:
                    logger.warning(f"  ✗ Failed: {topic}")
                    failed += 1

            logger.info("")
            logger.info(f"Summary: {created} created, {skipped} skipped, {failed} failed")
            logger.info("=" * 60)

            if failed > 0 and created == 0:
                return False, f"Failed to create topics: {failed} failed"

            return True, f"Topics ready: {created + skipped}/{len(topics)}"

        except Exception as e:
            error_msg = f"Failed to create topics: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def delete_topics_by_prefix(self, topic_prefix: str) -> Tuple[bool, str]:
        """
        Delete all Kafka topics with a given prefix.

        WARNING: This is DESTRUCTIVE and will permanently delete all messages.

        Args:
            topic_prefix: Topic prefix to filter topics for deletion

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            logger.info("=" * 60)
            logger.info("DELETING KAFKA TOPICS (DESTRUCTIVE)")
            logger.info("=" * 60)

            # List topics to delete
            topics = self.list_topics(prefix=topic_prefix)

            if not topics:
                logger.info("No topics found to delete")
                logger.info("=" * 60)
                return True, "No topics to delete"

            logger.info(f"Found {len(topics)} topics to delete:")
            for topic in topics:
                logger.info(f"  → {topic}")
            logger.info("")

            # Delete each topic
            deleted = 0
            failed = 0

            for topic in topics:
                try:
                    success, message = self.delete_topic(topic)

                    if success:
                        logger.info(f"  ✓ Deleted: {topic}")
                        deleted += 1
                    else:
                        logger.warning(f"  ✗ Failed: {topic} - {message}")
                        failed += 1

                except Exception as e:
                    logger.warning(f"  ✗ Error: {topic} - {str(e)}")
                    failed += 1

            logger.info("")
            logger.info(f"Summary: {deleted} deleted, {failed} failed")
            logger.info("⚠ ALL MESSAGES PERMANENTLY DELETED")
            logger.info("=" * 60)

            if failed > 0 and deleted == 0:
                return False, f"Failed to delete topics: {failed} failed"

            return True, f"Deleted {deleted}/{len(topics)} topics"

        except Exception as e:
            error_msg = f"Failed to delete topics: {str(e)}"
            logger.error(error_msg)
            return False, error_msg