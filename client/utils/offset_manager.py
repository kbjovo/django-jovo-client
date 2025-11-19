"""
Debezium Offset Management Utility

Handles cleanup of Debezium connector offsets stored in Kafka.
This ensures connectors can perform fresh snapshots when recreated.
"""

import json
import logging
from typing import Optional, List, Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from django.conf import settings

logger = logging.getLogger(__name__)


class DebeziumOffsetManager:
    """
    Manages Debezium connector offsets in Kafka

    Background:
    - Debezium stores connector offsets in a compacted topic (debezium_connect_offsets)
    - Offsets track the position in the source database (binlog position, LSN, etc.)
    - When a connector is deleted, offsets remain in Kafka
    - On recreation, Debezium reads old offsets → skips snapshot → only streams new changes

    Solution:
    - Delete offset entries when connector is deleted
    - Provide "force resnapshot" capability by clearing offsets
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        offset_topic: str = 'debezium_connect_offsets'
    ):
        """
        Initialize offset manager

        Args:
            bootstrap_servers: Kafka bootstrap servers (e.g., 'kafka:29092')
            offset_topic: Kafka topic where Debezium stores offsets
        """
        self.bootstrap_servers = bootstrap_servers or settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka:29092'
        )
        self.offset_topic = offset_topic

        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'offset_manager_{id(self)}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
        }

        logger.info(f"Initialized DebeziumOffsetManager with bootstrap_servers={self.bootstrap_servers}")

    def delete_connector_offsets(self, connector_name: str) -> bool:
        """
        Delete all offset entries for a specific connector

        How it works:
        - Kafka offsets are stored as key-value pairs in a compacted topic
        - Key format: ["debezium-cluster", {"server": "connector_name"}]
        - To delete: write null value for the key (tombstone record)
        - Kafka compaction will remove the entry

        Args:
            connector_name: Name of the connector (e.g., 'gaurav_kbe_prod_connector')

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"🗑️  Deleting offsets for connector: {connector_name}")

            # Step 1: Read all offsets to find matching keys
            matching_keys = self._find_connector_offset_keys(connector_name)

            if not matching_keys:
                logger.warning(f"No offsets found for connector: {connector_name}")
                return True  # Nothing to delete, consider success

            logger.info(f"Found {len(matching_keys)} offset entries to delete")

            # Step 2: Write tombstone records (null values) to delete offsets
            deleted_count = self._write_tombstones(matching_keys)

            logger.info(f"✅ Successfully deleted {deleted_count} offset entries for {connector_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to delete offsets for {connector_name}: {e}", exc_info=True)
            return False

    def _find_connector_offset_keys(self, connector_name: str) -> List[bytes]:
        """
        Find all offset keys for a specific connector

        Args:
            connector_name: Connector name to search for

        Returns:
            List[bytes]: List of matching offset keys (raw bytes)
        """
        matching_keys = []
        consumer = None

        try:
            consumer = Consumer(self.consumer_config)
            consumer.subscribe([self.offset_topic])

            logger.info(f"Scanning offset topic: {self.offset_topic}")

            # Read all messages from offset topic
            messages_processed = 0
            no_message_count = 0
            max_empty_polls = 10  # Stop after 10 consecutive empty polls

            while no_message_count < max_empty_polls:
                msg = consumer.poll(timeout=2.0)

                if msg is None:
                    no_message_count += 1
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition
                        no_message_count += 1
                        continue
                    else:
                        raise KafkaException(msg.error())

                no_message_count = 0  # Reset counter when we get a message
                messages_processed += 1

                # Check if this key matches our connector
                key = msg.key()
                value = msg.value()

                # Skip tombstone records (null values)
                if value is None:
                    continue

                # Try to parse the key
                if key and self._key_matches_connector(key, connector_name):
                    matching_keys.append(key)
                    logger.debug(f"Found matching offset key for {connector_name}")

            logger.info(f"Scanned {messages_processed} offset messages, found {len(matching_keys)} matches")

        except Exception as e:
            logger.error(f"Error scanning offset topic: {e}", exc_info=True)
            raise

        finally:
            if consumer:
                consumer.close()

        return matching_keys

    def _key_matches_connector(self, key: bytes, connector_name: str) -> bool:
        """
        Check if an offset key belongs to the specified connector

        Offset key format:
        - JSON array: ["debezium-cluster", {"server": "connector_name"}]
        - Or similar variations

        Args:
            key: Raw key bytes from Kafka
            connector_name: Connector name to match

        Returns:
            bool: True if key matches connector
        """
        try:
            # Try to parse as JSON
            key_str = key.decode('utf-8')
            key_data = json.loads(key_str)

            # Check various formats
            if isinstance(key_data, list):
                # Format: ["group_id", {...}]
                for item in key_data:
                    if isinstance(item, dict):
                        # Look for server/connector name in dict
                        server_name = item.get('server', '')
                        if connector_name in server_name or server_name in connector_name:
                            return True

            # Also check if connector name appears anywhere in the key
            if connector_name in key_str:
                return True

        except Exception as e:
            logger.debug(f"Could not parse offset key: {e}")

        return False

    def _write_tombstones(self, keys: List[bytes]) -> int:
        """
        Write tombstone records (null values) to delete offset entries

        Args:
            keys: List of keys to delete

        Returns:
            int: Number of tombstones written
        """
        producer = None
        deleted_count = 0

        try:
            producer = Producer(self.producer_config)

            for key in keys:
                # Write null value (tombstone) to delete the offset
                producer.produce(
                    topic=self.offset_topic,
                    key=key,
                    value=None,  # Tombstone
                )
                deleted_count += 1

            # Wait for all messages to be delivered
            producer.flush(timeout=10)

            logger.info(f"Wrote {deleted_count} tombstone records to {self.offset_topic}")

        except Exception as e:
            logger.error(f"Error writing tombstones: {e}", exc_info=True)
            raise

        finally:
            if producer:
                producer.flush()

        return deleted_count

    def check_connector_has_offsets(self, connector_name: str) -> bool:
        """
        Check if a connector has existing offsets in Kafka

        Args:
            connector_name: Connector name

        Returns:
            bool: True if offsets exist, False otherwise
        """
        try:
            matching_keys = self._find_connector_offset_keys(connector_name)
            return len(matching_keys) > 0
        except Exception as e:
            logger.error(f"Error checking offsets for {connector_name}: {e}")
            return False

    def list_all_connector_offsets(self) -> Dict[str, int]:
        """
        List all connectors with offsets and their count

        Returns:
            Dict[str, int]: Dictionary of connector_name -> offset_count
        """
        consumer = None
        connector_offsets = {}

        try:
            consumer = Consumer(self.consumer_config)
            consumer.subscribe([self.offset_topic])

            messages_processed = 0
            no_message_count = 0
            max_empty_polls = 10

            while no_message_count < max_empty_polls:
                msg = consumer.poll(timeout=2.0)

                if msg is None:
                    no_message_count += 1
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        no_message_count += 1
                        continue
                    else:
                        raise KafkaException(msg.error())

                no_message_count = 0
                messages_processed += 1

                # Extract connector name from key
                key = msg.key()
                value = msg.value()

                if key and value:  # Skip tombstones
                    try:
                        key_str = key.decode('utf-8')
                        key_data = json.loads(key_str)

                        # Try to extract connector/server name
                        if isinstance(key_data, list):
                            for item in key_data:
                                if isinstance(item, dict) and 'server' in item:
                                    server_name = item['server']
                                    connector_offsets[server_name] = connector_offsets.get(server_name, 0) + 1
                    except:
                        pass

            logger.info(f"Found offsets for {len(connector_offsets)} connectors")

        except Exception as e:
            logger.error(f"Error listing connector offsets: {e}", exc_info=True)

        finally:
            if consumer:
                consumer.close()

        return connector_offsets


def delete_connector_offsets(connector_name: str) -> bool:
    """
    Convenience function to delete connector offsets

    Args:
        connector_name: Name of the connector

    Returns:
        bool: True if successful
    """
    manager = DebeziumOffsetManager()
    return manager.delete_connector_offsets(connector_name)


def check_connector_has_offsets(connector_name: str) -> bool:
    """
    Convenience function to check if connector has offsets

    Args:
        connector_name: Name of the connector

    Returns:
        bool: True if offsets exist
    """
    manager = DebeziumOffsetManager()
    return manager.check_connector_has_offsets(connector_name)
