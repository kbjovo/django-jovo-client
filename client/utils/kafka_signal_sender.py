"""
Kafka Signal Sender for Debezium - FIXED for PostgreSQL

Sends signals to Debezium connectors via Kafka topics to trigger operations
like incremental snapshots without modifying the source database.

CRITICAL FIX: PostgreSQL uses schema.table format, MySQL uses database.table format

Supports:
- Incremental snapshots for new tables
- Ad-hoc snapshots
- Pause/resume signals
"""

import logging
import json
import time
from typing import List, Optional, Tuple
from confluent_kafka import Producer
from django.conf import settings

logger = logging.getLogger(__name__)


class KafkaSignalSender:
    """
    Send signals to Debezium connectors via Kafka topics

    This allows triggering incremental snapshots and other operations
    without creating a signal table in the source database.
    """

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """
        Initialize Kafka Signal Sender

        Args:
            bootstrap_servers: Kafka bootstrap servers (defaults to settings)
        """
        self.bootstrap_servers = bootstrap_servers or settings.DEBEZIUM_CONFIG['KAFKA_BOOTSTRAP_SERVERS']

        # Kafka Producer configuration
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'debezium-signal-sender',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,
            'retry.backoff.ms': 1000,
            'max.in.flight.requests.per.connection': 1,  # Ensure ordering
        }

        self.producer = Producer(producer_config)
        logger.info(f"Initialized KafkaSignalSender with bootstrap servers: {self.bootstrap_servers}")

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            logger.error(f"❌ Signal delivery failed: {err}")
        else:
            logger.info(f"✅ Signal delivered to {msg.topic()} [partition {msg.partition()}]")

    def send_incremental_snapshot_signal(
        self,
        topic_prefix: str,
        database_name: str,
        table_names: List[str],
        schema_name: str = 'public',
        db_type: str = 'postgresql',
        signal_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send incremental snapshot signal for specific tables

        This triggers Debezium to snapshot ONLY the specified tables
        without affecting ongoing CDC for existing tables.

        CRITICAL FIX: PostgreSQL uses schema.table, MySQL uses database.table

        Args:
            topic_prefix: Connector topic prefix (e.g., 'client_1_db_2')
            database_name: Source database name (e.g., 'kbbio')
            table_names: List of table names to snapshot (e.g., ['users', 'orders'])
            schema_name: PostgreSQL schema name (default: 'public', ignored for MySQL)
            db_type: Database type ('postgresql' or 'mysql')
            signal_id: Optional unique signal ID (auto-generated if not provided)

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            if not table_names:
                return False, "No tables specified for snapshot"

            # Generate signal ID if not provided
            if not signal_id:
                signal_id = f"snapshot-{int(time.time())}"

            # Signal topic follows pattern: {topic_prefix}.signals
            signal_topic = f"{topic_prefix}.signals"

            # ================================================================
            # CRITICAL FIX: Format data collections based on database type
            # ================================================================
            if db_type.lower() == 'postgresql':
                # PostgreSQL: Use schema.table format
                # Example: "public.users", "public.orders"
                data_collections = [f"{schema_name}.{table}" for table in table_names]
            elif db_type.lower() == 'mssql':
                # SQL Server: Use database.schema.table format
                # Example: "AppDB.dbo.Orders", "AppDB.dbo.Customers"
                data_collections = [f"{database_name}.{schema_name}.{table}" for table in table_names]
            else:
                # MySQL: Use database.table format
                # Example: "mydb.users", "mydb.orders"
                data_collections = [f"{database_name}.{table}" for table in table_names]

            # Build signal payload
            # IMPORTANT: The 'id' field must be in BOTH the key AND the value
            # Debezium will ignore signals that don't have 'id' in the value
            signal_payload = {
                "id": signal_id,
                "type": "execute-snapshot",
                "data": {
                    "data-collections": data_collections,
                    "type": "incremental"
                }
            }

            # Signal key must be the connector's topic prefix (server name)
            # so Debezium can identify which connector should process this signal
            signal_key = topic_prefix

            logger.info(f"📡 Sending incremental snapshot signal:")
            logger.info(f"   Signal ID: {signal_id}")
            logger.info(f"   Topic: {signal_topic}")
            logger.info(f"   Database Type: {db_type}")
            logger.info(f"   Schema/Database: {schema_name if db_type.lower() == 'postgresql' else database_name}")
            logger.info(f"   Tables: {table_names}")
            logger.info(f"   Data collections: {data_collections}")
            logger.info(f"   Signal payload: {json.dumps(signal_payload, indent=2)}")

            # Send signal to Kafka
            self.producer.produce(
                topic=signal_topic,
                key=signal_key.encode('utf-8'),
                value=json.dumps(signal_payload).encode('utf-8'),
                callback=self._delivery_callback
            )

            # Wait for message to be delivered
            self.producer.flush(timeout=10)

            message = (
                f"Incremental snapshot signal sent for {len(table_names)} table(s). "
                f"Debezium will snapshot these tables in the background."
            )
            logger.info(f"✅ {message}")

            return True, message

        except Exception as e:
            error_msg = f"Failed to send incremental snapshot signal: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return False, error_msg

    def send_stop_snapshot_signal(
        self,
        topic_prefix: str,
        signal_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send signal to stop ongoing snapshot

        Args:
            topic_prefix: Connector topic prefix
            signal_id: Optional unique signal ID

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            if not signal_id:
                signal_id = f"stop-snapshot-{int(time.time())}"

            signal_topic = f"{topic_prefix}.signals"

            # IMPORTANT: The 'id' field must be in BOTH the key AND the value
            signal_payload = {
                "id": signal_id,
                "type": "stop-snapshot",
                "data": {}
            }

            # Signal key must be the topic prefix (connector's server name)
            signal_key = topic_prefix

            logger.info(f"📡 Sending stop snapshot signal to {signal_topic}")

            self.producer.produce(
                topic=signal_topic,
                key=signal_key.encode('utf-8'),
                value=json.dumps(signal_payload).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.flush(timeout=10)

            return True, "Stop snapshot signal sent successfully"

        except Exception as e:
            error_msg = f"Failed to send stop snapshot signal: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return False, error_msg

    def send_pause_signal(
        self,
        topic_prefix: str,
        signal_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send signal to pause incremental snapshot

        Args:
            topic_prefix: Connector topic prefix
            signal_id: Optional unique signal ID

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            if not signal_id:
                signal_id = f"pause-{int(time.time())}"

            signal_topic = f"{topic_prefix}.signals"

            # IMPORTANT: The 'id' field must be in BOTH the key AND the value
            signal_payload = {
                "id": signal_id,
                "type": "pause-snapshot",
                "data": {}
            }

            # Signal key must be the topic prefix (connector's server name)
            signal_key = topic_prefix

            logger.info(f"📡 Sending pause snapshot signal to {signal_topic}")

            self.producer.produce(
                topic=signal_topic,
                key=signal_key.encode('utf-8'),
                value=json.dumps(signal_payload).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.flush(timeout=10)

            return True, "Pause snapshot signal sent successfully"

        except Exception as e:
            error_msg = f"Failed to send pause signal: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return False, error_msg

    def send_resume_signal(
        self,
        topic_prefix: str,
        signal_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send signal to resume paused incremental snapshot

        Args:
            topic_prefix: Connector topic prefix
            signal_id: Optional unique signal ID

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            if not signal_id:
                signal_id = f"resume-{int(time.time())}"

            signal_topic = f"{topic_prefix}.signals"

            # IMPORTANT: The 'id' field must be in BOTH the key AND the value
            signal_payload = {
                "id": signal_id,
                "type": "resume-snapshot",
                "data": {}
            }

            # Signal key must be the topic prefix (connector's server name)
            signal_key = topic_prefix

            logger.info(f"📡 Sending resume snapshot signal to {signal_topic}")

            self.producer.produce(
                topic=signal_topic,
                key=signal_key.encode('utf-8'),
                value=json.dumps(signal_payload).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.flush(timeout=10)

            return True, "Resume snapshot signal sent successfully"

        except Exception as e:
            error_msg = f"Failed to send resume signal: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return False, error_msg

    def close(self):
        """Close the Kafka producer"""
        try:
            self.producer.flush()
            logger.info("Kafka signal sender closed")
        except Exception as e:
            logger.error(f"Error closing Kafka signal sender: {e}")