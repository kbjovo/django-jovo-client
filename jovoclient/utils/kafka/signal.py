import uuid
import json
import logging
from typing import List, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)

class DebeziumSignalManager:
    """
    Manages Debezium signals for MySQL, Postgres, SQL Server, and Oracle.
    Uses GUID-based signaling to trigger connector actions via the source DB.

    NOTE: This requires WRITE access to the source database.
    Use KafkaSignalManager for read-only databases.
    """

    def __init__(self, db_engine: Engine, signal_table: str = "inventory.debezium_signal"):
        """
        Args:
            db_engine: SQLAlchemy engine for the SOURCE database.
            signal_table: Fully qualified name (schema.table) of the signal table.
        """
        self.engine = db_engine
        self.signal_table = signal_table

    def _send_signal(self, signal_type: str, data: dict) -> str:
        """
        Generic method to insert a signal into the database.

        Args:
            signal_type: The Debezium command (e.g., 'execute-snapshot')
            data: The parameters for that command

        Returns:
            str: The GUID (ID) of the signal sent.
        """
        # Generate a unique GUID for this signal
        signal_id = str(uuid.uuid4())

        # SQL insert using the standard Debezium 3-column structure
        query = text(f"""
            INSERT INTO {self.signal_table} (id, type, data)
            VALUES (:id, :type, :data)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, {
                    "id": signal_id,
                    "type": signal_type,
                    "data": json.dumps(data)
                })
            logger.info(f"Signal '{signal_type}' sent successfully with GUID: {signal_id}")
            return signal_id
        except Exception as e:
            logger.error(f"Failed to send Debezium signal: {e}")
            raise

    def trigger_adhoc_snapshot(self, tables: List[str]):
        """
        Triggers an incremental snapshot for specific tables.
        This allows re-syncing data without locking the database.
        """
        # data-collections format: ["schema.table1", "schema.table2"]
        payload = {
            "type": "incremental",
            "data-collections": tables
        }
        return self._send_signal("execute-snapshot", payload)

    def pause_incremental_snapshot(self):
        """Pauses any currently running incremental snapshots."""
        return self._send_signal("pause-snapshot-window", {})

    def resume_incremental_snapshot(self):
        """Resumes a paused incremental snapshot."""
        return self._send_signal("resume-snapshot-window", {})

    def log_marker(self, message: str):
        """Injects a custom string marker into the Kafka partition."""
        return self._send_signal("log", {"message": message})


class KafkaSignalManager:
    """
    Manages Debezium signals via Kafka topics (no database write access required).

    This is ideal for read-only source databases where the user lacks write permissions.
    Signals are sent to a Kafka topic that Debezium connectors listen to.

    Usage:
        signal_manager = KafkaSignalManager(
            bootstrap_servers="localhost:9092",
            signal_topic="client_1_db_2.signals",
            connector_name="client_1_db_2"
        )
        signal_id = signal_manager.trigger_adhoc_snapshot(["schema.table1", "schema.table2"])
    """

    def __init__(self, bootstrap_servers: str, signal_topic: str, connector_name: str):
        """
        Args:
            bootstrap_servers: Kafka bootstrap servers (e.g., 'kafka-1:29092,kafka-2:29092')
            signal_topic: Signal topic name (e.g., 'client_1_db_2.signals')
            connector_name: Debezium connector name (e.g., 'client_1_db_2' or 'test_client_mysql_connector_connector_v_1')
        """
        from confluent_kafka import Producer

        self.bootstrap_servers = bootstrap_servers
        self.signal_topic = signal_topic
        self.connector_name = connector_name

        # Create Kafka producer
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'debezium-signal-manager',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,
        })

        logger.info(f"Initialized KafkaSignalManager for connector: {connector_name}")
        logger.info(f"  Signal topic: {signal_topic}")

    def _send_signal(self, signal_type: str, data: dict, additional_data: dict = None) -> str:
        """
        Send a signal to Debezium via Kafka topic.

        Args:
            signal_type: The Debezium command (e.g., 'execute-snapshot')
            data: The parameters for that command
            additional_data: Optional additional fields for the signal message

        Returns:
            str: The GUID (ID) of the signal sent.
        """
        # Generate a unique GUID for this signal
        signal_id = str(uuid.uuid4())

        # Create signal message in Debezium format
        # The message contains id, type, and data fields
        # IMPORTANT: The 'data' field must be a JSON object, NOT a JSON string
        # See: https://debezium.io/documentation/reference/stable/configuration/signalling.html
        signal_message = {
            "id": signal_id,
            "type": signal_type,
            "data": data  # data is already a dict, will be serialized with the whole message
        }

        # Add any additional data fields
        if additional_data:
            signal_message.update(additional_data)

        try:
            # Send message to Kafka signal topic
            # CRITICAL: For Kafka signals, Debezium expects:
            # - Key: The topic.prefix value (e.g., "client_1_db_2") - this is how Debezium routes signals
            # - Value: JSON with 'id', 'type' and 'data' fields
            # - The 'data' field must be a JSON object (NOT a string)
            self.producer.produce(
                topic=self.signal_topic,
                key=self.connector_name.encode('utf-8'),  # ✅ Use connector name as key
                value=json.dumps(signal_message).encode('utf-8'),
                callback=self._delivery_callback
            )
            logger.info(f"signal-msg= topic: {self.signal_topic}, \
                        key={self.connector_name.encode('utf-8')}, value={json.dumps(signal_message).encode('utf-8')},")

            # Wait for message to be delivered
            self.producer.flush(timeout=10)

            logger.info(f"✅ Kafka signal '{signal_type}' sent successfully")
            logger.info(f"   Connector: {self.connector_name}")
            logger.info(f"   Signal ID: {signal_id}")
            logger.info(f"   Topic: {self.signal_topic}")
            logger.info(f"   Data: {data}")

            return signal_id

        except Exception as e:
            logger.error(f"❌ Failed to send Kafka signal: {e}")
            raise

    def _delivery_callback(self, err, msg):
        """Callback for Kafka message delivery confirmation."""
        if err:
            logger.error(f"❌ Signal delivery failed: {err}")
        else:
            logger.debug(f"✅ Signal delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def trigger_adhoc_snapshot(self, tables: List[str]) -> str:
        """
        Trigger an incremental snapshot for specific tables via Kafka signal.

        This allows adding new tables to a running connector without restart.
        Works even when the user has NO WRITE ACCESS to the source database.

        Args:
            tables: List of fully-qualified table names (e.g., ["schema.table1", "schema.table2"])

        Returns:
            str: The signal ID (GUID)
        """
        # data-collections format: ["schema.table1", "schema.table2"]
        payload = {
            "type": "incremental",
            "data-collections": tables
        }
        return self._send_signal("execute-snapshot", payload)

    def pause_incremental_snapshot(self) -> str:
        """Pause any currently running incremental snapshots."""
        return self._send_signal("pause-snapshot", {})

    def resume_incremental_snapshot(self) -> str:
        """Resume a paused incremental snapshot."""
        return self._send_signal("resume-snapshot", {})

    def log_marker(self, message: str) -> str:
        """Inject a custom string marker into the Kafka stream."""
        return self._send_signal("log", {"message": message})

    def stop_signal(self, data_collections: Optional[List[str]] = None) -> str:
        """
        Stop an incremental snapshot for specific tables or all tables.

        Args:
            data_collections: Optional list of table names to stop. If None, stops all.

        Returns:
            str: The signal ID (GUID)
        """
        payload = {}
        if data_collections:
            payload["data-collections"] = data_collections
        return self._send_signal("stop-snapshot", payload)

    def close(self):
        """Close the Kafka producer connection."""
        if hasattr(self, 'producer'):
            self.producer.flush()
            logger.info("Kafka producer closed")


def send_incremental_snapshot_signal(database, replication_config, tables: List[str]) -> tuple:
    """
    Send incremental snapshot signal via appropriate method based on db_type.

    Abstracts the db-type-specific signaling logic:
    - PostgreSQL/MS SQL/Oracle: Use DebeziumSignalManager (database-based)
    - MySQL: Use KafkaSignalManager (Kafka-based)

    Args:
        database: ClientDatabase instance
        replication_config: ReplicationConfig instance
        tables: List of table names (will be formatted based on db_type)

    Returns:
        tuple: (signal_id, method_used)  # method_used: 'database' or 'kafka'

    Raises:
        Exception: If signal sending fails
    """
    from django.conf import settings
    from client.utils.database_utils import get_database_engine
    from client.views.cdc_views import format_table_for_connector

    db_type = database.db_type.lower()
    client = database.client

    # Format tables for signal based on db_type
    formatted_tables = []
    for table_name in tables:
        # Get schema from table mapping if available
        table_mapping = replication_config.table_mappings.filter(
            source_table=table_name.split('.')[-1]  # Handle pre-formatted names
        ).first()
        schema_name = table_mapping.source_schema if table_mapping else None

        # MS SQL needs full database.schema.table format for signals
        if db_type == 'mssql':
            if '.' not in table_name:
                schema = schema_name or 'dbo'
                formatted_tables.append(f"{database.database_name}.{schema}.{table_name}")
            elif table_name.count('.') == 1:
                formatted_tables.append(f"{database.database_name}.{table_name}")
            else:
                formatted_tables.append(table_name)
        else:
            formatted_tables.append(format_table_for_connector(database, table_name, schema_name))

    logger.info(f"Sending incremental snapshot signal for {len(formatted_tables)} tables")
    logger.debug(f"Formatted tables: {formatted_tables}")

    # Database-based signaling (PostgreSQL, MS SQL, Oracle)
    if db_type in ['postgresql', 'mssql', 'oracle']:
        db_engine = get_database_engine(database)

        # Signal table names by db_type
        signal_tables = {
            'postgresql': 'public.debezium_signal',
            'mssql': 'dbo.debezium_signal',
            'oracle': f'{database.username.upper()}.DEBEZIUM_SIGNAL',
        }
        signal_table = signal_tables.get(db_type, 'debezium_signal')

        signal_manager = DebeziumSignalManager(
            db_engine=db_engine,
            signal_table=signal_table
        )

        signal_id = signal_manager.trigger_adhoc_snapshot(formatted_tables)
        logger.info(f"Database signal sent - ID: {signal_id}, table: {signal_table}")

        return signal_id, 'database'

    # Kafka-based signaling (MySQL)
    else:
        kafka_bootstrap = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            settings.DEBEZIUM_CONFIG.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:29092,kafka-2:29092,kafka-3:29092')
        )

        # Signal topic format: client_{client_id}_db_{db_id}.signals
        signal_topic = f"client_{client.id}_db_{database.id}.signals"
        topic_prefix = f"client_{client.id}_db_{database.id}"

        signal_manager = KafkaSignalManager(
            bootstrap_servers=kafka_bootstrap,
            signal_topic=signal_topic,
            connector_name=topic_prefix
        )

        signal_id = signal_manager.trigger_adhoc_snapshot(formatted_tables)
        signal_manager.close()

        logger.info(f"Kafka signal sent - ID: {signal_id}, topic: {signal_topic}")

        return signal_id, 'kafka'

