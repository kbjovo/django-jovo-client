"""
Kafka Signal Sender for Debezium Incremental Snapshots

Sends control signals to Debezium connectors via Kafka topics or database signal tables.
Supports incremental snapshots for MySQL, PostgreSQL, and SQL Server.

For SQL Server, supports dual-channel signaling:
1. Kafka topic (standard approach)
2. Database signal table (more reliable for MS SQL)
"""

import json
import logging
from typing import List, Tuple, Optional
from confluent_kafka import Producer
import uuid
from datetime import datetime
from sqlalchemy import text

logger = logging.getLogger(__name__)


class KafkaSignalSender:
    """
    Send signals to Debezium connectors for incremental snapshots.
    
    Incremental snapshots allow adding new tables to replication without
    restarting the connector or re-snapshotting existing tables.
    """
    
    def __init__(self, bootstrap_servers: str = 'kafka:29092'):
        """
        Initialize Kafka producer for sending signals.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        self.bootstrap_servers = bootstrap_servers
        
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'debezium-signal-sender'
        }
        
        try:
            self.producer = Producer(self.producer_config)
            logger.info(f"✅ Kafka signal sender initialized: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
    
    def send_incremental_snapshot_signal(
        self,
        topic_prefix: str,
        database_name: str,
        table_names: List[str],
        schema_name: str = None,
        db_type: str = 'mysql'
    ) -> Tuple[bool, str]:
        """
        Send incremental snapshot signal to Debezium connector.
        
        CRITICAL: Signal key MUST match connector's topic.prefix configuration!
        
        This triggers a snapshot of specific tables without affecting
        ongoing CDC streaming of other tables.
        
        Args:
            topic_prefix: Topic prefix (e.g., 'client_1_db_3') - MUST match connector config
            database_name: Source database name
            table_names: List of table names to snapshot
            schema_name: Schema name (PostgreSQL/Oracle)
            db_type: Database type ('mysql', 'postgresql', 'sqlserver', 'oracle')
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            signal_topic = f"{topic_prefix}.signals"
            
            # ✅ CRITICAL FIX: Use topic_prefix as signal key
            # Debezium signal matching uses topic.prefix (NOT database.server.name!)
            # See: https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/pipeline/signal/channels/KafkaSignalChannel.java
            
            signal_id = str(uuid.uuid4())  # For tracking in logs
            
            # Signal key MUST match the connector's topic.prefix configuration
            signal_key = topic_prefix  # This is what Debezium checks against!
            
            logger.info(f"🔑 Signal key will be: {signal_key} (matches topic.prefix)")
            
            # Build table identifiers based on database type
            table_identifiers = self._build_table_identifiers(
                database_name,
                table_names,
                schema_name,
                db_type
            )
            
            if not table_identifiers:
                return False, "No valid table identifiers generated"
            
            # Build signal payload
            signal_payload = {
                "id": signal_id,
                "type": "execute-snapshot",
                "data": {
                    "data-collections": table_identifiers,
                    "type": "incremental"
                }
            }
            
            logger.info(f"📡 Sending incremental snapshot signal:")
            logger.info(f"   Signal ID: {signal_id}")
            logger.info(f"   Signal Key: {signal_key} (must match topic.prefix)")
            logger.info(f"   Topic: {signal_topic}")
            logger.info(f"   Database: {database_name} ({db_type.upper()})")
            logger.info(f"   Tables: {len(table_names)}")
            logger.info(f"   Identifiers: {table_identifiers}")
            
            # Send signal to Kafka
            self.producer.produce(
                topic=signal_topic,
                key=signal_key.encode('utf-8'),  # ✅ CRITICAL: Use server name, not UUID!
                value=json.dumps(signal_payload).encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Wait for message delivery
            self.producer.flush(timeout=10)
            
            logger.info(f"✅ Incremental snapshot signal sent successfully!")
            logger.info(f"   Signal ID: {signal_id}")
            logger.info(f"   Tables: {', '.join(table_identifiers)}")
            
            return True, f"Incremental snapshot initiated for {len(table_names)} table(s)"
            
        except Exception as e:
            error_msg = f"Failed to send incremental snapshot signal: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return False, error_msg
    
    def send_incremental_snapshot_signal_via_db(
        self,
        engine,
        database_name: str,
        table_names: List[str],
        schema_name: str = 'dbo',
        signal_table: str = 'dbo.debezium_signal'
    ) -> Tuple[bool, str]:
        """
        Send incremental snapshot signal via SQL Server signal table.

        This is the RECOMMENDED approach for MS SQL Server as it's more reliable
        than Kafka topic signals. Debezium monitors both channels, but database
        signals are processed more consistently for SQL Server.

        Args:
            engine: SQLAlchemy engine for the source database
            database_name: Source database name
            table_names: List of table names to snapshot
            schema_name: Schema name (default: 'dbo')
            signal_table: Signal table name (default: 'dbo.debezium_signal')

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            signal_id = str(uuid.uuid4())

            # Build table identifiers for MS SQL: database.schema.table
            table_identifiers = []
            for table in table_names:
                parts = table.split('.')
                if len(parts) == 3:
                    # Already full path
                    identifier = table
                elif len(parts) == 2:
                    # schema.table format
                    identifier = f"{database_name}.{table}"
                else:
                    # Just table name
                    identifier = f"{database_name}.{schema_name}.{table}"
                table_identifiers.append(identifier)

            # Build signal data JSON
            signal_data = {
                "data-collections": table_identifiers,
                "type": "INCREMENTAL"
            }

            signal_data_json = json.dumps(signal_data)

            logger.info(f"📡 Sending incremental snapshot signal via DATABASE:")
            logger.info(f"   Database: {database_name} (SQLSERVER)")
            logger.info(f"   Signal Table: {signal_table}")
            logger.info(f"   Identifiers: {table_identifiers}")

            # Insert signal into database signal table
            with engine.connect() as conn:
                sql = text(f"""
                    INSERT INTO {signal_table} (id, type, data)
                    VALUES (:signal_id, 'execute-snapshot', :signal_data)
                """)

                conn.execute(sql, {
                    'signal_id': signal_id,
                    'signal_data': signal_data_json
                })
                conn.commit()

            logger.info(f"✅ Incremental snapshot signal sent successfully!")
            logger.info(f"   Signal ID: {signal_id}")
            logger.info(f"   Tables: {', '.join(table_identifiers)}")
            logger.info(f"   Method: DATABASE TABLE (recommended for MS SQL)")

            return True, f"Incremental snapshot initiated for {len(table_names)} table(s) via database signal"

        except Exception as e:
            error_msg = f"Failed to send incremental snapshot signal via database: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return False, error_msg

    def _build_table_identifiers(
        self,
        database_name: str,
        table_names: List[str],
        schema_name: str,
        db_type: str
    ) -> List[str]:
        """
        Build database-specific table identifiers for signal payload.
        
        Different databases use different naming conventions:
        - MySQL:      <database>.<table>
        - PostgreSQL: <schema>.<table>
        - SQL Server: <database>.<schema>.<table>
        - Oracle:     <schema>.<table>
        
        Args:
            database_name: Database name
            table_names: List of table names
            schema_name: Schema name (for PostgreSQL/Oracle)
            db_type: Database type
        
        Returns:
            List[str]: Formatted table identifiers
        """
        identifiers = []
        
        for table in table_names:
            if db_type.lower() == 'mysql':
                # MySQL format: database.table
                if '.' in table:
                    identifier = table  # Already formatted
                else:
                    identifier = f"{database_name}.{table}"
            
            elif db_type.lower() == 'postgresql':
                # PostgreSQL format: schema.table
                schema = schema_name or 'public'
                if '.' in table:
                    identifier = table  # Already has schema
                else:
                    identifier = f"{schema}.{table}"
            
            elif db_type.lower() in ['sqlserver', 'mssql']:
                # ✅ SQL Server format: database.schema.table (3-part!)
                # CRITICAL: Must match Debezium's internal schema registry format
                # Even though table.include.list uses "dbo.Products" (2-part),
                # Debezium's internal schema stores "AppDB.dbo.Products" (3-part)
                # Signal payload MUST use 3-part format for schema lookup to succeed
                #
                # Input might be:
                # - 'AppDB.dbo.Orders' (full path) → use as-is
                # - 'dbo.Orders' (schema.table) → add database prefix
                # - 'Orders' (just table) → add database and schema prefix

                parts = table.split('.')

                if len(parts) == 3:
                    # Already full path: database.schema.table - perfect!
                    identifier = table
                elif len(parts) == 2:
                    # schema.table format → add database prefix
                    identifier = f"{database_name}.{table}"
                else:
                    # Just table name → add database and schema prefix
                    schema = schema_name or 'dbo'
                    identifier = f"{database_name}.{schema}.{table}"
            
            elif db_type.lower() == 'oracle':
                # Oracle format: schema.table
                schema = schema_name or database_name.upper()
                if '.' in table:
                    identifier = table
                else:
                    identifier = f"{schema}.{table}"
            
            else:
                # Generic fallback
                identifier = f"{database_name}.{table}"
            
            identifiers.append(identifier)
            logger.debug(f"   {table} → {identifier}")
        
        return identifiers
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"❌ Signal delivery failed: {err}")
        else:
            logger.debug(f"✅ Signal delivered to {msg.topic()} [{msg.partition()}]")
    
    def send_pause_signal(self, topic_prefix: str) -> Tuple[bool, str]:
        """
        Send pause signal to connector.
        
        Args:
            topic_prefix: Topic prefix
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            signal_topic = f"{topic_prefix}.signals"
            signal_id = str(uuid.uuid4())
            
            signal_payload = {
                "id": signal_id,
                "type": "pause",
                "data": {}
            }
            
            logger.info(f"⏸️ Sending pause signal to {signal_topic}")
            
            self.producer.produce(
                topic=signal_topic,
                key=signal_id.encode('utf-8'),
                value=json.dumps(signal_payload).encode('utf-8')
            )
            
            self.producer.flush(timeout=10)
            
            return True, "Pause signal sent"
            
        except Exception as e:
            return False, f"Failed to send pause signal: {str(e)}"
    
    def send_resume_signal(self, topic_prefix: str) -> Tuple[bool, str]:
        """
        Send resume signal to connector.
        
        Args:
            topic_prefix: Topic prefix
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            signal_topic = f"{topic_prefix}.signals"
            signal_id = str(uuid.uuid4())
            
            signal_payload = {
                "id": signal_id,
                "type": "resume",
                "data": {}
            }
            
            logger.info(f"▶️ Sending resume signal to {signal_topic}")
            
            self.producer.produce(
                topic=signal_topic,
                key=signal_id.encode('utf-8'),
                value=json.dumps(signal_payload).encode('utf-8')
            )
            
            self.producer.flush(timeout=10)
            
            return True, "Resume signal sent"
            
        except Exception as e:
            return False, f"Failed to send resume signal: {str(e)}"
    
    def close(self):
        """Close the Kafka producer."""
        try:
            if hasattr(self, 'producer'):
                self.producer.flush()
                logger.info("✅ Kafka signal sender closed")
        except Exception as e:
            logger.warning(f"Error closing producer: {e}")