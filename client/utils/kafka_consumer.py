# """
# Kafka Consumer for Debezium CDC Events
# Reads change events and writes to target database
# """

# import json
# import logging
# from typing import Dict, List, Optional, Any, Tuple
# from datetime import datetime
# from confluent_kafka import Consumer, KafkaError, KafkaException
# from sqlalchemy import create_engine, MetaData, Table, Column, inspect
# from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time
# from sqlalchemy.dialects.mysql import DECIMAL, BIGINT, TINYINT
# from sqlalchemy.exc import SQLAlchemyError
# from django.conf import settings

# from .database_utils import get_database_engine, DatabaseConnectionError
# from .notification_utils import log_and_notify_error, send_error_notification

# logger = logging.getLogger(__name__)


# class KafkaConsumerException(Exception):
#     """Base exception for Kafka consumer operations"""
#     pass


# class DebeziumCDCConsumer:
#     """
#     Kafka Consumer for Debezium CDC events
    
#     Consumes change events from Debezium topics and applies them to target database
#     """
    
#     def __init__(
#         self,
#         consumer_group_id: str,
#         topics: List[str],
#         target_engine,
#         bootstrap_servers: str = 'localhost:9092',
#         auto_offset_reset: str = 'earliest',
#     ):
#         """
#         Initialize CDC Consumer
        
#         Args:
#             consumer_group_id: Kafka consumer group ID
#             topics: List of topics to subscribe to
#             target_engine: SQLAlchemy engine for target database
#             bootstrap_servers: Kafka bootstrap servers
#             auto_offset_reset: Where to start reading (earliest/latest)
#         """
#         self.consumer_group_id = consumer_group_id
#         self.topics = topics
#         self.target_engine = target_engine
#         self.bootstrap_servers = bootstrap_servers
        
#         # Kafka consumer configuration
#         self.consumer_config = {
#             'bootstrap.servers': bootstrap_servers,
#             'group.id': consumer_group_id,
#             'auto.offset.reset': auto_offset_reset,
#             'enable.auto.commit': False,  # Manual commit for reliability
#             'max.poll.interval.ms': 300000,  # 5 minutes
#             'session.timeout.ms': 10000,
#             'heartbeat.interval.ms': 3000,
#         }
        
#         # Initialize consumer
#         self.consumer = None
#         self.metadata = MetaData()
#         self.table_cache = {}  # Cache SQLAlchemy table objects
        
#         # Statistics
#         self.stats = {
#             'messages_processed': 0,
#             'inserts': 0,
#             'updates': 0,
#             'deletes': 0,
#             'errors': 0,
#             'last_message_time': None,
#         }
        
#         logger.info(f"CDC Consumer initialized for group: {consumer_group_id}")
    
#     def connect(self):
#         """Connect to Kafka and subscribe to topics"""
#         try:
#             self.consumer = Consumer(self.consumer_config)
#             self.consumer.subscribe(self.topics)
#             logger.info(f"Subscribed to topics: {', '.join(self.topics)}")
#         except Exception as e:
#             error_msg = f"Failed to connect to Kafka: {str(e)}"
#             logger.error(error_msg)
#             raise KafkaConsumerException(error_msg) from e
    
#     def disconnect(self):
#         """Disconnect from Kafka"""
#         if self.consumer:
#             self.consumer.close()
#             logger.info("Kafka consumer disconnected")
    
#     def parse_debezium_message(self, message_value: Dict) -> Tuple[str, Optional[Dict], Optional[Dict]]:
#         """
#         Parse Debezium CDC message
        
#         Args:
#             message_value: Debezium message payload
            
#         Returns:
#             Tuple[str, Optional[Dict], Optional[Dict]]: (operation, before_data, after_data)
            
#         Operations:
#             'c' = create (INSERT)
#             'u' = update (UPDATE)
#             'd' = delete (DELETE)
#             'r' = read (initial snapshot)
#         """
#         operation = message_value.get('op', 'unknown')
#         before = message_value.get('before')
#         after = message_value.get('after')
        
#         return operation, before, after
    
#     def extract_table_name(self, topic: str) -> str:
#         """
#         Extract table name from Kafka topic
        
#         Topic format: {prefix}.{database}.{table}
#         Example: client_1.kbe.users -> users
#         """
#         parts = topic.split('.')
#         if len(parts) >= 3:
#             return parts[-1]  # Last part is table name
#         return topic
    
#     def map_debezium_type_to_sqlalchemy(self, debezium_type: str) -> Any:
#         """
#         Map Debezium/MySQL types to SQLAlchemy types
        
#         Args:
#             debezium_type: Debezium field type
            
#         Returns:
#             SQLAlchemy column type
#         """
#         type_mapping = {
#             # Integer types
#             'int8': TINYINT,
#             'int16': Integer,
#             'int32': Integer,
#             'int64': BIGINT,
            
#             # String types
#             'string': String(255),
#             'bytes': Text,
            
#             # Float types
#             'float': Float,
#             'double': Float,
            
#             # Decimal
#             'decimal': DECIMAL(10, 2),
            
#             # Boolean
#             'boolean': Boolean,
            
#             # Date/Time
#             'date': Date,
#             'time': Time,
#             'timestamp': DateTime,
#             'datetime': DateTime,
#         }
        
#         return type_mapping.get(debezium_type.lower(), String(255))
    
#     def create_table_from_schema(self, table_name: str, schema: Dict) -> Table:
#         """
#         Create SQLAlchemy table object from Debezium schema
        
#         Args:
#             table_name: Name of the table
#             schema: Debezium schema definition
            
#         Returns:
#             SQLAlchemy Table object
#         """
#         try:
#             # Check if table exists in cache
#             if table_name in self.table_cache:
#                 return self.table_cache[table_name]
            
#             # Extract fields from schema
#             fields = schema.get('fields', [])
            
#             # Build columns
#             columns = []
#             for field in fields:
#                 field_name = field.get('field')
#                 field_type = field.get('type')
#                 optional = field.get('optional', True)
                
#                 # Skip if field is inside 'before' or 'after' nested structure
#                 if field_name in ['before', 'after', 'source', 'op', 'ts_ms']:
#                     continue
                
#                 # Map type
#                 col_type = self.map_debezium_type_to_sqlalchemy(field_type)
                
#                 # Create column
#                 col = Column(
#                     field_name,
#                     col_type,
#                     nullable=optional,
#                     primary_key=(field_name == 'id')  # Assume 'id' is primary key
#                 )
#                 columns.append(col)
            
#             if not columns:
#                 # Fallback: create columns from first message data
#                 logger.warning(f"No columns found in schema for {table_name}, will infer from data")
#                 return None
            
#             # Create table object
#             table = Table(table_name, self.metadata, *columns, extend_existing=True)
            
#             # Cache it
#             self.table_cache[table_name] = table
            
#             logger.info(f"Created table schema for {table_name} with {len(columns)} columns")
#             return table
            
#         except Exception as e:
#             logger.error(f"Failed to create table schema for {table_name}: {str(e)}")
#             return None
    
#     def infer_table_from_data(self, table_name: str, data: Dict) -> Table:
#         """
#         Infer table structure from actual data
        
#         Args:
#             table_name: Name of the table
#             data: Sample data dictionary
            
#         Returns:
#             SQLAlchemy Table object
#         """
#         try:
#             if table_name in self.table_cache:
#                 return self.table_cache[table_name]
            
#             columns = []
#             for field_name, value in data.items():
#                 # Infer type from value
#                 if isinstance(value, bool):
#                     col_type = Boolean
#                 elif isinstance(value, int):
#                     col_type = Integer if abs(value) < 2147483647 else BIGINT
#                 elif isinstance(value, float):
#                     col_type = Float
#                 elif isinstance(value, (datetime, str)) and field_name.endswith('_at'):
#                     col_type = DateTime
#                 else:
#                     col_type = String(255)
                
#                 col = Column(
#                     field_name,
#                     col_type,
#                     nullable=True,
#                     primary_key=(field_name == 'id')
#                 )
#                 columns.append(col)
            
#             table = Table(table_name, self.metadata, *columns, extend_existing=True)
#             self.table_cache[table_name] = table
            
#             logger.info(f"Inferred table schema for {table_name} from data")
#             return table
            
#         except Exception as e:
#             logger.error(f"Failed to infer table schema for {table_name}: {str(e)}")
#             return None
    
#     def ensure_table_exists(self, table_name: str, sample_data: Dict):
#         """
#         Ensure table exists in target database, create if not
        
#         Args:
#             table_name: Name of the table
#             sample_data: Sample data to infer schema
#         """
#         try:
#             # Check if table exists
#             inspector = inspect(self.target_engine)
#             if table_name in inspector.get_table_names():
#                 logger.debug(f"Table {table_name} already exists")
#                 return
            
#             # Create table from data
#             table = self.infer_table_from_data(table_name, sample_data)
#             if table is not None:
#                 table.create(self.target_engine, checkfirst=True)
#                 logger.info(f"Created table {table_name} in target database")
            
#         except Exception as e:
#             logger.error(f"Failed to ensure table exists {table_name}: {str(e)}")
#             raise
    
#     def apply_insert(self, table_name: str, data: Dict):
#         """
#         Apply INSERT operation to target database
        
#         Args:
#             table_name: Name of the table
#             data: Data to insert
#         """
#         try:
#             # Ensure table exists
#             self.ensure_table_exists(table_name, data)
            
#             # Get or create table object
#             if table_name not in self.table_cache:
#                 self.infer_table_from_data(table_name, data)
            
#             table = self.table_cache.get(table_name)
#             if table is None:
#                 raise Exception(f"Failed to get table object for {table_name}")
            
#             # Execute insert
#             with self.target_engine.connect() as conn:
#                 conn.execute(table.insert().values(**data))
#                 conn.commit()
            
#             self.stats['inserts'] += 1
#             logger.debug(f"Inserted row into {table_name}: {data.get('id', 'unknown')}")
            
#         except Exception as e:
#             self.stats['errors'] += 1
#             logger.error(f"Failed to insert into {table_name}: {str(e)}")
#             raise
    
#     def apply_update(self, table_name: str, before: Dict, after: Dict):
#         """
#         Apply UPDATE operation to target database
        
#         Args:
#             table_name: Name of the table
#             before: Data before update
#             after: Data after update
#         """
#         try:
#             if table_name not in self.table_cache:
#                 self.ensure_table_exists(table_name, after)
#                 self.infer_table_from_data(table_name, after)
            
#             table = self.table_cache.get(table_name)
#             if table is None:
#                 raise Exception(f"Failed to get table object for {table_name}")
            
#             # Use 'id' as primary key for WHERE clause
#             pk_value = after.get('id') or before.get('id')
#             if not pk_value:
#                 logger.warning(f"No primary key found for update in {table_name}")
#                 return
            
#             # Execute update
#             with self.target_engine.connect() as conn:
#                 stmt = table.update().where(table.c.id == pk_value).values(**after)
#                 conn.execute(stmt)
#                 conn.commit()
            
#             self.stats['updates'] += 1
#             logger.debug(f"Updated row in {table_name}: {pk_value}")
            
#         except Exception as e:
#             self.stats['errors'] += 1
#             logger.error(f"Failed to update {table_name}: {str(e)}")
#             raise
    
#     def apply_delete(self, table_name: str, data: Dict):
#         """
#         Apply DELETE operation to target database
        
#         Args:
#             table_name: Name of the table
#             data: Data before deletion (contains PK)
#         """
#         try:
#             if table_name not in self.table_cache:
#                 # Table might not exist yet, skip delete
#                 logger.warning(f"Table {table_name} not found for delete operation")
#                 return
            
#             table = self.table_cache.get(table_name)
#             if table is None:
#                 return
            
#             # Get primary key
#             pk_value = data.get('id')
#             if not pk_value:
#                 logger.warning(f"No primary key found for delete in {table_name}")
#                 return
            
#             # Execute delete
#             with self.target_engine.connect() as conn:
#                 stmt = table.delete().where(table.c.id == pk_value)
#                 conn.execute(stmt)
#                 conn.commit()
            
#             self.stats['deletes'] += 1
#             logger.debug(f"Deleted row from {table_name}: {pk_value}")
            
#         except Exception as e:
#             self.stats['errors'] += 1
#             logger.error(f"Failed to delete from {table_name}: {str(e)}")
#             raise
    
#     def process_message(self, message):
#         """
#         Process a single Kafka message
        
#         Args:
#             message: Kafka message object
#         """
#         try:
#             # Parse message
#             topic = message.topic()
#             value = message.value()
            
#             if not value:
#                 return
            
#             # Parse JSON
#             try:
#                 message_data = json.loads(value.decode('utf-8'))
#             except json.JSONDecodeError as e:
#                 logger.error(f"Failed to parse JSON from {topic}: {str(e)}")
#                 return
            
#             # Extract table name
#             table_name = self.extract_table_name(topic)
            
#             # Parse Debezium message
#             operation, before, after = self.parse_debezium_message(message_data)
            
#             # Apply operation
#             if operation in ['c', 'r']:  # Create or Read (snapshot)
#                 if after:
#                     self.apply_insert(table_name, after)
#             elif operation == 'u':  # Update
#                 if after:
#                     self.apply_update(table_name, before or {}, after)
#             elif operation == 'd':  # Delete
#                 if before:
#                     self.apply_delete(table_name, before)
#             else:
#                 logger.warning(f"Unknown operation: {operation} for {table_name}")
            
#             # Update stats
#             self.stats['messages_processed'] += 1
#             self.stats['last_message_time'] = datetime.now()
            
#         except Exception as e:
#             self.stats['errors'] += 1
#             logger.error(f"Error processing message from {message.topic()}: {str(e)}")
#             # Don't raise - continue processing other messages
    
#     def consume(self, max_messages: Optional[int] = None, timeout: float = 1.0):
#         """
#         Start consuming messages
        
#         Args:
#             max_messages: Maximum messages to process (None = infinite)
#             timeout: Poll timeout in seconds
#         """
#         try:
#             if not self.consumer:
#                 self.connect()
            
#             messages_count = 0
            
#             logger.info(f"Starting consumption from topics: {', '.join(self.topics)}")
            
#             while True:
#                 # Check if we've hit max messages
#                 if max_messages and messages_count >= max_messages:
#                     break
                
#                 # Poll for message
#                 msg = self.consumer.poll(timeout=timeout)
                
#                 if msg is None:
#                     continue
                
#                 if msg.error():
#                     if msg.error().code() == KafkaError._PARTITION_EOF:
#                         logger.debug(f"End of partition reached: {msg.topic()}")
#                     else:
#                         logger.error(f"Kafka error: {msg.error()}")
#                     continue
                
#                 # Process message
#                 self.process_message(msg)
#                 messages_count += 1
                
#                 # Commit offset after successful processing
#                 self.consumer.commit(asynchronous=False)
                
#                 # Log progress every 100 messages
#                 if messages_count % 100 == 0:
#                     logger.info(f"Processed {messages_count} messages. Stats: {self.stats}")
            
#         except KeyboardInterrupt:
#             logger.info("Consumption interrupted by user")
#         except Exception as e:
#             logger.error(f"Error during consumption: {str(e)}")
#             raise
#         finally:
#             self.disconnect()
    
#     def get_stats(self) -> Dict[str, Any]:
#         """Get consumption statistics"""
#         return self.stats.copy()


"""
Kafka Consumer for Debezium CDC Events (multi-topic + table prefixing)

Place this at: client/utils/kafka_consumer.py

Behavior:
- Accepts 'topics' (list or single topic).
- Subscribes to topics via confluent_kafka.Consumer.
- Writes events to a centralized target_engine (SQLAlchemy Engine).
- Prefixes target table names with source DB name to avoid collisions:
    source_db.table  ->  source_db_table
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import MetaData, Table, Column, inspect, text
from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time
from sqlalchemy.dialects.mysql import DECIMAL, BIGINT, TINYINT
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class KafkaConsumerException(Exception):
    pass


class DebeziumCDCConsumer:
    def __init__(
        self,
        consumer_group_id: str,
        topics: List[str] | str,
        target_engine,
        bootstrap_servers: str = "localhost:9092",
        auto_offset_reset: str = "earliest",
    ):
        """
        Initialize CDC Consumer.

        Args:
            consumer_group_id: Kafka consumer group id
            topics: list or single topic to subscribe to
            target_engine: SQLAlchemy Engine connected to the centralized DB
            bootstrap_servers: kafka bootstrap servers
            auto_offset_reset: 'earliest' or 'latest'
        """
        self.consumer_group_id = consumer_group_id
        self.topics = topics if isinstance(topics, (list, tuple)) else [topics]
        self.target_engine = target_engine
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset

        # kafka consumer config
        self.consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,  # manual commit after processing
        }

        # initialize
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe(self.topics)
            logger.info(f"Subscribed to topics: {', '.join(self.topics)}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise KafkaConsumerException(str(e)) from e

        # SQLAlchemy metadata & caches
        self.metadata = MetaData()
        self.table_cache: Dict[str, Table] = {}  # keyed by fully prefixed table name

        # stats
        self.stats = {
            "messages_processed": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
            "last_message_time": None,
        }

    # -------------------------
    # Type mapping helper
    # -------------------------
    def map_debezium_type_to_sqlalchemy(self, debezium_type: str) -> Any:
        """Map Debezium field type strings to SQLAlchemy types (best-effort)."""
        if not debezium_type:
            return String(255)

        t = str(debezium_type).lower()
        mapping = {
            "int8": TINYINT,
            "int16": Integer,
            "int32": Integer,
            "int64": BIGINT,
            "string": String(255),
            "varchar": String(255),
            "char": String(255),
            "text": Text,
            "bytes": Text,
            "float": Float,
            "double": Float,
            "decimal": DECIMAL(18, 4),
            "boolean": Boolean,
            "bool": Boolean,
            "date": Date,
            "time": Time,
            "timestamp": DateTime,
            "datetime": DateTime,
        }
        return mapping.get(t, String(255))

    # -------------------------
    # Table creation / inference
    # -------------------------
    def _prefixed_table_name(self, source_db: str, table_name: str) -> str:
        """Return a safe prefixed table name used in central DB (source_db_table)."""
        # sanitize names - keep alnum and underscores
        def clean(s: str) -> str:
            return "".join(c if (c.isalnum() or c == "_") else "_" for c in (s or "")).lower()

        return f"{clean(source_db)}_{clean(table_name)}"

    def infer_table_from_row(self, prefixed_table: str, sample_row: Dict[str, Any]) -> Optional[Table]:
        """
        Create a SQLAlchemy Table object based on sample_row.
        Does not persist schema changes beyond creating the Table object (create() will persist).
        """
        try:
            if prefixed_table in self.table_cache:
                return self.table_cache[prefixed_table]

            cols = []
            for field_name, value in (sample_row or {}).items():
                # skip None keys
                if not field_name:
                    continue

                # infer type
                if isinstance(value, bool):
                    col_type = Boolean
                elif isinstance(value, int):
                    # choose BIGINT vs Integer conservatively
                    col_type = Integer if abs(value) < 2_000_000_000 else BIGINT
                elif isinstance(value, float):
                    col_type = Float
                elif isinstance(value, str):
                    # attempt datetime inference by pattern (basic)
                    if field_name.endswith("_at") or field_name.endswith("_ts"):
                        col_type = DateTime
                    else:
                        col_type = String(255)
                else:
                    col_type = String(255)

                col = Column(field_name, col_type, nullable=True, primary_key=(field_name == "id"))
                cols.append(col)

            if not cols:
                logger.warning(f"No columns inferred for {prefixed_table}")
                return None

            table = Table(prefixed_table, self.metadata, *cols, extend_existing=True)
            self.table_cache[prefixed_table] = table
            return table
        except Exception as e:
            logger.exception(f"Failed to infer table {prefixed_table}: {e}")
            return None

    def ensure_table_exists(self, prefixed_table: str, sample_row: Dict[str, Any]):
        """
        Ensure the table exists in the target database. Create if not present.
        prefixed_table: name already prefixed with source db.
        sample_row: dictionary of a row to infer schema if needed.
        """
        try:
            inspector = inspect(self.target_engine)
            if prefixed_table in inspector.get_table_names():
                # load Table into cache so SQLAlchemy can reference columns
                t = Table(prefixed_table, self.metadata, autoload_with=self.target_engine)
                self.table_cache[prefixed_table] = t
                return

            # infer schema and create
            table = self.infer_table_from_row(prefixed_table, sample_row)
            if table is None:
                raise Exception("Could not infer table schema")

            # create on DB
            table.create(self.target_engine, checkfirst=True)
            logger.info(f"Created table {prefixed_table} in target DB")
        except Exception as e:
            logger.exception(f"Failed to ensure table {prefixed_table} exists: {e}")
            raise

    # -------------------------
    # Message parsing helpers
    # -------------------------
    def parse_debezium_payload(self, message_value: Dict[str, Any]) -> Tuple[str, Optional[Dict], Optional[Dict], Optional[str]]:
        """
        Given the message value (decoded JSON payload), return:
        (op, before, after, source_db_name)
        op: 'c'/'u'/'d'/'r' etc.
        """
        # Debezium envelope typically has top-level 'payload' (if using Kafka Connect REST pickup)
        payload = message_value.get("payload") if isinstance(message_value, dict) else message_value

        if payload is None:
            # sometimes message_value itself is the payload
            payload = message_value

        # standard debezium fields:
        op = payload.get("op")
        before = payload.get("before")
        after = payload.get("after")

        # source DB may be in payload['source'] -> {'db': 'dbname'} or 'schema' depending on connector
        source_section = payload.get("source", {}) or {}
        source_db = source_section.get("db") or source_section.get("database") or source_section.get("schema") or source_section.get("schema_name")

        # fallback: sometimes message_value may include keys directly
        return op, before, after, source_db

    def extract_table_from_topic(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Given topic like 'client_3.new_project_db.users' -> return (new_project_db, users)
        """
        parts = topic.split(".")
        if len(parts) >= 3:
            return parts[1], parts[2]
        # fallback
        return None, parts[-1] if parts else None

    # -------------------------
    # Apply operations
    # -------------------------
    def apply_insert(self, prefixed_table: str, row: Dict[str, Any]):
        try:
            with self.target_engine.connect() as conn:
                table = self.table_cache.get(prefixed_table)
                if table is None:
                    raise Exception(f"Table object not found for {prefixed_table}")

                stmt = table.insert().values(**row)
                conn.execute(stmt)
                conn.commit()
            self.stats["inserts"] += 1
            logger.debug(f"Inserted into {prefixed_table}")
        except Exception as e:
            self.stats["errors"] += 1
            logger.exception(f"Failed to insert into {prefixed_table}: {e}")
            raise

    def apply_update(self, prefixed_table: str, before: Dict[str, Any], after: Dict[str, Any]):
        try:
            with self.target_engine.connect() as conn:
                table = self.table_cache.get(prefixed_table)
                if table is None:
                    raise Exception(f"Table object not found for {prefixed_table}")

                # prefer pk 'id' if available
                pk_val = (after or {}).get("id") or (before or {}).get("id")
                if pk_val is None:
                    # if no id, try to use all primary key columns (not implemented) => skip
                    logger.warning(f"No PK for update on {prefixed_table}, skipping")
                    return

                stmt = table.update().where(table.c.id == pk_val).values(**(after or {}))
                conn.execute(stmt)
                conn.commit()
            self.stats["updates"] += 1
            logger.debug(f"Updated row in {prefixed_table}")
        except Exception as e:
            self.stats["errors"] += 1
            logger.exception(f"Failed to update {prefixed_table}: {e}")
            raise

    def apply_delete(self, prefixed_table: str, before: Dict[str, Any]):
        try:
            with self.target_engine.connect() as conn:
                table = self.table_cache.get(prefixed_table)
                if table is None:
                    logger.warning(f"Table {prefixed_table} not found for delete - skipping")
                    return

                pk_val = (before or {}).get("id")
                if pk_val is None:
                    logger.warning(f"No PK for delete on {prefixed_table}, skipping")
                    return

                stmt = table.delete().where(table.c.id == pk_val)
                conn.execute(stmt)
                conn.commit()
            self.stats["deletes"] += 1
            logger.debug(f"Deleted row from {prefixed_table}")
        except Exception as e:
            self.stats["errors"] += 1
            logger.exception(f"Failed to delete from {prefixed_table}: {e}")
            raise

    # -------------------------
    # Message processing
    # -------------------------
    def process_message(self, msg):
        """
        Process a confluent_kafka.Message object
        """
        try:
            topic = msg.topic()
            raw_value = msg.value()
            if raw_value is None:
                return

            # raw_value may already be bytes/string/json depending on producer config
            if isinstance(raw_value, (bytes, str)):
                try:
                    message_value = json.loads(raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value)
                except Exception:
                    # not JSON -> skip
                    logger.warning(f"Non-JSON message on topic {topic} - skipping")
                    return
            else:
                message_value = raw_value

            op, before, after, payload_source_db = self.parse_debezium_payload(message_value)

            # determine source_db & table (prefer payload source, fallback to topic)
            topic_db, topic_table = self.extract_table_from_topic(topic)
            source_db = payload_source_db or topic_db or "unknown_source"
            table_name = topic_table or "unknown_table"

            prefixed_table = self._prefixed_table_name(source_db, table_name)

            # ensure table exists (use after|before to infer)
            sample = after or before or {}
            if prefixed_table not in self.table_cache:
                # attempt to load existing table or create from sample
                try:
                    self.ensure_table_exists(prefixed_table, sample)
                    # re-load Table instance into cache if created by ensure_table_exists
                    if prefixed_table not in self.table_cache:
                        # attempt autoload
                        t = Table(prefixed_table, self.metadata, autoload_with=self.target_engine)
                        self.table_cache[prefixed_table] = t
                except Exception as e:
                    logger.exception(f"Failed to prepare table {prefixed_table}: {e}")
                    return  # skip this message

            # handle operations
            if op in ("c", "r"):  # create or snapshot read
                if after:
                    # Debezium 'after' is a dict of column->value
                    self.apply_insert(prefixed_table, after)
            elif op == "u":
                # update
                self.apply_update(prefixed_table, before or {}, after or {})
            elif op == "d":
                # delete
                self.apply_delete(prefixed_table, before or {})
            else:
                # Unknown op: could still be message with payload structure; treat as insert if 'after' present
                if after:
                    self.apply_insert(prefixed_table, after)
                else:
                    logger.debug(f"Unknown op '{op}' on topic {topic} - skipping")

            # update stats
            self.stats["messages_processed"] += 1
            self.stats["last_message_time"] = datetime.utcnow()

        except Exception as e:
            self.stats["errors"] += 1
            logger.exception(f"Error processing message from {msg.topic()}: {e}")
            # do not raise - continue processing

    # -------------------------
    # Consume loop
    # -------------------------
    def consume(self, max_messages: Optional[int] = None, timeout: float = 1.0):
        """
        Start consuming messages. Blocks until max_messages are processed (if set).
        """
        try:
            if not self.consumer:
                raise KafkaConsumerException("Consumer not initialized")

            messages_count = 0
            logger.info(f"Starting consumption from topics: {', '.join(self.topics)}")

            while True:
                if max_messages is not None and messages_count >= max_messages:
                    break

                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    # no message received
                    continue

                if msg.error():
                    # handle error or partition EOF
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached: {msg.topic()}:{msg.partition()}")
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                # process message
                self.process_message(msg)
                messages_count += 1

                # commit offset after processing
                try:
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.warning(f"Failed to commit offset: {e}")

            logger.info("Finished consumption loop.")
        except KeyboardInterrupt:
            logger.info("Consumption interrupted by user")
        except Exception:
            logger.exception("Error during consumption")
            raise
        finally:
            try:
                if self.consumer:
                    self.consumer.close()
            except Exception:
                pass

    def get_stats(self) -> Dict[str, Any]:
        """Return a copy of stats"""
        return dict(self.stats)
