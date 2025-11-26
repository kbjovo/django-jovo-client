"""
Kafka Consumer for Debezium CDC Events (multi-topic + table prefixing)

"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from sqlalchemy import MetaData, Table, Column, inspect, text
from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time
from sqlalchemy.dialects.mysql import DECIMAL, BIGINT, TINYINT, insert
from sqlalchemy.exc import SQLAlchemyError
import re

logger = logging.getLogger(__name__)


class KafkaConsumerException(Exception):
    pass


class DebeziumCDCConsumer:
    def __init__(
        self,
        consumer_group_id: str,
        topics: List[str] | str,
        target_engine,
        replication_config,
        bootstrap_servers: str = "localhost:9092",
        auto_offset_reset: str = "earliest",
    ):
        """
        Initialize CDC Consumer.

        Args:
            consumer_group_id: Kafka consumer group id
            topics: list or single topic to subscribe to
            target_engine: SQLAlchemy Engine connected to the centralized DB
            replication_config: ReplicationConfig model instance
            bootstrap_servers: kafka bootstrap servers
            auto_offset_reset: 'earliest' or 'latest'
        """
        self.consumer_group_id = consumer_group_id
        self.requested_topics = (topics if isinstance(topics, (list, tuple)) else [topics])
        self.target_engine = target_engine
        self.replication_config = replication_config
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset

        # kafka consumer config for Avro
        self.consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,  # manual commit after processing
            "schema.registry.url": "http://schema-registry:8081",  # Avro Schema Registry
        }

        logger.info(f"[DEBUG] Kafka Consumer Config: {json.dumps(self.consumer_config, indent=2)}")

            # Debezium Kafka creates "-value" topics containing the actual CDC messages
        self.topics = self.requested_topics

        try:
            self.consumer = AvroConsumer(self.consumer_config)
            logger.info(f"[DEBUG] Initializing AvroConsumer with group.id={self.consumer_group_id} and topics={self.topics}")

            self.consumer.subscribe(self.topics)

            logger.info(f"📡 Subscribed to CDC topics: {', '.join(self.topics)}")
            # logger.info(f"[DEBUG] Subscription done. Current subscription: {self.consumer.subscription()}")

        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise KafkaConsumerException(str(e))

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
    # Helper to check existing topics
    # -------------------------
    def _get_existing_topics(self) -> List[str]:
        """Get list of existing topics from Kafka cluster"""
        try:
            # Use admin client to get topic metadata
            from confluent_kafka.admin import AdminClient
            admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})

            # Get cluster metadata (timeout in seconds)
            metadata = admin_client.list_topics(timeout=5)

            # Extract topic names
            existing_topics = list(metadata.topics.keys())
            logger.debug(f"Found {len(existing_topics)} existing topics in Kafka")

            return existing_topics
        except Exception as e:
            logger.warning(f"Could not fetch existing topics: {e}. Proceeding with subscription anyway.")
            return []

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
                elif isinstance(value, datetime):
                    # If value is already a datetime object (after conversion)
                    col_type = DateTime
                elif isinstance(value, date):
                    # If value is a date object (not datetime)
                    col_type = Date
                elif isinstance(value, int):
                    # Check if this might be a timestamp field that will be converted
                    if (field_name.endswith(('_at', '_time', 'date', 'timestamp')) and
                        value > 1000000000000):
                        col_type = DateTime
                    else:
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

    def get_table_mapping(self, source_db: str, source_table: str) -> Optional[Any]:
        """
        Look up TableMapping for the given source table.

        Args:
            source_db: Source database name
            source_table: Source table name

        Returns:
            TableMapping instance or None if not found
        """
        try:
            from client.models import TableMapping

            # Look up TableMapping by source_table in the current replication_config
            table_mapping = TableMapping.objects.filter(
                replication_config=self.replication_config,
                source_table=source_table,
                is_enabled=True
            ).first()

            if not table_mapping:
                logger.warning(
                    f"   ⚠️  No TableMapping found for {source_db}.{source_table} "
                    f"in ReplicationConfig {self.replication_config.id}. Skipping."
                )
                return None

            logger.debug(f"   ✓ Found TableMapping: {table_mapping}")
            return table_mapping

        except Exception as e:
            logger.error(f"   ❌ Error looking up TableMapping: {e}")
            return None

    def convert_debezium_timestamps(self, row: dict, table: Table = None) -> dict:
        """
        Convert Debezium timestamp/date fields based on actual table schema.

        Args:
            row: Dictionary of column names to values
            table: SQLAlchemy Table object to determine column types (optional)
                   If None, uses heuristic-based detection for initial table creation

        Handles:
        - Unix timestamps (seconds or milliseconds)
        - ISO datetime strings
        - Weird integers like 20238
        - Mixed date/datetime fields

        Returns: dict with date/datetime fields converted to proper Python objects
        """
        if not row:
            return row

        # Determine which columns are date/time columns
        # Strategy 1: Use table schema if available (most reliable)
        # Strategy 2: Use heuristics if table is None (for initial table creation)
        date_time_columns = {}  # col_name -> col_type

        if table is not None:
            # Schema-based: Get actual column types from table
            for col in table.columns:
                col_type = type(col.type).__name__
                if col_type in ('Date', 'DateTime', 'DATETIME', 'TIMESTAMP', 'Time', 'DATE', 'TIME'):
                    date_time_columns[col.name] = col_type

        converted = {}

        for key, value in row.items():
            # Skip if value is None
            if value is None:
                converted[key] = None
                continue

            # Determine if this is a date/time column
            if table is not None:
                # Use schema-based detection
                if key not in date_time_columns:
                    converted[key] = value
                    continue
                col_type = date_time_columns[key]
            else:
                # Use heuristic-based detection for table creation
                # Check common date/time column naming patterns
                if not any([
                    key.endswith('_at'),
                    key.endswith('_time'),
                    key.endswith('_date'),
                    key.endswith('_from'),
                    key.endswith('_to'),
                    'date' in key.lower(),
                    'time' in key.lower(),
                    'timestamp' in key.lower(),
                    key in ['created', 'updated', 'modified', 'deleted']
                ]):
                    converted[key] = value
                    continue
                col_type = None  # Unknown, will use smart default

            # Now we know this column needs date/time conversion
            try:
                parsed = None

                # ---- Case 1: Already datetime/date ----
                if isinstance(value, (datetime, date)):
                    parsed = value

                # ---- Case 2: Integer timestamp or malformed numeric ----
                elif isinstance(value, int):
                    if value > 1000000000000:  # milliseconds
                        parsed = datetime.fromtimestamp(value / 1000.0)
                    elif value > 1000000000:  # seconds
                        parsed = datetime.fromtimestamp(value)
                    else:
                        # Handle short numeric dates (YYYYMMDD, YYYYMM, etc.)
                        s = str(value)
                        try:
                            if len(s) == 8:  # YYYYMMDD
                                parsed = datetime.strptime(s, "%Y%m%d")
                            elif len(s) == 6:  # YYYYMM - treat as first day of month
                                parsed = datetime.strptime(s + "01", "%Y%m%d")
                            elif len(s) == 5:  # 20238 -> 2023-08-01
                                year = int(s[:4])
                                month = int(s[4:])
                                if 1 <= month <= 12:
                                    parsed = datetime(year, month, 1)
                                else:
                                    logger.warning(f"Invalid month in {key}={value}, defaulting to None")
                                    converted[key] = None
                                    continue
                            elif len(s) == 4:  # just a year - first day of year
                                parsed = datetime(int(s), 1, 1)
                            else:
                                logger.warning(f"Cannot parse short numeric date {key}={value}, setting to None")
                                converted[key] = None
                                continue
                        except (ValueError, OverflowError) as e:
                            logger.warning(f"Failed to interpret numeric date {key}={value}: {e}, setting to None")
                            converted[key] = None
                            continue

                # ---- Case 3: String timestamp/date ----
                elif isinstance(value, str):
                    v = value.strip()
                    for fmt in (
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y/%m/%d %H:%M:%S",
                        "%Y-%m-%d",
                        "%d-%m-%Y",
                        "%Y/%m/%d",
                        "%Y%m%d"
                    ):
                        try:
                            parsed = datetime.strptime(v, fmt)
                            break
                        except ValueError:
                            continue

                # ---- Final Conversion ----
                if parsed:
                    # Convert based on column type (if known from schema) or use smart default (if heuristic)
                    # col_type is already set from the detection logic above

                    if col_type == 'Date' or col_type == 'DATE':
                        # DATE column - return date object
                        if isinstance(parsed, datetime):
                            converted[key] = parsed.date()
                        else:
                            converted[key] = parsed
                    elif col_type in ('DateTime', 'DATETIME', 'TIMESTAMP'):
                        # DATETIME/TIMESTAMP column - return datetime object
                        if isinstance(parsed, date) and not isinstance(parsed, datetime):
                            # Convert date to datetime (midnight)
                            converted[key] = datetime.combine(parsed, datetime.min.time())
                        else:
                            converted[key] = parsed
                    elif col_type in ('Time', 'TIME'):
                        # TIME column - extract time
                        if isinstance(parsed, datetime):
                            converted[key] = parsed.time()
                        else:
                            converted[key] = parsed
                    else:
                        # Unknown type (heuristic mode), use smart default
                        if isinstance(parsed, datetime):
                            if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
                                converted[key] = parsed.date()
                            else:
                                converted[key] = parsed
                        else:
                            converted[key] = parsed
                else:
                    # Could not parse - set to None to avoid MySQL errors
                    logger.warning(f"Could not parse {key}={value}, setting to None")
                    converted[key] = None

            except Exception as e:
                logger.error(f"Unexpected error converting {key}={value}: {e}, setting to None")
                converted[key] = None

        return converted



    # -------------------------
    # Apply operations
    # -------------------------
    def apply_insert(self, target_table, row_data, table_mapping):
        """
        Apply INSERT operation with column name mapping from ColumnMapping.

        Args:
            target_table: SQLAlchemy Table object
            row_data: Dict with source column names
            table_mapping: TableMapping model instance
        """
        try:
            logger.info(f"   🔹 Starting INSERT operation for table: {target_table.name}")
            logger.info(f"   🔹 Original row data: {json.dumps(row_data, default=str)[:300]}...")

            # MAP SOURCE COLUMNS TO TARGET COLUMNS using ColumnMapping FIRST
            # The mapping function includes fallback logic and filtering
            mapped_data = self._map_columns_from_db(row_data, table_mapping, target_table)

            logger.info(f"   🔹 Original data: {list(row_data.keys())}")
            logger.info(f"   🔹 Mapped data (with fallback): {list(mapped_data.keys())}")

            # CRITICAL: Convert timestamps AFTER mapping (so column names match target schema)
            mapped_data = self.convert_debezium_timestamps(mapped_data, target_table)
            logger.info(f"   🔹 After timestamp conversion: {json.dumps(mapped_data, default=str)[:300]}...")

            # Use INSERT ... ON DUPLICATE KEY UPDATE
            stmt = insert(target_table).values(**mapped_data)

            # Add ON DUPLICATE KEY UPDATE for all non-PK columns
            pk_columns = self._get_primary_keys(target_table)
            update_dict = {
                col: stmt.inserted[col]
                for col in mapped_data.keys()
                if col not in pk_columns
            }
            
            if update_dict:
                stmt = stmt.on_duplicate_key_update(**update_dict)
            
            # Execute
            with self.target_engine.begin() as conn:
                conn.execute(stmt)

            self.stats["inserts"] += 1
            logger.info(f"   ✅ INSERT successful for {target_table.name}")

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"   ❌ FAILED to insert into {target_table.name}: {e}")
            logger.exception(f"   ❌ Full exception:")
            raise


    def _map_columns_from_db(self, row_data, table_mapping, target_table):
        """
        Map source column names to target column names using ColumnMapping table.

        Args:
            row_data: Dict with source column names as keys
            table_mapping: TableMapping instance
            target_table: SQLAlchemy Table object for target table

        Returns:
            Dict with target column names as keys (validated against target table)
        """
        from client.models import ColumnMapping

        # Get all column mappings for this table
        column_mappings = ColumnMapping.objects.filter(
            table_mapping=table_mapping,
            is_enabled=True
        ).values('source_column', 'target_column')

        # Build mapping dict: {source_column: target_column}
        column_map = {
            cm['source_column']: cm['target_column']
            for cm in column_mappings
        }

        # Get target table columns
        target_columns = {col.name for col in target_table.columns}

        # Map the data with fallback logic
        mapped_data = {}
        for source_col, value in row_data.items():
            # Get mapped name from ColumnMapping
            mapped_col = column_map.get(source_col, source_col)

            # If mapped column exists in target table, use it
            if mapped_col in target_columns:
                mapped_data[mapped_col] = value
            # Otherwise, fallback to source column name if it exists in target
            elif source_col in target_columns:
                logger.warning(
                    f"   ⚠️  Mapped column '{mapped_col}' not found in target table, "
                    f"falling back to source column '{source_col}'"
                )
                mapped_data[source_col] = value
            else:
                # Neither mapped nor source column exists in target
                logger.debug(
                    f"   ⚠️  Column '{source_col}' (mapped to '{mapped_col}') "
                    f"not found in target table, skipping"
                )

        return mapped_data


    def _get_primary_keys(self, table):
        """Get primary key column names for a table."""
        return {col.name for col in table.primary_key.columns}

    
    def apply_update(self, target_table, row_data, table_mapping):
        try:
            logger.info(f"   🔹 Starting UPDATE operation for table: {target_table.name}")
            logger.info(f"   🔹 Original row data: {json.dumps(row_data, default=str)[:300]}...")

            # MAP COLUMNS with fallback logic FIRST
            mapped_data = self._map_columns_from_db(row_data, table_mapping, target_table)

            # CRITICAL: Convert timestamps AFTER mapping (so column names match target schema)
            mapped_data = self.convert_debezium_timestamps(mapped_data, target_table)
            logger.info(f"   🔹 After timestamp conversion: {json.dumps(mapped_data, default=str)[:300]}...")

            # Build UPDATE statement with primary key filter
            pk_columns = self._get_primary_keys(target_table)
            pk_values = {k: v for k, v in mapped_data.items() if k in pk_columns}
            update_values = {k: v for k, v in mapped_data.items() if k not in pk_columns}
            
            if not pk_values:
                raise ValueError("No primary key values found for UPDATE")
            
            stmt = target_table.update()
            for pk_col, pk_val in pk_values.items():
                stmt = stmt.where(target_table.c[pk_col] == pk_val)
            stmt = stmt.values(**update_values)
            
            with self.target_engine.begin() as conn:
                conn.execute(stmt)

            self.stats["updates"] += 1
            logger.info(f"   ✅ UPDATE successful for {target_table.name}")

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"   ❌ FAILED to update {target_table.name}: {e}")
            logger.exception(f"   ❌ Full exception:")
            raise

    def apply_delete(self, target_table, row_data, table_mapping):
        """Apply DELETE operation with column mapping."""
        try:
            logger.info(f"   🔹 Starting DELETE operation for table: {target_table.name}")
            logger.info(f"   🔹 Original row data: {json.dumps(row_data, default=str)[:300]}...")

            # MAP COLUMNS for primary key with fallback logic FIRST
            mapped_data = self._map_columns_from_db(row_data, table_mapping, target_table)

            # CRITICAL: Convert timestamps AFTER mapping (so column names match target schema)
            mapped_data = self.convert_debezium_timestamps(mapped_data, target_table)
            logger.info(f"   🔹 After timestamp conversion: {json.dumps(mapped_data, default=str)[:300]}...")
            
            # Get primary key columns and values
            pk_columns = self._get_primary_keys(target_table)
            pk_values = {k: v for k, v in mapped_data.items() if k in pk_columns}
            
            if not pk_values:
                raise ValueError("No primary key values found for DELETE")
            
            stmt = target_table.delete()
            for pk_col, pk_val in pk_values.items():
                stmt = stmt.where(target_table.c[pk_col] == pk_val)
            
            with self.target_engine.begin() as conn:
                conn.execute(stmt)

            self.stats["deletes"] += 1
            logger.info(f"   ✅ DELETE successful for {target_table.name}")

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"   ❌ FAILED to delete from {target_table.name}: {e}")
            logger.exception(f"   ❌ Full exception:")
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
            partition = msg.partition()
            offset = msg.offset()

            logger.info("=" * 80)
            logger.info(f"📨 NEW MESSAGE RECEIVED")
            logger.info(f"   Topic: {topic}")
            logger.info(f"   Partition: {partition}")
            logger.info(f"   Offset: {offset}")
            logger.info("=" * 80)

            # AvroConsumer automatically deserializes Avro messages to Python dicts
            message_value = msg.value()
            if message_value is None:
                logger.warning(f"⚠️  Empty message received on topic {topic}")
                return

            logger.debug(f"📄 Avro-deserialized message: {json.dumps(message_value, indent=2, default=str)[:500]}...")

            op, before, after, payload_source_db = self.parse_debezium_payload(message_value)

            logger.info(f"🔍 Parsed CDC Event:")
            logger.info(f"   Operation: {op}")
            logger.info(f"   Source DB: {payload_source_db}")
            logger.info(f"   Has 'before': {before is not None}")
            logger.info(f"   Has 'after': {after is not None}")

            # determine source_db & table (prefer payload source, fallback to topic)
            topic_db, topic_table = self.extract_table_from_topic(topic)
            source_db = payload_source_db or topic_db or "unknown_source"
            table_name = topic_table or "unknown_table"

            logger.info(f"📊 Table Info:")
            logger.info(f"   Source DB: {source_db}")
            logger.info(f"   Source Table: {table_name}")

            # Look up TableMapping for this source table
            table_mapping = self.get_table_mapping(source_db, table_name)
            if not table_mapping:
                logger.warning(f"   ⚠️  Skipping message - no TableMapping found for {source_db}.{table_name}")
                return

            # Get target table name from TableMapping
            target_table_name = table_mapping.target_table
            logger.info(f"   Target Table: {target_table_name}")

            # ensure table exists (use after|before to infer)
            # Convert timestamps in sample data for proper type inference
            sample = self.convert_debezium_timestamps(after or before or {})
            if target_table_name not in self.table_cache:
                # attempt to load existing table or create from sample
                try:
                    self.ensure_table_exists(target_table_name, sample)
                    # re-load Table instance into cache if created by ensure_table_exists
                    if target_table_name not in self.table_cache:
                        # attempt autoload
                        t = Table(target_table_name, self.metadata, autoload_with=self.target_engine)
                        self.table_cache[target_table_name] = t
                except Exception as e:
                    logger.exception(f"Failed to prepare table {target_table_name}: {e}")
                    return  # skip this message

            # Get SQLAlchemy Table object from cache
            target_table_obj = self.table_cache.get(target_table_name)
            if target_table_obj is None:
                logger.error(f"   ❌ Table object not in cache for {target_table_name}")
                return

            # handle operations
            logger.info(f"🔧 Processing Operation: {op}")

            if op in ("c", "r"):  # create or snapshot read
                if after:
                    logger.info(f"➕ INSERT operation detected")
                    logger.info(f"   Data to insert: {json.dumps(after, default=str)[:200]}...")
                    # Debezium 'after' is a dict of column->value
                    self.apply_insert(target_table_obj, after, table_mapping)
                else:
                    logger.warning(f"⚠️  CREATE/READ operation but no 'after' data")
            elif op == "u":
                logger.info(f"✏️  UPDATE operation detected")
                logger.info(f"   Before: {json.dumps(before, default=str)[:150] if before else 'None'}...")
                logger.info(f"   After: {json.dumps(after, default=str)[:150] if after else 'None'}...")
                # update
                self.apply_update(target_table_obj, after or {}, table_mapping)
            elif op == "d":
                logger.info(f"🗑️  DELETE operation detected")
                logger.info(f"   Before: {json.dumps(before, default=str)[:200] if before else 'None'}...")
                # delete
                self.apply_delete(target_table_obj, before or {}, table_mapping)
            else:
                logger.warning(f"⚠️  Unknown operation '{op}'")
                # Unknown op: could still be message with payload structure; treat as insert if 'after' present
                if after:
                    logger.info(f"   Treating as INSERT since 'after' data is present")
                    self.apply_insert(target_table_obj, after, table_mapping)
                else:
                    logger.warning(f"   No 'after' data - skipping message")

            # update stats
            self.stats["messages_processed"] += 1
            self.stats["last_message_time"] = datetime.utcnow()

            logger.info(f"✅ Message processed successfully")
            logger.info(f"📈 Stats: {self.stats['messages_processed']} messages, {self.stats['inserts']} inserts, {self.stats['updates']} updates, {self.stats['deletes']} deletes, {self.stats['errors']} errors")
            logger.info("=" * 80)

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

            # Track last check time to avoid DB queries on every message
            import time
            last_config_check = time.time()
            config_check_interval = 2  # Check every 2 seconds

            while True:
                if max_messages is not None and messages_count >= max_messages:
                    break

                # Periodically check if config still exists and is active
                current_time = time.time()
                if current_time - last_config_check >= config_check_interval:
                    try:
                        from client.models import ReplicationConfig
                        # Refresh config from DB
                        config = ReplicationConfig.objects.filter(
                            id=self.replication_config.id
                        ).first()

                        if not config:
                            logger.warning(f"🛑 ReplicationConfig {self.replication_config.id} was deleted. Stopping consumer.")
                            break
                        elif not config.is_active:
                            logger.warning(f"🛑 ReplicationConfig {self.replication_config.id} is no longer active. Stopping consumer.")
                            break

                        last_config_check = current_time
                    except Exception as e:
                        logger.error(f"Error checking config status: {e}")
                        # Don't break on check error, continue consuming
                        last_config_check = current_time

                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    # no message received
                    continue

                if msg.error():
                    # handle error or partition EOF
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached: {msg.topic()}:{msg.partition()}")
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Topic doesn't exist yet - this is expected when no CDC events have occurred
                        logger.debug(f"⚠️  Topic not available yet (waiting for first CDC event): {msg.error()}")
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