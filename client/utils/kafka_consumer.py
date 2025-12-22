"""
Kafka Consumer for Debezium CDC Events (multi-topic + table prefixing)
✅ FIXED: Oracle NUMBER decoding + schema.table handling
✅ PRESERVED: MySQL, PostgreSQL, SQL Server functionality
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date, timedelta
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
            from confluent_kafka.admin import AdminClient
            admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            metadata = admin_client.list_topics(timeout=5)
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
                if not field_name:
                    continue

                # infer type
                if isinstance(value, bool):
                    col_type = Boolean
                elif isinstance(value, datetime):
                    col_type = DateTime
                elif isinstance(value, date):
                    col_type = Date
                elif isinstance(value, int):
                    if (field_name.endswith(('_at', '_time', 'date', 'timestamp')) and
                        value > 1000000000000):
                        col_type = DateTime
                    else:
                        col_type = Integer if abs(value) < 2_000_000_000 else BIGINT
                elif isinstance(value, float):
                    col_type = Float
                elif isinstance(value, str):
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
        """
        try:
            inspector = inspect(self.target_engine)
            if prefixed_table in inspector.get_table_names():
                t = Table(prefixed_table, self.metadata, autoload_with=self.target_engine)
                self.table_cache[prefixed_table] = t
                return

            table = self.infer_table_from_row(prefixed_table, sample_row)
            if table is None:
                raise Exception("Could not infer table schema")

            table.create(self.target_engine, checkfirst=True)
            logger.info(f"Created table {prefixed_table} in target DB")
        except Exception as e:
            logger.exception(f"Failed to ensure table {prefixed_table} exists: {e}")
            raise

    # -------------------------
    # ✅ NEW: Oracle NUMBER decoding
    # -------------------------
    def decode_oracle_number(self, value):
        """
        ✅ Decode Oracle NUMBER type from Debezium format.
        
        Oracle sends NUMBER as: {"scale": 0, "value": "b'\\x06'"}
        We need to extract and decode the actual numeric value.
        
        Args:
            value: The Oracle NUMBER value (dict or regular value)
        
        Returns:
            Decoded numeric value (int or float) or original value
        """
        # If not a dict, return as-is (already decoded or not Oracle NUMBER)
        if not isinstance(value, dict):
            return value
        
        # Check if this is Oracle NUMBER format
        if 'value' not in value:
            return value
        
        try:
            raw_value = value['value']
            scale = value.get('scale', 0)
            
            # Handle different formats
            if isinstance(raw_value, bytes):
                # Direct bytes
                decoded = self._decode_oracle_number_bytes(raw_value, scale)
            elif isinstance(raw_value, str):
                if raw_value.startswith("b'") or raw_value.startswith('b"'):
                    # String representation of bytes: "b'\\x06'"
                    import ast
                    try:
                        raw_value = ast.literal_eval(raw_value)
                        decoded = self._decode_oracle_number_bytes(raw_value, scale)
                    except:
                        # Failed to parse, try direct conversion
                        decoded = float(raw_value) if scale > 0 else int(float(raw_value))
                else:
                    # Might be a string number
                    decoded = float(raw_value) if scale > 0 else int(float(raw_value))
            elif isinstance(raw_value, (int, float)):
                # Already numeric
                decoded = raw_value
            else:
                logger.warning(f"Unknown Oracle NUMBER format: {value}")
                decoded = None
            
            logger.debug(f"✅ Decoded Oracle NUMBER: {value} → {decoded}")
            return decoded
            
        except Exception as e:
            logger.warning(f"Failed to decode Oracle NUMBER: {value}, error: {e}")
            return None

    def _decode_oracle_number_bytes(self, raw_bytes, scale):
        """
        Decode Oracle NUMBER from byte representation.
        
        Oracle uses a proprietary format for NUMBER storage.
        This is a simplified decoder that handles common cases.
        
        Args:
            raw_bytes: Raw bytes from Oracle
            scale: Decimal scale
        
        Returns:
            Decoded number (int or float)
        """
        if not raw_bytes:
            return None
        
        try:
            # For simple integers, Oracle often uses direct byte encoding
            # Simple case: Single byte for small integers (1-100)
            if len(raw_bytes) == 1:
                value = raw_bytes[0]
                # Oracle encodes small numbers with offset
                if 1 <= value <= 100:
                    return value
                # Adjust for Oracle's encoding (exponent-based)
                if value >= 193:  # Positive numbers
                    return value - 192
                elif value <= 62:  # Negative numbers
                    return 62 - value
            
            # Multi-byte encoding
            # Oracle uses base-100 representation
            # First byte is exponent, remaining are mantissa digits
            if len(raw_bytes) >= 2:
                first_byte = raw_bytes[0]
                
                # Positive numbers: first byte >= 193
                if first_byte >= 193:
                    exponent = first_byte - 193
                    mantissa_bytes = raw_bytes[1:]
                    
                    # Each byte encodes a base-100 digit (minus 1)
                    mantissa = 0
                    for i, byte_val in enumerate(mantissa_bytes):
                        digit = byte_val - 1
                        mantissa += digit * (100 ** (len(mantissa_bytes) - i - 1))
                    
                    value = mantissa * (100 ** exponent)
                    
                    if scale > 0:
                        return float(value) / (10 ** scale)
                    return int(value)
            
            # Fallback: Try to interpret as big-endian integer
            value = int.from_bytes(raw_bytes, byteorder='big')
            
            if scale > 0:
                return float(value) / (10 ** scale)
            return value
                
        except Exception as e:
            logger.warning(f"Failed to decode Oracle NUMBER bytes: {raw_bytes}, error: {e}")
            return None

    # -------------------------
    # ✅ NEW: Normalize row data (decode Oracle NUMBERs)
    # -------------------------
    def normalize_row_data(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize row data by decoding Oracle NUMBER fields.
        
        This must be called BEFORE timestamp conversion to ensure
        all values are in standard Python types.
        
        Args:
            row_data: Raw row data from Debezium
        
        Returns:
            Normalized row data with Oracle NUMBERs decoded
        """
        if not row_data:
            return row_data
        
        normalized = {}
        for key, value in row_data.items():
            # Decode Oracle NUMBER if needed
            normalized[key] = self.decode_oracle_number(value)
        
        return normalized

    # -------------------------
    # Message parsing helpers
    # -------------------------
    def parse_debezium_payload(self, message_value: Dict[str, Any]) -> Tuple[str, Optional[Dict], Optional[Dict], Optional[str]]:
        """
        Given the message value (decoded JSON payload), return:
        (op, before, after, source_db_name)
        """
        payload = message_value.get("payload") if isinstance(message_value, dict) else message_value

        if payload is None:
            payload = message_value

        op = payload.get("op")
        before = payload.get("before")
        after = payload.get("after")

        source_section = payload.get("source", {}) or {}
        source_db = source_section.get("db") or source_section.get("database") or source_section.get("schema") or source_section.get("schema_name")

        return op, before, after, source_db

    def extract_table_from_topic(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        """
        ✅ Extract database/schema and table from topic name
        
        Handles different database topic formats:
        - MySQL:      'prefix.database.table' → (database, table)
        - PostgreSQL: 'prefix.schema.table' → (schema, table)
        - SQL Server: 'prefix.database.schema.table' → (database, schema.table)
        - Oracle:     'prefix.schema.table' → (schema, schema.table)
        """
        parts = topic.split(".")
        
        logger.debug(f"🔍 Extracting from topic: {topic}")
        logger.debug(f"   Topic parts: {parts}")
        
        db_type = self.replication_config.client_database.db_type.lower()
        logger.debug(f"   Database type: {db_type}")
        
        # SQL Server: 4 parts
        if len(parts) >= 4:
            database = parts[1]
            schema = parts[2]
            table = parts[3]
            qualified_table = f"{schema}.{table}"
            logger.info(f"✅ SQL Server format: {database} / {qualified_table}")
            return database, qualified_table
        
        # Oracle/PostgreSQL/MySQL: 3 parts
        elif len(parts) >= 3:
            schema_or_db = parts[1]
            table = parts[2]
            
            if db_type == 'oracle':
                schema = schema_or_db
                qualified_table = f"{schema}.{table}"
                logger.info(f"✅ Oracle format: {schema} / {table} → {qualified_table}")
                return schema, qualified_table
            
            elif db_type == 'postgresql':
                schema = schema_or_db
                qualified_table = f"{schema}.{table}"
                logger.info(f"✅ PostgreSQL format: {schema} / {qualified_table}")
                return schema, qualified_table
            
            else:  # MySQL
                database = schema_or_db
                logger.info(f"✅ MySQL format: {database} / {table}")
                return database, table
        
        logger.warning(f"⚠️ Unexpected topic format: {topic} ({len(parts)} parts)")
        return None, parts[-1] if parts else None

    def get_table_mapping(self, source_table: str) -> Optional[Any]:
        """
        Look up TableMapping for the given source table.
        ✅ Handles Oracle/SQL Server schema matching
        """
        try:
            from client.models import TableMapping
            
            logger.debug(f"🔍 Looking up TableMapping for: {source_table}")
            
            db_type = self.replication_config.client_database.db_type.lower()
            
            # STRATEGY 1: Exact match
            table_mapping = TableMapping.objects.filter(
                replication_config=self.replication_config,
                source_table=source_table,
                is_enabled=True
            ).first()
            
            if table_mapping:
                logger.info(f"   ✅ Found exact match: {table_mapping.source_table}")
                return table_mapping
            
            # STRATEGY 2: Oracle - Try with schema prefix
            if db_type == 'oracle':
                if '.' not in source_table:
                    username = self.replication_config.client_database.username.upper()
                    schema_name = username[3:] if username.startswith('C##') else username
                    table_with_schema = f"{schema_name}.{source_table.upper()}"
                    
                    logger.debug(f"   🔍 Oracle: Trying with schema: {table_with_schema}")
                    
                    table_mapping = TableMapping.objects.filter(
                        replication_config=self.replication_config,
                        source_table=table_with_schema,
                        is_enabled=True
                    ).first()
                    
                    if table_mapping:
                        logger.info(f"   ✅ Found with schema prefix: {table_mapping.source_table}")
                        return table_mapping
                else:
                    table_without_schema = source_table.split('.', 1)[1]
                    logger.debug(f"   🔍 Oracle: Trying without schema: {table_without_schema}")
                    
                    table_mapping = TableMapping.objects.filter(
                        replication_config=self.replication_config,
                        source_table=table_without_schema,
                        is_enabled=True
                    ).first()
                    
                    if table_mapping:
                        logger.warning(f"   ⚠️ Found legacy mapping: '{table_without_schema}'")
                        return table_mapping
            
            # STRATEGY 3: SQL Server - Try with/without schema
            elif db_type == 'mssql':
                if '.' in source_table:
                    table_without_schema = source_table.split('.', 1)[1]
                    logger.debug(f"   🔍 SQL Server: Trying without schema: {table_without_schema}")
                    
                    table_mapping = TableMapping.objects.filter(
                        replication_config=self.replication_config,
                        source_table=table_without_schema,
                        is_enabled=True
                    ).first()
                    
                    if table_mapping:
                        logger.warning(f"   ⚠️ Found legacy mapping: '{table_without_schema}'")
                        return table_mapping
                else:
                    table_with_schema = f"dbo.{source_table}"
                    logger.debug(f"   🔍 SQL Server: Trying with schema: {table_with_schema}")
                    
                    table_mapping = TableMapping.objects.filter(
                        replication_config=self.replication_config,
                        source_table=table_with_schema,
                        is_enabled=True
                    ).first()
                    
                    if table_mapping:
                        logger.info(f"   ✅ Found with schema prefix: {table_mapping.source_table}")
                        return table_mapping
            
            # STRATEGY 4: Case-insensitive match
            logger.debug(f"   🔍 Trying case-insensitive match")
            
            table_mapping = TableMapping.objects.filter(
                replication_config=self.replication_config,
                source_table__iexact=source_table,
                is_enabled=True
            ).first()
            
            if table_mapping:
                logger.warning(f"   ⚠️ Found case-insensitive match: '{table_mapping.source_table}'")
                return table_mapping
            
            # NOT FOUND
            logger.warning(f"   ❌ No TableMapping found for '{source_table}'")
            
            existing_mappings = TableMapping.objects.filter(
                replication_config=self.replication_config,
                is_enabled=True
            ).values_list('source_table', flat=True)
            
            if existing_mappings:
                logger.info(f"   📋 Available mappings: {list(existing_mappings)}")
            
            return None

        except Exception as e:
            logger.error(f"   ❌ Error looking up TableMapping: {e}", exc_info=True)
            return None

    def convert_debezium_timestamps(self, row: dict, table: Table = None) -> dict:
        """
        Convert Debezium timestamp/date fields based on actual table schema.
        """
        if not row:
            return row

        date_time_columns = {}

        if table is not None:
            for col in table.columns:
                col_type = type(col.type).__name__
                if col_type in ('Date', 'DateTime', 'DATETIME', 'TIMESTAMP', 'Time', 'DATE', 'TIME'):
                    date_time_columns[col.name] = col_type

        converted = {}

        for key, value in row.items():
            if value is None:
                converted[key] = None
                continue

            if table is not None:
                if key not in date_time_columns:
                    converted[key] = value
                    continue
                col_type = date_time_columns[key]
            else:
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
                col_type = None

            try:
                parsed = None

                if isinstance(value, (datetime, date)):
                    parsed = value

                elif isinstance(value, int):
                    if value > 1000000000000000:
                        parsed = datetime.fromtimestamp(value / 1000000.0)
                    elif value > 1000000000000:
                        parsed = datetime.fromtimestamp(value / 1000.0)
                    elif value > 1000000000:
                        parsed = datetime.fromtimestamp(value)
                    elif value >= -719162 and value <= 2932896:
                        unix_epoch = date(1970, 1, 1)
                        parsed = unix_epoch + timedelta(days=value)
                    else:
                        s = str(value)
                        try:
                            if len(s) == 8:
                                parsed = datetime.strptime(s, "%Y%m%d")
                            elif len(s) == 6:
                                parsed = datetime.strptime(s + "01", "%Y%m%d")
                            elif len(s) == 4:
                                parsed = datetime(int(s), 1, 1)
                            else:
                                logger.warning(f"Cannot parse numeric date {key}={value}")
                                converted[key] = None
                                continue
                        except (ValueError, OverflowError):
                            logger.warning(f"Failed to parse {key}={value}")
                            converted[key] = None
                            continue

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

                if parsed:
                    if col_type == 'Date' or col_type == 'DATE':
                        if isinstance(parsed, datetime):
                            converted[key] = parsed.date()
                        else:
                            converted[key] = parsed
                    elif col_type in ('DateTime', 'DATETIME', 'TIMESTAMP'):
                        if isinstance(parsed, date) and not isinstance(parsed, datetime):
                            converted[key] = datetime.combine(parsed, datetime.min.time())
                        else:
                            converted[key] = parsed
                    elif col_type in ('Time', 'TIME'):
                        if isinstance(parsed, datetime):
                            converted[key] = parsed.time()
                        else:
                            converted[key] = parsed
                    else:
                        if isinstance(parsed, datetime):
                            if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
                                converted[key] = parsed.date()
                            else:
                                converted[key] = parsed
                        else:
                            converted[key] = parsed
                else:
                    logger.warning(f"Could not parse {key}={value}")
                    converted[key] = None

            except Exception as e:
                logger.error(f"Error converting {key}={value}: {e}")
                converted[key] = None

        return converted

    # -------------------------
    # Apply operations
    # -------------------------
    def apply_insert(self, target_table, row_data, table_mapping):
        """
        Apply INSERT operation with column name mapping.
        ✅ Includes Oracle NUMBER decoding before processing
        """
        try:
            logger.info(f"   🔹 Starting INSERT operation for table: {target_table.name}")
            logger.info(f"   🔹 Original row data: {json.dumps(row_data, default=str)[:300]}...")

            # ✅ STEP 1: Normalize data (decode Oracle NUMBERs)
            normalized_data = self.normalize_row_data(row_data)
            logger.info(f"   🔹 After Oracle NUMBER normalization: {json.dumps(normalized_data, default=str)[:300]}...")

            # ✅ STEP 2: Map columns
            mapped_data = self._map_columns_from_db(normalized_data, table_mapping, target_table)
            logger.info(f"   ✅ Mapped {len(mapped_data)} columns successfully")
            logger.info(f"   🔹 Original data: {list(row_data.keys())}")
            logger.info(f"   🔹 Mapped data (with fallback): {list(mapped_data.keys())}")

            # ✅ STEP 3: Convert timestamps
            mapped_data = self.convert_debezium_timestamps(mapped_data, target_table)
            logger.info(f"   🔹 After timestamp conversion: {json.dumps(mapped_data, default=str)[:300]}...")

            # ✅ STEP 4: Execute INSERT with UPSERT
            stmt = insert(target_table).values(**mapped_data)

            pk_columns = self._get_primary_keys(target_table)
            update_dict = {
                col: stmt.inserted[col]
                for col in mapped_data.keys()
                if col not in pk_columns
            }
            
            if update_dict:
                stmt = stmt.on_duplicate_key_update(**update_dict)
            
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
        Map source column names to target column names using ColumnMapping.
        ✅ Case-insensitive matching for Oracle
        """
        from client.models import ColumnMapping

        column_mappings = ColumnMapping.objects.filter(
            table_mapping=table_mapping,
            is_enabled=True
        ).values('source_column', 'target_column')

        column_map = {
            cm['source_column']: cm['target_column']
            for cm in column_mappings
        }
        
        column_map_lower = {
            cm['source_column'].lower(): (cm['source_column'], cm['target_column'])
            for cm in column_mappings
        }

        target_columns = {col.name for col in target_table.columns}
        target_columns_lower = {col.name.lower(): col.name for col in target_table.columns}

        logger.debug(f"   📋 Column mapping:")
        logger.debug(f"      Source columns: {list(row_data.keys())}")
        logger.debug(f"      Configured mappings: {len(column_map)}")
        logger.debug(f"      Target columns: {len(target_columns)}")

        mapped_data = {}
        unmapped_columns = []
        
        for source_col, value in row_data.items():
            # STRATEGY 1: Exact match
            if source_col in column_map:
                mapped_col = column_map[source_col]
                if mapped_col in target_columns:
                    mapped_data[mapped_col] = value
                    logger.debug(f"      ✅ Exact: {source_col} → {mapped_col}")
                    continue
            
            # STRATEGY 2: Case-insensitive
            source_col_lower = source_col.lower()
            if source_col_lower in column_map_lower:
                original_source, mapped_col = column_map_lower[source_col_lower]
                if mapped_col in target_columns:
                    mapped_data[mapped_col] = value
                    logger.debug(f"      ✅ Case-insensitive: {source_col} → {mapped_col}")
                    continue
                mapped_col_lower = mapped_col.lower()
                if mapped_col_lower in target_columns_lower:
                    actual_target_col = target_columns_lower[mapped_col_lower]
                    mapped_data[actual_target_col] = value
                    logger.debug(f"      ✅ Case-insensitive both: {source_col} → {actual_target_col}")
                    continue
            
            # STRATEGY 3: Direct fallback
            if source_col in target_columns:
                mapped_data[source_col] = value
                logger.warning(f"      ⚠️ No mapping, using source name: {source_col}")
                continue
            if source_col_lower in target_columns_lower:
                actual_target_col = target_columns_lower[source_col_lower]
                mapped_data[actual_target_col] = value
                logger.warning(f"      ⚠️ No mapping, case-insensitive: {source_col} → {actual_target_col}")
                continue
            
            unmapped_columns.append(source_col)
            logger.debug(f"      ⚠️ Skipping: {source_col}")
        
        if mapped_data:
            logger.info(f"   ✅ Mapped {len(mapped_data)} columns")
        else:
            logger.error(f"   ❌ NO COLUMNS MAPPED!")
        
        if unmapped_columns:
            logger.warning(f"   ⚠️ Skipped {len(unmapped_columns)} columns: {unmapped_columns}")

        return mapped_data

    def _get_primary_keys(self, table):
        """Get primary key column names."""
        return {col.name for col in table.primary_key.columns}

    def apply_update(self, target_table, row_data, table_mapping):
        """Apply UPDATE operation."""
        try:
            logger.info(f"   🔹 Starting UPDATE for {target_table.name}")
            normalized_data = self.normalize_row_data(row_data)
            mapped_data = self._map_columns_from_db(normalized_data, table_mapping, target_table)
            mapped_data = self.convert_debezium_timestamps(mapped_data, target_table)

            pk_columns = self._get_primary_keys(target_table)
            pk_values = {k: v for k, v in mapped_data.items() if k in pk_columns}
            update_values = {k: v for k, v in mapped_data.items() if k not in pk_columns}
            
            if not pk_values:
                raise ValueError("No primary key values")
            
            stmt = target_table.update()
            for pk_col, pk_val in pk_values.items():
                stmt = stmt.where(target_table.c[pk_col] == pk_val)
            stmt = stmt.values(**update_values)
            
            with self.target_engine.begin() as conn:
                conn.execute(stmt)

            self.stats["updates"] += 1
            logger.info(f"   ✅ UPDATE successful")

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"   ❌ UPDATE failed: {e}")
            logger.exception(f"   ❌ Full exception:")
            raise

    def apply_delete(self, target_table, row_data, table_mapping):
        """Apply DELETE operation."""
        try:
            logger.info(f"   🔹 Starting DELETE for {target_table.name}")
            normalized_data = self.normalize_row_data(row_data)
            mapped_data = self._map_columns_from_db(normalized_data, table_mapping, target_table)
            mapped_data = self.convert_debezium_timestamps(mapped_data, target_table)
            
            pk_columns = self._get_primary_keys(target_table)
            pk_values = {k: v for k, v in mapped_data.items() if k in pk_columns}
            
            if not pk_values:
                raise ValueError("No primary key values")
            
            stmt = target_table.delete()
            for pk_col, pk_val in pk_values.items():
                stmt = stmt.where(target_table.c[pk_col] == pk_val)
            
            with self.target_engine.begin() as conn:
                conn.execute(stmt)

            self.stats["deletes"] += 1
            logger.info(f"   ✅ DELETE successful")

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"   ❌ DELETE failed: {e}")
            logger.exception(f"   ❌ Full exception:")
            raise

    # -------------------------
    # Message processing
    # -------------------------
    def process_message(self, msg):
        """Process a Kafka message."""
        try:
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()

            logger.info("=" * 80)
            logger.info(f"📨 NEW MESSAGE")
            logger.info(f"   Topic: {topic}")
            logger.info(f"   Partition: {partition}")
            logger.info(f"   Offset: {offset}")
            logger.info("=" * 80)

            message_value = msg.value()
            if message_value is None:
                logger.warning(f"⚠️ Empty message")
                return

            op, before, after, payload_source_db = self.parse_debezium_payload(message_value)

            logger.info(f"🔍 CDC Event:")
            logger.info(f"   Operation: {op}")
            logger.info(f"   Source DB: {payload_source_db}")

            topic_db, topic_table = self.extract_table_from_topic(topic)
            source_db = payload_source_db or topic_db or "unknown"
            table_name = topic_table or "unknown"

            logger.info(f"📊 Table: {source_db} / {table_name}")

            table_mapping = self.get_table_mapping(table_name)
            if not table_mapping:
                logger.warning(f"⚠️ No TableMapping for {table_name}")
                return

            target_table_name = table_mapping.target_table
            logger.info(f"   ✅ Target: {target_table_name}")

            # Normalize and convert sample data
            sample = after or before or {}
            sample = self.normalize_row_data(sample)
            sample = self.convert_debezium_timestamps(sample)
            
            if target_table_name not in self.table_cache:
                try:
                    self.ensure_table_exists(target_table_name, sample)
                    if target_table_name not in self.table_cache:
                        t = Table(target_table_name, self.metadata, autoload_with=self.target_engine)
                        self.table_cache[target_table_name] = t
                except Exception as e:
                    logger.exception(f"Failed to prepare table: {e}")
                    return

            target_table_obj = self.table_cache.get(target_table_name)
            if target_table_obj is None:
                logger.error(f"❌ Table not in cache")
                return

            logger.info(f"🔧 Processing: {op}")

            if op in ("c", "r"):
                if after:
                    logger.info(f"➕ INSERT")
                    self.apply_insert(target_table_obj, after, table_mapping)
                else:
                    logger.warning(f"⚠️ No 'after' data")
                    
            elif op == "u":
                logger.info(f"✏️ UPDATE")
                self.apply_update(target_table_obj, after or {}, table_mapping)
                
            elif op == "d":
                logger.info(f"🗑️ DELETE")
                self.apply_delete(target_table_obj, before or {}, table_mapping)
                
            else:
                logger.warning(f"⚠️ Unknown op: {op}")
                if after:
                    self.apply_insert(target_table_obj, after, table_mapping)

            self.stats["messages_processed"] += 1
            self.stats["last_message_time"] = datetime.utcnow()

            logger.info(f"✅ Success")
            logger.info(f"📈 Stats: {self.stats['messages_processed']} messages, "
                       f"{self.stats['inserts']} inserts, {self.stats['updates']} updates, "
                       f"{self.stats['deletes']} deletes, {self.stats['errors']} errors")

        except Exception as e:
            self.stats["errors"] += 1
            logger.exception(f"Error processing message: {e}")

    # -------------------------
    # Consume loop
    # -------------------------
    def consume(self, max_messages: Optional[int] = None, timeout: float = 1.0):
        """Start consuming messages."""
        try:
            if not self.consumer:
                raise KafkaConsumerException("Consumer not initialized")

            messages_count = 0
            logger.info(f"Starting consumption from: {', '.join(self.topics)}")

            import time
            last_config_check = time.time()
            config_check_interval = 2

            while True:
                if max_messages and messages_count >= max_messages:
                    break

                current_time = time.time()
                if current_time - last_config_check >= config_check_interval:
                    try:
                        from client.models import ReplicationConfig
                        config = ReplicationConfig.objects.filter(
                            id=self.replication_config.id
                        ).first()

                        if not config:
                            logger.warning(f"🛑 Config deleted")
                            break
                        elif not config.is_active:
                            logger.warning(f"🛑 Config inactive")
                            break

                        last_config_check = current_time
                    except Exception as e:
                        logger.error(f"Error checking config: {e}")
                        last_config_check = current_time

                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition")
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.debug(f"⚠️ Topic not available yet")
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                self.process_message(msg)
                messages_count += 1

                try:
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.warning(f"Failed to commit: {e}")

            logger.info("Finished consumption")
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception:
            logger.exception("Error during consumption")
            raise
        finally:
            try:
                if self.consumer:
                    self.consumer.close()
            except:
                pass

    def get_stats(self) -> Dict[str, Any]:
        """Return stats."""
        return dict(self.stats)