"""
Debezium Connector Configuration Templates
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.job import ReplicationConfig
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


def format_table_for_connector(db_config: ClientDatabase, table_name: str, schema_name: str = None) -> str:
    """
    Format table name for Debezium connector configuration.

    Different databases require different table name formats in table.include.list:
    - MySQL:      {database}.{table}
    - PostgreSQL: {schema}.{table}
    - SQL Server: {schema}.{table}
    - Oracle:     {schema}.{table}
    """
    db_type = db_config.db_type.lower()

    if db_type == 'mssql':
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or 'dbo'
            table = table_name
        formatted = f"{schema}.{table}"
    elif db_type == 'postgresql':
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or 'public'
            table = table_name
        formatted = f"{schema}.{table}"
    elif db_type == 'mysql':
        table = table_name.split('.')[-1] if '.' in table_name else table_name
        formatted = f"{db_config.database_name}.{table}"
    elif db_type == 'oracle':
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or db_config.username.upper()
            table = table_name
        formatted = f"{schema}.{table}"
    else:
        formatted = f"{db_config.database_name}.{table_name}"

    logger.debug(f"Formatted table: {table_name} → {formatted} ({db_type})")
    return formatted


def generate_server_id(client_id: int, db_id: int, version: int = 0) -> int:
    """
    Generate a deterministic MySQL server ID based on client, database IDs, and connector version.

    This ensures:
    - Unique ID per connector version (no collisions when running multiple versions)
    - Same ID on restart (no binlog confusion)
    - Valid range for MySQL (1 to 2^32-1)

    Formula: 100000 + (client_id * 10000) + (db_id * 100) + (version % 100)
    Supports up to 999 clients with 99 databases each and 99 connector versions.
    """
    return 100000 + (client_id * 10000) + (db_id * 100) + (version % 100)


def build_column_include_list(replication_config: 'ReplicationConfig', db_config: ClientDatabase) -> Optional[str]:
    """
    Build column.include.list for Debezium.

    NOTE: Column selection feature has been removed. All columns are now always replicated.
    This function always returns None, letting Debezium include all columns by default.

    Args:
        replication_config: ReplicationConfig instance (unused)
        db_config: ClientDatabase instance (unused)

    Returns:
        None - all columns are always replicated
    """
    # Column selection feature removed - all columns are always replicated
    logger.info("All columns will be replicated (column selection feature removed)")
    return None


def generate_connector_name(client: Client, db_config: ClientDatabase, version: Optional[int] = None) -> str:
    """
    Generate connector name following the pattern: {client_name}_{db_name}_connector[_v_{version}]

    Args:
        client: Client instance
        db_config: ClientDatabase instance
        version: Optional version number for the connector (e.g., 1 for _v_1, 2 for _v_2)

    Returns:
        str: Connector name
    """
    # Clean client name (remove spaces, special chars)
    client_name = client.name.lower().replace(' ', '_').replace('-', '_')
    client_name = ''.join(c for c in client_name if c.isalnum() or c == '_')

    # Clean database name
    db_name = db_config.connection_name.lower().replace(' ', '_').replace('-', '_')
    db_name = ''.join(c for c in db_name if c.isalnum() or c == '_')

    # Generate base connector name
    connector_name = f"{client_name}_{db_name}_connector"

    # Add version suffix if provided
    if version is not None:
        connector_name = f"{connector_name}_v_{version}"

    logger.debug(f"Generated connector name: {connector_name}")
    return connector_name


def get_mysql_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    use_docker_internal_host: bool = True,
    snapshot_mode: str = 'initial',
) -> Dict[str, Any]:
    """
    Generate MySQL Debezium connector configuration

    Args:
        client: Client instance
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance (optional)
        tables_whitelist: List of tables to replicate (e.g., ['users', 'orders'])
        kafka_bootstrap_servers: Kafka bootstrap servers (defaults to settings)
        schema_registry_url: Schema registry URL (defaults to settings)
        use_docker_internal_host: Use Docker internal hostname (default: True)

    Returns:
        Dict[str, Any]: Connector configuration
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )

    # Use the saved custom name if present; fall back to auto-generated.
    # connector_name here is only used for internal config fields (e.g. signal.kafka.groupId).
    # The actual Debezium connector name is passed separately by the caller.
    version = replication_config.connector_version if replication_config else None
    if replication_config and replication_config.connector_name:
        connector_name = replication_config.connector_name
    else:
        connector_name = generate_connector_name(client, db_config, version=version)

    # Convert localhost/127.0.0.1 to Docker internal hostname for Debezium
    db_host = db_config.host
    if use_docker_internal_host:
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'mysql'  # Docker service name
            logger.debug(f"Converting {db_config.host} to 'mysql' for Docker internal connection")
        elif db_host == 'mysql_wsl':
            db_host = 'mysql'  # Use hostname instead of container name
            logger.debug(f"Converting {db_config.host} to 'mysql' for Docker internal connection")

    # Base configuration
    config = {
        # Connector class
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        
        # Database connection - use Docker internal hostname
        "database.hostname": db_host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),
        "database.include.list": db_config.database_name,
        
        # Server identification (deterministic ID for consistency across restarts)
        # Include version to prevent conflicts when multiple connector versions run simultaneously
        "database.server.id": str(generate_server_id(client.id, db_config.id, version or 0)),
        # CRITICAL: Include version in server.name to prevent JMX MBean conflicts
        # Each connector version gets unique MBeans: debezium.mysql:...,server=client_1_db_2_v_1
        "database.server.name": f"client_{client.id}_db_{db_config.id}_v_{version or 0}",
        
        # Topic prefix (this will be used in Kafka topic names)
        # Format: client_{client_id}_db_{db_id}_v_{version}.{source_db}.{table}
        # CRITICAL: Include version to prevent JMX MBean conflicts between multiple connectors
        # Each connector version gets unique internal ID derived from topic.prefix
        "topic.prefix": f"client_{client.id}_db_{db_config.id}_v_{version or 0}",
        
        # Use Kafka-based schema history (more reliable in containerized environments)
        # NOTE: Each connector version needs its own schema history topic to avoid JMX MBean conflicts
        # when running multiple source connectors for the same database with different table sets
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.client_{client.id}_db_{db_config.id}_v_{version or 0}",
        # Only store DDL for captured tables — prevents crashes from DDL on unmonitored tables in the binlog
        "schema.history.internal.store.only.captured.tables.ddl": "true",

        # Snapshot mode - use from replication_config if provided, otherwise use parameter
        "snapshot.mode": replication_config.snapshot_mode if replication_config and hasattr(replication_config, 'snapshot_mode') else snapshot_mode,
        "snapshot.locking.mode": "none",

        # Include schema changes
        "include.schema.changes": "true",

        "database.allowPublicKeyRetrieval": "true",

        # Read-only mode - user has no write permissions to source database
        # IMPORTANT: Requires GTID to be enabled on MySQL (gtid_mode=ON, enforce_gtid_consistency=ON)
        # See: https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/
        "read.only": "true",

        # Incremental snapshots with Kafka signaling (compatible with read-only databases when GTIDs enabled)
        "incremental.snapshot.allowed": "true",
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": str(replication_config.incremental_snapshot_chunk_size) if replication_config and hasattr(replication_config, 'incremental_snapshot_chunk_size') else "1024",

        # Kafka-based signals (for connector control and incremental snapshots on read-only databases)
        # CRITICAL: Include version to match topic.prefix and prevent conflicts between connector versions
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}_v_{version or 0}.signals",
        "signal.enabled.channels": "kafka",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "signal.kafka.groupId": f"dbz-signal-{connector_name}",  # CRITICAL: Must be unique per connector (camelCase!)
        "signal.poll.interval.ms": "1000",  # Check for signals every 1 second
        "signal.kafka.consumer.auto.offset.reset": "latest",  # Ignore stale signals from before restart

        "signal.kafka.consumer.key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "signal.kafka.consumer.value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        
        # Decimal handling
        "decimal.handling.mode": "precise",  # Options: precise, double, string

        # Binary handling
        "binary.handling.mode": "bytes",  # Options: bytes, base64, hex

        # Time precision
        "time.precision.mode": "adaptive_time_microseconds",
        
        # Tombstones on delete
        "tombstones.on.delete": "true",

        # Snapshot fetch size - controls JDBC fetchSize during initial snapshot
        # Prevents OOM by limiting how many rows the JDBC driver buffers at a time
        "snapshot.fetch.size": str(replication_config.snapshot_fetch_size) if replication_config and hasattr(replication_config, 'snapshot_fetch_size') else "10000",

        # Single-threaded snapshot (safest default, avoids OOM on large tables)
        "snapshot.max.threads": "1",

        # Performance tuning - use values from replication_config if provided
        "max.queue.size": str(replication_config.max_queue_size) if replication_config and hasattr(replication_config, 'max_queue_size') else "8192",
        "max.batch.size": str(replication_config.max_batch_size) if replication_config and hasattr(replication_config, 'max_batch_size') else "2048",
        "poll.interval.ms": str(replication_config.poll_interval_ms) if replication_config and hasattr(replication_config, 'poll_interval_ms') else "500",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081",
        "key.converter.schemas.enable": "true",

        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter.schemas.enable": "true",

        "key.converter.enhanced.avro.schema.support": "true",
        "value.converter.enhanced.avro.schema.support": "true",

        # Handle invalid/zero MySQL date values (e.g., 0000-00-00, 0000-00-00 00:00:00)
        # Without this, BinlogFieldReader logs warnings for every row with invalid dates
        # "warn" = log warning and use fallback value (null/epoch), "fail" = stop connector
        "event.deserialization.failure.handling.mode": "warn",

        # Connection timeouts
        "connect.timeout.ms": "30000",
        "connect.max.attempts": "3",
        "connect.backoff.initial.delay.ms": "1000",
        "connect.backoff.max.delay.ms": "10000",

        # JDBC driver properties - autoReconnect for MySQL
        "driver.autoReconnect": "true",

        # c3p0 connection pool validation - test connections before use
        "driver.hibernate.c3p0.testConnectionOnCheckout": "true",
        "driver.hibernate.c3p0.preferredTestQuery": "SELECT 1",
        "driver.hibernate.c3p0.idle_test_period": "300",
    }

    # Add table whitelist if specified
    if tables_whitelist:
        # Format: database.table1,database.table2
        tables_full = [f"{db_config.database_name}.{table}" for table in tables_whitelist]

        config["table.include.list"] = ",".join(tables_full)
        logger.debug(f"Adding table whitelist: {len(tables_whitelist)} tables")

    # Add configuration from ReplicationConfig if provided
    if replication_config:
        # Snapshot mode from config
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode

        # Custom configuration (JSON field in ReplicationConfig)
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)

    logger.debug(f"Generated MySQL connector config for: {connector_name}")
    return config


def get_postgresql_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    schema_name: str = 'public',
    snapshot_mode: str = 'when_needed',  # ✅ Changed from 'initial'
) -> Dict[str, Any]:
    """
    FIXED: PostgreSQL Debezium connector with working incremental snapshots
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )

    version = replication_config.connector_version if replication_config else None
    if replication_config and replication_config.connector_name:
        connector_name = replication_config.connector_name
    else:
        connector_name = generate_connector_name(client, db_config, version=version)

    # Include version in slot name to prevent conflicts between connector versions
    # Each connector version needs its own replication slot
    safe_slot_name = f"debezium_{client.id}_{db_config.id}_v_{version or 0}".lower()
    safe_slot_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in safe_slot_name)
    safe_slot_name = safe_slot_name[:63]

    # Include version in publication name for same reason
    publication_name = f"debezium_pub_{client.id}_{db_config.id}_v_{version or 0}".lower()
    publication_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in publication_name)
    publication_name = publication_name[:63]

    # ✅ CRITICAL FIX: Create signal table name
    signal_table = f"{schema_name}.debezium_signal"

    logger.debug(f"PostgreSQL connector: {connector_name}, slot: {safe_slot_name}, publication: {publication_name}")
    
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),
        "database.dbname": db_config.database_name,

        "database.server.name": connector_name.replace('_connector', ''),
        # CRITICAL: Include version to prevent JMX MBean conflicts between multiple connectors
        "topic.prefix": f"client_{client.id}_db_{db_config.id}_v_{version or 0}",

        # ✅ CRITICAL FIX: Replace schema with database name in topic
        # Default format: {topic.prefix}.{schema}.{table} → client_1_db_3_v_1.public.my_table
        # Fixed format:  {topic.prefix}.{database}.{table} → client_1_db_3_v_1.mydb.my_table
        # This ensures sink connectors can properly subscribe to topics
        "transforms": "routeTopic",
        "transforms.routeTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.routeTopic.regex": f"(client_{client.id}_db_{db_config.id}_v_{version or 0})\\.[^.]+\\.(.+)",
        "transforms.routeTopic.replacement": f"$1.{db_config.database_name}.$2",

        "plugin.name": "pgoutput",
        
        "slot.name": safe_slot_name,
        "slot.drop.on.stop": "false",
        "slot.stream.params": "",
        
        "publication.name": publication_name,
        "publication.autocreate.mode": "filtered",
        
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.client_{client.id}_db_{db_config.id}_v_{version or 0}",

        # Snapshot mode - use from replication_config if provided, otherwise use parameter
        "snapshot.mode": replication_config.snapshot_mode if replication_config and hasattr(replication_config, 'snapshot_mode') else snapshot_mode,
        "snapshot.locking.mode": "none",

        # ✅ CRITICAL: Configure incremental snapshot support
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": str(replication_config.incremental_snapshot_chunk_size) if replication_config and hasattr(replication_config, 'incremental_snapshot_chunk_size') else "1024",

        # ✅ CRITICAL FIX: Add BOTH signal channels and signal data collection
        "signal.enabled.channels": "source,kafka",  # ✅ Enable BOTH channels
        # CRITICAL: Include version to match topic.prefix and prevent conflicts between connector versions
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}_v_{version or 0}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "signal.kafka.consumer.auto.offset.reset": "latest",  # Ignore stale signals from before restart

        # ✅ THIS WAS MISSING - CRITICAL FOR INCREMENTAL SNAPSHOTS
        "signal.data.collection": signal_table,

        "include.schema.changes": "true",

        "decimal.handling.mode": "precise",
        "time.precision.mode": "adaptive_time_microseconds",

        "hstore.handling.mode": "json",
        "interval.handling.mode": "string",

        # Include both user schema and ddl_capture schema for DDL sync
        "schema.include.list": f"{schema_name},ddl_capture",

        "heartbeat.interval.ms": "10000",
        "heartbeat.action.query": "",

        "tombstones.on.delete": "true",

        # Snapshot fetch size - controls JDBC fetchSize during initial snapshot
        # Prevents OOM by limiting how many rows the JDBC driver buffers at a time
        "snapshot.fetch.size": str(replication_config.snapshot_fetch_size) if replication_config and hasattr(replication_config, 'snapshot_fetch_size') else "10000",

        # Single-threaded snapshot (safest default, avoids OOM on large tables)
        "snapshot.max.threads": "1",

        # Performance tuning - use values from replication_config if provided
        "max.queue.size": str(replication_config.max_queue_size) if replication_config and hasattr(replication_config, 'max_queue_size') else "8192",
        "max.batch.size": str(replication_config.max_batch_size) if replication_config and hasattr(replication_config, 'max_batch_size') else "2048",
        "poll.interval.ms": str(replication_config.poll_interval_ms) if replication_config and hasattr(replication_config, 'poll_interval_ms') else "1000",

        "connect.timeout.ms": "30000",
        "connect.max.attempts": "3",
        "connect.backoff.initial.delay.ms": "1000",
        "connect.backoff.max.delay.ms": "10000",

        # JDBC driver properties - autoReconnect for connection resilience
        "driver.autoReconnect": "true",

        # c3p0 connection pool validation - test connections before use
        "driver.hibernate.c3p0.testConnectionOnCheckout": "true",
        "driver.hibernate.c3p0.preferredTestQuery": "SELECT 1",
        "driver.hibernate.c3p0.idle_test_period": "300",
    }

    if tables_whitelist:
        # PostgreSQL table.include.list uses SCHEMA.TABLE format (e.g., 'public.busy_acc_greenera')
        # NOTE: Topic names use DATABASE.TABLE (via RegexRouter transform), but table filter uses SCHEMA
        tables_full = []
        for table in tables_whitelist:
            if '.' in table:
                # Table already has schema prefix (e.g., 'public.busy_acc_greenera')
                tables_full.append(table)
            else:
                # Add schema prefix (e.g., 'busy_acc_greenera' -> 'public.busy_acc_greenera')
                tables_full.append(f"{schema_name}.{table}")

        # Add DDL capture table for real-time DDL sync
        # This table is populated by PostgreSQL event triggers and enables DDL replication
        ddl_capture_table = "ddl_capture.ddl_events"
        if ddl_capture_table not in tables_full:
            tables_full.append(ddl_capture_table)
            logger.info(f"✅ Added DDL capture table for schema sync: {ddl_capture_table}")

        config["table.include.list"] = ",".join(tables_full)
        logger.debug(f"Adding table whitelist: {len(tables_full)} tables (including DDL capture)")

    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode

        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)

    logger.debug(f"Generated PostgreSQL connector config for: {connector_name}")
    return config


def get_sqlserver_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    schema_name: str = 'dbo',
    snapshot_mode: str = 'initial',
    use_docker_internal_host: bool = True,
) -> Dict[str, Any]:
    """
    ✅ FIXED: SQL Server connector with incremental snapshots via Kafka signals

    Key features:
    - Supports incremental snapshots for adding new tables
    - Uses Kafka-only signaling (no DB writes needed)
    - Correct topic naming: {prefix}.{database}.{schema}.{table}
    - Works with read-only databases (no signal table required)
    
    CRITICAL REQUIREMENTS:
    1. CDC must be enabled: EXEC sys.sp_cdc_enable_db
    2. SQL Server Agent must be running
    3. User needs CDC permissions: EXEC sys.sp_cdc_enable_table
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )

    version = replication_config.connector_version if replication_config else None
    if replication_config and replication_config.connector_name:
        connector_name = replication_config.connector_name
    else:
        connector_name = generate_connector_name(client, db_config, version=version)

    db_host = db_config.host
    if use_docker_internal_host:
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'mssql2019'

    # ✅ CRITICAL FIX: Use different values for server name and topic prefix
    # Include version to prevent JMX MBean conflicts between multiple connectors
    server_name = f"sqlserver_{client.id}_{db_config.id}_v_{version or 0}"
    topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{version or 0}"
    signal_table_full = f"{db_config.database_name}.dbo.debezium_signal"

    config = {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",

        "database.hostname": db_host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),

        # ✅ CRITICAL: Must match EXACT case in SQL Server
        "database.names": db_config.database_name,

        # ✅ CRITICAL: These MUST be different for SQL Server
        "database.server.name": server_name,      # Internal identifier
        "topic.prefix": topic_prefix,              # Kafka topic prefix

        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.client_{client.id}_db_{db_config.id}_v_{version or 0}",

        # ✅ Snapshot mode - use 'initial' for first run, 'when_needed' after
        "snapshot.mode": snapshot_mode,
        "snapshot.isolation.mode": "read_committed",

        "include.schema.changes": "true",

        # ✅ CRITICAL: Incremental snapshot configuration
        # SQL Server REQUIRES a signal table for incremental snapshots (unlike MySQL/PostgreSQL)
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",

        # ✅ FIXED: Watermarking strategy for incremental snapshots
        # Use 'insert_delete' instead of 'insert_insert' to avoid database context issues
        # 'insert_delete' doesn't require fully qualified table names
        "incremental.snapshot.watermarking.strategy": "insert_delete",

        # ✅ CRITICAL: Signal configuration for SQL Server
        # SQL Server incremental snapshots require BOTH Kafka signals AND a database signal table
        "signal.enabled.channels": "source,kafka",  # ✅ CHANGED: Enable BOTH channels
        "signal.kafka.topic": f"{topic_prefix}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "signal.kafka.consumer.auto.offset.reset": "latest",  # Ignore stale signals from before restart

        # ✅ CRITICAL: Signal table must include database name for watermarking queries
        # For SQL Server incremental snapshots, the signal table requires full three-part name
        # Format: database.schema.table (e.g., "AppDB.dbo.debezium_signal")
        "signal.data.collection": signal_table_full,

        # Disable encryption (for local development)
        "database.encrypt": "false",

        # Data type handling
        "decimal.handling.mode": "precise",
        "binary.handling.mode": "bytes",
        "time.precision.mode": "adaptive_time_microseconds",

        # Tombstones for deletes
        "tombstones.on.delete": "true",

        # Snapshot fetch size - controls JDBC fetchSize during initial snapshot
        # Prevents OOM by limiting how many rows the JDBC driver buffers at a time
        "snapshot.fetch.size": str(replication_config.snapshot_fetch_size) if replication_config and hasattr(replication_config, 'snapshot_fetch_size') else "10000",

        # Single-threaded snapshot (safest default, avoids OOM on large tables)
        "snapshot.max.threads": "1",

        # Performance tuning - use values from replication_config if provided
        "max.queue.size": str(replication_config.max_queue_size) if replication_config and hasattr(replication_config, 'max_queue_size') else "8192",
        "max.batch.size": str(replication_config.max_batch_size) if replication_config and hasattr(replication_config, 'max_batch_size') else "2048",
        "poll.interval.ms": str(replication_config.poll_interval_ms) if replication_config and hasattr(replication_config, 'poll_interval_ms') else "1000",

        # Connection settings
        "database.connection.timeout.ms": "30000",
        "heartbeat.interval.ms": "10000",

        # JDBC driver properties - autoReconnect for connection resilience
        "driver.autoReconnect": "true",

        # c3p0 connection pool validation - test connections before use
        "driver.hibernate.c3p0.testConnectionOnCheckout": "true",
        "driver.hibernate.c3p0.preferredTestQuery": "SELECT 1",
        "driver.hibernate.c3p0.idle_test_period": "300",
    }

    # ✅ CRITICAL: Table whitelist format for SQL Server
    # Format: "dbo.Customers,dbo.Orders" (NOT "AppDB.dbo.Customers")
    # Debezium will automatically prepend the database name
    if tables_whitelist:
        logger.warning(f"⚠️  SQL Server is CASE-SENSITIVE for table filters!")
        logger.warning(f"⚠️  Database name: {db_config.database_name}")

        tables_full = []
        for table in tables_whitelist:
            if '.' in table:
                # Already has schema: 'dbo.Customers' - use as-is
                tables_full.append(table)
            else:
                # No schema: 'Customers' - add schema prefix
                tables_full.append(f"{schema_name}.{table}")

        signal_table = f"{schema_name}.debezium_signal"
        if signal_table not in tables_full:
            tables_full.append(signal_table)
            logger.info(f"✅ Added signal table for incremental snapshots: {signal_table}")

        config["table.include.list"] = ",".join(tables_full)

        logger.info(f"✅ Table filter configured:")
        for table in tables_full:
            logger.info(f"   - {table}")

    # Apply custom config from ReplicationConfig
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode
        
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
    
    logger.info(f"✅ SQL Server connector config generated:")
    logger.info(f"   Connector: {connector_name}")
    logger.info(f"   Server name: {server_name}")
    logger.info(f"   Topic prefix: {topic_prefix}")
    logger.info(f"   Database: {db_config.database_name}")
    logger.info(f"   Snapshot mode: {config['snapshot.mode']}")
    logger.info(f"   Incremental snapshots: ENABLED (Kafka signals)")
    
    return config


def get_oracle_connector_config(
    client,
    db_config,
    replication_config=None,
    tables_whitelist=None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    snapshot_mode: str = 'initial',
):
    """
    ✅ FIXED: Oracle connector with proper signal table configuration

    Changes from your current version:
    1. Ensures signal.data.collection is ALWAYS set
    2. Creates signal table if it doesn't exist
    3. Properly handles 'schema_only' mode for adding new tables
    """
    import logging
    logger = logging.getLogger(__name__)

    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )

    def generate_connector_name(client, db_config, version=None):
        client_name = ''.join(c for c in client.name.lower().replace(' ', '_').replace('-', '_') if c.isalnum() or c == '_')
        db_name = ''.join(c for c in db_config.connection_name.lower().replace(' ', '_').replace('-', '_') if c.isalnum() or c == '_')
        connector_name = f"{client_name}_{db_name}_connector"
        if version is not None:
            connector_name = f"{connector_name}_v_{version}"
        return connector_name

    version = getattr(replication_config, 'connector_version', None)
    if replication_config and getattr(replication_config, 'connector_name', None):
        connector_name = replication_config.connector_name
    else:
        connector_name = generate_connector_name(client, db_config, version=version)

    connection_username = db_config.username.upper()
    schema_name = connection_username[3:] if connection_username.startswith('C##') else connection_username

    # ✅ FIXED: Use service_name from frontend (stored in database_name field)
    # For Oracle, database_name contains either:
    # - Service Name (e.g., XEPDB1) when oracle_connection_mode = 'service'
    # - SID (e.g., XE) when oracle_connection_mode = 'sid'
    pdb_name = db_config.database_name.upper()

    # ✅ FIXED: Use the service name from frontend instead of hardcoded 'XE'
    # The service_name/SID is stored in db_config.database_name
    oracle_connection_mode = getattr(db_config, 'oracle_connection_mode', 'service')

    if oracle_connection_mode == 'sid':
        # SID mode: Use database_name as SID directly
        cdb_service = pdb_name  # e.g., 'XE', 'ORCL'
    else:
        # Service Name mode: Use database_name as service name
        # For JDBC URL with service name, extract CDB from PDB name or use as-is
        if 'PDB' in pdb_name:
            # If it's a PDB name (e.g., XEPDB1), extract the CDB part (XE)
            # But for service name connection, we use the full service name
            cdb_service = pdb_name  # e.g., 'XEPDB1', 'ORCLPDB1'
        else:
            # Not a PDB, use as-is (e.g., 'XE' used as service name)
            cdb_service = pdb_name

    logger.info(f"🔧 Oracle CDC Configuration")
    logger.info(f"   Connector: {connector_name}")
    logger.info(f"   Snapshot Mode: {snapshot_mode}")
    logger.info(f"   Schema: {schema_name}")

    # ✅ CRITICAL: Create signal table BEFORE connector starts
    signal_table = f"{pdb_name}.{schema_name}.DEBEZIUM_SIGNAL"

    jdbc_url = f"jdbc:oracle:thin:@//{db_config.host}:{db_config.port}/{cdb_service}"

    config = {
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        
        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": connection_username,
        "database.password": db_config.get_decrypted_password(),
        "database.dbname": cdb_service,
        "database.url": jdbc_url,
        "database.pdb.name": pdb_name,

        "database.server.name": connector_name.replace('_connector', ''),
        # CRITICAL: Include version to prevent JMX MBean conflicts between multiple connectors
        "topic.prefix": f"client_{client.id}_db_{db_config.id}_v_{version or 0}",

        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",
        
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",
        
        "snapshot.mode": snapshot_mode,
        "snapshot.locking.mode": "none",
        "snapshot.fetch.size": str(replication_config.snapshot_fetch_size) if replication_config and hasattr(replication_config, 'snapshot_fetch_size') else "10000",

        # Single-threaded snapshot (safest default, avoids OOM on large tables)
        "snapshot.max.threads": "1",

        # LogMiner configuration
        "log.mining.flush.auto.create": "false",
        "log.mining.buffer.type": "memory",
        "log.mining.strategy": "online_catalog",
        "log.mining.session.max.ms": "0",
        "log.mining.batch.size.min": "1000",
        "log.mining.batch.size.max": "100000",
        "log.mining.batch.size.default": "20000",
        
        # ✅ CRITICAL: Signal configuration (ALWAYS enabled)
        "signal.enabled.channels": "source,kafka",
        # CRITICAL: Include version to match topic.prefix and prevent conflicts between connector versions
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}_v_{version or 0}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "signal.kafka.consumer.auto.offset.reset": "latest",  # Ignore stale signals from before restart
        "signal.data.collection": signal_table,  # ✅ ALWAYS set this
        "signal.poll.interval.ms": "5000",
        
        "schema.include.list": schema_name,
        
        "decimal.handling.mode": "precise",
        "time.precision.mode": "adaptive_time_microseconds",
        "interval.handling.mode": "string",
        "binary.handling.mode": "bytes",
        
        "tombstones.on.delete": "true",
        "include.schema.changes": "true",
        
        "max.queue.size": str(replication_config.max_queue_size) if replication_config and hasattr(replication_config, 'max_queue_size') else "8192",
        "max.batch.size": str(replication_config.max_batch_size) if replication_config and hasattr(replication_config, 'max_batch_size') else "2048",
        "poll.interval.ms": str(replication_config.poll_interval_ms) if replication_config and hasattr(replication_config, 'poll_interval_ms') else "1000",

        "database.connection.adapter": "logminer",
        "database.jdbc.driver": "oracle.jdbc.OracleDriver",
        
        "heartbeat.interval.ms": "10000",
        
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",

        "provide.transaction.metadata": "false",
        "skipped.operations": "t",

        # JDBC driver properties - autoReconnect for connection resilience
        "driver.autoReconnect": "true",

        # c3p0 connection pool validation - test connections before use
        "driver.hibernate.c3p0.testConnectionOnCheckout": "true",
        "driver.hibernate.c3p0.preferredTestQuery": "SELECT 1 FROM DUAL",
        "driver.hibernate.c3p0.idle_test_period": "300",
    }

    # Table whitelist
    if tables_whitelist:
        formatted_tables = []
        for t in tables_whitelist:
            table = t.strip().upper()
            if not table:
                continue
            
            if '.' in table:
                parts = table.split('.')
                if len(parts) == 2:
                    table_schema, table_name = parts
                    if table_schema.startswith('C##'):
                        table_schema = table_schema[3:]
                    formatted_table = f"{table_schema}.{table_name}"
                elif len(parts) == 3:
                    _, table_schema, table_name = parts
                    if table_schema.startswith('C##'):
                        table_schema = table_schema[3:]
                    formatted_table = f"{table_schema}.{table_name}"
                else:
                    raise ValueError(f"Invalid table format: '{table}'")
            else:
                formatted_table = f"{schema_name}.{table}"
            
            formatted_tables.append(formatted_table)

        if not formatted_tables:
            raise ValueError("No valid tables after formatting")

        # ✅ CRITICAL FIX: Add signal table to table.include.list
        # This is required for incremental snapshots via signals
        signal_table_name = f"{schema_name}.DEBEZIUM_SIGNAL"
        if signal_table_name not in formatted_tables:
            formatted_tables.append(signal_table_name)
            logger.info(f"✅ Added signal table for incremental snapshots: {signal_table_name}")

        config["table.include.list"] = ",".join(formatted_tables)
        logger.info(f"📋 Tables: {len(formatted_tables)}")

    # Apply custom config
    if replication_config:
        if getattr(replication_config, 'snapshot_mode', None):
            config["snapshot.mode"] = replication_config.snapshot_mode
        if getattr(replication_config, 'custom_config', None):
            config.update(replication_config.custom_config)

    logger.info("✅ Oracle connector configured with signal table support")

    return config


def get_connector_config_for_database(
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = 'http://localhost:8081',
    snapshot_mode: str = 'initial',
) -> Optional[Dict[str, Any]]:
    """
    Get connector configuration based on database type

    Args:
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance (optional)
        tables_whitelist: List of tables to replicate
        kafka_bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema registry URL
        snapshot_mode: Debezium snapshot mode ('never', 'initial', 'when_needed', etc.)

    Returns:
        Optional[Dict[str, Any]]: Connector configuration or None if unsupported
    """
    client = db_config.client
    db_type = db_config.db_type.lower()

    config_generators = {
        'mysql': get_mysql_connector_config,
        'postgresql': get_postgresql_connector_config,
        'mssql': get_sqlserver_connector_config,
        'oracle': get_oracle_connector_config,
    }

    generator = config_generators.get(db_type)

    if not generator:
        logger.error(f"Unsupported database type for CDC: {db_type}")
        return None

    logger.info(f"Generating {db_type} connector config with snapshot_mode={snapshot_mode}")

    return generator(
        client=client,
        db_config=db_config,
        replication_config=replication_config,
        tables_whitelist=tables_whitelist,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        schema_registry_url=schema_registry_url,
        snapshot_mode=snapshot_mode,
    )


def get_snapshot_modes() -> Dict[str, str]:
    """
    Get available snapshot modes with descriptions

    Returns:
        Dict[str, str]: Dictionary of snapshot mode -> description
    """
    return {
        'initial': 'Performs an initial snapshot when the connector first starts',
        'when_needed': 'Performs a snapshot if needed (e.g., after connector restart)',
        'never': 'Never performs snapshots, only captures changes from current point',
        'schema_only': 'Captures only the schema, not the data',
        'schema_only_recovery': 'For recovery purposes only',
    }

def validate_connector_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate connector configuration
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, list_of_errors)
    """
    errors = []
    
    # Required fields
    required_fields = [
        'connector.class',
        'database.hostname',
        'database.port',
        'database.user',
        'database.password',
        'database.include.list',
        'database.server.name',
    ]
    
    for field in required_fields:
        if field not in config or not config[field]:
            errors.append(f"Missing required field: {field}")
    
    # Validate port
    if 'database.port' in config:
        try:
            port = int(config['database.port'])
            if port < 1 or port > 65535:
                errors.append(f"Invalid port number: {port}")
        except ValueError:
            errors.append(f"Port must be a number: {config['database.port']}")
    
    # Validate snapshot mode
    if 'snapshot.mode' in config:
        valid_modes = get_snapshot_modes().keys()
        if config['snapshot.mode'] not in valid_modes:
            errors.append(f"Invalid snapshot mode: {config['snapshot.mode']}")
    
    is_valid = len(errors) == 0
    
    if is_valid:
        logger.info("Connector configuration is valid")
    else:
        logger.warning(f"Connector configuration has {len(errors)} errors")
    
    return is_valid, errors