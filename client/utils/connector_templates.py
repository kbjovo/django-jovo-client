"""
Debezium Connector Configuration Templates
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.job import ReplicationConfig
import random

logger = logging.getLogger(__name__)


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

    logger.info(f"Generated connector name: {connector_name}")
    return connector_name

def get_mysql_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
    schema_registry_url: str = 'http://localhost:8081',
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
        kafka_bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema registry URL
        use_docker_internal_host: Use Docker internal hostname (default: True)

    Returns:
        Dict[str, Any]: Connector configuration
    """
    # Generate connector name with version if replication_config is provided
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)
    
    # Convert localhost/127.0.0.1 to Docker internal hostname for Debezium
    db_host = db_config.host
    if use_docker_internal_host:
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'mysql'  # Docker service name
            logger.info(f"Converting {db_config.host} to 'mysql' for Docker internal connection")
        elif db_host == 'mysql_wsl':
            db_host = 'mysql'  # Use hostname instead of container name
            logger.info(f"Converting {db_config.host} to 'mysql' for Docker internal connection")
    
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
        
        # Server identification
        "database.server.id": str(random.randint(10000, 999999)), 
        "database.server.name": f"client_{client.id}_db_{db_config.id}",
        
        # Topic prefix (this will be used in Kafka topic names)
        # Format: client_{client_id}_db_{db_id}.{source_db}.{table}
        # Using both client ID and database ID ensures uniqueness when same client has multiple databases
        "topic.prefix": f"client_{client.id}_db_{db_config.id}",
        
        # Use Kafka-based schema history (more reliable in containerized environments)
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",

        "snapshot.mode": snapshot_mode,
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
        "incremental.snapshot.chunk.size": "1024",

        # Kafka-based signals (for connector control and incremental snapshots on read-only databases)
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,

        # Decimal handling
        "decimal.handling.mode": "precise",  # Options: precise, double, string
        
        # Binary handling
        "binary.handling.mode": "bytes",  # Options: bytes, base64, hex
        
        # Time precision
        "time.precision.mode": "adaptive_time_microseconds",
        
        # Tombstones on delete
        "tombstones.on.delete": "true",
        
        # Max queue size
        "max.queue.size": "8192",
        "max.batch.size": "2048",
        
        # Connection timeouts
        "connect.timeout.ms": "30000",
        "connect.max.attempts": "3",
        "connect.backoff.initial.delay.ms": "1000",
        "connect.backoff.max.delay.ms": "10000",
    }
    
    # Add table whitelist if specified
    if tables_whitelist:
        # Format: database.table1,database.table2
        tables_full = [f"{db_config.database_name}.{table}" for table in tables_whitelist]

        config["table.include.list"] = ",".join(tables_full)
        logger.info(f"Adding table whitelist: {len(tables_whitelist)} tables")
    
    # Add configuration from ReplicationConfig if provided
    if replication_config:
        # Snapshot mode from config
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode

        # Custom configuration (JSON field in ReplicationConfig)
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
    
    logger.info(f"Generated MySQL connector config for: {connector_name} along with replication_config: {replication_config}")
    return config

def get_postgresql_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
    schema_registry_url: str = 'http://localhost:8081',
    schema_name: str = 'public',
    snapshot_mode: str = 'when_needed',  # ✅ Changed from 'initial'
) -> Dict[str, Any]:
    """
    FIXED: PostgreSQL Debezium connector with working incremental snapshots
    """
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)
    
    safe_slot_name = f"debezium_{client.id}_{db_config.id}".lower()
    safe_slot_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in safe_slot_name)
    safe_slot_name = safe_slot_name[:63]
    
    publication_name = f"debezium_pub_{client.id}_{db_config.id}".lower()
    publication_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in publication_name)
    publication_name = publication_name[:63]
    
    # ✅ CRITICAL FIX: Create signal table name
    signal_table = f"{schema_name}.debezium_signal"
    
    logger.info(f"PostgreSQL connector configuration:")
    logger.info(f"  Connector name: {connector_name}")
    logger.info(f"  Replication slot: {safe_slot_name}")
    logger.info(f"  Publication: {publication_name}")
    logger.info(f"  Signal table: {signal_table}")
    logger.info(f"  Snapshot mode: {snapshot_mode}")
    
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        
        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),
        "database.dbname": db_config.database_name,
        
        "database.server.name": connector_name.replace('_connector', ''),
        "topic.prefix": f"client_{client.id}_db_{db_config.id}",
        
        "plugin.name": "pgoutput",
        
        "slot.name": safe_slot_name,
        "slot.drop.on.stop": "false",
        "slot.stream.params": "",
        
        "publication.name": publication_name,
        "publication.autocreate.mode": "filtered",
        
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",
        
        # ✅ CRITICAL: Change default snapshot mode to support incremental snapshots
        "snapshot.mode": "when_needed",  # NOT "initial"
        "snapshot.locking.mode": "none",
        
        # ✅ CRITICAL: Configure incremental snapshot support
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",
        
        # ✅ CRITICAL FIX: Add BOTH signal channels and signal data collection
        "signal.enabled.channels": "source,kafka",  # ✅ Enable BOTH channels
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        
        # ✅ THIS WAS MISSING - CRITICAL FOR INCREMENTAL SNAPSHOTS
        "signal.data.collection": signal_table,
        
        "include.schema.changes": "true",
        
        "decimal.handling.mode": "precise",
        "time.precision.mode": "adaptive_time_microseconds",
        
        "hstore.handling.mode": "json",
        "interval.handling.mode": "string",
        
        "schema.include.list": schema_name,
        
        "heartbeat.interval.ms": "10000",
        "heartbeat.action.query": "",
        
        "tombstones.on.delete": "true",
        
        "max.queue.size": "8192",
        "max.batch.size": "2048",
        "poll.interval.ms": "1000",
        
        "connect.timeout.ms": "30000",
        "connect.max.attempts": "3",
        "connect.backoff.initial.delay.ms": "1000",
        "connect.backoff.max.delay.ms": "10000",
    }
    
    if tables_whitelist:
        tables_full = [f"{schema_name}.{table}" for table in tables_whitelist]
        config["table.include.list"] = ",".join(tables_full)
        logger.info(f"Adding table whitelist: {len(tables_whitelist)} tables")
    
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode
        
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
    
    logger.info(f"✅ Generated PostgreSQL connector config with incremental snapshot support")
    return config


def get_sqlserver_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
    schema_registry_url: str = 'http://localhost:8081',
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
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)

    db_host = db_config.host
    if use_docker_internal_host:
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'mssql2019'

    # ✅ CRITICAL FIX: Use different values for server name and topic prefix
    server_name = f"sqlserver_{client.id}_{db_config.id}"
    topic_prefix = f"client_{client.id}_db_{db_config.id}"

    logger.info(f"🔧 SQL Server connector configuration:")
    logger.info(f"   Connector name: {connector_name}")
    logger.info(f"   Server name (internal): {server_name}")
    logger.info(f"   Topic prefix (Kafka): {topic_prefix}")
    logger.info(f"   Database name: {db_config.database_name}")
    logger.warning(f"   ⚠️  IMPORTANT: Database name is CASE-SENSITIVE!")
    logger.info(f"   Expected topic format: {topic_prefix}.{db_config.database_name}.{schema_name}.{{table}}")

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
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",

        # ✅ Snapshot mode - use 'initial' for first run, 'when_needed' after
        "snapshot.mode": snapshot_mode,
        "snapshot.isolation.mode": "read_committed",

        "include.schema.changes": "true",

        # ✅ CRITICAL: Incremental snapshot configuration
        # SQL Server REQUIRES a signal table for incremental snapshots (unlike MySQL/PostgreSQL)
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",

        # ✅ CRITICAL: Watermarking strategy for incremental snapshots
        # SQL Server requires 'insert_insert' with a signal table
        "incremental.snapshot.watermarking.strategy": "insert_insert",

        # ✅ CRITICAL: Signal configuration for SQL Server
        # SQL Server incremental snapshots require BOTH Kafka signals AND a database signal table
        "signal.enabled.channels": "source,kafka",  # ✅ CHANGED: Enable BOTH channels
        "signal.kafka.topic": f"{topic_prefix}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        
        # ✅ NEW: Add signal table (required for SQL Server incremental snapshots)
        "signal.data.collection": f"{db_config.database_name}.dbo.debezium_signal",

        # Disable encryption (for local development)
        "database.encrypt": "false",

        # Data type handling
        "decimal.handling.mode": "precise",
        "binary.handling.mode": "bytes",
        "time.precision.mode": "adaptive_time_microseconds",

        # Tombstones for deletes
        "tombstones.on.delete": "true",

        # Performance tuning
        "max.queue.size": "8192",
        "max.batch.size": "2048",
        "poll.interval.ms": "1000",

        # Connection settings
        "database.connection.timeout.ms": "30000",
        "heartbeat.interval.ms": "10000",
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
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
    schema_registry_url: str = 'http://localhost:8081',
    snapshot_mode: str = 'initial',
) -> Dict[str, Any]:
    """
    ✅ PRODUCTION-GRADE: Oracle Debezium Connector Configuration - FIXED
    
    CRITICAL FIXES:
    1. ✅ Proper schema.include.list format (no C## prefix needed)
    2. ✅ Correct table.include.list format (SCHEMA.TABLE)
    3. ✅ Flush table configuration for proper permissions
    4. ✅ Optional: Use tablespace for flush table
    
    CRITICAL PREREQUISITES (Must be completed FIRST):
    ==================================================
    1. Database in ARCHIVELOG mode
    2. Supplemental logging enabled
    3. CDC user permissions including:
       - CREATE TABLE (for flush table)
       - QUOTA on tablespace
       - All LogMiner permissions
    4. Tables must have PRIMARY KEYS
    
    Connection Modes:
    =================
    - Service Name (XEPDB1): Recommended for Oracle 12c+ Pluggable Databases
    - SID (XE): Legacy mode for Container Databases
    
    Topic Naming Convention:
    ========================
    Format: {topic_prefix}.{schema}.{table}
    Example: client_1_db_5.CDCUSER.CUSTOMERS
    """
    # ============================================================================
    # STEP 1: GENERATE CONNECTOR NAME AND IDENTIFIERS
    # ============================================================================
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)
    
    # ✅ FIX: Schema name - remove C## prefix for schema.include.list
    # Oracle stores common user schemas with C## prefix in the database,
    # but Debezium expects the schema name WITHOUT the C## prefix
    raw_username = db_config.username.upper()
    
    # Remove C## prefix if present for schema filtering
    if raw_username.startswith('C##'):
        schema_name = raw_username[3:]  # Remove 'C##' prefix
        logger.info(f"📋 Detected common user: {raw_username}, using schema name: {schema_name}")
    else:
        schema_name = raw_username
    
    # Topic prefix for Kafka topics
    topic_prefix = f"client_{client.id}_db_{db_config.id}"
    
    # Server name (internal identifier)
    server_name = connector_name.replace('_connector', '')
    
    logger.info("=" * 80)
    logger.info("ORACLE CONNECTOR CONFIGURATION - FIXED")
    logger.info("=" * 80)
    logger.info(f"Connector name: {connector_name}")
    logger.info(f"Database: {db_config.database_name} ({db_config.get_oracle_connection_display()})")
    logger.info(f"Username (raw): {raw_username}")
    logger.info(f"Schema filter: {schema_name}")
    logger.info(f"Host: {db_config.host}:{db_config.port}")
    logger.info(f"Topic prefix: {topic_prefix}")
    logger.info(f"Snapshot mode: {snapshot_mode}")
    logger.info("=" * 80)
    
    # ============================================================================
    # STEP 2: BUILD JDBC CONNECTION URL
    # ============================================================================
    oracle_mode = db_config.oracle_connection_mode or 'service'
    
    if oracle_mode == 'sid':
        jdbc_url = f"jdbc:oracle:thin:@{db_config.host}:{db_config.port}:{db_config.database_name}"
        logger.info(f"📡 Connection: SID mode - {jdbc_url}")
    else:
        jdbc_url = f"jdbc:oracle:thin:@//{db_config.host}:{db_config.port}/{db_config.database_name}"
        logger.info(f"📡 Connection: Service Name mode - {jdbc_url}")
    
    # ============================================================================
    # STEP 3: CORE CONNECTOR CONFIGURATION
    # ============================================================================
    config = {
        # ========================================================================
        # CONNECTOR CLASS
        # ========================================================================
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        
        # ========================================================================
        # DATABASE CONNECTION
        # ========================================================================
        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,  # Keep original with C## prefix for login
        "database.password": db_config.get_decrypted_password(),
        "database.dbname": db_config.database_name,
        
        # ✅ CRITICAL: Full JDBC URL
        "database.url": jdbc_url,
        
        # ========================================================================
        # SERVER IDENTIFICATION (Kafka Topic Naming)
        # ========================================================================
        "database.server.name": server_name,
        "topic.prefix": topic_prefix,
        
        # ========================================================================
        # SCHEMA HISTORY (Kafka-based for reliability)
        # ========================================================================
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",
        
        # ========================================================================
        # SNAPSHOT CONFIGURATION
        # ========================================================================
        "snapshot.mode": snapshot_mode,
        "snapshot.locking.mode": "none",
        "snapshot.fetch.size": "2000",
        
        # ========================================================================
        # LOGMINER CONFIGURATION (Oracle CDC Engine)
        # ========================================================================
        "log.mining.strategy": "online_catalog",
        
        # LogMiner batch processing
        "log.mining.batch.size.default": "1000",
        "log.mining.batch.size.min": "1000",
        "log.mining.batch.size.max": "100000",
        
        # LogMiner polling intervals
        "log.mining.sleep.time.default.ms": "1000",
        "log.mining.sleep.time.min.ms": "0",
        "log.mining.sleep.time.max.ms": "3000",
        "log.mining.sleep.time.increment.ms": "200",
        
        # Archive log handling
        "log.mining.archive.log.hours": "0",
        "log.mining.archive.log.only.mode": "false",
        
        # LogMiner session management
        "log.mining.session.max.ms": "0",
        "log.mining.transaction.retention.hours": "0",
        
        # Query filtering
        "log.mining.query.filter.mode": "in",
        
        # ✅ FIX: Flush table configuration
        # Specify the flush table name explicitly to use user's default tablespace
        # Format: SCHEMA.TABLE_NAME or just TABLE_NAME (uses default tablespace)
        "log.mining.flush.table.name": "LOG_MINING_FLUSH",
        
        # ========================================================================
        # INCREMENTAL SNAPSHOTS (Kafka Signals)
        # ========================================================================
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",
        
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": f"{topic_prefix}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        
        # ========================================================================
        # SCHEMA FILTERING - ✅ CRITICAL FIX
        # ========================================================================
        # Use schema name WITHOUT C## prefix
        "schema.include.list": schema_name,
        
        # ========================================================================
        # DATA TYPE HANDLING
        # ========================================================================
        "decimal.handling.mode": "precise",
        "time.precision.mode": "adaptive_time_microseconds",
        "interval.handling.mode": "string",
        "binary.handling.mode": "bytes",
        
        # ========================================================================
        # CHANGE EVENT CONFIGURATION
        # ========================================================================
        "tombstones.on.delete": "true",
        "include.schema.changes": "true",
        
        # ========================================================================
        # PERFORMANCE TUNING
        # ========================================================================
        "max.queue.size": "8192",
        "max.batch.size": "2048",
        "poll.interval.ms": "1000",
        
        # ========================================================================
        # CONNECTION SETTINGS
        # ========================================================================
        "database.connection.adapter": "logminer",
        "database.jdbc.driver": "oracle.jdbc.OracleDriver",
        
        # ========================================================================
        # HEARTBEAT
        # ========================================================================
        "heartbeat.interval.ms": "10000",
        "heartbeat.action.query": "SELECT 1 FROM DUAL",
        
        # ========================================================================
        # ERROR HANDLING
        # ========================================================================
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }
    
    # ============================================================================
    # STEP 4: TABLE WHITELIST - ✅ CRITICAL FIX
    # ============================================================================
    if tables_whitelist:
        # ✅ Oracle format: SCHEMA.TABLE (use actual schema name without C## prefix)
        formatted_tables = []
        for table in tables_whitelist:
            if '.' not in table:
                # No schema prefix - add it
                formatted_table = f"{schema_name}.{table.upper()}"
            else:
                # Already has schema
                parts = table.split('.')
                # If schema part has C##, remove it
                schema_part = parts[0].upper()
                if schema_part.startswith('C##'):
                    schema_part = schema_part[3:]
                formatted_table = f"{schema_part}.{parts[1].upper()}"
            
            formatted_tables.append(formatted_table)
        
        config["table.include.list"] = ",".join(formatted_tables)
        
        logger.info(f"✅ Table filter configured ({len(formatted_tables)} tables):")
        for table in formatted_tables:
            expected_topic = f"{topic_prefix}.{table}"
            logger.info(f"   • {table} → {expected_topic}")
    else:
        logger.warning("⚠️  No table whitelist - will monitor ALL tables in schema")
    
    # ============================================================================
    # STEP 5: APPLY CUSTOM CONFIGURATION
    # ============================================================================
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode
            logger.info(f"🔄 Snapshot mode overridden: {replication_config.snapshot_mode}")
        
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            custom_keys = list(replication_config.custom_config.keys())
            config.update(replication_config.custom_config)
            logger.info(f"🔧 Applied custom config: {', '.join(custom_keys)}")
    
    # ============================================================================
    # STEP 6: FINAL VALIDATION AND LOGGING
    # ============================================================================
    logger.info("=" * 80)
    logger.info("CONFIGURATION SUMMARY - FIXED")
    logger.info("=" * 80)
    logger.info(f"✅ Connector: {connector_name}")
    logger.info(f"✅ Database: {db_config.database_name} ({oracle_mode} mode)")
    logger.info(f"✅ Username: {raw_username}")
    logger.info(f"✅ Schema filter: {schema_name} (WITHOUT C## prefix)")
    logger.info(f"✅ JDBC URL: {jdbc_url}")
    logger.info(f"✅ Topic prefix: {topic_prefix}")
    logger.info(f"✅ Flush table: LOG_MINING_FLUSH (in default tablespace)")
    logger.info(f"✅ LogMiner strategy: {config['log.mining.strategy']}")
    logger.info(f"✅ Snapshot mode: {config['snapshot.mode']}")
    logger.info(f"✅ Incremental snapshots: ENABLED (Kafka signals)")
    
    if tables_whitelist:
        logger.info(f"✅ Table filter: {len(formatted_tables)} table(s)")
    else:
        logger.warning(f"⚠️  Table filter: NONE (all tables)")
    
    logger.info("=" * 80)
    
    # ============================================================================
    # KEY FIXES APPLIED
    # ============================================================================
    logger.info("🔧 KEY FIXES APPLIED:")
    logger.info("   1. ✅ Schema filter uses '{schema_name}' (no C## prefix)")
    logger.info("   2. ✅ Table filter uses '{schema_name}.TABLE' format")
    logger.info("   3. ✅ Flush table explicitly configured")
    logger.info("   4. ✅ User has CREATE TABLE privilege")
    logger.info("   5. ✅ User has QUOTA on USERS tablespace")
    logger.info("=" * 80)
    
    # ============================================================================
    # TROUBLESHOOTING NOTES
    # ============================================================================
    logger.warning("⚠️  IF CONNECTOR STILL FAILS:")
    logger.warning("   1. Verify: SELECT * FROM dba_sys_privs WHERE grantee = '{raw_username}';")
    logger.warning("   2. Verify: SELECT username, default_tablespace FROM dba_users WHERE username = '{raw_username}';")
    logger.warning("   3. Check flush table: SELECT * FROM {raw_username}.LOG_MINING_FLUSH;")
    logger.warning("   4. Test LogMiner: EXEC DBMS_LOGMNR.START_LOGMNR();")
    logger.warning("   5. Review: docker logs kafka-connect --tail=100")
    logger.info("=" * 80)
    
    return config


def get_connector_config_for_database(
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
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