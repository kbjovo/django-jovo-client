"""
Debezium Connector Configuration Templates
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.job import ReplicationConfig
import random
from sqlalchemy.sql import text

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
    client,
    db_config,
    replication_config=None,
    tables_whitelist=None,
    kafka_bootstrap_servers: str = 'kafka:29092',
    schema_registry_url: str = 'http://localhost:8081',
    snapshot_mode: str = 'initial',
):
    """
    ✅ PRODUCTION: Oracle Debezium connector with proper CDB+PDB handling
    
    CRITICAL FIX:
    - Connects to CDB root in JDBC URL
    - Uses database.pdb.name to tell Debezium which PDB to query
    - This ensures 2-part table names (SCHEMA.TABLE), not 3-part (PDB.SCHEMA.TABLE)
    - Prevents NullPointerException in LogMiner
    
    Args:
        client: Client instance
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance (optional)
        tables_whitelist: List of table names to replicate
        kafka_bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema registry URL
        snapshot_mode: Debezium snapshot mode ('initial', 'when_needed', etc.)
    
    Returns:
        dict: Debezium connector configuration
    
    Raises:
        ValueError: If table validation fails or configuration is invalid
    """
    import logging
    import random
    
    logger = logging.getLogger(__name__)
    
    # ====================================================================
    # Generate connector name with version
    # ====================================================================
    def generate_connector_name(client, db_config, version=None):
        client_name = client.name.lower().replace(' ', '_').replace('-', '_')
        client_name = ''.join(c for c in client_name if c.isalnum() or c == '_')
        db_name = db_config.connection_name.lower().replace(' ', '_').replace('-', '_')
        db_name = ''.join(c for c in db_name if c.isalnum() or c == '_')
        connector_name = f"{client_name}_{db_name}_connector"
        if version is not None:
            connector_name = f"{connector_name}_v_{version}"
        return connector_name
    
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)
    
    logger.info("=" * 80)
    logger.info("🔧 ORACLE CONNECTOR CONFIGURATION")
    logger.info("=" * 80)
    logger.info(f"   Connector: {connector_name}")
    
    # ====================================================================
    # STEP 1: Handle PDB detection and determine connection strategy
    # ====================================================================
    database_name_from_config = db_config.database_name
    
    def is_oracle_pdb(database_name: str) -> bool:
        """Check if database name indicates a Pluggable Database"""
        db_upper = database_name.upper()
        return 'PDB' in db_upper or db_upper.endswith('PDB1')
    
    def get_oracle_cdb_name(pdb_name: str) -> str:
        """Convert PDB name to CDB root name"""
        pdb_upper = pdb_name.upper()
        if pdb_upper.endswith('PDB1'):
            return pdb_upper[:-4]  # Remove 'PDB1'
        elif pdb_upper.endswith('PDB'):
            return pdb_upper[:-3]  # Remove 'PDB'
        return pdb_name
    
    is_pdb = is_oracle_pdb(database_name_from_config)
    
    if is_pdb:
        cdb_name = get_oracle_cdb_name(database_name_from_config)
        pdb_name = database_name_from_config
        
        logger.warning("=" * 80)
        logger.warning("✅ PDB DETECTED - Using CDB+PDB Strategy")
        logger.warning("=" * 80)
        logger.warning(f"   PDB Name: {pdb_name}")
        logger.warning(f"   CDB Root: {cdb_name}")
        logger.warning(f"   JDBC connects to: {cdb_name}")
        logger.warning(f"   database.pdb.name: {pdb_name}")
        logger.warning(f"   ✅ This prevents ORA-65040 error")
        logger.warning("=" * 80)
        
        connection_db = cdb_name  # ✅ Connect to CDB root
        
    else:
        logger.info(f"✅ Detected Container Database: {database_name_from_config}")
        connection_db = database_name_from_config
        pdb_name = None
    
    is_pdb = is_oracle_pdb(database_name_from_config)
    
    # ✅ CRITICAL FIX: Always connect to CDB, use database.pdb.name for PDB
    if is_pdb:
        cdb_name = get_oracle_cdb_name(database_name_from_config)
        pdb_name = database_name_from_config
        
        logger.warning("=" * 80)
        logger.warning("✅ PDB DETECTED - Using CDB+PDB Strategy")
        logger.warning("=" * 80)
        logger.warning(f"   PDB Name: {pdb_name}")
        logger.warning(f"   CDB Root: {cdb_name}")
        logger.warning(f"   JDBC connects to: {cdb_name}")
        logger.warning(f"   database.pdb.name: {pdb_name}")
        logger.warning(f"   ✅ This ensures 2-part table names (SCHEMA.TABLE)")
        logger.warning("=" * 80)
        
        connection_db = cdb_name  # ✅ Connect to CDB root
        
    else:
        logger.info(f"✅ Detected Container Database: {database_name_from_config}")
        connection_db = database_name_from_config
        pdb_name = None
    
    # ====================================================================
    # STEP 2: Extract schema name (remove C## prefix if present)
    # ====================================================================
    raw_username = db_config.username.upper()
    
    if raw_username.startswith('C##'):
        schema_name = raw_username[3:]  # Remove C## prefix
        logger.info(f"🔍 Common user detected: {raw_username}")
        logger.info(f"   Schema filter: {schema_name}")
    else:
        schema_name = raw_username
        logger.info(f"🔍 Local user detected: {raw_username}")
        logger.info(f"   Schema filter: {schema_name}")
    
    # ====================================================================
    # STEP 3: Build identifiers
    # ====================================================================
    topic_prefix = f"client_{client.id}_db_{db_config.id}"
    server_name = connector_name.replace('_connector', '')
    
    logger.info(f"📋 Configuration:")
    logger.info(f"   Topic Prefix: {topic_prefix}")
    logger.info(f"   Server Name: {server_name}")
    
    # ====================================================================
    # STEP 4: Build JDBC URL to CDB
    # ====================================================================
    oracle_mode = db_config.oracle_connection_mode or 'service'
    
    if oracle_mode == 'sid':
        jdbc_url = f"jdbc:oracle:thin:@{db_config.host}:{db_config.port}:{connection_db}"
    else:
        jdbc_url = f"jdbc:oracle:thin:@//{db_config.host}:{db_config.port}/{connection_db}"
    
    if pdb_name:
        logger.info(f"   ✅ Will query PDB: {pdb_name} (via database.pdb.name)")
    
    # ====================================================================
    # STEP 5: Build connector configuration
    # ====================================================================
    config = {
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        
        # Database connection to CDB
        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,  # Keep C## prefix for connection
        "database.password": db_config.get_decrypted_password(),
        "database.dbname": connection_db,  # ✅ CDB name (e.g., XE)
        "database.url": jdbc_url,
        
        # Server identification
        "database.server.name": server_name,
        "topic.prefix": topic_prefix,
        
        # Schema history
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",
        
        # Snapshot configuration
        "snapshot.mode": snapshot_mode,
        "snapshot.locking.mode": "none",
        "snapshot.fetch.size": "2000",
        
        # LogMiner configuration
        "log.mining.strategy": "online_catalog",
        "log.mining.batch.size.default": "1000",
        "log.mining.batch.size.min": "1000",
        "log.mining.batch.size.max": "100000",
        "log.mining.sleep.time.default.ms": "1000",
        "log.mining.sleep.time.min.ms": "0",
        "log.mining.sleep.time.max.ms": "3000",
        "log.mining.sleep.time.increment.ms": "200",
        "log.mining.archive.log.hours": "0",
        "log.mining.archive.log.only.mode": "false",
        "log.mining.session.max.ms": "0",
        "log.mining.transaction.retention.hours": "0",
        "log.mining.query.filter.mode": "in",
        # "log.mining.flush.table.name": "LOG_MINING_FLUSH",
        "log.mining.flush.table.name": f"{schema_name}.LOG_MINING_FLUSH",
        
        # ✅ CRITICAL: Incremental snapshots configuration
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",
        
        # ✅ CRITICAL: Signal configuration (BOTH channels required for Oracle)
        "signal.enabled.channels": "source,kafka",  # Must have BOTH
        "signal.kafka.topic": f"{topic_prefix}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,        
        "signal.data.collection": f"{schema_name}.DEBEZIUM_SIGNAL",
        
        "schema.include.list": schema_name,  # e.g., "CDCUSER"
        
        # Data type handling
        "decimal.handling.mode": "precise",
        "time.precision.mode": "adaptive_time_microseconds",
        "interval.handling.mode": "string",
        "binary.handling.mode": "bytes",
        
        # Other settings
        "tombstones.on.delete": "true",
        "include.schema.changes": "true",
        
        # Performance
        "max.queue.size": "8192",
        "max.batch.size": "2048",
        "poll.interval.ms": "1000",
        
        # Connection adapter
        "database.connection.adapter": "logminer",
        "database.jdbc.driver": "oracle.jdbc.OracleDriver",
        
        # Heartbeat
        "heartbeat.interval.ms": "10000",
        "heartbeat.action.query": "SELECT 1 FROM DUAL",
        
        # Error handling
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }
    
    # ✅ CRITICAL: Add PDB name parameter if detected
    if pdb_name:
        config["database.pdb.name"] = pdb_name
        logger.info(f"✅ Added database.pdb.name: {pdb_name}")
    
    # Remove None values
    config = {k: v for k, v in config.items() if v is not None}
    
    # ====================================================================
    # STEP 6: Format and VALIDATE table.include.list
    # ====================================================================
    if tables_whitelist:
        formatted_tables = []
        
        for table in tables_whitelist:
            table_upper = table.upper()
            
            logger.info(f"   Processing table: '{table}'")
            
            if '.' not in table_upper:
                # Add schema prefix
                formatted_table = f"{schema_name}.{table_upper}"
                logger.warning(f"      ⚠️ Added schema: {table} → {formatted_table}")
            else:
                # Already has schema
                parts = table_upper.split('.', 1)
                table_schema = parts[0].strip()
                table_name = parts[1].strip()
                
                # Remove C## prefix if present
                if table_schema.startswith('C##'):
                    table_schema = table_schema[3:]
                    logger.info(f"      🔧 Removed C## prefix: C##{table_schema} → {table_schema}")
                
                formatted_table = f"{table_schema}.{table_name}"
                logger.info(f"      ✅ Formatted: {formatted_table}")
            
            formatted_tables.append(formatted_table)
        
        # ✅ CRITICAL VALIDATION
        logger.info("=" * 80)
        logger.info(f"🛡️ ORACLE TABLE VALIDATION")
        logger.info("=" * 80)
        
        for i, table in enumerate(formatted_tables, 1):
            if '.' not in table:
                raise ValueError(
                    f"❌ Table {i}: '{table}' is missing schema prefix!\n"
                    f"   This will cause NullPointerException in LogMiner.\n"
                    f"   Required format: SCHEMA.TABLE"
                )
            
            parts = table.split('.', 1)
            if not parts[0].strip():
                raise ValueError(f"❌ Table {i}: '{table}' has empty schema (before dot)")
            if not parts[1].strip():
                raise ValueError(f"❌ Table {i}: '{table}' has empty table name (after dot)")
        
        logger.info(f"✅ All {len(formatted_tables)} tables validated successfully")
        logger.info("=" * 80)
        
        config["table.include.list"] = ",".join(formatted_tables)
        
        logger.info(f"✅ Validation passed: All {len(formatted_tables)} tables are valid")
        logger.info("=" * 80)
        logger.info(f"📋 TABLE FILTER CONFIGURED:")
        logger.info("=" * 80)
        for i, table in enumerate(formatted_tables, 1):
            # Extract table name for topic
            table_name_only = table.split('.')[1]
            expected_topic = f"{topic_prefix}.{schema_name}.{table_name_only}"
            logger.info(f"   • {table}")
            logger.info(f"     → Kafka Topic: {expected_topic}")
        logger.info("=" * 80)
    
    # Apply custom config from ReplicationConfig
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode
        
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
    
    # ====================================================================
    # FINAL SUMMARY
    # ====================================================================
    logger.info("=" * 80)
    logger.info("📊 ORACLE CONNECTOR CONFIGURATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"✅ Connector: {connector_name}")
    logger.info(f"✅ Connection DB (CDB): {connection_db}")
    if pdb_name:
        logger.info(f"✅ PDB Name: {pdb_name} (WHERE TABLES LIVE)")
    logger.info(f"✅ JDBC URL: {jdbc_url}")
    logger.info(f"✅ Username: {raw_username}")
    logger.info(f"✅ Schema filter: {schema_name}")
    logger.info(f"✅ Topic prefix: {topic_prefix}")
    logger.info(f"✅ Signal table: {schema_name}.DEBEZIUM_SIGNAL")
    logger.info(f"✅ Signal channels: source,kafka")
    logger.info(f"✅ Snapshot mode: {config.get('snapshot.mode', 'initial')}")
    
    if tables_whitelist:
        logger.info(f"✅ Tables to replicate: {len(formatted_tables)}")
        if formatted_tables:
            logger.info(f"   Format example: {formatted_tables[0]}")
        logger.info(f"   ⚠️ NOTE: Kafka topics INCLUDE schema prefix for Oracle")
        if formatted_tables:
            example_table = formatted_tables[0].split('.')[1]
            logger.info(f"   Example: {formatted_tables[0]} → Topic: {topic_prefix}.{schema_name}.{example_table}")
    
    logger.info("=" * 80)
    
    return config


def create_oracle_signal_table(db_config, schema_name=None):
    """
    Create DEBEZIUM_SIGNAL table in Oracle database for incremental snapshots.
    
    ✅ CRITICAL: Oracle incremental snapshots REQUIRE this table in the database.
    This is non-negotiable due to LogMiner architecture.
    
    The signal table stores incremental snapshot state during execution.
    Without it, you'll get: "sinalling data collection is not provided"
    
    Args:
        db_config: ClientDatabase instance
        schema_name: Schema name (optional, will auto-detect from username)
    
    Returns:
        tuple: (success: bool, message: str)
        
    Example:
        success, msg = create_oracle_signal_table(db_config)
        if not success:
            raise Exception(f"Signal table creation failed: {msg}")
    """
    import logging
    from sqlalchemy import text
    from client.utils.database_utils import get_database_engine
    
    logger = logging.getLogger(__name__)
    
    try:
        # Determine schema name
        if not schema_name:
            username = db_config.username.upper()
            # Remove C## prefix if present (common user)
            schema_name = username[3:] if username.startswith('C##') else username
        
        logger.info("=" * 80)
        logger.info(f"🔧 ORACLE SIGNAL TABLE CHECK")
        logger.info("=" * 80)
        logger.info(f"   Database: {db_config.database_name}")
        logger.info(f"   Schema: {schema_name}")
        logger.info(f"   Table: DEBEZIUM_SIGNAL")
        logger.info(f"   Purpose: Required for incremental snapshots")
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # ================================================================
            # Check if table already exists
            # ================================================================
            check_query = text("""
                SELECT COUNT(*) as cnt
                FROM all_tables 
                WHERE owner = :schema_name
                  AND table_name = 'DEBEZIUM_SIGNAL'
            """)
            
            result = conn.execute(check_query, {"schema_name": schema_name})
            exists = result.scalar() > 0
            
            if exists:
                logger.info(f"✅ Signal table already exists: {schema_name}.DEBEZIUM_SIGNAL")
                
                # Verify table structure
                col_query = text("""
                    SELECT column_name, data_type, data_length
                    FROM all_tab_columns
                    WHERE owner = :schema_name
                      AND table_name = 'DEBEZIUM_SIGNAL'
                    ORDER BY column_id
                """)
                
                cols = conn.execute(col_query, {"schema_name": schema_name}).fetchall()
                
                logger.info(f"   Columns found:")
                for col in cols:
                    logger.info(f"      • {col[0]} ({col[1]})")
                
                # Verify we have the required columns
                col_names = [c[0].upper() for c in cols]
                required_cols = ['ID', 'TYPE', 'DATA']
                missing_cols = [c for c in required_cols if c not in col_names]
                
                if missing_cols:
                    logger.error(f"   ❌ Missing columns: {missing_cols}")
                    return False, f"Signal table exists but missing columns: {missing_cols}"
                
                logger.info("=" * 80)
                return True, f"Signal table verified: {schema_name}.DEBEZIUM_SIGNAL"
            
            # ================================================================
            # Create signal table (doesn't exist)
            # ================================================================
            logger.info(f"🔨 Creating signal table...")
            logger.info(f"   Location: {schema_name}.DEBEZIUM_SIGNAL")
            
            create_query = text(f"""
                CREATE TABLE {schema_name}.DEBEZIUM_SIGNAL (
                    id VARCHAR2(100) PRIMARY KEY,
                    type VARCHAR2(100) NOT NULL,
                    data CLOB
                )
            """)
            
            conn.execute(create_query)
            conn.commit()
            
            logger.info(f"✅ Successfully created signal table!")
            logger.info(f"   Table: {schema_name}.DEBEZIUM_SIGNAL")
            logger.info(f"   Columns:")
            logger.info(f"      • id (VARCHAR2(100), PRIMARY KEY)")
            logger.info(f"      • type (VARCHAR2(100), NOT NULL)")
            logger.info(f"      • data (CLOB)")
            logger.info(f"   ")
            logger.info(f"   This table will be used by Debezium to:")
            logger.info(f"      1. Track incremental snapshot progress")
            logger.info(f"      2. Store window boundaries during snapshot")
            logger.info(f"      3. Coordinate with LogMiner for data extraction")
            logger.info("=" * 80)
            
            return True, f"Created signal table: {schema_name}.DEBEZIUM_SIGNAL"
        
    except Exception as e:
        error_msg = f"Failed to create Oracle signal table: {str(e)}"
        logger.error("=" * 80)
        logger.error(f"❌ ORACLE SIGNAL TABLE CREATION FAILED")
        logger.error("=" * 80)
        logger.error(f"   Error: {error_msg}")
        logger.error(f"   ")
        logger.error(f"   Possible causes:")
        logger.error(f"      1. User lacks CREATE TABLE permission")
        logger.error(f"      2. Insufficient tablespace quota")
        logger.error(f"      3. Schema name incorrect")
        logger.error(f"   ")
        logger.error(f"   Required SQL permission:")
        logger.error(f"      GRANT CREATE TABLE TO {schema_name};")
        logger.error("=" * 80)
        logger.error(error_msg, exc_info=True)
        return False, error_msg
    
    finally:
        if 'engine' in locals():
            engine.dispose()


def create_oracle_log_mining_flush_table(db_config, schema_name=None):
    """
    Create LOG_MINING_FLUSH table in Oracle database for LogMiner.
    
    This table is used by Debezium to manage LogMiner state.
    
    Args:
        db_config: ClientDatabase instance
        schema_name: Schema name (optional, will auto-detect from username)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    import logging
    from sqlalchemy import text
    from client.utils.database_utils import get_database_engine
    
    logger = logging.getLogger(__name__)
    
    try:
        # Determine schema name
        if not schema_name:
            username = db_config.username.upper()
            schema_name = username[3:] if username.startswith('C##') else username
        
        logger.info("=" * 80)
        logger.info(f"🔧 ORACLE LOG_MINING_FLUSH TABLE CHECK")
        logger.info("=" * 80)
        logger.info(f"   Schema: {schema_name}")
        logger.info(f"   Table: LOG_MINING_FLUSH")
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # Check if table exists
            check_query = text("""
                SELECT COUNT(*) as cnt
                FROM all_tables 
                WHERE owner = :schema_name
                  AND table_name = 'LOG_MINING_FLUSH'
            """)
            
            result = conn.execute(check_query, {"schema_name": schema_name})
            exists = result.scalar() > 0
            
            if exists:
                logger.info(f"✅ LOG_MINING_FLUSH table already exists")
                logger.info("=" * 80)
                return True, f"LOG_MINING_FLUSH table verified: {schema_name}.LOG_MINING_FLUSH"
            
            # Create the table
            logger.info(f"🔨 Creating LOG_MINING_FLUSH table...")
            
            create_query = text(f"""
                CREATE TABLE {schema_name}.LOG_MINING_FLUSH (
                    id NUMBER(19) PRIMARY KEY
                )
            """)
            
            conn.execute(create_query)
            conn.commit()
            
            logger.info(f"✅ Successfully created LOG_MINING_FLUSH table!")
            logger.info("=" * 80)
            
            return True, f"Created LOG_MINING_FLUSH table: {schema_name}.LOG_MINING_FLUSH"
        
    except Exception as e:
        error_msg = f"Failed to create LOG_MINING_FLUSH table: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg
    
    finally:
        if 'engine' in locals():
            engine.dispose()

def verify_oracle_signal_table(db_config, schema_name=None):
    """
    Verify Oracle signal table exists and has correct structure.
    
    Use this before starting replication to ensure incremental snapshots will work.
    
    Args:
        db_config: ClientDatabase instance
        schema_name: Schema name (optional)
        
    Returns:
        tuple: (is_valid: bool, message: str)
    """
    import logging
    from sqlalchemy import text
    from client.utils.database_utils import get_database_engine
    
    logger = logging.getLogger(__name__)
    
    try:
        if not schema_name:
            username = db_config.username.upper()
            schema_name = username[3:] if username.startswith('C##') else username
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # Check existence
            check_query = text("""
                SELECT COUNT(*) as cnt
                FROM all_tables 
                WHERE owner = :schema_name
                  AND table_name = 'DEBEZIUM_SIGNAL'
            """)
            
            result = conn.execute(check_query, {"schema_name": schema_name})
            exists = result.scalar() > 0
            
            if not exists:
                return False, f"Signal table does not exist: {schema_name}.DEBEZIUM_SIGNAL"
            
            # Check structure
            col_query = text("""
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :schema_name
                  AND table_name = 'DEBEZIUM_SIGNAL'
            """)
            
            cols = conn.execute(col_query, {"schema_name": schema_name}).fetchall()
            col_names = [c[0].upper() for c in cols]
            
            required = ['ID', 'TYPE', 'DATA']
            missing = [c for c in required if c not in col_names]
            
            if missing:
                return False, f"Signal table missing columns: {missing}"
            
            return True, f"Signal table valid: {schema_name}.DEBEZIUM_SIGNAL"
        
    except Exception as e:
        return False, f"Verification failed: {str(e)}"
    
    finally:
        if 'engine' in locals():
            engine.dispose()

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