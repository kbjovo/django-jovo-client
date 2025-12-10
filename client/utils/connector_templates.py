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


# def get_sqlserver_connector_config(
#     client: Client,
#     db_config: ClientDatabase,
#     replication_config: Optional[ReplicationConfig] = None,
#     tables_whitelist: Optional[List[str]] = None,
#     kafka_bootstrap_servers: str = 'localhost:9092',
#     schema_registry_url: str = 'http://localhost:8081',
#     schema_name: str = 'dbo',
#     snapshot_mode: str = 'initial',
#     use_docker_internal_host: bool = True,
# ) -> Dict[str, Any]:
#     """
#     CRITICAL FIX: SQL Server requires matching database.server.name and topic.prefix
#     """
#     version = replication_config.connector_version if replication_config else None
#     connector_name = generate_connector_name(client, db_config, version=version)
    
#     db_host = db_config.host
#     if use_docker_internal_host:
#         if db_host in ['localhost', '127.0.0.1']:
#             db_host = 'mssql2019'
    
#     # ✅ CRITICAL: Both must be the same for SQL Server
#     server_name = f"client_{client.id}_db_{db_config.id}"
    
#     config = {
#         "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        
#         "database.hostname": db_host,
#         "database.port": str(db_config.port),
#         "database.user": db_config.username,
#         "database.password": db_config.get_decrypted_password(),
        
#         # ✅ Use database.names (not database.include.list)
#         "database.names": db_config.database_name,
        
#         # ✅ CRITICAL: These MUST match
#         "database.server.name": server_name,
#         "topic.prefix": server_name,
        
#         "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
#         "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",
        
#         "snapshot.mode": snapshot_mode,
#         "snapshot.isolation.mode": "read_committed",
        
#         "include.schema.changes": "true",
        
#         "incremental.snapshot.allow.schema.changes": "true",
#         "incremental.snapshot.chunk.size": "1024",
        
#         "signal.enabled.channels": "kafka",
#         "signal.kafka.topic": f"{server_name}.signals",
#         "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        
#         "database.encrypt": "false",
        
#         "decimal.handling.mode": "precise",
#         "binary.handling.mode": "bytes",
#         "time.precision.mode": "adaptive_time_microseconds",
        
#         "tombstones.on.delete": "true",
        
#         "max.queue.size": "8192",
#         "max.batch.size": "2048",
#         "poll.interval.ms": "1000",
        
#         "database.connection.timeout.ms": "30000",
#         "heartbeat.interval.ms": "10000",
#     }
    
#     if tables_whitelist:
#         tables_full = []
#         for table in tables_whitelist:
#             if '.' in table:
#                 # Has schema: 'dbo.Customers'
#                 tables_full.append(f"{db_config.database_name}.{table}")
#             else:
#                 # No schema: 'Customers'
#                 tables_full.append(f"{db_config.database_name}.{schema_name}.{table}")
        
#         config["table.include.list"] = ",".join(tables_full)
        
#         logger.info(f"✅ SQL Server table whitelist:")
#         logger.info(f"   Input: {tables_whitelist}")
#         logger.info(f"   Formatted: {tables_full}")
#         logger.info(f"   Expected topics:")
#         for table in tables_whitelist:
#             table_name = table.split('.')[-1] if '.' in table else table
#             expected_topic = f"{server_name}.{db_config.database_name}.{schema_name}.{table_name}"
#             logger.info(f"      {expected_topic}")
    
#     if replication_config:
#         if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
#             config["snapshot.mode"] = replication_config.snapshot_mode
        
#         if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
#             config.update(replication_config.custom_config)
    
#     logger.info(f"✅ SQL Server connector config:")
#     logger.info(f"   Connector: {connector_name}")
#     logger.info(f"   Server name: {server_name}")
#     logger.info(f"   Topic prefix: {server_name}")
#     logger.info(f"   Database: {db_config.database_name}")
    
#     return config


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
    ✅ FIXED: SQL Server connector with correct topic naming and case sensitivity

    Key fixes:
    - database.server.name and topic.prefix must be DIFFERENT
    - database.server.name: Logical server identifier for Debezium internals
    - topic.prefix: Kafka topic prefix for actual topic names
    - SQL Server is CASE-SENSITIVE for table.include.list - must match exact database name case
    """
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)

    db_host = db_config.host
    if use_docker_internal_host:
        if db_host in ['localhost', '127.0.0.1']:
            db_host = 'mssql2019'

    # ✅ CRITICAL FIX: Use different values for server name and topic prefix
    # database.server.name: Used internally by Debezium for offset tracking
    # Format: sqlserver_{connector_name} (must be unique per connector)
    server_name = f"sqlserver_{client.id}_{db_config.id}"

    # topic.prefix: Used for Kafka topic names
    # Format: client_{client_id}_db_{db_id} (consistent with other DB types)
    topic_prefix = f"client_{client.id}_db_{db_config.id}"

    # ✅ CRITICAL: SQL Server is CASE-SENSITIVE for database.names
    # Must use the EXACT case as stored in SQL Server for both database.names AND table.include.list
    # Using lowercase causes "no changes will be captured" warning in CDC streaming

    # ⚠️ WARNING: If the database name in Django config doesn't match the exact case in SQL Server,
    # CDC will fail with "no changes will be captured" warning.
    # Solution: Update database_name in Django to match SQL Server's exact case
    # Or use Option 2: Query SQL Server to get the actual case (requires additional DB connection)

    logger.info(f"🔧 SQL Server connector configuration:")
    logger.info(f"   Connector name: {connector_name}")
    logger.info(f"   Server name (internal): {server_name}")
    logger.info(f"   Topic prefix (Kafka): {topic_prefix}")
    logger.info(f"   Database name from config: {db_config.database_name}")
    logger.warning(f"   ⚠️  IMPORTANT: Database name must match EXACT case in SQL Server!")
    logger.warning(f"   ⚠️  If you entered 'appdb' but SQL Server has 'AppDB', CDC will NOT work!")
    logger.info(f"   Expected topic format: {topic_prefix}.{db_config.database_name}.{schema_name}.{{table}}")

    config = {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",

        "database.hostname": db_host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),

        "database.names": db_config.database_name,

        # ✅ CRITICAL FIX: These MUST be different for SQL Server
        "database.server.name": server_name,      # Internal identifier
        "topic.prefix": topic_prefix,              # Kafka topic prefix

        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",

        "snapshot.mode": snapshot_mode,
        "snapshot.isolation.mode": "read_committed",

        "include.schema.changes": "true",

        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",

        # ✅ Signal topic uses topic_prefix (not server_name)
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": f"{topic_prefix}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,

        "database.encrypt": "false",

        "decimal.handling.mode": "precise",
        "binary.handling.mode": "bytes",
        "time.precision.mode": "adaptive_time_microseconds",

        "tombstones.on.delete": "true",

        "max.queue.size": "8192",
        "max.batch.size": "2048",
        "poll.interval.ms": "1000",

        "database.connection.timeout.ms": "30000",
        "heartbeat.interval.ms": "10000",
    }

    # Format: "dbo.Customers,dbo.Orders" (NOT "AppDB.dbo.Customers")
    if tables_whitelist:
        logger.warning(f"⚠️  SQL Server is CASE-SENSITIVE for table filters!")
        logger.warning(f"⚠️  Ensure database name case matches SQL Server: {db_config.database_name}")

        tables_full = []
        for table in tables_whitelist:
            # Preserve exact case provided by user for table names
            if '.' in table:
                # Already has schema: 'dbo.Customers' - use as-is
                tables_full.append(table)
            else:
                # No schema: 'Customers' - add schema prefix
                tables_full.append(f"{schema_name}.{table}")

        config["table.include.list"] = ",".join(tables_full)
    
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode
        
        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
    
    logger.info(f"✅ SQL Server connector config:")
    logger.info(f"   Connector: {connector_name}")
    logger.info(f"   Server name: {server_name}")
    logger.info(f"   Topic prefix: {topic_prefix}")
    logger.info(f"   Database: {db_config.database_name}")
    
    return config


 
def get_oracle_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
    schema_registry_url: str = 'http://localhost:8081',
    snapshot_mode: str = 'when_needed',
) -> Dict[str, Any]:
    """
    Generate Oracle Debezium connector configuration

    Args:
        client: Client instance
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance (optional)
        tables_whitelist: List of tables to replicate
        kafka_bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema registry URL

    Returns:
        Dict[str, Any]: Connector configuration
    """
    # Generate connector name with version if replication_config is provided
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)
    
    config = {
        # Connector class
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        
        # Database connection
        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),
        "database.include.list": db_config.database_name,

        # Server identification
        "database.server.name": connector_name.replace('_connector', ''),
        "topic.prefix": f"client_{client.id}_db_{db_config.id}",

        # Use Kafka-based schema history (more reliable in containerized environments)
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",

        # Snapshot mode - configurable (Debezium 3.x)
        # never: No snapshot, CDC only
        # when_needed: Re-snapshot if offsets are missing or incomplete
        # initial: Full snapshot on first connector creation (respects existing offsets)
        # always: ALWAYS perform snapshot on every connector start (ignores offsets)
        # no_data: Capture schema only, no data (use after manual data copy)
        "snapshot.mode": snapshot_mode,

        # Incremental snapshot configuration (for adding new tables after creation)
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",

        # Kafka-based signals (no source DB modification required)
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,

        "database.allowPublicKeyRetrieval": "true",

        # Log mining settings
        "log.mining.strategy": "online_catalog",
        "log.mining.batch.size.default": "1000",
        "log.mining.sleep.time.default.ms": "1000",
        "log.mining.sleep.time.min.ms": "0",
        "log.mining.sleep.time.max.ms": "3000",
        "log.mining.sleep.time.increment.ms": "200",
    }
    
    # Add table whitelist if specified
    if tables_whitelist:
        config["table.include.list"] = ",".join(tables_whitelist)
        logger.info(f"Adding table whitelist: {len(tables_whitelist)} tables")

    # Add custom configuration
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode

        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
    
    logger.info(f"Generated Oracle connector config for: {connector_name}")
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