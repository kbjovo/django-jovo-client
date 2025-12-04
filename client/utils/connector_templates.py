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


"""
Complete PostgreSQL Debezium Connector Configuration
Replace get_postgresql_connector_config in connector_templates.py with this
"""

def get_postgresql_connector_config(
    client: Client,
    db_config: ClientDatabase,
    replication_config: Optional[ReplicationConfig] = None,
    tables_whitelist: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = 'localhost:9092',
    schema_registry_url: str = 'http://localhost:8081',
    schema_name: str = 'public',
    snapshot_mode: str = 'initial',
) -> Dict[str, Any]:
    """
    Generate PostgreSQL Debezium connector configuration (PRODUCTION-READY)
    
    Key features:
    1. ✅ Uses database.dbname (not database.include.list)
    2. ✅ Auto-creates publication (no manual setup needed)
    3. ✅ Proper slot naming (PostgreSQL 63-char limit)
    4. ✅ No table locking during snapshot
    5. ✅ Kafka-based heartbeat (no DB table needed)
    6. ✅ Incremental snapshot support
    7. ✅ Signal-based control
    
    Args:
        client: Client instance
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance (optional)
        tables_whitelist: List of tables to replicate
        kafka_bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema registry URL
        schema_name: PostgreSQL schema name (default: public)
        snapshot_mode: 'initial', 'never', 'always', 'when_needed'

    Returns:
        Dict[str, Any]: Connector configuration
    """
    # Generate connector name with version
    version = replication_config.connector_version if replication_config else None
    connector_name = generate_connector_name(client, db_config, version=version)
    
    # ===================================================================
    # CRITICAL: Generate safe slot name
    # PostgreSQL replication slot names have strict requirements:
    # - Max 63 characters
    # - Lowercase letters, numbers, underscores only
    # - Must be unique across the database
    # ===================================================================
    safe_slot_name = f"debezium_{client.id}_{db_config.id}".lower()
    safe_slot_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in safe_slot_name)
    safe_slot_name = safe_slot_name[:63]  # PostgreSQL limit
    
    # ===================================================================
    # Generate unique publication name
    # Publication defines which tables are replicated
    # ===================================================================
    publication_name = f"debezium_pub_{client.id}_{db_config.id}".lower()
    publication_name = ''.join(c if (c.isalnum() or c == '_') else '_' for c in publication_name)
    publication_name = publication_name[:63]  # PostgreSQL limit
    
    logger.info(f"PostgreSQL connector configuration:")
    logger.info(f"  Connector name: {connector_name}")
    logger.info(f"  Replication slot: {safe_slot_name}")
    logger.info(f"  Publication: {publication_name}")
    logger.info(f"  Database: {db_config.database_name}")
    logger.info(f"  Schema: {schema_name}")
    logger.info(f"  Snapshot mode: {snapshot_mode}")
    
    config = {
        # ===================================================================
        # Connector Class
        # ===================================================================
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        
        # ===================================================================
        # Database Connection (CRITICAL FIX)
        # MySQL uses: database.include.list
        # PostgreSQL uses: database.dbname (single database only)
        # ===================================================================
        "database.hostname": db_config.host,
        "database.port": str(db_config.port),
        "database.user": db_config.username,
        "database.password": db_config.get_decrypted_password(),
        "database.dbname": db_config.database_name,  # ✅ CORRECT for PostgreSQL

        # ===================================================================
        # Server Identification
        # ===================================================================
        "database.server.name": connector_name.replace('_connector', ''),
        "topic.prefix": f"client_{client.id}_db_{db_config.id}",

        # ===================================================================
        # Logical Decoding Plugin
        # pgoutput: Built-in plugin (PostgreSQL 10+), recommended
        # decoderbufs: Requires installation
        # wal2json: Requires installation
        # ===================================================================
        "plugin.name": "pgoutput",
        
        # ===================================================================
        # Replication Slot Configuration
        # Slot stores CDC position (like MySQL binlog position)
        # Persists across connector restarts
        # ===================================================================
        "slot.name": safe_slot_name,
        "slot.drop.on.stop": "false",  # Keep slot when connector stops
        "slot.stream.params": "",  # Additional slot parameters (optional)
        
        # ===================================================================
        # Publication Configuration
        # Publication = which tables are replicated (like MySQL table whitelist)
        # autocreate.mode options:
        # - filtered: Auto-create for selected tables only
        # - all_tables: Replicate all tables in database
        # - disabled: Don't auto-create (requires manual setup)
        # ===================================================================
        "publication.name": publication_name,
        "publication.autocreate.mode": "filtered",  # ✅ Auto-create for selected tables
        
        # ===================================================================
        # Schema History (Kafka-based)
        # Tracks DDL changes (ALTER TABLE, etc.)
        # ===================================================================
        "schema.history.internal.kafka.bootstrap.servers": kafka_bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{connector_name}",

        # ===================================================================
        # Snapshot Configuration
        # Modes:
        # - initial: Snapshot on first start, then CDC (recommended)
        # - never: CDC only, no snapshot
        # - always: Always snapshot on start (slow, use for testing)
        # - when_needed: Snapshot if no offset found
        # - initial_only: Snapshot then stop
        # ===================================================================
        "snapshot.mode": snapshot_mode,
        "snapshot.locking.mode": "none",  # ✅ Don't lock tables (read-only friendly)
        
        # ===================================================================
        # Incremental Snapshot
        # Allows snapshotting new tables after connector is created
        # Triggered via Kafka signals (no DB modification needed)
        # ===================================================================
        "incremental.snapshot.allow.schema.changes": "true",
        "incremental.snapshot.chunk.size": "1024",

        # ===================================================================
        # Kafka-based Signals
        # Control connector via Kafka messages (pause, snapshot, etc.)
        # No source DB modification required (read-only friendly)
        # ===================================================================
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": f"client_{client.id}_db_{db_config.id}.signals",
        "signal.kafka.bootstrap.servers": kafka_bootstrap_servers,

        # ===================================================================
        # Schema Change Events
        # Include DDL changes (CREATE TABLE, ALTER TABLE, etc.)
        # ===================================================================
        "include.schema.changes": "true",

        # ===================================================================
        # Data Type Handling
        # ===================================================================
        "decimal.handling.mode": "precise",  # Keep decimal precision
        "time.precision.mode": "adaptive_time_microseconds",  # Microsecond precision
        
        # ===================================================================
        # HSTORE Support (PostgreSQL key-value type)
        # ===================================================================
        "hstore.handling.mode": "json",  # Convert HSTORE to JSON
        
        # ===================================================================
        # Interval Handling (PostgreSQL time intervals)
        # ===================================================================
        "interval.handling.mode": "string",  # Convert intervals to strings
        
        # ===================================================================
        # Schema Whitelist
        # PostgreSQL organizes tables into schemas (like MySQL databases)
        # Common schemas: public, information_schema, pg_catalog
        # ===================================================================
        "schema.include.list": schema_name,
        
        # ===================================================================
        # Heartbeat Configuration
        # Prevents connection timeout during idle periods
        # FIXED: Use Kafka-based heartbeat (no DB table needed)
        # ===================================================================
        "heartbeat.interval.ms": "10000",  # 10 seconds
        "heartbeat.action.query": "",  # Empty = Kafka heartbeat (no DB write)
        
        # ===================================================================
        # Tombstone Events
        # Send null message after DELETE (for log compaction)
        # ===================================================================
        "tombstones.on.delete": "true",
        
        # ===================================================================
        # Performance Tuning
        # ===================================================================
        "max.queue.size": "8192",
        "max.batch.size": "2048",
        "poll.interval.ms": "1000",
        
        # ===================================================================
        # Connection Management
        # ===================================================================
        "connect.timeout.ms": "30000",
        "connect.max.attempts": "3",
        "connect.backoff.initial.delay.ms": "1000",
        "connect.backoff.max.delay.ms": "10000",
        
        # ===================================================================
        # Replica Identity (PostgreSQL-specific)
        # Controls which columns are included in UPDATE/DELETE events
        # DEFAULT: Only primary key columns
        # FULL: All columns (requires ALTER TABLE ... REPLICA IDENTITY FULL)
        # ===================================================================
        # Note: This is set per-table in PostgreSQL, not in connector config
        # See documentation for changing replica identity
    }
    
    # ===================================================================
    # Table Whitelist (CRITICAL FORMAT)
    # PostgreSQL requires schema-qualified table names
    # Format: schema.table1,schema.table2
    # Example: public.users,public.orders
    # ===================================================================
    if tables_whitelist:
        tables_full = [f"{schema_name}.{table}" for table in tables_whitelist]
        config["table.include.list"] = ",".join(tables_full)
        logger.info(f"Adding table whitelist: {len(tables_whitelist)} tables")
        logger.debug(f"Tables: {', '.join(tables_full)}")

    # ===================================================================
    # Custom Configuration Override
    # Allows per-replication customization
    # ===================================================================
    if replication_config:
        if hasattr(replication_config, 'snapshot_mode') and replication_config.snapshot_mode:
            config["snapshot.mode"] = replication_config.snapshot_mode
            logger.info(f"Overriding snapshot mode: {replication_config.snapshot_mode}")

        if hasattr(replication_config, 'custom_config') and replication_config.custom_config:
            config.update(replication_config.custom_config)
            logger.info(f"Applied custom config overrides")
    
    logger.info(f"✅ Generated PostgreSQL connector config for: {connector_name}")
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