"""
JDBC Sink Connector Configuration Templates

This module handles the configuration generation for Kafka JDBC Sink Connectors
that write CDC data from Kafka topics to target databases (MySQL and PostgreSQL).

Key Features:
- MySQL JDBC Sink Connector configuration
- PostgreSQL JDBC Sink Connector configuration
- Configurable settings with sensible defaults
- UI-customizable options via custom_config parameter
- Primary keys passed via custom_config from database metadata
"""

import logging
from typing import Dict, List, Optional, Any
from client.models.client import Client
from client.models.database import ClientDatabase

logger = logging.getLogger(__name__)


def get_mysql_sink_connector_config(
    client: Client,
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate MySQL JDBC Sink Connector configuration

    Primary keys should be passed via custom_config['primary.key.fields'].

    Args:
        client: Client instance
        db_config: Target ClientDatabase instance (must be MySQL)
        topics: List of Kafka topics to consume (e.g., ['client_1_db_2.mydb.users'])
        kafka_bootstrap_servers: Kafka bootstrap servers (defaults to settings)
        delete_enabled: Enable DELETE operations
        custom_config: Optional custom configuration to override defaults
                      Should include 'primary.key.fields' for upsert mode

    Returns:
        Dict[str, Any]: JDBC Sink Connector configuration
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka-1:29092,kafka-2:29092,kafka-3:29092'
        )

    if db_config.db_type != 'mysql':
        raise ValueError(f"MySQL sink connector requires MySQL database, got: {db_config.db_type}")

    connector_name = f"{client.name.lower().replace(' ', '_')}_{db_config.connection_name.lower().replace(' ', '_')}_sink"
    connector_name = ''.join(c for c in connector_name if c.isalnum() or c == '_')

    # Primary key fields should be passed via custom_config
    # No longer extracting from Schema Registry as it may not have schemas yet
    primary_key_fields = custom_config.get('primary.key.fields', '') if custom_config else ''

    # Topic regex matches ALL databases for this client (not just one database)
    # This allows a single sink connector to handle topics from all source databases
    topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

    # Build JDBC connection URL
    jdbc_url = f"jdbc:mysql://{db_config.host}:{db_config.port}/{db_config.database_name}"

    # Default configuration
    config = {
        # Connector class
        "name": connector_name,
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",

        # Connection settings
        "connection.url": jdbc_url,
        "connection.username": db_config.username,  # Fixed: was connection.user
        "connection.password": db_config.get_decrypted_password(),

        # Topics to consume
        "topics.regex": topic_regex,

        # ✅ FIX: Extract {database}_{table} from topic using RegexRouter SMT
        # Example: 'client_2_db_5.kbe.busyuk_items' → 'kbe_busyuk_items'
        # This extracts last two segments and joins with underscore
        "transforms": "extractTableName",
        "transforms.extractTableName.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.extractTableName.regex": ".*\\.([^.]+)\\.([^.]+)",
        "transforms.extractTableName.replacement": "$1_$2",

        # Table and schema management
        "schema.evolution": "basic",  # Options: none, basic
        "collection.name.format": "${topic}",  # Uses transformed topic (database_table)

        # Insert mode
        "insert.mode": "upsert",  # Options: insert, upsert, update
        "primary.key.mode": "record_key",  # Extract PK from Kafka message key
        "delete.enabled": str(delete_enabled).lower(),  # Enable/disable delete operations (requires PKs)

        # Batch settings
        "batch.size": "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",

        # Error handling
        "errors.tolerance": "none",  # Options: none, all
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",

        # Connection pool settings
        "connection.attempts": "3",
        "connection.backoff.ms": "10000",

        # Topic discovery settings - reduce delay for new topics matching regex
        # Default is 300000ms (5 min), set to 5 seconds for faster discovery
        "topic.tracking.refresh.interval.ms": "5000",

        # Consumer metadata refresh - speeds up initial topic subscription
        # Default is 300000ms (5 min), set to 10 seconds for faster initial discovery
        "consumer.override.metadata.max.age.ms": "10000",
    }

    # Add primary.key.fields only if explicitly provided
    # When using record_key mode, omitting this allows auto-detection from message key schema
    if primary_key_fields:
        config["primary.key.fields"] = primary_key_fields

    # Override with custom configuration if provided
    if custom_config:
        config.update(custom_config)

    logger.info(f"Generated MySQL JDBC Sink connector config: {connector_name}")
    return config


def get_postgresql_sink_connector_config(
    client: Client,
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate PostgreSQL JDBC Sink Connector configuration

    Primary keys should be passed via custom_config['primary.key.fields'].

    Args:
        client: Client instance
        db_config: Target ClientDatabase instance (must be PostgreSQL)
        topics: List of Kafka topics to consume (e.g., ['client_1_db_2.mydb.users'])
        kafka_bootstrap_servers: Kafka bootstrap servers (defaults to settings)
        delete_enabled: Enable DELETE operations
        custom_config: Optional custom configuration to override defaults
                      Should include 'primary.key.fields' for upsert mode

    Returns:
        Dict[str, Any]: JDBC Sink Connector configuration
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka-1:29092,kafka-2:29092,kafka-3:29092'
        )

    if db_config.db_type != 'postgresql':
        raise ValueError(f"PostgreSQL sink connector requires PostgreSQL database, got: {db_config.db_type}")

    connector_name = f"{client.name.lower().replace(' ', '_')}_{db_config.connection_name.lower().replace(' ', '_')}_sink"
    connector_name = ''.join(c for c in connector_name if c.isalnum() or c == '_')

    # Primary key fields should be passed via custom_config
    # No longer extracting from Schema Registry as it may not have schemas yet
    primary_key_fields = custom_config.get('primary.key.fields', '') if custom_config else ''

    # Build JDBC connection URL
    jdbc_url = f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database_name}"

    # Topic regex matches ALL databases for this client (not just one database)
    # This allows a single sink connector to handle topics from all source databases
    topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

    # Default configuration
    config = {
        # Connector class
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",

        # Connection settings
        "connection.url": jdbc_url,
        "connection.username": db_config.username,  # Fixed: was connection.user
        "connection.password": db_config.get_decrypted_password(),

        # Topics to consume - use regex by default for auto-discovery of new topics
        # If explicit topics list is provided, it will be overridden via custom_config
        "topics.regex": topic_regex,

        # ✅ FIX: Extract {schema}_{table} from topic using RegexRouter SMT
        # Example: 'client_2_db_5.public.orders' → 'public_orders'
        # This extracts last two segments and joins with underscore
        "transforms": "extractTableName",
        "transforms.extractTableName.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.extractTableName.regex": ".*\\.([^.]+)\\.([^.]+)",
        "transforms.extractTableName.replacement": "$1_$2",

        # Table and schema management
        "schema.evolution": "basic",  # Options: none, basic
        "collection.name.format": "${topic}",  # Uses transformed topic (schema_table)

        # Insert mode
        "insert.mode": "upsert",  # Options: insert, upsert, update
        "primary.key.mode": "record_key",  # Extract PK from Kafka message key
        "delete.enabled": str(delete_enabled).lower(),  # Enable/disable delete operations (requires PKs)

        # Batch settings
        "batch.size": "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",

        # Error handling
        "errors.tolerance": "none",  # Options: none, all
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",

        # Connection pool settings
        "connection.attempts": "3",
        "connection.backoff.ms": "10000",

        # Topic discovery settings - reduce delay for new topics matching regex
        # Default is 300000ms (5 min), set to 5 seconds for faster discovery
        "topic.tracking.refresh.interval.ms": "5000",

        # Consumer metadata refresh - speeds up initial topic subscription
        # Default is 300000ms (5 min), set to 10 seconds for faster initial discovery
        "consumer.override.metadata.max.age.ms": "10000",
    }

    # Add primary.key.fields only if explicitly provided
    # When using record_key mode, omitting this allows auto-detection from message key schema
    if primary_key_fields:
        config["primary.key.fields"] = primary_key_fields

    # Override with custom configuration if provided
    if custom_config:
        config.update(custom_config)

    logger.info(f"Generated PostgreSQL JDBC Sink connector config: {connector_name}")
    return config


def get_sink_connector_config_for_database(
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
    primary_key_fields: Optional[str] = None,  # Deprecated - pass via custom_config instead
) -> Optional[Dict[str, Any]]:
    """
    Get JDBC Sink connector configuration based on target database type

    Primary keys should be passed via custom_config['primary.key.fields'].

    Args:
        db_config: Target ClientDatabase instance
        topics: List of Kafka topics to consume
        kafka_bootstrap_servers: Kafka bootstrap servers (defaults to settings)
        delete_enabled: Enable DELETE operations (requires primary keys)
        custom_config: Optional custom configuration to override defaults
                      Should include 'primary.key.fields' for upsert mode
        primary_key_fields: Deprecated - pass via custom_config['primary.key.fields'] instead

    Returns:
        Optional[Dict[str, Any]]: Sink connector configuration or None if unsupported
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka-1:29092,kafka-2:29092,kafka-3:29092'
        )

    if not db_config.is_target:
        logger.error(f"Database {db_config.connection_name} is not a target database")
        return None

    client = db_config.client
    db_type = db_config.db_type.lower()

    sink_generators = {
        'mysql': get_mysql_sink_connector_config,
        'postgresql': get_postgresql_sink_connector_config,
    }

    generator = sink_generators.get(db_type)

    if not generator:
        logger.error(f"Unsupported target database type for sink connector: {db_type}")
        return None

    logger.info(f"Generating {db_type} sink connector config (primary keys from custom_config)")

    return generator(
        client=client,
        db_config=db_config,
        topics=topics,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        delete_enabled=delete_enabled,
        custom_config=custom_config,
    )


def get_default_sink_config_options() -> Dict[str, Dict[str, Any]]:
    """
    Get default JDBC Sink Connector configuration options with descriptions

    These options can be displayed in the UI for users to customize sink connector behavior

    Returns:
        Dict[str, Dict[str, Any]]: Configuration options with metadata for UI rendering
    """
    return {
        "auto.create": {
            "default": "true",
            "type": "boolean",
            "description": "Automatically create tables if they don't exist",
            "options": ["true", "false"],
            "ui_label": "Auto-create Tables"
        },
        "auto.evolve": {
            "default": "true",
            "type": "boolean",
            "description": "Automatically evolve table schema when source schema changes",
            "options": ["true", "false"],
            "ui_label": "Auto-evolve Schema"
        },
        "insert.mode": {
            "default": "upsert",
            "type": "select",
            "description": "How to insert data into the target table",
            "options": ["insert", "upsert", "update"],
            "ui_label": "Insert Mode"
        },
        "pk.mode": {
            "default": "record_key",
            "type": "select",
            "description": "Primary key mode for the target table",
            "options": ["none", "kafka", "record_key", "record_value"],
            "ui_label": "Primary Key Mode"
        },
        "pk.fields": {
            "default": "",
            "type": "text",
            "description": "Comma-separated list of primary key field names (required for upsert/update modes)",
            "ui_label": "Primary Key Fields"
        },
        "batch.size": {
            "default": "3000",
            "type": "number",
            "description": "Maximum number of records to batch together for each write",
            "min": 1,
            "max": 10000,
            "ui_label": "Batch Size"
        },
        "max.retries": {
            "default": "10",
            "type": "number",
            "description": "Maximum number of retries on errors",
            "min": 0,
            "max": 100,
            "ui_label": "Max Retries"
        },
        "retry.backoff.ms": {
            "default": "3000",
            "type": "number",
            "description": "Time in milliseconds to wait between retries",
            "min": 0,
            "max": 60000,
            "ui_label": "Retry Backoff (ms)"
        },
        "errors.tolerance": {
            "default": "none",
            "type": "select",
            "description": "Error tolerance level",
            "options": ["none", "all"],
            "ui_label": "Error Tolerance"
        },
        "table.name.format": {
            "default": "${topic}",
            "type": "text",
            "description": "Format string for the target table name. Use ${topic} for topic name",
            "ui_label": "Table Name Format"
        },
    }


def validate_sink_connector_config(config: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate JDBC Sink Connector configuration

    Args:
        config: Sink connector configuration dictionary

    Returns:
        tuple[bool, List[str]]: (is_valid, list_of_errors)
    """
    errors = []

    # Required fields for JDBC Sink Connector
    required_fields = [
        'connector.class',
        'connection.url',
        'connection.user',
        'connection.password',
        'topics',
    ]

    for field in required_fields:
        if field not in config or not config[field]:
            errors.append(f"Missing required field: {field}")

    # Validate connector class
    if 'connector.class' in config:
        if config['connector.class'] != 'io.debezium.connector.jdbc.JdbcSinkConnector':
            errors.append(f"Invalid connector class: {config['connector.class']}")

    # Validate insert mode
    if 'insert.mode' in config:
        valid_modes = ['insert', 'upsert', 'update']
        if config['insert.mode'] not in valid_modes:
            errors.append(f"Invalid insert.mode: {config['insert.mode']}")

    # Validate pk.mode
    if 'pk.mode' in config:
        valid_pk_modes = ['none', 'kafka', 'record_key', 'record_value']
        if config['pk.mode'] not in valid_pk_modes:
            errors.append(f"Invalid pk.mode: {config['pk.mode']}")

    # Validate that upsert/update requires pk.mode and pk.fields
    if config.get('insert.mode') in ['upsert', 'update']:
        if config.get('pk.mode') == 'none':
            errors.append("insert.mode 'upsert' or 'update' requires pk.mode to be set")

    # Validate batch.size
    if 'batch.size' in config:
        try:
            batch_size = int(config['batch.size'])
            if batch_size < 1 or batch_size > 10000:
                errors.append(f"batch.size must be between 1 and 10000: {batch_size}")
        except ValueError:
            errors.append(f"batch.size must be a number: {config['batch.size']}")

    # Validate error tolerance
    if 'errors.tolerance' in config:
        valid_tolerance = ['none', 'all']
        if config['errors.tolerance'] not in valid_tolerance:
            errors.append(f"Invalid errors.tolerance: {config['errors.tolerance']}")

    is_valid = len(errors) == 0

    if is_valid:
        logger.info("Sink connector configuration is valid")
    else:
        logger.warning(f"Sink connector configuration has {len(errors)} errors")

    return is_valid, errors


