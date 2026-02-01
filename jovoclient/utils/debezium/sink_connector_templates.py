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
    schema_registry_url: str = None,  # Added this parameter
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate MySQL JDBC Sink Connector configuration matching Source Avro config
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka-1:29092,kafka-2:29092,kafka-3:29092'
        )
    
    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )

    if db_config.db_type != 'mysql':
        raise ValueError(f"MySQL sink connector requires MySQL database, got: {db_config.db_type}")

    connector_name = f"{client.name.lower().replace(' ', '_')}_{db_config.connection_name.lower().replace(' ', '_')}_sink"
    connector_name = ''.join(c for c in connector_name if c.isalnum() or c == '_')

    primary_key_fields = custom_config.get('primary.key.fields', '') if custom_config else ''
    jdbc_url = f"jdbc:mysql://{db_config.host}:{db_config.port}/{db_config.database_name}"

    # Default configuration
    config = {
        "name": connector_name,
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "connection.url": jdbc_url,
        "connection.username": db_config.username,
        "connection.password": db_config.get_decrypted_password(),

        # Transforms
        "transforms": "extractTableName",
        "transforms.extractTableName.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.extractTableName.regex": ".*\\.([^.]+)\\.([^.]+)",
        "transforms.extractTableName.replacement": "$1_$2",

        "schema.evolution": "basic",
        "collection.name.format": "${topic}",

        # --- UPDATED TO MATCH SOURCE (AVRO) ---
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": schema_registry_url,
        "key.converter.schemas.enable": "true",
        
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": schema_registry_url,
        "value.converter.schemas.enable": "true",
        # --------------------------------------

        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "delete.enabled": str(delete_enabled).lower(),
        "batch.size": "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",
        "connection.attempts": "3",
        "connection.backoff.ms": "10000",
        "topic.tracking.refresh.interval.ms": "5000",
        "consumer.override.metadata.max.age.ms": "10000",

        # Error handling (configurable via settings)
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }

    # Apply DLQ settings from Django settings
    from django.conf import settings
    sink_config = getattr(settings, 'SINK_CONNECTOR_CONFIG', {})

    errors_tolerance = sink_config.get('ERRORS_TOLERANCE', 'all')
    dlq_enabled = sink_config.get('DLQ_ENABLED', True)
    dlq_replication_factor = sink_config.get('DLQ_REPLICATION_FACTOR', 3)
    dlq_context_headers = sink_config.get('DLQ_CONTEXT_HEADERS', True)

    config["errors.tolerance"] = errors_tolerance

    # Add Dead Letter Queue configuration if enabled
    if dlq_enabled and errors_tolerance == 'all':
        dlq_topic = f"client_{client.id}.dlq"
        config["errors.deadletterqueue.topic.name"] = dlq_topic
        config["errors.deadletterqueue.topic.replication.factor"] = str(dlq_replication_factor)
        config["errors.deadletterqueue.context.headers.enable"] = str(dlq_context_headers).lower()
        logger.info(f"DLQ enabled for sink connector: {dlq_topic}")

    if primary_key_fields:
        config["primary.key.fields"] = primary_key_fields

    # Use topics.regex for auto-subscription to all source connector topics
    # This allows sink to automatically subscribe when new source connectors are added
    # Pattern matches: client_{id}_db_{id}_v_{version}.{database}.{table}
    topic_regex = f"client_{client.id}_db_\\d+_v_\\d+\\.(?!signals$).*"
    config["topics.regex"] = topic_regex
    logger.info(f"Using topics.regex for auto-subscription: {topic_regex}")

    if custom_config:
        config.update(custom_config)

    logger.debug(f"Generated MySQL JDBC Sink connector config: {connector_name}")
    return config


def get_postgresql_sink_connector_config(
    client: Client,
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,  # Added this parameter
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate PostgreSQL JDBC Sink Connector configuration matching Source Avro config
    """
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka-1:29092,kafka-2:29092,kafka-3:29092'
        )

    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
        )

    if db_config.db_type != 'postgresql':
        raise ValueError(f"PostgreSQL sink connector requires PostgreSQL database, got: {db_config.db_type}")

    connector_name = f"{client.name.lower().replace(' ', '_')}_{db_config.connection_name.lower().replace(' ', '_')}_sink"
    connector_name = ''.join(c for c in connector_name if c.isalnum() or c == '_')

    primary_key_fields = custom_config.get('primary.key.fields', '') if custom_config else ''
    jdbc_url = f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database_name}"

    # Default configuration
    config = {
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "connection.url": jdbc_url,
        "connection.username": db_config.username,
        "connection.password": db_config.get_decrypted_password(),

        # Transforms
        "transforms": "extractTableName",
        "transforms.extractTableName.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.extractTableName.regex": ".*\\.([^.]+)\\.([^.]+)",
        "transforms.extractTableName.replacement": "$1_$2",

        # Let sink connector auto-create tables and add columns
        # 'basic' mode: creates tables if missing, adds new columns as NULLABLE
        "schema.evolution": "basic",
        "collection.name.format": "${topic}",

        # --- UPDATED TO MATCH SOURCE (AVRO) ---
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": schema_registry_url,
        "key.converter.schemas.enable": "true",
        
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": schema_registry_url,
        "value.converter.schemas.enable": "true",
        # --------------------------------------

        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "delete.enabled": str(delete_enabled).lower(),
        "batch.size": "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",
        "connection.attempts": "3",
        "connection.backoff.ms": "10000",
        "topic.tracking.refresh.interval.ms": "5000",
        "consumer.override.metadata.max.age.ms": "10000",

        # Error handling (configurable via settings)
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }

    # Apply DLQ settings from Django settings
    from django.conf import settings
    sink_config = getattr(settings, 'SINK_CONNECTOR_CONFIG', {})

    errors_tolerance = sink_config.get('ERRORS_TOLERANCE', 'all')
    dlq_enabled = sink_config.get('DLQ_ENABLED', True)
    dlq_replication_factor = sink_config.get('DLQ_REPLICATION_FACTOR', 3)
    dlq_context_headers = sink_config.get('DLQ_CONTEXT_HEADERS', True)

    config["errors.tolerance"] = errors_tolerance

    # Add Dead Letter Queue configuration if enabled
    if dlq_enabled and errors_tolerance == 'all':
        dlq_topic = f"client_{client.id}.dlq"
        config["errors.deadletterqueue.topic.name"] = dlq_topic
        config["errors.deadletterqueue.topic.replication.factor"] = str(dlq_replication_factor)
        config["errors.deadletterqueue.context.headers.enable"] = str(dlq_context_headers).lower()
        logger.info(f"DLQ enabled for sink connector: {dlq_topic}")

    if primary_key_fields:
        config["primary.key.fields"] = primary_key_fields

    # Use topics.regex for auto-subscription to all source connector topics
    # This allows sink to automatically subscribe when new source connectors are added
    # Pattern matches: client_{id}_db_{id}_v_{version}.{database}.{table}
    topic_regex = f"client_{client.id}_db_\\d+_v_\\d+\\.(?!signals$).*"
    config["topics.regex"] = topic_regex
    logger.info(f"Using topics.regex for auto-subscription: {topic_regex}")

    if custom_config:
        config.update(custom_config)

    logger.info(f"Generated PostgreSQL JDBC Sink connector config: {connector_name}")
    return config


def get_sink_connector_config_for_database(
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,  # Added argument
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
    primary_key_fields: Optional[str] = None,  # Deprecated - pass via custom_config instead
) -> Optional[Dict[str, Any]]:
    """
    Get JDBC Sink connector configuration based on target database type
    """
    # Get from settings if not provided
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_INTERNAL_SERVERS',
            'kafka-1:29092,kafka-2:29092,kafka-3:29092'
        )
    
    if schema_registry_url is None:
        from django.conf import settings
        schema_registry_url = settings.DEBEZIUM_CONFIG.get(
            'SCHEMA_REGISTRY_URL',
            'http://schema-registry:8081'
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

    logger.debug(f"Generating {db_type} sink connector config")

    return generator(
        client=client,
        db_config=db_config,
        topics=topics,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        schema_registry_url=schema_registry_url,  # Pass it down
        delete_enabled=delete_enabled,
        custom_config=custom_config,
    )


def get_default_sink_config_options() -> Dict[str, Dict[str, Any]]:
    """
    Get default JDBC Sink Connector configuration options with descriptions.
    Updated to match Debezium JDBC Sink properties.
    """
    return {
        "schema.evolution": {
            "default": "none",
            "type": "select",
            "description": "How the connector evolves the destination table schema. Use 'none' for manual table creation via manual_create_target_tables()",
            "options": ["none", "basic"],
            "ui_label": "Schema Evolution"
        },
        "insert.mode": {
            "default": "upsert",
            "type": "select",
            "description": "How to insert data into the target table",
            "options": ["insert", "upsert", "update"],
            "ui_label": "Insert Mode"
        },
        "primary.key.mode": {
            "default": "record_key",
            "type": "select",
            "description": "How to determine the Primary Key",
            "options": ["none", "kafka", "record_key", "record_value"],
            "ui_label": "Primary Key Mode"
        },
        "primary.key.fields": {
            "default": "",
            "type": "text",
            "description": "Comma-separated list of primary key field names (required if mode is record_value)",
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
        "collection.name.format": {
            "default": "${topic}",
            "type": "text",
            "description": "Format string for the target table name. Use ${topic} for topic name",
            "ui_label": "Table Name Format"
        },
    }


def validate_sink_connector_config(config: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate JDBC Sink Connector configuration
    """
    errors = []

    # Required fields for JDBC Sink Connector
    required_fields = [
        'connector.class',
        'connection.url',
        'connection.username',  # Corrected from 'connection.user'
        'connection.password',
    ]

    for field in required_fields:
        if field not in config or not config[field]:
            errors.append(f"Missing required field: {field}")

    # Must have either 'topics' or 'topics.regex'
    if not config.get('topics') and not config.get('topics.regex'):
        errors.append("Must provide either 'topics' or 'topics.regex'")

    # Validate connector class
    if 'connector.class' in config:
        if config['connector.class'] != 'io.debezium.connector.jdbc.JdbcSinkConnector':
            errors.append(f"Invalid connector class: {config['connector.class']}")

    # Validate insert mode
    if 'insert.mode' in config:
        valid_modes = ['insert', 'upsert', 'update']
        if config['insert.mode'] not in valid_modes:
            errors.append(f"Invalid insert.mode: {config['insert.mode']}")

    # Validate primary.key.mode
    if 'primary.key.mode' in config:
        valid_pk_modes = ['none', 'kafka', 'record_key', 'record_value']
        if config['primary.key.mode'] not in valid_pk_modes:
            errors.append(f"Invalid primary.key.mode: {config['primary.key.mode']}")

    # Validate that upsert/update requires pk mode
    if config.get('insert.mode') in ['upsert', 'update']:
        if config.get('primary.key.mode') == 'none':
            errors.append("insert.mode 'upsert' or 'update' requires primary.key.mode to be set")

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


