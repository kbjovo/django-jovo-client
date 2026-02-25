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
import re as _re
from typing import Dict, List, Optional, Any
from client.models.client import Client
from client.models.database import ClientDatabase

logger = logging.getLogger(__name__)


def _get_custom_table_transforms(client: Client, config: Dict) -> List[str]:
    """
    Build per-table RegexRouter transforms for tables whose target_table name
    differs from the default sink connector naming ({schema}_{table}).

    These transforms are injected BEFORE the generic extractTableName transform
    so that topics for custom-named tables are routed to the correct table, while
    remaining topics fall through to the generic $1_$2 replacement.

    The generated transforms are added directly into the ``config`` dict as a
    side-effect; the returned list contains only the transform names.

    Args:
        client: Client instance (used to query all table mappings)
        config: Sink connector config dict to update in-place

    Returns:
        List of transform names (in insertion order, duplicates removed)
    """
    try:
        from client.models.replication import TableMapping

        all_mappings = TableMapping.objects.filter(
            replication_config__client_database__client=client,
            is_enabled=True,
        ).select_related('replication_config__client_database')

        custom_transform_names: List[str] = []
        seen_names: set = set()

        for mapping in all_mappings:
            source_db = mapping.replication_config.client_database
            source_table = mapping.source_table
            source_schema = mapping.source_schema or ''
            target_table = mapping.target_table

            db_type = source_db.db_type.lower()
            if db_type == 'mysql':
                schema_in_topic = source_db.database_name
            elif db_type == 'postgresql':
                schema_in_topic = source_schema or 'public'
            elif db_type in ('mssql', 'sqlserver'):
                schema_in_topic = source_schema or 'dbo'
            elif db_type == 'oracle':
                schema_in_topic = source_schema or getattr(source_db, 'username', 'public').upper()
            else:
                schema_in_topic = source_schema or getattr(source_db, 'database_name', '')

            default_target = f"{schema_in_topic}_{source_table}"

            if target_table == default_target:
                continue  # Default name — no custom transform needed

            db_id = source_db.id
            safe_schema = _re.sub(r'[^a-zA-Z0-9]', '_', schema_in_topic)
            safe_table = _re.sub(r'[^a-zA-Z0-9]', '_', source_table)
            transform_name = f"rename_db{db_id}_{safe_schema}_{safe_table}"

            if transform_name in seen_names:
                continue  # Already added (same source db/schema/table)
            seen_names.add(transform_name)

            # Match the specific topic for this source db / schema / table across
            # any connector version: client_{cid}_db_{db_id}_v_\d+.{schema}.{table}
            # Use single backslash escaping: Python \\d+ -> string \d+ -> Java regex digit+
            topic_regex = (
                f"client_{client.id}_db_{db_id}_v_\\d+"
                f"\\.{_re.escape(schema_in_topic)}"
                f"\\.{_re.escape(source_table)}"
            )

            config[f"transforms.{transform_name}.type"] = (
                "org.apache.kafka.connect.transforms.RegexRouter"
            )
            config[f"transforms.{transform_name}.regex"] = topic_regex
            config[f"transforms.{transform_name}.replacement"] = target_table

            custom_transform_names.append(transform_name)
            logger.info(
                f"Custom table rename transform: "
                f"{schema_in_topic}.{source_table} -> {target_table}"
            )

        return custom_transform_names

    except Exception as e:
        logger.warning(f"Could not build custom table transforms: {e}")
        return []


def get_mysql_sink_connector_config(
    client: Client,
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
    replication_config=None,
) -> Dict[str, Any]:
    """
    Generate MySQL JDBC Sink Connector configuration matching Source Avro config
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

    if db_config.db_type != 'mysql':
        raise ValueError(f"MySQL sink connector requires MySQL database, got: {db_config.db_type}")

    connector_name = f"{client.name.lower().replace(' ', '_')}_{db_config.connection_name.lower().replace(' ', '_')}_sink"
    connector_name = ''.join(c for c in connector_name if c.isalnum() or c == '_')

    primary_key_fields = custom_config.get('primary.key.fields', '') if custom_config else ''
    jdbc_url = f"jdbc:mysql://{db_config.host}:{db_config.port}/{db_config.database_name}?autoReconnect=true"

    # Default configuration
    config = {
        "name": connector_name,
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "connection.url": jdbc_url,
        "connection.username": db_config.username,
        "connection.password": db_config.get_decrypted_password(),

        # c3p0 connection pool validation - test connections before use
        "hibernate.c3p0.testConnectionOnCheckout": "true",
        "hibernate.c3p0.preferredTestQuery": "SELECT 1",
        "hibernate.c3p0.idle_test_period": "300",

        # --- TRANSFORMS ---
        # 1. unwrap: flattens the Debezium envelope so the sink sees the actual data fields.
        # 2. (optional) custom per-table renames: route specific topics to custom table names.
        # 3. extractTableName: routes remaining topics using the $1_$2 pattern.
        # Custom rename transforms are injected below after querying table mappings.
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "true",
        "transforms.unwrap.delete.handling.mode": "rewrite",

        "transforms.extractTableName.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.extractTableName.regex": ".*\\.([^.]+)\\.([^.]+)",
        "transforms.extractTableName.replacement": "$1_$2",

        "schema.evolution": "basic",
        "collection.name.format": "${topic}",

        # --- CONVERTERS (AVRO) ---
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": schema_registry_url,
        "key.converter.schemas.enable": "true",

        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": schema_registry_url,
        "value.converter.schemas.enable": "true",

        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "delete.enabled": str(delete_enabled).lower(),
        "batch.size": str(replication_config.sink_batch_size) if replication_config and hasattr(replication_config, 'sink_batch_size') else "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",
        "connection.attempts": "3",
        "connection.backoff.ms": "10000",
        "topic.tracking.refresh.interval.ms": "5000",
        "consumer.override.metadata.max.age.ms": "10000",
        # Permanent: controls how many records sink pulls per poll (should be >= batch.size)
        "consumer.override.max.poll.records": str(replication_config.sink_max_poll_records) if replication_config and hasattr(replication_config, 'sink_max_poll_records') else "5000",
        # Permanent: generous offset flush timeout to prevent "Commit of offsets timed out" during snapshot
        "offset.flush.timeout.ms": "60000",
        # Prevent "Timeout expired before position could be determined" when the broker is under
        # heavy write load during the initial snapshot. The default is 60 s; 120 s gives the broker
        # more headroom to respond to ListOffsets / position() requests for new partitions.
        "consumer.override.default.api.timeout.ms": "120000",
        # Permanent: 50MB fetch buffer per partition
        "consumer.override.fetch.max.bytes": "52428800",

        # Error handling (configurable via settings)
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }

    # Apply DLQ settings from Django settings
    from django.conf import settings
    sink_config = getattr(settings, 'SINK_CONNECTOR_CONFIG', {})

    errors_tolerance = sink_config.get('ERRORS_TOLERANCE', 'none')
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
    # Excludes: ddl_events and debezium_signal tables (DDL events are processed separately)
    # Note: signals topic (client_X_db_Y_v_Z.signals) is naturally excluded as it has only 2 parts
    topic_regex = f"client_{client.id}_db_\\d+_v_\\d+\\.[^.]+\\.(?!ddl_events$|debezium_signal$)[^.]+"
    config["topics.regex"] = topic_regex
    logger.info(f"Using topics.regex for auto-subscription: {topic_regex}")

    # Build custom per-table rename transforms and assemble the full transforms chain.
    # Custom transforms are prepended so they run before the generic extractTableName.
    # Topics that match a custom rename get routed to the custom table name;
    # the generic $1_$2 extractTableName handles all remaining topics.
    custom_transforms = _get_custom_table_transforms(client, config)
    if custom_transforms:
        config["transforms"] = "unwrap," + ",".join(custom_transforms) + ",extractTableName"
    else:
        config["transforms"] = "unwrap,extractTableName"

    if custom_config:
        config.update(custom_config)

    logger.debug(f"Generated MySQL JDBC Sink connector config: {connector_name}")
    return config


def get_postgresql_sink_connector_config(
    client: Client,
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
    replication_config=None,
) -> Dict[str, Any]:
    """
    Generate PostgreSQL JDBC Sink Connector configuration matching Source Avro config
    """
    if kafka_bootstrap_servers is None:
        from django.conf import settings
        kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']

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

        # c3p0 connection pool validation - test connections before use
        "hibernate.c3p0.testConnectionOnCheckout": "true",
        "hibernate.c3p0.preferredTestQuery": "SELECT 1",
        "hibernate.c3p0.idle_test_period": "300",

        # Transforms
        # castMicroTime: MySQL TIME columns > 24h cause a DateTimeException in the JDBC sink.
        # Casting to string stores the raw microsecond value and avoids the crash.
        # Custom per-table rename transforms are injected below after querying table mappings.
        "transforms.extractTableName.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.extractTableName.regex": ".*\\.([^.]+)\\.([^.]+)",
        "transforms.extractTableName.replacement": "$1_$2",
        "transforms.castMicroTime.type": "org.apache.kafka.connect.transforms.Cast$Value",
        "transforms.castMicroTime.spec": "time_taken:string",

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
        "batch.size": str(replication_config.sink_batch_size) if replication_config and hasattr(replication_config, 'sink_batch_size') else "3000",
        "max.retries": "10",
        "retry.backoff.ms": "3000",
        "connection.attempts": "3",
        "connection.backoff.ms": "10000",
        "topic.tracking.refresh.interval.ms": "5000",
        "consumer.override.metadata.max.age.ms": "10000",
        # Permanent: controls how many records sink pulls per poll (should be >= batch.size)
        "consumer.override.max.poll.records": str(replication_config.sink_max_poll_records) if replication_config and hasattr(replication_config, 'sink_max_poll_records') else "5000",
        # Permanent: generous offset flush timeout to prevent "Commit of offsets timed out" during snapshot
        "offset.flush.timeout.ms": "60000",
        # Prevent "Timeout expired before position could be determined" when the broker is under
        # heavy write load during the initial snapshot. The default is 60 s; 120 s gives the broker
        # more headroom to respond to ListOffsets / position() requests for new partitions.
        "consumer.override.default.api.timeout.ms": "120000",
        # Permanent: 50MB fetch buffer per partition
        "consumer.override.fetch.max.bytes": "52428800",

        # Error handling (configurable via settings)
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
    }

    # Apply DLQ settings from Django settings
    from django.conf import settings
    sink_config = getattr(settings, 'SINK_CONNECTOR_CONFIG', {})

    errors_tolerance = sink_config.get('ERRORS_TOLERANCE', 'none')
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
    # Excludes: ddl_events and debezium_signal tables (DDL events are processed separately)
    # Note: signals topic (client_X_db_Y_v_Z.signals) is naturally excluded as it has only 2 parts
    topic_regex = f"client_{client.id}_db_\\d+_v_\\d+\\.[^.]+\\.(?!ddl_events$|debezium_signal$)[^.]+"
    config["topics.regex"] = topic_regex
    logger.info(f"Using topics.regex for auto-subscription: {topic_regex}")

    # Build custom per-table rename transforms and assemble the full transforms chain.
    # Custom transforms are prepended so they run before the generic extractTableName.
    custom_transforms = _get_custom_table_transforms(client, config)
    if custom_transforms:
        config["transforms"] = ",".join(custom_transforms) + ",extractTableName,castMicroTime"
    else:
        config["transforms"] = "extractTableName,castMicroTime"

    if custom_config:
        config.update(custom_config)

    logger.info(f"Generated PostgreSQL JDBC Sink connector config: {connector_name}")
    return config


def get_sink_connector_config_for_database(
    db_config: ClientDatabase,
    topics: Optional[List[str]] = None,
    kafka_bootstrap_servers: str = None,
    schema_registry_url: str = None,
    delete_enabled: bool = False,
    custom_config: Optional[Dict[str, Any]] = None,
    primary_key_fields: Optional[str] = None,  # Deprecated - pass via custom_config instead
    replication_config=None,
) -> Optional[Dict[str, Any]]:
    """
    Get JDBC Sink connector configuration based on target database type
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
        schema_registry_url=schema_registry_url,
        delete_enabled=delete_enabled,
        custom_config=custom_config,
        replication_config=replication_config,
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


