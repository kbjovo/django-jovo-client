"""
Schema Registry Utilities for Debezium Connector Management

This module provides utilities to interact with Kafka Schema Registry
to extract metadata like primary keys from registered schemas.
"""

import logging
import requests
from typing import List, Optional, Dict, Any
from django.conf import settings

logger = logging.getLogger(__name__)


def get_schema_registry_url() -> str:
    """
    Get Schema Registry URL from Django settings

    Returns:
        Schema Registry URL
    """
    # Try DEBEZIUM_CONFIG first, then fall back to direct attribute
    if hasattr(settings, 'DEBEZIUM_CONFIG'):
        return settings.DEBEZIUM_CONFIG.get('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
    return getattr(settings, 'SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')


def get_registered_subjects() -> List[str]:
    """
    Get all registered schema subjects from Schema Registry

    Returns:
        List of subject names
    """
    try:
        schema_registry_url = get_schema_registry_url()
        response = requests.get(f"{schema_registry_url}/subjects", timeout=10)
        response.raise_for_status()

        subjects = response.json()
        logger.info(f"Found {len(subjects)} registered schemas in Schema Registry")
        return subjects

    except Exception as e:
        logger.error(f"Failed to get registered subjects: {e}")
        return []


def get_schema_by_subject(subject_name: str, version: str = "latest") -> Optional[Dict[str, Any]]:
    """
    Get schema details for a specific subject

    Args:
        subject_name: Schema subject name (e.g., 'client_1_db_2.kbe.busyuk_items-key')
        version: Schema version (default: 'latest')

    Returns:
        Schema dictionary or None if not found
    """
    try:
        schema_registry_url = get_schema_registry_url()
        url = f"{schema_registry_url}/subjects/{subject_name}/versions/{version}"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        logger.info(f"Retrieved schema for subject: {subject_name} (version: {data.get('version')})")
        return data

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Schema subject not found: {subject_name}")
        else:
            logger.error(f"HTTP error retrieving schema for {subject_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to get schema for {subject_name}: {e}")
        return None


def extract_primary_key_fields_from_schema(topic_prefix: str, table_name: str) -> List[str]:
    """
    Extract primary key field names from Schema Registry key schema

    Debezium registers schemas with the pattern: {topic_prefix}.{database}.{table}-key
    The key schema contains the primary key fields.

    Args:
        topic_prefix: Debezium topic prefix (e.g., 'client_1_db_2')
        table_name: Full table name including database (e.g., 'kbe.busyuk_items')

    Returns:
        List of primary key field names (e.g., ['id'] or ['user_id', 'tenant_id'])
    """
    try:
        # Build subject name for the key schema
        # Pattern: {topic_prefix}.{database}.{table}-key
        subject_name = f"{topic_prefix}.{table_name}-key"

        logger.info(f"Querying Schema Registry for subject: {subject_name}")

        # Get the schema from Schema Registry
        schema_data = get_schema_by_subject(subject_name)

        if not schema_data:
            logger.warning(f"No schema found for {subject_name}")
            return []

        # Parse the schema JSON string
        import json
        schema = json.loads(schema_data['schema'])

        # Extract field names from the key schema
        if 'fields' in schema:
            pk_fields = [field['name'] for field in schema['fields']]
            logger.info(f"Extracted primary key fields from Schema Registry: {pk_fields}")
            return pk_fields
        else:
            logger.warning(f"No fields found in key schema for {subject_name}")
            return []

    except Exception as e:
        logger.error(f"Failed to extract primary key fields from schema: {e}")
        return []


def get_primary_key_fields_for_sink(topic_prefix: str, table_name: str) -> str:
    """
    Get comma-separated primary key fields for JDBC sink connector

    This is the main function to use when creating sink connectors.

    Args:
        topic_prefix: Debezium topic prefix (e.g., 'client_1_db_2')
        table_name: Full table name including database (e.g., 'kbe.busyuk_items')

    Returns:
        Comma-separated primary key field names (e.g., 'id' or 'user_id,tenant_id')
        Returns empty string if no primary key found

    Example:
        >>> get_primary_key_fields_for_sink('client_1_db_2', 'kbe.busyuk_items')
        'id'

        >>> get_primary_key_fields_for_sink('client_1_db_2', 'mydb.composite_table')
        'user_id,tenant_id'
    """
    pk_fields = extract_primary_key_fields_from_schema(topic_prefix, table_name)

    if pk_fields:
        result = ','.join(pk_fields)
        logger.info(f"Primary key fields for sink connector ({table_name}): {result}")
        return result
    else:
        logger.warning(f"No primary key found for {table_name}. Sink connector may fail with upsert mode.")
        return ""


def get_all_table_schemas(topic_prefix: str) -> Dict[str, Dict[str, Any]]:
    """
    Get all table schemas for a given topic prefix

    Useful for bulk connector setup or validation

    Args:
        topic_prefix: Debezium topic prefix (e.g., 'client_1_db_2')

    Returns:
        Dict mapping table names to their schema info
        Example: {
            'kbe.busyuk_items': {
                'primary_keys': ['id'],
                'key_schema_version': 1,
                'value_schema_version': 1
            }
        }
    """
    try:
        subjects = get_registered_subjects()

        # Filter subjects for this topic prefix
        prefix_pattern = f"{topic_prefix}."
        table_schemas = {}

        for subject in subjects:
            if subject.startswith(prefix_pattern) and subject.endswith('-key'):
                # Extract table name
                # Format: {topic_prefix}.{database}.{table}-key
                # Example: client_1_db_2.kbe.busyuk_items-key -> kbe.busyuk_items
                table_name = subject[len(topic_prefix)+1:-4]  # Remove prefix and '-key'

                # Get key schema
                key_schema_data = get_schema_by_subject(subject)

                # Get value schema
                value_subject = subject.replace('-key', '-value')
                value_schema_data = get_schema_by_subject(value_subject)

                if key_schema_data:
                    import json
                    key_schema = json.loads(key_schema_data['schema'])
                    pk_fields = [field['name'] for field in key_schema.get('fields', [])]

                    table_schemas[table_name] = {
                        'primary_keys': pk_fields,
                        'key_schema_version': key_schema_data.get('version'),
                        'value_schema_version': value_schema_data.get('version') if value_schema_data else None,
                        'key_subject': subject,
                        'value_subject': value_subject
                    }

        logger.info(f"Found {len(table_schemas)} table schemas for topic prefix: {topic_prefix}")
        return table_schemas

    except Exception as e:
        logger.error(f"Failed to get table schemas: {e}")
        return {}


def validate_schema_exists(topic_prefix: str, table_name: str) -> bool:
    """
    Check if schema exists in Schema Registry for a given table

    Args:
        topic_prefix: Debezium topic prefix
        table_name: Full table name including database

    Returns:
        True if schema exists, False otherwise
    """
    subject_name = f"{topic_prefix}.{table_name}-key"
    schema_data = get_schema_by_subject(subject_name)
    return schema_data is not None


def get_schema_field_info(topic_prefix: str, table_name: str) -> Dict[str, Any]:
    """
    Get detailed field information from value schema

    Args:
        topic_prefix: Debezium topic prefix
        table_name: Full table name including database

    Returns:
        Dict with field information including names, types, and primary keys
    """
    try:
        import json

        # Get key schema for primary keys
        key_subject = f"{topic_prefix}.{table_name}-key"
        key_schema_data = get_schema_by_subject(key_subject)

        # Get value schema for all fields
        value_subject = f"{topic_prefix}.{table_name}-value"
        value_schema_data = get_schema_by_subject(value_subject)

        result = {
            'table_name': table_name,
            'primary_keys': [],
            'all_fields': [],
            'schema_exists': False
        }

        if key_schema_data:
            key_schema = json.loads(key_schema_data['schema'])
            result['primary_keys'] = [field['name'] for field in key_schema.get('fields', [])]
            result['schema_exists'] = True

        if value_schema_data:
            value_schema = json.loads(value_schema_data['schema'])
            # Navigate to the 'after' field which contains the actual table schema
            if 'fields' in value_schema:
                for field in value_schema['fields']:
                    if field['name'] == 'after' and isinstance(field['type'], list):
                        # Get the struct type (not null)
                        for field_type in field['type']:
                            if isinstance(field_type, dict) and field_type.get('type') == 'record':
                                result['all_fields'] = [
                                    {
                                        'name': f['name'],
                                        'type': f['type']
                                    }
                                    for f in field_type.get('fields', [])
                                ]
                                break

        return result

    except Exception as e:
        logger.error(f"Failed to get schema field info: {e}")
        return {
            'table_name': table_name,
            'primary_keys': [],
            'all_fields': [],
            'schema_exists': False,
            'error': str(e)
        }