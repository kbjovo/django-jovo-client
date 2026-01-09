#!/usr/bin/env python
"""
CDC Diagnostic Script
Checks all components to identify why CDC is not working.
"""

import os
import sys
import django

# Setup Django
sys.path.insert(0, '/root/projects/django/django-jovo-client')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'jovoclient.settings')
django.setup()

import requests
from client.models import ReplicationConfig
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

print("=" * 80)
print("CDC DIAGNOSTIC REPORT")
print("=" * 80)

# Get the replication config
config = ReplicationConfig.objects.filter(is_active=True).first()
if not config:
    print("ERROR: No active replication config found!")
    sys.exit(1)
print(f"\n1. REPLICATION CONFIG")
print(f"   Connector Name: {config.connector_name}")
print(f"   Status: {config.status}")
print(f"   Is Active: {config.is_active}")
print(f"   Consumer State: {config.consumer_state}")
print(f"   Topic Prefix: {config.kafka_topic_prefix}")

# Check Debezium connector status
print(f"\n2. DEBEZIUM CONNECTOR STATUS")
try:
    manager = DebeziumConnectorManager()
    exists, status = manager.get_connector_status(config.connector_name)

    if exists and status:
        connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
        print(f"   ✓ Connector exists: {config.connector_name}")
        print(f"   State: {connector_state}")

        # Check tasks
        tasks = status.get('tasks', [])
        for task in tasks:
            task_id = task.get('id')
            task_state = task.get('state')
            print(f"   Task {task_id}: {task_state}")

            if task_state == 'FAILED':
                trace = task.get('trace', 'No error trace')
                # Only show first 500 chars of trace
                print(f"   ERROR: {trace[:500]}")
    else:
        print(f"   ✗ Connector NOT FOUND: {config.connector_name}")

except Exception as e:
    print(f"   ✗ Error checking connector: {e}")

# Check Kafka topics
print(f"\n3. KAFKA TOPICS")
try:
    topic_manager = KafkaTopicManager()

    # Expected topic format: {topic_prefix}.{database}.{table}
    enabled_tables = config.table_mappings.filter(is_enabled=True)

    for table_mapping in enabled_tables:
        expected_topic = f"{config.kafka_topic_prefix}.{config.client_database.database_name}.{table_mapping.source_table}"

        exists = topic_manager.topic_exists(expected_topic)
        if exists:
            print(f"   ✓ Topic exists: {expected_topic}")
        else:
            print(f"   ✗ Topic NOT FOUND: {expected_topic}")

except Exception as e:
    print(f"   ✗ Error checking topics: {e}")

# Check source database binlog configuration (for MySQL)
print(f"\n4. SOURCE DATABASE BINLOG (MySQL)")
try:
    from django.db import connections
    from client.models import ClientDatabase

    source_db = config.client_database

    # Create a test connection
    import pymysql
    connection = pymysql.connect(
        host=source_db.host,
        port=source_db.port,
        user=source_db.username,
        password=source_db.password,
        database=source_db.database_name
    )

    with connection.cursor() as cursor:
        # Check binlog format
        cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        result = cursor.fetchone()
        if result:
            binlog_format = result[1]
            if binlog_format == 'ROW':
                print(f"   ✓ Binlog format: {binlog_format} (CORRECT)")
            else:
                print(f"   ✗ Binlog format: {binlog_format} (SHOULD BE 'ROW')")

        # Check if binlog is enabled
        cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
        result = cursor.fetchone()
        if result:
            log_bin = result[1]
            if log_bin == 'ON':
                print(f"   ✓ Binary logging: {log_bin}")
            else:
                print(f"   ✗ Binary logging: {log_bin} (SHOULD BE 'ON')")

        # Check binlog row image
        cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        result = cursor.fetchone()
        if result:
            binlog_row_image = result[1]
            if binlog_row_image == 'FULL':
                print(f"   ✓ Binlog row image: {binlog_row_image}")
            else:
                print(f"   ⚠ Binlog row image: {binlog_row_image} (RECOMMENDED: 'FULL')")

    connection.close()

except Exception as e:
    print(f"   ✗ Error checking binlog: {e}")

# Check connector configuration
print(f"\n5. DEBEZIUM CONNECTOR CONFIGURATION")
try:
    url = f"http://localhost:8083/connectors/{config.connector_name}/config"
    response = requests.get(url)

    if response.status_code == 200:
        connector_config = response.json()

        # Key settings to check
        important_settings = [
            'snapshot.mode',
            'database.hostname',
            'database.port',
            'database.user',
            'database.server.name',
            'table.include.list',
            'tasks.max',
        ]

        for setting in important_settings:
            value = connector_config.get(setting, 'NOT SET')
            print(f"   {setting}: {value}")
    else:
        print(f"   ✗ Could not get connector config: HTTP {response.status_code}")

except Exception as e:
    print(f"   ✗ Error getting connector config: {e}")

# Check recent consumer activity
print(f"\n6. CONSUMER ACTIVITY")
print(f"   Consumer State: {config.consumer_state}")
print(f"   Last Heartbeat: {config.consumer_last_heartbeat}")
print(f"   Consumer Task ID: {config.consumer_task_id}")

if config.consumer_last_heartbeat:
    from django.utils import timezone
    time_since_heartbeat = timezone.now() - config.consumer_last_heartbeat
    print(f"   Time since last heartbeat: {time_since_heartbeat.total_seconds():.1f} seconds")

    if time_since_heartbeat.total_seconds() > 60:
        print(f"   ⚠ Consumer may not be running (no heartbeat in {time_since_heartbeat.total_seconds():.1f}s)")
    else:
        print(f"   ✓ Consumer appears to be running")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
