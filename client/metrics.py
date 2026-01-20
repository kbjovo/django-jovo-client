"""
Custom Prometheus metrics for CDC replication monitoring
Create this file: client/metrics.py
"""
from prometheus_client import Counter, Histogram, Gauge, Info

# ====================================
# DATABASE CONNECTION METRICS
# ====================================
database_connections_total = Counter(
    'cdc_database_connections_total',
    'Total number of database connection attempts',
    ['status', 'database_type']  # status: success/failed, database_type: mysql/postgres/etc
)

database_connection_test_duration = Histogram(
    'cdc_database_connection_test_duration_seconds',
    'Time taken to test database connections',
    ['database_type'],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, float("inf"))
)

# ====================================
# TABLE DISCOVERY METRICS
# ====================================
tables_discovered_total = Counter(
    'cdc_tables_discovered_total',
    'Total number of tables discovered',
    ['client_id', 'database_name']
)

table_discovery_duration = Histogram(
    'cdc_table_discovery_duration_seconds',
    'Time taken to discover tables',
    ['database_name'],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, float("inf"))
)

# ====================================
# DEBEZIUM CONNECTOR METRICS
# ====================================
debezium_connectors_total = Counter(
    'cdc_debezium_connectors_total',
    'Total number of Debezium connector operations',
    ['operation', 'status']  # operation: create/delete/update, status: success/failed
)

debezium_connector_creation_duration = Histogram(
    'cdc_debezium_connector_creation_duration_seconds',
    'Time taken to create Debezium connectors',
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, float("inf"))
)

active_debezium_connectors = Gauge(
    'cdc_active_debezium_connectors',
    'Number of currently active Debezium connectors',
    ['client_id']
)

# ====================================
# REPLICATION METRICS
# ====================================
replication_events_processed = Counter(
    'cdc_replication_events_processed_total',
    'Total number of CDC events processed',
    ['client_id', 'table_name', 'operation']  # operation: insert/update/delete
)

replication_lag_seconds = Gauge(
    'cdc_replication_lag_seconds',
    'Replication lag in seconds',
    ['client_id', 'connector_name']
)

replication_errors_total = Counter(
    'cdc_replication_errors_total',
    'Total number of replication errors',
    ['client_id', 'error_type']
)

# ====================================
# KAFKA CONSUMER METRICS
# ====================================
kafka_messages_consumed = Counter(
    'cdc_kafka_messages_consumed_total',
    'Total number of Kafka messages consumed',
    ['topic', 'consumer_group']
)

kafka_message_processing_duration = Histogram(
    'cdc_kafka_message_processing_duration_seconds',
    'Time taken to process Kafka messages',
    ['topic'],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, float("inf"))
)

kafka_consumer_lag = Gauge(
    'cdc_kafka_consumer_lag',
    'Kafka consumer lag (messages behind)',
    ['topic', 'partition', 'consumer_group']
)

# ====================================
# CLIENT METRICS
# ====================================
registered_clients = Gauge(
    'cdc_registered_clients_total',
    'Total number of registered clients'
)

client_databases = Gauge(
    'cdc_client_databases_total',
    'Total number of databases per client',
    ['client_id']
)

# ====================================
# CELERY TASK METRICS
# ====================================
celery_task_duration = Histogram(
    'cdc_celery_task_duration_seconds',
    'Duration of Celery tasks',
    ['task_name', 'queue'],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0, float("inf"))
)

celery_task_status = Counter(
    'cdc_celery_task_status_total',
    'Celery task execution status',
    ['task_name', 'status', 'queue']  # status: success/failed/retry
)

# ====================================
# SYSTEM INFO METRICS (for debugging)
# ====================================
cdc_system_info = Info(
    'cdc_system',
    'CDC system information'
)

# Set initial system info
cdc_system_info.info({
    'version': '1.0.0',
    'environment': 'development',
    'kafka_brokers': '3',
    'celery_queues': '2'
})