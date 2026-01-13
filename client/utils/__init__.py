"""
Utility modules for database operations and Debezium management
"""

from .database_utils import (
    test_database_connection,
    get_database_engine,
    execute_query,
    get_table_list,
    get_table_schema,
    create_client_database,
    check_binary_logging,
    check_sql_server_cdc,
    get_table_cdc_status,
    get_database_size,
)

from .notification_utils import (
    send_replication_status_email,
    log_and_notify_error,
)

from .kafka_signal_sender import (
    KafkaSignalSender,
)

__all__ = [
    # Database utilities
    'test_database_connection',
    'get_database_engine',
    'execute_query',
    'get_table_list',
    'get_table_schema',
    'create_client_database',
    'check_binary_logging',
    'check_sql_server_cdc',
    'get_table_cdc_status',
    'get_database_size',

    # Notifications
    'send_replication_status_email',
    'log_and_notify_error',

    # Kafka Signal Sender
    'KafkaSignalSender',
]
