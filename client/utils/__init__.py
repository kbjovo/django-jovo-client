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
    get_database_size,
)

from .debezium_manager import (
    DebeziumConnectorManager,
    DebeziumException,
    ConnectorNotFoundException,
    ConnectorCreationException,
)

from .connector_templates import (
    generate_connector_name,
    get_mysql_connector_config,
    get_postgresql_connector_config,
    get_oracle_connector_config,
    get_connector_config_for_database,
    get_snapshot_modes,
    validate_connector_config,
)

from .notification_utils import (
    send_error_notification,
    send_replication_status_email,
    log_and_notify_error,
)

from .kafka_consumer import (
    DebeziumCDCConsumer,
    KafkaConsumerException,
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
    'get_database_size',
    
    # Debezium management
    'DebeziumConnectorManager',
    'DebeziumException',
    'ConnectorNotFoundException',
    'ConnectorCreationException',
    
    # Connector templates
    'generate_connector_name',
    'get_mysql_connector_config',
    'get_postgresql_connector_config',
    'get_oracle_connector_config',
    'get_connector_config_for_database',
    'get_snapshot_modes',
    'validate_connector_config',
    
    # Notifications
    'send_error_notification',
    'send_replication_status_email',
    'log_and_notify_error',
    
    # Kafka Consumer
    'DebeziumCDCConsumer',
    'KafkaConsumerException',
]
