"""
CDC (Change Data Capture) views module.

This module is organized into separate files for better maintainability:
- discovery.py: Table discovery workflow
- configuration.py: Configuration management and editing
- connector.py: Debezium connector creation and actions
- monitoring.py: Connector monitoring and status
- replication.py: Replication control (start, stop, restart)
- ajax.py: AJAX endpoints for async operations
"""

from .discovery import cdc_discover_tables
from .configuration import (
    cdc_configure_tables,
    cdc_edit_config,
    cdc_delete_config,
    cdc_config_details,
)
from .connector import (
    cdc_create_connector,
    cdc_connector_action,
)
from .monitoring import (
    cdc_monitor_connector,
    cdc_unified_status,
    ajax_get_table_schema,
    ajax_get_table_schemas_batch,
)
from .replication import (
    start_replication,
    stop_replication,
    restart_replication_view,
    replication_status,
)
from .ajax import (
    ajax_update_basic_config,
    ajax_update_table_mapping,
    ajax_add_tables,
    ajax_remove_tables,
)

__all__ = [
    # Discovery
    'cdc_discover_tables',

    # Configuration
    'cdc_configure_tables',
    'cdc_edit_config',
    'cdc_delete_config',
    'cdc_config_details',

    # Connector
    'cdc_create_connector',
    'cdc_connector_action',

    # Monitoring
    'cdc_monitor_connector',
    'cdc_unified_status',
    'ajax_get_table_schema',
    'ajax_get_table_schemas_batch',

    # Replication
    'start_replication',
    'stop_replication',
    'restart_replication_view',
    'replication_status',

    # AJAX
    'ajax_update_basic_config',
    'ajax_update_table_mapping',
    'ajax_add_tables',
    'ajax_remove_tables',
]
