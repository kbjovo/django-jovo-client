"""
Client app views module.

This module is organized into separate files for better maintainability:
- client_views.py: Client CRUD operations
- database_views.py: ClientDatabase CRUD and connection testing
- cdc_views.py: CDC workflow and replication management
- dashboard_views.py: Dashboard, replications list, and monitoring views
"""

from .client_views import (
    ClientCreateView,
    ClientUpdateView,
    ClientDetailView,
    client_soft_delete,
    check_client_unique_field,
)

from .database_views import (
    ClientDatabaseCreateView,
    ClientDatabaseUpdateView,
    ClientDatabaseDeleteView,
)

from .cdc_views import (
    cdc_discover_tables,
    cdc_configure_tables,
    cdc_create_connector,
    cdc_monitor_connector,
    cdc_connector_action,
    cdc_edit_config,
    cdc_delete_config,
    cdc_config_details,
    cdc_config_update,
    ajax_get_table_schema,
    ajax_get_table_schemas_batch,
    start_replication,
    stop_replication,
    restart_replication_view,
    replication_status,
)

from .dashboard_views import (
    dashboard,
    clients_list,
    replications_list,
    monitoring_dashboard,
)

__all__ = [
    # Client views
    'ClientCreateView',
    'ClientUpdateView',
    'ClientDetailView',
    'client_soft_delete',
    'check_client_unique_field',

    # Database views
    'ClientDatabaseCreateView',
    'ClientDatabaseUpdateView',
    'ClientDatabaseDeleteView',

    # CDC views
    'cdc_discover_tables',
    'cdc_configure_tables',
    'cdc_create_connector',
    'cdc_monitor_connector',
    'cdc_connector_action',
    'cdc_edit_config',
    'cdc_delete_config',
    'cdc_config_details',
    'cdc_config_update',
    'ajax_get_table_schema',
    'ajax_get_table_schemas_batch',
    'start_replication',
    'stop_replication',
    'restart_replication_view',
    'replication_status',

    # Dashboard views
    'dashboard',
    'clients_list',
    'replications_list',
    'monitoring_dashboard',
]