"""
Client app views module.

This module is organized into separate files for better maintainability:
- client_views.py: Client CRUD operations
- database_views.py: ClientDatabase CRUD and connection testing
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
    SinkConnectorCreateView,
    SinkConnectorUpdateView,
    SinkConnectorDeleteView,
)

from .dashboard_views import (
    dashboard,
    clients_list,
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
    'SinkConnectorCreateView',
    'SinkConnectorUpdateView',
    'SinkConnectorDeleteView',

    # Dashboard views
    'dashboard',
    'clients_list',
    'monitoring_dashboard',
]
