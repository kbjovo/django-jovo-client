"""
Dashboard, clients list, replications list, and monitoring views.

This module provides:
- Main dashboard with stats and overview
- Clients list page
- Global replications list page
- Real-time monitoring dashboard
"""

from django.shortcuts import render
from django.db.models import Q, Count
from django.views.decorators.http import require_http_methods
import logging

from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig
from jovoclient.utils.table_utils import build_paginated_table

logger = logging.getLogger(__name__)


@require_http_methods(["GET"])
def dashboard(request):
    """
    Main dashboard with compact stat cards and quick overview.

    Displays:
    - 5 stat cards (Total Clients, Active Clients, Total Replications, Active Replications, Failed Replications)
    - No tabs - just stats
    - Links to dedicated pages via sidebar
    """
    # Calculate statistics
    total_clients = Client.objects.filter(status__in=["active", "inactive"]).count()
    active_clients = Client.objects.filter(status="active").count()

    all_replications = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).exclude(
        Q(connector_name__isnull=True) | Q(connector_name='')
    )

    total_replications = all_replications.count()
    active_replications = all_replications.filter(status='active').count()
    failed_replications = all_replications.filter(status='error').count()

    # Get recent clients
    recent_clients = Client.objects.filter(
        status__in=["active", "inactive"]
    ).order_by('-created_at')[:5]

    # Get recent replications with issues
    recent_failed = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).filter(status='error').order_by('-updated_at')[:5]

    context = {
        'total_clients': total_clients,
        'active_clients': active_clients,
        'total_replications': total_replications,
        'active_replications': active_replications,
        'failed_replications': failed_replications,
        'recent_clients': recent_clients,
        'recent_failed': recent_failed,
    }

    return render(request, 'dashboard.html', context)


@require_http_methods(["GET"])
def clients_list(request):
    """
    Dedicated clients list page with full table and search.

    Features:
    - Paginated table
    - Search by name, email, phone, company
    - Filter by status
    - Quick actions (view, edit, delete)
    """
    clients = Client.objects.filter(status__in=["active", "inactive"]).order_by('-id')

    table_config = {
        'exclude': ['country', 'updated_at', 'deleted_at'],
        'searchable': ['name', 'email', 'phone', 'company_name', 'db_name'],
        'detail_url_name': 'client_detail',
        'per_page': 15,
        'selectable': True,
        'empty_message': 'No clients found',
        'column_overrides': {
            'name': {
                'type': 'link',
                'clickable': True,
            },
            'status': {
                'type': 'badge',
                'badge_colors': {
                    'active': 'green',
                    'inactive': 'yellow',
                    'deleted': 'red',
                }
            },
        }
    }

    table_data = build_paginated_table(
        queryset=clients,
        request=request,
        config=table_config
    )

    return render(request, 'clients_list.html', table_data)


@require_http_methods(["GET"])
def replications_list(request):
    """
    Global replications list page.

    Shows all replication configurations across all clients with:
    - Filterable by client, status
    - Searchable by client name, database name, connector name
    - Quick actions (edit, start/stop, monitor, delete)
    - Status indicators
    """
    # Get all replications (including incomplete setups)
    all_replications = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).prefetch_related('table_mappings').order_by('-created_at')

    # Apply filters
    search_query = request.GET.get('search', '').strip()
    status_filter = request.GET.get('status_filter', '')
    client_filter = request.GET.get('client_filter', '')

    if search_query:
        all_replications = all_replications.filter(
            Q(client_database__client__name__icontains=search_query) |
            Q(client_database__connection_name__icontains=search_query) |
            Q(connector_name__icontains=search_query)
        )

    if status_filter:
        if status_filter == 'incomplete':
            all_replications = all_replications.filter(
                Q(connector_name__isnull=True) | Q(connector_name='')
            )
        else:
            all_replications = all_replications.filter(status=status_filter).exclude(
                Q(connector_name__isnull=True) | Q(connector_name='')
            )

    if client_filter:
        all_replications = all_replications.filter(client_database__client_id=client_filter)

    # Calculate statistics
    total_replications = all_replications.count()
    active_count = all_replications.filter(status='active').exclude(
        Q(connector_name__isnull=True) | Q(connector_name='')
    ).count()
    configured_count = all_replications.filter(status='configured').exclude(
        Q(connector_name__isnull=True) | Q(connector_name='')
    ).count()
    paused_count = all_replications.filter(status='paused').count()
    error_count = all_replications.filter(status='error').count()
    incomplete_count = all_replications.filter(
        Q(connector_name__isnull=True) | Q(connector_name='')
    ).count()

    # Build replication rows for table display - grouped by status
    replication_rows_by_status = {
        'active': [],
        'error': [],
        'paused': [],
        'configured': [],
        'incomplete': [],
    }

    for config in all_replications:
        # Detect incomplete status (no connector created)
        is_incomplete = not config.connector_name or config.connector_name == ''
        effective_status = 'incomplete' if is_incomplete else config.status

        # Determine if tables are configured
        has_tables = config.table_mappings.filter(is_enabled=True).exists()

        row_data = {
            'id': config.id,
            'client_name': config.client_database.client.name,
            'client_id': config.client_database.client.id,
            'database_id': config.client_database.id,
            'database_name': config.client_database.connection_name,
            'database_type': config.client_database.get_db_type_display(),
            'status': effective_status,
            'status_display': 'Incomplete Setup' if is_incomplete else config.get_status_display(),
            'connector_name': config.connector_name if not is_incomplete else None,
            'tables_count': config.table_mappings.filter(is_enabled=True).count(),
            'last_sync': config.last_sync_at,
            'created_at': config.created_at,
            'sync_type': config.get_sync_type_display(),
            'is_incomplete': is_incomplete,
            'has_tables': has_tables,
        }

        # Add to appropriate status group
        if effective_status in replication_rows_by_status:
            replication_rows_by_status[effective_status].append(row_data)

    # Get all clients for filter dropdown
    all_clients = Client.objects.filter(status__in=["active", "inactive"]).order_by('name')

    context = {
        'replication_rows_by_status': replication_rows_by_status,
        'total_replications': total_replications,
        'active_count': active_count,
        'configured_count': configured_count,
        'paused_count': paused_count,
        'error_count': error_count,
        'incomplete_count': incomplete_count,
        'all_clients': all_clients,
        'search_query': search_query,
        'status_filter': status_filter,
        'client_filter': client_filter,
    }

    return render(request, 'replications_list.html', context)


@require_http_methods(["GET"])
def monitoring_dashboard(request):
    """
    Real-time monitoring dashboard for all active replications.

    Shows:
    - Active connectors with health status
    - Recent sync activity
    - Error alerts
    - Performance metrics
    - Task statuses
    """
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

    # Get all active replications
    active_replications = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).filter(
        is_active=True,
        status='active'
    ).exclude(
        Q(connector_name__isnull=True) | Q(connector_name='')
    ).order_by('-updated_at')

    manager = DebeziumConnectorManager()

    # Build monitoring data
    monitoring_data = []
    for config in active_replications:
        try:
            exists, status_data = manager.get_connector_status(config.connector_name)

            if exists and status_data:
                connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                tasks = status_data.get('tasks', [])

                monitoring_data.append({
                    'config': config,
                    'connector_state': connector_state,
                    'tasks': tasks,
                    'is_healthy': connector_state == 'RUNNING' and all(
                        task.get('state') == 'RUNNING' for task in tasks
                    ),
                    'error_count': sum(1 for task in tasks if task.get('state') == 'FAILED'),
                })
            else:
                monitoring_data.append({
                    'config': config,
                    'connector_state': 'NOT_FOUND',
                    'tasks': [],
                    'is_healthy': False,
                    'error_count': 1,
                })
        except Exception as e:
            logger.error(f"Error fetching status for {config.connector_name}: {e}")
            monitoring_data.append({
                'config': config,
                'connector_state': 'ERROR',
                'tasks': [],
                'is_healthy': False,
                'error_count': 1,
                'error_message': str(e),
            })

    # Calculate summary stats
    total_active = len(monitoring_data)
    healthy_count = sum(1 for item in monitoring_data if item.get('is_healthy'))
    unhealthy_count = total_active - healthy_count
    total_errors = sum(item.get('error_count', 0) for item in monitoring_data)

    context = {
        'monitoring_data': monitoring_data,
        'total_active': total_active,
        'healthy_count': healthy_count,
        'unhealthy_count': unhealthy_count,
        'total_errors': total_errors,
    }

    return render(request, 'monitoring_dashboard.html', context)