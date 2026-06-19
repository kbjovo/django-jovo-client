"""
Dashboard, clients list, and monitoring views.

This module provides:
- Main dashboard with stats and overview
- Clients list page
- Real-time monitoring dashboard
"""

from django.shortcuts import render
from django.db.models import Q
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

    all_connectors = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).exclude(
        Q(connector_name__isnull=True) | Q(connector_name='')
    )

    total_connectors = all_connectors.count()
    active_connectors = all_connectors.filter(status='active').count()
    failed_connectors = all_connectors.filter(status='error').count()

    # Recently active connectors (ordered by last updated)
    recent_active_connectors = all_connectors.filter(
        status='active'
    ).order_by('-updated_at')[:5]

    # Recent connectors with issues — verify live against Debezium to avoid stale DB state
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
    _manager = DebeziumConnectorManager()
    _candidates = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).filter(status='error').order_by('-updated_at')[:20]

    recent_failed = []
    for _config in _candidates:
        try:
            _exists, _status_data = _manager.get_connector_status(_config.connector_name)
            if not _exists or not _status_data:
                recent_failed.append(_config)
                continue
            _connector_state = _status_data.get('connector', {}).get('state', 'UNKNOWN')
            _tasks = _status_data.get('tasks', [])
            _has_failed = any(t.get('state') == 'FAILED' for t in _tasks)
            if _connector_state == 'FAILED' or _has_failed:
                recent_failed.append(_config)
            else:
                # Connector recovered — clear the stale error status
                _config.status = 'active'
                _config.save(update_fields=['status'])
        except Exception:
            recent_failed.append(_config)
        if len(recent_failed) == 5:
            break

    context = {
        'total_clients': total_clients,
        'active_clients': active_clients,
        'total_connectors': total_connectors,
        'active_connectors': active_connectors,
        'failed_connectors': failed_connectors,
        'recent_active_connectors': recent_active_connectors,
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
        'searchable': ['name', 'email', 'phone', 'company_name'],
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
def monitoring_dashboard(request):
    """
    Real-time monitoring dashboard for all active replications.
    """
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
    from client.models.database import ClientDatabase

    active_replications = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).filter(
        is_active=True,
    ).exclude(
        Q(connector_name__isnull=True) | Q(connector_name='')
    ).order_by('-updated_at')

    # Pre-fetch sink (target) databases keyed by client_id to avoid N+1
    client_ids = {c.client_database.client_id for c in active_replications}
    sink_db_map = {
        db.client_id: db
        for db in ClientDatabase.objects.filter(client_id__in=client_ids, is_target=True)
    }

    manager = DebeziumConnectorManager()

    STATE_PRIORITY = {'ERROR': 0, 'FAILED': 1, 'PAUSED': 2, 'RUNNING': 3, 'UNKNOWN': 4}

    # Sink connectors are shared per target DB — cache state by name to avoid
    # repeating the same Kafka Connect REST call for connectors sharing a sink.
    sink_state_cache = {}

    def get_sink_state(sink_name):
        if not sink_name:
            return 'NOT_CONFIGURED'
        if sink_name in sink_state_cache:
            return sink_state_cache[sink_name]
        try:
            s_exists, s_data = manager.get_connector_status(sink_name)
            state = s_data.get('connector', {}).get('state', 'UNKNOWN') if (s_exists and s_data) else 'NOT_FOUND'
        except Exception:
            state = 'ERROR'
        sink_state_cache[sink_name] = state
        return state

    monitoring_data = []
    for config in active_replications:
        sink_db = sink_db_map.get(config.client_database.client_id)
        try:
            exists, status_data = manager.get_connector_status(config.connector_name)

            if not exists or not status_data:
                logger.warning(f"Connector {config.connector_name} not found in Kafka Connect — skipping from monitoring")
                continue

            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = status_data.get('tasks', [])
            has_failed_task = any(task.get('state') == 'FAILED' for task in tasks)
            connector_trace = status_data.get('connector', {}).get('trace', '')
            monitoring_data.append({
                'config': config,
                'connector_state': connector_state,
                'connector_trace': connector_trace,
                'tasks': tasks,
                'is_healthy': connector_state in ('RUNNING', 'PAUSED') and not has_failed_task,
                'error_count': sum(1 for task in tasks if task.get('state') == 'FAILED'),
                'sink_database': sink_db,
                'sink_state': get_sink_state(config.sink_connector_name),
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
                'sink_database': sink_db,
                'sink_state': get_sink_state(config.sink_connector_name),
            })

    # Sort: FAILED/ERROR first, then PAUSED, then RUNNING; alpha within each group
    monitoring_data.sort(key=lambda x: (
        STATE_PRIORITY.get(x['connector_state'], 4),
        x['config'].connector_name,
    ))

    total_active = len(monitoring_data)
    healthy_count = sum(1 for item in monitoring_data if item.get('is_healthy'))
    unhealthy_count = total_active - healthy_count
    total_errors = sum(item.get('error_count', 0) for item in monitoring_data)

    clients = sorted(
        {(item['config'].client_database.client.pk, item['config'].client_database.client.name)
         for item in monitoring_data},
        key=lambda c: c[1],
    )

    context = {
        'monitoring_data': monitoring_data,
        'total_active': total_active,
        'healthy_count': healthy_count,
        'unhealthy_count': unhealthy_count,
        'total_errors': total_errors,
        'clients': [{'id': c[0], 'name': c[1]} for c in clients],
    }

    return render(request, 'monitoring_dashboard.html', context)