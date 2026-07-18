"""
Connector List Views
- Global connectors list (all clients)
- Client-level connectors list
- Database-level connector list
"""

import logging
from django.conf import settings
from django.db.models import Q
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping
from client.utils.database_utils import get_unassigned_tables

logger = logging.getLogger(__name__)


def client_sink_capacity(request, client_pk):
    """
    AJAX GET: how many source/sink pairs the client's shared target DB can support,
    derived live from the target's max_connections. Re-queried on each call so the
    UI can refresh after the DBA raises max_connections.
    """
    from client.models.client import Client
    from client.utils.database_utils import get_max_connections

    client = get_object_or_404(Client, pk=client_pk)

    # Current pairs = configured source connectors (each has its own sink).
    current_pairs = ReplicationConfig.objects.filter(
        client_database__client=client,
        status__in=['configured', 'active', 'paused', 'error'],
    ).exclude(Q(connector_name__isnull=True) | Q(connector_name='')).count()

    target = client.get_target_database()
    if not target:
        return JsonResponse({'success': True, 'has_target': False, 'current_pairs': current_pairs})

    per_sink = settings.DEBEZIUM_CONFIG.get('CONNECTIONS_PER_SINK', 5)
    headroom = settings.DEBEZIUM_CONFIG.get('TARGET_CONN_HEADROOM', 15)
    max_conn = get_max_connections(target)

    if max_conn is None:
        return JsonResponse({
            'success': True, 'has_target': True, 'reachable': False,
            'target_name': target.connection_name, 'db_type': target.db_type.upper(),
            'current_pairs': current_pairs,
        })

    max_pairs = max(0, (max_conn - headroom) // per_sink)
    return JsonResponse({
        'success': True, 'has_target': True, 'reachable': True,
        'target_name': target.connection_name, 'db_type': target.db_type.upper(),
        'max_connections': max_conn, 'per_sink': per_sink, 'headroom': headroom,
        'max_pairs': max_pairs, 'current_pairs': current_pairs,
        'available': max(0, max_pairs - current_pairs),
    })


# ========================================
# Global Connectors List (All Connectors Across All Clients)
# ========================================

def connectors_list(request):
    """
    Global connectors list page (for sidebar navigation).
    Shows all connectors across all clients with filters and sorting.
    Replaces the old 'Replications' page.
    """
    from client.models.client import Client
    from django.core.paginator import Paginator
    from django.db.models import Q, Count

    # Get all replication configs (connectors) across all clients
    connectors_query = ReplicationConfig.objects.select_related(
        'client_database',
        'client_database__client'
    ).prefetch_related('table_mappings')

    # Apply filters
    client_filter = request.GET.get('client', '')
    database_filter = request.GET.get('database', '')
    status_filter = request.GET.get('status', '')
    search_query = request.GET.get('search', '')

    if client_filter:
        connectors_query = connectors_query.filter(client_database__client_id=client_filter)

    if database_filter:
        connectors_query = connectors_query.filter(client_database_id=database_filter)

    if status_filter:
        connectors_query = connectors_query.filter(status=status_filter)

    if search_query:
        connectors_query = connectors_query.filter(
            Q(connector_name__icontains=search_query) |
            Q(client_database__client__name__icontains=search_query) |
            Q(client_database__database_name__icontains=search_query)
        )

    # Apply sorting
    sort_by = request.GET.get('sort', '-created_at')
    allowed_sorts = ['connector_name', '-connector_name', 'status', '-status',
                     'created_at', '-created_at', 'connector_version', '-connector_version',
                     'client_database__client__name', '-client_database__client__name']
    if sort_by in allowed_sorts:
        connectors_query = connectors_query.order_by(sort_by)

    # Summary stats — a single aggregate query with conditional counts instead of
    # five separate COUNT round-trips.
    stats = connectors_query.aggregate(
        total=Count('id'),
        active=Count('id', filter=Q(status='active')),
        paused=Count('id', filter=Q(status='paused')),
        failed=Count('id', filter=Q(status='error')),
        configured=Count('id', filter=Q(status='configured')),
    )
    total_count = stats['total']
    active_count = stats['active']
    paused_count = stats['paused']
    failed_count = stats['failed']
    configured_count = stats['configured']

    # Pagination
    paginator = Paginator(connectors_query, 15)  # 15 per page for global view
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)

    # Debezium status is served from the background-refreshed Redis cache
    # (client.utils.connector_status_cache) — one bulk read, no blocking per-row
    # Connect REST calls in the request path.
    from client.utils.connector_status_cache import get_statuses
    statuses = get_statuses([c.connector_name for c in page_obj if c.connector_name])
    for connector in page_obj:
        status_data = statuses.get(connector.connector_name)
        if status_data:
            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            connector.debezium_status = {'state': connector_state, 'raw': status_data}
        else:
            connector.debezium_status = {'state': 'NOT_FOUND'}

        # table_mappings is prefetched — count enabled ones in Python to avoid a
        # per-connector COUNT query (.filter() on the manager would bypass the cache).
        connector.table_count = sum(1 for tm in connector.table_mappings.all() if tm.is_enabled)

    # Get all clients and databases for filter dropdowns
    all_clients = Client.objects.filter(status='active').order_by('name')
    all_databases = ClientDatabase.objects.filter(
        client__status='active'
    ).select_related('client').order_by('client__name', 'database_name')

    context = {
        'page_obj': page_obj,
        'total_count': total_count,
        'active_count': active_count,
        'paused_count': paused_count,
        'failed_count': failed_count,
        'configured_count': configured_count,

        # For filters
        'all_clients': all_clients,
        'all_databases': all_databases,
        'client_filter': client_filter,
        'database_filter': database_filter,
        'status_filter': status_filter,
        'search_query': search_query,
        'sort_by': sort_by,

        # For building filter query params
        'query_params': request.GET.copy(),
    }

    return render(request, 'client/connectors/connectors_list.html', context)


# ========================================
# Client-Level Connectors List (All Connectors for a Client)
# ========================================

def client_connectors_list(request, client_pk):
    """
    Display all connectors across all databases for a specific client.
    Shows summary cards, filters, sortable table, and pagination.
    """
    from client.models.client import Client
    from django.core.paginator import Paginator
    from django.db.models import Q, Count

    client = get_object_or_404(Client, pk=client_pk)

    # Get all databases for this client
    databases = client.client_databases.all()

    # Get all replication configs (connectors) for this client
    connectors_query = ReplicationConfig.objects.filter(
        client_database__client=client
    ).select_related('client_database').prefetch_related('table_mappings')

    # Apply filters
    database_filter = request.GET.get('database', '')
    status_filter = request.GET.get('status', '')
    search_query = request.GET.get('search', '')

    if database_filter:
        connectors_query = connectors_query.filter(client_database_id=database_filter)

    if status_filter:
        connectors_query = connectors_query.filter(status=status_filter)

    if search_query:
        connectors_query = connectors_query.filter(
            Q(connector_name__icontains=search_query) |
            Q(client_database__database_name__icontains=search_query)
        )

    # Apply sorting
    sort_by = request.GET.get('sort', '-created_at')
    allowed_sorts = ['connector_name', '-connector_name', 'status', '-status',
                     'created_at', '-created_at', 'connector_version', '-connector_version']
    if sort_by in allowed_sorts:
        connectors_query = connectors_query.order_by(sort_by)

    # Summary stats — single aggregate query instead of five COUNTs.
    stats = connectors_query.aggregate(
        total=Count('id'),
        active=Count('id', filter=Q(status='active')),
        paused=Count('id', filter=Q(status='paused')),
        failed=Count('id', filter=Q(status='error')),
        configured=Count('id', filter=Q(status='configured')),
    )
    total_count = stats['total']
    active_count = stats['active']
    paused_count = stats['paused']
    failed_count = stats['failed']
    configured_count = stats['configured']

    from client.utils.connector_status_cache import get_statuses

    # Sink connectors are now one-per-source-connector. Aggregate their states into
    # a single badge for the client-level list: RUNNING only if every sink is running,
    # FAILED if any failed, PAUSED if any paused, else UNKNOWN. States come from the
    # background-refreshed status cache (one bulk read, no blocking Connect calls).
    sink_connector_name = None
    sink_status = None
    sink_names = ClientDatabase.get_all_sink_connector_names(client)
    if sink_names:
        sink_statuses = get_statuses(sink_names)
        states = [
            (sink_statuses.get(name) or {}).get('connector', {}).get('state', 'NOT_CREATED')
            for name in sink_names
        ]

        if any(s == 'FAILED' for s in states):
            agg_state = 'FAILED'
        elif any(s == 'PAUSED' for s in states):
            agg_state = 'PAUSED'
        elif states and all(s == 'RUNNING' for s in states):
            agg_state = 'RUNNING'
        else:
            agg_state = 'UNKNOWN'

        sink_connector_name = f"{len(sink_names)} sink connector(s)"
        sink_status = {'state': agg_state}

    # Pagination
    paginator = Paginator(connectors_query, 10)  # 10 per page
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)

    # Debezium status for connectors on current page — one bulk cache read.
    statuses = get_statuses([c.connector_name for c in page_obj if c.connector_name])
    for connector in page_obj:
        status_data = statuses.get(connector.connector_name)
        if status_data:
            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            connector.debezium_status = {'state': connector_state, 'raw': status_data}
        else:
            connector.debezium_status = {'state': 'NOT_FOUND'}

        # table_mappings is prefetched — count enabled ones in Python (no extra query).
        connector.table_count = sum(1 for tm in connector.table_mappings.all() if tm.is_enabled)

    context = {
        'client': client,
        'databases': databases,
        'page_obj': page_obj,
        'total_count': total_count,
        'active_count': active_count,
        'paused_count': paused_count,
        'failed_count': failed_count,
        'configured_count': configured_count,
        'sink_connector_name': sink_connector_name,
        'sink_status': sink_status,

        # Filters
        'database_filter': database_filter,
        'status_filter': status_filter,
        'search_query': search_query,
        'sort_by': sort_by,

        # For building filter query params
        'query_params': request.GET.copy(),
    }

    return render(request, 'client/connectors/client_connectors_list.html', context)


# ========================================
# Database-Level Connector List View
# ========================================

def connector_list(request, database_pk):
    """
    Display all source connectors for a database.
    Shows combined view and individual connector details.
    """
    database = get_object_or_404(ClientDatabase, pk=database_pk)
    client = database.client

    # Get all source connectors for this database
    source_connectors = database.get_source_connectors()

    # Get sink connector info
    sink_connector_name = database.get_sink_connector_name()

    # Get connector health summary
    health_summary = database.get_connector_health_summary()

    # Detailed status from the background-refreshed cache — one bulk read for all
    # source + sink connectors instead of a blocking Connect call per row.
    from client.utils.connector_status_cache import get_statuses
    _names = [c.connector_name for c in source_connectors if c.connector_name]
    _names += [c.sink_connector_name for c in source_connectors if c.sink_connector_name]
    if sink_connector_name:
        _names.append(sink_connector_name)
    _statuses = get_statuses(_names)

    connectors_with_status = []
    for config in source_connectors:
        status_data = _statuses.get(config.connector_name)
        if status_data:
            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            config.debezium_status = {'state': connector_state, 'raw': status_data}
        else:
            config.debezium_status = {'state': 'NOT_FOUND'}
            logger.warning(f"Connector {config.connector_name} not found in Kafka Connect — showing as stale")

        # Sink connector status (per source connector; controlled via row toggle)
        if config.sink_connector_name:
            s_data = _statuses.get(config.sink_connector_name)
            s_state = s_data.get('connector', {}).get('state', 'UNKNOWN') if s_data else 'NOT_FOUND'
            config.sink_status = {'state': s_state}
        else:
            config.sink_status = {'state': 'NOT_CONFIGURED'}

        # Get table count
        config.table_count = config.get_table_count()

        connectors_with_status.append(config)

    # Recompute health summary from connectors that actually exist in Kafka
    health_summary['total_source_connectors'] = len(connectors_with_status)
    health_summary['running_source_connectors'] = sum(
        1 for c in connectors_with_status if c.debezium_status.get('state') == 'RUNNING'
    )
    health_summary['failed_source_connectors'] = sum(
        1 for c in connectors_with_status if c.debezium_status.get('state') == 'FAILED'
    )
    health_summary['total_tables'] = sum(c.table_count for c in connectors_with_status)

    # Aggregate sink connector status (from the same bulk cache read above)
    _sink_data = _statuses.get(sink_connector_name)
    sink_status = {'state': _sink_data.get('connector', {}).get('state', 'UNKNOWN')} if _sink_data else {'state': 'NOT_CREATED'}

    # Check if there are unassigned tables
    try:
        unassigned_count = len(get_unassigned_tables(database_pk))
        can_add_connector = unassigned_count > 0
    except Exception as e:
        logger.error(f"Error getting unassigned tables: {e}")
        can_add_connector = False
        unassigned_count = 0

    # Get all tables across all connectors for combined view
    active_config_pks = [c.pk for c in connectors_with_status]
    all_table_mappings = TableMapping.objects.filter(
        replication_config__pk__in=active_config_pks,
        is_enabled=True
    ).select_related('replication_config').order_by('source_table')

    context = {
        'database': database,
        'client': client,
        'source_connectors': connectors_with_status,
        'sink_connector_name': sink_connector_name,
        'sink_status': sink_status,
        'target_database': client.get_target_database(),
        'health_summary': health_summary,
        'can_add_connector': can_add_connector,
        'unassigned_count': unassigned_count,
        'all_table_mappings': all_table_mappings,
        'view_mode': request.GET.get('view', 'by_connector'),  # 'combined' or 'by_connector'
    }

    return render(request, 'client/connectors/connector_list.html', context)