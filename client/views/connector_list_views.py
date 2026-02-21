"""
Connector List Views
- Global connectors list (all clients)
- Client-level connectors list
- Database-level connector list
"""

import logging
from django.shortcuts import render, get_object_or_404
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping
from client.utils.database_utils import get_unassigned_tables
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

logger = logging.getLogger(__name__)


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

    # Calculate summary stats (before pagination)
    total_count = connectors_query.count()
    active_count = connectors_query.filter(status='active').count()
    paused_count = connectors_query.filter(status='paused').count()
    failed_count = connectors_query.filter(status='error').count()
    configured_count = connectors_query.filter(status='configured').count()

    # Pagination
    paginator = Paginator(connectors_query, 15)  # 15 per page for global view
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)

    # Get Debezium status for connectors on current page
    connector_manager = DebeziumConnectorManager()
    for connector in page_obj:
        try:
            exists, status_data = connector_manager.get_connector_status(connector.connector_name)
            if exists and status_data:
                connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                connector.debezium_status = {'state': connector_state, 'raw': status_data}
            else:
                connector.debezium_status = {'state': 'NOT_FOUND'}
        except Exception as e:
            logger.warning(f"Could not get status for {connector.connector_name}: {e}")
            connector.debezium_status = {'state': 'UNKNOWN'}

        # Get table count
        connector.table_count = connector.table_mappings.filter(is_enabled=True).count()

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

    # Calculate summary stats (before pagination)
    total_count = connectors_query.count()
    active_count = connectors_query.filter(status='active').count()
    paused_count = connectors_query.filter(status='paused').count()
    failed_count = connectors_query.filter(status='error').count()
    configured_count = connectors_query.filter(status='configured').count()

    # Get sink connector status (shared across all)
    sink_connector_name = None
    sink_status = None
    if databases.exists():
        first_db = databases.first()
        sink_connector_name = first_db.get_sink_connector_name()

        try:
            connector_manager = DebeziumConnectorManager()
            sink_status = connector_manager.get_connector_status(sink_connector_name)
        except Exception as e:
            logger.warning(f"Could not get sink connector status: {e}")
            sink_status = {'state': 'UNKNOWN'}

    # Pagination
    paginator = Paginator(connectors_query, 10)  # 10 per page
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)

    # Get Debezium status for connectors on current page
    connector_manager = DebeziumConnectorManager()
    for connector in page_obj:
        try:
            exists, status_data = connector_manager.get_connector_status(connector.connector_name)
            if exists and status_data:
                connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                connector.debezium_status = {'state': connector_state, 'raw': status_data}
            else:
                connector.debezium_status = {'state': 'NOT_FOUND'}
        except Exception as e:
            logger.warning(f"Could not get status for {connector.connector_name}: {e}")
            connector.debezium_status = {'state': 'UNKNOWN'}

        # Get table count
        connector.table_count = connector.table_mappings.filter(is_enabled=True).count()

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

    # Get detailed status from Debezium for each connector
    connector_manager = DebeziumConnectorManager()

    connectors_with_status = []
    for config in source_connectors:
        try:
            exists, status_data = connector_manager.get_connector_status(config.connector_name)
            if exists and status_data:
                connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                config.debezium_status = {'state': connector_state, 'raw': status_data}
            else:
                config.debezium_status = {'state': 'NOT_FOUND'}
        except Exception as e:
            logger.warning(f"Could not get status for {config.connector_name}: {e}")
            config.debezium_status = {'state': 'UNKNOWN'}

        # Get table count
        config.table_count = config.get_table_count()

        connectors_with_status.append(config)

    # Get sink connector status
    try:
        sink_status = connector_manager.get_connector_status(sink_connector_name)
    except Exception as e:
        logger.warning(f"Could not get sink connector status: {e}")
        sink_status = {'state': 'NOT_CREATED'}

    # Check if there are unassigned tables
    try:
        unassigned_count = len(get_unassigned_tables(database_pk))
        can_add_connector = unassigned_count > 0
    except Exception as e:
        logger.error(f"Error getting unassigned tables: {e}")
        can_add_connector = False
        unassigned_count = 0

    # Get all tables across all connectors for combined view
    all_table_mappings = TableMapping.objects.filter(
        replication_config__client_database=database,
        replication_config__status__in=['configured', 'active', 'paused', 'error'],
        is_enabled=True
    ).select_related('replication_config').order_by('source_table')

    context = {
        'database': database,
        'client': client,
        'source_connectors': connectors_with_status,
        'sink_connector_name': sink_connector_name,
        'sink_status': sink_status,
        'health_summary': health_summary,
        'can_add_connector': can_add_connector,
        'unassigned_count': unassigned_count,
        'all_table_mappings': all_table_mappings,
        'view_mode': request.GET.get('view', 'by_connector'),  # 'combined' or 'by_connector'
    }

    return render(request, 'client/connectors/connector_list.html', context)