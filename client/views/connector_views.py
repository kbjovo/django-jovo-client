"""
Multi-Source Connector Management Views
Supports multiple source connectors per database with shared sink connector
"""

import logging
import json
from django.conf import settings
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.db import transaction
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ConnectorHistory
from client.utils.database_utils import get_table_list, get_table_schema, get_unassigned_tables, get_row_count
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.connector_templates import (
    generate_connector_name,
    get_connector_config_for_database
)
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager

logger = logging.getLogger(__name__)


# ========================================
# AJAX Endpoints
# ========================================

def ajax_get_table_schema(request, database_pk, table_name):
    """
    AJAX endpoint to get table schema (columns) for a specific table.
    Used by accordion UI to load columns on-demand.
    """
    database = get_object_or_404(ClientDatabase, pk=database_pk)

    try:
        schema = get_table_schema(database, table_name)
        columns = schema.get('columns', [])

        # Convert SQLAlchemy types to JSON-serializable format
        serializable_columns = []
        for col in columns:
            serializable_col = {
                'name': col.get('name'),
                'type': str(col.get('type', '')),  # Convert SQLAlchemy type to string
                'nullable': col.get('nullable', True),
                'default': str(col.get('default')) if col.get('default') is not None else None,
                'primary_key': col.get('name') in schema.get('primary_keys', []),
            }
            serializable_columns.append(serializable_col)

        return JsonResponse({
            'success': True,
            'table_name': table_name,
            'columns': serializable_columns,
            'row_count': schema.get('row_count', 0),
        })

    except Exception as e:
        logger.error(f"Error getting table schema for {table_name}: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


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


# ========================================
# Add New Source Connector
# ========================================

def connector_add(request, database_pk):
    """
    Add a new source connector to an existing database.
    Shows only unassigned tables and performance settings.
    """
    database = get_object_or_404(ClientDatabase, pk=database_pk)
    client = database.client

    # Get next version number
    next_version = ConnectorHistory.get_next_version(
        client.id,
        database.id,
        connector_type='source'
    )

    # Generate connector name preview
    connector_name_preview = generate_connector_name(client, database, version=next_version)

    # Get unassigned tables (row counts loaded lazily via AJAX on accordion expand)
    try:
        unassigned_tables = get_unassigned_tables(database_pk)
        tables_with_info = [{'name': t} for t in unassigned_tables]

    except Exception as e:
        logger.error(f"Error getting unassigned tables: {e}")
        messages.error(request, f"Error loading tables: {str(e)}")
        return redirect('connector_list', database_pk=database_pk)

    if not unassigned_tables:
        messages.warning(request, "No unassigned tables available. All tables are already assigned to connectors.")
        return redirect('connector_list', database_pk=database_pk)

    # POST: Create new source connector
    if request.method == 'POST':
        try:
            with transaction.atomic():
                # Get selected tables
                selected_tables = request.POST.getlist('selected_tables')
                if not selected_tables:
                    messages.error(request, "Please select at least one table")
                    return redirect('connector_add', database_pk=database_pk)

                # Validate table count limit
                max_tables = settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25)
                if len(selected_tables) > max_tables:
                    messages.error(request, f"Maximum {max_tables} tables per connector allowed. You selected {len(selected_tables)}. Reduce the selection to prevent worker crashes during snapshots.")
                    return redirect('connector_add', database_pk=database_pk)

                # Validate all selected tables are unassigned
                assigned_tables = set(unassigned_tables)
                invalid_tables = [t for t in selected_tables if t not in assigned_tables]
                if invalid_tables:
                    messages.error(request, f"Tables already assigned: {', '.join(invalid_tables)}")
                    return redirect('connector_add', database_pk=database_pk)

                # Get processing mode settings
                processing_mode = request.POST.get('processing_mode', 'cdc')
                batch_interval = request.POST.get('batch_interval') if processing_mode == 'batch' else None
                batch_max_catchup = int(request.POST.get('batch_max_catchup_minutes', 5))
                # Validate batch_max_catchup is one of the allowed values
                if batch_max_catchup not in [5, 10, 20]:
                    batch_max_catchup = 5

                # Create ReplicationConfig with performance settings
                replication_config = ReplicationConfig.objects.create(
                    client_database=database,
                    connector_version=next_version,
                    sync_type='realtime',
                    sync_frequency='realtime',
                    status='configured',
                    is_active=False,
                    auto_create_tables=request.POST.get('auto_create_tables') == 'on',

                    # Processing mode settings
                    processing_mode=processing_mode,
                    batch_interval=batch_interval,
                    batch_max_catchup_minutes=batch_max_catchup,

                    # Performance tuning settings
                    snapshot_mode=request.POST.get('snapshot_mode', 'initial'),
                    max_queue_size=int(request.POST.get('max_queue_size', 8192)),
                    max_batch_size=int(request.POST.get('max_batch_size', 2048)),
                    poll_interval_ms=int(request.POST.get('poll_interval_ms', 500)),
                    incremental_snapshot_chunk_size=int(request.POST.get('incremental_snapshot_chunk_size', 1024)),
                    snapshot_fetch_size=int(request.POST.get('snapshot_fetch_size', 10000)),
                    sink_batch_size=int(request.POST.get('sink_batch_size', 3000)),
                    sink_max_poll_records=int(request.POST.get('sink_max_poll_records', 5000)),

                    created_by=request.user if request.user.is_authenticated else None
                )

                # Use user-provided name or fall back to auto-generated
                custom_name = request.POST.get('connector_name', '').strip()
                connector_name = custom_name if custom_name else generate_connector_name(client, database, version=next_version)
                replication_config.connector_name = connector_name
                # Topic prefix must match connector template (includes version for JMX uniqueness)
                replication_config.kafka_topic_prefix = f"client_{client.id}_db_{database.id}_v_{next_version}"
                replication_config.save()

                logger.info(f"Created ReplicationConfig v{next_version}: {replication_config.id}")

                # Create TableMappings for selected tables
                for table_name in selected_tables:
                    # Get table schema for columns
                    try:
                        schema = get_table_schema(database, table_name)
                        columns = schema.get('columns', [])
                        primary_keys = schema.get('primary_keys', [])

                        # Merge primary_keys info into columns
                        for col in columns:
                            col['primary_key'] = col['name'] in primary_keys

                        logger.info(f"Table {table_name}: {len(columns)} columns, {len(primary_keys)} primary keys: {primary_keys}")

                        # Parse schema and table name for MS SQL and Oracle
                        # MS SQL tables come as "schema.table" (e.g., "dbo.Customers")
                        # Oracle tables come as "SCHEMA.TABLE" (e.g., "CDC_USER.CUSTOMERS")
                        # We need to store them separately to avoid duplicate schema in topics
                        if (database.db_type == 'mssql' or database.db_type == 'oracle') and '.' in table_name:
                            source_schema, actual_table_name = table_name.split('.', 1)
                        else:
                            source_schema = schema.get('schema', '')
                            actual_table_name = table_name

                        # Build default target table name to match sink connector transform
                        # Sink connector uses: transforms.extractTableName.replacement = "$1_$2"
                        # Where $1 is schema/database and $2 is table name
                        # Result format: {schema}_{table} (e.g., kbe_tally_item_mapping)
                        if database.db_type == 'mysql':
                            default_target_table = f"{database.database_name}_{actual_table_name}"
                        elif database.db_type == 'postgresql':
                            pg_schema = source_schema or 'public'
                            default_target_table = f"{pg_schema}_{actual_table_name}"
                        elif database.db_type == 'mssql':
                            mssql_schema = source_schema or 'dbo'
                            default_target_table = f"{mssql_schema}_{actual_table_name}"
                        elif database.db_type == 'oracle':
                            oracle_schema = source_schema or database.username.upper()
                            default_target_table = f"{oracle_schema}_{actual_table_name}"
                        else:
                            default_target_table = actual_table_name

                        # Get target table name from form (editable field) with proper default
                        target_table_name = request.POST.get(f'target_table_{table_name}', default_target_table)

                        # Create TableMapping
                        table_mapping = TableMapping.objects.create(
                            replication_config=replication_config,
                            source_table=actual_table_name,
                            target_table=target_table_name,
                            source_schema=source_schema,
                            is_enabled=True,
                        )

                        logger.info(f"Created TableMapping for {table_name} -> {target_table_name}")

                    except Exception as e:
                        logger.error(f"Error creating mappings for table {table_name}: {e}")
                        raise

                # Record in connector history
                ConnectorHistory.record_connector_creation(
                    replication_config,
                    connector_name,
                    next_version,
                    connector_type='source'
                )

                messages.success(request, f"Source connector v{next_version} created successfully with {len(selected_tables)} tables")

                # Redirect to connector creation (which will also create sink if first)
                return redirect('connector_create_debezium', config_pk=replication_config.id)

        except Exception as e:
            logger.error(f"Error creating connector: {e}", exc_info=True)
            messages.error(request, f"Error creating connector: {str(e)}")
            return redirect('connector_add', database_pk=database_pk)

    # GET: Show form
    context = {
        'database': database,
        'client': client,
        'next_version': next_version,
        'connector_name_preview': connector_name_preview,
        'unassigned_tables': tables_with_info,
        'snapshot_mode_choices': ReplicationConfig.SNAPSHOT_MODE_CHOICES,
        'processing_mode_choices': ReplicationConfig.PROCESSING_MODE_CHOICES,
        'batch_interval_choices': ReplicationConfig.BATCH_INTERVAL_CHOICES,
        'max_tables_per_connector': settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25),
    }

    return render(request, 'client/connectors/connector_add.html', context)


# ========================================
# Create Debezium Connectors (Source + Sink)
# ========================================

def connector_create_debezium(request, config_pk):
    """
    Create Debezium source connector and sink connector (if first).
    This is called after connector configuration is saved.

    For batch processing mode, the connector is created in PAUSED state
    and a Celery Beat schedule is set up.
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    # Check if batch mode - use orchestrator for proper setup
    if replication_config.processing_mode == 'batch':
        try:
            from client.replication.orchestrator import ReplicationOrchestrator
            orchestrator = ReplicationOrchestrator(replication_config)

            success, message = orchestrator.start_batch_replication()

            if success:
                messages.success(
                    request,
                    f"Batch connector {replication_config.connector_name} created successfully. "
                    f"Next sync: {replication_config.next_batch_run}"
                )
            else:
                messages.error(request, f"Error creating batch connector: {message}")
                replication_config.status = 'error'
                replication_config.last_error_message = message
                replication_config.save()

            return redirect('connector_monitor', config_pk=replication_config.pk)

        except Exception as e:
            logger.error(f"Error creating batch connector: {e}", exc_info=True)
            messages.error(request, f"Error creating batch connector: {str(e)}")
            replication_config.status = 'error'
            replication_config.last_error_message = str(e)
            replication_config.save()
            return redirect('connector_monitor', config_pk=replication_config.pk)

    # CDC mode - continue with existing logic
    try:
        connector_manager = DebeziumConnectorManager()

        # Step 1: Create Kafka topics (required since auto-create is disabled)
        logger.info(f"Creating Kafka topics for connector: {replication_config.connector_name}")
        topic_manager = KafkaTopicManager()

        topics_success, topics_message = topic_manager.create_topics_for_config(replication_config)
        if not topics_success:
            raise Exception(f"Failed to create Kafka topics: {topics_message}")

        logger.info(f"Topics created: {topics_message}")

        # Note: Target tables are auto-created by sink connector (schema.evolution=basic)
        # No manual table creation needed

        # Step 2: Create source connector
        logger.info(f"Creating source connector: {replication_config.connector_name}")

        # Get table list for this connector
        table_mappings = replication_config.table_mappings.filter(is_enabled=True)
        tables_list = [tm.source_table for tm in table_mappings]

        # Generate source connector config
        source_config = get_connector_config_for_database(
            db_config=database,
            replication_config=replication_config,
            tables_whitelist=tables_list
        )

        logger.info(f"All columns will be replicated (column selection feature removed)")

        # Create source connector
        connector_manager.create_connector(replication_config.connector_name, source_config)

        # Update status
        replication_config.status = 'active'
        replication_config.is_active = True
        replication_config.connector_state = 'RUNNING'
        replication_config.save()

        messages.success(request, f"Source connector {replication_config.connector_name} created successfully")

        # Step 3: Check if sink connector exists, if not create it
        sink_connector_name = database.get_sink_connector_name()

        exists, _ = connector_manager.get_connector_status(sink_connector_name)
        if not exists:
            logger.info(f"Creating sink connector: {sink_connector_name}")

            # Get target database
            target_database = ClientDatabase.objects.filter(
                client=client,
                is_target=True
            ).first()

            if not target_database:
                messages.warning(request, "No target database configured. Sink connector not created.")
            else:
                # Configure sink connector to handle tables with and without PKs
                # - primary.key.mode=record_key: Extract PK from message key (Debezium extracts automatically)
                # - DO NOT specify primary.key.fields when using record_key mode - it auto-extracts from key schema
                # - This allows different tables with different PKs to work correctly
                # - delete_enabled=True: Process tombstone delete events
                # - schema.evolution=basic: Allow table schema to evolve with source changes
                # - topics.regex is set by sink_connector_templates.py (excludes ddl_events and debezium_signal)
                sink_config = get_sink_connector_config_for_database(
                    db_config=target_database,
                    topics=None,  # Use regex instead of explicit topic list
                    delete_enabled=True,
                    custom_config={
                        'name': sink_connector_name,
                        # Do NOT include 'primary.key.fields' - record_key mode extracts keys automatically
                    }
                )

                # Create sink connector
                connector_manager.create_connector(sink_connector_name, sink_config)

                # Update all configs with sink name
                database.replication_configs.update(
                    sink_connector_name=sink_connector_name,
                    sink_connector_state='RUNNING'
                )

                # Record sink in history
                ConnectorHistory.record_connector_creation(
                    replication_config,
                    sink_connector_name,
                    1,  # Sink always version 1
                    connector_type='sink'
                )

                messages.success(request, f"Sink connector {sink_connector_name} created successfully")
        else:
            # Sink connector exists - ensure it uses topics.regex for auto-subscription
            logger.info(f"Sink connector {sink_connector_name} already exists - ensuring topics.regex is set")

            target_database = ClientDatabase.objects.filter(
                client=client,
                is_target=True
            ).first()

            if target_database:
                # topics.regex is set by sink_connector_templates.py (excludes ddl_events and debezium_signal)
                sink_config = get_sink_connector_config_for_database(
                    db_config=target_database,
                    topics=None,
                    delete_enabled=True,
                    custom_config={
                        'name': sink_connector_name,
                    }
                )

                connector_manager.update_connector_config(sink_connector_name, sink_config)
                logger.info(f"✓ Updated sink connector with topics.regex from template")

            replication_config.sink_connector_name = sink_connector_name
            replication_config.save()

        # FK constraints are now applied manually via the monitor page button
        return redirect('connector_monitor', config_pk=replication_config.pk)

    except Exception as e:
        logger.error(f"Error creating connectors: {e}", exc_info=True)
        messages.error(request, f"Error creating connectors: {str(e)}")

        # Mark config as error
        replication_config.status = 'error'
        replication_config.last_error_message = str(e)
        replication_config.save()

        return redirect('connector_list', database_pk=database.id)


# ========================================
# Edit Connector Tables (Add/Remove with Signals)
# ========================================

def connector_edit_tables(request, config_pk):
    """
    Edit tables assigned to a source connector.
    - Remove tables: Uses orchestrator.remove_tables() for full cleanup
    - Add tables: Uses orchestrator.add_tables() with incremental snapshot signals
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    # Get current tables
    current_tables = list(
        replication_config.table_mappings.filter(is_enabled=True)
        .values_list('source_table', flat=True)
    )

    # Get available tables to add with row counts
    try:
        unassigned_table_names = get_unassigned_tables(database.id)

        # Get row counts for each table (same as connector_add)
        unassigned_tables = []
        for table_name in unassigned_table_names:
            try:
                schema = get_table_schema(database, table_name)
                row_count = schema.get('row_count', 0)
                unassigned_tables.append({
                    'name': table_name,
                    'row_count': row_count,
                })
            except Exception as e:
                logger.warning(f"Could not get info for table {table_name}: {e}")
                unassigned_tables.append({
                    'name': table_name,
                    'row_count': None,
                })
    except Exception as e:
        logger.error(f"Error getting unassigned tables: {e}")
        unassigned_tables = []
        unassigned_table_names = []

    # POST: Process changes
    if request.method == 'POST':
        try:
            # Update connector settings
            settings_changed = False
            settings_fields = {
                'snapshot_mode': request.POST.get('snapshot_mode'),
                'max_queue_size': request.POST.get('max_queue_size'),
                'max_batch_size': request.POST.get('max_batch_size'),
                'poll_interval_ms': request.POST.get('poll_interval_ms'),
                'incremental_snapshot_chunk_size': request.POST.get('incremental_snapshot_chunk_size'),
                'snapshot_fetch_size': request.POST.get('snapshot_fetch_size'),
                'sink_batch_size': request.POST.get('sink_batch_size'),
                'sink_max_poll_records': request.POST.get('sink_max_poll_records'),
            }

            if settings_fields['snapshot_mode'] and settings_fields['snapshot_mode'] != replication_config.snapshot_mode:
                replication_config.snapshot_mode = settings_fields['snapshot_mode']
                settings_changed = True
            for int_field in ['max_queue_size', 'max_batch_size', 'poll_interval_ms', 'incremental_snapshot_chunk_size',
                               'snapshot_fetch_size', 'sink_batch_size', 'sink_max_poll_records']:
                if settings_fields[int_field]:
                    new_val = int(settings_fields[int_field])
                    if new_val != getattr(replication_config, int_field):
                        setattr(replication_config, int_field, new_val)
                        settings_changed = True

            drop_before_sync = 'drop_before_sync' in request.POST
            if drop_before_sync != replication_config.drop_before_sync:
                replication_config.drop_before_sync = drop_before_sync
                settings_changed = True

            # Processing mode & batch settings
            processing_mode = request.POST.get('processing_mode', replication_config.processing_mode)
            old_processing_mode = replication_config.processing_mode
            mode_switched = processing_mode != old_processing_mode

            if mode_switched:
                replication_config.processing_mode = processing_mode
                settings_changed = True

            if processing_mode == 'batch':
                batch_interval = request.POST.get('batch_interval')
                if mode_switched and not batch_interval:
                    messages.error(request, "Please select a sync interval for batch mode")
                    return redirect('connector_edit_tables', config_pk=config_pk)
                if batch_interval and batch_interval != replication_config.batch_interval:
                    replication_config.batch_interval = batch_interval
                    settings_changed = True
                batch_max_catchup = int(request.POST.get('batch_max_catchup_minutes', replication_config.batch_max_catchup_minutes))
                if batch_max_catchup not in [5, 10, 20]:
                    batch_max_catchup = 5
                if batch_max_catchup != replication_config.batch_max_catchup_minutes:
                    replication_config.batch_max_catchup_minutes = batch_max_catchup
                    settings_changed = True
            elif processing_mode == 'cdc' and mode_switched:
                # Switching to CDC — clear batch settings (schedule cleanup happens below)
                replication_config.batch_interval = None
                settings_changed = True

            if settings_changed:
                replication_config.save()

                from client.replication.orchestrator import ReplicationOrchestrator
                orch = ReplicationOrchestrator(replication_config)

                if mode_switched:
                    if processing_mode == 'batch':
                        # CDC → Batch: setup schedule (will pause connector on schedule)
                        orch.setup_batch_schedule()
                        # Pause connector immediately — batch schedule will resume it
                        orch.pause_connector()
                        messages.success(request, "Switched to batch mode — connector paused, batch schedule created")
                    elif processing_mode == 'cdc':
                        # Batch → CDC: remove schedule and resume connector for continuous streaming
                        orch.remove_batch_schedule()
                        orch.resume_connector()
                        messages.success(request, "Switched to CDC mode — batch schedule removed, connector resumed")
                else:
                    # Same mode, just settings changed — reschedule if batch
                    if processing_mode == 'batch' and replication_config.batch_celery_task_name:
                        orch.setup_batch_schedule()
                    messages.success(request, "Connector settings updated")

            # Handle table changes
            tables_to_remove = request.POST.getlist('remove_tables')
            tables_to_add = request.POST.getlist('add_tables')

            if not tables_to_remove and not tables_to_add:
                if settings_changed:
                    return redirect('connector_list', database_pk=database.id)
                messages.warning(request, "No changes specified")
                return redirect('connector_edit_tables', config_pk=config_pk)

            from client.replication.orchestrator import ReplicationOrchestrator
            orchestrator = ReplicationOrchestrator(replication_config)

            # Handle removals
            if tables_to_remove:
                remaining_count = replication_config.table_mappings.filter(
                    is_enabled=True
                ).exclude(source_table__in=tables_to_remove).count()

                if remaining_count == 0:
                    messages.error(request, "Cannot remove all tables. Delete the connector instead.")
                    return redirect('connector_edit_tables', config_pk=config_pk)

                success, message = orchestrator.remove_tables(tables_to_remove)
                if success:
                    messages.success(request, message)
                else:
                    messages.error(request, f"Error removing tables: {message}")
                    return redirect('connector_edit_tables', config_pk=config_pk)

            # Handle additions
            if tables_to_add:
                # Validate tables are unassigned
                invalid_tables = [t for t in tables_to_add if t not in unassigned_table_names]
                if invalid_tables:
                    messages.error(request, f"Tables already assigned: {', '.join(invalid_tables)}")
                    return redirect('connector_edit_tables', config_pk=config_pk)

                # Validate table count limit (current tables minus removals plus additions)
                max_tables = settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25)
                current_count = len(current_tables) - len(tables_to_remove)
                total_after = current_count + len(tables_to_add)
                if total_after > max_tables:
                    messages.error(request, f"Maximum {max_tables} tables per connector. After changes you'd have {total_after}. Reduce the selection.")
                    return redirect('connector_edit_tables', config_pk=config_pk)

                success, message = orchestrator.add_tables(tables_to_add)
                if success:
                    messages.success(request, message)
                else:
                    messages.error(request, f"Error adding tables: {message}")

            return redirect('connector_list', database_pk=database.id)

        except Exception as e:
            logger.error(f"Error editing tables: {e}", exc_info=True)
            messages.error(request, f"Error: {str(e)}")
            return redirect('connector_edit_tables', config_pk=config_pk)

    # GET: Show form
    context = {
        'replication_config': replication_config,
        'database': database,
        'client': client,
        'current_tables': current_tables,
        'unassigned_tables': unassigned_tables,
        'max_tables_per_connector': settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25),
    }

    return render(request, 'client/connectors/connector_edit_tables.html', context)


# ========================================
# Delete Connector (Global - handles all delete cases)
# ========================================

def connector_delete(request, config_pk):
    """
    Delete a source connector with validations.
    Uses orchestrator for proper cleanup including topics and target tables.
    Handles redirect based on 'next' parameter or defaults to global connectors list.
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    # Determine redirect destination
    next_url = request.GET.get('next') or request.POST.get('next')

    # Check if this is the last connector
    other_connectors = database.replication_configs.exclude(pk=config_pk).filter(
        status__in=['configured', 'active', 'paused', 'error']
    )
    is_last_connector = not other_connectors.exists()

    # POST: Confirm and delete
    if request.method == 'POST':
        try:
            from client.replication.orchestrator import ReplicationOrchestrator

            # Read optional cleanup flags from modal checkboxes
            delete_topics = bool(request.POST.get('delete_topics'))
            drop_tables = bool(request.POST.get('drop_tables'))
            truncate_tables = bool(request.POST.get('truncate_tables'))

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.delete_replication(
                delete_topics=delete_topics,
                drop_tables=drop_tables,
                truncate_tables=truncate_tables,
            )

            if success:
                extras = []
                if delete_topics:
                    extras.append("topics deleted")
                if drop_tables:
                    extras.append("target tables dropped")
                if truncate_tables and not drop_tables:
                    extras.append("target tables truncated")
                suffix = f" ({', '.join(extras)})" if extras else ""
                messages.success(
                    request,
                    f"Connector {replication_config.connector_name} deleted successfully{suffix}"
                )
            else:
                messages.error(request, f"Error deleting connector: {message}")

            # Redirect based on 'next' parameter
            if next_url == 'database':
                return redirect('connector_list', database_pk=database.id)
            elif next_url == 'client':
                return redirect('client_connectors_list', client_pk=client.id)
            else:
                return redirect('connectors_list')

        except Exception as e:
            logger.error(f"Error deleting connector: {e}", exc_info=True)
            messages.error(request, f"Error deleting connector: {str(e)}")
            return redirect('connector_delete', config_pk=config_pk)

    # GET: Show confirmation
    context = {
        'replication_config': replication_config,
        'database': database,
        'client': client,
        'is_last_connector': is_last_connector,
        'table_count': replication_config.get_table_count(),
        'next': next_url,
    }

    return render(request, 'client/connectors/connector_delete.html', context)


# ========================================
# Recreate Source Connector (same version, same topics)
# ========================================

def connector_recreate(request, config_pk):
    """
    Recreate a source connector without changing version, topics, or sink.

    Use when a connector is FAILED/stuck and needs a fresh start while
    keeping the same Kafka topics and target tables intact.

    POST only. Accepts optional 'snapshot_mode' parameter:
      - 'when_needed' (default): Resume from Kafka offset
      - 'initial': Full re-snapshot of all data
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database

    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)

    try:
        from client.replication.orchestrator import ReplicationOrchestrator

        snapshot_mode = request.POST.get('snapshot_mode', 'when_needed')
        if snapshot_mode not in ('when_needed', 'initial'):
            snapshot_mode = 'when_needed'

        orchestrator = ReplicationOrchestrator(replication_config)
        success, message = orchestrator.recreate_source_connector(
            snapshot_mode=snapshot_mode
        )

        if success:
            messages.success(request, message)
        else:
            messages.error(request, f"Recreate failed: {message}")

    except Exception as e:
        logger.error(f"Error recreating connector: {e}", exc_info=True)
        messages.error(request, f"Error recreating connector: {str(e)}")

    return redirect('connector_monitor', config_pk=config_pk)


# ========================================
# Connector Monitor (Real-time Status + Snapshot Progress)
# ========================================

def connector_monitor(request, config_pk):
    """
    Real-time monitoring page for a connector.
    Shows connector status, snapshot progress via Jolokia, and table mappings.
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    if not replication_config.connector_name:
        messages.info(request, 'No connector created yet for this configuration.')
        return redirect('connector_list', database_pk=database.id)

    try:
        from client.replication.orchestrator import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(replication_config)
        unified_status = orchestrator.get_unified_status()

        source_state = unified_status['source_connector']['state']
        sink_state = unified_status['sink_connector']['state']
        source_exists = source_state not in ('NOT_FOUND', 'ERROR')
        sink_exists = sink_state not in ('NOT_FOUND', 'ERROR', 'NOT_CONFIGURED')

        source_tasks = unified_status['source_connector'].get('tasks', [])
        sink_tasks = unified_status['sink_connector'].get('tasks', [])

        # Connector uptime — most recent active ConnectorHistory entry for this source connector
        from client.models.replication import ConnectorHistory
        connector_history = ConnectorHistory.objects.filter(
            connector_name=replication_config.connector_name,
            connector_type='source',
        ).exclude(status='deleted').order_by('-created_at').first()

        context = {
            'replication_config': replication_config,
            'database': database,
            'client': client,
            'unified_status': unified_status,
            'source_exists': source_exists,
            'sink_exists': sink_exists,
            'source_state': source_state,
            'sink_state': sink_state,
            'source_tasks': source_tasks,
            'sink_tasks': sink_tasks,
            'snapshot': unified_status.get('snapshot'),
            'enabled_table_mappings': replication_config.table_mappings.filter(is_enabled=True).order_by('source_table'),
            'connector_history': connector_history,
        }

        return render(request, 'client/connectors/connector_monitor.html', context)

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        messages.error(request, f'Failed to get connector status: {str(e)}')
        return redirect('connector_list', database_pk=database.id)


def connector_status_api(request, config_pk):
    """
    AJAX endpoint returning unified status JSON including snapshot progress.
    Polled by the monitor page for live updates.
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        from client.replication.orchestrator import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(replication_config)
        unified_status = orchestrator.get_unified_status()

        return JsonResponse({'success': True, 'status': unified_status})

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# ========================================
# Connector Action Endpoints (AJAX)
# ========================================

def connector_pause(request, config_pk):
    """Pause a running source connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.pause_connector()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to pause connector: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_resume(request, config_pk):
    """Resume a paused source connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.resume_connector()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to resume connector: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_restart(request, config_pk):
    """Restart a source connector (light restart via Kafka Connect REST API)."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        connector_mgr = DebeziumConnectorManager()
        success, error = connector_mgr.restart_connector(config.connector_name)
        if success:
            return JsonResponse({'success': True, 'message': 'Connector restarted successfully'})
        return JsonResponse({'success': False, 'message': error})
    except Exception as e:
        logger.error(f'Failed to restart connector: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_restart_failed_tasks(request, config_pk):
    """Restart only FAILED tasks for the source connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.restart_failed_tasks()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to restart failed tasks: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_restart_all_tasks(request, config_pk):
    """Restart all tasks for the source connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.restart_all_tasks()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to restart tasks: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_sync_schedule(request, config_pk):
    """Re-establish batch schedule for a connector that is out of sync."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.sync_with_schedule()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to sync schedule: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# ========================================
# Sink Connector Action Endpoints (AJAX)
# ========================================

def sink_restart(request, config_pk):
    """Restart the shared sink connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        if not config.sink_connector_name:
            return JsonResponse({'success': False, 'message': 'No sink connector configured'})
        connector_mgr = DebeziumConnectorManager()
        success, error = connector_mgr.restart_connector(config.sink_connector_name)
        if success:
            return JsonResponse({'success': True, 'message': 'Sink connector restarted'})
        return JsonResponse({'success': False, 'message': error})
    except Exception as e:
        logger.error(f'Failed to restart sink: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def sink_restart_failed_tasks(request, config_pk):
    """Restart only FAILED tasks for the shared sink connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        if not config.sink_connector_name:
            return JsonResponse({'success': False, 'message': 'No sink connector configured'})
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.restart_failed_tasks(connector_name=config.sink_connector_name)
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to restart sink failed tasks: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def sink_restart_all_tasks(request, config_pk):
    """Restart all tasks for the shared sink connector."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        if not config.sink_connector_name:
            return JsonResponse({'success': False, 'message': 'No sink connector configured'})
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.restart_all_tasks(connector_name=config.sink_connector_name)
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to restart sink tasks: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_table_rows_api(request, config_pk):
    """
    AJAX endpoint returning row counts for all enabled tables
    from both source and target databases.
    """
    from client.utils.database_utils import get_database_connection

    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        source_db = config.client_database
        target_db = source_db.client.get_target_database()

        # Test source database connectivity
        source_reachable = True
        try:
            with get_database_connection(source_db) as conn:
                pass
        except Exception as e:
            source_reachable = False
            logger.warning(f'Source database unreachable ({source_db.connection_name}): {e}')

        # Test target database connectivity
        target_reachable = True
        if target_db:
            try:
                with get_database_connection(target_db) as conn:
                    pass
            except Exception as e:
                target_reachable = False
                logger.warning(f'Target database unreachable ({target_db.connection_name}): {e}')

        mappings = config.table_mappings.filter(is_enabled=True).order_by('source_table')
        rows = []

        for mapping in mappings:
            source_count = None
            source_error = None
            target_count = None
            target_error = None

            # Count source rows
            if not source_reachable:
                source_error = 'db_unreachable'
            else:
                try:
                    source_count = get_row_count(
                        source_db,
                        mapping.source_table,
                        schema=mapping.source_schema,
                    )
                except Exception as e:
                    err_str = str(e).lower()
                    if "doesn't exist" in err_str or 'not exist' in err_str or 'not found' in err_str or 'unknown table' in err_str or 'invalid object' in err_str:
                        source_error = 'table_not_found'
                    else:
                        source_error = 'error'
                    logger.warning(f'Source row count failed for {mapping.source_table}: {e}')

            # Count target rows
            if not target_db:
                target_error = 'no_target_db'
            elif not target_reachable:
                target_error = 'db_unreachable'
            else:
                try:
                    target_count = get_row_count(
                        target_db,
                        mapping.target_table,
                        schema=mapping.target_schema or None,
                    )
                except Exception as e:
                    err_str = str(e).lower()
                    if "doesn't exist" in err_str or 'not exist' in err_str or 'not found' in err_str or 'unknown table' in err_str or 'invalid object' in err_str:
                        target_error = 'table_not_found'
                    else:
                        target_error = 'error'
                    logger.warning(f'Target row count failed for {mapping.target_table}: {e}')

            rows.append({
                'source_table': mapping.source_table,
                'target_table': mapping.target_table,
                'source_rows': source_count,
                'target_rows': target_count,
                'source_error': source_error,
                'target_error': target_error,
            })

        return JsonResponse({
            'success': True,
            'tables': rows,
            'source_reachable': source_reachable,
            'target_reachable': target_reachable,
            'has_target_db': target_db is not None,
        })

    except Exception as e:
        logger.error(f'Failed to get table row counts: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_fk_preview_api(request, config_pk):
    """
    AJAX GET endpoint returning a preview of FK constraints for the monitor page.
    Read-only — does not create anything.
    """
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.utils.table_creator import preview_foreign_keys
        data = preview_foreign_keys(config)
        return JsonResponse({'success': True, **data})
    except Exception as e:
        logger.error(f'FK preview failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_fk_apply_api(request, config_pk):
    """
    AJAX POST endpoint that applies FK constraints to the target database.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.utils.table_creator import add_foreign_keys_to_target
        created, skipped, errors = add_foreign_keys_to_target(config)
        return JsonResponse({
            'success': True,
            'created': created,
            'skipped': skipped,
            'errors': errors,
        })
    except Exception as e:
        logger.error(f'FK apply failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_live_metrics_api(request, config_pk):
    """
    AJAX endpoint for live connector metrics, polled every ~10s by the monitor page.
    Returns:
      - streaming: Jolokia JMX streaming metrics (lag, throughput, queue utilisation)
      - dlq: Dead Letter Queue message count via Kafka Admin
      - config_diff: Live connector config vs stored model values
    """
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        database = config.client_database
        result = {}

        # ── 1. Streaming metrics via Jolokia ───────────────────────────────
        try:
            from jovoclient.utils.debezium.jolokia_client import JolokiaClient
            jolokia = JolokiaClient()
            raw = jolokia.get_streaming_metrics(database.db_type, config.kafka_topic_prefix or '')
            if raw:
                lag_ms = raw.get('MilliSecondsBehindSource', 0) or 0
                queue_total = raw.get('QueueTotalCapacity', 0) or 0
                queue_remaining = raw.get('QueueRemainingCapacity', 0) or 0
                queue_used = queue_total - queue_remaining
                queue_pct = round((queue_used / queue_total) * 100) if queue_total else 0
                ms_since_last = raw.get('MilliSecondsSinceLastEvent', 0) or 0
                result['streaming'] = {
                    'available': True,
                    'lag_ms': lag_ms,
                    'lag_display': f"{lag_ms:,} ms" if lag_ms < 60000 else f"{lag_ms // 1000:,} s",
                    'total_events': raw.get('TotalNumberOfEventsSeen', 0),
                    'filtered_events': raw.get('NumberOfEventsFiltered', 0),
                    'queue_total': queue_total,
                    'queue_used': queue_used,
                    'queue_pct': queue_pct,
                    'ms_since_last_event': ms_since_last,
                    'idle_display': f"{ms_since_last // 1000}s ago" if ms_since_last < 3600000 else "over 1h ago",
                }
            else:
                result['streaming'] = {'available': False}
        except Exception as e:
            result['streaming'] = {'available': False, 'error': str(e)}

        # ── 2. DLQ message count via confluent_kafka consumer ──────────────
        try:
            from confluent_kafka import Consumer, TopicPartition
            from django.conf import settings as django_settings
            kafka_servers = django_settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS', 'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            dlq_topic = f"client_{database.client.id}.dlq"
            consumer = Consumer({
                'bootstrap.servers': kafka_servers,
                'group.id': 'jovo-dlq-inspector',
                'auto.offset.reset': 'earliest',
            })
            meta = consumer.list_topics(topic=dlq_topic, timeout=5)
            topic_meta = meta.topics.get(dlq_topic)
            if topic_meta and not topic_meta.error:
                total = 0
                for partition_id in topic_meta.partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(dlq_topic, partition_id), timeout=3
                    )
                    total += max(0, high - low)
                result['dlq'] = {'available': True, 'topic': dlq_topic, 'count': total}
            else:
                result['dlq'] = {'available': True, 'topic': dlq_topic, 'count': 0}
            consumer.close()
        except Exception as e:
            result['dlq'] = {'available': False, 'error': str(e)}

        # ── 3. Live config diff (Kafka Connect REST vs stored model) ───────
        try:
            from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
            manager = DebeziumConnectorManager()
            live_config = manager.get_connector_config(config.connector_name)
            if live_config:
                TRACKED = {
                    'max.queue.size': ('max_queue_size', int),
                    'max.batch.size': ('max_batch_size', int),
                    'poll.interval.ms': ('poll_interval_ms', int),
                    'snapshot.fetch.size': ('snapshot_fetch_size', int),
                }
                diffs = []
                for live_key, (model_attr, cast) in TRACKED.items():
                    live_val = live_config.get(live_key)
                    stored_val = getattr(config, model_attr, None)
                    if live_val is not None and stored_val is not None:
                        if cast(live_val) != stored_val:
                            diffs.append({
                                'key': live_key,
                                'live': live_val,
                                'stored': str(stored_val),
                            })
                result['config_diff'] = {
                    'available': True,
                    'diffs': diffs,
                    'in_sync': len(diffs) == 0,
                }
            else:
                result['config_diff'] = {'available': False}
        except Exception as e:
            result['config_diff'] = {'available': False, 'error': str(e)}

        return JsonResponse({'success': True, **result})

    except Exception as e:
        logger.error(f'Failed to get live metrics: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)
