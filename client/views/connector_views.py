"""
Multi-Source Connector Management Views
Supports multiple source connectors per database with shared sink connector
"""

import logging
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.db import transaction
from django.views.decorators.http import require_http_methods

from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping, ConnectorHistory
from client.utils.database_utils import get_table_list, get_table_schema, get_unassigned_tables
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.connector_templates import (
    generate_connector_name,
    get_connector_config_for_database
)
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
from jovoclient.utils.kafka.signal import DebeziumSignalManager, KafkaSignalManager

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
            status = connector_manager.get_connector_status(connector.connector_name)
            connector.debezium_status = status
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
            status = connector_manager.get_connector_status(connector.connector_name)
            connector.debezium_status = status
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
            status = connector_manager.get_connector_status(config.connector_name)
            config.debezium_status = status
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

    # Get unassigned tables
    try:
        unassigned_tables = get_unassigned_tables(database_pk)

        # Get row counts for each table
        tables_with_info = []
        for table_name in unassigned_tables:
            try:
                schema = get_table_schema(database, table_name)
                row_count = schema.get('row_count', 0)
                tables_with_info.append({
                    'name': table_name,
                    'row_count': row_count,
                })
            except Exception as e:
                logger.warning(f"Could not get info for table {table_name}: {e}")
                tables_with_info.append({
                    'name': table_name,
                    'row_count': 0,
                })

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

                # Validate all selected tables are unassigned
                assigned_tables = set(unassigned_tables)
                invalid_tables = [t for t in selected_tables if t not in assigned_tables]
                if invalid_tables:
                    messages.error(request, f"Tables already assigned: {', '.join(invalid_tables)}")
                    return redirect('connector_add', database_pk=database_pk)

                # Create ReplicationConfig with performance settings
                replication_config = ReplicationConfig.objects.create(
                    client_database=database,
                    connector_version=next_version,
                    sync_type='realtime',
                    sync_frequency='realtime',
                    status='configured',
                    is_active=False,
                    auto_create_tables=request.POST.get('auto_create_tables') == 'on',

                    # Performance tuning settings
                    snapshot_mode=request.POST.get('snapshot_mode', 'initial'),
                    max_queue_size=int(request.POST.get('max_queue_size', 8192)),
                    max_batch_size=int(request.POST.get('max_batch_size', 2048)),
                    poll_interval_ms=int(request.POST.get('poll_interval_ms', 500)),
                    incremental_snapshot_chunk_size=int(request.POST.get('incremental_snapshot_chunk_size', 1024)),

                    created_by=request.user if request.user.is_authenticated else None
                )

                # Generate and save connector name
                connector_name = generate_connector_name(client, database, version=next_version)
                replication_config.connector_name = connector_name
                # Topic prefix must match connector template (without _connector_v_X suffix)
                replication_config.kafka_topic_prefix = f"client_{client.id}_db_{database.id}"
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

                        # Get target table name from form (editable field)
                        target_table_name = request.POST.get(f'target_table_{table_name}', table_name)

                        # Parse schema and table name for MS SQL and Oracle
                        # MS SQL tables come as "schema.table" (e.g., "dbo.Customers")
                        # Oracle tables come as "SCHEMA.TABLE" (e.g., "CDC_USER.CUSTOMERS")
                        # We need to store them separately to avoid duplicate schema in topics
                        if (database.db_type == 'mssql' or database.db_type == 'oracle') and '.' in table_name:
                            source_schema, actual_table_name = table_name.split('.', 1)
                        else:
                            source_schema = schema.get('schema', '')
                            actual_table_name = table_name

                        # Create TableMapping
                        table_mapping = TableMapping.objects.create(
                            replication_config=replication_config,
                            source_table=actual_table_name,
                            target_table=target_table_name,
                            source_schema=source_schema,
                            is_enabled=True,
                        )

                        logger.info(f"Created TableMapping for {table_name} -> {target_table_name}")

                        # Create ColumnMappings with is_enabled based on user selection
                        enabled_columns_count = 0
                        disabled_columns = []
                        for col in columns:
                            # Check if column checkbox was checked
                            # Checkbox name format: column_{table_name}_{column_name}
                            checkbox_name = f"column_{table_name}_{col['name']}"
                            checkbox_value = request.POST.get(checkbox_name)
                            is_column_enabled = checkbox_value == '1'

                            # Debug logging
                            if not is_column_enabled:
                                disabled_columns.append(col['name'])
                                logger.debug(f"Column {col['name']}: checkbox_name={checkbox_name}, value={checkbox_value}, enabled={is_column_enabled}")

                            ColumnMapping.objects.create(
                                table_mapping=table_mapping,
                                source_column=col['name'],
                                target_column=col['name'],  # Same as source for now
                                source_type=col.get('type', ''),
                                target_type=col.get('type', ''),
                                is_enabled=is_column_enabled,
                                is_primary_key=col.get('primary_key', False),
                                is_nullable=col.get('nullable', True),
                            )

                            if is_column_enabled:
                                enabled_columns_count += 1

                        logger.info(f"Created {enabled_columns_count}/{len(columns)} enabled ColumnMappings for {table_name}")
                        if disabled_columns:
                            logger.info(f"Disabled columns for {table_name}: {disabled_columns}")

                        # Validate at least one column is enabled
                        if enabled_columns_count == 0:
                            raise ValueError(f"Table '{table_name}' must have at least one column enabled")

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
    }

    return render(request, 'client/connectors/connector_add.html', context)


# ========================================
# Create Debezium Connectors (Source + Sink)
# ========================================

def connector_create_debezium(request, config_pk):
    """
    Create Debezium source connector and sink connector (if first).
    This is called after connector configuration is saved.
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    try:
        connector_manager = DebeziumConnectorManager()

        # Step 1: Create Kafka topics (required since auto-create is disabled)
        logger.info(f"Creating Kafka topics for connector: {replication_config.connector_name}")
        topic_manager = KafkaTopicManager()

        topics_success, topics_message = topic_manager.create_topics_for_config(replication_config)
        if not topics_success:
            raise Exception(f"Failed to create Kafka topics: {topics_message}")

        logger.info(f"Topics created: {topics_message}")

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

        # Debug: Check if column.include.list was generated
        has_column_filtering = "column.include.list" in source_config
        if has_column_filtering:
            logger.info(f"✅ column.include.list was generated with value: {source_config['column.include.list']}")

            # Delete old schemas to prevent compatibility issues
            # When column filtering is used, the new schema may have fewer fields than the old schema
            # which causes BACKWARD compatibility errors in Schema Registry
            from jovoclient.utils.debezium.schema_registry_utils import delete_schemas_for_tables

            logger.info(f"🗑️  Deleting old schemas for tables with column filtering to prevent compatibility issues")

            # Build topic names for schema deletion
            # For MySQL: database.table (e.g., kbe.busyuk_items)
            # For PostgreSQL: schema.table (e.g., public.users)
            # For MS SQL: database.schema.table (e.g., AppDB.dbo.Customers)
            # For Oracle: pdb.schema.table (e.g., XEPDB1.CDC_USER.CUSTOMERS)
            topic_prefix = replication_config.kafka_topic_prefix
            schema_table_names = []

            for tm in table_mappings:
                if database.db_type == 'mysql':
                    # MySQL: database.table
                    table_name_for_schema = f"{database.database_name}.{tm.source_table}"
                elif database.db_type == 'postgresql':
                    # PostgreSQL: schema.table
                    schema = tm.source_schema or 'public'
                    table_name_for_schema = f"{schema}.{tm.source_table}"
                elif database.db_type == 'mssql':
                    # MS SQL: database.schema.table
                    schema = tm.source_schema or 'dbo'
                    table_name_for_schema = f"{database.database_name}.{schema}.{tm.source_table}"
                elif database.db_type == 'oracle':
                    # Oracle: pdb.schema.table
                    schema = database.username.upper()
                    if schema.startswith('C##'):
                        schema = schema[3:]
                    pdb = database.database_name.upper()
                    table_name_for_schema = f"{pdb}.{schema}.{tm.source_table}"
                else:
                    table_name_for_schema = tm.source_table

                schema_table_names.append(table_name_for_schema)

            # Delete schemas for all tables
            try:
                delete_results = delete_schemas_for_tables(topic_prefix, schema_table_names, permanent=True)
                deleted_count = sum(1 for r in delete_results.values() if r.get('key') and r.get('value'))
                logger.info(f"✅ Deleted schemas for {deleted_count}/{len(schema_table_names)} tables")
            except Exception as e:
                logger.warning(f"⚠️  Failed to delete some schemas (will try to create connector anyway): {e}")
        else:
            logger.warning(f"⚠️ column.include.list was NOT generated - all columns will be replicated")

        # Create source connector
        connector_manager.create_connector(replication_config.connector_name, source_config)

        # Update status
        replication_config.status = 'active'
        replication_config.is_active = True
        replication_config.connector_state = 'RUNNING'
        replication_config.save()

        messages.success(request, f"Source connector {replication_config.connector_name} created successfully")

        # Step 2: Check if sink connector exists, if not create it
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
                # Generate sink connector config with regex to match all DATA topics from this database
                # Topic format: client_{client_id}_db_{db_id}.{database}.{table}
                # or: client_{client_id}_db_{db_id}.{schema}.{table} (for PostgreSQL/Oracle)
                # IMPORTANT: Exclude .signals topic (used for Debezium signaling)
                topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

                # Configure sink connector to handle tables with and without PKs
                # - primary.key.mode=record_key: Extract PK from message key (Debezium extracts automatically)
                # - DO NOT specify primary.key.fields when using record_key mode - it auto-extracts from key schema
                # - This allows different tables with different PKs to work correctly
                # - delete_enabled=True: Process tombstone delete events
                # - schema.evolution=basic: Allow table schema to evolve with source changes
                sink_config = get_sink_connector_config_for_database(
                    db_config=target_database,
                    topics=None,  # Use regex instead of explicit topic list
                    delete_enabled=True,
                    custom_config={
                        'name': sink_connector_name,
                        'topics.regex': topic_regex,  # Regex to match all data topics from this database
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
            logger.info(f"Sink connector {sink_connector_name} already exists")
            replication_config.sink_connector_name = sink_connector_name
            replication_config.save()

        return redirect('connector_list', database_pk=database.id)

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
    - Remove tables: Requires connector restart
    - Add tables: Uses Debezium signals (no restart if supported)
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    # Get current tables
    current_tables = list(
        replication_config.table_mappings.filter(is_enabled=True)
        .values_list('source_table', flat=True)
    )

    # Get available tables to add
    try:
        unassigned_tables = get_unassigned_tables(database.id)
    except Exception as e:
        logger.error(f"Error getting unassigned tables: {e}")
        unassigned_tables = []

    # POST: Process changes
    if request.method == 'POST':
        try:
            tables_to_remove = request.POST.getlist('remove_tables')
            tables_to_add = request.POST.getlist('add_tables')

            if not tables_to_remove and not tables_to_add:
                messages.warning(request, "No changes specified")
                return redirect('connector_edit_tables', config_pk=config_pk)

            with transaction.atomic():
                # Handle removals - use orchestrator for full cleanup
                if tables_to_remove:
                    from client.replication.orchestrator import ReplicationOrchestrator

                    # Check if removing all tables
                    remaining_count = replication_config.table_mappings.filter(
                        is_enabled=True
                    ).exclude(source_table__in=tables_to_remove).count()

                    if remaining_count == 0:
                        messages.error(request, "Cannot remove all tables. Delete the connector instead.")
                        return redirect('connector_edit_tables', config_pk=config_pk)

                    # Use orchestrator to remove tables with full cleanup
                    # (deletes topics, drops target tables, updates connectors)
                    orchestrator = ReplicationOrchestrator(replication_config)
                    success, message = orchestrator.remove_tables(tables_to_remove)

                    if success:
                        messages.success(request, message)
                    else:
                        messages.error(request, f"Error removing tables: {message}")
                        return redirect('connector_edit_tables', config_pk=config_pk)

                # Handle additions (try signals first)
                if tables_to_add:
                    # Validate tables are unassigned
                    for table in tables_to_add:
                        if table not in unassigned_tables:
                            messages.error(request, f"Table {table} is already assigned")
                            return redirect('connector_edit_tables', config_pk=config_pk)

                    # Create table mappings
                    if tables_to_add:
                        # Validate tables are unassigned
                        for table in tables_to_add:
                            if table not in unassigned_tables:
                                messages.error(request, f"Table {table} is already assigned")
                                return redirect('connector_edit_tables', config_pk=config_pk)

                        # Create or re-enable table mappings
                        for table_name in tables_to_add:
                            try:
                                schema = get_table_schema(database, table_name)
                                columns = schema.get('columns', [])
                                primary_keys = schema.get('primary_keys', [])

                                for col in columns:
                                    col['primary_key'] = col['name'] in primary_keys

                                logger.info(f"Table {table_name}: {len(columns)} columns, {len(primary_keys)} primary keys: {primary_keys}")

                                if (database.db_type == 'mssql' or database.db_type == 'oracle') and '.' in table_name:
                                    source_schema, actual_table_name = table_name.split('.', 1)
                                else:
                                    source_schema = schema.get('schema', '')
                                    actual_table_name = table_name

                                # Check if a disabled mapping already exists (from previous removal)
                                existing_mapping = TableMapping.objects.filter(
                                    replication_config=replication_config,
                                    source_table=actual_table_name,
                                    is_enabled=False
                                ).first()

                                if existing_mapping:
                                    # Re-enable the existing mapping
                                    existing_mapping.is_enabled = True
                                    existing_mapping.source_schema = source_schema
                                    existing_mapping.save()
                                    table_mapping = existing_mapping

                                    # Delete old column mappings and recreate fresh ones
                                    table_mapping.column_mappings.all().delete()
                                    logger.info(f"Re-enabled existing mapping for table {table_name}")
                                else:
                                    # Create new table mapping
                                    table_mapping = TableMapping.objects.create(
                                        replication_config=replication_config,
                                        source_table=actual_table_name,
                                        target_table=actual_table_name,
                                        source_schema=source_schema,
                                        is_enabled=True,
                                    )
                                    logger.info(f"Created new mapping for table {table_name}")

                                # Create column mappings
                                for col in columns:
                                    ColumnMapping.objects.create(
                                        table_mapping=table_mapping,
                                        source_column=col['name'],
                                        target_column=col['name'],
                                        source_type=col.get('type', ''),
                                        target_type=col.get('type', ''),
                                        is_enabled=True,
                                        is_primary_key=col.get('primary_key', False),
                                        is_nullable=col.get('nullable', True),
                                    )

                                logger.info(f"Created {len(columns)} column mappings for table {table_name}")

                            except Exception as e:
                                logger.error(f"Error creating mappings for {table_name}: {e}")
                                raise

                        # Try to add tables using Kafka signals (MySQL, PostgreSQL, MS SQL supported)
                        try:
                            if database.db_type in ['mysql', 'postgresql', 'mssql']:
                                # Create Kafka topics for new tables BEFORE sending signals
                                logger.info("Creating Kafka topics for new tables...")
                                topic_manager = KafkaTopicManager()
                                topics_success, topics_message = topic_manager.create_topics_for_config(replication_config)

                                if not topics_success:
                                    logger.warning(f"Topic creation warning: {topics_message}")
                                else:
                                    logger.info(f"✅ Topics ready: {topics_message}")

                                # Update source connector config with new tables
                                logger.info("Updating source connector configuration...")
                                connector_manager = DebeziumConnectorManager()

                                all_tables = list(
                                    replication_config.table_mappings.filter(is_enabled=True)
                                    .values_list('source_table', flat=True)
                                )

                                source_config = get_connector_config_for_database(
                                    db_config=database,
                                    replication_config=replication_config,
                                    tables_whitelist=all_tables
                                )

                                connector_manager.update_connector_config(replication_config.connector_name, source_config)
                                logger.info(f"✅ Source connector config updated with {len(all_tables)} tables")

                                # Wait for connector to reload and verify it's running before sending signal
                                import time
                                max_retries = 6
                                for i in range(max_retries):
                                    time.sleep(0.5)
                                    exists, status_data = connector_manager.get_connector_status(replication_config.connector_name)
                                    if exists and status_data:
                                        state = status_data.get('connector', {}).get('state', '')
                                        if state == 'RUNNING':
                                            logger.info(f"✅ Connector is RUNNING, ready for signal")
                                            break
                                        logger.info(f"⏳ Connector state: {state}, waiting...")
                                else:
                                    logger.warning("⚠️ Connector not confirmed RUNNING, proceeding anyway")

                                # ========================================
                                # Signal Implementation - Database vs Kafka based on DB type
                                # ========================================
                                from django.conf import settings

                                # Format tables for signal based on database type
                                formatted_tables = []
                                for table in tables_to_add:
                                    if database.db_type == 'mssql':
                                        # MS SQL: Always needs database.schema.table format (e.g., AppDB.dbo.Products)
                                        # Tables may come as "Orders", "dbo.Orders", or "AppDB.dbo.Orders"
                                        dot_count = table.count('.')
                                        if dot_count == 0:
                                            # Just table name: Orders -> AppDB.dbo.Orders
                                            formatted_tables.append(f"{database.database_name}.dbo.{table}")
                                        elif dot_count == 1:
                                            # schema.table: dbo.Orders -> AppDB.dbo.Orders
                                            formatted_tables.append(f"{database.database_name}.{table}")
                                        else:
                                            # Already fully qualified: AppDB.dbo.Orders
                                            formatted_tables.append(table)
                                    elif '.' in table:
                                        formatted_tables.append(table)
                                    elif database.db_type == 'mysql':
                                        # MySQL: database.table format
                                        formatted_tables.append(f"{database.database_name}.{table}")
                                    elif database.db_type == 'postgresql':
                                        # PostgreSQL: schema.table format (default schema is 'public')
                                        formatted_tables.append(f"public.{table}")

                                if database.db_type == 'postgresql':
                                    # ✅ PostgreSQL: Use DATABASE signals (more reliable)
                                    # Insert signal directly into debezium_signal table
                                    logger.info(f"Using DATABASE-based signaling for PostgreSQL")

                                    from jovoclient.utils.kafka.signal import DebeziumSignalManager
                                    from client.utils.database_utils import get_database_engine

                                    # Get SQLAlchemy engine for the source database
                                    db_engine = get_database_engine(database)
                                    signal_table = "public.debezium_signal"

                                    signal_manager = DebeziumSignalManager(
                                        db_engine=db_engine,
                                        signal_table=signal_table
                                    )

                                    # Send incremental snapshot signal via database
                                    signal_id = signal_manager.trigger_adhoc_snapshot(formatted_tables)

                                    logger.info(f"✅ Database signal sent - ID: {signal_id}")
                                    logger.debug(f"   Signal table: {signal_table}")
                                    logger.debug(f"   Tables: {formatted_tables}")

                                    # Wait for Debezium to process the signal and confirm source connector is producing
                                    # Poll the source connector topics endpoint to verify new topics appear
                                    logger.debug("Waiting for source connector to process signal and produce to new topics...")
                                    max_wait_retries = 15  # 15 * 1 second = 15 seconds max
                                    expected_topic_count = len(all_tables) + 1  # data tables + signal table
                                    for retry in range(max_wait_retries):
                                        time.sleep(1)
                                        try:
                                            topics_response = connector_manager.get_connector_topics(replication_config.connector_name)
                                            if topics_response:
                                                current_topics = topics_response.get(replication_config.connector_name, {}).get('topics', [])
                                                logger.debug(f"Retry {retry + 1}: Source connector has {len(current_topics)} topics")
                                                # Check if we have topics for the new tables
                                                if len(current_topics) >= expected_topic_count:
                                                    logger.debug(f"Source connector now producing to {len(current_topics)} topics")
                                                    break
                                        except Exception as poll_err:
                                            logger.debug(f"Poll error: {poll_err}")
                                    else:
                                        logger.debug("Timeout waiting for new topics, proceeding anyway")

                                    # Reconfigure and restart sink connector to pick up new topics
                                    # Need to update config (not just restart) to force re-evaluation of topics.regex
                                    if replication_config.sink_connector_name:
                                        try:
                                            logger.debug(f"Reconfiguring sink connector to pick up new topics: {replication_config.sink_connector_name}")

                                            # Get target database
                                            target_db = ClientDatabase.objects.filter(
                                                client=client,
                                                is_target=True
                                            ).first()

                                            if target_db:
                                                # Generate updated sink config with topics.regex
                                                topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

                                                sink_config = get_sink_connector_config_for_database(
                                                    db_config=target_db,
                                                    topics=None,
                                                    delete_enabled=True,
                                                    custom_config={
                                                        'name': replication_config.sink_connector_name,
                                                        'topics.regex': topic_regex,
                                                    }
                                                )

                                                if sink_config:
                                                    connector_manager.update_connector_config(
                                                        replication_config.sink_connector_name,
                                                        sink_config
                                                    )
                                                    # CRITICAL: Restart sink connector to re-evaluate topics.regex
                                                    # Kafka Connect only discovers topics matching regex at startup
                                                    connector_manager.restart_connector(replication_config.sink_connector_name)
                                                    logger.info(f"✅ Sink connector reconfigured and restarted with topics.regex: {topic_regex}")
                                        except Exception as sink_err:
                                            logger.warning(f"Could not reconfigure sink connector: {sink_err}")

                                    messages.success(
                                        request,
                                        f"Added {len(tables_to_add)} tables using database-based incremental snapshot (Signal ID: {signal_id})."
                                    )

                                elif database.db_type == 'mssql':
                                    # ✅ MS SQL: Use DATABASE signals (requires signal table)
                                    # Insert signal directly into debezium_signal table
                                    logger.info(f"Using DATABASE-based signaling for MS SQL")

                                    from jovoclient.utils.kafka.signal import DebeziumSignalManager
                                    from client.utils.database_utils import get_database_engine

                                    # Get SQLAlchemy engine for the source database
                                    db_engine = get_database_engine(database)
                                    # MS SQL signal table: dbo.debezium_signal
                                    signal_table = "dbo.debezium_signal"

                                    signal_manager = DebeziumSignalManager(
                                        db_engine=db_engine,
                                        signal_table=signal_table
                                    )

                                    # Send incremental snapshot signal via database
                                    signal_id = signal_manager.trigger_adhoc_snapshot(formatted_tables)

                                    logger.info(f"✅ Database signal sent - ID: {signal_id}")
                                    logger.info(f"   Signal table: {signal_table}")
                                    logger.info(f"   Tables: {formatted_tables}")

                                    # Wait for Debezium to process the signal and confirm source connector is producing
                                    # Poll the source connector topics endpoint to verify new topics appear
                                    logger.debug("Waiting for source connector to process signal and produce to new topics...")
                                    max_wait_retries = 15  # 15 * 1 second = 15 seconds max
                                    expected_topic_count = len(all_tables) + 1  # data tables + signal table
                                    for retry in range(max_wait_retries):
                                        time.sleep(1)
                                        try:
                                            topics_response = connector_manager.get_connector_topics(replication_config.connector_name)
                                            if topics_response:
                                                current_topics = topics_response.get(replication_config.connector_name, {}).get('topics', [])
                                                logger.debug(f"Retry {retry + 1}: Source connector has {len(current_topics)} topics")
                                                # Check if we have topics for the new tables
                                                if len(current_topics) >= expected_topic_count:
                                                    logger.debug(f"Source connector now producing to {len(current_topics)} topics")
                                                    break
                                        except Exception as poll_err:
                                            logger.debug(f"Poll error: {poll_err}")
                                    else:
                                        logger.debug("Timeout waiting for new topics, proceeding anyway")

                                    # Reconfigure and restart sink connector to pick up new topics
                                    if replication_config.sink_connector_name:
                                        try:
                                            logger.debug(f"Reconfiguring sink connector to pick up new topics: {replication_config.sink_connector_name}")

                                            # Get target database
                                            target_db = ClientDatabase.objects.filter(
                                                client=client,
                                                is_target=True
                                            ).first()

                                            if target_db:
                                                # Generate updated sink config with topics.regex
                                                topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

                                                sink_config = get_sink_connector_config_for_database(
                                                    db_config=target_db,
                                                    topics=None,
                                                    delete_enabled=True,
                                                    custom_config={
                                                        'name': replication_config.sink_connector_name,
                                                        'topics.regex': topic_regex,
                                                    }
                                                )

                                                if sink_config:
                                                    connector_manager.update_connector_config(
                                                        replication_config.sink_connector_name,
                                                        sink_config
                                                    )
                                                    # CRITICAL: Restart sink connector to re-evaluate topics.regex
                                                    # Kafka Connect only discovers topics matching regex at startup
                                                    connector_manager.restart_connector(replication_config.sink_connector_name)
                                                    logger.info(f"✅ Sink connector reconfigured and restarted with topics.regex: {topic_regex}")
                                        except Exception as sink_err:
                                            logger.warning(f"Could not reconfigure sink connector: {sink_err}")

                                    messages.success(
                                        request,
                                        f"Added {len(tables_to_add)} tables using database-based incremental snapshot (Signal ID: {signal_id})."
                                    )

                                else:
                                    # MySQL: Use KAFKA signals (read-only database support)
                                    logger.info(f"Using Kafka-based signaling for MySQL")

                                    kafka_bootstrap_servers = settings.DEBEZIUM_CONFIG.get(
                                        'KAFKA_INTERNAL_SERVERS',
                                        settings.DEBEZIUM_CONFIG.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:29092,kafka-2:29092,kafka-3:29092')
                                    )

                                    # Signal topic format: client_{client_id}_db_{db_id}.signals
                                    signal_topic = f"client_{client.id}_db_{database.id}.signals"
                                    topic_prefix = f"client_{client.id}_db_{database.id}"

                                    signal_manager = KafkaSignalManager(
                                        bootstrap_servers=kafka_bootstrap_servers,
                                        signal_topic=signal_topic,
                                        connector_name=topic_prefix
                                    )

                                    # Send incremental snapshot signal via Kafka
                                    signal_id = signal_manager.trigger_adhoc_snapshot(formatted_tables)
                                    signal_manager.close()

                                    # Restart connector to ensure SignalProcessor polls and processes the signal
                                    logger.debug("Restarting connector to ensure signal is processed...")
                                    connector_manager.restart_connector(replication_config.connector_name)

                                    # Wait for source connector to process signal and produce to new topics
                                    # Poll the source connector topics endpoint to verify new topics appear
                                    logger.debug("Waiting for source connector to process signal and produce to new topics...")
                                    max_wait_retries = 15  # 15 * 1 second = 15 seconds max
                                    expected_topic_count = len(all_tables) + 1  # data tables + signal table
                                    for retry in range(max_wait_retries):
                                        time.sleep(1)
                                        try:
                                            topics_response = connector_manager.get_connector_topics(replication_config.connector_name)
                                            if topics_response:
                                                current_topics = topics_response.get(replication_config.connector_name, {}).get('topics', [])
                                                logger.debug(f"Retry {retry + 1}: Source connector has {len(current_topics)} topics")
                                                # Check if we have topics for the new tables
                                                if len(current_topics) >= expected_topic_count:
                                                    logger.debug(f"Source connector now producing to {len(current_topics)} topics")
                                                    break
                                        except Exception as poll_err:
                                            logger.debug(f"Poll error: {poll_err}")
                                    else:
                                        logger.debug("Timeout waiting for new topics, proceeding anyway")

                                    # Reconfigure sink connector to pick up new topics
                                    if replication_config.sink_connector_name:
                                        try:
                                            logger.debug(f"Reconfiguring sink connector to pick up new topics: {replication_config.sink_connector_name}")

                                            # Get target database
                                            target_db = ClientDatabase.objects.filter(
                                                client=client,
                                                is_target=True
                                            ).first()

                                            if target_db:
                                                # Generate updated sink config with topics.regex
                                                topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

                                                sink_config = get_sink_connector_config_for_database(
                                                    db_config=target_db,
                                                    topics=None,
                                                    delete_enabled=True,
                                                    custom_config={
                                                        'name': replication_config.sink_connector_name,
                                                        'topics.regex': topic_regex,
                                                    }
                                                )

                                                if sink_config:
                                                    connector_manager.update_connector_config(
                                                        replication_config.sink_connector_name,
                                                        sink_config
                                                    )
                                                    # CRITICAL: Restart sink connector to re-evaluate topics.regex
                                                    # Kafka Connect only discovers topics matching regex at startup
                                                    connector_manager.restart_connector(replication_config.sink_connector_name)
                                                    logger.info(f"✅ Sink connector reconfigured and restarted with topics.regex: {topic_regex}")
                                        except Exception as sink_err:
                                            logger.warning(f"Could not reconfigure sink connector: {sink_err}")

                                    messages.success(
                                        request,
                                        f"Added {len(tables_to_add)} tables using Kafka-based incremental snapshot (Signal ID: {signal_id})."
                                    )

                            else:
                                # Other DBs: Fall back to restart
                                logger.info(f"Database type {database.db_type} - falling back to connector restart")
                                raise Exception("Signals not fully supported for this database type")

                        except Exception as e:
                            logger.warning(f"Could not use signals, falling back to restart: {e}")

                            # Fallback: Restart connector
                            connector_manager = DebeziumConnectorManager()

                            all_tables = list(
                                replication_config.table_mappings.filter(is_enabled=True)
                                .values_list('source_table', flat=True)
                            )

                            # ✅ FIX: Create Kafka topics for new tables (was missing for non-MySQL)
                            logger.info("Creating Kafka topics for new tables...")
                            topic_manager = KafkaTopicManager()
                            topics_success, topics_message = topic_manager.create_topics_for_config(replication_config)

                            if not topics_success:
                                logger.warning(f"Topic creation warning: {topics_message}")
                            else:
                                logger.info(f"✅ Topics ready: {topics_message}")

                            # Update source connector config
                            source_config = get_connector_config_for_database(
                                db_config=database,
                                replication_config=replication_config,
                                tables_whitelist=all_tables
                            )

                            connector_manager.update_connector_config(replication_config.connector_name, source_config)
                            connector_manager.restart_connector(replication_config.connector_name)
                            logger.info(f"✅ Source connector updated and restarted")

                            # ✅ FIX: Update sink connector with new topics
                            if replication_config.sink_connector_name:
                                try:
                                    from client.views.cdc_views import get_kafka_topics_for_tables
                                    # Note: get_sink_connector_config_for_database is imported at top of file

                                    # Get all enabled table mappings
                                    table_mappings = replication_config.table_mappings.filter(is_enabled=True)

                                    # Generate topic names for sink
                                    kafka_topics = get_kafka_topics_for_tables(database, replication_config, table_mappings)

                                    # Get target database
                                    target_db = replication_config.target_database
                                    if target_db:
                                        # Build topic regex for sink
                                        topic_regex = f"client_{client.id}_db_\\d+\\.(?!signals$).*"

                                        sink_config = get_sink_connector_config_for_database(
                                            db_config=target_db,
                                            topics=None,
                                            custom_config={
                                                'name': replication_config.sink_connector_name,
                                                'topics.regex': topic_regex,
                                            }
                                        )

                                        if sink_config:
                                            connector_manager.update_connector_config(
                                                replication_config.sink_connector_name,
                                                sink_config
                                            )
                                            connector_manager.restart_connector(replication_config.sink_connector_name)
                                            logger.info(f"✅ Sink connector updated and restarted")

                                except Exception as sink_error:
                                    logger.warning(f"Could not update sink connector: {sink_error}")

                            messages.success(request, f"Added {len(tables_to_add)} tables (connector restarted)")

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

            # Always delete topics and target tables
            # Use orchestrator for proper cleanup
            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.delete_replication(delete_topics=True)

            if success:
                messages.success(
                    request,
                    f"Connector {replication_config.connector_name} deleted successfully "
                    "(Topics and target tables deleted)"
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




