"""
Connector CRUD Views
- AJAX table schema endpoint
- Add new source connector
- Create Debezium connectors (source + sink)
- Edit connector tables
- Delete connector
- Recreate source connector
"""

import logging
import re
from django.conf import settings
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.db import transaction
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ConnectorHistory
from client.utils.database_utils import get_table_schema, get_unassigned_tables
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.connector_templates import (
    generate_connector_name,
    get_connector_config_for_database,
)
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager

logger = logging.getLogger(__name__)

_VALID_TABLE_NAME_RE = re.compile(r'^[a-zA-Z0-9_]+$')


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
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                if not selected_tables:
                    msg = "Please select at least one table"
                    messages.error(request, msg)
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': msg}, status=400)
                    return redirect('connector_add', database_pk=database_pk)

                # Validate table count limit
                max_tables = settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25)
                if len(selected_tables) > max_tables:
                    msg = f"Maximum {max_tables} tables per connector allowed. You selected {len(selected_tables)}."
                    messages.error(request, msg)
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': msg}, status=400)
                    return redirect('connector_add', database_pk=database_pk)

                # Validate all selected tables are unassigned
                assigned_tables = set(unassigned_tables)
                invalid_tables = [t for t in selected_tables if t not in assigned_tables]
                if invalid_tables:
                    msg = f"Tables already assigned: {', '.join(invalid_tables)}"
                    messages.error(request, msg)
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': msg}, status=400)
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
                custom_name = re.sub(r'[^a-zA-Z0-9._\-]', '', request.POST.get('connector_name', '').strip())
                connector_name = custom_name if custom_name else generate_connector_name(client, database, version=next_version)
                replication_config.connector_name = connector_name
                # Topic prefix must match connector template (includes version for JMX uniqueness)
                replication_config.kafka_topic_prefix = f"client_{client.id}_db_{database.id}_v_{next_version}"
                replication_config.save()

                logger.info(f"Created ReplicationConfig v{next_version}: {replication_config.id}")

                # Create TableMappings for selected tables
                for table_name in selected_tables:
                    # Derive source_schema and actual_table_name without hitting the source DB.
                    # MS SQL / Oracle tables arrive as "schema.table" (e.g. "dbo.Customers").
                    # MySQL always belongs to database_name; PostgreSQL defaults to "public".
                    # This avoids one expensive DB round-trip per table and prevents Gunicorn
                    # timeouts when many tables are selected.
                    try:
                        if (database.db_type == 'mssql' or database.db_type == 'oracle') and '.' in table_name:
                            source_schema, actual_table_name = table_name.split('.', 1)
                        elif database.db_type == 'mysql':
                            source_schema = database.database_name
                            actual_table_name = table_name
                        elif database.db_type == 'postgresql' and '.' in table_name:
                            source_schema, actual_table_name = table_name.split('.', 1)
                        elif database.db_type == 'postgresql':
                            source_schema = 'public'
                            actual_table_name = table_name
                        else:
                            source_schema = ''
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
                        target_table_name = request.POST.get(f'target_table_{table_name}', default_target_table).strip()
                        if not _VALID_TABLE_NAME_RE.match(target_table_name or ''):
                            raise ValueError(
                                f"Invalid target table name '{target_table_name}' for table '{table_name}'. "
                                "Only letters, numbers and underscores are allowed."
                            )

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

                # AJAX modal flow: return JSON so the frontend can proceed to provision steps
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return JsonResponse({
                        'success': True,
                        'config_pk': replication_config.id,
                        'processing_mode': replication_config.processing_mode,
                        'table_count': len(selected_tables),
                        'batch_interval': replication_config.batch_interval,
                    })

                # Non-AJAX fallback: redirect through original provision view
                return redirect('connector_create_debezium', config_pk=replication_config.id)

        except Exception as e:
            logger.error(f"Error creating connector: {e}", exc_info=True)
            messages.error(request, f"Error creating connector: {str(e)}")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({'success': False, 'error': str(e)}, status=400)
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

    # Current tables — include target name so the template can display it
    current_table_mappings = list(
        replication_config.table_mappings.filter(is_enabled=True)
        .values('source_table', 'target_table')
        .order_by('source_table')
    )
    current_tables = [m['source_table'] for m in current_table_mappings]

    # Available tables — no schema calls at load time; row counts and columns are
    # fetched lazily via AJAX when the user expands the accordion (same as connector_add).
    try:
        unassigned_table_names = get_unassigned_tables(database.id)

        # Compute the default target table name for each unassigned table without
        # hitting the database (uses the same formula as the sink connector transform).
        db_type = database.db_type.lower()
        unassigned_tables = []
        for table_name in unassigned_table_names:
            if db_type == 'mysql':
                default_target = f"{database.database_name}_{table_name}"
            elif db_type == 'postgresql':
                default_target = f"public_{table_name}"
            elif db_type in ('mssql', 'sqlserver'):
                default_target = f"dbo_{table_name}"
            elif db_type == 'oracle':
                default_target = f"{database.username.upper()}_{table_name}"
            else:
                default_target = table_name
            unassigned_tables.append({'name': table_name, 'default_target': default_target})

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
                    if processing_mode == 'batch':
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

                # Collect any custom target table names the user entered in the form
                custom_target_names = {}
                for table_name in tables_to_add:
                    custom = request.POST.get(f'target_table_{table_name}', '').strip()
                    if custom:
                        if not _VALID_TABLE_NAME_RE.match(custom):
                            messages.error(
                                request,
                                f"Invalid target table name '{custom}' for table '{table_name}'. "
                                "Only letters, numbers and underscores are allowed."
                            )
                            return redirect('connector_edit_tables', config_pk=config_pk)
                        custom_target_names[table_name] = custom

                success, message = orchestrator.add_tables(
                    tables_to_add,
                    target_table_names=custom_target_names or None,
                )
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
        'current_table_mappings': current_table_mappings,
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
            drop_tables = bool(request.POST.get('drop_tables'))
            truncate_tables = bool(request.POST.get('truncate_tables'))

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.delete_replication(
                delete_topics=True,
                drop_tables=drop_tables,
                truncate_tables=truncate_tables,
            )

            if success:
                extras = ["topics deleted"]
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


