"""
CDC (Change Data Capture) workflow and replication management.

This module handles the complete CDC setup workflow:
1. Discover tables from source database
2. Configure tables (select columns, mappings)
3. Create Debezium connector
4. Monitor connector status
5. Manage replication lifecycle (start, stop, restart)

It also provides:
- AJAX endpoints for dynamic table schema fetching
- Configuration editing and updates
- Connector action handlers (pause, resume, delete)
"""

# import pprint

from client.utils.database_utils import get_table_list, get_table_schema
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Q
from sqlalchemy import text
import logging
import json
import time 

from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.kafka_topic_manager import KafkaTopicManager
from client.replication import ReplicationOrchestrator

from client.utils.connector_templates import (
    get_connector_config_for_database,
    generate_connector_name
)
from client.utils.database_utils import (
    get_table_list,
    get_table_schema,
    get_database_engine,
    check_binary_logging
)
from client.utils.offset_manager import delete_connector_offsets
from client.tasks import (
    create_debezium_connector,
    start_kafka_consumer,
    stop_kafka_consumer,
    delete_debezium_connector,
    restart_replication
)

logger = logging.getLogger(__name__)

def cdc_discover_tables(request, database_pk):
    """
    Discover all tables from source database
    Returns list of tables with metadata including totals
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    if request.method == "POST":
        # User selected tables to replicate
        selected_tables = request.POST.getlist('selected_tables')
        
        if not selected_tables:
            messages.error(request, 'Please select at least one table')
            return redirect('cdc_discover_tables', database_pk=database_pk)
        
        # Store in session and redirect to configuration
        request.session['selected_tables'] = selected_tables
        request.session['database_pk'] = database_pk
        
        return redirect('cdc_configure_tables', database_pk=database_pk)
    
    # GET: Discover tables
    try:
        # Check if binary logging is enabled (for MySQL)
        if db_config.db_type.lower() == 'mysql':
            is_enabled, log_format = check_binary_logging(db_config)
            if not is_enabled:
                messages.warning(
                    request, 
                    'Binary logging is not enabled on this MySQL database. CDC requires binary logging to be enabled.'
                )
        
        # Get list of tables
        tables = get_table_list(db_config)
        logger.info(f"Found {len(tables)} tables in database")
        
        # Get table details (row count, size estimates)
        tables_with_info = []
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            for table_name in tables:
                try:
                    # Get row count - use proper text() wrapper
                    if db_config.db_type.lower() == 'mysql':
                        count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                    elif db_config.db_type.lower() == 'postgresql':
                        count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
                    else:
                        count_query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")
                    
                    result = conn.execute(count_query)
                    row = result.fetchone()
                    row_count = row[0] if row else 0
                    
                    # Get table schema
                    schema = get_table_schema(db_config, table_name)
                    columns = schema.get('columns', [])
                    
                    # Check if table has timestamp column
                    has_timestamp = any(
                        'timestamp' in str(col.get('type', '')).lower() or 
                        'datetime' in str(col.get('type', '')).lower() or
                        str(col.get('name', '')).endswith('_at')
                        for col in columns
                    )
                    
                    # Generate prefixed target table name
                    source_db_name = db_config.database_name
                    target_table_name = f"{source_db_name}_{table_name}"

                    tables_with_info.append({
                        'name': table_name,
                        'target_name': target_table_name,
                        'row_count': row_count,
                        'column_count': len(columns),
                        'has_timestamp': has_timestamp,
                        'primary_keys': schema.get('primary_keys', []),
                    })
                    
                    logger.debug(f"Table {table_name}: {row_count} rows, {len(columns)} columns")
                    
                except Exception as e:
                    logger.error(f"Error getting info for table {table_name}: {str(e)}", exc_info=True)
                    source_db_name = db_config.database_name
                    target_table_name = f"{source_db_name}_{table_name}"
                    tables_with_info.append({
                        'name': table_name,
                        'target_name': target_table_name,
                        'row_count': 0,
                        'column_count': 0,
                        'has_timestamp': False,
                        'primary_keys': [],
                        'error': str(e)
                    })
        
        engine.dispose()
        
        # Calculate totals
        total_rows = sum(t['row_count'] for t in tables_with_info)
        total_cols = sum(t['column_count'] for t in tables_with_info)
        logger.info(f"Discovery complete: {len(tables_with_info)} tables, {total_rows} total rows, {total_cols} total columns")
        
        context = {
            'db_config': db_config,
            'client': db_config.client,
            'tables': tables_with_info,
            'total_tables': len(tables_with_info),
            'total_rows': total_rows,
            'total_columns': total_cols,
        }
        
        return render(request, 'client/cdc/discover_tables.html', context)
        
    except Exception as e:
        logger.error(f'Failed to discover tables: {str(e)}', exc_info=True)
        messages.error(request, f'Failed to discover tables: {str(e)}')
        return redirect('client_detail', pk=db_config.client.pk)

# ============================================
# 3. Configure Tables - Step 2
# ============================================
"""
File: client/views/cdc_views.py
Action: REPLACE your cdc_configure_tables function with this

Find this function in your file and REPLACE it entirely
"""

@require_http_methods(["GET", "POST"])
def cdc_configure_tables(request, database_pk):
    """
    Configure selected tables with column selection and mapping
    - Select which columns to replicate
    - Map column names (source -> target)
    - Map table names (source -> target)
    - Auto-create tables in target database
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    # Get selected tables from session
    selected_tables = request.session.get('selected_tables', [])
    if not selected_tables:
        messages.error(request, 'No tables selected')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    if request.method == "POST":
        try:
            # VALIDATE BEFORE CREATING - Prevent transaction rollbacks and ID skipping
            # Pre-validate that we can get schemas for all selected tables
            for table_name in selected_tables:
                try:
                    schema = get_table_schema(db_config, table_name)
                    if not schema.get('columns'):
                        raise ValueError(f"Table {table_name} has no columns or doesn't exist")
                except Exception as e:
                    logger.error(f"Pre-validation failed for table {table_name}: {e}")
                    messages.error(request, f'Cannot configure table {table_name}: {str(e)}')
                    return redirect('cdc_configure_tables', database_pk=database_pk)

            with transaction.atomic():
                # Create ReplicationConfig (only after validation passes)
                logger.info(f"drop_before_sync: {request.POST.get('drop_before_sync')}")
                replication_config = ReplicationConfig.objects.create(
                    client_database=db_config,
                    sync_type=request.POST.get('sync_type', 'realtime'),
                    sync_frequency=request.POST.get('sync_frequency', 'realtime'),
                    status='configured',
                    is_active=False,
                    auto_create_tables=request.POST.get('auto_create_tables') == 'on',
                    drop_before_sync=request.POST.get('drop_before_sync') == 'on',
                    created_by=request.user if request.user.is_authenticated else None
                )

                logger.info(f"Created ReplicationConfig: {replication_config.id}")
                
                # Create TableMappings for each table
                for table_name in selected_tables:
                    # Get table-specific settings
                    sync_type = request.POST.get(f'sync_type_{table_name}', '')
                    if not sync_type:  # Use global if not specified
                        sync_type = request.POST.get('sync_type', 'realtime')
                    
                    incremental_col = request.POST.get(f'incremental_col_{table_name}', '')
                    conflict_resolution = request.POST.get(f'conflict_resolution_{table_name}', 'source_wins')

                    # Get target table name (mapped name) - default to prefixed name
                    target_table_name = request.POST.get(f'target_table_name_{table_name}', '')
                    if not target_table_name:
                        # Auto-prefix with source database name to avoid collisions
                        source_db_name = db_config.database_name
                        target_table_name = f"{source_db_name}_{table_name}"
                    
                    # Create TableMapping
                    table_mapping = TableMapping.objects.create(
                        replication_config=replication_config,
                        source_table=table_name,
                        target_table=target_table_name,
                        is_enabled=True,
                        sync_type=sync_type,
                        incremental_column=incremental_col if sync_type == 'incremental' else None,
                        incremental_column_type='timestamp' if incremental_col else '',
                        conflict_resolution=conflict_resolution,
                    )
                    
                    logger.info(f"Created TableMapping: {table_name} -> {target_table_name}")
                    
                    # Get selected columns for this table
                    selected_columns = request.POST.getlist(f'selected_columns_{table_name}')
                    
                    if selected_columns:
                        # Create ColumnMapping for each selected column
                        for source_column in selected_columns:
                            # Get mapped target column name
                            target_column = request.POST.get(
                                f'column_mapping_{table_name}_{source_column}', 
                                source_column
                            )
                            
                            # Get column type from schema
                            try:
                                schema = get_table_schema(db_config, table_name)
                                columns = schema.get('columns', [])
                                column_info = next(
                                    (col for col in columns if col.get('name') == source_column), 
                                    None
                                )
                                
                                source_type = str(column_info.get('type', 'VARCHAR')) if column_info else 'VARCHAR'
                                
                                # Create ColumnMapping
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=source_column,
                                    target_column=target_column or source_column,
                                    source_type=source_type,
                                    target_type=source_type,  # Same type by default
                                    is_enabled=True,
                                )
                                
                                logger.debug(f"Created ColumnMapping: {source_column} -> {target_column}")
                                
                            except Exception as e:
                                logger.error(f"Error creating column mapping for {source_column}: {e}")
                    else:
                        # If no columns selected, include all columns by default
                        logger.warning(f"No columns selected for {table_name}, including all columns")
                        try:
                            schema = get_table_schema(db_config, table_name)
                            columns = schema.get('columns', [])
                            
                            for col in columns:
                                col_name = col.get('name')
                                col_type = str(col.get('type', 'VARCHAR'))
                                
                                # Get mapped name if provided
                                target_column = request.POST.get(
                                    f'column_mapping_{table_name}_{col_name}', 
                                    col_name
                                )
                                
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=col_name,
                                    target_column=target_column or col_name,
                                    source_type=col_type,
                                    target_type=col_type,
                                    is_enabled=True,
                                )
                        except Exception as e:
                            logger.error(f"Error creating default column mappings: {e}")
                
                # Auto-create tables in target database if option is checked
                if replication_config.auto_create_tables:
                    try:
                        from client.utils.table_creator import create_target_tables
                        create_target_tables(replication_config)
                        messages.success(request, 'Target tables created successfully!')
                    except Exception as e:
                        logger.error(f"Failed to create target tables: {e}")
                        messages.warning(request, f'Configuration saved, but failed to create target tables: {str(e)}')
                
                messages.success(request, 'CDC configuration saved successfully!')
                
                # Clear session
                request.session.pop('selected_tables', None)
                request.session.pop('database_pk', None)
                
                return redirect('cdc_create_connector', config_pk=replication_config.pk)
                
        except Exception as e:
            logger.error(f'Failed to save configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to save configuration: {str(e)}')
    
    # GET: Show configuration form
    tables_with_columns = []
    for table_name in selected_tables:
        try:
            schema = get_table_schema(db_config, table_name)
            columns = schema.get('columns', [])
            
            # Find potential incremental columns
            incremental_candidates = [
                col for col in columns
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time') or
                   (col.get('name', '') in ['id', 'created_at', 'updated_at'] and 
                    'int' in str(col.get('type', '')).lower())
            ]
            
            # Generate prefixed target table name
            source_db_name = db_config.database_name
            target_table_name = f"{source_db_name}_{table_name}"

            tables_with_columns.append({
                'name': table_name,
                'target_name': target_table_name,
                'columns': columns,
                'primary_keys': schema.get('primary_keys', []),
                'incremental_candidates': incremental_candidates,
            })
            
            logger.debug(f"Table {table_name}: {len(columns)} columns, {len(incremental_candidates)} incremental candidates")
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}", exc_info=True)
            messages.warning(request, f'Could not load schema for table {table_name}')
    
    context = {
        'db_config': db_config,
        'client': db_config.client,
        'tables': tables_with_columns,
    }
    
    return render(request, 'client/cdc/configure_tables.html', context)



@require_http_methods(["GET", "POST"])
def cdc_create_connector(request, config_pk):
    """
    Finalize replication configuration and optionally auto-start.

    With simplified flow:
    1. Saves configuration (connector name, topic prefix, tables)
    2. Optionally validates prerequisites
    3. Optionally auto-starts replication (all-in-one)
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    db_config = replication_config.client_database
    client = db_config.client

    if request.method == "POST":
        try:
            # Generate connector name
            connector_name = generate_connector_name(client, db_config)

            # Get list of tables to replicate
            table_mappings = replication_config.table_mappings.filter(is_enabled=True)
            tables_list = [tm.source_table for tm in table_mappings]

            if not tables_list:
                raise Exception("No tables selected for replication")

            logger.info(f"Debezium connector manager starts")
            # Verify Kafka Connect is healthy
            manager = DebeziumConnectorManager()
            is_healthy, health_error = manager.check_kafka_connect_health()
            if not is_healthy:
                raise Exception(f"Kafka Connect is not healthy: {health_error}")
    
            # Update replication config
            replication_config.connector_name = connector_name
            replication_config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}"
            replication_config.status = 'configured'
            replication_config.is_active = False
            replication_config.save()

            logger.info(f"Configuration saved for connector: {connector_name}")
            logger.info(f"Tables configured: {len(tables_list)}")

            # Check if user wants to auto-start
            auto_start = request.POST.get('auto_start', 'false').lower() == 'true'
            logger.info(f"Auto-starting replication for {connector_name}")
            if auto_start:
                # Auto-start replication using simplified flow
                logger.info(f"Auto-starting replication for {connector_name}")

                from client.replication import ReplicationOrchestrator
                orchestrator = ReplicationOrchestrator(replication_config)

                success, message = orchestrator.start_replication()

                if success:
                    messages.success(
                        request,
                        f'✅ Replication started successfully! '
                        f'{len(tables_list)} tables are now being replicated in real-time.'
                    )
                else:
                    messages.warning(request, f'⚠️ Configuration saved, but failed to start: {message}')
            else:
                # Just save configuration
                messages.success(
                    request,
                    f'✅ Replication configured successfully! '
                    f'{len(tables_list)} tables ready. '
                    f'Click "Start Replication" to begin.'
                )

            return redirect('cdc_monitor_connector', config_pk=replication_config.pk)

        except Exception as e:
            logger.error(f"Failed to configure replication: {e}", exc_info=True)
            messages.error(request, f'Failed to configure replication: {str(e)}')

    # GET: Show connector creation confirmation
    table_mappings = replication_config.table_mappings.filter(is_enabled=True)

    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'table_mappings': table_mappings,
    }

    return render(request, 'client/cdc/create_connector.html', context)



# ============================================
# 5. Monitor Connector Status
# ============================================
@require_http_methods(["GET"])
def cdc_monitor_connector(request, config_pk):
    """
    Real-time monitoring of connector status (NEW: Uses orchestrator)
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    if not replication_config.connector_name:
        messages.error(request, 'No connector configured for this replication')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)

    try:
        # Use orchestrator for unified status
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(replication_config)
        unified_status = orchestrator.get_unified_status()

        # Check if connector actually exists in Kafka Connect
        connector_exists = unified_status['connector']['state'] not in ['NOT_FOUND', 'ERROR']

        # Determine what actions are available based on state
        can_start = (
            replication_config.status == 'configured' and
            not replication_config.is_active
        )

        can_stop = (
            replication_config.is_active and
            connector_exists
        )

        can_restart = (
            replication_config.is_active and
            connector_exists
        )

        context = {
            'replication_config': replication_config,
            'client': replication_config.client_database.client,
            'unified_status': unified_status,
            'connector_exists': connector_exists,
            'can_start': can_start,
            'can_stop': can_stop,
            'can_restart': can_restart,
            'enabled_table_mappings': replication_config.table_mappings.filter(is_enabled=True),
            # Legacy fields for backward compatibility with template
            'connector_status': {
                'connector': {'state': unified_status['connector']['state']},
                'tasks': unified_status['connector'].get('tasks', [])
            } if connector_exists else None,
            'tasks': unified_status['connector'].get('tasks', []),
            'exists': connector_exists,
        }

        return render(request, 'client/cdc/monitor_connector.html', context)

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        messages.error(request, f'Failed to get connector status: {str(e)}')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)


# ============================================
# 6. AJAX: Get Table Schema
# ============================================
@require_http_methods(["GET"])
def ajax_get_table_schema(request, database_pk, table_name):
    """
    AJAX endpoint to get table schema details
    """
    try:
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        schema = get_table_schema(db_config, table_name)
        
        return JsonResponse({
            'success': True,
            'schema': schema
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)



@require_http_methods(["POST"])
def cdc_connector_action(request, config_pk, action):
    """
    Handle connector actions: start, pause, resume, restart, delete
    """
    from client.replication import ReplicationOrchestrator
    
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    connector_name = replication_config.connector_name
    
    if not connector_name and action not in ['start', 'delete']:
        return JsonResponse({
            'success': False,
            'error': 'No connector configured'
        }, status=400)
        
    try:
        
        if action == 'start':
            # Use orchestrator to start replication
            logger.info(f"🚀 Starting replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)

            # Start replication with new simplified flow
            success, message = orchestrator.start_replication()

            if success:
                # Update client replication status
                client = replication_config.client_database.client
                client.replication_enabled = True
                client.replication_status = 'active'
                client.save()

                error = None
            else:
                error = message
                message = None
            
        elif action == 'pause':
            # Use orchestrator to stop replication
            logger.info(f"⏸️ Pausing replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.stop_replication()

            if not success:
                error = message
                message = None
            else:
                error = None
            
        elif action == 'resume':
            # Use orchestrator to resume replication
            logger.info(f"▶️ Resuming replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.start_replication()

            if not success:
                error = message
                message = None
            else:
                error = None
            
        elif action == 'restart':
            # Use orchestrator to restart replication
            logger.info(f"🔄 Restarting replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.restart_replication()

            if not success:
                error = message
                message = None
            else:
                error = None
            
        elif action == 'delete':
            # Use orchestrator to delete replication
            logger.info(f"🗑️ Deleting replication for config {config_pk} via orchestrator")

            # Store client ID before deletion (for redirect)
            client_id = replication_config.client_database.client.id

            # Create orchestrator BEFORE deletion
            orchestrator = ReplicationOrchestrator(replication_config)

            # This will delete the config and all related records
            success, message = orchestrator.delete_replication(delete_topics=False)

            logger.info(f"Delete result: {success} - {message}")

            if not success:
                error = message
                message = None
            else:
                error = None
                # Note: replication_config no longer exists in DB after this point
                # Return redirect URL so frontend knows where to go
                from django.urls import reverse
                messages.success(request, message)
                return JsonResponse({
                    'success': True,
                    'message': message,
                    'redirect_url': reverse('replications_list')
                })

        elif action == 'force_resnapshot':
            # Force resnapshot by clearing offsets and recreating connector
            logger.info(f"🔄 Forcing resnapshot for config {config_pk}")

            from client.tasks import force_resnapshot as force_resnapshot_task

            # Run async task
            result = force_resnapshot_task.apply_async(args=[config_pk])

            success = True
            message = 'Resnapshot initiated. This will delete and recreate the connector with a fresh snapshot of all data.'
            error = None

        else:
            return JsonResponse({
                'success': False,
                'error': f'Unknown action: {action}'
            }, status=400)
        
        if success:
            # Only save if config still exists (not deleted)
            if action != 'delete':
                replication_config.save()
            
            messages.success(request, message)
            return JsonResponse({'success': True, 'message': message})
        else:
            return JsonResponse({
                'success': False,
                'error': error
            }, status=400)
            
    except Exception as e:
        logger.error(f"❌ Error performing action {action}: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
def start_replication(request, config_id):
    """Start CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Check if already running
        if config.is_active and config.status == 'active':
            messages.warning(request, "Replication is already running!")
            return redirect('cdc_config_detail', config_id=config_id)

        # Use orchestrator for simplified flow (Option A)
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        # Get force_resync parameter from request (default: False)
        force_resync = request.POST.get('force_resync', 'false').lower() == 'true'

        logger.info(f"Starting replication for config {config_id} (force_resync={force_resync})")

        # Start replication with new simplified flow
        # This will:
        # 1. Validate prerequisites
        # 2. Perform initial SQL copy
        # 3. Start Debezium connector (CDC-only mode)
        # 4. Start consumer with fresh group ID
        success, message = orchestrator.start_replication(force_resync=force_resync)

        if success:
            messages.success(
                request,
                f"🚀 Replication started successfully! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to start replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to start replication: {e}", exc_info=True)
        messages.error(request, f"Failed to start replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def stop_replication(request, config_id):
    """Stop CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        if not config.is_active:
            messages.warning(request, "Replication is not running!")
            return redirect('cdc_config_detail', config_id=config_id)

        # Use orchestrator to stop replication
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"Stopping replication for config {config_id}")

        success, message = orchestrator.stop_replication()

        if success:
            messages.success(
                request,
                f"⏸️ Replication stopped! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to stop replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to stop replication: {e}", exc_info=True)
        messages.error(request, f"Failed to stop replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def restart_replication_view(request, config_id):
    """Restart CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator to restart replication
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"Restarting replication for config {config_id}")

        success, message = orchestrator.restart_replication()

        if success:
            messages.success(
                request,
                f"🔄 Replication restarted! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to restart replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to restart replication: {e}", exc_info=True)
        messages.error(request, f"Failed to restart replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["GET"])
def replication_status(request, config_id):
    """Get replication status (AJAX endpoint) - NEW: Uses orchestrator"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator for unified status
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)
        unified_status = orchestrator.get_unified_status()

        # Return comprehensive status
        return JsonResponse({
            'success': True,
            'status': unified_status
        })

    except Exception as e:
        logger.error(f"Failed to get replication status: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)



# def wait_for_connector_running(manager, connector_name, timeout=30, interval=2):
#     end_time = time.time() + timeout
#     while time.time() < end_time:
#         status, details = manager.get_connector_status(connector_name)
#         if status:
#             if all(task['state'] == 'RUNNING' for task in details.get('tasks', [])):
#                 logger.info("✅ All connector tasks are RUNNING.")
#                 return True
#         time.sleep(interval)
#         logger.info(f"⏳ Waiting for connector tasks to stabilize. Current status: {status}")
#     logger.error(f"❌ Connector tasks did not stabilize within {timeout} seconds.")
#     return False


# @require_http_methods(["GET", "POST"])
# def cdc_edit_config(request, config_pk):
#     """
#     Edit replication configuration
#     - Enable/disable tables
#     - Rename target tables
#     - Enable/disable columns
#     - Change sync settings
#     """
#     replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
#     db_config = replication_config.client_database
#     client = db_config.client

#     if request.method == "POST":
#         try:
#             with transaction.atomic():
#                 # Update basic configuration
#                 replication_config.sync_type = request.POST.get('sync_type', replication_config.sync_type)
#                 replication_config.sync_frequency = request.POST.get('sync_frequency', replication_config.sync_frequency)
#                 replication_config.auto_create_tables = request.POST.get('auto_create_tables') == 'on'
#                 replication_config.drop_before_sync = request.POST.get('drop_before_sync') == 'on'
#                 replication_config.save()

#                 # Get selected tables from form
#                 selected_table_names = request.POST.getlist('selected_tables')
#                 logger.info(f"Selected tables: {selected_table_names}")

#                 # Get existing mappings
#                 existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}
#                 newly_added_tables = []
#                 removed_table_names = []

#                 # Track tables being removed (BEFORE deletion for cleanup)
#                 for table_name in existing_mappings.keys():
#                     if table_name not in selected_table_names:
#                         removed_table_names.append(table_name)

#                 if removed_table_names:
#                     logger.info(f"🗑️ Tables to remove: {removed_table_names}")

#                 # Process newly added tables
#                 for table_name in selected_table_names:
#                     if table_name not in existing_mappings:
#                         newly_added_tables.append(table_name)
#                         logger.info(f"➕ New table to add: {table_name}")
                        
#                         # Create new table mapping for newly added table
#                         source_db_name = db_config.database_name
#                         target_table_name = f"{source_db_name}_{table_name}"

#                         table_mapping = TableMapping.objects.create(
#                             replication_config=replication_config,
#                             source_table=table_name,
#                             target_table=target_table_name,
#                             is_enabled=True,
#                             sync_type='realtime',
#                             conflict_resolution='source_wins'
#                         )

#                         # Create column mappings for new table
#                         try:
#                             schema = get_table_schema(db_config, table_name)
#                             for column in schema.get('columns', []):
#                                 ColumnMapping.objects.create(
#                                     table_mapping=table_mapping,
#                                     source_column=column['name'],
#                                     target_column=column['name'],
#                                     source_type=str(column.get('type', 'unknown')),
#                                     target_type=str(column.get('type', 'unknown')),
#                                     is_enabled=True
#                                 )
#                         except Exception as e:
#                             logger.error(f"Failed to create column mappings for {table_name}: {e}")

#                         logger.info(f"✅ Created new table mapping for {table_name}")

#                 # ========================================================================
#                 # CRITICAL: CREATE TARGET TABLES FOR NEWLY ADDED TABLES
#                 # ========================================================================
#                 if newly_added_tables and replication_config.auto_create_tables:
#                     try:
#                         from client.utils.table_creator import create_target_tables
#                         logger.info(f"🔨 Creating target tables for {len(newly_added_tables)} newly added tables...")
#                         logger.info(f"   Tables: {newly_added_tables}")

#                         # Create only the specific new tables
#                         create_target_tables(replication_config, specific_tables=newly_added_tables)

#                         messages.success(request, f'✅ Successfully created {len(newly_added_tables)} new target tables!')
#                     except Exception as e:
#                         logger.error(f"❌ Failed to create target tables: {e}", exc_info=True)
#                         messages.warning(request, f'⚠️ Configuration saved, but failed to create target tables: {str(e)}')

#                 # ========================================================================
#                 # CRITICAL: CREATE KAFKA TOPICS FOR NEWLY ADDED TABLES
#                 # ========================================================================
#                 if newly_added_tables:
#                     try:
#                         from client.utils.kafka_topic_manager import KafkaTopicManager
#                         logger.info(f"📡 Creating Kafka topics for {len(newly_added_tables)} newly added tables...")
#                         logger.info(f"   New tables: {newly_added_tables}")

#                         topic_manager = KafkaTopicManager()

#                         # Use saved topic prefix or derive from client and database IDs
#                         topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"

#                         # Create signal topic (needed for incremental snapshots)
#                         signal_topic_success, signal_topic_error = topic_manager.create_signal_topic(topic_prefix)
#                         if signal_topic_success:
#                             logger.info(f"✅ Signal topic ready: {topic_prefix}.signals")
#                         else:
#                             logger.warning(f"⚠️ Signal topic issue: {signal_topic_error}")

#                         # Create topics for new tables
#                         topic_results = topic_manager.create_cdc_topics_for_tables(
#                             server_name=topic_prefix,
#                             database=db_config.database_name,
#                             table_names=newly_added_tables
#                         )

#                         # Verify all new topics exist
#                         topics_missing = []
#                         for table in newly_added_tables:
#                             topic_name = f"{topic_prefix}.{db_config.database_name}.{table}"
#                             if not topic_manager.topic_exists(topic_name):
#                                 topics_missing.append(topic_name)

#                         # If any topics are missing, log warning but continue
#                         if topics_missing:
#                             error_msg = f"Failed to create Kafka topics: {', '.join(topics_missing)}"
#                             logger.error(f"❌ {error_msg}")
#                             messages.warning(request, f'⚠️ {error_msg}. Replication may not work for new tables.')
#                         else:
#                             # Log success
#                             successful_topics = [t for t, success in topic_results.items() if success]
#                             if successful_topics:
#                                 logger.info(f"✅ Created {len(successful_topics)} Kafka topics for newly added tables")
#                             elif newly_added_tables:
#                                 logger.info(f"ℹ️  {len(newly_added_tables)} topics already existed (reusing existing topics)")

#                     except Exception as e:
#                         logger.error(f"❌ Error creating Kafka topics: {e}", exc_info=True)
#                         messages.warning(request, f'⚠️ Failed to create Kafka topics for new tables: {str(e)}')

#                 # Process removed tables (tables that were mapped but not selected)
#                 for table_name in removed_table_names:
#                     table_mapping = existing_mappings[table_name]
#                     logger.info(f"🗑️ Deleting table mapping for {table_name}")
#                     table_mapping.delete()
    
#                 # ========================================================================
#                 # CRITICAL: DROP TARGET TABLES FOR REMOVED TABLES (if drop_before_sync enabled)
#                 # ========================================================================
#                 if removed_table_names:
#                     try:
#                         from client.utils.table_creator import drop_target_tables
#                         logger.info(f"🗑️ Attempting to drop {len(removed_table_names)} removed tables...")
#                         logger.info(f"   Tables: {removed_table_names}")
#                         logger.info(f"   drop_before_sync setting: {replication_config.drop_before_sync}")
                        
#                         # This will only drop if drop_before_sync=True (safety check inside function)
#                         drop_target_tables(replication_config, removed_table_names)
                        
#                         if replication_config.drop_before_sync:
#                             messages.success(request, f'✅ Dropped {len(removed_table_names)} removed tables from target database!')
#                         else:
#                             messages.info(request, f'ℹ️ {len(removed_table_names)} tables removed from replication (not dropped from target - drop_before_sync is disabled)')
#                     except Exception as e:
#                         logger.error(f"❌ Failed to drop target tables: {e}", exc_info=True)
#                         messages.warning(request, f'⚠️ Configuration saved, but failed to drop target tables: {str(e)}')

#                 # Update existing table mappings
#                 for table_mapping in replication_config.table_mappings.all():
#                     table_key = f'table_{table_mapping.id}'

#                     # Update target table name
#                     new_target_name = request.POST.get(f'target_{table_key}')
#                     if new_target_name:
#                         table_mapping.target_table = new_target_name

#                     # Update sync type
#                     table_sync_type = request.POST.get(f'sync_type_{table_key}')
#                     if table_sync_type:
#                         table_mapping.sync_type = table_sync_type

#                     # Update incremental column
#                     incremental_col = request.POST.get(f'incremental_col_{table_key}')
#                     if incremental_col:
#                         table_mapping.incremental_column = incremental_col

#                     # Update conflict resolution
#                     conflict_res = request.POST.get(f'conflict_resolution_{table_key}')
#                     if conflict_res:
#                         table_mapping.conflict_resolution = conflict_res

#                     table_mapping.save()

#                     # Update column mappings
#                     for column_mapping in table_mapping.column_mappings.all():
#                         column_key = f'column_{column_mapping.id}'

#                         # Enable/disable column
#                         column_mapping.is_enabled = request.POST.get(f'enabled_{column_key}') == 'on'

#                         # Update target column name
#                         new_target_col = request.POST.get(f'target_{column_key}')
#                         if new_target_col:
#                             column_mapping.target_column = new_target_col

#                         column_mapping.save()

#                 # Check if restart connector checkbox is checked
#                 restart_connector = request.POST.get('restart_connector') == 'on'

#                 messages.success(request, '✅ Configuration updated successfully!')

#                 # ========================================================================
#                 # CONNECTOR UPDATE: Update connector with new table list
#                 # ========================================================================
#                 if replication_config.connector_name:
#                     try:
#                         manager = DebeziumConnectorManager()

#                         # Get ALL enabled tables (not just newly added ones)
#                         all_enabled_tables = replication_config.table_mappings.filter(is_enabled=True)
#                         tables_list = [tm.source_table for tm in all_enabled_tables]

#                         if tables_list:
#                             # Check if connector actually exists in Kafka Connect
#                             connector_result = manager.get_connector_status(replication_config.connector_name)
#                             connector_exists = isinstance(connector_result, tuple) and connector_result[0]

#                             if not connector_exists:
#                                 logger.warning(f"⚠️ Connector {replication_config.connector_name} doesn't exist in Kafka Connect")
#                                 logger.info(f"   Clearing connector_name from config. User will need to recreate connector.")
#                                 # Clear the connector name since it doesn't exist
#                                 replication_config.connector_name = None
#                                 replication_config.status = 'configured'
#                                 replication_config.is_active = False
#                                 replication_config.save()
#                             else:
#                                 # ========================================================================
#                                 # IMPROVED APPROACH: Update connector config instead of delete+recreate
#                                 # Benefits:
#                                 # - Non-disruptive (existing tables keep replicating)
#                                 # - Preserves offsets (no data loss/duplication)
#                                 # - Faster (no connector restart delay)
#                                 # - Simpler (fewer failure points)
#                                 # ========================================================================
#                                 if newly_added_tables or removed_table_names:
#                                     import time
#                                     logger.info(f"📝 Updating connector config (added: {len(newly_added_tables)}, removed: {len(removed_table_names)})")
#                                     logger.info(f"   Added tables: {newly_added_tables}")
#                                     logger.info(f"   Removed tables: {removed_table_names}")

#                                     try:
#                                         # Get current connector configuration
#                                         current_config = manager.get_connector_config(replication_config.connector_name)

#                                         if not current_config:
#                                             raise Exception("Failed to retrieve current connector configuration")

#                                         # Update table.include.list with new table list
#                                         tables_full = [f"{db_config.database_name}.{table}" for table in tables_list]
#                                         current_config['table.include.list'] = ','.join(tables_full)

#                                         # CRITICAL: Ensure snapshot mode supports incremental snapshots
#                                         # 'initial' mode only snapshots on first start, then ignores signals
#                                         # 'when_needed' mode allows both automatic and incremental snapshots
#                                         if newly_added_tables and current_config.get('snapshot.mode') == 'initial':
#                                             logger.info(f"⚙️  Changing snapshot.mode from 'initial' to 'when_needed' to support incremental snapshots")
#                                             current_config['snapshot.mode'] = 'when_needed'

#                                         logger.info(f"📋 Updated table.include.list: {current_config['table.include.list']}")
#                                         logger.info(f"📋 Snapshot mode: {current_config.get('snapshot.mode', 'not set')}")

#                                         # Update connector configuration (live update, no restart needed!)
#                                         update_success, update_error = manager.update_connector_config(
#                                             replication_config.connector_name,
#                                             current_config
#                                         )

#                                         if not update_success:
#                                             raise Exception(f"Failed to update connector config: {update_error}")

#                                         logger.info(f"✅ Connector config updated successfully")

#                                         # ========================================================================
#                                         # CRITICAL: Restart connector tasks to pick up new table list
#                                         # Config updates are not applied to tasks until they restart
#                                         # ========================================================================
#                                         logger.info(f"🔄 Restarting connector tasks to apply new table list...")
#                                         restart_success, restart_error = manager.restart_connector(replication_config.connector_name)

#                                         if not restart_success:
#                                             logger.warning(f"⚠️ Failed to restart connector: {restart_error}")
#                                             raise Exception(f"Failed to restart connector after config update: {restart_error}")

#                                         logger.info(f"✅ Connector tasks restarted successfully")

#                                         # ========================================================================
#                                         # For newly added tables: Send incremental snapshot signal
#                                         # ========================================================================
#                                         if newly_added_tables:
#                                             logger.info(f"📡 Preparing incremental snapshot for {len(newly_added_tables)} new table(s)...")

#                                             # Wait for connector tasks to fully restart and start monitoring new tables
#                                             # Tasks need time to:
#                                             # 1. Restart and reload configuration
#                                             # 2. Connect to database and start monitoring new tables
#                                             # 3. Enter STREAMING phase
#                                             # 4. Start consuming signal topic (happens AFTER streaming starts)
#                                             # IMPORTANT: Signal topic consumer initialization can take 10-20 seconds
#                                             wait_success = wait_for_connector_running(manager, replication_config.connector_name)
#                                             if not wait_success:
#                                                 raise Exception(f"Connector {replication_config.connector_name} did not become RUNNING after restart.")
#                                             logger.info(f"✅ Connector verified as RUNNING")

#                                             # Verify connector is still running
#                                             connector_status = manager.get_connector_status(replication_config.connector_name)
#                                             connector_running = isinstance(connector_status, tuple) and connector_status[0]

#                                             if not connector_running:
#                                                 raise Exception(f"Connector {replication_config.connector_name} is not running")

#                                             logger.info(f"✅ Connector verified as RUNNING")

#                                             try:
#                                                 from client.utils.kafka_signal_sender import KafkaSignalSender
#                                                 from client.utils.kafka_topic_manager import KafkaTopicManager

#                                                 # Ensure signal topic exists
#                                                 topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"
#                                                 topic_manager = KafkaTopicManager()
#                                                 signal_topic_success, signal_topic_error = topic_manager.create_signal_topic(topic_prefix)

#                                                 if signal_topic_success:
#                                                     logger.info(f"✅ Signal topic ready: {topic_prefix}.signals")
#                                                 else:
#                                                     logger.warning(f"⚠️ Signal topic issue: {signal_topic_error}")

#                                                 # Send incremental snapshot signal for new tables ONLY
#                                                 signal_sender = KafkaSignalSender()
#                                                 logger.info(f"📡 Sending incremental snapshot signal for: {newly_added_tables}")

#                                                 success, message = signal_sender.send_incremental_snapshot_signal(
#                                                     topic_prefix=topic_prefix,
#                                                     database_name=db_config.database_name,
#                                                     table_names=newly_added_tables
#                                                 )

#                                                 signal_sender.close()

#                                                 if success:
#                                                     logger.info(f"✅ {message}")
#                                                     logger.info(f"⏳ Snapshot may take 1-5 minutes depending on table size")
#                                                     messages.success(
#                                                         request,
#                                                         f'✅ {len(newly_added_tables)} table(s) added! '
#                                                         f'Incremental snapshot in progress. Data will replicate within 1-5 minutes.'
#                                                     )
#                                                 else:
#                                                     logger.error(f"❌ Failed to send snapshot signal: {message}")
#                                                     messages.warning(
#                                                         request,
#                                                         f'⚠️ Tables added but snapshot signal failed: {message}. '
#                                                         f'New tables will only capture future changes (no initial data).'
#                                                     )

#                                             except Exception as e:
#                                                 logger.error(f"❌ Error sending incremental snapshot signal: {e}", exc_info=True)
#                                                 messages.warning(
#                                                     request,
#                                                     f'⚠️ Tables added but snapshot failed: {str(e)}. '
#                                                     f'New tables will only capture future changes.'
#                                                 )

#                                             # ========================================================================
#                                             # Restart consumer to subscribe to new table topics
#                                             # ========================================================================
#                                             if replication_config.is_active:
#                                                 try:
#                                                     logger.info(f"🔄 Restarting consumer to subscribe to {len(newly_added_tables)} new topic(s)...")

#                                                     # Stop current consumer
#                                                     stop_result = stop_kafka_consumer(replication_config.id)
#                                                     if stop_result.get('success'):
#                                                         logger.info(f"✅ Consumer stopped")
#                                                     else:
#                                                         logger.warning(f"⚠️ Consumer stop returned: {stop_result}")

#                                                     # Wait for consumer to fully stop
#                                                     time.sleep(2)

#                                                     # Re-activate the config
#                                                     replication_config.refresh_from_db()
#                                                     replication_config.is_active = True
#                                                     replication_config.status = 'active'
#                                                     replication_config.save()
#                                                     logger.info(f"✅ Config re-activated")

#                                                     # Restart consumer with updated topic list
#                                                     logger.info(f"🚀 Starting consumer with updated topic list...")
#                                                     start_kafka_consumer.apply_async(
#                                                         args=[replication_config.id],
#                                                         countdown=3
#                                                     )
#                                                     logger.info(f"✅ Consumer restart scheduled")

#                                                 except Exception as e:
#                                                     logger.error(f"❌ Error restarting consumer: {e}", exc_info=True)
#                                                     messages.warning(
#                                                         request,
#                                                         f'⚠️ Config updated but consumer restart failed. '
#                                                         f'Please restart replication manually.'
#                                                     )

#                                         # ========================================================================
#                                         # For removed tables: Just log (connector already stopped tracking them)
#                                         # ========================================================================
#                                         if removed_table_names:
#                                             logger.info(f"✅ Removed {len(removed_table_names)} table(s) from connector")
#                                             messages.success(
#                                                 request,
#                                                 f'✅ {len(removed_table_names)} table(s) removed from replication'
#                                             )

#                                         # Success message if both operations happened
#                                         if newly_added_tables and removed_table_names:
#                                             messages.success(
#                                                 request,
#                                                 f'✅ Connector updated: +{len(newly_added_tables)} table(s), '
#                                                 f'-{len(removed_table_names)} table(s)'
#                                             )

#                                     except Exception as e:
#                                         logger.error(f"❌ Error updating connector: {e}", exc_info=True)
#                                         messages.error(
#                                             request,
#                                             f'❌ Failed to update connector: {str(e)}. '
#                                             f'Configuration saved in database but connector not updated.'
#                                         )
#                                 elif restart_connector:
#                                     # No table changes, just restart if requested
#                                     logger.info(f"🔄 Restarting connector to apply changes...")
#                                     success, error = manager.restart_connector(replication_config.connector_name)
#                                     if not success:
#                                         logger.warning(f"⚠️ Failed to restart connector: {error}")
#                                         messages.warning(request, f'⚠️ Failed to restart connector: {error}')
#                                     else:
#                                         logger.info(f"✅ Connector restarted successfully")
#                                         messages.success(request, '✅ Connector restarted successfully!')

#                     except Exception as e:
#                         logger.error(f"❌ Error updating connector: {e}", exc_info=True)
#                         messages.warning(request, f'⚠️ Configuration saved, but connector update failed: {str(e)}')

#                 # Redirect to monitor page
#                 if replication_config.connector_name:
#                     return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
#                 else:
#                     return redirect('main-dashboard')

#         except Exception as e:
#             logger.error(f'Failed to update configuration: {e}', exc_info=True)
#             messages.error(request, f'Failed to update configuration: {str(e)}')

#     # GET: Show edit form
#     # Discover ALL tables from the database (not just mapped ones)
#     try:
#         all_table_names = get_table_list(db_config)
#         logger.info(f"Discovered {len(all_table_names)} tables from database")
#     except Exception as e:
#         logger.error(f"Failed to discover tables: {e}")
#         messages.error(request, f"Failed to discover tables from database: {str(e)}")
#         all_table_names = []

#     # Build a map of existing table mappings
#     existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}

#     # Combine discovered tables with existing mappings
#     tables_with_columns = []
#     for table_name in all_table_names:
#         existing_mapping = existing_mappings.get(table_name)

#         try:
#             # Get schema from source database
#             schema = get_table_schema(db_config, table_name)
#             columns_from_schema = schema.get('columns', [])

#             # Find potential incremental columns
#             incremental_candidates = [
#                 col for col in columns_from_schema
#                 if 'timestamp' in str(col.get('type', '')).lower() or
#                    'datetime' in str(col.get('type', '')).lower() or
#                    col.get('name', '').endswith('_at') or
#                    col.get('name', '').endswith('_time') or
#                    (col.get('name', '') in ['id', 'created_at', 'updated_at'] and
#                     'int' in str(col.get('type', '')).lower())
#             ]

#             # Get row count
#             try:
#                 engine = get_database_engine(db_config)
#                 with engine.connect() as conn:
#                     if db_config.db_type.lower() == 'mysql':
#                         count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
#                     elif db_config.db_type.lower() == 'postgresql':
#                         count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
#                     else:
#                         count_query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")

#                     result = conn.execute(count_query)
#                     row = result.fetchone()
#                     row_count = row[0] if row else 0
#                 engine.dispose()
#             except Exception as e:
#                 logger.debug(f"Could not get row count for {table_name}: {e}")
#                 row_count = 0

#             # Generate prefixed target table name for unmapped tables
#             source_db_name = db_config.database_name
#             default_target_table_name = f"{source_db_name}_{table_name}"

#             table_data = {
#                 'table_name': table_name,
#                 'row_count': row_count,
#                 'column_count': len(columns_from_schema),
#                 'is_mapped': existing_mapping is not None,
#                 'mapping': existing_mapping,
#                 'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
#                 'all_columns': columns_from_schema,  # For unmapped tables to show in frontend
#                 'incremental_candidates': incremental_candidates,
#                 'primary_keys': schema.get('primary_keys', []),
#                 'default_target_name': default_target_table_name,  # For unmapped tables
#             }

#             tables_with_columns.append(table_data)

#         except Exception as e:
#             logger.error(f"Error getting schema for {table_name}: {e}")

#             # Generate prefixed target table name even in error case
#             source_db_name = db_config.database_name
#             default_target_table_name = f"{source_db_name}_{table_name}"

#             table_data = {
#                 'table_name': table_name,
#                 'row_count': 0,
#                 'column_count': 0,
#                 'is_mapped': existing_mapping is not None,
#                 'mapping': existing_mapping,
#                 'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
#                 'all_columns': [],  # Empty for errors
#                 'incremental_candidates': [],
#                 'primary_keys': [],
#                 'default_target_name': default_target_table_name,
#             }
#             tables_with_columns.append(table_data)

#     # Sort: mapped tables first, then unmapped
#     tables_with_columns.sort(key=lambda t: (not t['is_mapped'], t['table_name']))

#     context = {
#         'replication_config': replication_config,
#         'db_config': db_config,
#         'client': client,
#         'tables_with_columns': tables_with_columns,
#     }

#     return render(request, 'client/cdc/edit_config.html', context)



@require_http_methods(["POST"])
def cdc_delete_config(request, config_pk):
    """
    Delete replication configuration (for incomplete setups or when no longer needed)

    Now uses ReplicationOrchestrator for proper cleanup:
    - Stops consumer tasks
    - Pauses and deletes connector
    - Clears offsets
    - Optionally deletes Kafka topics
    - Deletes database configuration
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        logger.info(f"Deleting replication config {config_pk} (connector: {replication_config.connector_name})")

        # Check if user wants to delete topics (from query parameter)
        delete_topics = request.GET.get('delete_topics', 'false').lower() == 'true'

        # Use orchestrator for proper cleanup
        orchestrator = ReplicationOrchestrator(replication_config)
        success, message = orchestrator.delete_replication(delete_topics=delete_topics)

        if success:
            logger.info(f"✅ Successfully deleted replication config {config_pk}")

            from django.urls import reverse
            return JsonResponse({
                'success': True,
                'message': message,
                'redirect_url': reverse('replications_list')
            })
        else:
            logger.error(f"❌ Failed to delete replication config: {message}")
            return JsonResponse({
                'success': False,
                'error': message
            }, status=500)

    except Exception as e:
        logger.error(f"❌ Failed to delete configuration: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def cdc_config_details(request, config_pk):
    """
    AJAX endpoint: Get detailed configuration for editing in modal
    """
    try:
        replication_config = get_object_or_404(
            ReplicationConfig.objects.select_related('client_database', 'client_database__client')
            .prefetch_related('table_mappings__column_mappings'),
            pk=config_pk
        )

        client = replication_config.client_database.client
        db_config = replication_config.client_database

        # Build tables array with columns
        tables_data = []
        for table_mapping in replication_config.table_mappings.all():
            columns_data = []
            for col_mapping in table_mapping.column_mappings.all():
                columns_data.append({
                    'source_column': col_mapping.source_column,
                    'target_column': col_mapping.target_column,
                    'data_type': col_mapping.source_type or col_mapping.source_data_type or '',
                    'is_enabled': col_mapping.is_enabled,
                })

            tables_data.append({
                'id': table_mapping.id,
                'source_table': table_mapping.source_table,
                'target_table': table_mapping.target_table,
                'is_enabled': table_mapping.is_enabled,
                'sync_type': table_mapping.sync_type or '',  # Empty string if inheriting
                'sync_frequency': table_mapping.sync_frequency or '',  # Empty string if inheriting
                'columns': columns_data,
            })

        # Get all available tables from source database
        try:
            all_tables = get_table_list(db_config)
            # Filter out tables already configured
            configured_table_names = {tm.source_table for tm in replication_config.table_mappings.all()}
            available_tables = [t for t in all_tables if t not in configured_table_names]
        except Exception as e:
            logger.warning(f"Could not fetch available tables: {e}")
            available_tables = []

        config_data = {
            'id': replication_config.id,
            'client_name': client.name,
            'database_name': db_config.connection_name,
            'database_id': db_config.id,
            'connector_name': replication_config.connector_name or '',
            'status': replication_config.status,
            'status_display': dict(ReplicationConfig.STATUS_CHOICES).get(replication_config.status, replication_config.status).capitalize(),
            'sync_type': replication_config.sync_type,
            'sync_frequency': replication_config.sync_frequency,
            'auto_create_tables': replication_config.auto_create_tables,
            'drop_before_sync': replication_config.drop_before_sync,
            'tables': tables_data,
        }

        return JsonResponse({
            'success': True,
            'config': config_data,
            'available_tables': available_tables,
        })

    except Exception as e:
        logger.error(f"Failed to fetch config details: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)



@require_http_methods(["POST"])
def ajax_get_table_schemas_batch(request, database_pk):
    """
    AJAX endpoint: Get schemas for multiple tables to add to replication
    """
    try:
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        data = json.loads(request.body)
        table_names = data.get('tables', [])

        if not table_names:
            return JsonResponse({
                'success': False,
                'error': 'No tables specified'
            }, status=400)

        schemas = []
        for table_name in table_names:
            try:
                schema = get_table_schema(db_config, table_name)
                # Format columns for the modal
                formatted_columns = []
                for col in schema.get('columns', []):
                    formatted_columns.append({
                        'name': col.get('name'),
                        'type': str(col.get('type', '')),  # Convert SQLAlchemy type to string
                        'nullable': col.get('nullable', True),
                    })

                schemas.append({
                    'table_name': table_name,
                    'columns': formatted_columns
                })
            except Exception as e:
                logger.error(f"Error fetching schema for {table_name}: {e}")
                # Continue with other tables even if one fails

        return JsonResponse({
            'success': True,
            'schemas': schemas
        })

    except Exception as e:
        logger.error(f"Failed to fetch table schemas: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
def cdc_unified_status(request, config_pk):
    """
    Get comprehensive unified status for replication (NEW API).

    Returns detailed health information for both connector and consumer.
    Useful for debugging and monitoring.
    """
    try:
        config = ReplicationConfig.objects.get(pk=config_pk)

        # Use new ReplicationOrchestrator for unified status
        from client.replication import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)

        status = orchestrator.get_unified_status()

        return JsonResponse({
            'success': True,
            'status': status
        })

    except ReplicationConfig.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Replication configuration not found'
        }, status=404)

    except Exception as e:
        logger.error(f"Failed to get unified status: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


"""
Complete cdc_edit_config() function with all helper methods
Place this in your cdc_views.py file
"""

import logging
import time
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db import transaction
from sqlalchemy import text

from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.database_utils import get_table_list, get_table_schema, get_database_engine
from client.utils.debezium_manager import DebeziumConnectorManager

logger = logging.getLogger(__name__)



def terminate_active_slot_connections(db_config, slot_name):
    """
    Terminate any active connections holding the replication slot.
    
    This is necessary when restarting a connector because PostgreSQL
    only allows one active connection per replication slot.
    
    Args:
        db_config: ClientDatabase instance
        slot_name: Name of the replication slot (e.g., 'debezium_1_2')
    
    Returns:
        tuple: (success, message)
    """
    try:
        logger.info(f"🔍 Checking for active connections on slot: {slot_name}")
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # First, check if slot exists and get active PID
            check_query = text("""
                SELECT active, active_pid 
                FROM pg_replication_slots 
                WHERE slot_name = :slot_name
            """)
            
            result = conn.execute(check_query, {"slot_name": slot_name})
            slot_info = result.fetchone()
            
            if not slot_info:
                logger.warning(f"⚠️  Replication slot '{slot_name}' not found")
                return True, f"Slot {slot_name} doesn't exist (will be created)"
            
            is_active, active_pid = slot_info
            
            if not is_active or active_pid is None:
                logger.info(f"✅ Slot '{slot_name}' is not active")
                return True, f"Slot {slot_name} is available"
            
            logger.warning(f"⚠️  Slot '{slot_name}' is ACTIVE for PID {active_pid}")
            logger.info(f"🔪 Terminating connection PID {active_pid}...")
            
            # Terminate the active backend holding the slot
            terminate_query = text("""
                SELECT pg_terminate_backend(:pid)
            """)
            
            terminate_result = conn.execute(terminate_query, {"pid": active_pid})
            terminated = terminate_result.scalar()
            
            conn.commit()
            
            if terminated:
                logger.info(f"✅ Successfully terminated PID {active_pid}")
                
                # Verify slot is now inactive
                verify_result = conn.execute(check_query, {"slot_name": slot_name})
                verify_info = verify_result.fetchone()
                
                if verify_info and not verify_info[0]:
                    logger.info(f"✅ Slot '{slot_name}' is now available")
                    return True, f"Terminated blocking connection (PID {active_pid})"
                else:
                    logger.warning(f"⚠️  Slot may still be active, waiting for cleanup...")
                    return True, f"Terminated PID {active_pid}, slot should release shortly"
            else:
                logger.warning(f"⚠️  Failed to terminate PID {active_pid}")
                return False, f"Could not terminate blocking connection"
        
        engine.dispose()
        
    except Exception as e:
        error_msg = f"Error managing replication slot: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg


def get_replication_slot_status(db_config, slot_name):
    """
    Get detailed status of a replication slot.
    
    Returns:
        dict: Slot status information or None if not found
    """
    try:
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    slot_name,
                    plugin,
                    slot_type,
                    database,
                    active,
                    active_pid,
                    restart_lsn,
                    confirmed_flush_lsn
                FROM pg_replication_slots
                WHERE slot_name = :slot_name
            """)
            
            result = conn.execute(query, {"slot_name": slot_name})
            row = result.fetchone()
            
            if not row:
                return None
            
            return {
                'slot_name': row[0],
                'plugin': row[1],
                'slot_type': row[2],
                'database': row[3],
                'active': row[4],
                'active_pid': row[5],
                'restart_lsn': str(row[6]) if row[6] else None,
                'confirmed_flush_lsn': str(row[7]) if row[7] else None,
            }
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to get slot status: {e}")
        return None



def wait_for_connector_running(manager, connector_name, timeout=90, interval=3):
    """
    Wait for connector to be in RUNNING state with improved stability checking.
    
    Args:
        manager: DebeziumConnectorManager instance
        connector_name: Name of the connector
        timeout: Maximum seconds to wait (default: 90)
        interval: Seconds between status checks (default: 3)
    
    Returns:
        bool: True if connector is running, False if timeout exceeded
    """
    end_time = time.time() + timeout
    last_status = None
    
    logger.info(f"⏳ Waiting for connector {connector_name} to be RUNNING (timeout: {timeout}s)...")
    
    while time.time() < end_time:
        try:
            result = manager.get_connector_status(connector_name)
            
            if isinstance(result, tuple):
                success, details = result
                if not success or not details:
                    logger.warning(f"⚠️  Failed to get connector status, retrying...")
                    time.sleep(interval)
                    continue
            else:
                details = result
                if not details:
                    logger.warning(f"⚠️  Empty connector status, retrying...")
                    time.sleep(interval)
                    continue
            
            connector_state = details.get('connector', {}).get('state', 'UNKNOWN')
            tasks = details.get('tasks', [])
            
            current_status = f"Connector: {connector_state}, Tasks: {len(tasks)}"
            if current_status != last_status:
                logger.info(f"📊 Status: {current_status}")
                if tasks:
                    task_states = [t.get('state', 'UNKNOWN') for t in tasks]
                    logger.info(f"   Task states: {task_states}")
                last_status = current_status
            
            if connector_state != 'RUNNING':
                logger.debug(f"   Connector state is {connector_state}, waiting...")
                time.sleep(interval)
                continue
            
            if not tasks:
                logger.debug(f"   Connector is RUNNING but no tasks assigned yet...")
                time.sleep(interval)
                continue
            
            task_states = [t.get('state', 'UNKNOWN') for t in tasks]
            acceptable_states = ['RUNNING', 'RESTARTING', 'UNASSIGNED']
            failed_states = ['FAILED', 'DESTROYED']
            
            failed_tasks = [s for s in task_states if s in failed_states]
            if failed_tasks:
                logger.error(f"❌ Connector has failed tasks: {failed_tasks}")
                for task in tasks:
                    if task.get('state') in failed_states:
                        logger.error(f"   Task {task.get('id')} error: {task.get('trace', 'No trace')[:200]}")
                return False
            
            tasks_ok = all(state in acceptable_states for state in task_states)
            
            if connector_state == 'RUNNING' and tasks_ok:
                running_count = sum(1 for s in task_states if s == 'RUNNING')
                if running_count == len(tasks):
                    logger.info(f"✅ All {len(tasks)} connector task(s) are RUNNING")
                else:
                    logger.info(f"✅ Connector is RUNNING with {running_count}/{len(tasks)} tasks RUNNING")
                    logger.info(f"   Other tasks: {[s for s in task_states if s != 'RUNNING']}")
                return True
            
            logger.debug(f"   Tasks not ready yet: {task_states}")
            time.sleep(interval)
            
        except Exception as e:
            logger.warning(f"⚠️  Error checking connector status: {e}")
            time.sleep(interval)
    
    logger.error(f"❌ Timeout after {timeout} seconds")
    return False



def manage_postgresql_publication(db_config, replication_config, table_names):
    """
    Drop and recreate PostgreSQL publication with new table list.
    
    This is necessary because PostgreSQL publications in 'filtered' mode
    cannot be altered - they must be dropped and recreated when the table
    list changes.
    
    Args:
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance
        table_names: List of table names to include in publication
    
    Returns:
        tuple: (success, message)
    """
    if db_config.db_type.lower() != 'postgresql':
        return True, "Not PostgreSQL - skipping publication management"
    
    try:
        client_id = db_config.client.id
        db_id = db_config.id
        publication_name = f"debezium_pub_{client_id}_{db_id}"
        
        logger.info(f"📋 Managing PostgreSQL publication: {publication_name}")
        logger.info(f"   Tables to include: {table_names}")
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # Check if publication exists
            check_query = text(
                f"SELECT COUNT(*) FROM pg_publication WHERE pubname = '{publication_name}'"
            )
            result = conn.execute(check_query)
            exists = result.scalar() > 0
            
            if exists:
                logger.info(f"🗑️  Dropping existing publication: {publication_name}")
                drop_query = text(f"DROP PUBLICATION IF EXISTS {publication_name}")
                conn.execute(drop_query)
                conn.commit()
                logger.info(f"✅ Dropped publication successfully")
            else:
                logger.info(f"ℹ️  Publication doesn't exist yet, will create new one")
            
            # Create publication with new table list
            schema_name = 'public'
            tables_qualified = [f"{schema_name}.{table}" for table in table_names]
            
            logger.info(f"🔨 Creating publication with {len(tables_qualified)} tables...")
            
            create_query = text(
                f"CREATE PUBLICATION {publication_name} FOR TABLE {', '.join(tables_qualified)}"
            )
            conn.execute(create_query)
            conn.commit()
            
            logger.info(f"✅ Created publication: {publication_name}")
            logger.info(f"   Tables: {', '.join(tables_qualified)}")
            
        engine.dispose()
        
        return True, f"Publication {publication_name} recreated with {len(table_names)} table(s)"
        
    except Exception as e:
        error_msg = f"Failed to manage publication: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg


@require_http_methods(["GET", "POST"])
def cdc_edit_config(request, config_pk):
    """
    Edit replication configuration
    - Enable/disable tables
    - Rename target tables
    - Enable/disable columns
    - Change sync settings
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    db_config = replication_config.client_database
    client = db_config.client

    if request.method == "POST":
        try:
            with transaction.atomic():
                # Update basic configuration
                replication_config.sync_type = request.POST.get('sync_type', replication_config.sync_type)
                replication_config.sync_frequency = request.POST.get('sync_frequency', replication_config.sync_frequency)
                replication_config.auto_create_tables = request.POST.get('auto_create_tables') == 'on'
                replication_config.drop_before_sync = request.POST.get('drop_before_sync') == 'on'
                replication_config.save()

                # Get selected tables
                selected_table_names = request.POST.getlist('selected_tables')
                logger.info(f"Selected tables: {selected_table_names}")

                existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}
                newly_added_tables = []
                removed_table_names = []

                # Find removed tables
                for table_name in existing_mappings.keys():
                    if table_name not in selected_table_names:
                        removed_table_names.append(table_name)

                if removed_table_names:
                    logger.info(f"🗑️ Tables to remove: {removed_table_names}")

                # Process newly added tables
                for table_name in selected_table_names:
                    if table_name not in existing_mappings:
                        newly_added_tables.append(table_name)
                        logger.info(f"➕ New table to add: {table_name}")
                        
                        # Get target table name
                        target_table_name = request.POST.get(f'target_table_new_{table_name}', '')
                        if not target_table_name:
                            source_db_name = db_config.database_name
                            target_table_name = f"{source_db_name}_{table_name}"

                        # Get table settings
                        sync_type = request.POST.get(f'sync_type_table_new_{table_name}', '')
                        incremental_col = request.POST.get(f'incremental_col_table_new_{table_name}', '')
                        conflict_resolution = request.POST.get(f'conflict_resolution_table_new_{table_name}', 'source_wins')

                        # Create table mapping
                        table_mapping = TableMapping.objects.create(
                            replication_config=replication_config,
                            source_table=table_name,
                            target_table=target_table_name,
                            is_enabled=True,
                            sync_type=sync_type if sync_type else 'realtime',
                            incremental_column=incremental_col if incremental_col else None,
                            conflict_resolution=conflict_resolution
                        )

                        # Create column mappings
                        try:
                            schema = get_table_schema(db_config, table_name)
                            for column in schema.get('columns', []):
                                col_enabled = request.POST.get(f'enabled_column_new_{table_name}_{column["name"]}') == 'on'
                                target_col_name = request.POST.get(f'target_column_new_{table_name}_{column["name"]}', column['name'])
                                
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=column['name'],
                                    target_column=target_col_name,
                                    source_type=str(column.get('type', 'unknown')),
                                    target_type=str(column.get('type', 'unknown')),
                                    is_enabled=col_enabled
                                )
                        except Exception as e:
                            logger.error(f"Failed to create column mappings for {table_name}: {e}")

                        logger.info(f"✅ Created new table mapping for {table_name}")

                # CREATE TARGET TABLES
                if newly_added_tables and replication_config.auto_create_tables:
                    try:
                        from client.utils.table_creator import create_target_tables
                        logger.info(f"🔨 Creating target tables for {len(newly_added_tables)} tables...")
                        create_target_tables(replication_config, specific_tables=newly_added_tables)
                        messages.success(request, f'✅ Created {len(newly_added_tables)} new target tables!')
                    except Exception as e:
                        logger.error(f"❌ Failed to create target tables: {e}", exc_info=True)
                        messages.warning(request, f'⚠️ Configuration saved, but failed to create target tables: {str(e)}')

                # CREATE KAFKA TOPICS
                if newly_added_tables:
                    try:
                        from client.utils.kafka_topic_manager import KafkaTopicManager
                        logger.info(f"📡 Creating Kafka topics for {len(newly_added_tables)} tables...")

                        topic_manager = KafkaTopicManager()
                        topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"

                        if db_config.db_type.lower() == 'postgresql':
                            schema_name = 'public'
                            topic_results = topic_manager.create_cdc_topics_for_tables(
                                server_name=topic_prefix,
                                database=schema_name,
                                table_names=newly_added_tables
                            )
                        else:
                            topic_results = topic_manager.create_cdc_topics_for_tables(
                                server_name=topic_prefix,
                                database=db_config.database_name,
                                table_names=newly_added_tables
                            )

                        # Verify topics created
                        topics_missing = []
                        for table in newly_added_tables:
                            if db_config.db_type.lower() == 'postgresql':
                                topic_name = f"{topic_prefix}.public.{table}"
                            else:
                                topic_name = f"{topic_prefix}.{db_config.database_name}.{table}"
                            
                            if not topic_manager.topic_exists(topic_name):
                                topics_missing.append(topic_name)

                        if topics_missing:
                            logger.error(f"❌ Failed to create topics: {', '.join(topics_missing)}")
                            messages.warning(request, f'⚠️ Failed to create Kafka topics')

                    except Exception as e:
                        logger.error(f"❌ Error creating Kafka topics: {e}", exc_info=True)
                        messages.warning(request, f'⚠️ Failed to create Kafka topics: {str(e)}')

                # Process removed tables
                for table_name in removed_table_names:
                    table_mapping = existing_mappings[table_name]
                    logger.info(f"🗑️ Deleting table mapping for {table_name}")
                    table_mapping.delete()
    
                # DROP TARGET TABLES
                if removed_table_names:
                    try:
                        from client.utils.table_creator import drop_target_tables
                        drop_target_tables(replication_config, removed_table_names)
                        
                        if replication_config.drop_before_sync:
                            messages.success(request, f'✅ Dropped {len(removed_table_names)} removed tables!')
                    except Exception as e:
                        logger.error(f"❌ Failed to drop target tables: {e}", exc_info=True)

                # Update existing table mappings
                for table_mapping in replication_config.table_mappings.all():
                    table_key = f'table_{table_mapping.id}'

                    new_target_name = request.POST.get(f'target_{table_key}')
                    if new_target_name:
                        table_mapping.target_table = new_target_name

                    table_sync_type = request.POST.get(f'sync_type_{table_key}')
                    if table_sync_type:
                        table_mapping.sync_type = table_sync_type

                    incremental_col = request.POST.get(f'incremental_col_{table_key}')
                    if incremental_col:
                        table_mapping.incremental_column = incremental_col

                    conflict_res = request.POST.get(f'conflict_resolution_{table_key}')
                    if conflict_res:
                        table_mapping.conflict_resolution = conflict_res

                    table_mapping.save()

                    # Update column mappings
                    for column_mapping in table_mapping.column_mappings.all():
                        column_key = f'column_{column_mapping.id}'
                        column_mapping.is_enabled = request.POST.get(f'enabled_{column_key}') == 'on'
                        new_target_col = request.POST.get(f'target_{column_key}')
                        if new_target_col:
                            column_mapping.target_column = new_target_col
                        column_mapping.save()

                restart_connector = request.POST.get('restart_connector') == 'on'
                messages.success(request, '✅ Configuration updated successfully!')

                # ========================================================================
                # CONNECTOR UPDATE
                # ========================================================================
                if replication_config.connector_name:
                    try:
                        manager = DebeziumConnectorManager()

                        # Get all enabled tables
                        all_enabled_tables = replication_config.table_mappings.filter(is_enabled=True)
                        tables_list = [tm.source_table for tm in all_enabled_tables]

                        if tables_list:
                            # Check if connector exists
                            connector_result = manager.get_connector_status(replication_config.connector_name)
                            connector_exists = isinstance(connector_result, tuple) and connector_result[0]

                            if not connector_exists:
                                logger.warning(f"⚠️  Connector {replication_config.connector_name} doesn't exist")
                                replication_config.connector_name = None
                                replication_config.status = 'configured'
                                replication_config.is_active = False
                                replication_config.save()
                            else:
                                # Process table changes
                                if newly_added_tables or removed_table_names:
                                    logger.info(f"🔧 Updating connector (added: {len(newly_added_tables)}, removed: {len(removed_table_names)})")

                                    try:
                                        # ================================================================
                                        # POSTGRESQL-SPECIFIC: Manage publication and slot
                                        # ================================================================
                                        if db_config.db_type.lower() == 'postgresql':
                                            logger.info(f"🔧 PostgreSQL detected - managing publication and slot...")
                                            
                                            # STEP 1: Manage publication
                                            pub_success, pub_message = manage_postgresql_publication(
                                                db_config, 
                                                replication_config, 
                                                tables_list
                                            )
                                            
                                            if not pub_success:
                                                raise Exception(f"Publication management failed: {pub_message}")
                                            
                                            logger.info(f"✅ {pub_message}")
                                            
                                            # STEP 2: Clear any active slot connections BEFORE restarting
                                            client_id = db_config.client.id
                                            db_id = db_config.id
                                            slot_name = f"debezium_{client_id}_{db_id}"
                                            
                                            logger.info(f"🔍 Checking replication slot: {slot_name}")
                                            slot_status = get_replication_slot_status(db_config, slot_name)
                                            
                                            if slot_status:
                                                logger.info(f"📊 Slot status: active={slot_status['active']}, pid={slot_status['active_pid']}")
                                                
                                                if slot_status['active'] and slot_status['active_pid']:
                                                    logger.warning(f"⚠️  Slot is active, terminating connection...")
                                                    term_success, term_msg = terminate_active_slot_connections(db_config, slot_name)
                                                    
                                                    if term_success:
                                                        logger.info(f"✅ {term_msg}")
                                                        # Wait for PostgreSQL to clean up
                                                        time.sleep(2)
                                                    else:
                                                        logger.warning(f"⚠️  {term_msg}")
                                            else:
                                                logger.info(f"ℹ️  Slot doesn't exist yet (will be created)")
                                            
                                            # STEP 3: Restart connector
                                            logger.info(f"🔄 Restarting connector to refresh publication...")
                                            restart_success, restart_error = manager.restart_connector(
                                                replication_config.connector_name
                                            )
                                            
                                            if not restart_success:
                                                logger.warning(f"⚠️  Connector restart failed: {restart_error}")
                                            else:
                                                logger.info(f"✅ Connector restarted successfully")
                                                # Give it a moment to stabilize
                                                time.sleep(5)
                                        
                                        # ================================================================
                                        # Update connector configuration
                                        # ================================================================
                                        current_config = manager.get_connector_config(replication_config.connector_name)

                                        if not current_config:
                                            raise Exception("Failed to retrieve connector configuration")

                                        # Format table list based on database type
                                        if db_config.db_type.lower() == 'postgresql':
                                            schema_name = 'public'
                                            tables_full = [f"{schema_name}.{table}" for table in tables_list]
                                        else:
                                            tables_full = [f"{db_config.database_name}.{table}" for table in tables_list]
                                        
                                        current_config['table.include.list'] = ','.join(tables_full)

                                        # Change snapshot mode to support incremental snapshots
                                        if newly_added_tables and current_config.get('snapshot.mode') == 'initial':
                                            logger.info(f"⚙️  Changing snapshot.mode to 'when_needed'")
                                            current_config['snapshot.mode'] = 'when_needed'

                                        logger.info(f"📋 Updated table.include.list: {current_config['table.include.list']}")

                                        # Update connector config
                                        update_success, update_error = manager.update_connector_config(
                                            replication_config.connector_name,
                                            current_config
                                        )

                                        if not update_success:
                                            raise Exception(f"Failed to update config: {update_error}")

                                        logger.info(f"✅ Connector config updated")

                                        # Restart connector for non-PostgreSQL databases
                                        if db_config.db_type.lower() != 'postgresql':
                                            logger.info(f"🔄 Restarting connector tasks...")
                                            restart_success, restart_error = manager.restart_connector(
                                                replication_config.connector_name
                                            )

                                            if not restart_success:
                                                raise Exception(f"Failed to restart: {restart_error}")

                                            logger.info(f"✅ Connector tasks restarted")

                                        # Wait for connector to stabilize
                                        logger.info(f"⏳ Waiting for connector to stabilize...")
                                        wait_success = wait_for_connector_running(
                                            manager, 
                                            replication_config.connector_name,
                                            timeout=90,
                                            interval=3
                                        )
                                        
                                        if not wait_success:
                                            logger.warning(f"⚠️  Connector did not fully stabilize")
                                            messages.warning(
                                                request,
                                                'Configuration updated. Check connector status - it may still be starting.'
                                            )
                                        else:
                                            logger.info(f"✅ Connector verified as RUNNING")

                                        # ================================================================
                                        # Send incremental snapshot signal for new tables
                                        # ================================================================
                                        if newly_added_tables:
                                            logger.info(f"📡 Preparing incremental snapshot for {len(newly_added_tables)} table(s)...")

                                            try:
                                                from client.utils.kafka_signal_sender import KafkaSignalSender
                                                from client.utils.kafka_topic_manager import KafkaTopicManager

                                                topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"
                                                signal_topic_name = f"{topic_prefix}.signals"
                                                
                                                logger.info(f"📡 Ensuring Kafka signal topic: {signal_topic_name}")
                                                
                                                topic_manager = KafkaTopicManager()
                                                signal_topic_success, signal_topic_error = topic_manager.create_signal_topic(topic_prefix)

                                                if signal_topic_success:
                                                    logger.info(f"✅ Kafka signal topic ready")
                                                else:
                                                    logger.warning(f"⚠️  Signal topic issue: {signal_topic_error}")

                                                # Send incremental snapshot signal
                                                signal_sender = KafkaSignalSender()
                                                logger.info(f"📡 Sending incremental snapshot signal via Kafka")
                                                logger.info(f"   Tables: {newly_added_tables}")

                                                # Pass db_type and schema_name for correct formatting
                                                if db_config.db_type.lower() == 'postgresql':
                                                    success, message = signal_sender.send_incremental_snapshot_signal(
                                                        topic_prefix=topic_prefix,
                                                        database_name=db_config.database_name,
                                                        table_names=newly_added_tables,
                                                        schema_name='public',
                                                        db_type='postgresql'
                                                    )
                                                else:
                                                    success, message = signal_sender.send_incremental_snapshot_signal(
                                                        topic_prefix=topic_prefix,
                                                        database_name=db_config.database_name,
                                                        table_names=newly_added_tables,
                                                        db_type='mysql'
                                                    )

                                                signal_sender.close()

                                                if success:
                                                    logger.info(f"✅ {message}")
                                                    messages.success(
                                                        request,
                                                        f'✅ {len(newly_added_tables)} table(s) added! Incremental snapshot in progress.'
                                                    )
                                                else:
                                                    logger.error(f"❌ Failed to send signal: {message}")
                                                    messages.warning(request, f'⚠️ Snapshot signal failed: {message}')

                                            except Exception as e:
                                                logger.error(f"❌ Error sending snapshot signal: {e}", exc_info=True)
                                                messages.warning(request, f'⚠️ Snapshot signal failed: {str(e)}')

                                            # Restart consumer to subscribe to new table topics
                                            if replication_config.is_active:
                                                try:
                                                    from client.tasks import stop_kafka_consumer, start_kafka_consumer
                                                    logger.info(f"🔄 Restarting consumer...")

                                                    stop_result = stop_kafka_consumer(replication_config.id)
                                                    if stop_result.get('success'):
                                                        logger.info(f"✅ Consumer stopped")

                                                    time.sleep(2)

                                                    replication_config.refresh_from_db()
                                                    replication_config.is_active = True
                                                    replication_config.status = 'active'
                                                    replication_config.save()

                                                    start_kafka_consumer.apply_async(
                                                        args=[replication_config.id],
                                                        countdown=3
                                                    )
                                                    logger.info(f"✅ Consumer restart scheduled")

                                                except Exception as e:
                                                    logger.error(f"❌ Error restarting consumer: {e}")
                                                    messages.warning(request, 'Consumer restart failed')

                                        # Log removed tables
                                        if removed_table_names:
                                            logger.info(f"✅ Removed {len(removed_table_names)} table(s)")
                                            messages.success(request, f'✅ {len(removed_table_names)} table(s) removed')

                                    except Exception as e:
                                        logger.error(f"❌ Error updating connector: {e}", exc_info=True)
                                        messages.error(request, f'❌ Failed to update connector: {str(e)}')
                                        
                                elif restart_connector:
                                    # No table changes, just restart if requested
                                    logger.info(f"🔄 Restarting connector...")
                                    
                                    # For PostgreSQL, clear slot before restart
                                    if db_config.db_type.lower() == 'postgresql':
                                        client_id = db_config.client.id
                                        db_id = db_config.id
                                        slot_name = f"debezium_{client_id}_{db_id}"
                                        terminate_active_slot_connections(db_config, slot_name)
                                        time.sleep(2)
                                    
                                    success, error = manager.restart_connector(replication_config.connector_name)
                                    if success:
                                        messages.success(request, '✅ Connector restarted!')
                                    else:
                                        messages.warning(request, f'⚠️ Restart failed: {error}')

                    except Exception as e:
                        logger.error(f"❌ Error with connector: {e}", exc_info=True)
                        messages.warning(request, f'⚠️ Configuration saved, connector update failed: {str(e)}')

                # Redirect
                if replication_config.connector_name:
                    return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
                else:
                    return redirect('main-dashboard')

        except Exception as e:
            logger.error(f'Failed to update configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to update configuration: {str(e)}')

    # ========================================================================
    # GET: Show edit form
    # ========================================================================
    try:
        all_table_names = get_table_list(db_config)
        logger.info(f"Discovered {len(all_table_names)} tables")
    except Exception as e:
        logger.error(f"Failed to discover tables: {e}")
        messages.error(request, f"Failed to discover tables: {str(e)}")
        all_table_names = []

    existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}

    tables_with_columns = []
    for table_name in all_table_names:
        existing_mapping = existing_mappings.get(table_name)

        try:
            schema = get_table_schema(db_config, table_name)
            columns_from_schema = schema.get('columns', [])

            incremental_candidates = [
                col for col in columns_from_schema
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time')
            ]

            try:
                engine = get_database_engine(db_config)
                with engine.connect() as conn:
                    if db_config.db_type.lower() == 'mysql':
                        count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                    elif db_config.db_type.lower() == 'postgresql':
                        count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
                    else:
                        count_query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")

                    result = conn.execute(count_query)
                    row = result.fetchone()
                    row_count = row[0] if row else 0
                engine.dispose()
            except:
                row_count = 0

            source_db_name = db_config.database_name
            default_target_table_name = f"{source_db_name}_{table_name}"

            table_data = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(columns_from_schema),
                'is_mapped': existing_mapping is not None,
                'mapping': existing_mapping,
                'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
                'all_columns': columns_from_schema,
                'incremental_candidates': incremental_candidates,
                'primary_keys': schema.get('primary_keys', []),
                'default_target_name': default_target_table_name,
            }

            tables_with_columns.append(table_data)

        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            
            source_db_name = db_config.database_name
            default_target_table_name = f"{source_db_name}_{table_name}"

            table_data = {
                'table_name': table_name,
                'row_count': 0,
                'column_count': 0,
                'is_mapped': existing_mapping is not None,
                'mapping': existing_mapping,
                'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
                'all_columns': [],
                'incremental_candidates': [],
                'primary_keys': [],
                'default_target_name': default_target_table_name,
            }
            tables_with_columns.append(table_data)

    # Sort tables: mapped first, then by name
    tables_with_columns.sort(key=lambda t: (not t['is_mapped'], t['table_name']))

    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'tables_with_columns': tables_with_columns,
    }

    return render(request, 'client/cdc/edit_config.html', context)