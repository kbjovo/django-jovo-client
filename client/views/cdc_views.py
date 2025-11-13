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

from client.utils.database_utils import get_table_list, get_table_schema
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db import transaction
from django.db.models import Q
from sqlalchemy import text
import logging
import json

from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.kafka_topic_manager import KafkaTopicManager

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
            with transaction.atomic():
                # Create ReplicationConfig
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
    Create Debezium connector for the replication configuration
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
            
            # Generate connector configuration
            connector_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=replication_config,
                tables_whitelist=tables_list,
            )
            
            if not connector_config:
                raise Exception("Failed to generate connector configuration")
            
            # Create connector via Debezium Manager
            manager = DebeziumConnectorManager()
            
            # Check Kafka Connect health
            is_healthy, health_error = manager.check_kafka_connect_health()
            if not is_healthy:
                raise Exception(f"Kafka Connect is not healthy: {health_error}")

            # CRITICAL: Setup signal table for incremental snapshots
            # This must be done BEFORE creating the connector
            try:
                from client.utils.debezium_snapshot import setup_signal_table
                logger.info("Setting up Debezium signal table for incremental snapshots...")
                setup_success = setup_signal_table(replication_config)
                if setup_success:
                    logger.info("✅ Signal table is ready for incremental snapshots")
                else:
                    logger.warning("⚠️ Signal table setup had issues, but continuing...")
            except Exception as e:
                logger.warning(f"⚠️ Failed to setup signal table: {e}")
                # Don't fail connector creation, signal table can be created later

            # Create connector
            success, error = manager.create_connector(
                connector_name=connector_name,
                config=connector_config,
                notify_on_error=True
            )

            if not success:
                raise Exception(f"Failed to create connector: {error}")

            # Pre-create Kafka topics with proper configuration
            # CRITICAL: Connector is useless without topics
            topic_manager = KafkaTopicManager()

            # Topic prefix is client_{client_id}_db_{database_id} (e.g., client_2_db_5)
            topic_prefix = f"client_{client.id}_db_{db_config.id}"

            # Create topics for all enabled tables
            topic_results = topic_manager.create_cdc_topics_for_tables(
                server_name=topic_prefix,
                database=db_config.database_name,
                table_names=tables_list
            )

            # Verify all topics exist (either newly created or already existed)
            topics_verified = []
            topics_missing = []

            for table in tables_list:
                topic_name = f"{topic_prefix}.{db_config.database_name}.{table}"
                if topic_manager.topic_exists(topic_name):
                    topics_verified.append(topic_name)
                else:
                    topics_missing.append(topic_name)

            # If any required topics are missing, abort connector creation
            if topics_missing:
                # Rollback: Delete the connector we just created
                try:
                    logger.warning(f"Rolling back connector {connector_name} due to missing topics")
                    manager.delete_connector(connector_name)
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback connector: {rollback_error}")

                error_msg = (
                    f"❌ Failed to create required Kafka topics: {', '.join(topics_missing)}. "
                    f"Connector creation aborted. "
                    f"Please check Kafka broker status and retry."
                )
                raise Exception(error_msg)

            # Log success
            successful_topics = [t for t, success in topic_results.items() if success]
            failed_topics = [t for t, success in topic_results.items() if not success]

            if successful_topics:
                logger.info(f"✅ Created {len(successful_topics)} new Kafka topics for connector {connector_name}")
            if failed_topics:
                # Topics "failed" to create but exist - that's OK (idempotent)
                logger.info(f"ℹ️  {len(failed_topics)} topics already existed (reusing existing topics)")

            # Update replication config (but don't start replication yet)
            replication_config.connector_name = connector_name
            replication_config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}"
            replication_config.status = 'configured'
            replication_config.is_active = False  # Changed from True
            replication_config.save()
            
            messages.success(
                request, 
                f'✅ Debezium connector "{connector_name}" created successfully! '
                f'You can now start replication from the monitoring page.'
            )
            
            return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
            
        except Exception as e:
            logger.error(f"Failed to create connector: {e}", exc_info=True)
            messages.error(request, f'Failed to create connector: {str(e)}')
    
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
    Real-time monitoring of connector status
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    
    if not replication_config.connector_name:
        messages.error(request, 'No connector configured for this replication')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)
    
    try:
        manager = DebeziumConnectorManager()
        
        # Get connector status
        exists, status_data = manager.get_connector_status(replication_config.connector_name)
        
        if not exists:
            messages.warning(request, 'Connector not found in Kafka Connect')
            status_data = None
        
        # Get connector tasks
        tasks = manager.get_connector_tasks(replication_config.connector_name) if exists else []
        
        context = {
            'replication_config': replication_config,
            'client': replication_config.client_database.client,
            'connector_status': status_data,
            'tasks': tasks,
            'exists': exists,
        }
        
        return render(request, 'client/cdc/monitor_connector.html', context)
        
    except Exception as e:
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
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    connector_name = replication_config.connector_name
    logger.info(f" ✅✅✅✅ CDC Action Print: {action}")
    
    if not connector_name:
        return JsonResponse({
            'success': False,
            'error': 'No connector configured'
        }, status=400)
    
    try:
        manager = DebeziumConnectorManager()
        
        if action == 'start':
            # Start the Kafka consumer to begin replication
            logger.info(f"✅ Testing replication for config {config_pk}")
            logger.info(f"🚀 Starting replication for config {config_pk}")
            
            # Check connector is running first
            exists, status_data = manager.get_connector_status(connector_name)
            if not exists:
                return JsonResponse({
                    'success': False,
                    'error': 'Connector not found'
                }, status=400)
            
            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            if connector_state != 'RUNNING':
                return JsonResponse({
                    'success': False,
                    'error': f'Connector is not running. Current state: {connector_state}'
                }, status=400)
            
            # Start Kafka consumer task
            task = start_kafka_consumer.apply_async(
                args=[config_pk],
                countdown=2  # Start after 2 seconds
            )
            
            # Update status
            replication_config.status = 'active'
            replication_config.is_active = True
            replication_config.save()
            
            # Update client replication status
            client = replication_config.client_database.client
            client.replication_enabled = True
            client.replication_status = 'active'
            client.save()
            
            message = f'Replication started successfully! Task ID: {task.id}'
            success = True
            error = None
            
        elif action == 'pause':
            success, error = manager.pause_connector(connector_name)
            if success:
                # Also stop the consumer
                stop_kafka_consumer(replication_config.pk)
                replication_config.status = 'paused'
                replication_config.is_active = False
            message = 'Connector paused and consumer stopped successfully'
            
        elif action == 'resume':
            success, error = manager.resume_connector(connector_name)
            if success:
                # Restart consumer
                start_kafka_consumer.apply_async(
                    args=[replication_config.pk],
                    countdown=2
                )
                replication_config.status = 'active'
                replication_config.is_active = True
            message = 'Connector resumed and consumer restarted successfully'
            
        elif action == 'restart':
            success, error = manager.restart_connector(connector_name)
            if success:
                # Restart consumer
                restart_replication.apply_async(args=[replication_config.pk])
            message = 'Connector and consumer restarted successfully'
            
        elif action == 'delete':
            success, error = manager.delete_connector(connector_name, notify=True)
            if success:
                # Stop consumer first
                stop_kafka_consumer(replication_config.pk)
                replication_config.status = 'disabled'
                replication_config.connector_name = None
                replication_config.is_active = False
            message = 'Connector deleted and consumer stopped successfully'
            
        else:
            return JsonResponse({
                'success': False,
                'error': f'Unknown action: {action}'
            }, status=400)
        
        if success:
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
    """Start CDC replication for a config"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        # Check if already running
        if config.is_active and config.status == 'active':
            messages.warning(request, "Replication is already running!")
            return redirect('cdc_config_detail', config_id=config_id)
        
        # Create connector and start consumer
        task = create_debezium_connector.apply_async(args=[config_id])
        
        messages.success(
            request,
            f"🚀 Replication started! Task ID: {task.id}"
        )
        
        return redirect('cdc_config_detail', config_id=config_id)
        
    except Exception as e:
        messages.error(request, f"Failed to start replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def stop_replication(request, config_id):
    """Stop CDC replication for a config"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        if not config.is_active:
            messages.warning(request, "Replication is not running!")
            return redirect('cdc_config_detail', config_id=config_id)
        
        # Stop consumer
        task = stop_kafka_consumer.apply_async(args=[config_id])
        
        messages.success(
            request,
            f"⏸️ Replication stopped! Task ID: {task.id}"
        )
        
        return redirect('cdc_config_detail', config_id=config_id)
        
    except Exception as e:
        messages.error(request, f"Failed to stop replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def restart_replication_view(request, config_id):
    """Restart CDC replication for a config"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        # Restart replication
        task = restart_replication.apply_async(args=[config_id])
        
        messages.success(
            request,
            f"🔄 Replication restarted! Task ID: {task.id}"
        )
        
        return redirect('cdc_config_detail', config_id=config_id)
        
    except Exception as e:
        messages.error(request, f"Failed to restart replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["GET"])
def replication_status(request, config_id):
    """Get replication status (AJAX endpoint)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        data = {
            'is_active': config.is_active,
            'status': config.status,
            'connector_name': config.connector_name,
            'last_sync_at': config.last_sync_at.isoformat() if config.last_sync_at else None,
            'table_count': config.table_mappings.filter(is_enabled=True).count(),
        }
        
        # Get connector status if exists
        if config.connector_name:
            from client.utils.debezium_manager import DebeziumConnectorManager
            manager = DebeziumConnectorManager()
            
            exists, status_data = manager.get_connector_status(config.connector_name)
            
            if exists and status_data:
                data['connector_state'] = status_data.get('connector', {}).get('state', 'UNKNOWN')
                data['tasks'] = status_data.get('tasks', [])
        
        return JsonResponse(data)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


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

                # Get selected tables from form
                selected_table_names = request.POST.getlist('selected_tables')
                logger.info(f"Selected tables: {selected_table_names}")

                # Get existing mappings
                existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}

                # Process newly added tables
                for table_name in selected_table_names:
                    if table_name not in existing_mappings:
                        # Create new table mapping for newly added table
                        source_db_name = db_config.database_name
                        target_table_name = f"{source_db_name}_{table_name}"

                        table_mapping = TableMapping.objects.create(
                            replication_config=replication_config,
                            source_table=table_name,
                            target_table=target_table_name,
                            is_enabled=True,
                            sync_type='realtime',
                            conflict_resolution='source_wins'
                        )

                        # Create column mappings for new table
                        try:
                            schema = get_table_schema(db_config, table_name)
                            for column in schema.get('columns', []):
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=column['name'],
                                    target_column=column['name'],
                                    source_type=column.get('type', 'unknown'),
                                    target_type=column.get('type', 'unknown'),
                                    is_enabled=True
                                )
                        except Exception as e:
                            logger.error(f"Failed to create column mappings for {table_name}: {e}")

                        logger.info(f"Created new table mapping for {table_name}")

                # Process removed tables (tables that were mapped but not selected)
                for table_name, table_mapping in existing_mappings.items():
                    if table_name not in selected_table_names:
                        # Delete table mapping (cascade will delete column mappings)
                        logger.info(f"Deleting table mapping for {table_name}")
                        table_mapping.delete()

                # Update existing table mappings
                for table_mapping in replication_config.table_mappings.all():
                    table_key = f'table_{table_mapping.id}'

                    # Update target table name
                    new_target_name = request.POST.get(f'target_{table_key}')
                    if new_target_name:
                        table_mapping.target_table = new_target_name

                    # Update sync type
                    table_sync_type = request.POST.get(f'sync_type_{table_key}')
                    if table_sync_type:
                        table_mapping.sync_type = table_sync_type

                    # Update incremental column
                    incremental_col = request.POST.get(f'incremental_col_{table_key}')
                    if incremental_col:
                        table_mapping.incremental_column = incremental_col

                    # Update conflict resolution
                    conflict_res = request.POST.get(f'conflict_resolution_{table_key}')
                    if conflict_res:
                        table_mapping.conflict_resolution = conflict_res

                    table_mapping.save()

                    # Update column mappings
                    for column_mapping in table_mapping.column_mappings.all():
                        column_key = f'column_{column_mapping.id}'

                        # Enable/disable column
                        column_mapping.is_enabled = request.POST.get(f'enabled_{column_key}') == 'on'

                        # Update target column name
                        new_target_col = request.POST.get(f'target_{column_key}')
                        if new_target_col:
                            column_mapping.target_column = new_target_col

                        column_mapping.save()

                # Check if restart connector checkbox is checked
                restart_connector = request.POST.get('restart_connector') == 'on'

                messages.success(request, 'Configuration updated successfully!')

                if restart_connector and replication_config.connector_name:
                    # Restart the connector
                    try:
                        manager = DebeziumConnectorManager()
                        success, error = manager.restart_connector(replication_config.connector_name)
                        if success:
                            messages.success(request, 'Connector restarted successfully!')
                        else:
                            messages.warning(request, f'Configuration saved but connector restart failed: {error}')
                    except Exception as e:
                        logger.error(f"Failed to restart connector: {e}")
                        messages.warning(request, f'Configuration saved but connector restart failed: {str(e)}')

                # Redirect to monitor page
                if replication_config.connector_name:
                    return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
                else:
                    return redirect('main-dashboard')

        except Exception as e:
            logger.error(f'Failed to update configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to update configuration: {str(e)}')

    # GET: Show edit form
    # Discover ALL tables from the database (not just mapped ones)
    try:
        all_table_names = get_table_list(db_config)
        logger.info(f"Discovered {len(all_table_names)} tables from database")
    except Exception as e:
        logger.error(f"Failed to discover tables: {e}")
        messages.error(request, f"Failed to discover tables from database: {str(e)}")
        all_table_names = []

    # Build a map of existing table mappings
    existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}

    # Combine discovered tables with existing mappings
    tables_with_columns = []
    for table_name in all_table_names:
        existing_mapping = existing_mappings.get(table_name)

        try:
            # Get schema from source database
            schema = get_table_schema(db_config, table_name)
            columns_from_schema = schema.get('columns', [])

            # Find potential incremental columns
            incremental_candidates = [
                col for col in columns_from_schema
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time') or
                   (col.get('name', '') in ['id', 'created_at', 'updated_at'] and
                    'int' in str(col.get('type', '')).lower())
            ]

            # Get row count
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
            except Exception as e:
                logger.debug(f"Could not get row count for {table_name}: {e}")
                row_count = 0

            table_data = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(columns_from_schema),
                'is_mapped': existing_mapping is not None,
                'mapping': existing_mapping,
                'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
                'columns_schema': columns_from_schema,
                'incremental_candidates': incremental_candidates,
                'primary_keys': schema.get('primary_keys', []),
            }

            tables_with_columns.append(table_data)

        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            table_data = {
                'table_name': table_name,
                'row_count': 0,
                'column_count': 0,
                'is_mapped': existing_mapping is not None,
                'mapping': existing_mapping,
                'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
                'columns_schema': [],
                'incremental_candidates': [],
                'primary_keys': [],
            }
            tables_with_columns.append(table_data)

    # Sort: mapped tables first, then unmapped
    tables_with_columns.sort(key=lambda t: (not t['is_mapped'], t['table_name']))

    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'tables_with_columns': tables_with_columns,
    }

    return render(request, 'client/cdc/edit_config.html', context)


@require_http_methods(["POST"])
def cdc_delete_config(request, config_pk):
    """
    Delete replication configuration (for incomplete setups or when no longer needed)
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        logger.info(f"Deleting replication config {config_pk} (connector: {replication_config.connector_name})")

        # If connector exists, delete it first (along with its topics)
        if replication_config.connector_name:
            try:
                manager = DebeziumConnectorManager()
                success, error = manager.delete_connector(
                    replication_config.connector_name,
                    notify=True,
                    delete_topics=True  # Explicitly delete associated Kafka topics
                )
                if not success:
                    logger.warning(f"Failed to delete connector: {error}")
                    # Continue anyway to delete the config from database
                else:
                    logger.info(f"Successfully deleted connector and topics for {replication_config.connector_name}")
            except Exception as e:
                logger.error(f"Error deleting connector: {e}")
                # Continue anyway to delete the config from database

        # Delete the replication config (cascade will delete table/column mappings)
        replication_config.delete()
        logger.info(f"Successfully deleted replication config {config_pk}")

        return JsonResponse({
            'success': True,
            'message': 'Configuration deleted successfully'
        })

    except Exception as e:
        logger.error(f"Failed to delete configuration: {e}", exc_info=True)
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
def cdc_config_update(request, config_pk):
    """
    AJAX endpoint: Update replication configuration from modal
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
        data = json.loads(request.body)

        tables_data = data.get('tables', [])
        restart_connector = data.get('restart_connector', False)

        # Update basic configuration if provided
        if 'sync_type' in data:
            replication_config.sync_type = data['sync_type']
        if 'sync_frequency' in data:
            replication_config.sync_frequency = data['sync_frequency']
        if 'auto_create_tables' in data:
            replication_config.auto_create_tables = data['auto_create_tables']
        if 'drop_before_sync' in data:
            replication_config.drop_before_sync = data['drop_before_sync']

        replication_config.save()

        # Track which tables to keep (by source_table name)
        submitted_table_names = {t['source_table'] for t in tables_data}

        # Delete tables that were removed
        replication_config.table_mappings.exclude(source_table__in=submitted_table_names).delete()

        # Track newly added tables for topic creation
        newly_added_tables = []

        # Update or create tables
        for table_data in tables_data:
            source_table = table_data['source_table']
            target_table = table_data['target_table']

            # Ensure target table has database prefix if not already present
            # This maintains consistency: all target tables should be prefixed with source DB name
            db_config = replication_config.client_database
            source_db_name = db_config.database_name
            expected_prefix = f"{source_db_name}_"

            if not target_table.startswith(expected_prefix):
                # Auto-prefix with source database name to avoid collisions
                target_table = f"{source_db_name}_{target_table}"
                logger.info(f"Auto-prefixing target table: {table_data['target_table']} -> {target_table}")

            table_mapping, created = TableMapping.objects.get_or_create(
                replication_config=replication_config,
                source_table=source_table,
                defaults={
                    'target_table': target_table,
                    'is_enabled': table_data.get('is_enabled', True),
                    'sync_type': table_data.get('sync_type', ''),  # Empty = inherit
                    'sync_frequency': table_data.get('sync_frequency', ''),  # Empty = inherit
                }
            )

            # Track newly created tables that need topics
            if created and table_data.get('is_enabled', True):
                newly_added_tables.append(source_table)

            if not created:
                # Update existing table - ensure prefix is maintained
                table_mapping.target_table = target_table
                table_mapping.is_enabled = table_data.get('is_enabled', True)
                table_mapping.sync_type = table_data.get('sync_type', '')
                table_mapping.sync_frequency = table_data.get('sync_frequency', '')
                table_mapping.save()

            # Track which columns to keep
            submitted_column_names = {c['source_column'] for c in table_data.get('columns', [])}

            # Delete columns that were removed
            table_mapping.column_mappings.exclude(source_column__in=submitted_column_names).delete()

            # Update or create columns
            for column_data in table_data.get('columns', []):
                col_mapping, col_created = ColumnMapping.objects.get_or_create(
                    table_mapping=table_mapping,
                    source_column=column_data['source_column'],
                    defaults={
                        'target_column': column_data['target_column'],
                        'source_type': column_data.get('data_type', ''),
                        'target_type': column_data.get('data_type', ''),
                        'is_enabled': column_data.get('is_enabled', True),
                    }
                )

                if not col_created:
                    # Update existing column
                    col_mapping.target_column = column_data['target_column']
                    col_mapping.is_enabled = column_data.get('is_enabled', True)
                    col_mapping.save()

        # Create Kafka topics for newly added tables
        # CRITICAL: New tables are useless without topics
        if newly_added_tables:
            from client.utils.kafka_topic_manager import KafkaTopicManager
            topic_manager = KafkaTopicManager()

            # Use saved topic prefix or derive from client and database IDs
            db_config = replication_config.client_database
            topic_prefix = replication_config.kafka_topic_prefix or f"client_{db_config.client.id}_db_{db_config.id}"

            topic_results = topic_manager.create_cdc_topics_for_tables(
                server_name=topic_prefix,
                database=db_config.database_name,
                table_names=newly_added_tables
            )

            # Verify all new topics exist
            topics_missing = []
            for table in newly_added_tables:
                topic_name = f"{topic_prefix}.{replication_config.client_database.database_name}.{table}"
                if not topic_manager.topic_exists(topic_name):
                    topics_missing.append(topic_name)

            # If any topics are missing, fail the update
            if topics_missing:
                error_msg = (
                    f"Failed to create Kafka topics for new tables: {', '.join(topics_missing)}. "
                    f"Update aborted. Please check Kafka broker status and retry."
                )
                raise Exception(error_msg)

            # Log success
            successful_topics = [t for t, success in topic_results.items() if success]
            if successful_topics:
                logger.info(f"✅ Created {len(successful_topics)} Kafka topics for newly added tables")
            elif newly_added_tables:
                logger.info(f"ℹ️  {len(newly_added_tables)} topics already existed (reusing existing topics)")

        # Create target tables for newly added tables if auto_create_tables is enabled
        if newly_added_tables and replication_config.auto_create_tables:
            try:
                from client.utils.table_creator import create_target_tables
                logger.info(f"🔨 Creating target tables for {len(newly_added_tables)} newly added tables...")
                create_target_tables(replication_config)
                logger.info(f"✅ Successfully created target tables for newly added tables")
            except Exception as e:
                logger.error(f"❌ Failed to create target tables: {e}", exc_info=True)
                # Don't fail the entire update, just log the error
                # User can manually create tables or retry
                return JsonResponse({
                    'success': False,
                    'error': f'Failed to create target tables: {str(e)}'
                }, status=500)

        # CRITICAL: Trigger incremental snapshot for newly added tables
        # This is REQUIRED because Debezium doesn't automatically snapshot new tables
        # The snapshot must be triggered via the signaling mechanism
        if newly_added_tables:
            try:
                from client.utils.debezium_snapshot import trigger_incremental_snapshot
                logger.info(f"📸 Triggering incremental snapshot for {len(newly_added_tables)} new tables: {newly_added_tables}")

                success = trigger_incremental_snapshot(replication_config, newly_added_tables)

                if success:
                    logger.info(f"✅ Incremental snapshot signal sent successfully!")
                    logger.info(f"   The snapshot will start automatically and data will flow to Kafka")
                else:
                    logger.error(f"❌ Failed to trigger incremental snapshot")
                    logger.warning(f"⚠️ MANUAL ACTION REQUIRED: New tables will NOT have initial data!")
                    logger.warning(f"   Run: python manage.py trigger_snapshot --config-id={replication_config.id}")
            except Exception as e:
                logger.error(f"❌ Error triggering incremental snapshot: {e}", exc_info=True)
                logger.warning(f"⚠️ MANUAL ACTION REQUIRED: Newly added tables will NOT get initial snapshot!")
                # Don't fail the entire update - user can manually trigger snapshot later

        # Update connector configuration if it exists
        if replication_config.connector_name:
            try:
                manager = DebeziumConnectorManager()

                # Get ALL enabled tables (not just newly added ones)
                all_enabled_tables = replication_config.table_mappings.filter(is_enabled=True)
                tables_list = [tm.source_table for tm in all_enabled_tables]

                if tables_list:
                    # Generate updated connector config with ALL tables
                    logger.info(f"🔄 Updating connector config with {len(tables_list)} tables: {tables_list}")

                    client = replication_config.client_database.client
                    db_config = replication_config.client_database

                    # Generate full connector configuration
                    updated_config = get_connector_config_for_database(
                        db_config=db_config,
                        replication_config=replication_config,
                        tables_whitelist=tables_list,
                        kafka_bootstrap_servers='kafka:29092',
                        schema_registry_url='http://schema-registry:8081'
                    )

                    if updated_config:
                        # Update the connector configuration
                        success, error = manager.update_connector_config(
                            replication_config.connector_name,
                            updated_config
                        )

                        if success:
                            logger.info(f"✅ Successfully updated connector configuration")

                            # Only restart if requested
                            if restart_connector:
                                logger.info(f"🔄 Restarting connector to apply changes...")
                                success, error = manager.restart_connector(replication_config.connector_name)
                                if not success:
                                    logger.warning(f"⚠️ Failed to restart connector: {error}")
                                else:
                                    logger.info(f"✅ Connector restarted successfully")
                        else:
                            logger.error(f"❌ Failed to update connector config: {error}")
                            return JsonResponse({
                                'success': False,
                                'error': f'Failed to update connector configuration: {error}'
                            }, status=500)
                    else:
                        logger.error(f"❌ Failed to generate connector configuration")
                        return JsonResponse({
                            'success': False,
                            'error': 'Failed to generate connector configuration'
                        }, status=500)

            except Exception as e:
                logger.error(f"❌ Error updating connector: {e}", exc_info=True)
                return JsonResponse({
                    'success': False,
                    'error': f'Failed to update connector: {str(e)}'
                }, status=500)

        # CRITICAL: Restart consumer if new tables were added and replication is active
        # The consumer needs to be restarted to subscribe to the new topics
        if newly_added_tables and replication_config.is_active:
            try:
                logger.info(f"🔄 Restarting consumer to subscribe to {len(newly_added_tables)} new topics...")

                # Stop the current consumer
                stop_result = stop_kafka_consumer(replication_config.id)
                if stop_result.get('success'):
                    logger.info(f"✅ Consumer stopped successfully")
                else:
                    logger.warning(f"⚠️ Consumer stop returned: {stop_result}")

                # Wait a moment for consumer to fully stop
                import time
                time.sleep(2)

                # IMPORTANT: Re-activate the config before starting consumer
                # stop_kafka_consumer sets is_active=False, but we want to restart it
                replication_config.refresh_from_db()
                replication_config.is_active = True
                replication_config.status = 'active'
                replication_config.save()
                logger.info(f"✅ Re-activated config for consumer restart")

                # Restart the consumer with updated topic list
                logger.info(f"🚀 Starting consumer with updated topic list...")
                start_kafka_consumer.apply_async(
                    args=[replication_config.id],
                    countdown=3  # Start after 3 seconds
                )
                logger.info(f"✅ Consumer restart scheduled")

            except Exception as e:
                logger.error(f"❌ Error restarting consumer: {e}", exc_info=True)
                # Don't fail the entire update, just warn
                return JsonResponse({
                    'success': True,
                    'message': 'Configuration updated, but failed to restart consumer. Please restart replication manually.',
                    'warning': str(e)
                })

        return JsonResponse({
            'success': True,
            'message': 'Configuration updated successfully'
        })

    except Exception as e:
        logger.error(f"Failed to update configuration: {e}", exc_info=True)
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


