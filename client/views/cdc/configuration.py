"""
CDC Configuration Views.

Handles configuration management:
- Configure tables workflow
- Edit configuration
- Delete configuration  
- Configuration details
"""

import logging
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db import transaction
from sqlalchemy import text
import time

from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.database_utils import (
    get_table_schema,
    get_table_list,
    get_database_engine
)
from client.utils.debezium_manager import DebeziumConnectorManager
from client.replication.orchestrator import ReplicationOrchestrator
from .replication import wait_for_connector_running


logger = logging.getLogger(__name__)

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
    Edit replication configuration - OPTIMIZED FOR READ-ONLY CONNECTORS
    - Enable/disable tables
    - Rename target tables
    - Enable/disable columns
    - Change sync settings
    - Restart connector when tables are added/removed (no incremental snapshots)
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
                # CONNECTOR UPDATE - READ-ONLY MODE (RESTART-BASED)
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
                                # ================================================================
                                # PROCESS TABLE CHANGES (READ-ONLY APPROACH)
                                # ================================================================
                                if newly_added_tables or removed_table_names:
                                    logger.info(f"🔧 READ-ONLY MODE: Updating connector via restart")
                                    logger.info(f"   Added: {len(newly_added_tables)} tables")
                                    logger.info(f"   Removed: {len(removed_table_names)} tables")

                                    try:
                                        # ================================================================
                                        # STEP 1: Update connector configuration
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
                                        logger.info(f"📋 Updated table.include.list: {current_config['table.include.list']}")

                                        # Update connector config
                                        update_success, update_error = manager.update_connector_config(
                                            replication_config.connector_name,
                                            current_config
                                        )

                                        if not update_success:
                                            raise Exception(f"Failed to update config: {update_error}")

                                        logger.info(f"✅ Connector config updated")

                                        # ================================================================
                                        # STEP 2: POSTGRESQL-SPECIFIC - Manage publication and clear slot
                                        # ================================================================
                                        if db_config.db_type.lower() == 'postgresql':
                                            logger.info(f"🔧 PostgreSQL READ-ONLY MODE: Managing publication...")
                                            
                                            # Update PostgreSQL publication with new table list
                                            pub_success, pub_message = manage_postgresql_publication(
                                                db_config, 
                                                replication_config, 
                                                tables_list
                                            )
                                            
                                            if not pub_success:
                                                raise Exception(f"Publication management failed: {pub_message}")
                                            
                                            logger.info(f"✅ {pub_message}")
                                            
                                            # Clear replication slot connections before restart
                                            client_id = db_config.client.id
                                            db_id = db_config.id
                                            slot_name = f"debezium_{client_id}_{db_id}"
                                            
                                            logger.info(f"🔍 Clearing replication slot: {slot_name}")
                                            slot_status = get_replication_slot_status(db_config, slot_name)
                                            
                                            if slot_status and slot_status['active'] and slot_status['active_pid']:
                                                logger.warning(f"⚠️  Terminating active slot connection (PID: {slot_status['active_pid']})")
                                                term_success, term_msg = terminate_active_slot_connections(db_config, slot_name)
                                                
                                                if term_success:
                                                    logger.info(f"✅ {term_msg}")
                                                    time.sleep(2)  # Wait for cleanup
                                                else:
                                                    logger.warning(f"⚠️  {term_msg}")

                                        # ================================================================
                                        # STEP 3: RESTART CONNECTOR (Required for read-only mode)
                                        # ================================================================
                                        logger.info(f"🔄 Restarting connector to apply table changes...")
                                        restart_success, restart_error = manager.restart_connector(
                                            replication_config.connector_name
                                        )
                                        
                                        if not restart_success:
                                            raise Exception(f"Failed to restart connector: {restart_error}")
                                        
                                        logger.info(f"✅ Connector restarted successfully")

                                        # ================================================================
                                        # STEP 4: Wait for connector to stabilize
                                        # ================================================================
                                        logger.info(f"⏳ Waiting for connector to stabilize...")
                                        wait_success = wait_for_connector_running(
                                            manager, 
                                            replication_config.connector_name,
                                            timeout=90,
                                            interval=3
                                        )
                                        
                                        if not wait_success:
                                            logger.warning(f"⚠️  Connector did not fully stabilize within timeout")
                                            messages.warning(
                                                request,
                                                '⚠️ Configuration updated. Connector is restarting - check status in a moment.'
                                            )
                                        else:
                                            logger.info(f"✅ Connector verified as RUNNING")
                                            
                                            if newly_added_tables:
                                                messages.success(
                                                    request,
                                                    f'✅ Added {len(newly_added_tables)} table(s)! Connector restarted - initial snapshot in progress.'
                                                )
                                            
                                            if removed_table_names:
                                                messages.success(
                                                    request,
                                                    f'✅ Removed {len(removed_table_names)} table(s)! Connector restarted.'
                                                )

                                        # ================================================================
                                        # STEP 5: Restart consumer to subscribe to new topics
                                        # ================================================================
                                        if newly_added_tables and replication_config.is_active:
                                            try:
                                                from client.tasks import stop_kafka_consumer, start_kafka_consumer
                                                logger.info(f"🔄 Restarting consumer for new topics...")

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
                                                messages.warning(request, '⚠️ Consumer restart failed - manual restart may be needed')

                                    except Exception as e:
                                        logger.error(f"❌ Error updating connector: {e}", exc_info=True)
                                        messages.error(request, f'❌ Failed to update connector: {str(e)}')
                                        
                                elif restart_connector:
                                    # No table changes, just restart if requested
                                    logger.info(f"🔄 Manual connector restart requested...")
                                    
                                    # For PostgreSQL, clear slot before restart
                                    if db_config.db_type.lower() == 'postgresql':
                                        client_id = db_config.client.id
                                        db_id = db_config.id
                                        slot_name = f"debezium_{client_id}_{db_id}"
                                        logger.info(f"🔍 Clearing replication slot: {slot_name}")
                                        terminate_active_slot_connections(db_config, slot_name)
                                        time.sleep(2)
                                    
                                    success, error = manager.restart_connector(replication_config.connector_name)
                                    if success:
                                        messages.success(request, '✅ Connector restarted successfully!')
                                    else:
                                        messages.warning(request, f'⚠️ Restart failed: {error}')

                    except Exception as e:
                        logger.error(f"❌ Error with connector: {e}", exc_info=True)
                        messages.warning(request, f'⚠️ Configuration saved, but connector update failed: {str(e)}')

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



