"""
CDC Connector Views.

Handles Debezium connector operations:
- Create connector
- Connector actions (pause, resume, restart, delete)
"""

import logging
import time
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages

from client.models.replication import ReplicationConfig
from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.connector_templates import get_connector_config_for_database, generate_connector_name
from client.models.database import ClientDatabase
from client.utils.database_utils import get_table_schema


logger = logging.getLogger(__name__)
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

