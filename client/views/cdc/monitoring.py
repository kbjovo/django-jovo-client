"""
CDC Monitoring Views.

Handles monitoring and status endpoints:
- Monitor connector
- Unified status  
- Table schema AJAX endpoints
"""

import logging
import json
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig
from client.utils.database_utils import get_table_schema
from client.replication import ReplicationOrchestrator

logger = logging.getLogger(__name__)
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

