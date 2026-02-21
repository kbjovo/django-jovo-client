"""
Connector Action Views (AJAX POST endpoints)
- Source connector: pause, resume, restart, restart failed tasks, restart all tasks, sync schedule
- Sink connector: restart, restart failed tasks, restart all tasks
"""

import logging
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from client.models.replication import ReplicationConfig
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

logger = logging.getLogger(__name__)


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