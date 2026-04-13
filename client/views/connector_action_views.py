"""
Connector Action Views (AJAX POST endpoints)
- Source connector: pause, resume, restart, restart failed tasks, restart all tasks, sync schedule
- Sink connector: restart, restart failed tasks, restart all tasks
"""

import logging
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from client.models.database import ClientDatabase
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


def resync_table(request, config_pk):
    """
    AJAX POST: Manually resync a single table after a source TRUNCATE + repopulate.

    Used for PostgreSQL and SQL Server where TRUNCATE is not auto-detected.
    MySQL and Oracle handle this automatically via the DDL processor.

    Body JSON: {"table": "orders"}
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        import json
        body = json.loads(request.body)
        table_name = body.get('table', '').strip()
        if not table_name:
            return JsonResponse({'success': False, 'error': 'table name required'}, status=400)

        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.resync_table(table_name)
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'resync_table failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def check_oracle_privileges_api(request, database_pk):
    """
    AJAX GET: Check Oracle privileges for LogMiner CDC.
    Returns privilege status for the connector_add form.
    """
    if request.method != 'GET':
        return JsonResponse({'error': 'GET required'}, status=405)
    try:
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        from client.utils.ddl.oracle_setup import check_oracle_privileges
        result = check_oracle_privileges(db_config)
        result['is_cdb_user'] = db_config.username.upper().startswith('C##')
        return JsonResponse(result)
    except Exception as e:
        logger.error(f'Oracle privilege check error: {e}', exc_info=True)
        return JsonResponse({'error': str(e)}, status=500)


def apply_oracle_privileges_api(request, database_pk):
    """
    AJAX POST: Connect as SYSDBA with provided admin credentials and apply all
    missing Debezium LogMiner privileges to the connector user.

    Body JSON: { "admin_user": "sys", "admin_password": "..." }
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST required'}, status=405)
    try:
        import json
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        body = json.loads(request.body)
        admin_user = body.get('admin_user', '').strip()
        admin_password = body.get('admin_password', '')
        if not admin_user or not admin_password:
            return JsonResponse({'error': 'admin_user and admin_password are required'}, status=400)
        from client.utils.ddl.oracle_setup import apply_oracle_privileges
        result = apply_oracle_privileges(db_config, admin_user, admin_password)
        return JsonResponse(result)
    except Exception as e:
        logger.error(f'Oracle apply privileges error: {e}', exc_info=True)
        return JsonResponse({'error': str(e)}, status=500)


def check_postgresql_privileges_api(request, database_pk):
    """
    AJAX GET: Check PostgreSQL privileges for CDC + DDL capture.
    Returns privilege status for the connector_add form.
    """
    if request.method != 'GET':
        return JsonResponse({'error': 'GET required'}, status=405)
    try:
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        from client.utils.ddl.postgres_setup import check_postgresql_privileges
        result = check_postgresql_privileges(db_config)
        return JsonResponse(result)
    except Exception as e:
        logger.error(f'Privilege check error: {e}', exc_info=True)
        return JsonResponse({'error': str(e)}, status=500)