"""
Connector Action Views (AJAX POST endpoints)
- Source connector: pause, resume, restart, restart failed tasks, restart all tasks, sync schedule
- Sink connector: pause, resume, restart, restart failed tasks, restart all tasks
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


def sink_pause(request, config_pk):
    """Pause the sink connector (also defers source TRUNCATEs until resume)."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        if not config.sink_connector_name:
            return JsonResponse({'success': False, 'message': 'No sink connector configured'})
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.pause_sink_connector()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to pause sink: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def sink_resume(request, config_pk):
    """Resume the sink connector and drain any deferred TRUNCATEs."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        if not config.sink_connector_name:
            return JsonResponse({'success': False, 'message': 'No sink connector configured'})
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.resume_sink_connector()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Failed to resume sink: {e}', exc_info=True)
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


def _queue_resync(config, tables):
    """Queue tables for the sequential resync job runner and build the JSON reply."""
    from client.replication.resync import enqueue_resync

    job, added, skipped = enqueue_resync(config, tables, source='manual')
    already = [t for t, why in skipped if why == 'already queued']
    invalid = [f"{t} ({why})" for t, why in skipped if why != 'already queued']

    parts = []
    if added:
        parts.append(
            f"Queued {len(added)} table(s) for resync. They are emptied and refilled "
            f"one at a time in the background."
        )
    if already:
        parts.append(f"Already queued: {', '.join(already)}.")
    if invalid:
        parts.append(f"Skipped: {', '.join(invalid)}.")

    success = bool(added or already)
    return JsonResponse({
        'success': success,
        'message': ' '.join(parts) if success else None,
        'error': None if success else (' '.join(parts) or 'No tables given'),
        'job_id': job.pk if job else None,
        'queued': added,
        'skipped': [{'table': t, 'reason': why} for t, why in skipped],
    })


def resync_tables(request, config_pk):
    """
    AJAX POST: queue tables for a sequential resync (truncate target + re-snapshot).

    Body JSON: {"tables": ["orders", "customers", ...]}

    Returns immediately — the work runs in Celery (client/replication/resync.py),
    one table at a time. Progress is reported by connector_status_api.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        import json
        body = json.loads(request.body or b'{}')
        tables = body.get('tables') or []
        if isinstance(tables, str):
            tables = [tables]
        tables = [str(t).strip() for t in tables if str(t).strip()]
        if not tables:
            return JsonResponse({'success': False, 'error': 'tables required'}, status=400)

        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        return _queue_resync(config, tables)
    except Exception as e:
        logger.error(f'resync_tables failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def resync_table(request, config_pk):
    """
    AJAX POST: queue a single table for resync (kept for backward compatibility).

    Body JSON: {"table": "orders"}
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        import json
        body = json.loads(request.body or b'{}')
        table_name = (body.get('table') or '').strip()
        if not table_name:
            return JsonResponse({'success': False, 'error': 'table name required'}, status=400)

        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        return _queue_resync(config, [table_name])
    except Exception as e:
        logger.error(f'resync_table failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def resync_job_cancel(request, config_pk, job_pk):
    """AJAX POST: skip the not-yet-started tables of a resync job."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        from client.models.replication import TableResyncJob
        from client.replication.resync import cancel_job

        get_object_or_404(TableResyncJob, pk=job_pk, replication_config_id=config_pk)
        success, message = cancel_job(job_pk)
        return JsonResponse({'success': success, 'message': message if success else None,
                             'error': None if success else message})
    except Exception as e:
        logger.error(f'resync_job_cancel failed for job {job_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_recover_binlog(request, config_pk):
    """
    Recover a MySQL connector stuck on a purged binlog offset.

    Temporarily sets snapshot.mode=schema_only_recovery, restarts failed tasks,
    waits for recovery, then reverts snapshot.mode to its original value.
    Safe to call without deleting the connector.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator.recover_binlog_offset()
        return JsonResponse({'success': success, 'message': message})
    except Exception as e:
        logger.error(f'Binlog recovery failed for config {config_pk}: {e}', exc_info=True)
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
        data_schema = body.get('data_schema', '').strip().upper() or None
        # Auto-derive data_schema from existing table mappings if not supplied
        if not data_schema:
            from client.models.replication import TableMapping
            first_mapping = (
                TableMapping.objects
                .filter(replication_config__database=db_config)
                .exclude(source_schema='')
                .values_list('source_schema', flat=True)
                .first()
            )
            if first_mapping:
                data_schema = first_mapping.upper()
        from client.utils.ddl.oracle_setup import apply_oracle_privileges
        result = apply_oracle_privileges(db_config, admin_user, admin_password,
                                         data_schema=data_schema)
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