"""
Connector Monitor Views
- Real-time connector monitor page
- Status API (polled by monitor page)
- Live metrics API (streaming lag, DLQ, config diff)
- Table row counts API
- FK constraint preview and apply APIs
"""

import logging
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from client.models.replication import ReplicationConfig

logger = logging.getLogger(__name__)


# ========================================
# Connector Monitor (Real-time Status + Snapshot Progress)
# ========================================

def connector_monitor(request, config_pk):
    """
    Real-time monitoring page for a connector.
    Shows connector status, snapshot progress via Jolokia, and table mappings.
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    if not replication_config.connector_name:
        messages.info(request, 'No connector created yet for this configuration.')
        return redirect('connector_list', database_pk=database.id)

    try:
        from client.replication.orchestrator import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(replication_config)
        unified_status = orchestrator.get_unified_status()

        source_state = unified_status['source_connector']['state']
        sink_state = unified_status['sink_connector']['state']
        source_exists = source_state not in ('NOT_FOUND', 'ERROR')
        sink_exists = sink_state not in ('NOT_FOUND', 'ERROR', 'NOT_CONFIGURED')

        source_tasks = unified_status['source_connector'].get('tasks', [])
        sink_tasks = unified_status['sink_connector'].get('tasks', [])

        # Connector uptime — most recent active ConnectorHistory entry for source and sink
        from client.models.replication import ConnectorHistory
        connector_history = ConnectorHistory.objects.filter(
            connector_name=replication_config.connector_name,
            connector_type='source',
        ).exclude(status='deleted').order_by('-created_at').first()

        sink_connector_history = ConnectorHistory.objects.filter(
            connector_name=replication_config.sink_connector_name,
            connector_type='sink',
        ).exclude(status='deleted').order_by('-created_at').first() if replication_config.sink_connector_name else None

        target_database = client.client_databases.filter(is_target=True).first()

        has_binlog_error = any(
            t.get('state') == 'FAILED' and 'BinlogOffsetContext' in (t.get('trace') or '')
            for t in source_tasks
        )

        context = {
            'replication_config': replication_config,
            'database': database,
            'target_database': target_database,
            'client': client,
            'unified_status': unified_status,
            'source_exists': source_exists,
            'sink_exists': sink_exists,
            'source_state': source_state,
            'sink_state': sink_state,
            'source_tasks': source_tasks,
            'sink_tasks': sink_tasks,
            'snapshot': unified_status.get('snapshot'),
            'enabled_table_mappings': replication_config.table_mappings.filter(is_enabled=True).order_by('source_table'),
            'connector_history': connector_history,
            'sink_connector_history': sink_connector_history,
            'has_binlog_error': has_binlog_error,
            'resync_job': _safe_job_summary(replication_config.pk),
        }

        return render(request, 'client/connectors/connector_monitor.html', context)

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        messages.error(request, f'Failed to get connector status: {str(e)}')
        return redirect('connector_list', database_pk=database.id)


def _safe_job_summary(config_pk):
    try:
        from client.replication.resync import job_summary
        return job_summary(config_pk)
    except Exception as e:
        logger.warning(f'Resync job summary failed for config {config_pk}: {e}')
        return None


def connector_status_api(request, config_pk):
    """
    AJAX endpoint returning unified status JSON including snapshot progress.
    Polled by the monitor page for live updates.
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        from client.replication.orchestrator import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(replication_config)
        unified_status = orchestrator.get_unified_status()

        resync_job = _safe_job_summary(replication_config.pk)

        return JsonResponse({'success': True, 'status': unified_status, 'resync_job': resync_job})

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_table_rows_api(request, config_pk):
    """
    AJAX endpoint returning row counts for all enabled tables (source + target).

    Counts are NOT run inline: this returns the last result computed by the
    ``refresh_row_counts`` Celery task (see client/utils/row_counts.py) and
    enqueues a refresh when there is none yet or when ``?refresh=1`` is passed.
    ``refreshing`` tells the page to re-fetch shortly.
    """
    from client.utils import row_counts

    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        force = request.GET.get('refresh') == '1'

        payload = row_counts.get_cached(config.pk)
        if force or payload is None:
            row_counts.request_refresh(config.pk, force=force)

        payload = payload or {}

        # Always list every enabled mapping (the page builds its resync checkboxes from
        # this list); overlay cached counts where we have them.
        cached_by_table = {t.get('source_table'): t for t in payload.get('tables', [])}
        rows = []
        for m in config.table_mappings.filter(is_enabled=True).order_by('source_table').values('source_table', 'target_table'):
            cached = cached_by_table.get(m['source_table'])
            if cached:
                rows.append({**cached, 'target_table': m['target_table']})
            else:
                rows.append({
                    'source_table': m['source_table'],
                    'target_table': m['target_table'],
                    'source_rows': None,
                    'target_rows': None,
                    'source_error': 'pending',
                    'target_error': 'pending',
                })

        return JsonResponse({
            'success': True,
            'status': payload.get('status', 'pending'),
            'tables': rows,
            'source_reachable': payload.get('source_reachable', True),
            'target_reachable': payload.get('target_reachable', True),
            'has_target_db': payload.get('has_target_db', True),
            'as_of': payload.get('as_of'),
            'refreshing': row_counts.is_refresh_pending(config.pk),
        })

    except Exception as e:
        logger.error(f'Failed to get table row counts: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_dashboard_card_api(request, config_pk):
    """
    Combined AJAX endpoint for monitoring dashboard card updates.
    Returns connector card health (state, tasks) + row-level sync status.

    Polled by the monitoring dashboard, so it must stay cheap:
      * connector state comes from the Redis status cache (refreshed by beat),
      * row-sync status comes from the cached background row counts — a refresh
        is only *requested* when the cached result is stale.
    """
    from client.utils import row_counts
    from client.utils.connector_status_cache import get_statuses
    from client.replication.resync import has_active_job

    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)

        # ── 1. Connector + sink state from the status cache ────────────────
        names = [n for n in (config.connector_name, config.sink_connector_name) if n]
        try:
            statuses = get_statuses(names)
            status_error = ''
        except Exception as e:
            statuses = {}
            status_error = str(e)

        status_data = statuses.get(config.connector_name) if config.connector_name else None
        if status_data:
            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = status_data.get('tasks', [])
            has_failed_task = any(t.get('state') == 'FAILED' for t in tasks)
            card = {
                'connector_state': connector_state,
                'is_healthy': connector_state in ('RUNNING', 'PAUSED') and not has_failed_task,
                'error_count': sum(1 for t in tasks if t.get('state') == 'FAILED'),
                'tasks': tasks,
                'connector_trace': status_data.get('connector', {}).get('trace', ''),
                'error_message': '',
            }
        else:
            card = {
                'connector_state': 'ERROR' if status_error else 'NOT_FOUND',
                'is_healthy': False,
                'error_count': 1,
                'tasks': [],
                'connector_trace': '',
                'error_message': status_error,
            }

        sink_state = 'NOT_CONFIGURED'
        if config.sink_connector_name:
            s_data = statuses.get(config.sink_connector_name)
            sink_state = s_data.get('connector', {}).get('state', 'UNKNOWN') if s_data else 'NOT_FOUND'
        card['sink_state'] = sink_state

        # ── 2. Row-sync status (cached; never counted inline) ──────────────
        if has_active_job(config.pk):
            sync = {'status': row_counts.STATUS_SNAPSHOT}
        else:
            payload = row_counts.get_cached(config.pk)
            age = row_counts.age_seconds(payload)
            # A cached "snapshot running" goes stale quickly; real counts after 5 min.
            max_age = 60 if (payload or {}).get('status') == row_counts.STATUS_SNAPSHOT else row_counts.DASHBOARD_MAX_AGE_S
            if payload is None or age is None or age > max_age:
                row_counts.request_refresh(config.pk)

            sync = row_counts.summarize_sync(payload) or {'status': 'checking'}
            if sync.get('status') in ('rows_in_sync', 'not_synced'):
                sync['last_fully_synced'] = config.last_success_at.isoformat() if config.last_success_at else None

        return JsonResponse({'success': True, 'card': card, 'sync': sync})

    except Exception as e:
        logger.error(f'Dashboard card API failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_fk_preview_api(request, config_pk):
    """
    AJAX GET endpoint returning a preview of FK constraints for the monitor page.
    Read-only — does not create anything.
    """
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.utils.table_creator import preview_foreign_keys
        data = preview_foreign_keys(config)
        return JsonResponse({'success': True, **data})
    except Exception as e:
        logger.error(f'FK preview failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_fk_apply_api(request, config_pk):
    """
    AJAX POST endpoint that applies FK constraints and indexes to the target database.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.utils.table_creator import add_foreign_keys_to_target, add_indexes_to_target
        created, skipped, errors = add_foreign_keys_to_target(config)
        idx_created, idx_skipped, idx_errors = add_indexes_to_target(config)
        return JsonResponse({
            'success': True,
            'created': created,
            'skipped': skipped,
            'errors': errors,
            'indexes_created': idx_created,
            'indexes_skipped': idx_skipped,
            'index_errors': idx_errors,
        })
    except Exception as e:
        logger.error(f'FK apply failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def database_fk_preview_api(request, database_pk):
    """
    AJAX GET endpoint returning a preview of FK constraints aggregated across
    ALL source connectors for a given target database (ClientDatabase).
    Read-only — does not create anything.
    """
    try:
        from client.models.database import ClientDatabase
        database = get_object_or_404(ClientDatabase, pk=database_pk)
        configs = list(database.get_source_connectors())

        from client.utils.table_creator import preview_foreign_keys

        aggregate = {
            'has_any_fks': False,
            'summary': {
                'tables_total': 0,
                'tables_created': 0,
                'tables_missing': 0,
                'fk_will_create': 0,
                'fk_already_exists': 0,
                'fk_cannot_create': 0,
                'fk_table_not_ready': 0,
            },
            'tables': [],
        }

        for config in configs:
            result = preview_foreign_keys(config)
            if result.get('has_any_fks'):
                aggregate['has_any_fks'] = True
            for key in aggregate['summary']:
                aggregate['summary'][key] += result.get('summary', {}).get(key, 0)
            for table in result.get('tables', []):
                table['connector_name'] = config.connector_name
                aggregate['tables'].append(table)

        return JsonResponse({'success': True, **aggregate})
    except Exception as e:
        logger.error(f'DB FK preview failed for database {database_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def database_fk_apply_api(request, database_pk):
    """
    AJAX POST endpoint that applies FK constraints across ALL source connectors
    for a given target database (ClientDatabase).
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        from client.models.database import ClientDatabase
        database = get_object_or_404(ClientDatabase, pk=database_pk)
        configs = list(database.get_source_connectors())

        from client.utils.table_creator import add_foreign_keys_to_target

        total_created = 0
        total_skipped = 0
        all_errors = []

        from client.utils.table_creator import add_indexes_to_target
        total_idx_created = 0
        total_idx_skipped = 0
        all_idx_errors = []

        for config in configs:
            created, skipped, errors = add_foreign_keys_to_target(config)
            total_created += created
            total_skipped += skipped
            all_errors.extend(errors)
            idx_created, idx_skipped, idx_errors = add_indexes_to_target(config)
            total_idx_created += idx_created
            total_idx_skipped += idx_skipped
            all_idx_errors.extend(idx_errors)

        return JsonResponse({
            'success': True,
            'created': total_created,
            'skipped': total_skipped,
            'errors': all_errors,
            'indexes_created': total_idx_created,
            'indexes_skipped': total_idx_skipped,
            'index_errors': all_idx_errors,
        })
    except Exception as e:
        logger.error(f'DB FK apply failed for database {database_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def database_index_preview_api(request, database_pk):
    """
    AJAX GET — preview indexes across ALL source connectors for a target database.
    Read-only; does not create anything.
    """
    try:
        from client.models.database import ClientDatabase
        database = get_object_or_404(ClientDatabase, pk=database_pk)
        configs = list(database.get_source_connectors())

        from client.utils.table_creator import preview_indexes

        aggregate = {
            'has_any_indexes': False,
            'summary': {
                'tables_total': 0,
                'tables_created': 0,
                'tables_missing': 0,
                'idx_will_create': 0,
                'idx_already_exists': 0,
                'idx_table_not_ready': 0,
            },
            'tables': [],
        }

        for config in configs:
            result = preview_indexes(config)
            if result.get('has_any_indexes'):
                aggregate['has_any_indexes'] = True
            for key in aggregate['summary']:
                aggregate['summary'][key] += result.get('summary', {}).get(key, 0)
            for table in result.get('tables', []):
                table['connector_name'] = config.connector_name
                aggregate['tables'].append(table)

        return JsonResponse({'success': True, **aggregate})
    except Exception as e:
        logger.error(f'DB index preview failed for database {database_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def database_index_apply_api(request, database_pk):
    """
    AJAX POST — apply indexes across ALL source connectors for a target database.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        from client.models.database import ClientDatabase
        database = get_object_or_404(ClientDatabase, pk=database_pk)
        configs = list(database.get_source_connectors())

        from client.utils.table_creator import add_indexes_to_target

        total_created = 0
        total_skipped = 0
        all_errors = []

        for config in configs:
            created, skipped, errors = add_indexes_to_target(config)
            total_created += created
            total_skipped += skipped
            all_errors.extend(errors)

        return JsonResponse({
            'success': True,
            'created': total_created,
            'skipped': total_skipped,
            'errors': all_errors,
        })
    except Exception as e:
        logger.error(f'DB index apply failed for database {database_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)