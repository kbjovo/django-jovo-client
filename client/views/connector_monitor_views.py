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
from client.utils.database_utils import get_row_count

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
        }

        return render(request, 'client/connectors/connector_monitor.html', context)

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        messages.error(request, f'Failed to get connector status: {str(e)}')
        return redirect('connector_list', database_pk=database.id)


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

        return JsonResponse({'success': True, 'status': unified_status})

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_live_metrics_api(request, config_pk):
    """
    AJAX endpoint for live connector metrics, polled every ~10s by the monitor page.
    Returns:
      - streaming: Jolokia JMX streaming metrics (lag, throughput, queue utilisation)
      - config_diff: Live connector config vs stored model values
    """
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        database = config.client_database
        result = {}

        # ── 1. Streaming metrics via Jolokia ───────────────────────────────
        try:
            from jovoclient.utils.debezium.jolokia_client import JolokiaClient
            jolokia = JolokiaClient()
            raw = jolokia.get_streaming_metrics(database.db_type, config.kafka_topic_prefix or '')
            if raw:
                lag_ms = raw.get('MilliSecondsBehindSource', 0) or 0
                queue_total = raw.get('QueueTotalCapacity', 0) or 0
                queue_remaining = raw.get('QueueRemainingCapacity', 0) or 0
                queue_used = queue_total - queue_remaining
                queue_pct = round((queue_used / queue_total) * 100) if queue_total else 0
                ms_since_last = raw.get('MilliSecondsSinceLastEvent', 0) or 0
                result['streaming'] = {
                    'available': True,
                    'lag_ms': lag_ms,
                    'lag_display': f"{lag_ms:,} ms" if lag_ms < 60000 else f"{lag_ms // 1000:,} s",
                    'total_events': raw.get('TotalNumberOfEventsSeen', 0),
                    'filtered_events': raw.get('NumberOfEventsFiltered', 0),
                    'queue_total': queue_total,
                    'queue_used': queue_used,
                    'queue_pct': queue_pct,
                    'ms_since_last_event': ms_since_last,
                    'idle_display': f"{ms_since_last // 1000}s ago" if ms_since_last < 3600000 else "over 1h ago",
                }
            else:
                result['streaming'] = {'available': False}
        except Exception as e:
            result['streaming'] = {'available': False, 'error': str(e)}

        # ── 2. Live config diff (Kafka Connect REST vs stored model) ───────
        try:
            from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
            manager = DebeziumConnectorManager()
            live_config = manager.get_connector_config(config.connector_name)
            if live_config:
                TRACKED = {
                    'max.queue.size': ('max_queue_size', int),
                    'max.batch.size': ('max_batch_size', int),
                    'poll.interval.ms': ('poll_interval_ms', int),
                    'snapshot.fetch.size': ('snapshot_fetch_size', int),
                }
                diffs = []
                for live_key, (model_attr, cast) in TRACKED.items():
                    live_val = live_config.get(live_key)
                    stored_val = getattr(config, model_attr, None)
                    if live_val is not None and stored_val is not None:
                        if cast(live_val) != stored_val:
                            diffs.append({
                                'key': live_key,
                                'live': live_val,
                                'stored': str(stored_val),
                            })
                result['config_diff'] = {
                    'available': True,
                    'diffs': diffs,
                    'in_sync': len(diffs) == 0,
                }
            else:
                result['config_diff'] = {'available': False}
        except Exception as e:
            result['config_diff'] = {'available': False, 'error': str(e)}

        return JsonResponse({'success': True, **result})

    except Exception as e:
        logger.error(f'Failed to get live metrics: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_table_rows_api(request, config_pk):
    """
    AJAX endpoint returning row counts for all enabled tables
    from both source and target databases.
    Opens one connection per database and runs all queries through it.
    """
    from client.utils.database_utils import get_database_connection
    from sqlalchemy import text as sa_text

    def _count_query(db_config, table_name, schema=None):
        """Build a COUNT(*) query string for the given db type."""
        db_type = db_config.db_type.lower()
        if db_type == 'mssql':
            if '.' in table_name:
                schema_name, tbl = table_name.split('.', 1)
            else:
                schema_name, tbl = (schema or 'dbo'), table_name
            return f"SELECT COUNT(*) FROM [{schema_name}].[{tbl}]"
        elif db_type == 'postgresql':
            if '.' in table_name:
                schema_name, tbl = table_name.rsplit('.', 1)
            else:
                schema_name, tbl = (schema or 'public'), table_name
            return f'SELECT COUNT(*) FROM "{schema_name}"."{tbl}"'
        elif db_type == 'oracle':
            if '.' in table_name:
                schema_name, tbl = table_name.rsplit('.', 1)
                schema_name, tbl = schema_name.upper(), tbl.upper()
            else:
                schema_name = (schema or db_config.username).upper()
                tbl = table_name.upper()
            return f"SELECT COUNT(*) FROM {schema_name}.{tbl}"
        else:  # mysql and generic
            return f"SELECT COUNT(*) FROM `{table_name}`"

    def _is_not_found_error(err_str):
        keywords = ("doesn't exist", 'not exist', 'not found', 'unknown table', 'invalid object')
        return any(k in err_str for k in keywords)

    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        source_db = config.client_database
        target_db = source_db.client.get_target_database()

        mappings = list(config.table_mappings.filter(is_enabled=True).order_by('source_table'))

        # ── Source counts (single connection for all tables) ─────────────────
        source_reachable = True
        source_counts = {}   # mapping.pk → (count, error)
        try:
            with get_database_connection(source_db, connect_timeout=5) as conn:
                for mapping in mappings:
                    try:
                        q = _count_query(source_db, mapping.source_table, schema=mapping.source_schema)
                        row = conn.execute(sa_text(q)).fetchone()
                        source_counts[mapping.pk] = (row[0] if row else 0, None)
                    except Exception as e:
                        err_str = str(e).lower()
                        error = 'table_not_found' if _is_not_found_error(err_str) else 'error'
                        source_counts[mapping.pk] = (None, error)
                        logger.warning(f'Source row count failed for {mapping.source_table}: {e}')
        except Exception as e:
            source_reachable = False
            logger.warning(f'Source database unreachable ({source_db.connection_name}): {e}')

        # ── Target counts (single connection for all tables) ─────────────────
        target_reachable = True
        target_counts = {}   # mapping.pk → (count, error)
        if target_db:
            try:
                with get_database_connection(target_db, connect_timeout=5) as conn:
                    for mapping in mappings:
                        try:
                            q = _count_query(target_db, mapping.target_table, schema=mapping.target_schema or None)
                            row = conn.execute(sa_text(q)).fetchone()
                            target_counts[mapping.pk] = (row[0] if row else 0, None)
                        except Exception as e:
                            err_str = str(e).lower()
                            if _is_not_found_error(err_str):
                                target_counts[mapping.pk] = (None, 'table_not_found')
                                logger.debug(f'Target table not yet created: {mapping.target_table}')
                            else:
                                target_counts[mapping.pk] = (None, 'error')
                                logger.warning(f'Target row count failed for {mapping.target_table}: {e}')
            except Exception as e:
                target_reachable = False
                logger.warning(f'Target database unreachable ({target_db.connection_name}): {e}')

        # ── Build response rows ───────────────────────────────────────────────
        rows = []
        for mapping in mappings:
            if not source_reachable:
                src_count, src_error = None, 'db_unreachable'
            else:
                src_count, src_error = source_counts.get(mapping.pk, (None, 'error'))

            if not target_db:
                tgt_count, tgt_error = None, 'no_target_db'
            elif not target_reachable:
                tgt_count, tgt_error = None, 'db_unreachable'
            else:
                tgt_count, tgt_error = target_counts.get(mapping.pk, (None, 'error'))

            rows.append({
                'source_table': mapping.source_table,
                'target_table': mapping.target_table,
                'source_rows': src_count,
                'target_rows': tgt_count,
                'source_error': src_error,
                'target_error': tgt_error,
            })

        return JsonResponse({
            'success': True,
            'tables': rows,
            'source_reachable': source_reachable,
            'target_reachable': target_reachable,
            'has_target_db': target_db is not None,
        })

    except Exception as e:
        logger.error(f'Failed to get table row counts: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def connector_dashboard_card_api(request, config_pk):
    """
    Combined AJAX endpoint for monitoring dashboard card updates.
    Returns connector card health (state, tasks) + row-level sync status.
    Polled every 15s per card by the monitoring dashboard.
    """
    from django.utils import timezone
    from datetime import timedelta
    from client.utils.database_utils import get_database_connection
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)

        # ── 1. Connector state via Debezium ────────────────────────────────
        try:
            manager = DebeziumConnectorManager()
            exists, status_data = manager.get_connector_status(config.connector_name)
            if exists and status_data:
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
                    'connector_state': 'NOT_FOUND',
                    'is_healthy': False,
                    'error_count': 1,
                    'tasks': [],
                    'connector_trace': '',
                    'error_message': '',
                }
        except Exception as e:
            card = {
                'connector_state': 'ERROR',
                'is_healthy': False,
                'error_count': 1,
                'tasks': [],
                'connector_trace': '',
                'error_message': str(e),
            }

        connector_state = card['connector_state']

        # ── 2. Snapshot detection (only connectors created < 24h ago) ──────
        is_new = (timezone.now() - config.created_at) < timedelta(hours=24)
        if is_new:
            try:
                from client.replication.orchestrator import ReplicationOrchestrator
                orchestrator = ReplicationOrchestrator(config)
                snapshot = orchestrator._get_snapshot_progress()
                if snapshot and snapshot.get('running'):
                    return JsonResponse({
                        'success': True,
                        'card': card,
                        'sync': {'status': 'snapshot_in_progress'},
                    })
            except Exception:
                pass

        # ── 3. Row count sync status ───────────────────────────────────────
        try:
            source_db = config.client_database
            target_db = source_db.client.get_target_database()

            source_reachable = True
            try:
                with get_database_connection(source_db):
                    pass
            except Exception:
                source_reachable = False

            target_reachable = True
            if target_db:
                try:
                    with get_database_connection(target_db):
                        pass
                except Exception:
                    target_reachable = False

            if not source_reachable or not target_db or not target_reachable:
                sync = {'status': 'unable_to_check'}
            else:
                mappings = config.table_mappings.filter(is_enabled=True)
                total_count = mappings.count()

                if total_count == 0:
                    sync = {'status': 'unable_to_check'}
                else:
                    in_sync_count = 0
                    for mapping in mappings:
                        try:
                            source_count = get_row_count(source_db, mapping.source_table, schema=mapping.source_schema)
                            target_count = get_row_count(target_db, mapping.target_table, schema=mapping.target_schema or None)
                            if source_count == target_count:
                                in_sync_count += 1
                        except Exception:
                            pass  # table unreachable counts as not in sync

                    not_synced_count = total_count - in_sync_count

                    if in_sync_count == total_count:
                        status = 'rows_in_sync'
                    elif config.processing_mode == 'batch':
                        status = 'syncing' if connector_state == 'RUNNING' else 'yet_to_be_synced'
                    else:
                        status = 'not_synced'

                    sync = {
                        'status': status,
                        'in_sync_count': in_sync_count,
                        'total_count': total_count,
                        'synced': in_sync_count,
                        'not_synced': not_synced_count,
                    }

        except Exception as e:
            logger.warning(f'Sync status check failed for config {config_pk}: {e}')
            sync = {'status': 'unable_to_check'}

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
    AJAX POST endpoint that applies FK constraints to the target database.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        from client.utils.table_creator import add_foreign_keys_to_target
        created, skipped, errors = add_foreign_keys_to_target(config)
        return JsonResponse({
            'success': True,
            'created': created,
            'skipped': skipped,
            'errors': errors,
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
        configs = list(database.replication_configs.all())

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
        configs = list(database.replication_configs.all())

        from client.utils.table_creator import add_foreign_keys_to_target

        total_created = 0
        total_skipped = 0
        all_errors = []

        for config in configs:
            created, skipped, errors = add_foreign_keys_to_target(config)
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
        logger.error(f'DB FK apply failed for database {database_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)