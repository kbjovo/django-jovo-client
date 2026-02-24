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

        context = {
            'replication_config': replication_config,
            'database': database,
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
      - dlq: Dead Letter Queue message count via Kafka Admin
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

        # ── 2. DLQ message count via confluent_kafka consumer ──────────────
        try:
            from confluent_kafka import Consumer, TopicPartition
            from django.conf import settings as django_settings
            kafka_servers = django_settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
            dlq_topic = f"client_{database.client.id}.dlq"
            consumer = Consumer({
                'bootstrap.servers': kafka_servers,
                'group.id': 'jovo-dlq-inspector',
                'auto.offset.reset': 'earliest',
            })
            meta = consumer.list_topics(topic=dlq_topic, timeout=5)
            topic_meta = meta.topics.get(dlq_topic)
            if topic_meta and not topic_meta.error:
                total = 0
                for partition_id in topic_meta.partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(dlq_topic, partition_id), timeout=3
                    )
                    total += max(0, high - low)
                result['dlq'] = {'available': True, 'topic': dlq_topic, 'count': total}
            else:
                result['dlq'] = {'available': True, 'topic': dlq_topic, 'count': 0}
            consumer.close()
        except Exception as e:
            result['dlq'] = {'available': False, 'error': str(e)}

        # ── 3. Live config diff (Kafka Connect REST vs stored model) ───────
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
    """
    from client.utils.database_utils import get_database_connection

    try:
        config = get_object_or_404(ReplicationConfig, pk=config_pk)
        source_db = config.client_database
        target_db = source_db.client.get_target_database()

        # Test source database connectivity
        source_reachable = True
        try:
            with get_database_connection(source_db) as conn:
                pass
        except Exception as e:
            source_reachable = False
            logger.warning(f'Source database unreachable ({source_db.connection_name}): {e}')

        # Test target database connectivity
        target_reachable = True
        if target_db:
            try:
                with get_database_connection(target_db) as conn:
                    pass
            except Exception as e:
                target_reachable = False
                logger.warning(f'Target database unreachable ({target_db.connection_name}): {e}')

        mappings = config.table_mappings.filter(is_enabled=True).order_by('source_table')
        rows = []

        for mapping in mappings:
            source_count = None
            source_error = None
            target_count = None
            target_error = None

            # Count source rows
            if not source_reachable:
                source_error = 'db_unreachable'
            else:
                try:
                    source_count = get_row_count(
                        source_db,
                        mapping.source_table,
                        schema=mapping.source_schema,
                    )
                except Exception as e:
                    err_str = str(e).lower()
                    if "doesn't exist" in err_str or 'not exist' in err_str or 'not found' in err_str or 'unknown table' in err_str or 'invalid object' in err_str:
                        source_error = 'table_not_found'
                    else:
                        source_error = 'error'
                    logger.warning(f'Source row count failed for {mapping.source_table}: {e}')

            # Count target rows
            if not target_db:
                target_error = 'no_target_db'
            elif not target_reachable:
                target_error = 'db_unreachable'
            else:
                try:
                    target_count = get_row_count(
                        target_db,
                        mapping.target_table,
                        schema=mapping.target_schema or None,
                    )
                except Exception as e:
                    err_str = str(e).lower()
                    if "doesn't exist" in err_str or 'not exist' in err_str or 'not found' in err_str or 'unknown table' in err_str or 'invalid object' in err_str:
                        target_error = 'table_not_found'
                    else:
                        target_error = 'error'
                    logger.warning(f'Target row count failed for {mapping.target_table}: {e}')

            rows.append({
                'source_table': mapping.source_table,
                'target_table': mapping.target_table,
                'source_rows': source_count,
                'target_rows': target_count,
                'source_error': source_error,
                'target_error': target_error,
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