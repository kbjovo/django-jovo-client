"""
Provision Step Views — AJAX endpoints for the connector creation/edit modal.

These are called sequentially by the frontend modal JS:

Create flow:
  POST /connector/<pk>/provision/topics/   → provision_topics
  POST /connector/<pk>/provision/source/   → provision_source
  POST /connector/<pk>/provision/sink/     → provision_sink
  POST /connector/<pk>/provision/cancel/   → provision_cancel

Edit flow:
  POST /connector/<pk>/edit/save-settings/ → edit_save_settings
  POST /connector/<pk>/edit/remove-tables/ → edit_remove_tables
  POST /connector/<pk>/edit/add-tables/    → edit_add_tables
  POST /connector/<pk>/edit/cancel/        → edit_cancel
"""

import json
import logging
import re

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import get_object_or_404

from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, ConnectorHistory
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.connector_templates import get_connector_config_for_database
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager

logger = logging.getLogger(__name__)

_VALID_TABLE_NAME_RE = re.compile(r'^[a-zA-Z0-9_]+$')


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _require_post(request):
    """Return a 405 JsonResponse if the request is not POST, else None."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'POST required'}, status=405)
    return None


def _parse_json_body(request):
    """Parse JSON request body; return ({}, error_str) on failure."""
    try:
        return json.loads(request.body), None
    except Exception as e:
        return {}, str(e)


# ──────────────────────────────────────────────────────────────────────────────
# CREATE FLOW — provision steps
# ──────────────────────────────────────────────────────────────────────────────

def provision_topics(request, config_pk):
    """
    AJAX Step 2 (create flow): Create Kafka topics for the connector.
    Called by the provision modal after connector config is saved.
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    try:
        topic_manager = KafkaTopicManager()
        success, message = topic_manager.create_topics_for_config(replication_config)

        if not success:
            return JsonResponse({'success': False, 'error': message})

        table_count = replication_config.table_mappings.filter(is_enabled=True).count()
        # Each connector: 1 topic per table + schema-changes topic + signal topic
        topic_count = table_count + 2

        return JsonResponse({
            'success': True,
            'message': f'Created {topic_count} Kafka topics ({table_count} data + schema-changes + signal)',
            'topic_count': topic_count,
        })

    except Exception as e:
        logger.error(f'provision_topics failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def provision_source(request, config_pk):
    """
    AJAX Step 3 (create flow): Create the Debezium source connector.
    Handles both CDC (streaming) and Batch (paused + Celery schedule) modes.
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database

    try:
        # ── Batch mode ───────────────────────────────────────────────────────
        if replication_config.processing_mode == 'batch':
            from client.replication.orchestrator import ReplicationOrchestrator
            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.start_batch_replication()

            if not success:
                replication_config.status = 'error'
                replication_config.last_error_message = message
                replication_config.save()
                return JsonResponse({'success': False, 'error': message})

            return JsonResponse({
                'success': True,
                'message': (
                    f'Source connector created (paused) — '
                    f'batch schedule: every {replication_config.batch_interval}'
                ),
                'processing_mode': 'batch',
                'batch_interval': replication_config.batch_interval,
            })

        # ── CDC mode ─────────────────────────────────────────────────────────
        connector_manager = DebeziumConnectorManager()
        table_mappings = replication_config.table_mappings.filter(is_enabled=True)
        tables_list = [tm.source_table for tm in table_mappings]

        source_config = get_connector_config_for_database(
            db_config=database,
            replication_config=replication_config,
            tables_whitelist=tables_list,
        )

        connector_manager.create_connector(replication_config.connector_name, source_config)

        replication_config.status = 'active'
        replication_config.is_active = True
        replication_config.connector_state = 'RUNNING'
        replication_config.save()

        return JsonResponse({
            'success': True,
            'message': 'Source connector created (streaming)',
            'processing_mode': 'cdc',
        })

    except Exception as e:
        logger.error(f'provision_source failed for config {config_pk}: {e}', exc_info=True)
        replication_config.status = 'error'
        replication_config.last_error_message = str(e)
        replication_config.save()
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def provision_sink(request, config_pk):
    """
    AJAX Step 4 (create flow): Create or update the shared sink connector.
    Returns created_new=True if the sink was freshly created (used by cancel logic).
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database
    client = database.client

    try:
        connector_manager = DebeziumConnectorManager()
        sink_connector_name = database.get_sink_connector_name()

        target_database = ClientDatabase.objects.filter(
            client=client,
            is_target=True,
        ).first()

        if not target_database:
            return JsonResponse({
                'success': True,
                'message': 'No target database configured — sink connector skipped',
                'created_new': False,
            })

        sink_config = get_sink_connector_config_for_database(
            db_config=target_database,
            topics=None,
            delete_enabled=True,
            custom_config={'name': sink_connector_name},
        )

        exists, _ = connector_manager.get_connector_status(sink_connector_name)

        if not exists:
            connector_manager.create_connector(sink_connector_name, sink_config)

            database.replication_configs.update(
                sink_connector_name=sink_connector_name,
                sink_connector_state='RUNNING',
            )

            ConnectorHistory.record_connector_creation(
                replication_config,
                sink_connector_name,
                1,
                connector_type='sink',
            )

            return JsonResponse({
                'success': True,
                'message': 'Sink connector created (will auto-create target tables on first event)',
                'created_new': True,
            })

        else:
            # Sink already exists — update topics.regex to cover any new source topics
            connector_manager.update_connector_config(sink_connector_name, sink_config)

            replication_config.sink_connector_name = sink_connector_name
            replication_config.save()

            return JsonResponse({
                'success': True,
                'message': 'Sink connector updated (existing — target tables already present)',
                'created_new': False,
            })

    except Exception as e:
        logger.error(f'provision_sink failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def provision_cancel(request, config_pk):
    """
    AJAX: Cancel and revert an in-progress connector provisioning.
    Body JSON: {completed_steps: [], sink_was_new: false}
    Cleans up in reverse step order: sink → source → topics → DB record.
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    database = replication_config.client_database

    body, _ = _parse_json_body(request)
    completed_steps = body.get('completed_steps', [])
    sink_was_new = body.get('sink_was_new', False)

    reverted = []
    errors = []

    connector_manager = DebeziumConnectorManager()
    topic_manager = KafkaTopicManager()

    # Revert in reverse step order ─────────────────────────────────────────

    if sink_was_new and 'sink_provisioned' in completed_steps:
        try:
            sink_name = database.get_sink_connector_name()
            connector_manager.delete_connector(sink_name, delete_topics=False)
            reverted.append('sink_deleted')
            logger.info(f'provision_cancel: deleted sink connector {sink_name}')
        except Exception as e:
            errors.append(f'Could not delete sink connector: {e}')
            logger.warning(f'provision_cancel: sink delete failed: {e}')

    if 'source_created' in completed_steps:
        try:
            connector_manager.delete_connector(replication_config.connector_name, delete_topics=False)
            reverted.append('source_deleted')
            logger.info(f'provision_cancel: deleted source connector {replication_config.connector_name}')
        except Exception as e:
            errors.append(f'Could not delete source connector: {e}')
            logger.warning(f'provision_cancel: source connector delete failed: {e}')

    if 'topics_created' in completed_steps and replication_config.kafka_topic_prefix:
        try:
            topic_manager.delete_topics_by_prefix(replication_config.kafka_topic_prefix)
            reverted.append('topics_deleted')
            logger.info(f'provision_cancel: deleted topics with prefix {replication_config.kafka_topic_prefix}')
        except Exception as e:
            errors.append(f'Could not delete Kafka topics: {e}')
            logger.warning(f'provision_cancel: topic delete failed: {e}')

    if 'db_saved' in completed_steps:
        try:
            config_name = replication_config.connector_name
            replication_config.delete()   # cascades TableMappings + ConnectorHistory (SET_NULL)
            reverted.append('db_deleted')
            logger.info(f'provision_cancel: deleted ReplicationConfig for {config_name}')
        except Exception as e:
            errors.append(f'Could not delete connector record: {e}')
            logger.warning(f'provision_cancel: DB delete failed: {e}')

    return JsonResponse({
        'success': True,
        'reverted': reverted,
        'errors': errors,
    })


# ──────────────────────────────────────────────────────────────────────────────
# EDIT FLOW — step endpoints
# ──────────────────────────────────────────────────────────────────────────────

def edit_save_settings(request, config_pk):
    """
    AJAX Step (edit flow): Persist connector settings changes.
    Handles processing mode switches (CDC ↔ Batch) including Celery schedule setup/teardown.
    Regular form POST body (same field names as connector_edit_tables form).
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    try:
        from client.replication.orchestrator import ReplicationOrchestrator

        old_mode = replication_config.processing_mode
        changed = False

        # Snapshot mode
        new_snapshot_mode = request.POST.get('snapshot_mode')
        if new_snapshot_mode and new_snapshot_mode != replication_config.snapshot_mode:
            replication_config.snapshot_mode = new_snapshot_mode
            changed = True

        # Numeric performance fields
        for field in [
            'max_queue_size', 'max_batch_size', 'poll_interval_ms',
            'incremental_snapshot_chunk_size', 'snapshot_fetch_size',
            'sink_batch_size', 'sink_max_poll_records',
        ]:
            raw = request.POST.get(field)
            if raw:
                val = int(raw)
                if val != getattr(replication_config, field):
                    setattr(replication_config, field, val)
                    changed = True

        # Processing mode
        new_mode = request.POST.get('processing_mode', old_mode)
        mode_switched = new_mode != old_mode
        if mode_switched:
            replication_config.processing_mode = new_mode
            changed = True

        if new_mode == 'batch':
            batch_interval = request.POST.get('batch_interval')
            if mode_switched and not batch_interval:
                return JsonResponse({'success': False, 'error': 'Please select a sync interval for batch mode'}, status=400)
            if batch_interval and batch_interval != replication_config.batch_interval:
                replication_config.batch_interval = batch_interval
                changed = True
            batch_max_catchup = int(request.POST.get('batch_max_catchup_minutes', replication_config.batch_max_catchup_minutes))
            if batch_max_catchup not in [5, 10, 20]:
                batch_max_catchup = 5
            if batch_max_catchup != replication_config.batch_max_catchup_minutes:
                replication_config.batch_max_catchup_minutes = batch_max_catchup
                changed = True
        elif new_mode == 'cdc' and mode_switched:
            replication_config.batch_interval = None
            changed = True

        if not changed:
            return JsonResponse({'success': True, 'message': 'No settings changes detected'})

        replication_config.save()

        orch = ReplicationOrchestrator(replication_config)
        if mode_switched:
            if new_mode == 'batch':
                orch.setup_batch_schedule()
                orch.pause_connector()
                return JsonResponse({'success': True, 'message': 'Settings saved — switched to batch mode, connector paused'})
            else:
                orch.remove_batch_schedule()
                orch.resume_connector()
                return JsonResponse({'success': True, 'message': 'Settings saved — switched to CDC mode, connector resumed'})
        else:
            if new_mode == 'batch':
                orch.setup_batch_schedule()
            return JsonResponse({'success': True, 'message': 'Connector settings saved'})

    except Exception as e:
        logger.error(f'edit_save_settings failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def edit_remove_tables(request, config_pk):
    """
    AJAX Step (edit flow): Remove tables from the connector.
    Body JSON: {tables: ["table1", "table2"]}
    NOTE: Cancel is locked in the frontend before this endpoint is called —
    table removal triggers a connector restart and truncates target data.
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    body, parse_err = _parse_json_body(request)
    if parse_err:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {parse_err}'}, status=400)

    tables_to_remove = body.get('tables', [])
    if not tables_to_remove:
        return JsonResponse({'success': False, 'error': 'No tables specified for removal'}, status=400)

    # Guard: cannot remove all tables
    remaining = replication_config.table_mappings.filter(is_enabled=True).exclude(
        source_table__in=tables_to_remove
    ).count()
    if remaining == 0:
        return JsonResponse({
            'success': False,
            'error': 'Cannot remove all tables. Delete the connector instead.',
        }, status=400)

    try:
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(replication_config)
        success, message = orchestrator.remove_tables(tables_to_remove)

        if not success:
            return JsonResponse({'success': False, 'error': message})

        return JsonResponse({'success': True, 'message': message})

    except Exception as e:
        logger.error(f'edit_remove_tables failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def edit_add_tables(request, config_pk):
    """
    AJAX Step (edit flow): Add new tables via incremental snapshot signal.
    Body JSON: {tables: ["t1", "t2"], target_names: {"t1": "db_t1"}}
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    body, parse_err = _parse_json_body(request)
    if parse_err:
        return JsonResponse({'success': False, 'error': f'Invalid JSON: {parse_err}'}, status=400)

    tables_to_add = body.get('tables', [])
    target_names = body.get('target_names', {})

    if not tables_to_add:
        return JsonResponse({'success': False, 'error': 'No tables specified to add'}, status=400)

    # Validate table count limit
    max_tables = settings.DEBEZIUM_CONFIG.get('MAX_TABLES_PER_CONNECTOR', 25)
    current_count = replication_config.table_mappings.filter(is_enabled=True).count()
    total_after = current_count + len(tables_to_add)
    if total_after > max_tables:
        return JsonResponse({
            'success': False,
            'error': f'Maximum {max_tables} tables per connector. Would have {total_after} after addition.',
        }, status=400)

    # Validate target table names
    for table_name, target in target_names.items():
        if not _VALID_TABLE_NAME_RE.match(target or ''):
            return JsonResponse({
                'success': False,
                'error': (
                    f"Invalid target table name '{target}' for '{table_name}'. "
                    "Only letters, numbers and underscores allowed."
                ),
            }, status=400)

    try:
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(replication_config)
        success, message = orchestrator.add_tables(
            tables_to_add,
            target_table_names=target_names or None,
        )

        if not success:
            return JsonResponse({'success': False, 'error': message})

        return JsonResponse({'success': True, 'message': message})

    except Exception as e:
        logger.error(f'edit_add_tables failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def edit_cancel(request, config_pk):
    """
    AJAX: Cancel and revert an in-progress edit operation.
    Body JSON: {completed_steps: [], added_tables: []}
    Only table additions are reverted (settings changes are left — harmless).
    Table removals cannot be reverted (cancel is locked before removal starts).
    """
    if err := _require_post(request):
        return err

    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    body, _ = _parse_json_body(request)
    completed_steps = body.get('completed_steps', [])
    added_tables = body.get('added_tables', [])

    if 'add_tables' not in completed_steps or not added_tables:
        return JsonResponse({'success': True, 'message': 'Nothing to revert'})

    try:
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(replication_config)
        success, message = orchestrator.remove_tables(added_tables)

        if success:
            return JsonResponse({'success': True, 'message': f'Reverted table additions: {message}'})

        return JsonResponse({
            'success': False,
            'error': f'Could not fully revert table additions: {message}. Please check connector state.',
        })

    except Exception as e:
        logger.error(f'edit_cancel failed for config {config_pk}: {e}', exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Revert failed: {str(e)}. Please check connector state manually.',
        }, status=500)