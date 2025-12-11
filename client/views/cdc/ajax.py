"""
CDC AJAX endpoints.

Handles asynchronous operations for CDC configuration editing:
- Basic configuration updates
- Table mapping updates
- Adding/removing tables
"""

import logging
import json
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.db import transaction

from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.database_utils import get_table_schema

logger = logging.getLogger(__name__)


@require_http_methods(["POST"])
@login_required
def ajax_update_basic_config(request, config_pk):
    """
    AJAX endpoint to update basic configuration settings.
    Handles: sync_type, sync_frequency, auto_create_tables, drop_before_sync
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        # Parse JSON body
        data = json.loads(request.body)

        # Update fields
        if 'sync_type' in data:
            replication_config.sync_type = data['sync_type']
        if 'sync_frequency' in data:
            replication_config.sync_frequency = data['sync_frequency']
        if 'auto_create_tables' in data:
            replication_config.auto_create_tables = data['auto_create_tables']
        if 'drop_before_sync' in data:
            replication_config.drop_before_sync = data['drop_before_sync']

        replication_config.save()

        return JsonResponse({
            'success': True,
            'message': 'Basic configuration updated successfully'
        })

    except Exception as e:
        logger.error(f"Failed to update basic config: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
@login_required
def ajax_update_table_mapping(request, config_pk, table_mapping_pk):
    """
    AJAX endpoint to update a specific table mapping and its columns.
    Handles all table-level and column-level settings for a single table.
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
        table_mapping = get_object_or_404(TableMapping, pk=table_mapping_pk, replication_config=replication_config)

        # Parse JSON body
        data = json.loads(request.body)

        with transaction.atomic():
            # Update table-level settings
            if 'target_table' in data:
                table_mapping.target_table = data['target_table']
            if 'sync_type' in data:
                table_mapping.sync_type = data['sync_type']
            if 'incremental_column' in data:
                table_mapping.incremental_column = data['incremental_column']
            if 'conflict_resolution' in data:
                table_mapping.conflict_resolution = data['conflict_resolution']
            if 'is_enabled' in data:
                table_mapping.is_enabled = data['is_enabled']

            table_mapping.save()

            # Update column mappings if provided
            if 'columns' in data:
                for column_data in data['columns']:
                    column_id = column_data.get('id')
                    if column_id:
                        try:
                            column_mapping = ColumnMapping.objects.get(
                                id=column_id,
                                table_mapping=table_mapping
                            )
                            if 'is_enabled' in column_data:
                                column_mapping.is_enabled = column_data['is_enabled']
                            if 'target_column' in column_data:
                                column_mapping.target_column = column_data['target_column']
                            column_mapping.save()
                        except ColumnMapping.DoesNotExist:
                            logger.warning(f"Column mapping {column_id} not found")

        return JsonResponse({
            'success': True,
            'message': f'Table "{table_mapping.source_table}" updated successfully'
        })

    except Exception as e:
        logger.error(f"Failed to update table mapping: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
@login_required
def ajax_add_tables(request, config_pk):
    """
    AJAX endpoint to add new tables to replication configuration.
    Creates table mappings and column mappings for newly added tables.
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
        db_config = replication_config.client_database
        client = db_config.client

        # Parse JSON body
        data = json.loads(request.body)
        table_names = data.get('tables', [])

        if not table_names:
            return JsonResponse({
                'success': False,
                'error': 'No tables provided'
            }, status=400)

        with transaction.atomic():
            newly_added_tables = []
            existing_mappings = {tm.source_table: tm for tm in replication_config.table_mappings.all()}

            for table_name in table_names:
                if table_name in existing_mappings:
                    continue  # Skip already mapped tables

                newly_added_tables.append(table_name)
                logger.info(f"➕ Adding new table: {table_name}")

                # Create new table mapping
                source_db_name = db_config.database_name
                # Sanitize table_name: replace dots with underscores for MSSQL
                sanitized_table = table_name.replace('.', '_')
                target_table_name = f"{source_db_name}_{sanitized_table}"

                table_mapping = TableMapping.objects.create(
                    replication_config=replication_config,
                    source_table=table_name,
                    target_table=target_table_name,
                    is_enabled=True,
                    sync_type='realtime',
                    conflict_resolution='source_wins'
                )

                # Create column mappings
                try:
                    schema = get_table_schema(db_config, table_name)
                    for column in schema.get('columns', []):
                        ColumnMapping.objects.create(
                            table_mapping=table_mapping,
                            source_column=column['name'],
                            target_column=column['name'],
                            source_type=str(column.get('type', 'unknown')),
                            target_type=str(column.get('type', 'unknown')),
                            is_enabled=True
                        )
                except Exception as e:
                    logger.error(f"Failed to create column mappings for {table_name}: {e}")

            # Create target tables if auto_create_tables is enabled
            if newly_added_tables and replication_config.auto_create_tables:
                try:
                    from client.utils.table_creator import create_target_tables
                    logger.info(f"🔨 Creating target tables for {len(newly_added_tables)} newly added tables...")
                    create_target_tables(replication_config, specific_tables=newly_added_tables)
                except Exception as e:
                    logger.error(f"❌ Failed to create target tables: {e}", exc_info=True)
                    return JsonResponse({
                        'success': False,
                        'error': f'Tables added but failed to create target tables: {str(e)}'
                    }, status=500)

            # Create Kafka topics for newly added tables
            if newly_added_tables:
                try:
                    from client.utils.kafka_topic_manager import KafkaTopicManager
                    topic_manager = KafkaTopicManager()
                    topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"

                    # Create signal topic
                    signal_topic_success, signal_topic_error = topic_manager.create_signal_topic(topic_prefix)
                    if signal_topic_success:
                        logger.info(f"✅ Signal topic ready: {topic_prefix}.signals")

                    # Determine namespace for topic naming
                    # PostgreSQL uses schema.table (e.g., public.users)
                    # MySQL uses database.table (e.g., mydb.users)
                    if db_config.db_type.lower() == 'postgresql':
                        topic_namespace = 'public'  # PostgreSQL schema name
                        logger.info(f"PostgreSQL detected - using schema '{topic_namespace}' for topic naming")
                    else:
                        topic_namespace = db_config.database_name  # MySQL/Oracle database name

                    # Create topics for new tables
                    topic_results = topic_manager.create_cdc_topics_for_tables(
                        server_name=topic_prefix,
                        database=topic_namespace,
                        table_names=newly_added_tables
                    )

                    # Verify topics exist
                    topics_missing = []
                    for table in newly_added_tables:
                        topic_name = f"{topic_prefix}.{topic_namespace}.{table}"
                        if not topic_manager.topic_exists(topic_name):
                            topics_missing.append(topic_name)

                    if topics_missing:
                        error_msg = f"Failed to create Kafka topics: {', '.join(topics_missing)}"
                        logger.error(f"❌ {error_msg}")
                        return JsonResponse({
                            'success': False,
                            'error': error_msg
                        }, status=500)

                except Exception as e:
                    logger.error(f"❌ Error creating Kafka topics: {e}", exc_info=True)
                    return JsonResponse({
                        'success': False,
                        'error': f'Tables added but failed to create Kafka topics: {str(e)}'
                    }, status=500)

        return JsonResponse({
            'success': True,
            'message': f'Successfully added {len(newly_added_tables)} table(s)',
            'tables_added': newly_added_tables
        })

    except Exception as e:
        logger.error(f"Failed to add tables: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
@login_required
def ajax_remove_tables(request, config_pk):
    """
    AJAX endpoint to remove tables from replication configuration.
    Optionally drops target tables if drop_before_sync is enabled.
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        # Parse JSON body
        data = json.loads(request.body)
        table_names = data.get('tables', [])

        if not table_names:
            return JsonResponse({
                'success': False,
                'error': 'No tables provided'
            }, status=400)

        with transaction.atomic():
            removed_count = 0
            for table_name in table_names:
                try:
                    table_mapping = TableMapping.objects.get(
                        replication_config=replication_config,
                        source_table=table_name
                    )
                    logger.info(f"🗑️ Removing table mapping for {table_name}")
                    table_mapping.delete()
                    removed_count += 1
                except TableMapping.DoesNotExist:
                    logger.warning(f"Table mapping for {table_name} not found")

            # Drop target tables if drop_before_sync is enabled
            if removed_count > 0 and table_names:
                try:
                    from client.utils.table_creator import drop_target_tables
                    drop_target_tables(replication_config, table_names)
                except Exception as e:
                    logger.error(f"❌ Failed to drop target tables: {e}", exc_info=True)
                    # Don't fail the whole operation if drop fails

        message = f'Successfully removed {removed_count} table(s)'
        if replication_config.drop_before_sync:
            message += ' (and dropped from target database)'

        return JsonResponse({
            'success': True,
            'message': message,
            'removed_count': removed_count
        })

    except Exception as e:
        logger.error(f"Failed to remove tables: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


