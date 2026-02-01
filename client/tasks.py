"""
Celery tasks for CDC replication using JDBC Sink Connectors
"""
import logging
from celery import shared_task
from django.utils import timezone
from datetime import timedelta
from django.conf import settings
from client.models.replication import ReplicationConfig
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from client.utils.database_utils import get_database_engine

logger = logging.getLogger(__name__)


# ============================================================================
# UNIFIED DDL PROCESSING TASKS
# ============================================================================

@shared_task(bind=True, max_retries=3)
def process_ddl_changes(self, config_id: int, auto_destructive: bool = True):
    """
    Unified DDL processing for all source database types.

    Automatically selects the appropriate processor based on source database type:
    - MySQL/MSSQL: Kafka-based DDL processor (consumes schema history topic)
    - PostgreSQL: Schema sync service (periodic comparison)

    Args:
        config_id: ReplicationConfig ID
        auto_destructive: Auto-execute destructive operations (DROP COLUMN, etc.)

    Returns:
        Dict with processing results
    """
    from client.utils.ddl import KafkaDDLProcessor, PostgreSQLSchemaSyncService

    try:
        config = ReplicationConfig.objects.get(id=config_id)
    except ReplicationConfig.DoesNotExist:
        logger.error(f"ReplicationConfig {config_id} not found")
        return {'success': False, 'error': 'Config not found'}

    # Get target database engine
    target_db = config.client_database.client.client_databases.filter(is_target=True).first()
    if not target_db:
        logger.error(f"No target database for config {config_id}")
        return {'success': False, 'error': 'No target database'}

    target_engine = get_database_engine(target_db)
    source_type = config.client_database.db_type.lower()

    processor = None

    try:
        if source_type in ('mysql', 'mssql', 'sqlserver'):
            # Kafka-based processor for MySQL/MSSQL
            kafka_servers = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_INTERNAL_SERVERS',
                'kafka-1:29092,kafka-2:29092,kafka-3:29092'
            )
            processor = KafkaDDLProcessor(
                replication_config=config,
                target_engine=target_engine,
                bootstrap_servers=kafka_servers,
                auto_execute_destructive=auto_destructive
            )
            processed, errors = processor.process(timeout_sec=30, max_messages=100)

        elif source_type in ('postgresql', 'postgres'):
            # Schema sync service for PostgreSQL
            processor = PostgreSQLSchemaSyncService(
                replication_config=config,
                target_engine=target_engine,
                auto_execute_destructive=auto_destructive
            )
            processed, errors = processor.process()

        else:
            logger.warning(f"DDL processing not supported for source type: {source_type}")
            return {'success': False, 'error': f'Unsupported source type: {source_type}'}

        if processed > 0 or errors > 0:
            logger.info(f"DDL processing for config {config_id}: {processed} processed, {errors} errors")

        return {
            'success': True,
            'config_id': config_id,
            'source_type': source_type,
            'processed': processed,
            'errors': errors
        }

    except Exception as e:
        logger.error(f"DDL processing failed for config {config_id}: {e}", exc_info=True)
        # Retry with exponential backoff
        try:
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for DDL processing config {config_id}")
        return {'success': False, 'error': str(e)}

    finally:
        if processor:
            processor.close()


@shared_task
def schedule_ddl_processing_all():
    """
    Schedule DDL processing for all active replication configs.

    Called periodically by Celery Beat (every 1 minute).
    Processes DDL for MySQL, MSSQL, and PostgreSQL sources.
    """
    supported_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')

    active_configs = ReplicationConfig.objects.filter(
        status='active',
        is_active=True
    ).select_related('client_database')

    scheduled = 0
    for config in active_configs:
        source_type = config.client_database.db_type.lower()
        if source_type not in supported_types:
            continue

        # Schedule DDL processing task
        process_ddl_changes.delay(config.id)
        scheduled += 1

    if scheduled > 0:
        logger.info(f"Scheduled DDL processing for {scheduled} configs")

    return {'scheduled': scheduled}


# Global flag to track running continuous consumers
_running_consumers = {}


@shared_task(bind=True, queue='ddl_consumer')
def start_continuous_ddl_consumer(self, config_id: int):
    """
    Start a continuous Kafka consumer for real-time DDL processing.

    This runs indefinitely, processing DDL changes as they arrive.
    Use stop_continuous_ddl_consumer() to stop it.
    """
    from client.utils.ddl import KafkaDDLProcessor
    import time

    global _running_consumers

    # Check if already running for this config
    if config_id in _running_consumers and _running_consumers[config_id]:
        logger.info(f"Continuous DDL consumer already running for config {config_id}")
        return {'success': False, 'error': 'Already running'}

    try:
        config = ReplicationConfig.objects.get(id=config_id)
    except ReplicationConfig.DoesNotExist:
        logger.error(f"ReplicationConfig {config_id} not found")
        return {'success': False, 'error': 'Config not found'}

    source_type = config.client_database.db_type.lower()
    if source_type not in ('mysql', 'mssql', 'sqlserver'):
        logger.error(f"Continuous consumer only supports MySQL/MSSQL, not {source_type}")
        return {'success': False, 'error': f'Unsupported source type: {source_type}'}

    target_db = config.client_database.client.client_databases.filter(is_target=True).first()
    if not target_db:
        logger.error(f"No target database for config {config_id}")
        return {'success': False, 'error': 'No target database'}

    target_engine = get_database_engine(target_db)
    kafka_servers = settings.DEBEZIUM_CONFIG.get(
        'KAFKA_INTERNAL_SERVERS',
        'kafka-1:29092,kafka-2:29092,kafka-3:29092'
    )

    _running_consumers[config_id] = True
    logger.info(f"Starting continuous DDL consumer for config {config_id}")

    processor = None
    try:
        processor = KafkaDDLProcessor(
            replication_config=config,
            target_engine=target_engine,
            bootstrap_servers=kafka_servers,
            auto_execute_destructive=True
        )

        # Continuous loop
        while _running_consumers.get(config_id, False):
            try:
                # Process with short timeout for responsiveness
                processed, errors = processor.process(timeout_sec=5, max_messages=10)

                if processed > 0 or errors > 0:
                    logger.info(f"[Continuous] Config {config_id}: {processed} processed, {errors} errors")

                # Small sleep to prevent tight loop when no messages
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in continuous consumer for config {config_id}: {e}")
                time.sleep(5)  # Back off on error

    except Exception as e:
        logger.error(f"Continuous DDL consumer failed for config {config_id}: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}
    finally:
        _running_consumers[config_id] = False
        if processor:
            processor.close()
        logger.info(f"Continuous DDL consumer stopped for config {config_id}")

    return {'success': True, 'config_id': config_id}


@shared_task
def stop_continuous_ddl_consumer(config_id: int):
    """Stop a running continuous DDL consumer."""
    global _running_consumers
    if config_id in _running_consumers:
        _running_consumers[config_id] = False
        logger.info(f"Stopping continuous DDL consumer for config {config_id}")
        return {'success': True}
    return {'success': False, 'error': 'Not running'}


@shared_task
def ensure_continuous_ddl_consumers():
    """
    Ensure continuous DDL consumers are running for all active MySQL/MSSQL replications.

    Called periodically by Celery Beat to auto-start/restart consumers.
    This keeps DDL sync always on.
    """
    global _running_consumers

    supported_types = ('mysql', 'mssql', 'sqlserver')

    active_configs = ReplicationConfig.objects.filter(
        status='active',
        is_active=True
    ).select_related('client_database')

    started = 0
    for config in active_configs:
        source_type = config.client_database.db_type.lower()
        if source_type not in supported_types:
            continue

        # Check if consumer is running
        if config.id not in _running_consumers or not _running_consumers[config.id]:
            logger.info(f"Starting continuous DDL consumer for config {config.id}")
            start_continuous_ddl_consumer.delay(config.id)
            started += 1

    if started > 0:
        logger.info(f"Started {started} continuous DDL consumers")

    return {'started': started}


@shared_task
def sync_postgresql_schemas():
    """
    Periodic schema sync for PostgreSQL source databases.

    Called every 5 minutes by Celery Beat.
    PostgreSQL doesn't emit DDL events, so we compare schemas periodically.
    """
    pg_configs = ReplicationConfig.objects.filter(
        status='active',
        is_active=True,
        client_database__db_type__in=['postgresql', 'postgres']
    )

    scheduled = 0
    for config in pg_configs:
        process_ddl_changes.delay(config.id)
        scheduled += 1

    if scheduled > 0:
        logger.info(f"Scheduled PostgreSQL schema sync for {scheduled} configs")

    return {'scheduled': scheduled}



@shared_task
def monitor_connectors():
    """
    Periodic task: Monitor all Debezium connectors
    """
    try:
        logger.info("Monitoring Debezium connectors...")

        manager = DebeziumConnectorManager()

        # Check Kafka Connect health
        is_healthy, error = manager.check_kafka_connect_health()

        if not is_healthy:
            logger.error(f"Kafka Connect is unhealthy: {error}")
            return {'success': False, 'error': error}

        # Get all active replication configs
        active_configs = ReplicationConfig.objects.filter(
            is_active=True,
            status='active'
        )

        issues = []

        for config in active_configs:
            if not config.connector_name:
                continue

            # Check connector status
            exists, status_data = manager.get_connector_status(config.connector_name)

            if not exists:
                issues.append({
                    'config_id': config.id,
                    'connector': config.connector_name,
                    'issue': 'Connector not found'
                })

                config.status = 'error'
                config.save()

            elif status_data:
                connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')

                if connector_state != 'RUNNING':
                    issues.append({
                        'config_id': config.id,
                        'connector': config.connector_name,
                        'issue': f'State: {connector_state}'
                    })

                    if connector_state == 'FAILED':
                        config.status = 'error'
                        config.save()

        if issues:
            logger.warning(f"Found {len(issues)} connector issues: {issues}")
        else:
            logger.info(f"All {active_configs.count()} connectors are healthy")

        return {
            'success': True,
            'monitored': active_configs.count(),
            'issues': len(issues),
            'details': issues
        }

    except Exception as e:
        logger.error(f"Error monitoring connectors: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def check_replication_health():
    """
    Periodic task: Check overall replication health
    """
    try:
        logger.info("Checking replication health...")

        active_configs = ReplicationConfig.objects.filter(is_active=True)

        health_report = {
            'total_configs': active_configs.count(),
            'healthy': 0,
            'unhealthy': 0,
            'issues': []
        }

        for config in active_configs:
            last_sync = config.last_sync_at

            if last_sync:
                time_since_sync = timezone.now() - last_sync

                if time_since_sync > timedelta(minutes=30):
                    health_report['unhealthy'] += 1
                    health_report['issues'].append({
                        'config_id': config.id,
                        'client': config.client_database.client.name,
                        'issue': f'No sync in {time_since_sync.seconds // 60} minutes'
                    })
                else:
                    health_report['healthy'] += 1
            else:
                health_report['unhealthy'] += 1
                health_report['issues'].append({
                    'config_id': config.id,
                    'client': config.client_database.client.name,
                    'issue': 'Never synced'
                })

        logger.info(f"Health check: {health_report}")

        return health_report

    except Exception as e:
        logger.error(f"Error checking health: {e}")
        return {'success': False, 'error': str(e)}


# ============================================================================
# BATCH PROCESSING TASKS
# ============================================================================

@shared_task(bind=True, max_retries=2)
def run_batch_sync(self, replication_config_id: int):
    """
    Execute a batch sync cycle for a connector in batch processing mode.

    This task is scheduled by Celery Beat based on the batch_interval setting.

    Cycle:
    1. Resume connector (starts catching up with accumulated changes)
    2. Wait until lag is cleared OR max duration reached (throttle)
    3. Pause connector
    4. Record completion time and schedule next run

    Args:
        replication_config_id: ID of the ReplicationConfig to sync

    Returns:
        Dict with success status and details
    """
    import time

    try:
        logger.info(f"=" * 60)
        logger.info(f"BATCH SYNC STARTING - Config ID: {replication_config_id}")
        logger.info(f"=" * 60)

        config = ReplicationConfig.objects.get(id=replication_config_id)

        # Validate batch mode
        if config.processing_mode != 'batch':
            logger.warning(f"Config {replication_config_id} is not in batch mode, skipping")
            return {'success': False, 'error': 'Not in batch mode'}

        # Validate connector exists
        if not config.connector_name:
            logger.error(f"No connector configured for config {replication_config_id}")
            return {'success': False, 'error': 'No connector configured'}

        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)

        # ========================================
        # STEP 1: Resume Connector
        # ========================================
        logger.info("Step 1/3: Resuming connector...")

        success, message = orchestrator.resume_connector()
        if not success:
            logger.error(f"Failed to resume connector: {message}")
            return {'success': False, 'error': message}

        logger.info(f"✓ Connector resumed: {config.connector_name}")

        # ========================================
        # STEP 2: Wait for Catch-up (with throttle)
        # ========================================
        logger.info("Step 2/3: Waiting for catch-up (throttled)...")

        max_duration_minutes = config.batch_max_catchup_minutes or 30
        max_duration_seconds = max_duration_minutes * 60
        poll_interval_seconds = 30  # Check every 30 seconds

        start_time = time.time()
        elapsed = 0

        manager = DebeziumConnectorManager()

        while elapsed < max_duration_seconds:
            # Check connector status
            exists, status_data = manager.get_connector_status(config.connector_name)

            if not exists:
                logger.error("Connector disappeared during batch sync")
                break

            if status_data:
                state = status_data.get('connector', {}).get('state', '')
                if state == 'FAILED':
                    logger.error("Connector failed during batch sync")
                    config.status = 'error'
                    config.last_error_message = "Connector failed during batch sync"
                    config.save()
                    return {'success': False, 'error': 'Connector failed'}

            # Log progress
            elapsed = time.time() - start_time
            remaining = max_duration_seconds - elapsed
            logger.info(f"  Batch sync running... {int(elapsed)}s elapsed, {int(remaining)}s remaining")

            if remaining <= 0:
                logger.info("  Max duration reached (throttle limit)")
                break

            # Wait before next check
            time.sleep(min(poll_interval_seconds, remaining))
            elapsed = time.time() - start_time

        total_elapsed = time.time() - start_time
        logger.info(f"✓ Batch window completed after {int(total_elapsed)} seconds")

        # ========================================
        # STEP 3: Pause Connector
        # ========================================
        logger.info("Step 3/3: Pausing connector...")

        success, message = orchestrator.pause_connector()
        if not success:
            logger.warning(f"Failed to pause connector: {message}")
            # Continue anyway - we'll try to pause next time

        logger.info(f"✓ Connector paused: {config.connector_name}")

        # ========================================
        # Update timestamps
        # ========================================
        config.last_batch_run = timezone.now()

        # Calculate next run based on interval
        interval_seconds = orchestrator._get_batch_interval_seconds()
        config.next_batch_run = timezone.now() + timedelta(seconds=interval_seconds)
        config.save()

        logger.info(f"=" * 60)
        logger.info(f"✓ BATCH SYNC COMPLETED")
        logger.info(f"  Duration: {int(total_elapsed)} seconds")
        logger.info(f"  Next run: {config.next_batch_run}")
        logger.info(f"=" * 60)

        return {
            'success': True,
            'config_id': replication_config_id,
            'duration_seconds': int(total_elapsed),
            'next_run': config.next_batch_run.isoformat() if config.next_batch_run else None,
        }

    except ReplicationConfig.DoesNotExist:
        logger.error(f"ReplicationConfig {replication_config_id} not found")
        return {'success': False, 'error': 'Config not found'}

    except Exception as e:
        logger.error(f"Error during batch sync: {e}", exc_info=True)

        # Update config status
        try:
            config = ReplicationConfig.objects.get(id=replication_config_id)
            config.last_error_message = str(e)
            config.save()
        except Exception:
            pass

        # Retry with backoff
        try:
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for batch sync")

        return {'success': False, 'error': str(e)}


@shared_task
def trigger_batch_sync_now(replication_config_id: int):
    """
    Manually trigger a batch sync immediately (Sync Now button).

    Args:
        replication_config_id: ID of the ReplicationConfig to sync

    Returns:
        Dict with task ID for tracking
    """
    logger.info(f"Manual batch sync triggered for config {replication_config_id}")

    # Run the batch sync task
    result = run_batch_sync.delay(replication_config_id)

    return {
        'success': True,
        'task_id': result.id,
        'message': 'Batch sync started'
    }


# ============================================================================
# FOREIGN KEY TASKS
# ============================================================================

@shared_task(bind=True, max_retries=3)
def add_foreign_keys_task(self, replication_config_id: int):
    """
    Add foreign key constraints to target tables after sink connector creates them.

    This task is called after connector creation with a delay to allow the sink
    connector to create tables from the initial snapshot.

    Args:
        replication_config_id: ID of the ReplicationConfig

    Returns:
        Dict with success status and details
    """
    try:
        logger.info(f"Adding foreign keys for config {replication_config_id}")

        config = ReplicationConfig.objects.get(id=replication_config_id)

        from client.utils.table_creator import add_foreign_keys_to_target

        created, skipped, errors = add_foreign_keys_to_target(config)

        logger.info(f"Foreign keys for config {replication_config_id}: "
                    f"{created} created, {skipped} skipped, {len(errors)} errors")

        if errors:
            logger.warning(f"FK errors: {errors}")

        return {
            'success': True,
            'config_id': replication_config_id,
            'created': created,
            'skipped': skipped,
            'errors': errors
        }

    except ReplicationConfig.DoesNotExist:
        logger.error(f"ReplicationConfig {replication_config_id} not found")
        return {'success': False, 'error': 'Config not found'}

    except Exception as e:
        logger.error(f"Error adding foreign keys: {e}", exc_info=True)

        # Retry with exponential backoff (30s, 60s, 120s)
        try:
            raise self.retry(countdown=30 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for FK creation config {replication_config_id}")

        return {'success': False, 'error': str(e)}




