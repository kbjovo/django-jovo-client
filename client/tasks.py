"""
Celery tasks for CDC replication using JDBC Sink Connectors
"""
import logging
import redis as redis_lib
from celery import shared_task
from django.utils import timezone
from datetime import timedelta
from django.conf import settings
from client.models.replication import ReplicationConfig
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from client.utils.database_utils import get_database_engine
from client.utils.notification_utils import maybe_send_alert, resolve_alert

logger = logging.getLogger(__name__)


# ============================================================================
# REDIS-BASED DISTRIBUTED LOCK FOR CONTINUOUS DDL CONSUMERS
# ============================================================================

_CONSUMER_LOCK_PREFIX = 'ddl_consumer_lock'
_CONSUMER_LOCK_TTL = 60        # seconds; must be > LOCK_REFRESH_INTERVAL
_LOCK_REFRESH_INTERVAL = 10.0  # refresh TTL every 10 seconds in the consumer loop


def _get_redis():
    """Create a fresh Redis client from the Celery broker URL."""
    return redis_lib.from_url(
        settings.CELERY_BROKER_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )


def _consumer_lock_key(config_id: int) -> str:
    return f'{_CONSUMER_LOCK_PREFIX}:{config_id}'


def _try_acquire_lock(config_id: int, task_id: str) -> bool:
    """
    Atomically acquire the consumer lock (SET NX EX).
    Returns True only if the lock was newly acquired.
    """
    try:
        r = _get_redis()
        return bool(r.set(_consumer_lock_key(config_id), task_id, ex=_CONSUMER_LOCK_TTL, nx=True))
    except Exception as e:
        logger.warning(f"Redis lock acquire failed for config {config_id}: {e}")
        return False


def _release_lock(config_id: int) -> None:
    """Delete the consumer lock (used on clean exit or by stop task)."""
    try:
        _get_redis().delete(_consumer_lock_key(config_id))
    except Exception as e:
        logger.warning(f"Redis lock release failed for config {config_id}: {e}")


def _refresh_and_check_lock(config_id: int) -> bool:
    """
    Refresh the lock TTL and return True if the lock still exists.
    Returns False when stop_continuous_ddl_consumer has deleted the key,
    signalling the consumer loop to exit.
    Fails open (returns True) on Redis errors so transient blips don't
    kill a healthy consumer.
    """
    try:
        r = _get_redis()
        # EXPIRE returns 1 if key exists and TTL was set, 0 if key is gone
        return bool(r.expire(_consumer_lock_key(config_id), _CONSUMER_LOCK_TTL))
    except Exception as e:
        logger.warning(f"Redis lock refresh failed for config {config_id}: {e}")
        return True  # fail open — keep running


def _is_consumer_running(config_id: int) -> bool:
    """Check whether a consumer lock is currently held for this config."""
    try:
        return bool(_get_redis().exists(_consumer_lock_key(config_id)))
    except Exception:
        return False


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
                'KAFKA_BOOTSTRAP_SERVERS',
                'localhost:9092,localhost:9094,localhost:9096'
            )
            processor = KafkaDDLProcessor(
                replication_config=config,
                target_engine=target_engine,
                bootstrap_servers=kafka_servers,
                auto_execute_destructive=auto_destructive
            )
            processed, errors = processor.process(timeout_sec=30, max_messages=100)

        elif source_type in ('postgresql', 'postgres'):
            # PostgreSQL DDL is handled by continuous DDL consumer (PostgreSQLKafkaDDLProcessor)
            # which consumes from ddl_capture.ddl_events Kafka topic.
            # Schema sync service is DISABLED because it detects RENAME as DROP+ADD causing data loss.
            logger.debug(f"Skipping PostgreSQL schema sync for config {config_id} - handled by continuous DDL consumer")
            return {
                'success': True,
                'config_id': config_id,
                'source_type': source_type,
                'processed': 0,
                'errors': 0,
                'message': 'PostgreSQL DDL handled by continuous consumer'
            }

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
            try:
                _cfg = ReplicationConfig.objects.get(id=config_id)
                maybe_send_alert(
                    _cfg, 'ddl_task_failed',
                    subject=f"DDL Task Failed: {_cfg.connector_name or f'config #{config_id}'}",
                    body=(
                        f"DDL processing task for config #{config_id} exhausted all retries.\n"
                        f"Last error: {str(e)}"
                    ),
                    connector_name=_cfg.connector_name or '',
                )
            except Exception:
                pass
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

    MySQL/MSSQL configs are skipped when a continuous DDL consumer is already running
    for them — both share the same Kafka consumer group, so running both simultaneously
    causes unnecessary rebalances with no benefit. The batch task only fires for configs
    whose continuous consumer has stopped (acts as a fallback).
    """
    supported_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')
    # Kafka-based sources that have a continuous consumer (see start_continuous_ddl_consumer)
    continuous_consumer_types = ('mysql', 'mssql', 'sqlserver')

    active_configs = ReplicationConfig.objects.filter(
        status='active',
        is_active=True
    ).select_related('client_database')

    scheduled = 0
    for config in active_configs:
        source_type = config.client_database.db_type.lower()
        if source_type not in supported_types:
            continue

        # Skip if a continuous consumer is already handling this config.
        # Avoids competing on the same Kafka consumer group and causing rebalances.
        if source_type in continuous_consumer_types and _is_consumer_running(config.id):
            continue

        process_ddl_changes.delay(config.id)
        scheduled += 1

    if scheduled > 0:
        logger.info(f"Scheduled DDL processing for {scheduled} configs")

    return {'scheduled': scheduled}


@shared_task(bind=True, queue='ddl_consumer', time_limit=None, soft_time_limit=None)
def start_continuous_ddl_consumer(self, config_id: int):
    """
    Start a continuous Kafka consumer for real-time DDL processing.

    This runs indefinitely, processing DDL changes as they arrive.
    Use stop_continuous_ddl_consumer() to stop it.

    Supports:
    - MySQL/MSSQL: Uses KafkaDDLProcessor (schema history topic)
    - PostgreSQL: Uses PostgreSQLKafkaDDLProcessor (ddl_capture.ddl_events topic)
    """
    from client.utils.ddl import KafkaDDLProcessor, PostgreSQLKafkaDDLProcessor
    import time

    task_id = self.request.id or f'consumer-{config_id}'

    # Acquire distributed Redis lock — prevents multiple workers from running the
    # same consumer simultaneously (fixes the per-process _running_consumers bug)
    if not _try_acquire_lock(config_id, task_id):
        logger.info(f"Continuous DDL consumer already running for config {config_id} (lock held)")
        return {'success': False, 'error': 'Already running'}

    # Outer try/finally ensures the lock is always released, even on validation failures
    # that return early before the processor loop's own finally block.
    try:
        try:
            config = ReplicationConfig.objects.get(id=config_id)
        except ReplicationConfig.DoesNotExist:
            logger.error(f"ReplicationConfig {config_id} not found")
            return {'success': False, 'error': 'Config not found'}

        source_type = config.client_database.db_type.lower()
        supported_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')
        if source_type not in supported_types:
            logger.error(f"Continuous consumer only supports MySQL/MSSQL/PostgreSQL, not {source_type}")
            return {'success': False, 'error': f'Unsupported source type: {source_type}'}

        target_db = config.client_database.client.client_databases.filter(is_target=True).first()
        if not target_db:
            logger.error(f"No target database for config {config_id}")
            return {'success': False, 'error': 'No target database'}

        target_engine = get_database_engine(target_db)
        kafka_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092,localhost:9094,localhost:9096'
        )

        logger.info(f"Starting continuous DDL consumer for config {config_id} (source: {source_type})")

        processor = None
        try:
            # Select appropriate processor based on source type
            if source_type in ('postgresql', 'postgres'):
                processor = PostgreSQLKafkaDDLProcessor(
                    replication_config=config,
                    target_engine=target_engine,
                    bootstrap_servers=kafka_servers,
                    auto_execute_destructive=True
                )
            else:
                # MySQL/MSSQL use KafkaDDLProcessor
                processor = KafkaDDLProcessor(
                    replication_config=config,
                    target_engine=target_engine,
                    bootstrap_servers=kafka_servers,
                    auto_execute_destructive=True
                )

            last_lock_refresh = time.time()

            # Continuous loop — exits when the Redis lock is deleted (by stop task or TTL expiry)
            while True:
                try:
                    # Process with short timeout for responsiveness
                    processed, errors = processor.process(timeout_sec=5, max_messages=10)

                    if errors > 0:
                        logger.warning(f"[Continuous] Config {config_id}: {processed} processed, {errors} errors")

                    # Small sleep to prevent tight loop when no messages
                    time.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error in continuous consumer for config {config_id}: {e}")
                    time.sleep(5)  # Back off on error

                # Every LOCK_REFRESH_INTERVAL seconds: refresh TTL and check if we should stop
                now = time.time()
                if now - last_lock_refresh >= _LOCK_REFRESH_INTERVAL:
                    if not _refresh_and_check_lock(config_id):
                        logger.info(f"Consumer lock gone for config {config_id} — stopping")
                        break
                    last_lock_refresh = now

        except Exception as e:
            logger.error(f"Continuous DDL consumer failed for config {config_id}: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
        finally:
            if processor:
                processor.close()
            logger.info(f"Continuous DDL consumer stopped for config {config_id}")

        return {'success': True, 'config_id': config_id}

    finally:
        _release_lock(config_id)


@shared_task
def stop_continuous_ddl_consumer(config_id: int):
    """Stop a running continuous DDL consumer by deleting its Redis lock."""
    if _is_consumer_running(config_id):
        _release_lock(config_id)
        logger.info(f"Stopping continuous DDL consumer for config {config_id}")
        return {'success': True}
    return {'success': False, 'error': 'Not running'}


@shared_task
def ensure_continuous_ddl_consumers():
    """
    Ensure continuous DDL consumers are running for all active replications.

    Called periodically by Celery Beat to auto-start/restart consumers.
    This keeps DDL sync always on.

    Supports:
    - MySQL/MSSQL: Schema history topic
    - PostgreSQL: ddl_capture.ddl_events topic (requires event triggers on source)
    """
    supported_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')

    active_configs = ReplicationConfig.objects.filter(
        status='active',
        is_active=True
    ).select_related('client_database')

    started = 0
    for config in active_configs:
        source_type = config.client_database.db_type.lower()
        if source_type not in supported_types:
            continue

        # Use Redis lock to check if a consumer is already running across all workers
        if not _is_consumer_running(config.id):
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

    DISABLED: PostgreSQL DDL is now handled by the continuous DDL consumer
    (PostgreSQLKafkaDDLProcessor) which consumes DDL events from Kafka.

    The schema comparison approach caused data loss on RENAME operations
    (detected as DROP + ADD instead of proper RENAME).
    """
    # PostgreSQL DDL handled by continuous DDL consumer - no schema sync needed
    logger.debug("PostgreSQL schema sync disabled - handled by continuous DDL consumer")
    return {'scheduled': 0}



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
            maybe_send_alert(
                None, 'kafka_down',
                subject="Kafka Connect Unreachable",
                body=f"Kafka Connect health check failed.\nError: {error}",
                connector_name='kafka_connect',
            )
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

            # Check DLQ for messages
            dlq_count = _get_dlq_count(config)
            if dlq_count > 0:
                maybe_send_alert(
                    config, 'dlq_messages',
                    subject=f"DLQ Has {dlq_count} Messages: {config.connector_name}",
                    body=(
                        f"{dlq_count} unprocessed message(s) found in the Dead Letter Queue.\n"
                        f"Topic: client_{config.client_database.client_id}.dlq\n"
                        f"These records failed to be written to the target database."
                    ),
                    connector_name=config.connector_name or '',
                )
            else:
                resolve_alert(config, 'dlq_messages', connector_name=config.connector_name or '')

        # Kafka Connect is reachable — resolve any open kafka_down alert
        resolve_alert(None, 'kafka_down', connector_name='kafka_connect')

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


def _get_dlq_count(config) -> int:
    """Return the number of messages in the DLQ topic for the given config. Returns 0 on error."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        client_id = config.client_database.client_id
        dlq_topic = f"client_{client_id}.dlq"
        kafka_servers = settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS']
        consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'jovo-dlq-inspector',
            'auto.offset.reset': 'earliest',
        })
        meta = consumer.list_topics(topic=dlq_topic, timeout=5)
        topic_meta = meta.topics.get(dlq_topic)
        total = 0
        if topic_meta and not topic_meta.error:
            for partition_id in topic_meta.partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(dlq_topic, partition_id), timeout=3
                )
                total += max(0, high - low)
        consumer.close()
        return total
    except Exception:
        return 0


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

        # Resolve any open missed-schedule alert now that the batch ran successfully
        resolve_alert(config, 'batch_missed', connector_name=config.connector_name or '')

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
            try:
                _cfg = ReplicationConfig.objects.get(id=replication_config_id)
                maybe_send_alert(
                    _cfg, 'batch_task_failed',
                    subject=f"Batch Sync Task Failed: {_cfg.connector_name or f'config #{replication_config_id}'}",
                    body=(
                        f"Batch sync task for config #{replication_config_id} exhausted all retries.\n"
                        f"Last error: {str(e)}"
                    ),
                    connector_name=_cfg.connector_name or '',
                )
            except Exception:
                pass

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


# ============================================================================
# ALERT TASKS
# ============================================================================

@shared_task
def check_batch_schedules():
    """
    Periodic task: Alert if a batch connector has missed its scheduled run.

    Grace period is 10 minutes — a batch job is considered missed only if
    next_batch_run is more than 10 minutes in the past with no completion.
    Runs every 10 minutes (configured in celery.py beat schedule).
    """
    overdue_configs = ReplicationConfig.objects.filter(
        is_active=True,
        processing_mode='batch',
        next_batch_run__isnull=False,
        next_batch_run__lt=timezone.now() - timedelta(minutes=10),
    ).select_related('client_database__client')

    alerted = 0
    for config in overdue_configs:
        overdue_by = timezone.now() - config.next_batch_run
        overdue_minutes = int(overdue_by.total_seconds() // 60)
        sent = maybe_send_alert(
            config, 'batch_missed',
            subject=f"Batch Missed Schedule: {config.connector_name or f'config #{config.id}'}",
            body=(
                f"Batch sync is overdue by {overdue_minutes} minutes.\n"
                f"Scheduled: {config.next_batch_run.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"The Celery task may not have run. Check the Celery worker and beat scheduler."
            ),
            connector_name=config.connector_name or '',
        )
        if sent:
            alerted += 1

    if alerted:
        logger.warning(f"check_batch_schedules: alerted {alerted} overdue batch connector(s)")

    return {'checked': overdue_configs.count(), 'alerted': alerted}


