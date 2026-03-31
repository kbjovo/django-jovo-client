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

# Pending-start flag: set before queuing a start task, cleared when the task begins.
# Prevents multiple ensure_continuous_ddl_consumers calls from each queuing a separate
# start_continuous_ddl_consumer task for the same config (task-storm on worker restart).
_CONSUMER_PENDING_PREFIX = 'ddl_consumer_pending'
_CONSUMER_PENDING_TTL = 30     # seconds; must be > worker task pickup latency


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


def _consumer_pending_key(config_id: int) -> str:
    return f'{_CONSUMER_PENDING_PREFIX}:{config_id}'


def _mark_consumer_pending(config_id: int) -> bool:
    """
    Atomically set the pending-start flag (SET NX EX).
    Returns True only if the flag was newly set (this caller "owns" the queuing).
    """
    try:
        r = _get_redis()
        return bool(r.set(_consumer_pending_key(config_id), '1', ex=_CONSUMER_PENDING_TTL, nx=True))
    except Exception as e:
        logger.warning(f"Redis pending flag set failed for config {config_id}: {e}")
        return False


def _clear_consumer_pending(config_id: int) -> None:
    """Clear the pending-start flag (called when the start task actually executes)."""
    try:
        _get_redis().delete(_consumer_pending_key(config_id))
    except Exception as e:
        logger.warning(f"Redis pending flag clear failed for config {config_id}: {e}")


def _is_consumer_active(config_id: int) -> bool:
    """Return True if the consumer is running OR a start task is already pending."""
    try:
        r = _get_redis()
        return bool(r.exists(_consumer_lock_key(config_id))) or bool(r.exists(_consumer_pending_key(config_id)))
    except Exception:
        return False


# ============================================================================
# REDIS-BASED SUPERVISOR LOCK
# Single supervisor task manages all configs via threads (one Celery slot total)
# ============================================================================

_SUPERVISOR_LOCK_KEY     = 'ddl_supervisor_lock'
_SUPERVISOR_LOCK_TTL     = 90    # seconds; must be > LOCK_REFRESH_INTERVAL
_SUPERVISOR_PENDING_KEY  = 'ddl_supervisor_pending'
_SUPERVISOR_PENDING_TTL  = 60    # seconds
_CONFIG_REFRESH_INTERVAL = 10.0  # re-query active configs every N seconds
_SUPERVISOR_POLL_INTERVAL = 5.0  # main-loop tick


def _acquire_supervisor_lock(task_id: str) -> bool:
    try:
        r = _get_redis()
        return bool(r.set(_SUPERVISOR_LOCK_KEY, task_id, ex=_SUPERVISOR_LOCK_TTL, nx=True))
    except Exception as e:
        logger.warning(f"Redis supervisor lock acquire failed: {e}")
        return False


def _release_supervisor_lock() -> None:
    try:
        _get_redis().delete(_SUPERVISOR_LOCK_KEY)
    except Exception as e:
        logger.warning(f"Redis supervisor lock release failed: {e}")


def _refresh_supervisor_lock() -> bool:
    try:
        return bool(_get_redis().expire(_SUPERVISOR_LOCK_KEY, _SUPERVISOR_LOCK_TTL))
    except Exception as e:
        logger.warning(f"Redis supervisor lock refresh failed: {e}")
        return True  # fail open


def _is_supervisor_running() -> bool:
    try:
        return bool(_get_redis().exists(_SUPERVISOR_LOCK_KEY))
    except Exception:
        return False


def _mark_supervisor_pending() -> bool:
    try:
        r = _get_redis()
        return bool(r.set(_SUPERVISOR_PENDING_KEY, '1', ex=_SUPERVISOR_PENDING_TTL, nx=True))
    except Exception as e:
        logger.warning(f"Redis supervisor pending flag set failed: {e}")
        return False


def _clear_supervisor_pending() -> None:
    try:
        _get_redis().delete(_SUPERVISOR_PENDING_KEY)
    except Exception as e:
        logger.warning(f"Redis supervisor pending flag clear failed: {e}")


def _is_supervisor_active() -> bool:
    try:
        r = _get_redis()
        return (
            bool(r.exists(_SUPERVISOR_LOCK_KEY))
            or bool(r.exists(_SUPERVISOR_PENDING_KEY))
        )
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
    - PostgreSQL: Kafka-based DDL processor (consumes ddl_capture.ddl_events topic)

    For types that also have a continuous consumer, this task acts as a batch
    fallback that runs only when the continuous consumer is not active.

    Args:
        config_id: ReplicationConfig ID
        auto_destructive: Auto-execute destructive operations (DROP COLUMN, etc.)

    Returns:
        Dict with processing results
    """
    from client.utils.ddl import KafkaDDLProcessor, PostgreSQLKafkaDDLProcessor, PostgreSQLSchemaSyncService

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
        kafka_servers = settings.DEBEZIUM_CONFIG.get(
            'KAFKA_BOOTSTRAP_SERVERS',
            'localhost:9092,localhost:9094,localhost:9096'
        )

        if source_type in ('mysql', 'mssql', 'sqlserver'):
            # Kafka-based processor for MySQL/MSSQL
            processor = KafkaDDLProcessor(
                replication_config=config,
                target_engine=target_engine,
                bootstrap_servers=kafka_servers,
                auto_execute_destructive=auto_destructive
            )
            processed, errors = processor.process(timeout_sec=30, max_messages=100)

        elif source_type in ('postgresql', 'postgres'):
            # Batch fallback: use PostgreSQLKafkaDDLProcessor when the continuous
            # consumer is not running (e.g. ddl_consumer queue at capacity).
            # The continuous consumer (start_continuous_ddl_consumer) is the preferred
            # path; this task acts as a catch-up run when it's unavailable.
            processor = PostgreSQLKafkaDDLProcessor(
                replication_config=config,
                target_engine=target_engine,
                bootstrap_servers=kafka_servers,
                auto_execute_destructive=auto_destructive
            )
            processed, errors = processor.process(timeout_sec=30, max_messages=100)

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

    All source types are skipped when their consumer is active (running or pending) —
    both the batch task and the supervisor-managed thread share the same Kafka consumer
    group, so running both simultaneously causes unnecessary rebalances with no benefit.
    The batch task fires only as a fallback when the supervisor thread is down.
    """
    supported_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')
    # All source types that have a continuous consumer (see start_continuous_ddl_consumer).
    # When the consumer is running we skip the batch task to avoid competing on the same
    # Kafka consumer group. The batch task fires as a fallback when the consumer is down.
    continuous_consumer_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')

    active_configs = ReplicationConfig.objects.filter(
        status='active',
        is_active=True
    ).select_related('client_database')

    scheduled = 0
    for config in active_configs:
        source_type = config.client_database.db_type.lower()
        if source_type not in supported_types:
            continue

        # Skip if a consumer is running OR pending for this config.
        # Using _is_consumer_active (not just _is_consumer_running) avoids firing
        # a batch task during the supervisor's startup window when the per-config
        # thread is pending but not yet holding the lock.
        if source_type in continuous_consumer_types and _is_consumer_active(config.id):
            continue

        process_ddl_changes.delay(config.id)
        scheduled += 1

    if scheduled > 0:
        logger.info(f"Scheduled DDL processing for {scheduled} configs")

    return {'scheduled': scheduled}


@shared_task(bind=True, queue='ddl_consumer', time_limit=None, soft_time_limit=None)
def start_ddl_supervisor(self):
    """
    Single supervisor task that manages DDL consumers for ALL active configs.

    Replaces N per-config start_continuous_ddl_consumer tasks with 1 Celery task
    that spawns per-config threads internally.  One worker slot serves every source
    database simultaneously regardless of how many configs exist.

    Thread lifecycle:
    - A thread is started for each active ReplicationConfig.
    - If a thread crashes it is restarted on the next poll tick (every 5 s).
    - If the per-config Redis lock is externally deleted (e.g. by the orchestrator
      to force a connector-version restart) the thread is stopped and restarted so
      it picks up the updated connector_version from the database.
    - Config changes (new configs added, configs deactivated) are detected every
      _CONFIG_REFRESH_INTERVAL seconds without restarting the whole supervisor.

    Stopped by: stop_ddl_supervisor() or by deleting the supervisor lock key.
    """
    import threading
    import time
    from django.db import close_old_connections
    from client.utils.ddl import KafkaDDLProcessor, PostgreSQLKafkaDDLProcessor

    task_id = self.request.id or 'ddl-supervisor'
    _clear_supervisor_pending()

    if not _acquire_supervisor_lock(task_id):
        logger.info("DDL supervisor already running (lock held)")
        return {'success': False, 'error': 'Already running'}

    supported_types = ('mysql', 'mssql', 'sqlserver', 'postgresql', 'postgres')
    threads: dict = {}      # config_id -> Thread
    stop_events: dict = {}  # config_id -> threading.Event

    def _run_config(config_id: int, stop_event: threading.Event) -> None:
        """Thread target: owns the processor loop for one ReplicationConfig."""
        # Each thread needs its own Django DB connection.
        close_old_connections()
        logger.info(f"[Supervisor] DDL thread starting for config {config_id}")
        processor = None
        try:
            config = ReplicationConfig.objects.get(id=config_id)
            source_type = config.client_database.db_type.lower()
            target_db = (
                config.client_database.client
                .client_databases.filter(is_target=True)
                .first()
            )
            if not target_db:
                logger.error(f"[Supervisor] No target database for config {config_id}")
                return

            target_engine = get_database_engine(target_db)
            kafka_servers = settings.DEBEZIUM_CONFIG.get(
                'KAFKA_BOOTSTRAP_SERVERS',
                'localhost:9092,localhost:9094,localhost:9096',
            )

            if source_type in ('postgresql', 'postgres'):
                processor = PostgreSQLKafkaDDLProcessor(
                    replication_config=config,
                    target_engine=target_engine,
                    bootstrap_servers=kafka_servers,
                    auto_execute_destructive=True,
                )
            else:
                processor = KafkaDDLProcessor(
                    replication_config=config,
                    target_engine=target_engine,
                    bootstrap_servers=kafka_servers,
                    auto_execute_destructive=True,
                )

            while not stop_event.is_set():
                try:
                    # Long-poll: Kafka blocks up to timeout_sec when the topic is
                    # empty, so no extra sleep is needed between iterations.
                    processed, errors = processor.process(timeout_sec=10, max_messages=50)
                    if errors:
                        logger.warning(
                            f"[Supervisor] Config {config_id}: "
                            f"{processed} processed, {errors} errors"
                        )
                except Exception as e:
                    logger.error(
                        f"[Supervisor] Error in consumer for config {config_id}: {e}",
                        exc_info=True,
                    )
                    stop_event.wait(timeout=5)  # back off before retrying

        except ReplicationConfig.DoesNotExist:
            logger.error(f"[Supervisor] Config {config_id} not found — thread exiting")
        except Exception as e:
            logger.error(
                f"[Supervisor] Thread for config {config_id} crashed: {e}",
                exc_info=True,
            )
        finally:
            if processor:
                try:
                    processor.close()
                except Exception:
                    pass
            # Release per-config lock so schedule_ddl_processing_all can fire as fallback.
            _release_lock(config_id)
            logger.info(f"[Supervisor] DDL thread stopped for config {config_id}")

    def _start_thread(config_id: int) -> None:
        """Acquire per-config lock and start a processor thread."""
        try:
            r = _get_redis()
            # SET (not SET NX) — overwrite any stale lock from a previous run.
            r.set(_consumer_lock_key(config_id), task_id, ex=_CONSUMER_LOCK_TTL)
        except Exception:
            pass
        stop_events[config_id] = threading.Event()
        t = threading.Thread(
            target=_run_config,
            args=(config_id, stop_events[config_id]),
            name=f"ddl-config-{config_id}",
            daemon=True,
        )
        t.start()
        threads[config_id] = t
        logger.info(f"[Supervisor] Started DDL thread for config {config_id}")

    def _stop_thread(config_id: int) -> None:
        """Signal a thread to stop and wait for it."""
        if config_id in stop_events:
            stop_events[config_id].set()
        if config_id in threads:
            threads[config_id].join(timeout=15)
            del threads[config_id]
        if config_id in stop_events:
            del stop_events[config_id]
        _release_lock(config_id)

    try:
        r = _get_redis()
        last_lock_refresh = time.time()
        last_config_refresh = 0.0  # force immediate query on first tick

        while True:
            time.sleep(_SUPERVISOR_POLL_INTERVAL)
            now = time.time()

            # 1. Refresh supervisor lock; exit if it was externally deleted.
            if now - last_lock_refresh >= _LOCK_REFRESH_INTERVAL:
                if not _refresh_supervisor_lock():
                    logger.info("[Supervisor] Supervisor lock gone — shutting down")
                    break
                last_lock_refresh = now

            # 2. Check per-config locks for running threads.
            #    If a lock is gone (deleted externally, e.g. by the orchestrator to
            #    force a restart with a new connector_version), stop the thread; it
            #    will be restarted with fresh DB config in the next config-refresh step.
            for config_id in list(threads.keys()):
                t = threads[config_id]
                if not t.is_alive():
                    # Thread crashed — will be restarted in config-refresh step.
                    del threads[config_id]
                    if config_id in stop_events:
                        del stop_events[config_id]
                    continue
                try:
                    if r.exists(_consumer_lock_key(config_id)):
                        r.expire(_consumer_lock_key(config_id), _CONSUMER_LOCK_TTL)
                    else:
                        logger.info(
                            f"[Supervisor] Lock released for config {config_id} "
                            "— restarting thread (connector version change?)"
                        )
                        stop_events[config_id].set()
                        threads[config_id].join(timeout=15)
                        del threads[config_id]
                        del stop_events[config_id]
                except Exception as e:
                    logger.warning(f"[Supervisor] Redis error for config {config_id}: {e}")

            # 3. Periodically re-query active configs to start/stop threads.
            if now - last_config_refresh < _CONFIG_REFRESH_INTERVAL:
                continue
            last_config_refresh = now

            try:
                close_old_connections()
                active_ids = {
                    c.id
                    for c in ReplicationConfig.objects.filter(
                        status='active', is_active=True,
                    ).select_related('client_database')
                    if c.client_database.db_type.lower() in supported_types
                }
            except Exception as e:
                logger.error(f"[Supervisor] DB query failed: {e}")
                continue

            # Start threads for new or crashed configs.
            for config_id in active_ids:
                if config_id not in threads or not threads[config_id].is_alive():
                    _start_thread(config_id)

            # Stop threads for configs that are no longer active.
            for config_id in list(threads.keys()):
                if config_id not in active_ids:
                    logger.info(f"[Supervisor] Config {config_id} deactivated — stopping thread")
                    _stop_thread(config_id)

    except Exception as e:
        logger.error(f"[Supervisor] Fatal error: {e}", exc_info=True)
    finally:
        logger.info(f"[Supervisor] Shutting down {len(threads)} thread(s)")
        for config_id in list(threads.keys()):
            _stop_thread(config_id)
        _release_supervisor_lock()
        logger.info("[Supervisor] DDL supervisor stopped")

    return {'success': True}


@shared_task
def stop_ddl_supervisor():
    """Stop the DDL supervisor by releasing its lock."""
    if _is_supervisor_running():
        _release_supervisor_lock()
        logger.info("Stopping DDL supervisor")
        return {'success': True}
    return {'success': False, 'error': 'Supervisor not running'}


@shared_task(bind=True, queue='ddl_consumer', time_limit=None, soft_time_limit=None)
def start_continuous_ddl_consumer(self, config_id: int):
    """
    Start a continuous Kafka consumer for a single config.

    Legacy per-config consumer kept for manual use or overrides.
    The preferred path is start_ddl_supervisor() which handles all configs
    in one task via threads.  Use stop_continuous_ddl_consumer() to stop it.

    Supports:
    - MySQL/MSSQL: Uses KafkaDDLProcessor (schema history topic)
    - PostgreSQL: Uses PostgreSQLKafkaDDLProcessor (ddl_capture.ddl_events topic)
    """
    from client.utils.ddl import KafkaDDLProcessor, PostgreSQLKafkaDDLProcessor
    import time

    task_id = self.request.id or f'consumer-{config_id}'

    # Clear the pending flag now that this task is executing.
    # This allows ensure_continuous_ddl_consumers to queue a new start task
    # if needed after this one exits (e.g., after a crash).
    _clear_consumer_pending(config_id)

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
                    # Long-poll: Kafka blocks up to timeout_sec when the topic is
                    # empty, returning immediately when messages arrive.
                    # No extra sleep needed — the poll itself acts as the idle wait.
                    processed, errors = processor.process(timeout_sec=10, max_messages=50)

                    if errors > 0:
                        logger.warning(f"[Continuous] Config {config_id}: {processed} processed, {errors} errors")

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
    Ensure the DDL supervisor is running.

    Called periodically by Celery Beat.  Starts start_ddl_supervisor if neither
    the supervisor nor a pending-start flag is present.  _mark_supervisor_pending
    is atomic (SET NX) so concurrent calls only queue one supervisor task.

    The supervisor manages all active configs internally via threads, so a single
    Celery task slot handles every source type simultaneously.
    """
    if not _is_supervisor_active():
        if _mark_supervisor_pending():
            logger.info("DDL supervisor not running — starting")
            start_ddl_supervisor.delay()
            return {'started': 1}
    return {'started': 0}


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

        # Record sync start time so the monitor page timer survives page refreshes
        config.batch_sync_started_at = timezone.now()
        config.save(update_fields=['batch_sync_started_at'])

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
        # batch_sync_started_at already cleared by pause_connector() above

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


