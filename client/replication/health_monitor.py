"""
Health monitoring for replication system.

Periodic task that checks health of all active replications and
automatically fixes issues when possible.
"""

import logging
from datetime import timedelta
from django.utils import timezone
from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name='client.replication.monitor_replication_health')
def monitor_replication_health():
    """
    Monitor health of all active replications.

    Runs every 1 minute to check:
    1. Connector state (RUNNING/FAILED/PAUSED)
    2. Consumer heartbeat (is consumer alive?)
    3. Auto-fix issues when possible

    This is a Celery periodic task configured in settings.py
    """
    from client.models import ReplicationConfig
    from .orchestrator import ReplicationOrchestrator

    logger.info("=" * 60)
    logger.info("HEALTH MONITORING - Starting health check")
    logger.info("=" * 60)

    # Get all active replications
    active_configs = ReplicationConfig.objects.filter(is_active=True)
    total_count = active_configs.count()

    if total_count == 0:
        logger.info("No active replications to monitor")
        return

    logger.info(f"Monitoring {total_count} active replications...")

    healthy_count = 0
    degraded_count = 0
    failed_count = 0
    fixed_count = 0

    for config in active_configs:
        try:
            orchestrator = ReplicationOrchestrator(config)
            status = orchestrator.get_unified_status()

            overall_health = status['overall']
            connector_health = status['connector']['healthy']
            consumer_health = status['consumer']['healthy']

            logger.info(f"[{config.connector_name}] Health: {overall_health} "
                       f"(Connector: {'✓' if connector_health else '✗'}, "
                       f"Consumer: {'✓' if consumer_health else '✗'})")

            # Track health statistics
            if overall_health == 'healthy':
                healthy_count += 1
            elif overall_health == 'degraded':
                degraded_count += 1
                # Try to fix degraded state
                if _try_fix_degraded(config, status):
                    fixed_count += 1
            else:
                failed_count += 1
                # Update config to error state
                config.status = 'error'
                config.save()

        except Exception as e:
            logger.error(f"[{config.connector_name}] Error monitoring health: {e}")
            failed_count += 1

    # Summary
    logger.info("=" * 60)
    logger.info(f"HEALTH MONITORING - Summary:")
    logger.info(f"  Total: {total_count}")
    logger.info(f"  Healthy: {healthy_count}")
    logger.info(f"  Degraded: {degraded_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"  Auto-fixed: {fixed_count}")
    logger.info("=" * 60)


def _try_fix_degraded(config, status):
    """
    Try to automatically fix degraded replication.

    Returns:
        True if fix was attempted, False otherwise
    """
    connector_health = status['connector']['healthy']
    consumer_health = status['consumer']['healthy']

    # Case 1: Connector healthy, consumer unhealthy
    if connector_health and not consumer_health:
        return _fix_consumer(config, status['consumer'])

    # Case 2: Consumer healthy, connector unhealthy
    elif consumer_health and not connector_health:
        return _fix_connector(config, status['connector'])

    return False


def _fix_consumer(config, consumer_status):
    """
    Try to fix unhealthy consumer.

    Checks:
    1. Is heartbeat stale? → Restart consumer
    2. Is consumer state ERROR? → Restart consumer

    Returns:
        True if restart attempted, False otherwise
    """
    from client.tasks import start_kafka_consumer

    consumer_state = consumer_status['state']
    heartbeat_recent = consumer_status['heartbeat_recent']

    # Check if consumer is stuck (no recent heartbeat)
    if consumer_state == 'RUNNING' and not heartbeat_recent:
        logger.warning(f"[{config.connector_name}] Consumer heartbeat stale, restarting...")

        # Restart consumer
        start_kafka_consumer.apply_async(args=[config.id])

        logger.info(f"[{config.connector_name}] ✓ Consumer restart triggered")
        return True

    # Check if consumer is in error state
    elif consumer_state == 'ERROR':
        logger.warning(f"[{config.connector_name}] Consumer in ERROR state, restarting...")

        # Restart consumer
        start_kafka_consumer.apply_async(args=[config.id])

        logger.info(f"[{config.connector_name}] ✓ Consumer restart triggered")
        return True

    return False


def _fix_connector(config, connector_status):
    """
    Try to fix unhealthy connector.

    Checks:
    1. Is connector PAUSED? → Resume it
    2. Is connector FAILED? → Restart it

    Returns:
        True if fix attempted, False otherwise
    """
    from client.utils.debezium_manager import DebeziumConnectorManager

    connector_state = connector_status['state']
    manager = DebeziumConnectorManager()

    # Check if connector is paused
    if connector_state == 'PAUSED':
        logger.warning(f"[{config.connector_name}] Connector is PAUSED, resuming...")

        try:
            manager.resume_connector(config.connector_name)
            logger.info(f"[{config.connector_name}] ✓ Connector resumed")
            return True
        except Exception as e:
            logger.error(f"[{config.connector_name}] Failed to resume connector: {e}")
            return False

    # Check if connector is failed
    elif connector_state == 'FAILED':
        logger.warning(f"[{config.connector_name}] Connector is FAILED, restarting...")

        try:
            manager.restart_connector(config.connector_name)
            logger.info(f"[{config.connector_name}] ✓ Connector restarted")
            return True
        except Exception as e:
            logger.error(f"[{config.connector_name}] Failed to restart connector: {e}")
            return False

    return False


@shared_task(name='client.replication.check_consumer_heartbeat')
def check_consumer_heartbeat():
    """
    Check for stale consumer heartbeats and restart if needed.

    Runs every 2 minutes to check if consumers are alive.
    If heartbeat is older than 2 minutes, restarts the consumer.
    """
    from client.models import ReplicationConfig
    from client.tasks import start_kafka_consumer

    logger.debug("Checking consumer heartbeats...")

    # Get all active configs with consumer running
    active_configs = ReplicationConfig.objects.filter(
        is_active=True,
        status='active'
    )

    for config in active_configs:
        try:
            last_heartbeat = getattr(config, 'consumer_last_heartbeat', None)

            if not last_heartbeat:
                logger.warning(f"[{config.connector_name}] No heartbeat recorded, restarting consumer...")
                start_kafka_consumer.apply_async(args=[config.id])
                continue

            # Check if heartbeat is stale (> 2 minutes)
            time_since_heartbeat = timezone.now() - last_heartbeat
            if time_since_heartbeat > timedelta(minutes=2):
                logger.warning(
                    f"[{config.connector_name}] Heartbeat stale "
                    f"({time_since_heartbeat.total_seconds():.0f}s ago), restarting consumer..."
                )
                start_kafka_consumer.apply_async(args=[config.id])

        except Exception as e:
            logger.error(f"[{config.connector_name}] Error checking heartbeat: {e}")