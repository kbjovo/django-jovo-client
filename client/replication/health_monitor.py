"""
Health monitoring for replication system.

Periodic task that checks health of all active replications and
automatically fixes issues when possible.

Uses JDBC Sink Connectors for data replication (no Celery-based consumers).
"""

import logging
from celery import shared_task
from client.utils.notification_utils import maybe_send_alert, resolve_alert

logger = logging.getLogger(__name__)


@shared_task(name='client.replication.monitor_replication_health')
def monitor_replication_health():
    """
    Monitor health of all active replications.

    Runs every 1 minute to check:
    1. Source connector state (RUNNING/FAILED/PAUSED)
    2. Sink connector state (RUNNING/FAILED/PAUSED)
    3. Auto-fix issues when possible

    This is a Celery periodic task configured in celery.py
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
            source_connector_health = status['source_connector']['healthy']
            sink_connector_health = status['sink_connector']['healthy']

            logger.info(f"[{config.connector_name}] Health: {overall_health} "
                       f"(Source: {'✓' if source_connector_health else '✗'}, "
                       f"Sink: {'✓' if sink_connector_health else '✗'})")

            # Track health statistics
            if overall_health == 'healthy':
                healthy_count += 1
                if config.status == 'error':
                    config.status = 'active'
                    config.save(update_fields=['status'])
                # Resolve any open alerts — connector is back to healthy
                resolve_alert(config, 'source_failed',     connector_name=config.connector_name or '')
                resolve_alert(config, 'connector_missing', connector_name=config.connector_name or '')
                resolve_alert(config, 'sink_failed',       connector_name=config.sink_connector_name or '')

            elif overall_health == 'degraded':
                degraded_count += 1
                # Send alerts for specific failure states
                _check_and_alert(config, status['source_connector'], status['sink_connector'])
                # Try to fix degraded state
                if _try_fix_degraded(config, status):
                    fixed_count += 1

            else:
                failed_count += 1
                # Send alerts for specific failure states
                _check_and_alert(config, status['source_connector'], status['sink_connector'])
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


def _has_failed_task(status):
    """True if any task in the connector status is in the FAILED state."""
    return any(t.get('state') == 'FAILED' for t in status.get('tasks', []))


def _check_and_alert(config, source_status, sink_status):
    """
    Fire transition-based alert emails for FAILED / NOT_FOUND connector states.

    A connector whose top-level state is RUNNING but which has a FAILED task
    (the usual shape of a source/sink DB outage) is treated as failed too.
    maybe_send_alert ensures only one email per incident (no spam).
    """
    src = config.connector_name or ''
    snk = config.sink_connector_name or ''

    source_state = source_status.get('state')
    sink_state = sink_status.get('state')
    source_failed = source_state == 'FAILED' or _has_failed_task(source_status)
    sink_failed = sink_state == 'FAILED' or _has_failed_task(sink_status)

    if source_failed:
        maybe_send_alert(
            config, 'source_failed',
            subject=f"Source Connector FAILED: {src}",
            body="Source connector (or its task) entered FAILED state — the source "
                 "database may be unresponsive. Check Kafka Connect logs for details.",
            connector_name=src,
        )
    elif source_state == 'NOT_FOUND':
        maybe_send_alert(
            config, 'connector_missing',
            subject=f"Connector Missing: {src}",
            body="Source connector not found in Kafka Connect. It may have been deleted externally.",
            connector_name=src,
        )

    if sink_failed and snk:
        maybe_send_alert(
            config, 'sink_failed',
            subject=f"Sink Connector FAILED: {snk}",
            body="Sink connector (or its task) entered FAILED state. Check Kafka Connect logs for details.",
            connector_name=snk,
        )


def _try_fix_degraded(config, status):
    """
    Try to automatically fix degraded replication.

    Returns:
        True if fix was attempted, False otherwise
    """
    source_connector_health = status['source_connector']['healthy']
    sink_connector_health = status['sink_connector']['healthy']

    # Case 1: Source healthy, sink unhealthy
    if source_connector_health and not sink_connector_health:
        return _fix_sink_connector(config, status['sink_connector'])

    # Case 2: Sink healthy, source unhealthy
    elif sink_connector_health and not source_connector_health:
        return _fix_source_connector(config, status['source_connector'])

    # Case 3: Both unhealthy - try to fix source first
    elif not source_connector_health and not sink_connector_health:
        source_fixed = _fix_source_connector(config, status['source_connector'])
        if source_fixed:
            return True
        return _fix_sink_connector(config, status['sink_connector'])

    return False


def _fix_sink_connector(config, sink_status):
    """
    Try to fix unhealthy sink connector.

    Checks:
    1. Is connector PAUSED? → Resume it
    2. Is connector FAILED? → Restart it
    3. Is connector NOT_CONFIGURED? → Skip (no sink connector configured)

    Returns:
        True if fix attempted, False otherwise
    """
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

    sink_state = sink_status['state']

    # Skip if no sink connector configured
    if sink_state == 'NOT_CONFIGURED':
        return False

    if not config.sink_connector_name:
        return False

    manager = DebeziumConnectorManager()

    # Check if connector is paused. Connectors stream continuously now, so a paused
    # sink is unintended and should be resumed.
    if sink_state == 'PAUSED':
        logger.warning(f"[{config.sink_connector_name}] Sink connector is PAUSED, resuming...")

        try:
            manager.resume_connector(config.sink_connector_name)
            logger.info(f"[{config.sink_connector_name}] ✓ Sink connector resumed")
            return True
        except Exception as e:
            logger.error(f"[{config.sink_connector_name}] Failed to resume sink connector: {e}")
            return False

    # Connector-level FAILED → restart the whole connector
    elif sink_state == 'FAILED':
        logger.warning(f"[{config.sink_connector_name}] Sink connector is FAILED, restarting...")

        try:
            manager.restart_connector(config.sink_connector_name)
            logger.info(f"[{config.sink_connector_name}] ✓ Sink connector restarted")
            return True
        except Exception as e:
            logger.error(f"[{config.sink_connector_name}] Failed to restart sink connector: {e}")
            return False

    # Connector RUNNING but one or more tasks FAILED (e.g. target DB outage) →
    # restart just the failed task(s).
    return _restart_failed_tasks(config, config.sink_connector_name, sink_status)


def _fix_source_connector(config, connector_status):
    """
    Try to fix unhealthy connector.

    Checks:
    1. Is connector PAUSED? → Resume it
    2. Is connector FAILED? → Restart it

    Returns:
        True if fix attempted, False otherwise
    """
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

    connector_state = connector_status['state']
    manager = DebeziumConnectorManager()

    # Check if connector is paused. The SOURCE connector now runs continuously in all
    # modes (batch windows are driven by pausing the SINK, not the source), so a paused
    # source is always unintended and should be resumed.
    if connector_state == 'PAUSED':
        logger.warning(f"[{config.connector_name}] Connector is PAUSED, resuming...")

        try:
            manager.resume_connector(config.connector_name)
            logger.info(f"[{config.connector_name}] ✓ Connector resumed")
            return True
        except Exception as e:
            logger.error(f"[{config.connector_name}] Failed to resume connector: {e}")
            return False

    # Connector-level FAILED → restart the whole connector
    elif connector_state == 'FAILED':
        logger.warning(f"[{config.connector_name}] Connector is FAILED, restarting...")

        try:
            manager.restart_connector(config.connector_name)
            logger.info(f"[{config.connector_name}] ✓ Connector restarted")
            return True
        except Exception as e:
            logger.error(f"[{config.connector_name}] Failed to restart connector: {e}")
            return False

    # Connector RUNNING but one or more tasks FAILED (e.g. source DB went
    # unresponsive / lost the binlog connection) → restart just the failed task(s).
    return _restart_failed_tasks(config, config.connector_name, connector_status)


def _restart_failed_tasks(config, connector_name, status) -> bool:
    """
    Restart every FAILED task of a connector whose top-level state is otherwise
    RUNNING. This is the recovery path for a source/target DB outage, where the
    connector stays RUNNING while its task drops into FAILED.

    Delegates to the orchestrator's canonical restart_failed_tasks() (handles the
    task-level restart calls, error aggregation and audit logging).

    Returns True if the restart succeeded.
    """
    if not _has_failed_task(status):
        return False

    from .orchestrator import ReplicationOrchestrator

    success, message = ReplicationOrchestrator(config).restart_failed_tasks(connector_name)
    if success:
        logger.info(f"[{connector_name}] ✓ {message}")
    else:
        logger.error(f"[{connector_name}] Failed to restart tasks: {message}")
    return success