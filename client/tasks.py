"""
Celery tasks for CDC replication using JDBC Sink Connectors
"""
import logging
from celery import shared_task
from django.utils import timezone
from datetime import timedelta

from client.models import Client
from client.models.replication import ReplicationConfig
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from client.utils.notification_utils import send_error_notification

logger = logging.getLogger(__name__)


# ============================================================================
# TASKS
# ============================================================================

@shared_task(bind=True, max_retries=3)
def create_debezium_connector(self, replication_config_id):
    """
    Task: Create Debezium source connector for a replication config
    """
    try:
        logger.info(f"Creating Debezium connector for ReplicationConfig {replication_config_id}")

        config = ReplicationConfig.objects.get(id=replication_config_id)

        from jovoclient.utils.debezium.connector_templates import (
            generate_connector_name,
            get_connector_config_for_database
        )

        # Generate connector configuration
        client = config.client_database.client
        db_config = config.client_database

        connector_name = generate_connector_name(client, db_config)

        # Get tables to replicate
        enabled_tables = config.table_mappings.filter(is_enabled=True)
        tables_list = [tm.source_table for tm in enabled_tables]

        if not tables_list:
            logger.warning(f"No tables enabled for replication in config {replication_config_id}")
            return {'success': False, 'error': 'No tables enabled'}

        logger.info(f"Tables to replicate: {tables_list}")

        # Generate connector config
        connector_config = get_connector_config_for_database(
            db_config=db_config,
            replication_config=config,
            tables_whitelist=tables_list,
            kafka_bootstrap_servers='kafka-1:29092,kafka-2:29092,kafka-3:29092',
            schema_registry_url='http://schema-registry:8081'
        )

        if not connector_config:
            raise Exception("Failed to generate connector configuration")

        # Create connector via Debezium Manager
        manager = DebeziumConnectorManager()

        # Check Kafka Connect health first
        is_healthy, error = manager.check_kafka_connect_health()
        if not is_healthy:
            raise Exception(f"Kafka Connect is not healthy: {error}")

        success, error = manager.create_connector(
            connector_name=connector_name,
            config=connector_config,
            notify_on_error=True
        )

        if success:
            # Update replication config
            config.connector_name = connector_name
            # Topic prefix includes version for JMX uniqueness
            config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{config.connector_version}"
            config.status = 'configured'
            config.is_active = False
            config.save()

            logger.info(f"Successfully created connector: {connector_name}")

            return {
                'success': True,
                'connector_name': connector_name,
                'tables': len(tables_list)
            }
        else:
            raise Exception(f"Failed to create connector: {error}")

    except ReplicationConfig.DoesNotExist:
        logger.error(f"ReplicationConfig {replication_config_id} not found")
        return {'success': False, 'error': 'Config not found'}

    except Exception as e:
        logger.error(f"Error creating connector: {e}", exc_info=True)

        # Retry with exponential backoff
        try:
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for connector creation")

        return {'success': False, 'error': str(e)}


@shared_task
def delete_debezium_connector(connector_name, replication_config_id=None, delete_topics=True, clear_offsets=True):
    """
    Task: Delete a Debezium connector and optionally its topics and offsets
    """
    try:
        logger.info(f"Deleting connector: {connector_name} (delete_topics={delete_topics}, clear_offsets={clear_offsets})")

        # Delete connector
        manager = DebeziumConnectorManager()
        success, error = manager.delete_connector(connector_name, notify=True, delete_topics=delete_topics)

        if success:
            logger.info(f"Successfully deleted connector: {connector_name}")

            # Update config
            if replication_config_id:
                try:
                    config = ReplicationConfig.objects.get(id=replication_config_id)
                    config.connector_name = None
                    config.status = 'disabled'
                    config.is_active = False
                    config.save()
                except:
                    pass

            return {'success': True}
        else:
            raise Exception(f"Failed to delete connector: {error}")

    except Exception as e:
        logger.error(f"Error deleting connector: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def restart_replication(replication_config_id):
    """
    Task: Restart replication by restarting the Debezium connector
    """
    try:
        logger.info(f"Restarting replication for config {replication_config_id}")

        config = ReplicationConfig.objects.get(id=replication_config_id)

        if config.connector_name:
            # Restart connector
            manager = DebeziumConnectorManager()
            success, error = manager.restart_connector(config.connector_name)

            if not success:
                raise Exception(f"Failed to restart connector: {error}")

        config.status = 'active'
        config.is_active = True
        config.save()

        logger.info(f"Successfully restarted replication")

        return {'success': True}

    except Exception as e:
        logger.error(f"Error restarting replication: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def force_resnapshot(replication_config_id):
    """
    Task: Force a fresh snapshot by clearing offsets and restarting connector
    """
    try:
        logger.info(f"Forcing resnapshot for config {replication_config_id}")

        config = ReplicationConfig.objects.get(id=replication_config_id)

        if not config.connector_name:
            raise Exception("No connector configured for this replication config")

        connector_name = config.connector_name

        # Step 1: Delete connector
        logger.info(f"Step 1/2: Deleting connector...")
        manager = DebeziumConnectorManager()
        success, error = manager.delete_connector(connector_name, notify=False, delete_topics=False)
        if not success:
            raise Exception(f"Failed to delete connector: {error}")

        # Step 2: Recreate connector
        logger.info(f"Step 2/2: Recreating connector for fresh snapshot...")
        from client.replication.orchestrator import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)

        result = orchestrator.create_debezium_connector()
        if not result['success']:
            raise Exception(f"Failed to recreate connector: {result.get('error')}")

        config.status = 'active'
        config.is_active = True
        config.save()

        logger.info(f"Successfully forced resnapshot for {connector_name}")

        return {
            'success': True,
            'message': 'Connector deleted and recreated with fresh snapshot.'
        }

    except Exception as e:
        logger.error(f"Error forcing resnapshot: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


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


# Import health monitor task (connector monitoring only)
from client.replication.health_monitor import monitor_replication_health


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