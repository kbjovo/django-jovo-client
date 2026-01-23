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
            config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}"
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