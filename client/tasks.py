"""
Celery tasks for CDC replication
"""
import logging
from celery import shared_task
from django.utils import timezone
from datetime import timedelta

from client.models import Client
from client.models.replication import ReplicationConfig
from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.kafka_consumer import DebeziumCDCConsumer
from client.utils.database_utils import get_database_engine
from client.utils.notification_utils import send_error_notification

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def create_debezium_connector(self, replication_config_id):
    """
    Task: Create Debezium connector for a replication config
    """
    try:
        logger.info(f"🚀 Creating Debezium connector for ReplicationConfig {replication_config_id}")
        
        config = ReplicationConfig.objects.get(id=replication_config_id)
        
        from client.utils.connector_templates import (
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
            logger.warning(f"⚠️ No tables enabled for replication in config {replication_config_id}")
            return {'success': False, 'error': 'No tables enabled'}
        
        logger.info(f"📋 Tables to replicate: {tables_list}")
        
        # Generate connector config
        connector_config = get_connector_config_for_database(
            db_config=db_config,
            replication_config=config,
            tables_whitelist=tables_list,
            kafka_bootstrap_servers='kafka:29092',  # Docker internal
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
            config.kafka_topic_prefix = f"client_{client.id}"
            config.status = 'active'
            config.is_active = True
            config.last_sync_at = timezone.now()
            config.save()
            
            logger.info(f"✅ Successfully created connector: {connector_name}")
            
            # Start consumer task (run in background)
            start_kafka_consumer.apply_async(
                args=[replication_config_id],
                countdown=5  # Start after 5 seconds
            )
            
            return {
                'success': True,
                'connector_name': connector_name,
                'tables': len(tables_list)
            }
        else:
            raise Exception(f"Failed to create connector: {error}")
            
    except ReplicationConfig.DoesNotExist:
        logger.error(f"❌ ReplicationConfig {replication_config_id} not found")
        return {'success': False, 'error': 'Config not found'}
        
    except Exception as e:
        logger.error(f"❌ Error creating connector: {e}", exc_info=True)
        
        # Retry with exponential backoff
        try:
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        except self.MaxRetriesExceededError:
            logger.error(f"❌ Max retries exceeded for connector creation")
            send_error_notification(
                error_title="Connector Creation Failed",
                error_message=str(e),
                context={'replication_config_id': replication_config_id}
            )
        
        return {'success': False, 'error': str(e)}


@shared_task(bind=True)
def start_kafka_consumer(self, replication_config_id):
    """
    Task: Start Kafka consumer for a replication config
    Runs continuously to consume CDC events
    """
    try:
        logger.info(f"🎧 Starting Kafka consumer for ReplicationConfig {replication_config_id}")
        
        config = ReplicationConfig.objects.get(id=replication_config_id)
        
        if not config.is_active or not config.connector_name:
            logger.warning(f"⚠️ Config {replication_config_id} is not active or has no connector")
            return {'success': False, 'error': 'Config not ready'}
        
        # Get target database
        client = config.client_database.client
        target_db = client.get_target_database()
        
        if not target_db:
            raise Exception("No target database found")
        
        logger.info(f"🎯 Target database: {target_db.database_name}")
        
        # Create target engine
        target_engine = get_database_engine(target_db)
        
        # Determine topics to subscribe to
        kafka_topic_prefix = config.kafka_topic_prefix or f"client_{client.id}"
        source_db_name = config.client_database.database_name
        
        # Get all table topics
        enabled_tables = config.table_mappings.filter(is_enabled=True)
        topics = [
            f"{kafka_topic_prefix}.{source_db_name}.{tm.source_table}"
            for tm in enabled_tables
        ]
        
        logger.info(f"📡 Subscribing to topics: {topics}")
        
        # Create and start consumer
        consumer = DebeziumCDCConsumer(
            consumer_group_id=f"cdc_consumer_{client.id}_{config.id}",
            topics=topics,
            target_engine=target_engine,
            bootstrap_servers='localhost:9092',  # External Kafka address
            auto_offset_reset='earliest'
        )
        
        # Consume messages (blocks until stopped)
        # This will run indefinitely until task is revoked
        logger.info(f"🔄 Starting message consumption...")
        consumer.consume(max_messages=None, timeout=1.0)
        
        logger.info(f"⏹️ Consumer stopped for config {replication_config_id}")
        
        stats = consumer.get_stats()
        logger.info(f"📊 Consumer stats: {stats}")
        
        # Update config with last sync
        config.last_sync_at = timezone.now()
        config.save()
        
        return {'success': True, 'stats': stats}
        
    except ReplicationConfig.DoesNotExist:
        logger.error(f"❌ ReplicationConfig {replication_config_id} not found")
        return {'success': False, 'error': 'Config not found'}
        
    except Exception as e:
        logger.error(f"❌ Error in Kafka consumer: {e}", exc_info=True)
        
        # Update config status
        try:
            config = ReplicationConfig.objects.get(id=replication_config_id)
            config.status = 'error'
            config.save()
        except:
            pass
        
        send_error_notification(
            error_title="Kafka Consumer Failed",
            error_message=str(e),
            context={'replication_config_id': replication_config_id}
        )
        
        return {'success': False, 'error': str(e)}


@shared_task
def stop_kafka_consumer(replication_config_id):
    """
    Task: Stop Kafka consumer for a replication config
    """
    try:
        logger.info(f"⏸️ Stopping Kafka consumer for ReplicationConfig {replication_config_id}")
        
        # Revoke running consumer tasks
        from celery import current_app
        
        # Get active tasks
        inspect = current_app.control.inspect()
        active_tasks = inspect.active()
        
        revoked_count = 0
        
        if active_tasks:
            for worker, tasks in active_tasks.items():
                for task in tasks:
                    if (task['name'] == 'client.tasks.start_kafka_consumer' and 
                        str(replication_config_id) in str(task['args'])):
                        
                        current_app.control.revoke(task['id'], terminate=True)
                        logger.info(f"✅ Revoked task {task['id']}")
                        revoked_count += 1
        
        # Update config status
        config = ReplicationConfig.objects.get(id=replication_config_id)
        config.status = 'paused'
        config.is_active = False
        config.save()
        
        logger.info(f"✅ Stopped consumer, revoked {revoked_count} tasks")
        
        return {'success': True, 'revoked_tasks': revoked_count}
        
    except Exception as e:
        logger.error(f"❌ Error stopping consumer: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def delete_debezium_connector(connector_name, replication_config_id=None):
    """
    Task: Delete a Debezium connector
    """
    try:
        logger.info(f"🗑️ Deleting connector: {connector_name}")
        
        # Stop consumer first
        if replication_config_id:
            stop_kafka_consumer(replication_config_id)
        
        # Delete connector
        manager = DebeziumConnectorManager()
        success, error = manager.delete_connector(connector_name, notify=True)
        
        if success:
            logger.info(f"✅ Successfully deleted connector: {connector_name}")
            
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
        logger.error(f"❌ Error deleting connector: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def restart_replication(replication_config_id):
    """
    Task: Restart replication (stop + start)
    """
    try:
        logger.info(f"🔄 Restarting replication for config {replication_config_id}")
        
        config = ReplicationConfig.objects.get(id=replication_config_id)
        
        if config.connector_name:
            # Stop consumer
            stop_kafka_consumer(replication_config_id)
            
            # Restart connector
            manager = DebeziumConnectorManager()
            success, error = manager.restart_connector(config.connector_name)
            
            if not success:
                raise Exception(f"Failed to restart connector: {error}")
        
        # Start consumer again
        start_kafka_consumer.apply_async(
            args=[replication_config_id],
            countdown=5
        )
        
        config.status = 'active'
        config.is_active = True
        config.save()
        
        logger.info(f"✅ Successfully restarted replication")
        
        return {'success': True}
        
    except Exception as e:
        logger.error(f"❌ Error restarting replication: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def monitor_connectors():
    """
    Periodic task: Monitor all Debezium connectors
    Runs every 5 minutes via Celery Beat
    """
    try:
        logger.info("🔍 Monitoring Debezium connectors...")
        
        manager = DebeziumConnectorManager()
        
        # Check Kafka Connect health
        is_healthy, error = manager.check_kafka_connect_health()
        
        if not is_healthy:
            logger.error(f"❌ Kafka Connect is unhealthy: {error}")
            send_error_notification(
                error_title="Kafka Connect Unhealthy",
                error_message=error or "Unknown error"
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
        
        if issues:
            logger.warning(f"⚠️ Found {len(issues)} connector issues: {issues}")
        else:
            logger.info(f"✅ All {active_configs.count()} connectors are healthy")
        
        return {
            'success': True,
            'monitored': active_configs.count(),
            'issues': len(issues),
            'details': issues
        }
        
    except Exception as e:
        logger.error(f"❌ Error monitoring connectors: {e}")
        return {'success': False, 'error': str(e)}


@shared_task
def check_replication_health():
    """
    Periodic task: Check overall replication health
    Runs every 10 minutes via Celery Beat
    """
    try:
        logger.info("🏥 Checking replication health...")
        
        active_configs = ReplicationConfig.objects.filter(is_active=True)
        
        health_report = {
            'total_configs': active_configs.count(),
            'healthy': 0,
            'unhealthy': 0,
            'issues': []
        }
        
        for config in active_configs:
            # Check last replication
            last_sync = config.last_sync_at
            
            if last_sync:
                time_since_sync = timezone.now() - last_sync
                
                # If no sync in last 30 minutes, mark as unhealthy
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
                # Never synced
                health_report['unhealthy'] += 1
                health_report['issues'].append({
                    'config_id': config.id,
                    'client': config.client_database.client.name,
                    'issue': 'Never synced'
                })
        
        logger.info(f"📊 Health check: {health_report}")
        
        return health_report
        
    except Exception as e:
        logger.error(f"❌ Error checking health: {e}")
        return {'success': False, 'error': str(e)}