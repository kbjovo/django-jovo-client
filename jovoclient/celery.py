"""
Celery configuration for Django project
"""
import os
from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready

# Set default Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'jovoclient.settings')

app = Celery('jovoclient')

# Load config from Django settings with CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks from all installed apps
app.autodiscover_tasks()

# Configure Celery Beat schedule
app.conf.beat_schedule = {
    'monitor-debezium-connectors': {
        'task': 'client.tasks.monitor_connectors',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
    },
    'check-replication-health': {
        'task': 'client.tasks.check_replication_health',
        'schedule': crontab(minute='*/10'),  # Every 10 minutes
    },
    # Comprehensive health monitoring with auto-fix (connector status only)
    'monitor-replication-health': {
        'task': 'client.replication.monitor_replication_health',
        'schedule': crontab(minute='*/1'),  # Every 1 minute
    },
    # Ensure continuous DDL consumers are always running (every 30 seconds)
    'ensure-continuous-ddl-consumers': {
        'task': 'client.tasks.ensure_continuous_ddl_consumers',
        'schedule': 30.0,  # Every 30 seconds
    },
    # PostgreSQL schema sync (every 5 minutes)
    'sync-postgresql-schemas': {
        'task': 'client.tasks.sync_postgresql_schemas',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
    },
    # Alert: batch connectors that missed their scheduled run (every 10 minutes)
    'check-batch-schedules': {
        'task': 'client.tasks.check_batch_schedules',
        'schedule': crontab(minute='*/10'),
    },
}

app.conf.task_routes = {
    'client.tasks.start_continuous_ddl_consumer': {'queue': 'ddl_consumer'},
    'client.tasks.ensure_continuous_ddl_consumers': {'queue': 'ddl_consumer'},
    'client.tasks.*': {'queue': 'celery'},
}

@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')


@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    """Auto-start continuous DDL consumers when ddl_consumer worker starts."""
    try:
        # Check if this worker handles ddl_consumer queue
        worker_queues = getattr(sender, 'task_consumer', None)
        if worker_queues:
            queue_names = [q.name for q in worker_queues.task_consumer.queues]
        else:
            # Fallback: check command line args
            import sys
            if '-Q' in sys.argv:
                idx = sys.argv.index('-Q')
                queue_names = sys.argv[idx + 1].split(',') if idx + 1 < len(sys.argv) else []
            else:
                return

        if 'ddl_consumer' not in queue_names:
            return

        print("=" * 60)
        print("DDL CONSUMER WORKER READY - Starting continuous consumers")
        print("=" * 60)

        from client.models.replication import ReplicationConfig
        from client.tasks import start_continuous_ddl_consumer

        supported_types = ('mysql', 'mssql', 'sqlserver')

        active_configs = ReplicationConfig.objects.filter(
            status='active',
            is_active=True
        ).select_related('client_database')

        started = 0
        for config in active_configs:
            source_type = config.client_database.db_type.lower()
            if source_type in supported_types:
                print(f"Starting continuous DDL consumer for config {config.id}")
                start_continuous_ddl_consumer.delay(config.id)
                started += 1

        print(f"Started {started} continuous DDL consumers")
        print("=" * 60)

    except Exception as e:
        print(f"Error in worker_ready signal: {e}")
        import traceback
        traceback.print_exc()