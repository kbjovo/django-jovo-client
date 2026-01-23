"""
Celery configuration for Django project
"""
import os
from celery import Celery
from celery.schedules import crontab

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
}

app.conf.task_routes = {
    'client.tasks.*': {'queue': 'celery'},
}

@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')