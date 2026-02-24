"""
File: client/models/replication.py
Action: REPLACE YOUR ENTIRE FILE with this content

Complete Replication Models
- ReplicationConfig: Main CDC configuration
- TableMapping: Table-level mapping and settings
"""

from django.db import models
from django.utils import timezone
import logging
from .database import ClientDatabase


logger = logging.getLogger(__name__)


class ConnectorHistory(models.Model):
    """
    History of all connectors created, even after deletion.
    Used to track version numbers and prevent collisions.
    """

    STATUS_CHOICES = [
        ('created', 'Created'),
        ('running', 'Running'),
        ('paused', 'Paused'),
        ('deleted', 'Deleted'),
        ('failed', 'Failed'),
    ]

    # Foreign key to replication config (can be null if config is deleted)
    replication_config = models.ForeignKey(
        'ReplicationConfig',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='connector_history'
    )

    # Client and database info (preserved even if FK is deleted)
    client_id = models.IntegerField(
        help_text="Client ID (preserved for history)"
    )
    client_name = models.CharField(
        max_length=255,
        help_text="Client name (preserved for history)"
    )
    database_id = models.IntegerField(
        help_text="Database ID (preserved for history)"
    )
    database_name = models.CharField(
        max_length=255,
        help_text="Database name (preserved for history)"
    )
    db_type = models.CharField(
        max_length=50,
        help_text="Database type (mysql, postgres, sqlserver, oracle)"
    )

    # Connector details
    connector_name = models.CharField(
        max_length=255,
        db_index=True,
        help_text="Full connector name including version"
    )
    connector_version = models.IntegerField(
        help_text="Version number of this connector"
    )
    connector_type = models.CharField(
        max_length=50,
        choices=[
            ('source', 'Source Connector (Debezium)'),
            ('sink', 'Sink Connector (JDBC)'),
        ],
        help_text="Type of connector"
    )

    # Status tracking
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='created'
    )

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    deleted_at = models.DateTimeField(null=True, blank=True)

    # Metadata
    kafka_topic_prefix = models.CharField(
        max_length=255,
        blank=True,
        help_text="Kafka topic prefix used by this connector"
    )
    notes = models.TextField(
        blank=True,
        help_text="Additional notes or reason for deletion"
    )

    class Meta:
        db_table = "connector_history"
        verbose_name = "Connector History"
        verbose_name_plural = "Connector History"
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['client_id', 'database_id', 'connector_version']),
            models.Index(fields=['connector_name']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"{self.connector_name} (v{self.connector_version}) - {self.status}"

    @classmethod
    def get_next_version(cls, client_id: int, database_id: int, connector_type: str = 'source') -> int:
        """
        Get the next available version number for a client/database connector.

        Args:
            client_id: Client ID
            database_id: Database ID
            connector_type: 'source' or 'sink'

        Returns:
            Next version number (starts at 1)
        """
        highest = cls.objects.filter(
            client_id=client_id,
            database_id=database_id,
            connector_type=connector_type
        ).aggregate(
            max_version=models.Max('connector_version')
        )['max_version']

        return (highest or 0) + 1

    @classmethod
    def record_connector_creation(cls, replication_config, connector_name: str,
                                  connector_version: int, connector_type: str = 'source'):
        """
        Record a connector creation in history.

        For sink connectors, this is idempotent - if the connector already exists in history,
        it updates the record instead of creating a duplicate.

        Args:
            replication_config: ReplicationConfig instance
            connector_name: Full connector name
            connector_version: Version number
            connector_type: 'source' or 'sink'
        """
        db_config = replication_config.client_database
        client = db_config.client

        # For sink connectors, check if already exists (they are reused)
        if connector_type == 'sink':
            existing = cls.objects.filter(connector_name=connector_name).first()
            if existing:
                # Update existing record
                existing.replication_config = replication_config
                existing.status = 'created'
                existing.deleted_at = None
                existing.kafka_topic_prefix = replication_config.kafka_topic_prefix or ''
                existing.save()
                logger.debug(f"Updated existing sink connector history: {connector_name}")
                return existing

        # Create new record (for source connectors or new sink connectors)
        return cls.objects.create(
            replication_config=replication_config,
            client_id=client.id,
            client_name=client.name,
            database_id=db_config.id,
            database_name=db_config.database_name,
            db_type=db_config.db_type,
            connector_name=connector_name,
            connector_version=connector_version,
            connector_type=connector_type,
            status='created',
            kafka_topic_prefix=replication_config.kafka_topic_prefix or ''
        )

    @classmethod
    def mark_connector_deleted(cls, connector_name: str, notes: str = ''):
        """Mark a connector as deleted in history."""
        history = cls.objects.filter(
            connector_name=connector_name
        ).exclude(status='deleted').order_by('-created_at').first()

        if not history:
            logger.warning(f"Connector {connector_name} not found in history")
            return False

        history.status = 'deleted'
        history.deleted_at = timezone.now()
        if notes:
            history.notes = notes
        history.save()
        return True


class NotificationLog(models.Model):
    """
    Tracks open alert incidents for transition-based email notifications.

    An entry with resolved_at=NULL means an alert has been sent and the
    incident is still active. Once the issue is resolved, resolved_at is set
    so the next failure will trigger a fresh email.
    """

    EVENT_CHOICES = [
        ('source_failed',     'Source Connector Failed'),
        ('sink_failed',       'Sink Connector Failed'),
        ('connector_missing', 'Connector Missing'),
        ('kafka_down',        'Kafka Connect Unreachable'),
        ('batch_task_failed', 'Batch Task Failed'),
        ('ddl_task_failed',   'DDL Task Failed'),
        ('dlq_messages',      'DLQ Has Messages'),
        ('batch_missed',      'Batch Missed Schedule'),
    ]

    replication_config = models.ForeignKey(
        'ReplicationConfig',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='notification_logs',
    )
    event_type = models.CharField(max_length=50, choices=EVENT_CHOICES)
    connector_name = models.CharField(max_length=255, blank=True, default='')
    message = models.TextField(blank=True)
    sent_at = models.DateTimeField(auto_now_add=True)
    resolved_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['connector_name', 'event_type', 'resolved_at']),
        ]

    def __str__(self):
        status = 'open' if self.resolved_at is None else 'resolved'
        return f"[{status}] {self.event_type} — {self.connector_name}"


class ReplicationConfig(models.Model):
    """Configuration for replication from source DB to client's database"""

    SYNC_TYPE_CHOICES = [
        ('full', 'Full Refresh'),
        ('incremental', 'Incremental Sync'),
        ('realtime', 'Real-time CDC'),
    ]

    SYNC_FREQUENCY_CHOICES = [
        ('realtime', 'Real-time (CDC)'),
        ('every_1min', 'Every 1 minute'),
        ('every_5min', 'Every 5 minutes'),
        ('every_15min', 'Every 15 minutes'),
        ('every_30min', 'Every 30 minutes'),
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('manual', 'Manual Only'),
    ]

    STATUS_CHOICES = [
        ('configured', 'Configured'),  # Added this status
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('error', 'Error'),
        ('disabled', 'Disabled'),
    ]

    # ====== PROCESSING MODE ======
    PROCESSING_MODE_CHOICES = [
        ('cdc', 'CDC (Real-time)'),
        ('batch', 'Batch Processing'),
    ]

    BATCH_INTERVAL_CHOICES = [
        ('5m', 'Every 5 minutes'),
        ('30m', 'Every 30 minutes'),
        ('2h', 'Every 2 hours'),
        ('6h', 'Every 6 hours'),
        ('12h', 'Every 12 hours'),
        ('24h', 'Every 24 hours (Daily)'),
    ]

    BATCH_MAX_CATCHUP_CHOICES = [
        (5, '5 minutes'),
        (10, '10 minutes'),
        (20, '20 minutes'),
    ]
    
    client_database = models.ForeignKey(
        ClientDatabase, 
        on_delete=models.CASCADE, 
        related_name='replication_configs'
    )
    
    # Sync settings
    sync_type = models.CharField(
        max_length=20, 
        choices=SYNC_TYPE_CHOICES, 
        default='realtime',
        help_text="Type of synchronization"
    )
    sync_frequency = models.CharField(
        max_length=20,
        choices=SYNC_FREQUENCY_CHOICES,
        default='realtime',
        help_text="How often to sync (ignored for real-time CDC)"
    )

    # ====== PROCESSING MODE SETTINGS ======
    processing_mode = models.CharField(
        max_length=20,
        choices=PROCESSING_MODE_CHOICES,
        default='cdc',
        help_text="CDC for real-time streaming, Batch for scheduled sync windows"
    )

    batch_interval = models.CharField(
        max_length=10,
        choices=BATCH_INTERVAL_CHOICES,
        null=True,
        blank=True,
        help_text="Sync interval for batch processing mode"
    )

    batch_max_catchup_minutes = models.IntegerField(
        choices=BATCH_MAX_CATCHUP_CHOICES,
        default=5,
        help_text="Maximum minutes to run during batch sync before pausing (throttle)"
    )

    last_batch_run = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp of last batch sync execution"
    )

    next_batch_run = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Scheduled timestamp for next batch sync"
    )

    batch_celery_task_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Celery Beat periodic task name for batch scheduling"
    )

    # Status
    status = models.CharField(
        max_length=20, 
        choices=STATUS_CHOICES, 
        default='configured'  # Changed default
    )
    is_active = models.BooleanField(default=False)  # Changed default to False
    
    # Timing
    last_sync_at = models.DateTimeField(null=True, blank=True)
    next_sync_at = models.DateTimeField(null=True, blank=True)
    last_success_at = models.DateTimeField(null=True, blank=True)
    
    # Debezium connector info
    connector_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Debezium connector name in Kafka Connect"
    )
    connector_version = models.IntegerField(
        default=1,
        help_text="Version number for the connector (e.g., 1 for _v_1, 2 for _v_2)"
    )
    kafka_topic_prefix = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Kafka topic prefix for this connector"
    )
    
    # Options
    auto_create_tables = models.BooleanField(
        default=True,
        help_text="Automatically create tables if they don't exist"
    )
    drop_before_sync = models.BooleanField(
        default=False,
        help_text="Drop and recreate tables (only for full refresh)"
    )

    # ====== PERFORMANCE TUNING SETTINGS ======
    # These settings control Debezium connector performance and behavior

    SNAPSHOT_MODE_CHOICES = [
        ('initial', 'Initial - Snapshot on first run only'),
        ('when_needed', 'When Needed - Snapshot if no offset'),
        ('never', 'Never - CDC only, no snapshot'),
        ('always', 'Always - Snapshot every restart'),
        ('schema_only', 'Schema Only - No data snapshot'),
        ('recovery', 'Recovery - For recovery scenarios'),
    ]

    snapshot_mode = models.CharField(
        max_length=20,
        choices=SNAPSHOT_MODE_CHOICES,
        default='initial',
        help_text="Snapshot mode for initial data load"
    )

    max_queue_size = models.IntegerField(
        default=8192,
        help_text="Maximum queue size for the connector (1024-32768). Higher values = better throughput but more memory usage."
    )

    max_batch_size = models.IntegerField(
        default=2048,
        help_text="Maximum batch size for processing (512-8192). Larger batches = faster throughput but more latency."
    )

    poll_interval_ms = models.IntegerField(
        default=500,
        help_text="Poll interval in milliseconds (100-5000). Lower = more real-time but more CPU usage."
    )

    incremental_snapshot_chunk_size = models.IntegerField(
        default=1024,
        help_text="Chunk size for incremental snapshots (256-10240). Used when adding tables via signals."
    )

    snapshot_fetch_size = models.IntegerField(
        default=10000,
        help_text="JDBC fetch size during initial snapshot (1000-50000). How many rows the source reads per fetch. Larger = faster snapshots but more memory."
    )

    sink_batch_size = models.IntegerField(
        default=3000,
        help_text="Sink connector batch size (500-10000). How many records to write to target DB in one batch."
    )

    sink_max_poll_records = models.IntegerField(
        default=5000,
        help_text="Max records the sink pulls from Kafka per poll (500-10000). Should be >= sink_batch_size."
    )

    # NEW: Health monitoring and state tracking
    connector_state = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        help_text="Debezium source connector state (RUNNING/PAUSED/FAILED/etc)"
    )

    # ✅ NEW: Sink connector tracking (JDBC Sink Connector to target database)
    sink_connector_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="JDBC Sink connector name in Kafka Connect"
    )
    sink_connector_state = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        help_text="JDBC Sink connector state (RUNNING/PAUSED/FAILED/etc)"
    )

    # DEPRECATED: Consumer fields (replaced by sink connectors)
    consumer_state = models.CharField(
        max_length=50,
        default='UNKNOWN',
        help_text="[DEPRECATED] Kafka consumer state - replaced by sink_connector_state"
    )
    consumer_task_id = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        help_text="[DEPRECATED] Celery task ID for running consumer - no longer used with sink connectors"
    )
    consumer_last_heartbeat = models.DateTimeField(
        null=True,
        blank=True,
        help_text="[DEPRECATED] Last heartbeat from consumer - no longer used with sink connectors"
    )

    last_error_message = models.TextField(
        blank=True,
        null=True,
        help_text="Last error message for debugging"
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(
        'auth.User', 
        on_delete=models.SET_NULL, 
        null=True,
        blank=True,
        related_name='replication_configs_created'
    )
    
    class Meta:
        db_table = "replication_config"
        verbose_name = "Replication Configuration"
        verbose_name_plural = "Replication Configurations"
        ordering = ['-created_at']
        # Removed unique_together for connector_name since it can be null during configuration
    
    def __str__(self):
        return f"{self.client_database.client.name} - {self.client_database.connection_name} ({self.get_sync_type_display()})"
    
    def get_table_count(self):
        """Get count of enabled table mappings"""
        return self.table_mappings.filter(is_enabled=True).count()
    
    def delete(self, using=None, keep_parents=False, *args, **kwargs):
        """
        Override delete to ensure proper cleanup of all CDC resources.

        This method automatically:
        1. Stops and revokes consumer Celery tasks
        2. Pauses the Debezium connector
        3. Deletes the connector from Kafka Connect
        4. Clears connector offsets
        5. Deletes the model from database (cascades to TableMapping)

        For deletion with topic removal, use:
            orchestrator = ReplicationOrchestrator(config)
            orchestrator.delete_replication(delete_topics=True)
        """
        logger.info(f"🗑️ Deleting ReplicationConfig {self.id}")
        logger.info(f"   Connector: {self.connector_name}")
        logger.info(f"   Using orchestrated cleanup for proper resource management")

        # Store config details before deletion
        config_id = self.id
        connector_name = self.connector_name
        client_id = self.client_database.client.id
        database_name = self.client_database.database_name
        consumer_group_id = f"cdc_consumer_{client_id}_{database_name}"

        try:
            # ========================================
            # STEP 1: Stop Consumer Tasks
            # ========================================
            logger.info("  → Stopping consumer tasks...")

            # Mark as inactive first
            self.is_active = False
            self.status = 'stopping'
            self.consumer_state = 'STOPPING'
            self.save(using=using)

            # Revoke Celery task if exists
            if self.consumer_task_id:
                try:
                    from jovoclient.celery import app as celery_app

                    # Try graceful termination
                    celery_app.control.revoke(
                        self.consumer_task_id,
                        terminate=True,
                        signal='SIGTERM'
                    )
                    logger.info(f"    ✓ Sent SIGTERM to task: {self.consumer_task_id}")

                    # Wait briefly, then force kill
                    import time
                    time.sleep(1)

                    celery_app.control.revoke(
                        self.consumer_task_id,
                        terminate=True,
                        signal='SIGKILL'
                    )
                    logger.info(f"    ✓ Force killed task: {self.consumer_task_id}")

                except Exception as e:
                    logger.warning(f"    ⚠ Could not revoke task: {e}")

            # Check for any other tasks for this config and kill them too
            try:
                from jovoclient.celery import app as celery_app
                inspect = celery_app.control.inspect()
                active_tasks = inspect.active()

                if active_tasks:
                    for worker, tasks in active_tasks.items():
                        for task in tasks:
                            if (task['name'] == 'client.tasks.start_kafka_consumer' and
                                str(config_id) in str(task.get('args', []))):

                                celery_app.control.revoke(
                                    task['id'],
                                    terminate=True,
                                    signal='SIGKILL'
                                )
                                logger.info(f"    ✓ Killed orphaned task: {task['id']}")
            except Exception as e:
                logger.warning(f"    ⚠ Error checking for orphaned tasks: {e}")

            # ========================================
            # STEP 2: Clean up Source Connector (Debezium)
            # ========================================
            if connector_name:
                logger.info(f"  → Cleaning up source connector: {connector_name}")

                try:
                    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
                    manager = DebeziumConnectorManager()

                    # Pause connector first
                    try:
                        manager.pause_connector(connector_name)
                        logger.info(f"    ✓ Paused source connector")
                    except Exception as e:
                        logger.warning(f"    ⚠ Could not pause source connector: {e}")

                    # Delete connector
                    try:
                        manager.delete_connector(connector_name, delete_topics=False)
                        logger.info(f"    ✓ Deleted source connector")
                    except Exception as e:
                        logger.warning(f"    ⚠ Could not delete source connector: {e}")

                except Exception as e:
                    logger.error(f"    ❌ Error during source connector cleanup: {e}")

            # ========================================
            # STEP 3: Clean up Sink Connector (JDBC Sink)
            # ========================================
            if self.sink_connector_name:
                logger.info(f"  → Cleaning up sink connector: {self.sink_connector_name}")

                try:
                    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
                    manager = DebeziumConnectorManager()

                    # Pause sink connector first
                    try:
                        manager.pause_connector(self.sink_connector_name)
                        logger.info(f"    ✓ Paused sink connector")
                    except Exception as e:
                        logger.warning(f"    ⚠ Could not pause sink connector: {e}")

                    # Delete sink connector
                    try:
                        manager.delete_connector(self.sink_connector_name, delete_topics=False)
                        logger.info(f"    ✓ Deleted sink connector")
                    except Exception as e:
                        logger.warning(f"    ⚠ Could not delete sink connector: {e}")

                except Exception as e:
                    logger.error(f"    ❌ Error during sink connector cleanup: {e}")

            # ========================================
            # STEP 3: Delete Model
            # ========================================
            logger.info(f"  → Deleting model from database...")
            super().delete(using=using, keep_parents=keep_parents)

            logger.info(f"✅ Successfully deleted ReplicationConfig {config_id}")
            logger.info(f"   Connector: {connector_name}")
            logger.info(f"   Consumer Group: {consumer_group_id} (preserved)")
            logger.info(f"   Topics: preserved (use orchestrator.delete_replication(delete_topics=True) to delete)")

        except Exception as e:
            logger.error(f"❌ Error during ReplicationConfig deletion: {e}", exc_info=True)
            # Still try to delete the model even if cleanup failed
            try:
                super().delete(using=using, keep_parents=keep_parents)
                logger.warning(f"⚠️ Model deleted but cleanup may be incomplete")
            except Exception as e2:
                logger.error(f"❌ Failed to delete model: {e2}")
                raise



class TableMapping(models.Model):
    """Mapping between source table and target table in client's database"""
    
    SYNC_TYPE_CHOICES = [
        ('full', 'Full Refresh'),
        ('incremental', 'Incremental'),
        ('realtime', 'Real-time CDC'),
    ]
    
    CONFLICT_RESOLUTION_CHOICES = [
        ('source_wins', 'Source Always Wins'),
        ('target_wins', 'Target Always Wins'),
        ('newest_wins', 'Newest Timestamp Wins'),
        ('manual', 'Manual Resolution'),
    ]
    
    replication_config = models.ForeignKey(
        ReplicationConfig,
        on_delete=models.CASCADE,
        related_name='table_mappings'
    )
    
    # Source table info
    source_table = models.CharField(max_length=255)
    source_schema = models.CharField(max_length=255, blank=True, null=True)
    
    # Target table info (in client's database)
    target_table = models.CharField(
        max_length=255,
        help_text="Table name in client's database (can be different from source)"
    )
    target_schema = models.CharField(
        max_length=255, 
        default='',
        blank=True,
        help_text="Schema in client database (leave empty for default)"
    )
    
    # Configuration
    is_enabled = models.BooleanField(default=True)
    sync_type = models.CharField(
        max_length=20,
        choices=SYNC_TYPE_CHOICES,
        blank=True,
        help_text="Leave empty to inherit from ReplicationConfig"
    )
    sync_frequency = models.CharField(
        max_length=20,
        choices=ReplicationConfig.SYNC_FREQUENCY_CHOICES,
        blank=True,
        help_text="Leave empty to inherit from ReplicationConfig"
    )
    
    # Incremental sync settings
    incremental_column = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Column to track for incremental sync (e.g., updated_at, id)"
    )
    incremental_column_type = models.CharField(
        max_length=50,
        choices=[
            ('timestamp', 'Timestamp'),
            ('integer', 'Integer/ID'),
            ('datetime', 'DateTime'),
        ],
        default='timestamp',
        blank=True
    )
    last_incremental_value = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Last synced value of incremental column"
    )
    
    # Conflict resolution
    conflict_resolution = models.CharField(
        max_length=20,
        choices=CONFLICT_RESOLUTION_CHOICES,
        default='source_wins',
        help_text="How to handle conflicts during sync"
    )
    
    # Statistics
    total_rows_synced = models.BigIntegerField(default=0)
    last_sync_at = models.DateTimeField(null=True, blank=True)
    last_sync_duration = models.FloatField(
        null=True,
        blank=True,
        help_text="Duration in seconds"
    )
    last_sync_status = models.CharField(
        max_length=20,
        choices=[
            ('success', 'Success'),
            ('failed', 'Failed'),
            ('partial', 'Partial Success'),
        ],
        default='success',
        blank=True
    )
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = "table_mapping"
        verbose_name = "Table Mapping"
        verbose_name_plural = "Table Mappings"
        unique_together = [['replication_config', 'source_table']]
        ordering = ['source_table']
    
    def __str__(self):
        if self.source_table == self.target_table:
            return f"{self.source_table}"
        return f"{self.source_table} → {self.target_table}"
    
    def get_effective_sync_type(self):
        """Get sync type (from table or config)"""
        return self.sync_type or self.replication_config.sync_type

    def get_effective_sync_frequency(self):
        """Get sync frequency (from table or config)"""
        return self.sync_frequency or self.replication_config.sync_frequency
    
    def has_name_mapping(self):
        """Check if table name is mapped (source != target)"""
        return self.source_table != self.target_table