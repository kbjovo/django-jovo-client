from django.db import models
from django.utils import timezone

from .database import ClientDatabase


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
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('error', 'Error'),
        ('disabled', 'Disabled'),
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
    
    # Status
    status = models.CharField(
        max_length=20, 
        choices=STATUS_CHOICES, 
        default='active'
    )
    is_active = models.BooleanField(default=True)
    
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
        unique_together = [['client_database', 'connector_name']]
    
    def __str__(self):
        return f"{self.client_database.client.name} - {self.client_database.connection_name} ({self.get_sync_type_display()})"


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
        help_text="Table name in client's database"
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
        default='source_wins'
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
        return f"{self.source_table} → {self.target_table}"
    
    def get_effective_sync_type(self):
        """Get sync type (from table or config)"""
        return self.sync_type or self.replication_config.sync_type


class ColumnMapping(models.Model):
    """Column-level mapping and transformation"""
    
    TRANSFORM_CHOICES = [
        ('none', 'No Transformation'),
        ('encrypt', 'Encrypt'),
        ('hash', 'Hash (SHA256)'),
        ('mask', 'Mask (e.g., XXX-XX-1234)'),
        ('uppercase', 'Convert to Uppercase'),
        ('lowercase', 'Convert to Lowercase'),
        ('trim', 'Trim Whitespace'),
        ('null_to_default', 'Convert NULL to Default'),
    ]
    
    table_mapping = models.ForeignKey(
        TableMapping,
        on_delete=models.CASCADE,
        related_name='column_mappings'
    )
    
    # Source column
    source_column = models.CharField(max_length=255)
    source_data_type = models.CharField(max_length=100, blank=True)
    
    # Target column
    target_column = models.CharField(max_length=255)
    target_data_type = models.CharField(max_length=100, blank=True)
    
    # Configuration
    is_enabled = models.BooleanField(default=True)
    is_primary_key = models.BooleanField(default=False)
    is_nullable = models.BooleanField(default=True)
    
    # Transformation
    transform_function = models.CharField(
        max_length=50,
        choices=TRANSFORM_CHOICES,
        default='none'
    )
    transform_params = models.JSONField(
        blank=True,
        null=True,
        help_text="Additional parameters for transformation"
    )
    
    # Default value handling
    default_value = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Default value if source is NULL"
    )
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = "column_mapping"
        verbose_name = "Column Mapping"
        verbose_name_plural = "Column Mappings"
        unique_together = [['table_mapping', 'source_column']]
        ordering = ['source_column']
    
    def __str__(self):
        return f"{self.source_column} → {self.target_column}"
    