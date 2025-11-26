"""
File: client/models/replication.py
Action: REPLACE YOUR ENTIRE FILE with this content

Complete Replication Models
- ReplicationConfig: Main CDC configuration
- TableMapping: Table-level mapping and settings
- ColumnMapping: Column-level mapping and transformations
"""

from django.db import models
from django.utils import timezone
import logging
from .database import ClientDatabase


logger = logging.getLogger(__name__)



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

    # NEW: Health monitoring and state tracking
    connector_state = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        help_text="Debezium connector state (RUNNING/PAUSED/FAILED/etc)"
    )
    consumer_state = models.CharField(
        max_length=50,
        default='UNKNOWN',
        help_text="Kafka consumer state (RUNNING/STOPPED/ERROR)"
    )
    consumer_task_id = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        help_text="Celery task ID for running consumer"
    )
    consumer_last_heartbeat = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last heartbeat from consumer (updated every 30s)"
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
    
    def get_total_columns_count(self):
        """Get total count of enabled columns across all tables"""
        total = 0
        for table_mapping in self.table_mappings.filter(is_enabled=True):
            total += table_mapping.column_mappings.filter(is_enabled=True).count()
        return total
    
        # --- PATCH: ReplicationConfig.delete (client/models/replication.py) ---
    def delete(self, *args, **kwargs):
        """
        Override delete to perform simple cascade deletion.
        
        NOTE: This does NOT clean up Debezium connector or offsets.
        For proper cleanup with connector/offset removal, use:
            orchestrator = ReplicationOrchestrator(config)
            orchestrator.delete_replication()
        
        This method only handles direct model deletion (e.g., via admin panel).
        The orchestrator handles the complete cleanup workflow.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Deleting ReplicationConfig {self.id} (simple cascade)")
        logger.info(f"  Connector: {self.connector_name}")
        logger.info(f"  Note: Use ReplicationOrchestrator.delete_replication() for full cleanup")
        
        # Just delete the model - Django will cascade to TableMapping and ColumnMapping
        super().delete(*args, **kwargs)
        
        logger.info(f"✓ ReplicationConfig {self.id} deleted from database")



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
    
    def get_selected_columns(self):
        """Get list of enabled column mappings"""
        return self.column_mappings.filter(is_enabled=True)
    
    def get_column_count(self):
        """Get count of enabled columns"""
        return self.column_mappings.filter(is_enabled=True).count()
    
    def has_name_mapping(self):
        """Check if table name is mapped (source != target)"""
        return self.source_table != self.target_table


class ColumnMapping(models.Model):
    """
    Column-level mapping and transformation
    Defines which columns to replicate and how to map them
    """
    
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
        related_name='column_mappings',
        help_text="Parent table mapping"
    )
    
    # Source column details
    source_column = models.CharField(
        max_length=255,
        help_text="Source column name"
    )
    source_type = models.CharField(
        max_length=100,
        blank=True,
        help_text="Source column data type (e.g., VARCHAR(255), INT, BIGINT)"
    )
    # Keep old field name for backward compatibility
    source_data_type = models.CharField(
        max_length=100, 
        blank=True,
        help_text="Deprecated: use source_type instead"
    )
    
    # Target column details
    target_column = models.CharField(
        max_length=255,
        help_text="Target column name (can be different from source)"
    )
    target_type = models.CharField(
        max_length=100,
        blank=True,
        help_text="Target column data type"
    )
    # Keep old field name for backward compatibility
    target_data_type = models.CharField(
        max_length=100, 
        blank=True,
        help_text="Deprecated: use target_type instead"
    )
    
    # Configuration
    is_enabled = models.BooleanField(
        default=True,
        help_text="Whether this column should be replicated"
    )
    is_primary_key = models.BooleanField(
        default=False,
        help_text="Is this column part of the primary key"
    )
    is_nullable = models.BooleanField(
        default=True,
        help_text="Can this column contain NULL values"
    )
    
    # Transformation
    transform_function = models.CharField(
        max_length=50,
        choices=TRANSFORM_CHOICES,
        default='none',
        help_text="Transformation to apply to column data"
    )
    transform_params = models.JSONField(
        blank=True,
        null=True,
        help_text="Additional parameters for transformation"
    )
    
    # Transformation rule (optional SQL expression)
    transformation_rule = models.TextField(
        blank=True,
        null=True,
        help_text="Optional transformation rule (SQL expression or function)"
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
        indexes = [
            models.Index(fields=['table_mapping', 'is_enabled']),
        ]
    
    def __str__(self):
        if self.source_column == self.target_column:
            return f"{self.source_column}"
        return f"{self.source_column} → {self.target_column}"
    
    def save(self, *args, **kwargs):
        """Sync old field names with new ones for backward compatibility"""
        # Sync source_type with source_data_type
        if self.source_type and not self.source_data_type:
            self.source_data_type = self.source_type
        elif self.source_data_type and not self.source_type:
            self.source_type = self.source_data_type
        
        # Sync target_type with target_data_type
        if self.target_type and not self.target_data_type:
            self.target_data_type = self.target_type
        elif self.target_data_type and not self.target_type:
            self.target_type = self.target_data_type
        
        super().save(*args, **kwargs)
    
    def get_type_mapping_display(self):
        """Return human-readable type mapping"""
        source = self.source_type or self.source_data_type or 'Unknown'
        target = self.target_type or self.target_data_type or 'Unknown'
        
        if source == target:
            return source
        return f"{source} → {target}"
    
    def has_name_mapping(self):
        """Check if column name is mapped (source != target)"""
        return self.source_column != self.target_column
    
    def has_type_transformation(self):
        """Check if column type is transformed"""
        source = self.source_type or self.source_data_type
        target = self.target_type or self.target_data_type
        return source and target and source != target
    
    def has_transformation(self):
        """Check if any transformation is applied"""
        return (
            self.transform_function != 'none' or 
            self.transformation_rule or 
            self.has_type_transformation()
        )