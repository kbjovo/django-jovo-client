from django.db import models
from django.utils import timezone

from .replication import ReplicationConfig, TableMapping


class ReplicationJob(models.Model):
    """Track each replication job execution"""
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('success', 'Success'),
        ('partial_success', 'Partial Success'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    TRIGGER_CHOICES = [
        ('scheduled', 'Scheduled'),
        ('manual', 'Manual'),
        ('api', 'API'),
        ('auto', 'Automatic'),
        ('retry', 'Retry'),
    ]
    
    replication_config = models.ForeignKey(
        ReplicationConfig,
        on_delete=models.CASCADE,
        related_name='jobs'
    )
    
    # Job info
    job_id = models.CharField(max_length=100, unique=True, db_index=True)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending',
        db_index=True
    )
    trigger = models.CharField(
        max_length=20,
        choices=TRIGGER_CHOICES,
        default='scheduled'
    )
    
    # Timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(null=True, blank=True)
    
    # Statistics
    tables_processed = models.IntegerField(default=0)
    tables_succeeded = models.IntegerField(default=0)
    tables_failed = models.IntegerField(default=0)
    
    rows_read = models.BigIntegerField(default=0)
    rows_written = models.BigIntegerField(default=0)
    rows_updated = models.BigIntegerField(default=0)
    rows_failed = models.BigIntegerField(default=0)
    
    # Error info
    error_message = models.TextField(blank=True, null=True)
    error_traceback = models.TextField(blank=True, null=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    created_by = models.ForeignKey(
        'auth.User',
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    
    class Meta:
        db_table = 'replication_job'
        verbose_name = "Replication Job"
        verbose_name_plural = "Replication Jobs"
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['-created_at']),
            models.Index(fields=['status']),
            models.Index(fields=['job_id']),
        ]
    
    def __str__(self):
        return f"Job {self.job_id} - {self.status}"
    
    def calculate_duration(self):
        """Calculate job duration"""
        if self.started_at and self.completed_at:
            delta = self.completed_at - self.started_at
            self.duration_seconds = delta.total_seconds()
            self.save(update_fields=['duration_seconds'])


class ReplicationLog(models.Model):
    """Detailed logs for each table in a job"""
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('skipped', 'Skipped'),
    ]
    
    job = models.ForeignKey(
        ReplicationJob,
        on_delete=models.CASCADE,
        related_name='logs'
    )
    table_mapping = models.ForeignKey(
        TableMapping,
        on_delete=models.CASCADE,
        related_name='logs'
    )
    
    # Status
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='pending',
        db_index=True
    )
    
    # Timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(null=True, blank=True)
    
    # Statistics
    rows_read = models.BigIntegerField(default=0)
    rows_inserted = models.BigIntegerField(default=0)
    rows_updated = models.BigIntegerField(default=0)
    rows_deleted = models.BigIntegerField(default=0)
    rows_failed = models.BigIntegerField(default=0)
    
    # Details
    query_executed = models.TextField(blank=True, null=True)
    error_details = models.JSONField(blank=True, null=True)
    warnings = models.JSONField(blank=True, null=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    
    class Meta:
        db_table = 'replication_log'
        verbose_name = "Replication Log"
        verbose_name_plural = "Replication Logs"
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['-created_at']),
            models.Index(fields=['status']),
        ]
    
    def __str__(self):
        return f"{self.table_mapping.source_table} - {self.status}"


class SchemaVersion(models.Model):
    """Track schema changes over time"""
    
    table_mapping = models.ForeignKey(
        TableMapping,
        on_delete=models.CASCADE,
        related_name='schema_versions'
    )
    
    version = models.IntegerField()
    schema_snapshot = models.JSONField(
        help_text="Complete table schema (columns, types, constraints)"
    )
    
    # Changes from previous version
    changes = models.JSONField(
        blank=True,
        null=True,
        help_text="List of changes from previous version"
    )
    
    # Metadata
    detected_at = models.DateTimeField(auto_now_add=True)
    detected_by_job = models.ForeignKey(
        ReplicationJob,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    
    class Meta:
        db_name = 'schema_versions'
        verbose_name = "Schema Version"
        verbose_name_plural = "Schema Versions"
        unique_together = [['table_mapping', 'version']]
        ordering = ['-version']
    
    def __str__(self):
        return f"{self.table_mapping.source_table} v{self.version}"
