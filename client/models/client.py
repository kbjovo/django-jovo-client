from django.db import models
from django.utils import timezone


class Client(models.Model):
    """Main client model with company information"""
    
    STATUS_CHOICES = [
        ("active", "Active"),
        ("inactive", "Inactive"),
        ("deleted", "Deleted"),
    ]

    REPLICATION_STATUS_CHOICES = [
        ('not_configured', 'Not Configured'),
        ('configured', 'Configured'),
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('error', 'Error'),
    ]

    # Basic Information
    name = models.CharField(max_length=100, blank=False, null=False)
    email = models.EmailField(null=False, unique=True, db_index=True, blank=False)
    phone = models.CharField(max_length=10, unique=True, db_index=True, null=False, blank=False)

    # Company Details
    company_name = models.CharField(max_length=255, blank=True)
    address = models.TextField(blank=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True, db_index=True)
    country = models.CharField(max_length=100, blank=True, default="India")
    postal_code = models.CharField(max_length=20, blank=True, null=True)
    
    # Status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="active")
    
    # Replication Settings
    replication_enabled = models.BooleanField(
        default=False,
        help_text="Enable real-time replication for this client"
    )
    replication_status = models.CharField(
        max_length=20,
        choices=REPLICATION_STATUS_CHOICES,
        default='not_configured'
    )
    last_replication_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last time data was replicated"
    )
    
    # Timestamps
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    deleted_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "client"
        verbose_name = "Client"
        verbose_name_plural = "Clients"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.name} ({self.status})"

    def activate(self):
        """Reactivate client (without recreating DB if already exists)."""
        self.status = "active"
        self.deleted_at = None
        self.save(update_fields=["status", "deleted_at"])

    def deactivate(self):
        """Deactivate client"""
        self.status = "inactive"
        self.save(update_fields=["status"])

    def soft_delete(self):
        """Mark as deleted and clean up associated resources."""
        self.status = "deleted"
        self.deleted_at = timezone.now()
        self.save(update_fields=["status", "deleted_at"])
        self.client_databases.all().delete()

    @property
    def is_active(self):
        """Check if client is active"""
        return self.status == "active"

    def get_target_database(self):
        """Get the target database for this client"""
        return self.client_databases.filter(is_target=True).first()
    
    def get_source_databases(self):
        """Get all source databases for this client"""
        return self.client_databases.filter(is_primary=True)
