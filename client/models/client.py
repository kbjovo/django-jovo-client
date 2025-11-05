from django.db import models, connection
from django.utils import timezone
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver


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
    
    # Database
    db_name = models.CharField(
        max_length=20, 
        null=False, 
        unique=True, 
        db_index=True, 
        default="temp",
        help_text="Database name where replicated data will be stored"
    )
    
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
        """Mark as deleted and drop its database."""
        self.status = "deleted"
        self.deleted_at = timezone.now()
        self.save(update_fields=["status", "deleted_at"])
        self.drop_database()  
        self.databases.all().delete()

    @property
    def is_active(self):
        """Check if client is active"""
        return self.status == "active"

    def save(self, *args, **kwargs):
        """Validate db_name before saving."""
        if not self.db_name or self.db_name == "temp":
            raise ValueError("Database name is required and cannot be 'temp'")
        super().save(*args, **kwargs)

    def drop_database(self):
        """Drop the client's dedicated database."""
        if self.db_name:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS `{self.db_name}`;")


# ──────────────────────────────
# SIGNALS
# ──────────────────────────────

@receiver(post_save, sender=Client)
def create_client_database(sender, instance, created, **kwargs):
    """Create database on Client creation"""
    if created:
        db_name = instance.db_name
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;")


@receiver(post_delete, sender=Client)
def delete_client_database(sender, instance, **kwargs):
    """Drop database on hard delete"""
    instance.drop_database()