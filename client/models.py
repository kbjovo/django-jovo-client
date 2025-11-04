from django.dispatch import receiver
from django.db import models, connection
from django.utils import timezone
from django.db.models.signals import post_save, post_delete
from django.db import connections, connection
from django.db.utils import OperationalError
import socket
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from .encryption import encrypt_password, decrypt_password



class Client(models.Model):
    STATUS_CHOICES = [
        ("active", "Active"),
        ("inactive", "Inactive"),
        ("deleted", "Deleted"),
    ]

    name = models.CharField(max_length=100, blank=False, null=False)
    email = models.EmailField(null=False, unique= True, db_index= True, blank=False)
    phone = models.CharField(max_length=10, unique= True, db_index= True, null=False, blank=False)
    db_name = models.CharField(max_length=20, null=False, unique=True, db_index= True, default="temp")
    company_name = models.CharField(max_length=255, blank=True)
    address = models.TextField(blank=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True, db_index= True)
    country = models.CharField(max_length=100, blank=True, default="India")
    postal_code = models.CharField(max_length=20, blank=True, null=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="active")
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    deleted_at = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"{self.name} ({self.status})"

    def activate(self):
        """Reactivate client (without recreating DB if already exists)."""
        self.status = "active"
        self.deleted_at = None
        self.save(update_fields=["status", "deleted_at"])

    def deactivate(self):
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
# SIGNAL: Create database on Client creation
# ──────────────────────────────
@receiver(post_save, sender=Client)
def create_client_database(sender, instance, created, **kwargs):
    if created:
        db_name = instance.db_name

        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;")

# ──────────────────────────────
# SIGNAL: Drop database on hard delete
# ──────────────────────────────
@receiver(post_delete, sender=Client)
def delete_client_database(sender, instance, **kwargs):
    instance.drop_database()




class ClientDatabase(models.Model):
    DB_TYPE_CHOICES = [
        ("mysql", "MySQL"),
        ("postgresql", "PostgreSQL"),
        ("sqlite", "SQLite"),
        ("oracle", "Oracle"),
    ]

    STATUS_CHOICES = [
        ("success", "Success"),
        ("failed", "Failed"),
        ("inactive", "Inactive"),
    ]

    client = models.ForeignKey(
        "Client", on_delete=models.CASCADE, related_name="databases"
    )
    connection_name = models.CharField(max_length=100, db_index= True)
    db_type = models.CharField(
        max_length=20, choices=DB_TYPE_CHOICES, default="mysql"
    )
    host = models.CharField(max_length=255, db_index= True)
    port = models.PositiveIntegerField(default=3306)
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=255)
    database_name = models.CharField(max_length=100)
    is_primary = models.BooleanField(default=True)
    connection_status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default="success"
    )
    last_checked = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.client.name} → {self.connection_name} ({self.db_type})"

    def save(self, *args, **kwargs):
        """Override save to encrypt password before storing."""
        # Only encrypt if password has changed and is not already encrypted
        if self.password and not self._is_password_encrypted():
            self.password = encrypt_password(self.password)
        super().save(*args, **kwargs)

    def _is_password_encrypted(self):
        """Check if password is already encrypted (simple heuristic)."""
        # Encrypted passwords are base64-encoded Fernet tokens
        # They typically start with 'gAAAAA' and are much longer than typical passwords
        if not self.password:
            return False
        return len(self.password) > 50 and 'gAAAAA' in self.password

    def get_decrypted_password(self):
        """Get the decrypted password for use in connections."""
        return decrypt_password(self.password)

    # 🧩 Method: check database connectivity
    def get_connection_url(self):
        """Build a universal SQLAlchemy connection URL."""
        # Use decrypted password for connections
        password = quote_plus(self.get_decrypted_password() or "")
        
        if self.db_type == "mysql":
            return f"mysql+pymysql://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"
        elif self.db_type == "postgresql":
            return f"postgresql+psycopg2://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"
        elif self.db_type == "sqlite":
            return f"sqlite:///{self.database_name}"
        elif self.db_type == "oracle":
            return f"oracle+oracledb://{self.username}:{password}@{self.host}:{self.port}/?service_name={self.database_name}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def check_connection_status(self, save=True):
        """Check database connectivity using SQLAlchemy."""
        try:
            # Basic TCP-level test first (for all except SQLite)
            if self.db_type != "sqlite":
                import socket
                socket.create_connection((self.host, self.port), timeout=5)

            # Create engine and test connection
            from sqlalchemy import create_engine, text
            engine = create_engine(self.get_connection_url(), pool_pre_ping=True)

            with engine.connect() as conn:
                # Use text() wrapper for the query
                conn.execute(text("SELECT 1"))

            self.connection_status = "success"

        except socket.timeout:
            self.connection_status = "failed"
            raise Exception("Connection timeout - host is unreachable")
        except socket.gaierror as e:
            self.connection_status = "failed"
            raise Exception(f"Host not found: {self.host}")
        except ConnectionRefusedError:
            self.connection_status = "failed"
            raise Exception(f"Connection refused on port {self.port}")
        except Exception as e:
            self.connection_status = "failed"
            # Parse and re-raise with user-friendly message (suppress traceback)
            error_msg = str(e)
            if "Access denied" in error_msg or "authentication failed" in error_msg.lower():
                raise Exception("Authentication failed - invalid username or password") from None
            elif "Unknown database" in error_msg or "does not exist" in error_msg:
                raise Exception(f"Database '{self.database_name}' does not exist") from None
            elif "Can't connect" in error_msg or "Unable to connect" in error_msg:
                raise Exception(f"Unable to connect to database server at {self.host}:{self.port}") from None
            else:
                # Generic error - extract just the relevant part
                clean_msg = error_msg.split('\n')[0] if '\n' in error_msg else error_msg
                raise Exception(f"Connection error: {clean_msg}") from None
        finally:
            # Update timestamp
            from django.utils import timezone
            self.last_checked = timezone.now()

            # Save if requested
            if save:
                self.save(update_fields=["connection_status", "last_checked"])

        return self.connection_status


