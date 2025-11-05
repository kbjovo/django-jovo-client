from django.db import models
from django.utils import timezone
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import socket

from ..encryption import encrypt_password, decrypt_password
from .client import Client


class ClientDatabase(models.Model):
    """External database connections for clients"""
    
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
        Client, 
        on_delete=models.CASCADE, 
        related_name="databases"
    )
    
    # Connection Details
    connection_name = models.CharField(max_length=100, db_index=True)
    db_type = models.CharField(
        max_length=20, 
        choices=DB_TYPE_CHOICES, 
        default="mysql"
    )
    host = models.CharField(max_length=255, db_index=True)
    port = models.PositiveIntegerField(default=3306)
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=255)
    database_name = models.CharField(max_length=100)
    
    # Settings
    is_primary = models.BooleanField(
        default=True,
        help_text="Is this the primary database for replication?"
    )
    
    # Status
    connection_status = models.CharField(
        max_length=20, 
        choices=STATUS_CHOICES, 
        default="success"
    )
    last_checked = models.DateTimeField(blank=True, null=True)
    
    # Timestamps
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "client_databases"
        verbose_name = "Client Database"
        verbose_name_plural = "Client Databases"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.client.name} → {self.connection_name} ({self.db_type})"

    def save(self, *args, **kwargs):
        """Override save to encrypt password before storing."""
        if self.password and not self._is_password_encrypted():
            self.password = encrypt_password(self.password)
        super().save(*args, **kwargs)

    def _is_password_encrypted(self):
        """Check if password is already encrypted."""
        if not self.password:
            return False
        return len(self.password) > 50 and 'gAAAAA' in self.password

    def get_decrypted_password(self):
        """Get the decrypted password for use in connections."""
        return decrypt_password(self.password)

    def get_connection_url(self):
        """Build a universal SQLAlchemy connection URL."""
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
            if self.db_type != "sqlite":
                socket.create_connection((self.host, self.port), timeout=5)
            
            engine = create_engine(self.get_connection_url(), pool_pre_ping=True)
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.connection_status = "success"
            
        except socket.timeout:
            self.connection_status = "failed"
            raise Exception("Connection timeout - host is unreachable")
        except socket.gaierror:
            self.connection_status = "failed"
            raise Exception(f"Host not found: {self.host}")
        except ConnectionRefusedError:
            self.connection_status = "failed"
            raise Exception(f"Connection refused on port {self.port}")
        except Exception as e:
            self.connection_status = "failed"
            error_msg = str(e)
            if "Access denied" in error_msg or "authentication failed" in error_msg.lower():
                raise Exception("Authentication failed - invalid username or password")
            elif "Unknown database" in error_msg or "does not exist" in error_msg:
                raise Exception(f"Database '{self.database_name}' does not exist")
            elif "Can't connect" in error_msg or "Unable to connect" in error_msg:
                raise Exception(f"Unable to connect to database server at {self.host}:{self.port}")
            else:
                clean_msg = error_msg.split('\n')[0] if '\n' in error_msg else error_msg
                raise Exception(f"Connection error: {clean_msg}")
        finally:
            self.last_checked = timezone.now()
            
            if save:
                self.save(update_fields=["connection_status", "last_checked"])
        
        return self.connection_status