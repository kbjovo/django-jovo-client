"""
File: client/models/database.py
FIXED: Oracle connection with proper oracledb driver configuration
"""

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
        ("mssql", "SQL Server"),
    ]

    STATUS_CHOICES = [
        ("success", "Success"),
        ("failed", "Failed"),
        ("inactive", "Inactive"),
    ]

    client = models.ForeignKey(
        Client,
        on_delete=models.CASCADE,
        related_name="client_databases"
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
        default=False,
        help_text="Source database for CDC"
    )
    is_target = models.BooleanField(
        default=False,
        help_text="Target database where replicated data will be stored"
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
        constraints = [
            models.UniqueConstraint(
                fields=['client', 'is_target'],
                condition=models.Q(is_target=True),
                name='unique_target_per_client'
            )
        ]

    def __str__(self):
        db_type_display = "Source" if self.is_primary else "Target" if self.is_target else "Database"
        return f"{self.client.name} → {self.connection_name} ({db_type_display})"

    def save(self, *args, **kwargs):
        """Encrypt password before storing."""
        if self.password and not self._is_password_encrypted():
            self.password = encrypt_password(self.password)
        super().save(*args, **kwargs)

    def _is_password_encrypted(self):
        if not self.password:
            return False
        return len(self.password) > 50 and 'gAAAAA' in self.password

    def get_decrypted_password(self):
        return decrypt_password(self.password)

    def get_connection_url(self):
        """
        Get SQLAlchemy connection URL for the database
        
        FIXED: Oracle now uses correct thin mode connection string
        """
        password = quote_plus(self.get_decrypted_password() or "")

        if self.db_type == "mysql":
            return f"mysql+pymysql://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"

        elif self.db_type == "postgresql":
            return f"postgresql+psycopg2://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"

        elif self.db_type == "sqlite":
            return f"sqlite:///{self.database_name}"

        elif self.db_type == "oracle":
            # ✅ FIXED: Proper Oracle connection string for thin mode
            # Format: oracle+oracledb://user:pass@host:port/?service_name=ORCLPDB1
            # 
            # IMPORTANT: database_name should be the SERVICE NAME (e.g., ORCLPDB1)
            # NOT the SID (e.g., XE)
            return f"oracle+oracledb://{self.username}:{password}@{self.host}:{self.port}/?service_name={self.database_name}"

        elif self.db_type == "mssql":
            return f"mssql+pymssql://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"

        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def check_connection_status(self, save=True):
        """
        Test database connection and update status
        
        FIXED: Better error handling for Oracle connections
        """
        try:
            # Network connectivity check (skip for SQLite)
            if self.db_type != "sqlite":
                try:
                    socket.create_connection((self.host, self.port), timeout=5)
                except socket.timeout:
                    self.connection_status = "failed"
                    raise Exception(f"Connection timeout - host {self.host}:{self.port} is unreachable")
                except socket.gaierror:
                    self.connection_status = "failed"
                    raise Exception(f"Host not found: {self.host}")
                except ConnectionRefusedError:
                    self.connection_status = "failed"
                    raise Exception(f"Connection refused on port {self.port}")

            # Database connection test
            connection_url = self.get_connection_url()
            
            # ✅ FIXED: Oracle-specific engine configuration
            if self.db_type == "oracle":
                # Oracle thin mode - no special connect_args needed
                engine = create_engine(
                    connection_url,
                    pool_pre_ping=True,
                    echo=False
                )
            else:
                engine = create_engine(connection_url, pool_pre_ping=True)
            
            with engine.connect() as conn:
                # Database-specific test queries
                if self.db_type == "oracle":
                    conn.execute(text("SELECT 1 FROM DUAL"))
                else:
                    conn.execute(text("SELECT 1"))

            engine.dispose()
            self.connection_status = "success"

        except Exception as e:
            self.connection_status = "failed"
            msg = str(e)
            
            # Enhanced error messages
            if "Access denied" in msg or "authentication failed" in msg.lower():
                raise Exception("Authentication failed - invalid username or password")
            elif "Unknown database" in msg or "does not exist" in msg:
                raise Exception(f"Database/Service '{self.database_name}' does not exist")
            elif "listener" in msg.lower() or "tns" in msg.lower():
                raise Exception(f"Oracle listener error - check host:port ({self.host}:{self.port}) and service name ({self.database_name})")
            elif "service_name" in msg.lower():
                raise Exception(f"Invalid service name '{self.database_name}'. Use ORCLPDB1 for pluggable database or XE for container database")
            elif "Can't connect" in msg or "Unable to connect" in msg:
                raise Exception(f"Unable to connect to database server at {self.host}:{self.port}")
            else:
                # Return first line of error for clarity
                raise Exception(f"Connection error: {msg.splitlines()[0]}")
        finally:
            self.last_checked = timezone.now()
            if save:
                self.save(update_fields=["connection_status", "last_checked"])
        
        return self.connection_status