"""
File: client/models/database.py
ENHANCED: Oracle connection with service_name/SID mode selection
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

    # ✅ NEW: Oracle connection mode choices
    ORACLE_CONN_MODE_CHOICES = [
        ("service", "Service Name (Recommended)"),
        ("sid", "SID (Legacy)"),
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

    # ✅ NEW: Oracle-specific connection mode
    oracle_connection_mode = models.CharField(
        max_length=20,
        choices=ORACLE_CONN_MODE_CHOICES,
        default="service",
        blank=True,
        null=True,
        help_text="Oracle connection type: service name (PDB, Docker, Cloud) or SID (legacy)"
    )

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
        
        # ✅ Auto-set oracle_connection_mode to 'service' if Oracle and not set
        if self.db_type == "oracle" and not self.oracle_connection_mode:
            self.oracle_connection_mode = "service"
        
        super().save(*args, **kwargs)

    def _is_password_encrypted(self):
        if not self.password:
            return False
        return len(self.password) > 50 and 'gAAAAA' in self.password

    def get_decrypted_password(self):
        return decrypt_password(self.password)

    def get_oracle_connection_display(self):
        """
        Get human-readable Oracle connection info
        Returns: 'XEPDB1 (Service)' or 'XE (SID)'
        """
        if self.db_type != "oracle":
            return self.database_name
        
        mode = self.oracle_connection_mode or "service"
        mode_display = "Service" if mode == "service" else "SID"
        return f"{self.database_name} ({mode_display})"

    def get_connection_url(self):
        """
        Get SQLAlchemy connection URL for the database
        
        ✅ ENHANCED: Oracle now supports both service_name and SID modes
        """
        password = quote_plus(self.get_decrypted_password() or "")

        if self.db_type == "mysql":
            return f"mysql+pymysql://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"

        elif self.db_type == "postgresql":
            return f"postgresql+psycopg2://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"

        elif self.db_type == "sqlite":
            return f"sqlite:///{self.database_name}"

        elif self.db_type == "oracle":
            # ✅ ENHANCED: Support both service_name and SID
            mode = self.oracle_connection_mode or "service"
            
            if mode == "sid":
                # Legacy SID-based connection (e.g., XE for container database)
                # Format: oracle+oracledb://user:pass@host:port/?sid=XE
                return (
                    f"oracle+oracledb://{self.username}:{password}"
                    f"@{self.host}:{self.port}/?sid={self.database_name}"
                )
            else:
                # Default: SERVICE NAME (recommended for PDB, Docker, Cloud)
                # Format: oracle+oracledb://user:pass@host:port/?service_name=XEPDB1
                return (
                    f"oracle+oracledb://{self.username}:{password}"
                    f"@{self.host}:{self.port}/?service_name={self.database_name}"
                )

        elif self.db_type == "mssql":
            return f"mssql+pymssql://{self.username}:{password}@{self.host}:{self.port}/{self.database_name}"

        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def check_connection_status(self, save=True):
        """
        Test database connection and update status
        
        ✅ ENHANCED: Better Oracle error messages with service/SID context
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
            
            # ✅ Oracle-specific engine configuration
            if self.db_type == "oracle":
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
            
            # ✅ ENHANCED: Oracle-specific error messages with service/SID context
            if "Access denied" in msg or "authentication failed" in msg.lower():
                raise Exception("Authentication failed - invalid username or password")
            elif "Unknown database" in msg or "does not exist" in msg:
                raise Exception(f"Database/Service '{self.database_name}' does not exist")
            elif "ORA-12505" in msg:
                # TNS listener error - suggest checking service/SID mode
                mode = self.oracle_connection_mode or "service"
                if mode == "service":
                    raise Exception(
                        f"TNS:listener does not currently know of SID - "
                        f"Service '{self.database_name}' not found. "
                        f"Try switching to SID mode if this is a container database (e.g., XE), "
                        f"or verify the service name (e.g., XEPDB1 for pluggable database)"
                    )
                else:
                    raise Exception(
                        f"TNS:listener error - SID '{self.database_name}' not found. "
                        f"Try switching to Service Name mode if this is a pluggable database (e.g., XEPDB1)"
                    )
            elif "listener" in msg.lower() or "tns" in msg.lower():
                raise Exception(
                    f"Oracle listener error - check host:port ({self.host}:{self.port}) "
                    f"and {self.oracle_connection_mode or 'service'} name ({self.database_name})"
                )
            elif "service_name" in msg.lower() or "sid" in msg.lower():
                mode = self.oracle_connection_mode or "service"
                raise Exception(
                    f"Invalid {mode} '{self.database_name}'. "
                    f"Use XEPDB1 for pluggable database or XE for container database. "
                    f"Try switching connection mode if needed."
                )
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