"""
EdgeAppliance model  —  TARGET PATH: client/models/edge.py

One appliance per client for v1 (OneToOne). It carries the client's whole data
plane at their site; the ReplicationConfigs for that client route to it.

Endpoints are populated by the appliance itself at registration (over the
Netbird mesh) — central never guesses them. When a client has no reachable
appliance, the connector manager / DDL consumer fall back to the central data
plane, so existing (non-edge) clients are completely unaffected.

Wire-up steps (urls, __init__ export, migration, manager & DDL changes) are in
edge-appliance/central-app/INTEGRATION.md.
"""
import hashlib
import secrets
import uuid
from datetime import timedelta

from django.db import models
from django.utils import timezone

from .client import Client


def _hash(raw: str) -> str:
    """SHA-256 hex of a secret. We store only the hash, never the plaintext."""
    return hashlib.sha256(raw.encode()).hexdigest()


class EdgeAppliance(models.Model):
    PLATFORM_CHOICES = [
        ("vm_hyperv", "VM · Hyper-V"),
        ("vm_vmware", "VM · VMware"),
        ("vm_kvm", "VM · KVM/Proxmox"),
        ("windows_service", "Windows · installer/service"),
        ("linux_service", "Linux · service"),
        ("debezium_server", "Native Debezium Server"),
        ("other", "Other"),
    ]
    STATUS_CHOICES = [
        ("pending", "Pending enrollment"),
        ("enrolling", "Enrolling"),
        ("online", "Online"),
        ("degraded", "Degraded"),
        ("offline", "Offline"),
        ("decommissioned", "Decommissioned"),
    ]

    # v1: one appliance per client. Widen to FK + a `site` field for multi-site later.
    client = models.OneToOneField(Client, on_delete=models.CASCADE, related_name="edge_appliance")
    appliance_uid = models.UUIDField(default=uuid.uuid4, unique=True, editable=False, db_index=True)
    name = models.CharField(max_length=80)
    platform = models.CharField(max_length=32, choices=PLATFORM_CHOICES, default="vm_hyperv")
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="pending", db_index=True)

    # Mesh endpoints — set by the appliance at registration (Netbird IP + known ports).
    netbird_ip = models.GenericIPAddressField(null=True, blank=True)
    connect_url = models.URLField(blank=True, help_text="Kafka Connect REST over the mesh, e.g. http://100.x.y.z:8083")
    kafka_bootstrap = models.CharField(max_length=255, blank=True, help_text="Edge Kafka on the mesh, e.g. 100.x.y.z:9092")
    schema_registry_url = models.URLField(blank=True)
    jolokia_url = models.URLField(blank=True)

    agent_version = models.CharField(max_length=32, blank=True)
    stack_versions = models.JSONField(default=dict, blank=True)

    # Auth material — hashes only.
    enroll_token_hash = models.CharField(max_length=64, blank=True)
    enroll_token_expires_at = models.DateTimeField(null=True, blank=True)
    agent_secret_hash = models.CharField(max_length=64, blank=True)

    last_heartbeat_at = models.DateTimeField(null=True, blank=True)
    enrolled_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    decommissioned_at = models.DateTimeField(null=True, blank=True)

    # An appliance is "online" only if it has heartbeated within this window.
    HEARTBEAT_ONLINE_WINDOW = timedelta(seconds=90)

    class Meta:
        db_table = "edge_appliance"
        verbose_name = "Edge Appliance"
        verbose_name_plural = "Edge Appliances"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.name} [{self.get_status_display()}] · {self.client.name}"

    # ------------------------------------------------------------------ tokens
    def issue_enrollment_token(self, ttl_hours: int = 24) -> str:
        """Mint a one-time enrollment token. Returns plaintext ONCE; we keep the hash."""
        raw = "EDGE-" + secrets.token_urlsafe(24)
        self.enroll_token_hash = _hash(raw)
        self.enroll_token_expires_at = timezone.now() + timedelta(hours=ttl_hours)
        self.status = "pending"
        self.save(update_fields=["enroll_token_hash", "enroll_token_expires_at", "status", "updated_at"])
        return raw

    def check_enrollment_token(self, raw: str) -> bool:
        if not self.enroll_token_hash or not raw:
            return False
        if self.enroll_token_expires_at and timezone.now() > self.enroll_token_expires_at:
            return False
        return secrets.compare_digest(self.enroll_token_hash, _hash(raw))

    def issue_agent_secret(self) -> str:
        """Mint the long-lived heartbeat credential. Returns plaintext ONCE."""
        raw = secrets.token_urlsafe(32)
        self.agent_secret_hash = _hash(raw)
        return raw

    def check_agent_secret(self, raw: str) -> bool:
        if not self.agent_secret_hash or not raw:
            return False
        return secrets.compare_digest(self.agent_secret_hash, _hash(raw))

    # ------------------------------------------------------------------- state
    def mark_registered(self, *, connect_url, kafka_bootstrap="", schema_registry_url="",
                        jolokia_url="", netbird_ip=None, agent_version="", stack_versions=None):
        self.connect_url = connect_url
        self.kafka_bootstrap = kafka_bootstrap or self.kafka_bootstrap
        self.schema_registry_url = schema_registry_url or self.schema_registry_url
        self.jolokia_url = jolokia_url or self.jolokia_url
        self.netbird_ip = netbird_ip or self.netbird_ip
        self.agent_version = agent_version or self.agent_version
        if stack_versions:
            self.stack_versions = stack_versions
        self.status = "online"
        self.enrolled_at = self.enrolled_at or timezone.now()
        self.last_heartbeat_at = timezone.now()
        # Enrollment token is single-use — burn it.
        self.enroll_token_hash = ""
        self.enroll_token_expires_at = None
        self.save()

    def touch_heartbeat(self, status="online", stack_versions=None):
        self.last_heartbeat_at = timezone.now()
        if stack_versions is not None:
            self.stack_versions = stack_versions
        if self.status != "decommissioned":
            self.status = status
        self.save(update_fields=["last_heartbeat_at", "stack_versions", "status", "updated_at"])

    # --------------------------------------------------------------- resolvers
    @property
    def is_reachable(self) -> bool:
        """True if central should route this client's connectors to the appliance."""
        return bool(self.connect_url) and self.status in ("online", "degraded")

    @property
    def is_online(self) -> bool:
        if not self.last_heartbeat_at:
            return False
        return timezone.now() - self.last_heartbeat_at <= self.HEARTBEAT_ONLINE_WINDOW
