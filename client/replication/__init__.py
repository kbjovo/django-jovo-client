"""
Replication module - Organized CDC replication system.

This module provides a clean, maintainable architecture for CDC replication:
- ReplicationOrchestrator: Main entry point for all replication operations
- ResilientKafkaConsumer: Auto-restarting consumer with health monitoring
- HealthMonitor: Monitors connector and consumer health
- Validators: Pre-flight checks before operations
"""

from .orchestrator import ReplicationOrchestrator
from .consumer import ResilientKafkaConsumer
from .health_monitor import monitor_replication_health
from .validators import ReplicationValidator

__all__ = [
    'ReplicationOrchestrator',
    'ResilientKafkaConsumer',
    'monitor_replication_health',
    'ReplicationValidator',
]