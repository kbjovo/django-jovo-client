"""
client.replication.orchestrator package.

Physically split from a single 2586-line module into concern-focused mixins. The
public API is unchanged: `from client.replication.orchestrator import
ReplicationOrchestrator` still works, and the composed class exposes exactly the
same methods with identical behavior.
"""
from .lifecycle import LifecycleMixin
from .sink import SinkConnectorMixin
from .source import SourceConnectorMixin
from .topics import TopicsMixin
from .tables import TablesMixin
from .status import StatusMixin
from .control import ControlMixin
from .base import OrchestratorBase


class ReplicationOrchestrator(LifecycleMixin, SinkConnectorMixin, SourceConnectorMixin, TopicsMixin, TablesMixin, StatusMixin, ControlMixin, OrchestratorBase):
    """Orchestrates all replication operations.

This is the single source of truth for managing CDC replication.
It ensures connector and consumer are always in sync."""
    pass


__all__ = ["ReplicationOrchestrator"]
