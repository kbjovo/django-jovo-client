"""
Jolokia JMX client for querying Debezium snapshot metrics.

Uses the Jolokia HTTP agent running inside Kafka Connect to read
Debezium's JMX MBeans for snapshot progress, streaming status, etc.
"""

import logging
from typing import Any, Dict, Optional

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

# Map db_type (as stored in ClientDatabase.db_type) to the Debezium
# connector type used in JMX MBean object names.
DB_TYPE_TO_MBEAN_TYPE = {
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'mssql': 'sqlserver',
}


class JolokiaClient:
    """
    HTTP client for Jolokia JVM agent running on Kafka Connect.

    Queries Debezium JMX MBeans to get real-time snapshot progress,
    streaming metrics, and connector health data.
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 5):
        url = (
            base_url
            or settings.DEBEZIUM_CONFIG.get('JOLOKIA_URL', 'http://localhost:8778/jolokia')
        )
        # Jolokia requires a trailing slash on the agent context path
        self.base_url = url.rstrip('/') + '/'
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Low-level MBean access
    # ------------------------------------------------------------------

    def _build_mbean(self, db_type: str, context: str, server: str) -> str:
        """Build a Debezium MBean object name.

        Args:
            db_type: Database type key (mysql, postgresql, mssql).
            context: MBean context (snapshot, incremental-snapshot, streaming).
            server: The topic.prefix / server name of the connector.

        Returns:
            Fully-qualified MBean name string.
        """
        connector_type = DB_TYPE_TO_MBEAN_TYPE.get(db_type)
        if not connector_type:
            raise ValueError(f"Unsupported db_type for Jolokia: {db_type}")
        return (
            f"debezium.{connector_type}:"
            f"type=connector-metrics,context={context},server={server}"
        )

    def _read_mbean(self, mbean: str) -> Optional[Dict[str, Any]]:
        """Read all attributes of an MBean via Jolokia POST API.

        Returns:
            Dict of attribute values, or None if the MBean does not exist
            or Jolokia is unreachable.
        """
        try:
            resp = requests.post(
                self.base_url,
                json={"type": "read", "mbean": mbean},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                logger.debug("Jolokia returned HTTP %s", resp.status_code)
                return None

            data = resp.json()

            # Jolokia returns status 404 inside the JSON body when an MBean
            # is not (yet) registered – e.g. before the snapshot starts.
            if data.get('status') != 200:
                return None

            return data.get('value')

        except requests.ConnectionError:
            logger.warning("Jolokia unreachable at %s", self.base_url)
            return None
        except requests.Timeout:
            logger.warning("Jolokia request timed out (%ss)", self.timeout)
            return None
        except Exception:
            logger.warning("Jolokia request failed", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Snapshot metrics (initial snapshot)
    # ------------------------------------------------------------------

    def get_snapshot_metrics(
        self, db_type: str, topic_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """Return raw MBean attributes for the initial-snapshot context."""
        mbean = self._build_mbean(db_type, 'snapshot', topic_prefix)
        return self._read_mbean(mbean)

    def get_snapshot_progress(
        self, db_type: str, topic_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """Return a normalised snapshot progress dict.

        Returns None when the MBean is unavailable (Jolokia down or
        snapshot context not yet registered).
        """
        raw = self.get_snapshot_metrics(db_type, topic_prefix)
        if raw is None:
            return None
        return self._normalize_snapshot(raw, snapshot_type='initial')

    def is_snapshot_complete(self, db_type: str, topic_prefix: str) -> Optional[bool]:
        """Quick check: has the initial snapshot finished?

        Returns None when Jolokia is unavailable (caller should fall back).
        """
        raw = self.get_snapshot_metrics(db_type, topic_prefix)
        if raw is None:
            return None
        return bool(raw.get('SnapshotCompleted'))

    # ------------------------------------------------------------------
    # Incremental snapshot metrics
    # ------------------------------------------------------------------

    def get_incremental_snapshot_metrics(
        self, db_type: str, topic_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """Return raw MBean attributes for the incremental-snapshot context."""
        mbean = self._build_mbean(db_type, 'incremental-snapshot', topic_prefix)
        return self._read_mbean(mbean)

    def get_incremental_snapshot_progress(
        self, db_type: str, topic_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """Return a normalised incremental-snapshot progress dict.

        Returns None when the MBean is unavailable.
        """
        raw = self.get_incremental_snapshot_metrics(db_type, topic_prefix)
        if raw is None:
            return None
        return self._normalize_snapshot(raw, snapshot_type='incremental')

    # ------------------------------------------------------------------
    # Streaming metrics
    # ------------------------------------------------------------------

    def get_streaming_metrics(
        self, db_type: str, topic_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """Return raw MBean attributes for the streaming context."""
        mbean = self._build_mbean(db_type, 'streaming', topic_prefix)
        return self._read_mbean(mbean)

    # ------------------------------------------------------------------
    # Combined snapshot status (initial OR incremental)
    # ------------------------------------------------------------------

    def get_active_snapshot_progress(
        self, db_type: str, topic_prefix: str
    ) -> Optional[Dict[str, Any]]:
        """Return progress of whichever snapshot type is currently active.

        Checks incremental first (more common after initial setup), then
        falls back to initial snapshot. Returns None if neither is active.
        """
        # Check incremental first
        inc = self.get_incremental_snapshot_progress(db_type, topic_prefix)
        if inc and inc.get('running'):
            return inc

        # Fall back to initial snapshot
        snap = self.get_snapshot_progress(db_type, topic_prefix)
        if snap and (snap.get('running') or snap.get('completed')):
            return snap

        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_snapshot(raw: Dict[str, Any], snapshot_type: str) -> Dict[str, Any]:
        """Convert raw MBean attributes into a clean, consistent dict."""
        total = raw.get('TotalTableCount', 0)
        remaining = raw.get('RemainingTableCount', 0)
        rows_scanned = raw.get('RowsScanned') or {}

        return {
            'type': snapshot_type,
            'running': bool(raw.get('SnapshotRunning', False)),
            'completed': bool(raw.get('SnapshotCompleted', False)),
            'aborted': bool(raw.get('SnapshotAborted', False)),
            'total_tables': total,
            'remaining_tables': remaining,
            'completed_tables': max(total - remaining, 0),
            'rows_scanned': rows_scanned,
            'total_rows_scanned': sum(rows_scanned.values()) if rows_scanned else 0,
            'duration_seconds': raw.get('SnapshotDurationInSeconds', 0),
            'current_table': raw.get('CurrentCollectionInSnapshot'),
        }