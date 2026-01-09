import uuid
import json
import logging
from typing import List, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

class DebeziumSignalManager:
    """
    Manages Debezium signals for MySQL, Postgres, SQL Server, and Oracle.
    Uses GUID-based signaling to trigger connector actions via the source DB.
    """

    def __init__(self, db_engine: Engine, signal_table: str = "inventory.debezium_signal"):
        """
        Args:
            db_engine: SQLAlchemy engine for the SOURCE database.
            signal_table: Fully qualified name (schema.table) of the signal table.
        """
        self.engine = db_engine
        self.signal_table = signal_table

    def _send_signal(self, signal_type: str, data: dict) -> str:
        """
        Generic method to insert a signal into the database.
        
        Args:
            signal_type: The Debezium command (e.g., 'execute-snapshot')
            data: The parameters for that command
            
        Returns:
            str: The GUID (ID) of the signal sent.
        """
        # Generate a unique GUID for this signal
        signal_id = str(uuid.uuid4())
        
        # SQL insert using the standard Debezium 3-column structure
        query = text(f"""
            INSERT INTO {self.signal_table} (id, type, data)
            VALUES (:id, :type, :data)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(query, {
                    "id": signal_id, 
                    "type": signal_type, 
                    "data": json.dumps(data)
                })
            logger.info(f"Signal '{signal_type}' sent successfully with GUID: {signal_id}")
            return signal_id
        except Exception as e:
            logger.error(f"Failed to send Debezium signal: {e}")
            raise

    def trigger_adhoc_snapshot(self, tables: List[str]):
        """
        Triggers an incremental snapshot for specific tables.
        This allows re-syncing data without locking the database.
        """
        # data-collections format: ["schema.table1", "schema.table2"]
        payload = {
            "type": "incremental",
            "data-collections": tables
        }
        return self._send_signal("execute-snapshot", payload)

    def pause_incremental_snapshot(self):
        """Pauses any currently running incremental snapshots."""
        return self._send_signal("pause-snapshot-window", {})

    def resume_incremental_snapshot(self):
        """Resumes a paused incremental snapshot."""
        return self._send_signal("resume-snapshot-window", {})

    def log_marker(self, message: str):
        """Injects a custom string marker into the Kafka partition."""
        return self._send_signal("log", {"message": message})



