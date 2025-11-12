"""
Debezium Incremental Snapshot Utilities

Provides functionality to trigger incremental snapshots for newly added tables
using Debezium's signaling feature.
"""

import logging
import uuid
from typing import List, Optional
from sqlalchemy import text

from client.utils.database_utils import get_database_engine

logger = logging.getLogger(__name__)


def trigger_incremental_snapshot(
    replication_config,
    table_names: Optional[List[str]] = None
) -> bool:
    """
    Trigger an incremental snapshot for specific tables using Debezium signaling.

    This is useful when:
    1. Adding new tables to an existing connector
    2. The snapshot.new.tables config is not available/working
    3. You need manual control over when snapshots occur

    Args:
        replication_config: ReplicationConfig instance
        table_names: List of table names to snapshot (if None, snapshot all enabled tables)

    Returns:
        bool: True if signal was sent successfully
    """
    try:
        # Get source database
        source_db = replication_config.client_database
        db_name = source_db.database_name

        # Get tables to snapshot
        if table_names is None:
            enabled_mappings = replication_config.table_mappings.filter(is_enabled=True)
            table_names = [tm.source_table for tm in enabled_mappings]

        if not table_names:
            logger.warning("No tables to snapshot")
            return False

        # Create engine for source database
        source_engine = get_database_engine(source_db)

        # Check if signal table exists
        signal_table = "debezium_signal"

        with source_engine.connect() as conn:
            # Check if signal table exists
            result = conn.execute(text(f"SHOW TABLES LIKE '{signal_table}'"))
            if not result.fetchone():
                logger.info(f"Creating signal table: {signal_table}")

                # Create signal table
                create_signal_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {signal_table} (
                    id VARCHAR(42) PRIMARY KEY,
                    type VARCHAR(32) NOT NULL,
                    data VARCHAR(2048)
                )
                """
                conn.execute(text(create_signal_table_sql))
                conn.commit()

                logger.info(f"✅ Created signal table: {signal_table}")

                # Important: Add signal table to connector's table.include.list
                logger.warning(
                    f"⚠️ IMPORTANT: Add '{db_name}.{signal_table}' to connector's table.include.list "
                    f"for signals to work!"
                )

        # Build data payload for incremental snapshot
        # Format: {"data-collections": ["db.table1", "db.table2"]}
        data_collections = [f"{db_name}.{table}" for table in table_names]
        data_payload = '{"data-collections": [' + ','.join(f'"{dc}"' for dc in data_collections) + ']}'

        # Generate unique signal ID
        signal_id = str(uuid.uuid4())

        # Insert signal to trigger incremental snapshot
        with source_engine.begin() as conn:
            insert_signal_sql = f"""
            INSERT INTO {signal_table} (id, type, data)
            VALUES (:id, 'execute-snapshot', :data)
            """

            conn.execute(
                text(insert_signal_sql),
                {"id": signal_id, "data": data_payload}
            )

            logger.info(
                f"✅ Incremental snapshot signal sent!\n"
                f"   Signal ID: {signal_id}\n"
                f"   Tables: {', '.join(data_collections)}"
            )

        source_engine.dispose()

        return True

    except Exception as e:
        logger.error(f"❌ Failed to trigger incremental snapshot: {e}", exc_info=True)
        return False


def check_snapshot_status(replication_config) -> dict:
    """
    Check the status of ongoing snapshots by querying the source database.

    Note: This requires the signal table to exist and be monitored.

    Returns:
        dict: Status information about snapshots
    """
    try:
        source_db = replication_config.client_database
        source_engine = get_database_engine(source_db)

        signal_table = "debezium_signal"

        with source_engine.connect() as conn:
            # Check if signal table exists
            result = conn.execute(text(f"SHOW TABLES LIKE '{signal_table}'"))
            if not result.fetchone():
                return {
                    'signal_table_exists': False,
                    'message': 'Signal table does not exist'
                }

            # Get recent signals
            query = f"""
            SELECT id, type, data
            FROM {signal_table}
            ORDER BY id DESC
            LIMIT 10
            """

            result = conn.execute(text(query))
            signals = [dict(row) for row in result]

            source_engine.dispose()

            return {
                'signal_table_exists': True,
                'recent_signals': signals,
                'count': len(signals)
            }

    except Exception as e:
        logger.error(f"Error checking snapshot status: {e}")
        return {
            'error': str(e)
        }


def setup_signal_table(replication_config) -> bool:
    """
    Set up the Debezium signal table in the source database.

    This table is used to send commands to Debezium (like triggering snapshots).

    Returns:
        bool: True if setup was successful
    """
    try:
        source_db = replication_config.client_database
        source_engine = get_database_engine(source_db)

        signal_table = "debezium_signal"

        with source_engine.begin() as conn:
            # Create signal table
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {signal_table} (
                id VARCHAR(42) PRIMARY KEY,
                type VARCHAR(32) NOT NULL,
                data VARCHAR(2048)
            )
            """
            conn.execute(text(create_sql))

            logger.info(f"✅ Signal table '{signal_table}' is ready")

        source_engine.dispose()

        # Remind user to add to table.include.list
        db_name = source_db.database_name
        logger.warning(
            f"\n{'='*80}\n"
            f"⚠️  IMPORTANT SETUP STEP:\n"
            f"   Add '{db_name}.{signal_table}' to your connector's table.include.list\n"
            f"   Example: table.include.list = '{db_name}.users,{db_name}.orders,{db_name}.{signal_table}'\n"
            f"{'='*80}\n"
        )

        return True

    except Exception as e:
        logger.error(f"❌ Failed to setup signal table: {e}", exc_info=True)
        return False