"""
PostgreSQL DDL capture infrastructure setup.

Creates the ddl_capture schema, ddl_events table, and event triggers on the
source PostgreSQL database so that DDL changes are captured and replicated via
Debezium to Kafka, where PostgreSQLKafkaDDLProcessor can consume them.

Must be called before the Debezium source connector is created, because the
connector includes ddl_capture.ddl_events in its table.include.list.

Requires: SUPERUSER or CREATE EVENT TRIGGER privilege on the source database.
All statements are idempotent — safe to run multiple times.
"""

import logging
from typing import Tuple

logger = logging.getLogger(__name__)

_SETUP_SQL = [
    # 1. Schema
    "CREATE SCHEMA IF NOT EXISTS ddl_capture",

    # 2. Events table — matches fields expected by PostgreSQLKafkaDDLProcessor
    """
    CREATE TABLE IF NOT EXISTS ddl_capture.ddl_events (
        id              BIGSERIAL    PRIMARY KEY,
        event_id        UUID         NOT NULL DEFAULT gen_random_uuid(),
        event_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        event_type      TEXT,
        object_type     TEXT,
        schema_name     TEXT,
        object_name     TEXT,
        ddl_command     TEXT,
        object_identity TEXT,
        processed       BOOLEAN      NOT NULL DEFAULT FALSE
    )
    """,

    # 3. Index for fast deduplication
    """
    CREATE INDEX IF NOT EXISTS ddl_events_event_id_idx
        ON ddl_capture.ddl_events (event_id)
    """,

    # 4. Function: captures ddl_command_end events (CREATE TABLE, ALTER TABLE, CREATE INDEX …)
    """
    CREATE OR REPLACE FUNCTION ddl_capture.log_ddl_event()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    AS $$
    DECLARE
        obj    record;
        _query text;
    BEGIN
        _query := current_query();
        FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            IF obj.schema_name IS NULL
               OR obj.schema_name IN (
                   'pg_catalog', 'information_schema',
                   'pg_toast', 'ddl_capture'
               ) THEN
                CONTINUE;
            END IF;

            INSERT INTO ddl_capture.ddl_events (
                event_type, object_type, schema_name,
                object_name, ddl_command, object_identity
            ) VALUES (
                TG_TAG,
                obj.object_type,
                obj.schema_name,
                split_part(obj.object_identity, '.', 2),
                _query,
                obj.object_identity
            );
        END LOOP;
    END;
    $$
    """,

    # 5. Function: captures sql_drop events (DROP TABLE, DROP COLUMN, DROP INDEX …)
    """
    CREATE OR REPLACE FUNCTION ddl_capture.log_ddl_drop_event()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    AS $$
    DECLARE
        obj    record;
        _query text;
    BEGIN
        _query := current_query();
        FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            IF obj.schema_name IS NULL
               OR obj.schema_name IN (
                   'pg_catalog', 'information_schema',
                   'pg_toast', 'ddl_capture'
               ) THEN
                CONTINUE;
            END IF;

            INSERT INTO ddl_capture.ddl_events (
                event_type, object_type, schema_name,
                object_name, ddl_command, object_identity
            ) VALUES (
                TG_TAG,
                obj.object_type,
                obj.schema_name,
                obj.object_name,
                _query,
                obj.object_identity
            );
        END LOOP;
    END;
    $$
    """,

    # 6. Event trigger: ddl_command_end (idempotent)
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_event_trigger WHERE evtname = 'capture_ddl'
        ) THEN
            CREATE EVENT TRIGGER capture_ddl
                ON ddl_command_end
                EXECUTE FUNCTION ddl_capture.log_ddl_event();
        END IF;
    END
    $$
    """,

    # 7. Event trigger: sql_drop (idempotent)
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_event_trigger WHERE evtname = 'capture_ddl_drop'
        ) THEN
            CREATE EVENT TRIGGER capture_ddl_drop
                ON sql_drop
                EXECUTE FUNCTION ddl_capture.log_ddl_drop_event();
        END IF;
    END
    $$
    """,
]


def check_postgresql_privileges(db_config) -> dict:
    """
    Check PostgreSQL privileges required for CDC replication and DDL capture.

    Returns a dict with:
        has_replication    bool  — REPLICATION role (required for CDC)
        can_create_pub     bool  — CREATE on database (required for publication)
        is_superuser       bool  — full superuser
        ddl_table_exists   bool  — ddl_capture.ddl_events table present
        ddl_triggers_found int   — count of our event triggers already installed
        any_ddl_triggers   int   — any ddl_command_end/sql_drop triggers (may be DBA-managed)
        can_setup_ddl      bool  — True if superuser OR triggers already present
        error              str   — non-empty if the check itself failed
    """
    result = {
        'has_replication': False,
        'can_create_pub': False,
        'is_superuser': False,
        'ddl_table_exists': False,
        'ddl_triggers_found': 0,
        'any_ddl_triggers': 0,
        'can_setup_ddl': False,
        'error': '',
    }

    if db_config.db_type.lower() not in ('postgresql', 'postgres'):
        result['error'] = f"Not a PostgreSQL database (type={db_config.db_type})"
        return result

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_config.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            # Replication role
            row = conn.execute(text(
                "SELECT rolreplication, rolsuper "
                "FROM pg_roles WHERE rolname = current_user"
            )).fetchone()
            if row:
                result['has_replication'] = bool(row[0])
                result['is_superuser'] = bool(row[1])

            # CREATE privilege on database (needed for publication in PG15+)
            row = conn.execute(text(
                "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
            )).fetchone()
            if row:
                result['can_create_pub'] = bool(row[0])

            # DDL capture table
            row = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'ddl_capture' AND table_name = 'ddl_events'"
            )).fetchone()
            result['ddl_table_exists'] = (row[0] > 0) if row else False

            # Our specific event triggers
            row = conn.execute(text(
                "SELECT COUNT(*) FROM pg_event_trigger "
                "WHERE evtname IN ('capture_ddl', 'capture_ddl_drop')"
            )).fetchone()
            result['ddl_triggers_found'] = int(row[0]) if row else 0

            # Any event triggers on ddl_command_end / sql_drop (DBA-managed)
            row = conn.execute(text(
                "SELECT COUNT(*) FROM pg_event_trigger "
                "WHERE evtevent IN ('ddl_command_end', 'sql_drop')"
            )).fetchone()
            result['any_ddl_triggers'] = int(row[0]) if row else 0

        engine.dispose()

        # can_setup_ddl: superuser can create triggers; or they're already there
        result['can_setup_ddl'] = (
            result['is_superuser']
            or result['ddl_triggers_found'] >= 2
            or result['any_ddl_triggers'] >= 2
        )

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Privilege check failed for {db_config.host}/{db_config.database_name}: {e}")

    return result


def setup_postgresql_ddl_capture(db_config) -> Tuple[bool, str]:
    """
    Create the DDL capture infrastructure on the source PostgreSQL database.

    Executes idempotent SQL to create:
    - ddl_capture schema
    - ddl_capture.ddl_events table
    - log_ddl_event / log_ddl_drop_event trigger functions
    - capture_ddl / capture_ddl_drop event triggers

    Args:
        db_config: ClientDatabase instance (must be PostgreSQL).

    Returns:
        (success, message)
    """
    if db_config.db_type.lower() not in ('postgresql', 'postgres'):
        return False, f"Not a PostgreSQL database (type={db_config.db_type})"

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_config.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            for sql in _SETUP_SQL:
                conn.execute(text(sql))
            conn.commit()
        engine.dispose()

        logger.info(
            f"PostgreSQL DDL capture setup complete: "
            f"{db_config.host}/{db_config.database_name}"
        )
        return True, "DDL capture infrastructure created successfully"

    except Exception as e:
        msg = f"Failed to set up PostgreSQL DDL capture: {e}"
        logger.error(msg)
        return False, msg