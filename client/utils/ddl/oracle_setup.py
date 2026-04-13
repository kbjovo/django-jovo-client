"""
Oracle CDC privilege checker.

Connects to the source Oracle database and verifies that the connector user has
all privileges required for Debezium LogMiner-based CDC.

Checks:
  - Database is in ARCHIVELOG mode
  - Supplemental logging is enabled (MIN level)
  - LOGMINING system privilege
  - SELECT ANY TABLE (or equivalent table grants)
  - CREATE TABLE (for DEBEZIUM_SIGNAL table auto-creation)
  - Access to required V$ dynamic views
  - EXECUTE_CATALOG_ROLE / SELECT_CATALOG_ROLE roles
  - Whether DEBEZIUM_SIGNAL table already exists in the user's schema
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def check_oracle_privileges(db_config) -> Dict[str, Any]:
    """
    Check Oracle privileges required for Debezium LogMiner CDC.

    Returns a dict with:
        archivelog_mode         bool  — DB is in ARCHIVELOG mode (required)
        supplemental_logging    bool  — MIN supplemental logging enabled (required)
        has_logmining           bool  — LOGMINING system privilege (required)
        has_select_any_table    bool  — SELECT ANY TABLE privilege
        has_flashback_any_table bool  — FLASHBACK ANY TABLE privilege (required for AS OF SCN snapshot)
        has_create_table        bool  — CREATE TABLE privilege (for signal table)
        has_v_database          bool  — SELECT on V$DATABASE
        has_v_logfile           bool  — SELECT on V$LOGFILE
        has_v_archived_log      bool  — SELECT on V$ARCHIVED_LOG
        has_v_transaction       bool  — SELECT on V$TRANSACTION
        has_execute_catalog     bool  — EXECUTE_CATALOG_ROLE role
        has_select_catalog      bool  — SELECT_CATALOG_ROLE role
        signal_table_exists     bool  — DEBEZIUM_SIGNAL table already created
        error                   str   — non-empty if the check itself failed
    """
    result = {
        'archivelog_mode': False,
        'supplemental_logging': False,
        'has_logmining': False,
        'has_select_any_table': False,
        'has_flashback_any_table': False,
        'has_create_table': False,
        'has_v_database': False,
        'has_v_logfile': False,
        'has_v_archived_log': False,
        'has_v_transaction': False,
        'has_execute_catalog': False,
        'has_select_catalog': False,
        'signal_table_exists': False,
        'error': '',
    }

    if db_config.db_type.lower() != 'oracle':
        result['error'] = f"Not an Oracle database (type={db_config.db_type})"
        return result

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_config.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:

            # 1. ARCHIVELOG mode
            try:
                row = conn.execute(text("SELECT LOG_MODE FROM V$DATABASE")).fetchone()
                if row:
                    result['archivelog_mode'] = (row[0].strip().upper() == 'ARCHIVELOG')
                    result['has_v_database'] = True
            except Exception:
                result['has_v_database'] = False

            # 2. Supplemental logging (MIN level)
            try:
                row = conn.execute(
                    text("SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")
                ).fetchone()
                if row:
                    result['supplemental_logging'] = (row[0].strip().upper() == 'YES')
            except Exception:
                pass

            # 3. LOGMINING privilege
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE = 'LOGMINING'"
                )).fetchone()
                result['has_logmining'] = (row[0] > 0) if row else False
            except Exception:
                pass

            # 4. SELECT ANY TABLE
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE = 'SELECT ANY TABLE'"
                )).fetchone()
                result['has_select_any_table'] = (row[0] > 0) if row else False
            except Exception:
                pass

            # 4b. FLASHBACK ANY TABLE (required for AS OF SCN snapshot queries)
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE = 'FLASHBACK ANY TABLE'"
                )).fetchone()
                result['has_flashback_any_table'] = (row[0] > 0) if row else False
            except Exception:
                pass

            # 5. CREATE TABLE
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM SESSION_PRIVS WHERE PRIVILEGE = 'CREATE TABLE'"
                )).fetchone()
                result['has_create_table'] = (row[0] > 0) if row else False
            except Exception:
                pass

            # 6. V$LOGFILE access
            try:
                conn.execute(text("SELECT COUNT(*) FROM V$LOGFILE WHERE ROWNUM = 1")).fetchone()
                result['has_v_logfile'] = True
            except Exception:
                result['has_v_logfile'] = False

            # 7. V$ARCHIVED_LOG access
            try:
                conn.execute(text("SELECT COUNT(*) FROM V$ARCHIVED_LOG WHERE ROWNUM = 1")).fetchone()
                result['has_v_archived_log'] = True
            except Exception:
                result['has_v_archived_log'] = False

            # 8. V$TRANSACTION access
            try:
                conn.execute(text("SELECT COUNT(*) FROM V$TRANSACTION WHERE ROWNUM = 1")).fetchone()
                result['has_v_transaction'] = True
            except Exception:
                result['has_v_transaction'] = False

            # 9. EXECUTE_CATALOG_ROLE
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM SESSION_ROLES WHERE ROLE = 'EXECUTE_CATALOG_ROLE'"
                )).fetchone()
                result['has_execute_catalog'] = (row[0] > 0) if row else False
            except Exception:
                pass

            # 10. SELECT_CATALOG_ROLE
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM SESSION_ROLES WHERE ROLE = 'SELECT_CATALOG_ROLE'"
                )).fetchone()
                result['has_select_catalog'] = (row[0] > 0) if row else False
            except Exception:
                pass

            # 11. DEBEZIUM_SIGNAL table
            try:
                row = conn.execute(text(
                    "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'DEBEZIUM_SIGNAL'"
                )).fetchone()
                result['signal_table_exists'] = (row[0] > 0) if row else False
            except Exception:
                pass

        engine.dispose()

    except Exception as e:
        result['error'] = str(e)
        logger.error(
            f"Oracle privilege check failed for {db_config.host}/{db_config.database_name}: {e}"
        )

    return result


def create_signal_table(db_config) -> tuple:
    """
    Create the DEBEZIUM_SIGNAL table in the connector user's schema if it doesn't exist.

    Returns:
        (created: bool, message: str)
        created=True  → table was just created
        created=False → table already existed or creation failed (check message)
    """
    if db_config.db_type.lower() != 'oracle':
        return False, f"Not an Oracle database (type={db_config.db_type})"

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_config.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            # Check if the table already exists
            row = conn.execute(text(
                "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'DEBEZIUM_SIGNAL'"
            )).fetchone()
            if row and row[0] > 0:
                engine.dispose()
                return False, "DEBEZIUM_SIGNAL table already exists"

            # Create the signal table
            conn.execute(text(
                "CREATE TABLE DEBEZIUM_SIGNAL ("
                "    ID   VARCHAR2(42)   NOT NULL PRIMARY KEY, "
                "    TYPE VARCHAR2(32)   NOT NULL, "
                "    DATA VARCHAR2(2048) NULL"
                ")"
            ))
            conn.commit()

        engine.dispose()
        logger.info(f"Created DEBEZIUM_SIGNAL table for {db_config.host}/{db_config.database_name}")
        return True, "DEBEZIUM_SIGNAL table created successfully"

    except Exception as e:
        logger.error(f"Failed to create DEBEZIUM_SIGNAL table: {e}")
        return False, str(e)


def apply_oracle_privileges(db_config, admin_user: str, admin_password: str) -> Dict[str, Any]:
    """
    Connect as SYSDBA and apply all missing Debezium LogMiner privileges to the connector user.

    What is automated:
      - Supplemental logging (ALTER DATABASE ADD SUPPLEMENTAL LOG DATA)
      - LOGMINING, SELECT ANY TABLE, SELECT ANY TRANSACTION, CREATE TABLE grants
      - SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE grants
      - Explicit V$DATABASE / V$LOGFILE / V$ARCHIVED_LOG / V$TRANSACTION grants
      - Creating the DEBEZIUM_SIGNAL table in the connector user's schema

    What is NOT automated (requires manual DBA action):
      - Enabling ARCHIVELOG mode (requires DB shutdown + restart)

    Returns:
        {
          applied: [str]   — actions successfully applied
          skipped: [str]   — already in place
          failed:  [str]   — actions that raised an error (with reason)
          error:   str     — non-empty only if the connection itself failed
        }
    """
    result: Dict[str, Any] = {'applied': [], 'skipped': [], 'failed': [], 'error': ''}

    if db_config.db_type.lower() != 'oracle':
        result['error'] = f"Not an Oracle database (type={db_config.db_type})"
        return result

    from urllib.parse import quote_plus

    host = db_config.host
    port = db_config.port
    service = db_config.database_name          # PDB service, e.g. XEPDB1
    connector_user = db_config.username.upper()

    try:
        import oracledb
        from sqlalchemy import create_engine, text

        admin_pass_enc = quote_plus(admin_password)
        admin_user_enc = quote_plus(admin_user)

        # Connect as SYSDBA to the same service (PDB-level SYSDBA works for grants)
        engine = create_engine(
            f"oracle+oracledb://{admin_user_enc}:{admin_pass_enc}@{host}:{port}/?service_name={service}",
            connect_args={"mode": oracledb.AUTH_MODE_SYSDBA},
            pool_pre_ping=True,
        )

        with engine.connect() as conn:

            # 1. Supplemental logging
            try:
                row = conn.execute(
                    text("SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")
                ).fetchone()
                if row and row[0].strip().upper() == 'YES':
                    result['skipped'].append("Supplemental logging already enabled")
                else:
                    conn.execute(text("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA"))
                    conn.commit()
                    result['applied'].append("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
            except Exception as e:
                result['failed'].append(f"Supplemental logging: {e}")

            # 2. User grants
            grants = [
                ("LOGMINING",              f"GRANT LOGMINING TO {connector_user}"),
                ("SELECT ANY TABLE",       f"GRANT SELECT ANY TABLE TO {connector_user}"),
                ("FLASHBACK ANY TABLE",    f"GRANT FLASHBACK ANY TABLE TO {connector_user}"),
                ("SELECT ANY TRANSACTION", f"GRANT SELECT ANY TRANSACTION TO {connector_user}"),
                ("CREATE TABLE",           f"GRANT CREATE TABLE TO {connector_user}"),
                ("SELECT_CATALOG_ROLE",    f"GRANT SELECT_CATALOG_ROLE TO {connector_user}"),
                ("EXECUTE_CATALOG_ROLE",   f"GRANT EXECUTE_CATALOG_ROLE TO {connector_user}"),
                # SET CONTAINER is required for CDB common users (C## prefix) so Debezium
                # can switch between PDB and CDB$ROOT for LogMiner.
                # For PDB-local users this grant will fail silently (ORA-01017/02225) and
                # that's fine — the connector template omits database.pdb.name for them.
                ("SET CONTAINER (CDB common users only)",
                 f"GRANT SET CONTAINER TO {connector_user} CONTAINER=ALL"),
            ]
            for label, sql in grants:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                    result['applied'].append(sql)
                except Exception as e:
                    err_str = str(e)
                    if 'ORA-01917' in err_str or 'already' in err_str.lower():
                        result['skipped'].append(f"{label} already granted")
                    else:
                        result['failed'].append(f"{label}: {e}")

            # 3. Explicit V$ view grants
            v_grants = [
                ("V$DATABASE",    f"GRANT SELECT ON SYS.V_$DATABASE TO {connector_user}"),
                ("V$LOGFILE",     f"GRANT SELECT ON SYS.V_$LOGFILE TO {connector_user}"),
                ("V$ARCHIVED_LOG",f"GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO {connector_user}"),
                ("V$TRANSACTION", f"GRANT SELECT ON SYS.V_$TRANSACTION TO {connector_user}"),
            ]
            for label, sql in v_grants:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                    result['applied'].append(sql)
                except Exception as e:
                    err_str = str(e)
                    if 'ORA-01917' in err_str or 'already' in err_str.lower():
                        result['skipped'].append(f"{label} grant already exists")
                    else:
                        result['failed'].append(f"{label}: {e}")

            # 4. DEBEZIUM_SIGNAL table (run as connector user, not admin)
            # We do this via a separate connection to the connector user
            try:
                from sqlalchemy import create_engine as _ce
                conn_user_enc = quote_plus(db_config.username)
                conn_pass_enc = quote_plus(db_config.get_decrypted_password())
                user_engine = _ce(
                    f"oracle+oracledb://{conn_user_enc}:{conn_pass_enc}@{host}:{port}/?service_name={service}",
                    pool_pre_ping=True,
                )
                with user_engine.connect() as uc:
                    row = uc.execute(text(
                        "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'DEBEZIUM_SIGNAL'"
                    )).fetchone()
                    if row and row[0] > 0:
                        result['skipped'].append("DEBEZIUM_SIGNAL table already exists")
                    else:
                        uc.execute(text(
                            "CREATE TABLE DEBEZIUM_SIGNAL ("
                            "    ID   VARCHAR2(42)   NOT NULL PRIMARY KEY, "
                            "    TYPE VARCHAR2(32)   NOT NULL, "
                            "    DATA VARCHAR2(2048) NULL"
                            ")"
                        ))
                        uc.commit()
                        result['applied'].append("CREATE TABLE DEBEZIUM_SIGNAL")
                user_engine.dispose()
            except Exception as e:
                result['failed'].append(f"DEBEZIUM_SIGNAL table: {e}")

        engine.dispose()
        logger.info(
            f"Oracle privilege apply for {host}/{service}: "
            f"{len(result['applied'])} applied, {len(result['skipped'])} skipped, "
            f"{len(result['failed'])} failed"
        )

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Oracle apply_privileges connection failed: {e}")

    return result