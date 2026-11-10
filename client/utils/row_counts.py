"""
Background, cached row counts for connector monitoring.

Exact ``COUNT(*)`` on large InnoDB tables is a full index scan. The monitoring
dashboard used to run one per enabled table, on BOTH source and target, every
15 s per card per open tab, each on a brand-new connection, with no timeout and
no cache. During a multi-table resync that saturated the production source, the
target MySQL and every gunicorn worker.

Counts are now computed by the ``client.tasks.refresh_row_counts`` Celery task:

* at most one sweep per connector at a time (cache lock),
* one connection per database, in AUTOCOMMIT so each COUNT releases its
  metadata lock immediately (a long-held MDL blocks TRUNCATE and the sink),
* a per-statement timeout (MySQL ``max_execution_time`` / PG ``statement_timeout``),
* skipped automatically while an incremental snapshot or resync job is running.

Views only ever read the cached result and *request* a refresh.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from django.core.cache import cache
from django.utils import timezone

logger = logging.getLogger(__name__)

CACHE_PREFIX = 'rowcounts:'
LOCK_PREFIX = 'rowcounts-lock:'
ENQUEUED_PREFIX = 'rowcounts-enq:'

RESULT_TTL = 24 * 3600          # keep the last result around; staleness is judged by as_of
LOCK_TTL = 600                  # safety net if a worker dies mid-sweep
ENQUEUE_DEDUPE_TTL = 120        # one queued refresh per connector at a time
STATEMENT_TIMEOUT_MS = 20000    # per COUNT(*) statement
CONNECT_TIMEOUT_S = 5

# How old a cached result may be before the dashboard asks for a new one.
DASHBOARD_MAX_AGE_S = 300

STATUS_OK = 'ok'
STATUS_SNAPSHOT = 'snapshot_in_progress'


# ---------------------------------------------------------------------------
# Cache helpers (a Redis outage must never break a page — degrade to "no data")
# ---------------------------------------------------------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except Exception as e:
        logger.warning(f"Row-count cache backend unavailable: {e}")
        return default


def get_cached(config_pk: int) -> Optional[Dict[str, Any]]:
    return _safe(lambda: cache.get(f'{CACHE_PREFIX}{config_pk}'))


def set_cached(config_pk: int, payload: Dict[str, Any]) -> None:
    _safe(lambda: cache.set(f'{CACHE_PREFIX}{config_pk}', payload, RESULT_TTL))


def acquire_lock(config_pk: int) -> bool:
    return bool(_safe(lambda: cache.add(f'{LOCK_PREFIX}{config_pk}', 1, LOCK_TTL), False))


def release_lock(config_pk: int) -> None:
    _safe(lambda: cache.delete_many([f'{LOCK_PREFIX}{config_pk}', f'{ENQUEUED_PREFIX}{config_pk}']))


def is_refresh_pending(config_pk: int) -> bool:
    found = _safe(
        lambda: cache.get_many([f'{LOCK_PREFIX}{config_pk}', f'{ENQUEUED_PREFIX}{config_pk}']),
        {},
    )
    return bool(found)


def age_seconds(payload: Optional[Dict[str, Any]]) -> Optional[float]:
    """Seconds since the cached payload was produced (None if unknown)."""
    if not payload or not payload.get('as_of'):
        return None
    try:
        from datetime import datetime
        as_of = datetime.fromisoformat(payload['as_of'])
        return (timezone.now() - as_of).total_seconds()
    except Exception:
        return None


def request_refresh(config_pk: int, force: bool = False) -> bool:
    """
    Enqueue a background refresh unless one is already queued or running.

    Returns True if a task was enqueued.
    """
    if not _safe(lambda: cache.add(f'{ENQUEUED_PREFIX}{config_pk}', 1, ENQUEUE_DEDUPE_TTL), False):
        return False
    try:
        from client.tasks import refresh_row_counts
        # expires: if no worker picks it up soon (worker down/backlogged), drop it rather
        # than letting dashboards queue an ever-growing pile of stale recounts.
        refresh_row_counts.apply_async(args=[config_pk], kwargs={'force': force},
                                       expires=ENQUEUE_DEDUPE_TTL * 2)
        return True
    except Exception as e:
        logger.warning(f"Could not enqueue row-count refresh for config {config_pk}: {e}")
        _safe(lambda: cache.delete(f'{ENQUEUED_PREFIX}{config_pk}'))
        return False


# ---------------------------------------------------------------------------
# Query building
# ---------------------------------------------------------------------------

def count_query(db_config, table_name: str, schema: Optional[str] = None) -> str:
    """Build a COUNT(*) statement for *table_name* in the dialect of *db_config*."""
    db_type = db_config.db_type.lower()
    if db_type == 'mssql':
        if '.' in table_name:
            schema_name, tbl = table_name.split('.', 1)
        else:
            schema_name, tbl = (schema or 'dbo'), table_name
        return f"SELECT COUNT(*) FROM [{schema_name}].[{tbl}]"
    if db_type == 'postgresql':
        if '.' in table_name:
            schema_name, tbl = table_name.rsplit('.', 1)
        else:
            schema_name, tbl = (schema or 'public'), table_name
        return f'SELECT COUNT(*) FROM "{schema_name}"."{tbl}"'
    if db_type == 'oracle':
        if '.' in table_name:
            schema_name, tbl = table_name.rsplit('.', 1)
            schema_name, tbl = schema_name.upper(), tbl.upper()
        else:
            schema_name = (schema or db_config.username).upper()
            tbl = table_name.upper()
        return f"SELECT COUNT(*) FROM {schema_name}.{tbl}"
    return f"SELECT COUNT(*) FROM `{table_name}`"


def is_not_found_error(err_str: str) -> bool:
    err_str = err_str.lower()
    keywords = ("doesn't exist", 'not exist', 'not found', 'unknown table', 'invalid object', 'ora-00942')
    return any(k in err_str for k in keywords)


def _prepare_session(conn, db_type: str):
    """
    AUTOCOMMIT so each COUNT(*) releases its shared metadata lock as soon as it
    finishes (inside one long transaction, MDL is held until commit and blocks
    TRUNCATE / ALTER — and everything that queues behind them). Then cap each
    statement so a huge table can't pin a connection for minutes.
    """
    from sqlalchemy import text as sa_text

    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    try:
        if db_type == 'mysql':
            conn.execute(sa_text(f"SET SESSION max_execution_time = {STATEMENT_TIMEOUT_MS}"))
        elif db_type == 'postgresql':
            conn.execute(sa_text(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}"))
        elif db_type == 'mssql':
            # No per-statement timeout in T-SQL; at least never wait on locks forever.
            conn.execute(sa_text(f"SET LOCK_TIMEOUT {STATEMENT_TIMEOUT_MS}"))
    except Exception as e:
        logger.debug(f"Could not set statement timeout ({db_type}): {e}")
    return conn


def _count_all(db_config, items: List[Tuple[Any, str, Optional[str]]]) -> Tuple[bool, Dict[Any, Tuple[Optional[int], Optional[str]]]]:
    """
    Count every (key, table, schema) in *items* over a single connection.

    Returns (reachable, {key: (count_or_None, error_code_or_None)}).
    """
    from sqlalchemy import text as sa_text
    from client.utils.database_utils import get_database_connection

    db_type = db_config.db_type.lower()
    results: Dict[Any, Tuple[Optional[int], Optional[str]]] = {}
    try:
        with get_database_connection(db_config, connect_timeout=CONNECT_TIMEOUT_S) as conn:
            conn = _prepare_session(conn, db_type)
            for key, table, schema in items:
                try:
                    row = conn.execute(sa_text(count_query(db_config, table, schema=schema))).fetchone()
                    results[key] = (row[0] if row else 0, None)
                except Exception as e:
                    if is_not_found_error(str(e)):
                        results[key] = (None, 'table_not_found')
                    else:
                        results[key] = (None, 'error')
                        logger.warning(f'Row count failed for {table} on {db_config.connection_name}: {e}')
    except Exception as e:
        logger.warning(f'Database unreachable for row counts ({db_config.connection_name}): {e}')
        return False, {}
    return True, results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_row_counts(config) -> Dict[str, Any]:
    """
    Run the actual counts for every enabled mapping of *config*.

    Returns the payload stored in the cache and served by the monitor views.
    """
    source_db = config.client_database
    target_db = source_db.client.get_target_database()
    mappings = list(config.table_mappings.filter(is_enabled=True).order_by('source_table'))

    source_reachable, source_counts = _count_all(
        source_db, [(m.pk, m.source_table, m.source_schema) for m in mappings]
    )

    target_reachable = False
    target_counts: Dict[Any, Tuple[Optional[int], Optional[str]]] = {}
    if target_db:
        target_reachable, target_counts = _count_all(
            target_db, [(m.pk, m.target_table, m.target_schema or None) for m in mappings]
        )

    rows = []
    for m in mappings:
        if not source_reachable:
            src_count, src_error = None, 'db_unreachable'
        else:
            src_count, src_error = source_counts.get(m.pk, (None, 'error'))

        if not target_db:
            tgt_count, tgt_error = None, 'no_target_db'
        elif not target_reachable:
            tgt_count, tgt_error = None, 'db_unreachable'
        else:
            tgt_count, tgt_error = target_counts.get(m.pk, (None, 'error'))

        rows.append({
            'source_table': m.source_table,
            'target_table': m.target_table,
            'source_rows': src_count,
            'target_rows': tgt_count,
            'source_error': src_error,
            'target_error': tgt_error,
        })

    return {
        'status': STATUS_OK,
        'as_of': timezone.now().isoformat(),
        'tables': rows,
        'source_reachable': source_reachable,
        'target_reachable': target_reachable,
        'has_target_db': target_db is not None,
    }


def summarize_sync(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Reduce a cached payload to the dashboard card's sync summary.

    Returns None when there is no usable payload (caller shows "checking").
    """
    if not payload:
        return None
    if payload.get('status') == STATUS_SNAPSHOT:
        return {'status': STATUS_SNAPSHOT}
    if not payload.get('source_reachable') or not payload.get('has_target_db') or not payload.get('target_reachable'):
        return {'status': 'unable_to_check'}

    tables = payload.get('tables') or []
    total = len(tables)
    if total == 0:
        return {'status': 'unable_to_check'}

    in_sync = sum(
        1 for t in tables
        if t.get('source_rows') is not None and t.get('source_rows') == t.get('target_rows')
    )
    return {
        'status': 'rows_in_sync' if in_sync == total else 'not_synced',
        'in_sync_count': in_sync,
        'total_count': total,
        'synced': in_sync,
        'not_synced': total - in_sync,
        'as_of': payload.get('as_of'),
    }


def snapshot_busy(config) -> bool:
    """
    True while an incremental snapshot or a resync job is running for *config*.

    Counting tables that are being bulk-refilled is both meaningless (the numbers
    change every second) and expensive, so automatic refreshes skip it.
    """
    try:
        from client.replication.resync import has_active_job
        if has_active_job(config.pk):
            return True
    except Exception as e:
        logger.debug(f"Resync job check failed for config {config.pk}: {e}")

    try:
        from client.replication.orchestrator import ReplicationOrchestrator
        progress = ReplicationOrchestrator(config)._get_snapshot_progress()
        return bool(progress and progress.get('running'))
    except Exception as e:
        logger.debug(f"Snapshot check failed for config {config.pk}: {e}")
        return False
