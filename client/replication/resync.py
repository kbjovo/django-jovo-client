"""
Sequential table resync (truncate target + incremental re-snapshot).

Why this exists
---------------
"Resync Selected" used to POST once per table; each request truncated its
target table immediately (inside the web request, with no lock timeout) and
sent its own snapshot signal. Debezium queues incremental snapshots, so all N
target tables sat empty while they were refilled one by one, the TRUNCATEs
piled up behind metadata locks, and nothing verified that a signal was
delivered or that a snapshot ever started.

Now every truncate+re-snapshot path enqueues into a ``TableResyncJob``. A short,
beat-driven Celery task (``client.tasks.advance_resync_jobs``) advances each
job by one step at a time:

    pending ─truncate─▶ truncated ─signal─▶ snapshotting ─▶ done
        │                   │                     │
        └──▶ failed ◀───────┴─────────────────────┘        (pending ─cancel─▶ skipped)

* ONE table is in flight per connector; the next one is not truncated until
  Debezium reports the previous table's snapshot finished.
* Nothing starts while the source or sink connector is paused, or while another
  snapshot (e.g. from add_tables) is running on the connector.
* TRUNCATE waits at most 30 s for its lock (retried on later ticks).
* A truncated table whose signal can't be delivered is retried and, if it still
  fails, surfaced loudly (the target is empty at that point).

The decision logic lives in :func:`step`, which works on plain dicts and an
injected ``ops`` object so it can be unit-tested without Kafka, Jolokia or a DB.
"""

import copy
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)

# Per-table entry states
PENDING = 'pending'
TRUNCATED = 'truncated'          # target truncated, snapshot signal not yet accepted
SNAPSHOTTING = 'snapshotting'
DONE = 'done'
FAILED = 'failed'
SKIPPED = 'skipped'

IN_FLIGHT_STATES = (TRUNCATED, SNAPSHOTTING)
ACTIVE_ENTRY_STATES = (PENDING, TRUNCATED, SNAPSHOTTING)

# Job status hints returned by step()
HINT_RUNNING = 'running'
HINT_QUEUED = 'queued'
HINT_WAITING_PAUSED = 'waiting_paused'
HINT_FINISHED = 'finished'

MAX_TRUNCATE_ATTEMPTS = 3        # lock timeouts retried on later ticks
MAX_SIGNAL_ATTEMPTS = 3          # delivery failures for one signal send
MAX_SIGNALS_PER_TABLE = 2        # original + one re-send if Debezium never starts
START_GRACE_SECONDS = 300        # time for Debezium to pick up a signal
STALL_GRACE_SECONDS = 1800       # no progress after the snapshot had started
START_CONFIRM_POLLS = 3          # quick Jolokia polls right after signalling
START_CONFIRM_INTERVAL = 2.0

LOCK_PREFIX = 'resync-lock:'
LOCK_TTL = 300


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def new_entry(table: str) -> Dict[str, Any]:
    return {
        'table': table,
        'state': PENDING,
        'truncate_attempts': 0,
        'signal_attempts': 0,
        'signals_sent': 0,
        'signal_id': None,
        'baseline': None,
        'rows': None,
        'truncated_at': None,
        'signaled_at': None,
        'started_at': None,
        'last_progress_at': None,
        'finished_at': None,
        'error': '',
    }


def match_table_key(rows_scanned: Dict[str, Any], table: str) -> Optional[str]:
    """Find *table* among Debezium's RowsScanned keys (``db.table``, ``schema.table`` …)."""
    t = table.lower()
    for key in rows_scanned or {}:
        k = str(key).lower().strip('"`[]')
        if k == t or k.endswith('.' + t):
            return key
    return None


def summarize_metrics(mbeans: List[Dict[str, Any]], table: str) -> Dict[str, Any]:
    """
    Reduce the normalised snapshot MBeans (see JolokiaClient._normalize_snapshot)
    to what the state machine needs for *table*.

    ``fingerprint`` captures the MBeans' state for this table so a completed
    snapshot from an earlier run (e.g. the initial snapshot, which stays
    "completed" with every table in RowsScanned) is not mistaken for ours.
    """
    running_any = any(p.get('running') for p in mbeans)
    completed_ours = False
    aborted_ours = False
    rows = None
    fingerprint = []
    for p in mbeans:
        rs = p.get('rows_scanned') or {}
        key = match_table_key(rs, table)
        has_table = key is not None
        table_rows = rs.get(key) if has_table else None
        if has_table and table_rows is not None:
            rows = table_rows if rows is None else max(rows, table_rows)
        if has_table and not p.get('running'):
            if p.get('completed'):
                completed_ours = True
            if p.get('aborted'):
                aborted_ours = True
        fingerprint.append([
            p.get('type'), bool(p.get('completed')), bool(p.get('aborted')),
            table_rows, p.get('duration_seconds'),
        ])
    fingerprint.sort(key=lambda x: str(x))
    return {
        'running_any': running_any,
        'completed_ours': completed_ours,
        'aborted_ours': aborted_ours,
        'rows': rows,
        'fingerprint': fingerprint,
    }


# ---------------------------------------------------------------------------
# Pure state machine
# ---------------------------------------------------------------------------

@dataclass
class StepResult:
    index: Optional[int]                 # entry that changed (None = no entry change)
    entry: Optional[Dict[str, Any]]      # the changed entry
    hint: str                            # running / queued / waiting_paused / finished
    message: str
    progressed: bool = False             # an entry reached a terminal state → tick again soon


def _first(entries: List[Dict[str, Any]], states: Iterable[str]) -> Optional[int]:
    states = tuple(states)
    for i, e in enumerate(entries):
        if e.get('state') in states:
            return i
    return None


def step(entries: List[Dict[str, Any]], ops) -> StepResult:
    """
    Advance the job by one step. Never mutates *entries*; returns at most one
    changed entry. ``ops`` provides the side effects:

        ops.now() -> datetime
        ops.pipeline_paused() -> Optional[str]          reason if source/sink paused
        ops.observe(table) -> Optional[dict]             summarize_metrics() or None if unavailable
        ops.truncate(table) -> (ok, kind, message)
        ops.signal(table) -> signal_id                   raises on failure
        ops.wait_for_start(table) -> Optional[dict]      observe() after a short wait, or None
    """
    # 1) A table already in flight always comes first.
    idx = _first(entries, IN_FLIGHT_STATES)
    if idx is not None:
        e = copy.deepcopy(entries[idx])
        if e['state'] == TRUNCATED:
            return _send_signal(idx, e, ops)
        return _check_snapshot(idx, e, ops)

    # 2) Otherwise start the next pending table.
    idx = _first(entries, (PENDING,))
    if idx is None:
        return StepResult(None, None, HINT_FINISHED, 'All tables processed')

    table = entries[idx]['table']
    paused = ops.pipeline_paused()
    if paused:
        return StepResult(None, None, HINT_WAITING_PAUSED, paused)

    obs = ops.observe(table)
    if obs is None:
        return StepResult(None, None, HINT_QUEUED,
                          'Waiting for connector snapshot metrics (Jolokia) to become available')
    if obs['running_any']:
        return StepResult(None, None, HINT_QUEUED,
                          'Waiting for another snapshot on this connector to finish')

    e = copy.deepcopy(entries[idx])
    now = ops.now()
    e['baseline'] = obs['fingerprint']
    e['truncate_attempts'] = int(e.get('truncate_attempts') or 0) + 1
    ok, kind, msg = ops.truncate(table)
    if not ok:
        if kind == 'lock_timeout' and e['truncate_attempts'] < MAX_TRUNCATE_ATTEMPTS:
            e['error'] = msg
            return StepResult(idx, e, HINT_RUNNING,
                              f"Target table for '{table}' is busy — truncate will be retried ({msg})")
        e.update(state=FAILED, error=f'Truncate failed: {msg}', finished_at=_iso(now))
        return StepResult(idx, e, HINT_RUNNING, f"'{table}': truncate failed — {msg}", progressed=True)

    e.update(state=TRUNCATED, truncated_at=_iso(now), error='')
    return _send_signal(idx, e, ops)


def _send_signal(idx: int, e: Dict[str, Any], ops) -> StepResult:
    table = e['table']
    now = ops.now()
    try:
        signal_id = ops.signal(table)
    except Exception as ex:
        e['signal_attempts'] = int(e.get('signal_attempts') or 0) + 1
        if e['signal_attempts'] >= MAX_SIGNAL_ATTEMPTS:
            e.update(
                state=FAILED,
                finished_at=_iso(now),
                error=(f'Target table was truncated but the snapshot signal could not be delivered '
                       f'({ex}). The target table is EMPTY — resync it again once Kafka is healthy.'),
            )
            return StepResult(idx, e, HINT_RUNNING, f"'{table}': snapshot signal failed", progressed=True)
        e['error'] = f'Snapshot signal failed, will retry: {ex}'
        return StepResult(idx, e, HINT_RUNNING, f"'{table}': {e['error']}")

    e.update(
        state=SNAPSHOTTING,
        signal_id=signal_id,
        signaled_at=_iso(now),
        signals_sent=int(e.get('signals_sent') or 0) + 1,
        signal_attempts=0,
        error='',
    )
    # Catch the start of fast snapshots that may finish before the next tick.
    started = ops.wait_for_start(table)
    if started and started.get('running_any'):
        e['started_at'] = e.get('started_at') or _iso(ops.now())
        e['last_progress_at'] = _iso(ops.now())
        if started.get('rows') is not None:
            e['rows'] = started['rows']
    return StepResult(idx, e, HINT_RUNNING, f"Snapshotting '{table}'")


def _check_snapshot(idx: int, e: Dict[str, Any], ops) -> StepResult:
    table = e['table']
    now = ops.now()
    obs = ops.observe(table)
    if obs is None:
        return StepResult(None, None, HINT_RUNNING,
                          f"Snapshotting '{table}' — snapshot metrics (Jolokia) unavailable, waiting")

    if obs['running_any']:
        e['started_at'] = e.get('started_at') or _iso(now)
        e['last_progress_at'] = _iso(now)
        if obs.get('rows') is not None:
            e['rows'] = obs['rows']
        rows = f" — {e['rows']:,} rows read" if isinstance(e.get('rows'), int) else ''
        return StepResult(idx, e, HINT_RUNNING, f"Snapshotting '{table}'{rows}")

    evidence = bool(e.get('started_at')) or obs['fingerprint'] != e.get('baseline')
    if evidence and obs['completed_ours']:
        if obs.get('rows') is not None:
            e['rows'] = obs['rows']
        e.update(state=DONE, finished_at=_iso(now), error='')
        return StepResult(idx, e, HINT_RUNNING, f"'{table}' resynced", progressed=True)
    if evidence and obs['aborted_ours']:
        e.update(state=FAILED, finished_at=_iso(now),
                 error='Debezium aborted the snapshot — the target table may be incomplete; resync it again.')
        return StepResult(idx, e, HINT_RUNNING, f"'{table}': snapshot aborted", progressed=True)

    # Not running and no completion yet: either Debezium hasn't picked the signal up,
    # or the connector restarted mid-snapshot (it resumes from its offsets).
    since = _parse(e.get('last_progress_at')) or _parse(e.get('signaled_at')) or now
    grace = STALL_GRACE_SECONDS if e.get('started_at') else START_GRACE_SECONDS
    if (now - since).total_seconds() < grace:
        waiting = 'resume' if e.get('started_at') else 'start'
        return StepResult(None, None, HINT_RUNNING,
                          f"Waiting for Debezium to {waiting} the snapshot of '{table}'")

    if int(e.get('signals_sent') or 0) < MAX_SIGNALS_PER_TABLE:
        logger.warning(f"Resync '{table}': no snapshot activity after {grace}s — re-sending the signal")
        e['state'] = TRUNCATED
        return _send_signal(idx, e, ops)

    e.update(
        state=FAILED, finished_at=_iso(now),
        error=('Debezium never reported a snapshot for this table (signal not received?). '
               'The target table may be empty or incomplete — check row counts and resync again.'),
    )
    return StepResult(idx, e, HINT_RUNNING, f"'{table}': snapshot never started", progressed=True)


# ---------------------------------------------------------------------------
# Live side effects
# ---------------------------------------------------------------------------

class LiveOps:
    """Real implementations of the side effects used by :func:`step`."""

    def __init__(self, config):
        from jovoclient.utils.debezium.jolokia_client import JolokiaClient
        self.config = config
        self.jolokia = JolokiaClient()

    def now(self) -> datetime:
        return timezone.now()

    def pipeline_paused(self) -> Optional[str]:
        from client.models.replication import ReplicationConfig
        from client.utils.connector_status_cache import get_status

        states = ReplicationConfig.objects.filter(pk=self.config.pk).values(
            'connector_state', 'sink_connector_state'
        ).first() or {}
        if states.get('connector_state') == 'PAUSED':
            return 'Waiting: source connector is paused'
        if states.get('sink_connector_state') == 'PAUSED':
            return 'Waiting: sink connector is paused'

        # Also honour the live state (e.g. batch windows pause the sink).
        for name, label in ((self.config.connector_name, 'source'), (self.config.sink_connector_name, 'sink')):
            if not name:
                continue
            data = get_status(name)
            if data and data.get('connector', {}).get('state') == 'PAUSED':
                return f'Waiting: {label} connector is paused'
        return None

    def _servers(self) -> List[str]:
        db = self.config.client_database
        servers = []
        if db.db_type == 'mssql' and self.config.connector_version:
            servers.append(f"sqlserver_{db.client.id}_{db.id}_v_{self.config.connector_version}")
        servers.append(self.config.kafka_topic_prefix)
        return servers

    def _read_mbeans(self) -> Optional[List[Dict[str, Any]]]:
        """
        Normalised snapshot MBeans for the connector, or None when Jolokia /
        the connector's metrics are unavailable (we must not truncate blind).
        """
        db_type = self.config.client_database.db_type
        for server in self._servers():
            found = []
            inc = self.jolokia.get_incremental_snapshot_progress(db_type, server)
            if inc is not None:
                found.append(inc)
            snap = self.jolokia.get_snapshot_progress(db_type, server)
            if snap is not None:
                found.append(snap)
            if found:
                return found
            # No snapshot MBean but streaming is registered → metrics are reachable
            # and simply nothing has been snapshotted since the connector started.
            if self.jolokia.get_streaming_metrics(db_type, server) is not None:
                return []
        return None

    def observe(self, table: str) -> Optional[Dict[str, Any]]:
        mbeans = self._read_mbeans()
        if mbeans is None:
            return None
        return summarize_metrics(mbeans, table)

    def truncate(self, table: str) -> Tuple[bool, Optional[str], str]:
        from client.utils.table_creator import truncate_target_table

        mapping = self.config.table_mappings.filter(source_table=table, is_enabled=True).first()
        if not mapping:
            return False, 'error', f"No enabled table mapping for '{table}'"
        target_db = self.config.client_database.client.client_databases.filter(is_target=True).first()
        if not target_db:
            return False, 'error', 'No target database configured'
        return truncate_target_table(target_db, mapping)

    def signal(self, table: str) -> str:
        from jovoclient.utils.kafka.signal import send_incremental_snapshot_signal
        signal_id, method = send_incremental_snapshot_signal(
            self.config.client_database, self.config, [table]
        )
        logger.info(f"[{self.config.connector_name}] Resync '{table}': snapshot signal {signal_id} via {method}")
        return signal_id

    def wait_for_start(self, table: str) -> Optional[Dict[str, Any]]:
        obs = None
        for _ in range(START_CONFIRM_POLLS):
            time.sleep(START_CONFIRM_INTERVAL)
            obs = self.observe(table)
            if obs and obs.get('running_any'):
                return obs
        return obs


# ---------------------------------------------------------------------------
# Job persistence / orchestration
# ---------------------------------------------------------------------------

def _job_model():
    from client.models.replication import TableResyncJob
    return TableResyncJob


def has_active_job(config_pk: int) -> bool:
    TableResyncJob = _job_model()
    return TableResyncJob.objects.filter(
        replication_config_id=config_pk, status__in=TableResyncJob.ACTIVE_STATUSES
    ).exists()


def _kick(job_id: int, countdown: int = 0) -> None:
    try:
        from client.tasks import advance_resync_jobs
        advance_resync_jobs.apply_async(kwargs={'job_id': job_id}, countdown=countdown,
                                        expires=countdown + 60)
    except Exception as e:
        # Beat will pick it up within ~20 s anyway.
        logger.warning(f"Could not enqueue resync tick for job {job_id}: {e}")


def plan_entries(requested: Iterable[str], enabled: Iterable[str], queued: Iterable[str]):
    """
    Decide which requested tables become new job entries.

    Returns (entries, added, skipped) — duplicates within the request, tables
    without an enabled mapping and tables already queued / in flight are skipped.
    """
    enabled = set(enabled)
    queued = set(queued)
    entries: List[Dict[str, Any]] = []
    added: List[str] = []
    skipped: List[Tuple[str, str]] = []
    for t in dict.fromkeys(x.strip() for x in requested if x and x.strip()):
        if t not in enabled:
            skipped.append((t, 'no enabled mapping'))
        elif t in queued:
            skipped.append((t, 'already queued'))
        else:
            entries.append(new_entry(t))
            queued.add(t)
            added.append(t)
    return entries, added, skipped


def cancel_entries(tables: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, bool]:
    """Mark pending entries skipped. Returns (tables, skipped_count, still_in_flight)."""
    tables = copy.deepcopy(tables)
    skipped = 0
    for e in tables:
        if e.get('state') == PENDING:
            e['state'] = SKIPPED
            e['error'] = 'Cancelled'
            skipped += 1
    in_flight = any(e.get('state') in IN_FLIGHT_STATES for e in tables)
    return tables, skipped, in_flight


def enqueue_resync(config, tables: Iterable[str], source: str = 'manual'):
    """
    Queue *tables* of *config* for a sequential resync.

    Appends to the connector's active job when there is one (skipping tables
    already queued or in flight); otherwise creates a new job.

    Returns (job, added_tables, skipped) where skipped is a list of
    ``(table, reason)``.
    """
    TableResyncJob = _job_model()
    from client.models.replication import ReplicationConfig

    requested = list(tables)
    enabled = set(config.table_mappings.filter(is_enabled=True).values_list('source_table', flat=True))

    with transaction.atomic():
        # Serialise enqueues for this connector.
        ReplicationConfig.objects.select_for_update().filter(pk=config.pk).first()
        job = (TableResyncJob.objects.select_for_update()
               .filter(replication_config=config, status__in=TableResyncJob.ACTIVE_STATUSES)
               .order_by('created_at').first())

        queued = set()
        if job:
            queued = {e['table'] for e in job.tables if e.get('state') in ACTIVE_ENTRY_STATES}

        entries, added, skipped = plan_entries(requested, enabled, queued)

        if entries:
            if job:
                job.tables = list(job.tables) + entries
                job.save(update_fields=['tables', 'updated_at'])
            else:
                job = TableResyncJob.objects.create(
                    replication_config=config,
                    source=source,
                    tables=entries,
                    status=TableResyncJob.STATUS_QUEUED,
                    status_message='Queued',
                )

    if job and added:
        logger.info(f"[{config.connector_name}] Resync job #{job.pk}: queued {added} (source={source})")
        job_id = job.pk
        transaction.on_commit(lambda: _kick(job_id))
    return job, added, skipped


def cancel_job(job_id: int) -> Tuple[bool, str]:
    """Skip every table that has not started yet. The table in flight finishes."""
    TableResyncJob = _job_model()
    with transaction.atomic():
        job = TableResyncJob.objects.select_for_update().filter(pk=job_id).first()
        if not job:
            return False, 'Resync job not found'
        if not job.is_active:
            return False, f'Resync job is already {job.status}'
        tables, skipped, in_flight = cancel_entries(job.tables)
        job.tables = tables
        job.cancelled_at = timezone.now()
        if not in_flight:
            job.status = TableResyncJob.STATUS_CANCELLED
            job.finished_at = timezone.now()
            job.status_message = 'Cancelled'
        else:
            job.status_message = 'Cancelling — the table currently being resynced will finish first'
        job.save()
    return True, f'Cancelled {skipped} pending table(s)'


def _commit(job_id: int, result: StepResult) -> Tuple[bool, Optional[int]]:
    """
    Merge a step result into the freshly locked job row (tables may have been
    appended or cancelled since we read it). Returns (finished, config_pk).
    """
    TableResyncJob = _job_model()
    with transaction.atomic():
        job = TableResyncJob.objects.select_for_update().filter(pk=job_id).first()
        if not job:
            return False, None
        tables = list(job.tables)
        if (result.index is not None and result.entry is not None
                and result.index < len(tables)
                and tables[result.index].get('table') == result.entry.get('table')):
            tables[result.index] = result.entry
        job.tables = tables
        if result.message:
            job.status_message = result.message[:500]

        finished = False
        if any(e.get('state') in ACTIVE_ENTRY_STATES for e in tables):
            if result.hint in (HINT_RUNNING, HINT_QUEUED, HINT_WAITING_PAUSED):
                job.status = result.hint
        else:
            if job.cancelled_at:
                job.status = TableResyncJob.STATUS_CANCELLED
            elif any(e.get('state') == FAILED for e in tables):
                job.status = TableResyncJob.STATUS_FAILED
            else:
                job.status = TableResyncJob.STATUS_COMPLETED
            job.finished_at = timezone.now()
            done = sum(1 for e in tables if e.get('state') == DONE)
            failed = sum(1 for e in tables if e.get('state') == FAILED)
            job.status_message = f'Finished: {done} resynced, {failed} failed'
            finished = True
        job.save()
        return finished, job.replication_config_id


def advance_job(job_id: int, ops_factory=LiveOps) -> None:
    """Run one step of a job under a per-connector lock."""
    TableResyncJob = _job_model()
    job = (TableResyncJob.objects
           .select_related('replication_config', 'replication_config__client_database',
                           'replication_config__client_database__client')
           .filter(pk=job_id).first())
    if not job or not job.is_active:
        return

    config = job.replication_config
    lock_key = f'{LOCK_PREFIX}{config.pk}'
    try:
        if not cache.add(lock_key, job_id, LOCK_TTL):
            return  # another tick is already working on this connector
    except Exception as e:
        logger.warning(f"Resync lock unavailable (cache down?) — skipping tick: {e}")
        return

    try:
        # Only the oldest active job of a connector runs; later ones wait their turn.
        oldest = (TableResyncJob.objects
                  .filter(replication_config=config, status__in=TableResyncJob.ACTIVE_STATUSES)
                  .order_by('created_at').values_list('pk', flat=True).first())
        if oldest != job.pk:
            return

        result = step(copy.deepcopy(job.tables), ops_factory(config))
        finished, config_pk = _commit(job.pk, result)

        if finished and config_pk:
            logger.info(f"[{config.connector_name}] Resync job #{job.pk} finished")
            from client.utils import row_counts
            row_counts.request_refresh(config_pk, force=True)
        elif result.progressed:
            _kick(job.pk, countdown=2)
    except Exception as e:
        logger.error(f"[{config.connector_name}] Resync job #{job_id} tick failed: {e}", exc_info=True)
        TableResyncJob.objects.filter(pk=job_id).update(status_message=f'Tick error (will retry): {e}'[:500])
    finally:
        try:
            cache.delete(lock_key)
        except Exception:
            pass


def advance_active_jobs() -> int:
    """Advance the oldest active job of every connector. Returns jobs touched."""
    TableResyncJob = _job_model()
    seen = set()
    count = 0
    for job_id, config_id in (TableResyncJob.objects
                              .filter(status__in=TableResyncJob.ACTIVE_STATUSES)
                              .order_by('created_at')
                              .values_list('pk', 'replication_config_id')):
        if config_id in seen:
            continue
        seen.add(config_id)
        advance_job(job_id)
        count += 1
    return count


def job_summary(config_pk: int, recent_seconds: int = 600) -> Optional[Dict[str, Any]]:
    """
    Compact view of the connector's current (or just-finished) resync job for
    the monitor page. Returns None when there is nothing to show.
    """
    TableResyncJob = _job_model()
    job = (TableResyncJob.objects
           .filter(replication_config_id=config_pk, status__in=TableResyncJob.ACTIVE_STATUSES)
           .order_by('created_at').first())
    if not job:
        job = (TableResyncJob.objects.filter(replication_config_id=config_pk)
               .order_by('-created_at').first())
        if not job or not job.finished_at:
            return None
        if (timezone.now() - job.finished_at).total_seconds() > recent_seconds:
            return None

    tables = [{
        'table': e.get('table'),
        'state': e.get('state'),
        'rows': e.get('rows'),
        'error': e.get('error') or '',
    } for e in job.tables or []]
    counts: Dict[str, int] = {}
    for t in tables:
        counts[t['state']] = counts.get(t['state'], 0) + 1
    return {
        'id': job.pk,
        'status': job.status,
        'active': job.is_active,
        'source': job.source,
        'message': job.status_message,
        'tables': tables,
        'counts': counts,
        'created_at': job.created_at.isoformat() if job.created_at else None,
        'finished_at': job.finished_at.isoformat() if job.finished_at else None,
    }
