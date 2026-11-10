"""
Unit tests for the production-incident fixes around multi-table resync:

* the sequential resync state machine (client/replication/resync.py),
* cached row-count summaries (client/utils/row_counts.py),
* health-monitor auto-restart budget,
* "Kafka Connect unreachable" vs "connector not found",
* bounded TRUNCATE lock-timeout detection,
* Kafka signal delivery verification.

Pure logic only (fakes, no Kafka / Jolokia / database).

Run: python manage.py test client.test_resync_job
"""

from datetime import datetime, timedelta, timezone as dt_tz
from unittest import mock

from django.test import SimpleTestCase, override_settings

from client.replication import resync
from client.replication.resync import (
    DONE, FAILED, PENDING, SKIPPED, SNAPSHOTTING, TRUNCATED,
    HINT_FINISHED, HINT_QUEUED, HINT_RUNNING, HINT_WAITING_PAUSED,
    MAX_TRUNCATE_ATTEMPTS, MAX_SIGNAL_ATTEMPTS, START_GRACE_SECONDS,
    cancel_entries, match_table_key, new_entry, plan_entries, step, summarize_metrics,
)


T0 = datetime(2026, 11, 10, 12, 0, 0, tzinfo=dt_tz.utc)


def mbean(running=False, completed=False, aborted=False, rows=None, duration=0, type_='incremental'):
    """A normalised snapshot MBean as returned by JolokiaClient._normalize_snapshot."""
    return {
        'type': type_,
        'running': running,
        'completed': completed,
        'aborted': aborted,
        'rows_scanned': rows or {},
        'duration_seconds': duration,
    }


class FakeOps:
    """Scriptable side effects for resync.step()."""

    def __init__(self, mbeans=(), paused=None, truncate_result=(True, None, 'ok'),
                 signal_error=None, start_mbeans=None):
        self.t = T0
        self.mbeans = list(mbeans) if mbeans is not None else None   # None → metrics unavailable
        self.paused = paused
        self.truncate_result = truncate_result
        self.signal_error = signal_error
        self.start_mbeans = start_mbeans
        self.truncated = []
        self.signalled = []

    def now(self):
        return self.t

    def pipeline_paused(self):
        return self.paused

    def observe(self, table):
        if self.mbeans is None:
            return None
        return summarize_metrics(self.mbeans, table)

    def truncate(self, table):
        self.truncated.append(table)
        return self.truncate_result

    def signal(self, table):
        if self.signal_error:
            raise RuntimeError(self.signal_error)
        self.signalled.append(table)
        return f'sig-{len(self.signalled)}'

    def wait_for_start(self, table):
        if self.start_mbeans is None:
            return None
        return summarize_metrics(self.start_mbeans, table)


def entries(*specs):
    out = []
    for table, state in specs:
        e = new_entry(table)
        e['state'] = state
        out.append(e)
    return out


def apply(entries_, result):
    """Apply a StepResult the way _commit does."""
    out = list(entries_)
    if result.index is not None and result.entry is not None:
        out[result.index] = result.entry
    return out


# --------------------------------------------------------------------------- #
# State machine
# --------------------------------------------------------------------------- #
class SequentialResyncTests(SimpleTestCase):

    def test_only_first_pending_table_is_truncated_and_signalled(self):
        ops = FakeOps()
        es = entries(('orders', PENDING), ('customers', PENDING))
        r = step(es, ops)
        self.assertEqual(ops.truncated, ['orders'])
        self.assertEqual(ops.signalled, ['orders'])
        self.assertEqual(r.entry['state'], SNAPSHOTTING)
        self.assertEqual(r.index, 0)
        self.assertEqual(es[1]['state'], PENDING)  # input never mutated

    def test_next_table_waits_while_one_is_in_flight(self):
        es = entries(('orders', SNAPSHOTTING), ('customers', PENDING))
        es[0]['started_at'] = T0.isoformat()
        ops = FakeOps(mbeans=[mbean(running=True, rows={'db.orders': 500})])
        r = step(es, ops)
        self.assertEqual(ops.truncated, [])
        self.assertEqual(r.entry['state'], SNAPSHOTTING)
        self.assertEqual(r.entry['rows'], 500)
        self.assertEqual(r.hint, HINT_RUNNING)

    def test_completion_then_next_table_starts(self):
        es = entries(('orders', SNAPSHOTTING), ('customers', PENDING))
        es[0]['started_at'] = T0.isoformat()
        ops = FakeOps(mbeans=[mbean(completed=True, rows={'db.orders': 1200}, duration=9)])
        r = step(es, ops)
        self.assertEqual(r.entry['state'], DONE)
        self.assertEqual(r.entry['rows'], 1200)
        self.assertTrue(r.progressed)

        es = apply(es, r)
        r2 = step(es, ops)
        self.assertEqual(ops.truncated, ['customers'])
        self.assertEqual(r2.entry['table'], 'customers')
        self.assertEqual(r2.entry['state'], SNAPSHOTTING)

    def test_all_done_reports_finished(self):
        r = step(entries(('orders', DONE), ('x', FAILED), ('y', SKIPPED)), FakeOps())
        self.assertEqual(r.hint, HINT_FINISHED)

    def test_waits_while_pipeline_paused(self):
        ops = FakeOps(paused='Waiting: sink connector is paused')
        r = step(entries(('orders', PENDING)), ops)
        self.assertEqual(r.hint, HINT_WAITING_PAUSED)
        self.assertEqual(ops.truncated, [])
        self.assertIsNone(r.entry)

    def test_waits_for_another_running_snapshot(self):
        ops = FakeOps(mbeans=[mbean(running=True, rows={'db.other': 10})])
        r = step(entries(('orders', PENDING)), ops)
        self.assertEqual(r.hint, HINT_QUEUED)
        self.assertEqual(ops.truncated, [])

    def test_never_truncates_blind_without_metrics(self):
        ops = FakeOps(mbeans=None)
        r = step(entries(('orders', PENDING)), ops)
        self.assertEqual(r.hint, HINT_QUEUED)
        self.assertEqual(ops.truncated, [])

    def test_lock_timeout_retried_then_failed_and_never_signalled(self):
        ops = FakeOps(truncate_result=(False, 'lock_timeout', 'Timed out after 30s'))
        es = entries(('orders', PENDING))
        for attempt in range(1, MAX_TRUNCATE_ATTEMPTS):
            r = step(es, ops)
            es = apply(es, r)
            self.assertEqual(es[0]['state'], PENDING)
            self.assertEqual(es[0]['truncate_attempts'], attempt)
        r = step(es, ops)
        self.assertEqual(r.entry['state'], FAILED)
        self.assertEqual(ops.signalled, [])
        self.assertEqual(len(ops.truncated), MAX_TRUNCATE_ATTEMPTS)

    def test_truncate_error_fails_without_signal(self):
        ops = FakeOps(truncate_result=(False, 'error', 'boom'))
        r = step(entries(('orders', PENDING)), ops)
        self.assertEqual(r.entry['state'], FAILED)
        self.assertEqual(ops.signalled, [])

    def test_missing_target_table_still_snapshots(self):
        ops = FakeOps(truncate_result=(True, 'not_found', 'does not exist yet'))
        r = step(entries(('orders', PENDING)), ops)
        self.assertEqual(r.entry['state'], SNAPSHOTTING)
        self.assertEqual(ops.signalled, ['orders'])

    def test_signal_failure_retries_without_truncating_again(self):
        ops = FakeOps(signal_error='broker down')
        es = entries(('orders', PENDING))
        r = step(es, ops)
        es = apply(es, r)
        self.assertEqual(es[0]['state'], TRUNCATED)
        self.assertEqual(ops.truncated, ['orders'])

        for _ in range(MAX_SIGNAL_ATTEMPTS - 1):
            r = step(es, ops)
            es = apply(es, r)
        self.assertEqual(ops.truncated, ['orders'])  # truncated exactly once
        self.assertEqual(es[0]['state'], FAILED)
        self.assertIn('EMPTY', es[0]['error'])

    def test_stale_completed_metrics_are_not_mistaken_for_our_snapshot(self):
        # Initial-snapshot MBean still says "completed" with the table in RowsScanned.
        stale = [mbean(type_='initial', completed=True, rows={'db.orders': 100}, duration=5)]
        ops = FakeOps(mbeans=stale)
        es = entries(('orders', PENDING))
        r = step(es, ops)          # truncate + signal, baseline captured
        es = apply(es, r)
        self.assertEqual(es[0]['state'], SNAPSHOTTING)

        ops.t = T0 + timedelta(seconds=30)
        r = step(es, ops)          # metrics unchanged → still waiting
        self.assertIsNone(r.entry)
        self.assertEqual(es[0]['state'], SNAPSHOTTING)

    def test_changed_fingerprint_counts_as_completion(self):
        ops = FakeOps(mbeans=[mbean(type_='initial', completed=True, rows={'db.orders': 100}, duration=5)])
        es = entries(('orders', PENDING))
        es = apply(es, step(es, ops))
        # A fast incremental snapshot ran and finished between ticks.
        ops.mbeans = [mbean(type_='initial', completed=True, rows={'db.orders': 130}, duration=1)]
        r = step(es, ops)
        self.assertEqual(r.entry['state'], DONE)
        self.assertEqual(r.entry['rows'], 130)

    def test_signal_resent_once_then_failed_when_snapshot_never_starts(self):
        ops = FakeOps(mbeans=[])
        es = entries(('orders', PENDING))
        es = apply(es, step(es, ops))
        self.assertEqual(ops.signalled, ['orders'])

        ops.t = T0 + timedelta(seconds=START_GRACE_SECONDS + 1)
        es = apply(es, step(es, ops))
        self.assertEqual(ops.signalled, ['orders', 'orders'])
        self.assertEqual(es[0]['state'], SNAPSHOTTING)
        self.assertEqual(ops.truncated, ['orders'])  # re-signal never re-truncates

        ops.t = ops.t + timedelta(seconds=START_GRACE_SECONDS + 1)
        r = step(es, ops)
        self.assertEqual(r.entry['state'], FAILED)
        self.assertEqual(len(ops.signalled), 2)

    def test_aborted_snapshot_fails_table(self):
        es = entries(('orders', SNAPSHOTTING))
        es[0]['started_at'] = T0.isoformat()
        ops = FakeOps(mbeans=[mbean(aborted=True, rows={'db.orders': 10})])
        r = step(es, ops)
        self.assertEqual(r.entry['state'], FAILED)

    def test_fast_snapshot_caught_by_start_confirmation(self):
        ops = FakeOps(start_mbeans=[mbean(running=True, rows={'db.orders': 3})])
        r = step(entries(('orders', PENDING)), ops)
        self.assertIsNotNone(r.entry['started_at'])


class ResyncHelperTests(SimpleTestCase):

    def test_match_table_key(self):
        rs = {'shop.orders': 1, 'shop.order_items': 2, 'PDB.APP.CUSTOMERS': 3}
        self.assertEqual(match_table_key(rs, 'orders'), 'shop.orders')
        self.assertEqual(match_table_key(rs, 'order_items'), 'shop.order_items')
        self.assertEqual(match_table_key(rs, 'customers'), 'PDB.APP.CUSTOMERS')
        self.assertIsNone(match_table_key(rs, 'items'))

    def test_plan_entries_dedupes_and_validates(self):
        entries_, added, skipped = plan_entries(
            ['a', 'b', 'a', ' c ', 'zzz', ''], enabled={'a', 'b', 'c'}, queued={'b'},
        )
        self.assertEqual(added, ['a', 'c'])
        self.assertEqual([e['table'] for e in entries_], ['a', 'c'])
        self.assertIn(('b', 'already queued'), skipped)
        self.assertIn(('zzz', 'no enabled mapping'), skipped)

    def test_cancel_skips_only_pending(self):
        tables, n, in_flight = cancel_entries(entries(('a', DONE), ('b', SNAPSHOTTING), ('c', PENDING)))
        self.assertEqual(n, 1)
        self.assertTrue(in_flight)
        self.assertEqual([t['state'] for t in tables], [DONE, SNAPSHOTTING, SKIPPED])

        tables, n, in_flight = cancel_entries(entries(('a', DONE), ('c', PENDING)))
        self.assertFalse(in_flight)


# --------------------------------------------------------------------------- #
# Row counts
# --------------------------------------------------------------------------- #
class RowCountSummaryTests(SimpleTestCase):

    def test_summary_counts_matches(self):
        from client.utils.row_counts import summarize_sync
        payload = {
            'status': 'ok', 'as_of': T0.isoformat(),
            'source_reachable': True, 'target_reachable': True, 'has_target_db': True,
            'tables': [
                {'source_rows': 10, 'target_rows': 10},
                {'source_rows': 5, 'target_rows': 4},
                {'source_rows': None, 'target_rows': None},
            ],
        }
        s = summarize_sync(payload)
        self.assertEqual(s['status'], 'not_synced')
        self.assertEqual((s['in_sync_count'], s['total_count']), (1, 3))

    def test_summary_snapshot_and_unreachable(self):
        from client.utils.row_counts import summarize_sync, STATUS_SNAPSHOT
        self.assertIsNone(summarize_sync(None))
        self.assertEqual(summarize_sync({'status': STATUS_SNAPSHOT})['status'], STATUS_SNAPSHOT)
        self.assertEqual(
            summarize_sync({'status': 'ok', 'source_reachable': False, 'has_target_db': True,
                            'target_reachable': True, 'tables': [{}]})['status'],
            'unable_to_check',
        )

    def test_not_found_classification(self):
        from client.utils.row_counts import is_not_found_error
        self.assertTrue(is_not_found_error("(1146, \"Table 'x.y' doesn't exist\")"))
        self.assertTrue(is_not_found_error('ORA-00942: table or view does not exist'))
        self.assertFalse(is_not_found_error('Query execution was interrupted, maximum statement execution time exceeded'))


# --------------------------------------------------------------------------- #
# Health monitor restart budget
# --------------------------------------------------------------------------- #
@override_settings(CACHES={'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
                                       'LOCATION': 'resync-job-tests'}})
class RestartBudgetTests(SimpleTestCase):

    def setUp(self):
        from django.core.cache import cache
        cache.clear()

    def test_restarts_capped_then_reset_when_healthy(self):
        from client.replication.health_monitor import _restart_allowed, _reset_restart_budget, _RESTART_MAX
        for _ in range(_RESTART_MAX):
            self.assertTrue(_restart_allowed('sink-a'))
        self.assertFalse(_restart_allowed('sink-a'))
        self.assertTrue(_restart_allowed('sink-b'))  # per connector
        _reset_restart_budget('sink-a')
        self.assertTrue(_restart_allowed('sink-a'))


# --------------------------------------------------------------------------- #
# Unreachable vs not found
# --------------------------------------------------------------------------- #
class ConnectorStatusMappingTests(SimpleTestCase):

    def _mixin(self, outcome, data=None):
        from client.replication.orchestrator.status import StatusMixin
        obj = object.__new__(StatusMixin)
        obj.connector_manager = mock.Mock()
        obj.connector_manager.get_connector_status_detailed.return_value = (outcome, data)
        return obj

    def test_timeout_is_unreachable_not_missing(self):
        obj = self._mixin('unreachable')
        status = obj._status_for_connector('c1')
        self.assertEqual(status['state'], 'UNREACHABLE')
        self.assertTrue(status['unknown'])
        healthy = {'state': 'RUNNING', 'healthy': True, 'tasks': []}
        self.assertEqual(obj._calculate_overall_health(status, healthy), 'unknown')

    def test_404_is_not_found(self):
        self.assertEqual(self._mixin('not_found')._status_for_connector('c1')['state'], 'NOT_FOUND')

    def test_ok_with_failed_task(self):
        data = {'connector': {'state': 'RUNNING'}, 'tasks': [{'state': 'FAILED'}]}
        status = self._mixin('ok', data)._status_for_connector('c1')
        self.assertEqual(status['state'], 'RUNNING')
        self.assertFalse(status['healthy'])

    def test_detailed_status_classification(self):
        from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
        mgr = object.__new__(DebeziumConnectorManager)
        mgr.connectors_url = 'http://connect/connectors'
        with mock.patch.object(DebeziumConnectorManager, '_make_request',
                               return_value=(False, None, 'HTTP 404: Connector c1 not found')):
            self.assertEqual(mgr.get_connector_status_detailed('c1'), ('not_found', None))
        with mock.patch.object(DebeziumConnectorManager, '_make_request',
                               return_value=(False, None, 'Request timeout after 8 seconds')):
            self.assertEqual(mgr.get_connector_status_detailed('c1'), ('unreachable', None))


# --------------------------------------------------------------------------- #
# Truncate lock timeout / signal delivery
# --------------------------------------------------------------------------- #
class TruncateAndSignalTests(SimpleTestCase):

    def test_lock_timeout_detection(self):
        from client.utils.table_creator import _is_lock_timeout_error
        self.assertTrue(_is_lock_timeout_error('(1205, "Lock wait timeout exceeded; try restarting transaction")'))
        self.assertTrue(_is_lock_timeout_error('canceling statement due to lock timeout'))
        self.assertTrue(_is_lock_timeout_error('Lock request time out period exceeded.'))
        self.assertTrue(_is_lock_timeout_error('ORA-00054: resource busy and acquire with NOWAIT specified'))
        self.assertFalse(_is_lock_timeout_error("(1146, \"Table 'x' doesn't exist\")"))

    def _manager(self, remaining=0, delivery_error=None):
        from jovoclient.utils.kafka.signal import KafkaSignalManager
        mgr = object.__new__(KafkaSignalManager)
        mgr.signal_topic = 'p.signals'
        mgr.connector_name = 'p'
        mgr._delivery_errors = []
        producer = mock.Mock()

        def produce(**kwargs):
            if delivery_error:
                mgr._delivery_errors.append(delivery_error)
        producer.produce.side_effect = produce
        producer.flush.return_value = remaining
        mgr.producer = producer
        return mgr

    def test_unacknowledged_signal_raises(self):
        with self.assertRaises(RuntimeError):
            self._manager(remaining=1).trigger_adhoc_snapshot(['db.orders'])

    def test_delivery_error_raises(self):
        with self.assertRaises(RuntimeError):
            self._manager(delivery_error='Broker: Not enough in-sync replicas').trigger_adhoc_snapshot(['db.orders'])

    def test_delivered_signal_returns_id(self):
        self.assertTrue(self._manager().trigger_adhoc_snapshot(['db.orders']))
