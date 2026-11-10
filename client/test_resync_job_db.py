"""
Database-backed tests for the sequential resync job runner: enqueue / merge /
cancel / finish bookkeeping around client.replication.resync.step().

Side effects (truncate, signal, Jolokia) are faked; Celery kicks and row-count
refreshes are patched out. Uses Django's test database.

Run: python manage.py test client.test_resync_job_db
"""

from unittest import mock

from django.test import TestCase, override_settings

from client.models import Client, ClientDatabase, ReplicationConfig, TableMapping, TableResyncJob
from client.replication import resync
from client.replication.resync import DONE, PENDING, SKIPPED, SNAPSHOTTING
from client.test_resync_job import FakeOps, mbean


@override_settings(CACHES={'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
                                       'LOCATION': 'resync-job-db-tests'}})
class ResyncJobDbTests(TestCase):

    def setUp(self):
        from django.core.cache import cache
        cache.clear()
        client = Client.objects.create(name='Acme', email='ops@example.com', phone='1')
        src = ClientDatabase.objects.create(
            client=client, connection_name='src', host='h', username='u', password='p',
            database_name='shop', db_type='mysql',
        )
        self.config = ReplicationConfig.objects.create(client_database=src, connector_name='c1')
        for t in ('orders', 'customers', 'items'):
            TableMapping.objects.create(replication_config=self.config, source_table=t,
                                        target_table=f'shop_{t}', is_enabled=True)
        TableMapping.objects.create(replication_config=self.config, source_table='disabled',
                                    target_table='shop_disabled', is_enabled=False)

        self.kick = mock.patch.object(resync, '_kick').start()
        self.refresh = mock.patch('client.utils.row_counts.request_refresh').start()
        self.addCleanup(mock.patch.stopall)

    def _job(self):
        return TableResyncJob.objects.get(replication_config=self.config)

    def test_enqueue_creates_then_appends_with_dedupe(self):
        job, added, skipped = resync.enqueue_resync(self.config, ['orders', 'customers', 'disabled', 'nope'])
        self.assertEqual(added, ['orders', 'customers'])
        self.assertEqual({t for t, _ in skipped}, {'disabled', 'nope'})
        self.assertEqual(job.status, TableResyncJob.STATUS_QUEUED)

        job2, added2, skipped2 = resync.enqueue_resync(self.config, ['customers', 'items'])
        self.assertEqual(job2.pk, job.pk)                      # appended to the active job
        self.assertEqual(added2, ['items'])
        self.assertIn(('customers', 'already queued'), skipped2)
        self.assertEqual([e['table'] for e in self._job().tables], ['orders', 'customers', 'items'])
        self.assertTrue(resync.has_active_job(self.config.pk))

    def test_advance_processes_one_table_and_persists(self):
        resync.enqueue_resync(self.config, ['orders', 'customers'])
        ops = FakeOps()
        resync.advance_job(self._job().pk, ops_factory=lambda cfg: ops)

        job = self._job()
        self.assertEqual(ops.truncated, ['orders'])
        self.assertEqual([e['state'] for e in job.tables], [SNAPSHOTTING, PENDING])
        self.assertEqual(job.status, TableResyncJob.STATUS_RUNNING)

    def test_commit_keeps_tables_appended_concurrently(self):
        job, _, _ = resync.enqueue_resync(self.config, ['orders'])
        snapshot_of_tables = list(job.tables)
        result = resync.step(snapshot_of_tables, FakeOps())   # decided on the old copy
        resync.enqueue_resync(self.config, ['items'])         # appended meanwhile
        resync._commit(job.pk, result)

        tables = self._job().tables
        self.assertEqual([e['table'] for e in tables], ['orders', 'items'])
        self.assertEqual(tables[0]['state'], SNAPSHOTTING)
        self.assertEqual(tables[1]['state'], PENDING)

    def test_job_completes_and_requests_recount(self):
        job, _, _ = resync.enqueue_resync(self.config, ['orders'])
        ops = FakeOps()
        resync.advance_job(job.pk, ops_factory=lambda cfg: ops)       # truncate + signal
        ops.mbeans = [mbean(completed=True, rows={'shop.orders': 42}, duration=3)]
        resync.advance_job(job.pk, ops_factory=lambda cfg: ops)       # done
        resync.advance_job(job.pk, ops_factory=lambda cfg: ops)       # no-op: already finished

        job = self._job()
        self.assertEqual(job.status, TableResyncJob.STATUS_COMPLETED)
        self.assertIsNotNone(job.finished_at)
        self.assertEqual(job.tables[0]['state'], DONE)
        self.assertEqual(job.tables[0]['rows'], 42)
        self.refresh.assert_called_once_with(self.config.pk, force=True)
        self.assertFalse(resync.has_active_job(self.config.pk))

    def test_cancel_with_and_without_table_in_flight(self):
        job, _, _ = resync.enqueue_resync(self.config, ['orders', 'customers'])
        resync.advance_job(job.pk, ops_factory=lambda cfg: FakeOps())   # orders in flight
        ok, _ = resync.cancel_job(job.pk)
        self.assertTrue(ok)
        job = self._job()
        self.assertEqual([e['state'] for e in job.tables], [SNAPSHOTTING, SKIPPED])
        self.assertTrue(job.is_active)            # in-flight table finishes first

        ops = FakeOps(mbeans=[mbean(completed=True, rows={'shop.orders': 1}, duration=1)])
        resync.advance_job(job.pk, ops_factory=lambda cfg: ops)
        self.assertEqual(self._job().status, TableResyncJob.STATUS_CANCELLED)
        self.assertEqual(ops.truncated, [])       # customers never touched

    def test_cancel_before_start_finishes_immediately(self):
        job, _, _ = resync.enqueue_resync(self.config, ['orders'])
        resync.cancel_job(job.pk)
        self.assertEqual(self._job().status, TableResyncJob.STATUS_CANCELLED)

    def test_only_oldest_active_job_runs(self):
        older = TableResyncJob.objects.create(replication_config=self.config,
                                              tables=[resync.new_entry('orders')])
        newer = TableResyncJob.objects.create(replication_config=self.config,
                                              tables=[resync.new_entry('items')])
        ops = FakeOps()
        resync.advance_job(newer.pk, ops_factory=lambda cfg: ops)
        self.assertEqual(ops.truncated, [])
        resync.advance_job(older.pk, ops_factory=lambda cfg: ops)
        self.assertEqual(ops.truncated, ['orders'])

    def test_summary_for_monitor_page(self):
        self.assertIsNone(resync.job_summary(self.config.pk))
        resync.enqueue_resync(self.config, ['orders', 'customers'])
        summary = resync.job_summary(self.config.pk)
        self.assertTrue(summary['active'])
        self.assertEqual(summary['counts'], {'pending': 2})
        self.assertEqual([t['table'] for t in summary['tables']], ['orders', 'customers'])

    def test_auto_truncate_path_enqueues_instead_of_truncating(self):
        from client.utils.ddl.base_processor import _apply_truncate_and_snapshot
        with mock.patch('client.utils.table_creator.truncate_tables_for_mappings') as trunc:
            self.assertTrue(_apply_truncate_and_snapshot(self.config, 'orders'))
            trunc.assert_not_called()
        job = self._job()
        self.assertEqual(job.source, TableResyncJob.SOURCE_AUTO_TRUNCATE)
        self.assertEqual([e['table'] for e in job.tables], ['orders'])
        # untracked table: nothing queued, still "handled"
        self.assertTrue(_apply_truncate_and_snapshot(self.config, 'unknown_table'))

    def test_orchestrator_resync_tables_queues(self):
        from client.replication.orchestrator import ReplicationOrchestrator
        ok, msg = ReplicationOrchestrator(self.config).resync_tables(['orders', 'items'])
        self.assertTrue(ok)
        self.assertIn('Queued 2', msg)
        ok, msg = ReplicationOrchestrator(self.config).resync_tables(['orders'])
        self.assertFalse(ok)
        self.assertIn('already queued', msg)


@override_settings(CACHES={'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
                                       'LOCATION': 'refresh-configs-tests'}})
class RefreshConnectorConfigsCommandTests(TestCase):
    """Dry run shows the template drift; --apply pushes it; active resyncs are skipped."""

    def setUp(self):
        client = Client.objects.create(name='Acme', email='ops@example.com', phone='1')
        src = ClientDatabase.objects.create(
            client=client, connection_name='src', host='h', username='u', password='p',
            database_name='shop', db_type='mysql',
        )
        ClientDatabase.objects.create(
            client=client, connection_name='tgt', host='t', username='u', password='p',
            database_name='dwh', db_type='mysql', is_target=True,
        )
        self.config = ReplicationConfig.objects.create(
            client_database=src, connector_name='c1', sink_connector_name='c1_sink', is_active=True,
        )
        TableMapping.objects.create(replication_config=self.config, source_table='orders',
                                    target_table='shop_orders', is_enabled=True)

        from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
        old_sink = {'connection.url': 'jdbc:mysql://t:3306/dwh?connectTimeout=30000', 'name': 'c1_sink'}
        old_source = {'connector.class': 'io.debezium.connector.mysql.MySqlConnector', 'name': 'c1'}
        self.get_cfg = mock.patch.object(
            DebeziumConnectorManager, 'get_connector_config',
            side_effect=lambda name: dict(old_sink if name.endswith('_sink') else old_source),
        ).start()
        self.put_cfg = mock.patch.object(
            DebeziumConnectorManager, 'update_connector_config', return_value=(True, None),
        ).start()
        mock.patch('jovoclient.utils.kafka.topic_manager.KafkaTopicManager.topic_exists', return_value=True).start()
        mock.patch('jovoclient.utils.kafka.topic_manager.KafkaTopicManager.__init__', return_value=None).start()
        self.addCleanup(mock.patch.stopall)

    def _run(self, *args):
        from io import StringIO
        from django.core.management import call_command
        out = StringIO()
        call_command('refresh_connector_configs', *args, stdout=out)
        return out.getvalue()

    def test_dry_run_reports_drift_without_pushing(self):
        out = self._run()
        self.assertIn('DRY RUN', out)
        self.assertIn('rewriteBatchedStatements=true', out)
        self.assertIn('+ heartbeat.interval.ms = 10000', out)
        self.assertIn('+ flush.max.retries = 10', out)
        self.assertNotIn('connection.password = p', out)   # secrets masked
        self.put_cfg.assert_not_called()

    def test_apply_pushes_sink_only(self):
        ReplicationConfig.objects.filter(pk=self.config.pk).update(sink_connector_state='PAUSED')
        out = self._run('--sink-only', '--apply')
        self.assertIn('applied', out)
        self.assertEqual(self.put_cfg.call_count, 1)
        name, cfg = self.put_cfg.call_args[0]
        self.assertEqual(name, 'c1_sink')
        self.assertIn('sessionVariables=foreign_key_checks=0', cfg['connection.url'])
        # never touches model state — a user-paused sink stays paused
        self.config.refresh_from_db()
        self.assertEqual(self.config.sink_connector_state, 'PAUSED')

    def test_skips_connector_with_active_resync(self):
        with mock.patch.object(resync, '_kick'):
            resync.enqueue_resync(self.config, ['orders'])
        out = self._run('--apply')
        self.assertIn('SKIPPED — resync job in progress', out)
        self.put_cfg.assert_not_called()

    # --- Regression: the 2026-11-10 rollout run from the host (.env lacks
    #     SINK_ERRORS_TOLERANCE) flipped errors.tolerance all→none and dropped the DLQ.
    def _live_sink(self, **extra):
        live = {
            'connection.url': 'jdbc:mysql://t:3306/dwh?connectTimeout=30000',
            'errors.tolerance': 'all',
            'errors.deadletterqueue.topic.name': 'client_1_db_1_v_1.dlq',
            'errors.deadletterqueue.topic.replication.factor': '3',
            'errors.deadletterqueue.context.headers.enable': 'true',
            'value.converter.schema.registry.url': 'http://schema-registry:8081',
            'some.live.only.key': 'keep-me',
            'name': 'c1_sink',
        }
        live.update(extra)
        self.get_cfg.side_effect = lambda name: dict(live)
        return live

    @override_settings(SINK_CONNECTOR_CONFIG={'ERRORS_TOLERANCE': 'none', 'DLQ_ENABLED': True,
                                              'DLQ_REPLICATION_FACTOR': 3, 'DLQ_CONTEXT_HEADERS': True})
    def test_live_error_handling_and_extra_keys_survive(self):
        self._live_sink()
        out = self._run('--sink-only', '--apply')
        _, cfg = self.put_cfg.call_args[0]
        self.assertEqual(cfg['errors.tolerance'], 'all')
        self.assertEqual(cfg['errors.deadletterqueue.topic.name'], 'client_1_db_1_v_1.dlq')
        self.assertEqual(cfg['errors.deadletterqueue.topic.replication.factor'], '3')
        self.assertEqual(cfg['value.converter.schema.registry.url'], 'http://schema-registry:8081')
        self.assertEqual(cfg['some.live.only.key'], 'keep-me')
        self.assertIn('rewriteBatchedStatements=true', cfg['connection.url'])   # the intended change
        self.assertIn('protected — live value kept', out)
        self.assertIn('errors.tolerance [error handling]', out)
        self.assertIn('SINK_ERRORS_TOLERANCE=none', out)                         # environment shown
        self.assertNotIn('- errors.', out)

    @override_settings(SINK_CONNECTOR_CONFIG={'ERRORS_TOLERANCE': 'all', 'DLQ_ENABLED': True,
                                              'DLQ_REPLICATION_FACTOR': 3, 'DLQ_CONTEXT_HEADERS': True})
    def test_overwrite_protected_restores_error_handling(self):
        # State left behind by the bad run: tolerance none, DLQ keys gone.
        live = self._live_sink(**{'errors.tolerance': 'none'})
        for k in [k for k in live if k.startswith('errors.deadletterqueue')]:
            live.pop(k)
        self.get_cfg.side_effect = lambda name: dict(live)

        self._run('--sink-only', '--apply', '--overwrite-protected')
        _, cfg = self.put_cfg.call_args[0]
        self.assertEqual(cfg['errors.tolerance'], 'all')
        prefix = f"client_{self.config.client_database.client_id}_db_{self.config.client_database_id}_v_{self.config.connector_version}"
        self.assertEqual(cfg['errors.deadletterqueue.topic.name'], f"{prefix}.dlq")
        self.assertEqual(cfg['errors.deadletterqueue.topic.replication.factor'], '3')

    def test_allow_removals_drops_live_only_keys(self):
        self._live_sink()
        self._run('--sink-only', '--apply', '--allow-removals')
        _, cfg = self.put_cfg.call_args[0]
        self.assertNotIn('some.live.only.key', cfg)
        self.assertEqual(cfg['errors.tolerance'], 'all')   # still protected
