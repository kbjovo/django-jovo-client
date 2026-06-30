"""
Unit tests for the truncate-ordering and rename fixes in KafkaDDLProcessor and
the target adapters.

These exercise the pure logic in isolation by bypassing the heavy __init__
(Kafka subscribe, DB lookups) via object.__new__, so they need neither a running
Kafka cluster nor a database.

Run: python manage.py test client.test_truncate_ordering
"""

from unittest import mock

from django.test import SimpleTestCase

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from client.utils.ddl.kafka_processor import KafkaDDLProcessor
from client.utils.ddl.adapters.mysql_adapter import MySQLTargetAdapter
from client.utils.ddl.adapters.postgres_adapter import PostgreSQLTargetAdapter
from client.utils.ddl.base_processor import DDLOperation, DDLOperationType


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _FakeFuture:
    def __init__(self, result):
        self._result = result

    def result(self, timeout=None):
        return self._result


class _FakeTP:
    def __init__(self, topic, partition, offset):
        self.topic, self.partition, self.offset = topic, partition, offset


class _FakeCGTP:
    def __init__(self, tps):
        self.topic_partitions = tps


class _FakeAdmin:
    """Stand-in for confluent_kafka AdminClient returning controlled committed offsets."""

    def __init__(self, committed):
        # committed: dict[(topic, partition)] -> next-offset-to-read
        self.committed = committed

    def list_consumer_group_offsets(self, requests):
        req = requests[0]
        tps = [
            _FakeTP(tp.topic, tp.partition, self.committed.get((tp.topic, tp.partition), -1001))
            for tp in req.topic_partitions
        ]
        return {req.group_id: _FakeFuture(_FakeCGTP(tps))}


class _FakeConsumer:
    """Minimal consumer for exercising _commit_truncate_watermark."""

    def __init__(self, positions):
        self._positions = positions  # list[TopicPartition] (next offsets)
        self.committed = None

    def assignment(self):
        return list(self._positions)

    def position(self, parts):
        return [TopicPartition(p.topic, p.partition, p.offset) for p in self._positions]

    def commit(self, offsets=None, asynchronous=True):
        self.committed = offsets


class _FakeSubConsumer:
    """Records subscribe() calls for the refresh test."""

    def __init__(self):
        self.subscribe_calls = []

    def subscribe(self, topics):
        self.subscribe_calls.append(list(topics))


def _bare_processor():
    """A KafkaDDLProcessor with only the attributes the truncate logic touches."""
    p = object.__new__(KafkaDDLProcessor)
    p._truncate_pending = []
    p._admin_client = None
    p._sink_consumer_group = "connect-sink"
    p.truncate_consumer = None
    p._truncate_deserializer = None
    p._watched_topics = set()
    p._last_topic_refresh = 0.0
    return p


# --------------------------------------------------------------------------- #
# _sink_caught_up
# --------------------------------------------------------------------------- #
class SinkCaughtUpTests(SimpleTestCase):
    def test_committed_past_offset_is_caught_up(self):
        p = _bare_processor()
        p._admin_client = _FakeAdmin({("t", 0): 5})
        # committed=5 (next read) means offsets 0..4 consumed -> caught up for offset 3, 4
        self.assertTrue(p._sink_caught_up("t", 0, 3))
        self.assertTrue(p._sink_caught_up("t", 0, 4))

    def test_committed_at_or_below_offset_is_not_caught_up(self):
        p = _bare_processor()
        p._admin_client = _FakeAdmin({("t", 0): 5})
        # truncate at offset 5 not yet consumed (next read is 5)
        self.assertFalse(p._sink_caught_up("t", 0, 5))
        self.assertFalse(p._sink_caught_up("t", 0, 9))

    def test_no_committed_offset_is_not_caught_up(self):
        p = _bare_processor()
        p._admin_client = _FakeAdmin({})  # -> -1001
        self.assertFalse(p._sink_caught_up("t", 0, 0))

    def test_missing_admin_degrades_to_proceed(self):
        p = _bare_processor()
        p._admin_client = None
        self.assertTrue(p._sink_caught_up("t", 0, 100))

    def test_admin_error_degrades_to_proceed(self):
        p = _bare_processor()
        boom = mock.Mock()
        boom.list_consumer_group_offsets.side_effect = RuntimeError("broker down")
        p._admin_client = boom
        self.assertTrue(p._sink_caught_up("t", 0, 1))


# --------------------------------------------------------------------------- #
# _retry_pending_truncates — the pause/lag deferral behaviour
# --------------------------------------------------------------------------- #
class RetryPendingTruncatesTests(SimpleTestCase):
    def test_truncate_deferred_while_sink_behind(self):
        p = _bare_processor()
        p._admin_client = _FakeAdmin({("pfx.db.orders", 0): 2})  # behind the truncate
        p._truncate_pending = [
            {"table": "orders", "topic": "pfx.db.orders", "partition": 0, "offset": 7}
        ]
        p._handle_truncate_for_table = mock.Mock(return_value=True)

        applied = p._retry_pending_truncates()

        self.assertEqual(applied, 0)
        p._handle_truncate_for_table.assert_not_called()
        self.assertEqual(len(p._truncate_pending), 1)  # still queued

    def test_truncate_applied_once_sink_catches_up(self):
        p = _bare_processor()
        p._admin_client = _FakeAdmin({("pfx.db.orders", 0): 99})  # well past
        p._truncate_pending = [
            {"table": "orders", "topic": "pfx.db.orders", "partition": 0, "offset": 7}
        ]
        p._handle_truncate_for_table = mock.Mock(return_value=True)

        applied = p._retry_pending_truncates()

        self.assertEqual(applied, 1)
        p._handle_truncate_for_table.assert_called_once_with("orders")
        self.assertEqual(p._truncate_pending, [])  # drained

    def test_failed_resync_stays_pending_for_retry(self):
        # Sink is caught up, but the resync (e.g. snapshot signal) fails -> keep it
        # queued so the next poll retries instead of dropping it (and leaving the
        # target truncated-but-empty forever).
        p = _bare_processor()
        p._admin_client = _FakeAdmin({("pfx.db.orders", 0): 99})
        p._truncate_pending = [
            {"table": "orders", "topic": "pfx.db.orders", "partition": 0, "offset": 7}
        ]
        p._handle_truncate_for_table = mock.Mock(return_value=False)

        applied = p._retry_pending_truncates()

        self.assertEqual(applied, 0)
        self.assertEqual(len(p._truncate_pending), 1)  # retained for retry


# --------------------------------------------------------------------------- #
# _commit_truncate_watermark — never advance past an un-applied truncate
# --------------------------------------------------------------------------- #
class CommitWatermarkTests(SimpleTestCase):
    def test_watermark_held_at_earliest_pending(self):
        p = _bare_processor()
        # consumer's next positions are well ahead...
        p.truncate_consumer = _FakeConsumer([TopicPartition("pfx.db.orders", 0, 50)])
        # ...but a truncate at offset 7 hasn't been applied yet
        p._truncate_pending = [
            {"table": "orders", "topic": "pfx.db.orders", "partition": 0, "offset": 7}
        ]

        p._commit_truncate_watermark()

        committed = {(tp.topic, tp.partition): tp.offset for tp in p.truncate_consumer.committed}
        self.assertEqual(committed[("pfx.db.orders", 0)], 7)  # rewound to the truncate

    def test_watermark_advances_when_nothing_pending(self):
        p = _bare_processor()
        p.truncate_consumer = _FakeConsumer([TopicPartition("pfx.db.orders", 0, 50)])
        p._truncate_pending = []

        p._commit_truncate_watermark()

        committed = {(tp.topic, tp.partition): tp.offset for tp in p.truncate_consumer.committed}
        self.assertEqual(committed[("pfx.db.orders", 0)], 50)  # normal forward progress


# --------------------------------------------------------------------------- #
# _refresh_truncate_subscription — pick up newly added tables
# --------------------------------------------------------------------------- #
class RefreshSubscriptionTests(SimpleTestCase):
    def _processor_with(self, watched, desired):
        p = _bare_processor()
        p.truncate_consumer = _FakeSubConsumer()
        p._watched_topics = set(watched)
        p._last_topic_refresh = 0.0  # force throttle window elapsed
        p._build_change_topics = mock.Mock(return_value=list(desired))
        return p

    def test_resubscribes_when_table_added(self):
        p = self._processor_with(
            watched={"pfx.db.orders"},
            desired={"pfx.db.orders", "pfx.db.customers"},
        )
        p._refresh_truncate_subscription()
        self.assertEqual(len(p.truncate_consumer.subscribe_calls), 1)
        self.assertEqual(
            set(p.truncate_consumer.subscribe_calls[0]),
            {"pfx.db.orders", "pfx.db.customers"},
        )
        self.assertEqual(p._watched_topics, {"pfx.db.orders", "pfx.db.customers"})

    def test_no_resubscribe_when_unchanged(self):
        p = self._processor_with(
            watched={"pfx.db.orders"},
            desired={"pfx.db.orders"},
        )
        p._refresh_truncate_subscription()
        self.assertEqual(p.truncate_consumer.subscribe_calls, [])

    def test_throttled_within_interval(self):
        p = self._processor_with(
            watched={"pfx.db.orders"},
            desired={"pfx.db.orders", "pfx.db.customers"},
        )
        import time as _t
        p._last_topic_refresh = _t.monotonic()  # just refreshed -> skip
        p._refresh_truncate_subscription()
        self.assertEqual(p.truncate_consumer.subscribe_calls, [])


# --------------------------------------------------------------------------- #
# RENAME TABLE adapters
# --------------------------------------------------------------------------- #
class RenameTableSqlTests(SimpleTestCase):
    def test_mysql_rename_sql(self):
        a = object.__new__(MySQLTargetAdapter)
        a.execute_sql = mock.Mock(return_value=(True, None))
        op = DDLOperation(
            operation_type=DDLOperationType.RENAME_TABLE,
            table_name="db_orders",
            details={"new_name": "db_sales"},
        )
        a._handle_rename_table(op)
        a.execute_sql.assert_called_once_with("RENAME TABLE `db_orders` TO `db_sales`")

    def test_mysql_rename_missing_new_name(self):
        a = object.__new__(MySQLTargetAdapter)
        a.execute_sql = mock.Mock()
        op = DDLOperation(operation_type=DDLOperationType.RENAME_TABLE, table_name="x", details={})
        ok, err = a._handle_rename_table(op)
        self.assertFalse(ok)
        a.execute_sql.assert_not_called()

    def test_postgres_rename_sql(self):
        a = object.__new__(PostgreSQLTargetAdapter)
        a.execute_sql = mock.Mock(return_value=(True, None))
        a._full_table_name = mock.Mock(return_value='"public"."db_orders"')
        op = DDLOperation(
            operation_type=DDLOperationType.RENAME_TABLE,
            table_name="db_orders",
            details={"new_name": "db_sales"},
        )
        a._handle_rename_table(op)
        a.execute_sql.assert_called_once_with('ALTER TABLE "public"."db_orders" RENAME TO "db_sales"')
