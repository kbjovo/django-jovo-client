"""
MySQL integration tests for the TRUNCATE / row-count session behaviour that
caused the production incident. Runs only when the Django database is MySQL;
all tables live in Django's throw-away test database.

Proves against a real server that:
  * row counts run in AUTOCOMMIT (no metadata lock held after each COUNT),
  * a parent table referenced by a target-side FK can be truncated (no 1701),
  * a TRUNCATE blocked by an open reader gives up after lock_wait_seconds and
    is reported as 'lock_timeout' instead of hanging (MySQL default: 1 year).

Run: python manage.py test client.test_resync_mysql_integration
"""

import unittest

from django.conf import settings
from django.db import connection
from django.test import TransactionTestCase


@unittest.skipUnless(connection.vendor == 'mysql', 'MySQL-only integration test')
class MySQLTruncateAndCountTests(TransactionTestCase):

    @classmethod
    def _client_db(cls):
        from client.encryption import encrypt_password
        from client.models import ClientDatabase
        cfg = connection.settings_dict
        db = ClientDatabase(
            connection_name='it-target', db_type='mysql',
            host=cfg.get('HOST') or 'localhost', port=int(cfg.get('PORT') or 3306),
            username=cfg['USER'], database_name=cfg['NAME'],
        )
        db.password = encrypt_password(cfg['PASSWORD'])
        return db

    def setUp(self):
        with connection.cursor() as c:
            c.execute("DROP TABLE IF EXISTS it_child")
            c.execute("DROP TABLE IF EXISTS it_parent")
            c.execute("CREATE TABLE it_parent (id INT PRIMARY KEY) ENGINE=InnoDB")
            c.execute("CREATE TABLE it_child (id INT PRIMARY KEY, parent_id INT, "
                      "CONSTRAINT fk_it_child FOREIGN KEY (parent_id) REFERENCES it_parent(id)) ENGINE=InnoDB")
            c.execute("INSERT INTO it_parent VALUES (1),(2),(3)")
            c.execute("INSERT INTO it_child VALUES (10,1),(11,2)")
        self.db = self._client_db()

    def tearDown(self):
        with connection.cursor() as c:
            c.execute("DROP TABLE IF EXISTS it_child")
            c.execute("DROP TABLE IF EXISTS it_parent")

    def _mapping(self, table):
        from types import SimpleNamespace
        return SimpleNamespace(target_table=table, target_schema=None)

    def test_counts_and_not_found(self):
        from client.utils.row_counts import _count_all
        reachable, res = _count_all(self.db, [(1, 'it_parent', None), (2, 'it_child', None), (3, 'it_missing', None)])
        self.assertTrue(reachable)
        self.assertEqual(res[1], (3, None))
        self.assertEqual(res[2], (2, None))
        self.assertEqual(res[3], (None, 'table_not_found'))

    def test_truncate_fk_parent_succeeds(self):
        from client.utils.table_creator import truncate_target_table
        ok, kind, msg = truncate_target_table(self.db, self._mapping('it_parent'), lock_wait_seconds=5)
        self.assertTrue(ok, msg)
        self.assertIsNone(kind)
        with connection.cursor() as c:
            c.execute("SELECT COUNT(*) FROM it_parent")
            self.assertEqual(c.fetchone()[0], 0)

    def test_truncate_missing_table_is_not_found(self):
        from client.utils.table_creator import truncate_target_table
        ok, kind, _ = truncate_target_table(self.db, self._mapping('it_missing'), lock_wait_seconds=1)
        self.assertTrue(ok)
        self.assertEqual(kind, 'not_found')

    def test_truncate_blocked_by_open_reader_times_out(self):
        from client.utils.database_utils import get_database_engine
        from client.utils.table_creator import truncate_target_table
        from sqlalchemy import text

        engine = get_database_engine(self.db, pool_size=1)
        holder = engine.connect()
        try:
            # An open transaction that has read the table holds a shared metadata lock
            # until it ends — exactly what a long COUNT(*) sweep used to do.
            holder.execute(text("SELECT COUNT(*) FROM it_child"))
            ok, kind, msg = truncate_target_table(self.db, self._mapping('it_child'), lock_wait_seconds=1)
            self.assertFalse(ok)
            self.assertEqual(kind, 'lock_timeout', msg)
        finally:
            holder.rollback()
            holder.close()
            engine.dispose()

        ok, kind, msg = truncate_target_table(self.db, self._mapping('it_child'), lock_wait_seconds=5)
        self.assertTrue(ok, msg)
