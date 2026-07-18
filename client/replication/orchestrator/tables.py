"""TablesMixin

Add / remove / resync tables and pending-truncate application.

Split from the original monolithic ReplicationOrchestrator.
"""

import logging
from typing import Dict, Any, Tuple, Optional, Set
from django.utils import timezone
from django.conf import settings

from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.debezium.jolokia_client import JolokiaClient
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager, format_topic_name
from jovoclient.utils.debezium.schema_registry_utils import delete_schema_subject, set_compatibility_mode
from jovoclient.utils.debezium.connector_templates import get_connector_config_for_database
from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database
from client.utils.table_creator import drop_tables_for_mappings, truncate_tables_for_mappings
from ..validators import ReplicationValidator
from sqlalchemy import text

logger = logging.getLogger("client.replication.orchestrator")


class TablesMixin:
    def resync_table(self, table_name: str) -> Tuple[bool, str]:
        """
        Manually resync a single table after a source TRUNCATE + repopulate.

        Use this for PostgreSQL and SQL Server sources where TRUNCATE events are
        not automatically detected by the DDL processor (MySQL and Oracle handle
        this automatically via the schema history topic).

        Steps:
          1. Truncate target table (clears stale rows, keeps schema).
          2. Send incremental snapshot signal so Debezium re-reads the source.

        Args:
            table_name: Source table name (unqualified, e.g. 'orders')

        Returns:
            Tuple[bool, str]: (success, message)
        """
        from client.utils.table_creator import truncate_tables_for_mappings
        from jovoclient.utils.kafka.signal import send_incremental_snapshot_signal

        table_mapping = self.config.table_mappings.filter(
            source_table=table_name,
            is_enabled=True
        ).first()
        if not table_mapping:
            return False, f"No enabled mapping found for table '{table_name}'"

        target_db = self.config.client_database.client.client_databases.filter(
            is_target=True
        ).first()
        if not target_db:
            return False, "No target database configured"

        # Step 1: truncate target
        self._log_info(f"Resync '{table_name}': truncating target table '{table_mapping.target_table}'")
        ok, msg = truncate_tables_for_mappings(target_db, [table_mapping])
        if not ok:
            return False, f"Failed to truncate target table: {msg}"

        # Step 2: incremental snapshot signal
        self._log_info(f"Resync '{table_name}': sending incremental snapshot signal")
        try:
            signal_id, method = send_incremental_snapshot_signal(
                self.config.client_database,
                self.config,
                [table_name]
            )
        except Exception as e:
            return False, f"Target truncated but snapshot signal failed: {e}"

        return True, f"Resync triggered for '{table_name}' (signal {signal_id} via {method})"


    def remove_tables(self, table_names: list, target_action: str = 'truncate', allow_empty: bool = False) -> Tuple[bool, str, list]:
        """
        Remove tables from a connector with full cleanup.

        This will:
        1. Delete Kafka topics for the removed tables
        2. Truncate or drop target tables in target database
        3. Disable table mappings in database
        4. Update source connector config  (skipped when allow_empty=True and no tables remain)
        5. Restart source connector        (skipped when allow_empty=True and no tables remain)
        6. Restart sink connector

        Args:
            table_names: List of source table names to remove
            target_action: 'truncate' (keep structure, clear data) or 'drop' (remove table entirely)

        Returns:
            (success, message, steps) where steps is a list of
            {'id': str, 'status': 'ok'|'warn'|'error', 'msg': str} dicts
        """
        self._log_info("=" * 60)
        self._log_info("REMOVING TABLES FROM CONNECTOR")
        self._log_info(f"Tables to remove: {', '.join(table_names)}")
        self._log_info("=" * 60)

        steps = []

        try:
            db_config = self.config.client_database
            client = db_config.client

            # Get table mappings for the tables being removed
            table_mappings_to_remove = self.config.table_mappings.filter(
                source_table__in=table_names,
                is_enabled=True
            )

            if not table_mappings_to_remove.exists():
                return False, "No matching enabled tables found to remove", steps

            # ========================================
            # STEP 1: Delete Kafka topics for removed tables
            # ========================================
            self._log_info("STEP 1/5: Deleting Kafka topics for removed tables...")

            topic_prefix = self.config.kafka_topic_prefix
            topics_to_delete = [
                format_topic_name(
                    db_config=db_config,
                    table_name=tm.source_table,
                    topic_prefix=topic_prefix,
                    schema=tm.source_schema
                )
                for tm in table_mappings_to_remove
            ]

            deleted_topics = []
            for topic in topics_to_delete:
                topic_deleted = False
                try:
                    topic_ok, topic_err = self.topic_manager.delete_topic(topic)
                    if topic_ok:
                        topic_deleted = True
                        deleted_topics.append(topic)
                        self._log_info(f"  ✓ Deleted topic: {topic}")
                    else:
                        self._log_warning(f"  ⚠️ Failed to delete topic {topic}: {topic_err}")
                except Exception as e:
                    self._log_warning(f"  ⚠️ Error deleting topic {topic}: {e}")

                # Delete schema subjects in the correct order: hard-delete ONLY when the
                # topic is gone (no retained message can reference the ids any more). If the
                # topic survived, soft-delete so any still-retained messages stay
                # deserializable — a hard delete there orphans them and fails the sink with
                # 40403 "Schema not found".
                try:
                    delete_schema_subject(f"{topic}-key", permanent=topic_deleted)
                    delete_schema_subject(f"{topic}-value", permanent=topic_deleted)
                    mode = "hard" if topic_deleted else "soft"
                    self._log_info(f"  ✓ Deleted schema subjects ({mode}) for: {topic}")
                except Exception as e:
                    self._log_warning(f"  ⚠️ Error deleting schema subjects for {topic}: {e}")

            self._log_info(f"✓ Deleted {len(deleted_topics)}/{len(topics_to_delete)} topics")
            steps.append({
                'id': 'topics',
                'status': 'ok' if len(deleted_topics) == len(topics_to_delete) else 'warn',
                'msg': f"{len(deleted_topics)}/{len(topics_to_delete)} Kafka topic(s) deleted",
            })

            # ========================================
            # STEP 2: Truncate or drop target tables
            # ========================================
            action_word = 'dropped' if target_action == 'drop' else 'truncated'
            self._log_info(f"STEP 2/5: {action_word.capitalize()} target database tables...")

            from client.models.database import ClientDatabase
            target_db = ClientDatabase.objects.filter(client=client, is_target=True).first()

            if target_db:
                if target_action == 'drop':
                    from client.utils.table_creator import drop_tables_for_mappings
                    tbl_ok, tbl_msg = drop_tables_for_mappings(target_db, table_mappings_to_remove)
                else:
                    tbl_ok, tbl_msg = truncate_tables_for_mappings(target_db, table_mappings_to_remove)

                if tbl_ok:
                    self._log_info(f"✓ {tbl_msg}")
                else:
                    self._log_warning(f"⚠️ {tbl_msg}")
                steps.append({
                    'id': 'target_tbl',
                    'status': 'ok' if tbl_ok else 'warn',
                    'msg': f"Target table(s) {action_word}" + (f" — {tbl_msg}" if not tbl_ok else ""),
                })
            else:
                self._log_warning("⚠️ No target database configured - skipping table truncate")
                steps.append({
                    'id': 'target_tbl',
                    'status': 'warn',
                    'msg': "No target database configured — skipped",
                })

            # ========================================
            # STEP 3: Disable table mappings in database
            # ========================================
            self._log_info("STEP 3/5: Disabling table mappings...")

            disabled_count = table_mappings_to_remove.update(is_enabled=False)
            self._log_info(f"✓ Disabled {disabled_count} table mappings")
            steps.append({
                'id': 'mappings',
                'status': 'ok',
                'msg': f"{disabled_count} table mapping(s) disabled",
            })

            # Check if any tables remain
            remaining_mappings = self.config.table_mappings.filter(is_enabled=True)
            if db_config.db_type == 'oracle':
                remaining_tables = [
                    f"{tm.source_schema}.{tm.source_table}" if tm.source_schema else tm.source_table
                    for tm in remaining_mappings
                ]
            else:
                remaining_tables = list(remaining_mappings.values_list('source_table', flat=True))

            if not remaining_tables:
                if not allow_empty:
                    return False, "Cannot remove all tables. Delete the connector instead.", steps
                # Caller intends to add tables immediately after (e.g. simultaneous add+remove).
                # Source connector update is deferred to add_tables (avoids empty table.include.list).
                # BUT we must still cycle the sink NOW — stale consumer group offsets for the
                # deleted topic-partitions cause an endless rebalance loop if not cleared here.
                self._log_info("No remaining tables — cycling sink to clear stale offsets, deferring source update to add_tables...")
                if self.config.sink_connector_name:
                    stop_ok, stop_err = self.connector_manager.stop_connector(self.config.sink_connector_name)
                    if stop_ok:
                        self._log_info("✓ Sink connector stopped")
                    else:
                        self._log_warning(f"⚠️ Sink stop failed: {stop_err}")

                    offsets_ok, offsets_err = self.connector_manager.delete_connector_offsets(self.config.sink_connector_name)
                    if offsets_ok:
                        self._log_info("✓ Sink consumer group offsets cleared")
                    else:
                        self._log_warning(f"⚠️ Could not clear sink offsets: {offsets_err}")

                    resume_ok, resume_err = self.connector_manager.resume_connector(self.config.sink_connector_name)
                    if resume_ok:
                        self._log_info("✓ Sink connector resumed")
                    else:
                        self._log_warning(f"⚠️ Sink resume failed: {resume_err}")

                    steps.append({
                        'id': 'sink_restart',
                        'status': 'ok' if (stop_ok and resume_ok) else 'warn',
                        'msg': 'Sink cycled (stop → clear offsets → resume)' if (stop_ok and resume_ok) else f"Sink cycle warning: {stop_err or resume_err}",
                    })

                action_past = 'dropped' if target_action == 'drop' else 'truncated'
                return True, f"Removed {len(table_names)} table(s) (topics deleted, target tables {action_past}) — source connector update deferred", steps

            # ========================================
            # STEP 4: Update and restart source connector
            # ========================================
            self._log_info("STEP 4/6: Updating source connector configuration...")

            # Ensure the connector is running before reconfiguring (resume if a user
            # manually paused it).
            self._resume_if_paused()

            source_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=remaining_tables
            )

            cfg_ok, cfg_err = self.connector_manager.update_connector_config(
                self.config.connector_name,
                source_config
            )
            if not cfg_ok:
                steps.append({
                    'id': 'src_config',
                    'status': 'error',
                    'msg': f"Failed to update source connector: {cfg_err}",
                })
                return False, f"Failed to update source connector: {cfg_err}", steps

            self._log_info(f"✓ Source connector updated with {len(remaining_tables)} tables")
            steps.append({
                'id': 'src_config',
                'status': 'ok',
                'msg': f"Source connector updated ({len(remaining_tables)} table(s) remaining)",
            })

            # Restart source connector
            src_restart_ok, src_restart_err = self.connector_manager.restart_connector(self.config.connector_name)
            if src_restart_ok:
                self._log_info(f"✓ Source connector restarted")
            else:
                self._log_warning(f"⚠️ Source connector restart failed: {src_restart_err}")
            steps.append({
                'id': 'src_restart',
                'status': 'ok' if src_restart_ok else 'warn',
                'msg': 'Source connector restarted' if src_restart_ok else f"Source connector restart warning: {src_restart_err}",
            })

            # ========================================
            # STEP 5: Cycle sink connector (stop → resume)
            # ========================================
            # A plain restart leaves the consumer as a group member, so it can
            # still try to commit in-flight offsets for the now-deleted topic
            # partitions. stop() evicts the consumer from the group entirely;
            # on resume Kafka re-assigns only partitions for topics that exist.
            self._log_info("STEP 5/6: Cycling sink connector (stop → resume)...")

            if self.config.sink_connector_name:
                # Stop (not pause) so the consumer leaves the group entirely
                stop_ok, stop_err = self.connector_manager.stop_connector(
                    self.config.sink_connector_name
                )
                if stop_ok:
                    self._log_info(f"✓ Sink connector stopped")
                else:
                    self._log_warning(f"⚠️ Sink connector stop failed: {stop_err}")

                # Purge stale consumer group offsets for the deleted topic-partitions.
                # Without this, the consumer re-inherits the deleted partition's offset
                # record on every rejoin and enters an endless rebalance loop.
                offsets_ok, offsets_err = self.connector_manager.delete_connector_offsets(
                    self.config.sink_connector_name
                )
                if offsets_ok:
                    self._log_info(f"✓ Sink consumer group offsets cleared")
                else:
                    self._log_warning(f"⚠️ Could not clear sink offsets: {offsets_err}")

                resume_ok, resume_err = self.connector_manager.resume_connector(
                    self.config.sink_connector_name
                )
                sink_ok = stop_ok and resume_ok
                if resume_ok:
                    self._log_info(f"✓ Sink connector resumed")
                else:
                    self._log_warning(f"⚠️ Sink connector resume failed: {resume_err}")

                steps.append({
                    'id': 'sink_restart',
                    'status': 'ok' if sink_ok else 'warn',
                    'msg': 'Sink connector cycled (stop → clear offsets → resume)' if sink_ok else f"Sink connector cycle warning: {stop_err or resume_err}",
                })

            self._log_info("=" * 60)
            self._log_info("✓ TABLES REMOVED SUCCESSFULLY")
            self._log_info("=" * 60)

            action_past = 'dropped' if target_action == 'drop' else 'truncated'
            return True, f"Removed {len(table_names)} table(s) (topics deleted, target tables {action_past})", steps

        except Exception as e:
            error_msg = f"Failed to remove tables: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg, steps


    def add_tables(self, table_names: list, target_table_names: dict = None) -> Tuple[bool, str, list]:
        """
        Add tables to a connector with incremental snapshot.

        This will:
        1. Create/re-enable table mappings (all columns enabled)
        2. Create Kafka topics for new tables
        3. Update source connector config
        4. Send incremental snapshot signal (db-based or Kafka-based)
        5. Restart sink connector to pick up new topics

        Args:
            table_names: List of source table names to add

        Returns:
            (success, message)
        """
        self._log_info("=" * 60)
        self._log_info("ADDING TABLES TO CONNECTOR")
        self._log_info(f"Tables to add: {', '.join(table_names)}")
        self._log_info("=" * 60)

        steps = []
        db_config = self.config.client_database
        client = db_config.client
        added_tables = []
        failed_tables = []

        try:
            # ========================================
            # STEP 1: Create/re-enable table mappings
            # ========================================
            self._log_info("STEP 1/6: Creating table mappings...")

            from client.models.replication import TableMapping
            from client.utils.database_utils import get_table_schema

            for table_name in table_names:
                try:
                    schema = get_table_schema(db_config, table_name)
                    columns = schema.get('columns', [])
                    primary_keys = schema.get('primary_keys', [])
                    row_count = schema.get('row_count', -1)

                    for col in columns:
                        col['primary_key'] = col['name'] in primary_keys

                    # Parse schema and table name
                    if (db_config.db_type in ['mssql', 'oracle']) and '.' in table_name:
                        source_schema, actual_table_name = table_name.split('.', 1)
                    elif db_config.db_type == 'postgresql':
                        source_schema = 'public'
                        actual_table_name = table_name
                    else:
                        source_schema = schema.get('schema', '')
                        actual_table_name = table_name

                    # Build target table name — use custom name from caller if provided,
                    # otherwise compute the default to match sink connector transform
                    # ($1_$2: {schema}_{table}).
                    custom_name = (target_table_names or {}).get(table_name, '')
                    if custom_name:
                        target_table_name = custom_name
                    elif db_config.db_type == 'mysql':
                        target_table_name = f"{db_config.database_name}_{actual_table_name}"
                    elif db_config.db_type == 'postgresql':
                        target_table_name = f"{db_config.database_name}_{actual_table_name}"
                    elif db_config.db_type == 'mssql':
                        target_table_name = f"{db_config.database_name}_{actual_table_name}"
                    elif db_config.db_type == 'oracle':
                        oracle_schema = source_schema or db_config.username.upper()
                        target_table_name = f"{oracle_schema}_{actual_table_name}"
                    else:
                        target_table_name = actual_table_name

                    self._log_info(f"  → Target table name: {target_table_name}")

                    # Check for existing disabled mapping
                    existing_mapping = TableMapping.objects.filter(
                        replication_config=self.config,
                        source_table=actual_table_name,
                        is_enabled=False
                    ).first()

                    if existing_mapping:
                        existing_mapping.is_enabled = True
                        existing_mapping.source_schema = source_schema
                        existing_mapping.target_table = target_table_name
                        existing_mapping.save()
                        table_mapping = existing_mapping
                        self._log_info(f"  ✓ Re-enabled mapping: {table_name} → {target_table_name}")
                    else:
                        table_mapping = TableMapping.objects.create(
                            replication_config=self.config,
                            source_table=actual_table_name,
                            target_table=target_table_name,
                            source_schema=source_schema,
                            is_enabled=True,
                        )
                        self._log_info(f"  ✓ Created mapping: {table_name} → {target_table_name}")

                    added_tables.append(table_name)

                except Exception as e:
                    self._log_warning(f"  ⚠️ Failed to create mapping for {table_name}: {e}")
                    failed_tables.append(table_name)

            if not added_tables:
                return False, "No tables could be added", steps

            self._log_info(f"✓ Created mappings for {len(added_tables)} tables")
            steps.append({
                'id': 'mappings',
                'status': 'ok' if not failed_tables else 'warn',
                'msg': (
                    f"{len(added_tables)} table mapping(s) created"
                    + (f" ({len(failed_tables)} failed: {', '.join(failed_tables)})" if failed_tables else "")
                ),
            })

            # ========================================
            # STEP 2: Create Kafka topics
            # ========================================
            self._log_info("STEP 2/6: Creating Kafka topics...")

            topics_ok, topics_msg = self.create_topics()
            if topics_ok:
                self._log_info(f"✓ {topics_msg}")
            else:
                self._log_warning(f"⚠️ Topic creation warning: {topics_msg}")
            steps.append({
                'id': 'topics',
                'status': 'ok' if topics_ok else 'warn',
                'msg': topics_msg or "Kafka topics created",
            })

            # ========================================
            # STEP 3: Update source connector config
            # ========================================
            self._log_info("STEP 3/6: Updating source connector...")

            # Ensure the connector is running before reconfiguring (resume if a user
            # manually paused it).
            self._resume_if_paused()

            enabled_mappings = self.config.table_mappings.filter(is_enabled=True)
            if db_config.db_type == 'oracle':
                all_tables = [
                    f"{tm.source_schema}.{tm.source_table}" if tm.source_schema else tm.source_table
                    for tm in enabled_mappings
                ]
            else:
                all_tables = list(enabled_mappings.values_list('source_table', flat=True))

            # Guard: never send an empty table list to the connector template.
            # An empty tables_whitelist causes the template to omit table.include.list
            # entirely, making Debezium capture every table in the database.
            if not all_tables:
                steps.append({
                    'id': 'src_config',
                    'status': 'error',
                    'msg': "No enabled table mappings found — cannot update connector with empty table list",
                })
                return False, "No enabled table mappings found — cannot update connector with empty table list", steps

            source_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=self.config,
                tables_whitelist=all_tables
            )

            add_cfg_ok, add_cfg_err = self.connector_manager.update_connector_config(
                self.config.connector_name,
                source_config
            )
            if not add_cfg_ok:
                steps.append({
                    'id': 'src_config',
                    'status': 'error',
                    'msg': f"Failed to update source connector: {add_cfg_err}",
                })
                return False, f"Failed to update source connector: {add_cfg_err}", steps

            self._log_info(f"✓ Source connector config updated with {len(all_tables)} tables")

            # ── PostgreSQL only: update publication ──────────────────────────
            # publication.autocreate.mode = "filtered" creates the publication on
            # first run but never modifies it afterward.  New tables must be added
            # explicitly with ALTER PUBLICATION ... ADD TABLE before the connector
            # restarts, otherwise Debezium never replicates them.
            if db_config.db_type == 'postgresql':
                self._ensure_postgres_publication_tables(db_config, added_tables)

            # Restart the connector so it reloads its table.include.list and
            # replays the schema history from Kafka for the new table(s).
            # We must NOT send the snapshot signal until schema replay is done —
            # doing so causes a NullPointerException in Debezium because the
            # in-memory schema cache doesn't yet know about the new table.
            self._log_info("  → Restarting connector to reload schema history...")
            self.connector_manager.restart_connector(self.config.connector_name)

            # Wait until the streaming MBean reports Connected=True.
            # That is the authoritative signal that schema history replay is
            # complete and the binlog connection is live — no hardcoded sleeps.
            self._log_info("  → Waiting for connector to finish schema history replay...")
            connector_ready = self._wait_for_connector_streaming(timeout=60)
            if connector_ready:
                self._log_info("✓ Connector streaming — schema ready for snapshot signal")
            else:
                # Check if at least RUNNING before giving up
                exists, status_data = self.connector_manager.get_connector_status(self.config.connector_name)
                state = (status_data or {}).get('connector', {}).get('state', '') if exists else ''
                if state == 'FAILED':
                    steps.append({
                        'id': 'src_config',
                        'status': 'error',
                        'msg': "Connector failed after config update",
                    })
                    return False, "Connector failed after config update", steps
                self._log_warning("⚠️ Streaming MBean not yet Connected — proceeding anyway")

            steps.append({
                'id': 'src_config',
                'status': 'ok' if connector_ready else 'warn',
                'msg': (
                    f"Source connector updated ({len(all_tables)} table(s)), schema ready"
                    if connector_ready else
                    f"Source connector updated ({len(all_tables)} table(s)) — schema readiness timed out"
                ),
            })

            # ========================================
            # STEP 4: Update sink connector BEFORE snapshot signal
            # ========================================
            # CRITICAL ORDER: sink config must be updated (with custom table rename
            # transforms) BEFORE the snapshot signal is sent.  The sink's topics.regex
            # already subscribes to the new topic, so snapshot records start arriving
            # the moment the signal fires.  If the sink still has the old config at that
            # point, extractTableName routes every record to the wrong table (e.g.
            # kbbio_busy_acc_greenera instead of kbbio_busy_acc_greenera_tst) and the
            # custom target table is never created.
            self._log_info("STEP 4/6: Updating sink connector (before snapshot signal)...")

            if self.config.sink_connector_name:
                target_db = client.get_target_database()
                if target_db:
                    sink_ok, sink_err = self._update_sink_connector(
                        self.config.sink_connector_name,
                        set(),  # topics unused — sink uses topics.regex
                        target_db
                    )
                    if sink_ok:
                        self._log_info("✓ Sink connector config updated with new table transforms")
                        steps.append({
                            'id': 'sink',
                            'status': 'ok',
                            'msg': "Sink connector updated with new table transforms",
                        })
                    else:
                        self._log_warning(f"⚠️ Sink connector update failed: {sink_err}")
                        steps.append({
                            'id': 'sink',
                            'status': 'warn',
                            'msg': f"Sink connector update failed: {sink_err}",
                        })
                else:
                    self.connector_manager.restart_connector(self.config.sink_connector_name)
                    self._log_info("✓ Sink connector restarted (uses topics.regex for auto-subscription)")
                    steps.append({
                        'id': 'sink',
                        'status': 'ok',
                        'msg': "Sink connector restarted",
                    })

            # ========================================
            # STEP 5: Send incremental snapshot signal
            # ========================================
            self._log_info("STEP 5/6: Sending incremental snapshot signal...")

            from jovoclient.utils.kafka.signal import send_incremental_snapshot_signal

            signal_id, method = send_incremental_snapshot_signal(
                database=db_config,
                replication_config=self.config,
                tables=added_tables
            )

            self._log_info(f"✓ Signal sent via {method} - ID: {signal_id}")
            steps.append({
                'id': 'signal_sent',
                'status': 'ok',
                'msg': f"Snapshot signal sent via {method} (ID: {signal_id})",
            })

            # Verify incremental snapshot started via Jolokia.
            # grace_seconds is reduced from 10→3 because we already waited for
            # the connector to be streaming before sending the signal.
            recv_result = self._verify_incremental_snapshot_started(signal_id, grace_seconds=3)
            steps.append({
                'id': 'signal_recv',
                'status': 'ok' if recv_result['confirmed'] else 'warn',
                'msg': recv_result['msg'],
            })

            # ========================================
            # STEP 5.5: Add foreign keys + indexes to new target tables
            # ========================================
            self._log_info("STEP 5.5/6: Adding foreign keys and indexes to new target tables...")
            from client.utils.table_creator import add_foreign_keys_to_target, add_indexes_to_target
            try:
                # Wait a moment for sink to create the tables
                import time
                time.sleep(5)
                created, skipped, errors = add_foreign_keys_to_target(self.config, specific_tables=added_tables)
                self._log_info(f"✓ Foreign keys: {created} created, {skipped} skipped")
                if errors:
                    self._log_warning(f"⚠️ FK errors: {errors}")
                idx_created, idx_skipped, idx_errors = add_indexes_to_target(self.config, specific_tables=added_tables)
                self._log_info(f"✓ Indexes: {idx_created} created, {idx_skipped} skipped")
                if idx_errors:
                    self._log_warning(f"⚠️ Index errors: {idx_errors}")
                steps.append({
                    'id': 'fk',
                    'status': 'ok' if not errors and not idx_errors else 'warn',
                    'msg': (
                        f"Foreign keys: {created} created, {skipped} skipped"
                        + (f" ({len(errors)} error(s))" if errors else "")
                        + f" | Indexes: {idx_created} created, {idx_skipped} skipped"
                        + (f" ({len(idx_errors)} error(s))" if idx_errors else "")
                    ),
                })
            except Exception as e:
                self._log_warning(f"⚠️ Could not add foreign keys: {e}")
                steps.append({
                    'id': 'fk',
                    'status': 'warn',
                    'msg': f"Foreign key setup skipped: {e}",
                })

            # The source connector stays RUNNING so the incremental snapshot can proceed.
            self.config.connector_state = 'RUNNING'
            self.config.save()

            self._log_info("=" * 60)
            self._log_info("✓ TABLES ADDED SUCCESSFULLY")
            self._log_info("=" * 60)

            result_msg = f"Added {len(added_tables)} table(s) via {method} signal (ID: {signal_id})"
            if failed_tables:
                result_msg += f" | {len(failed_tables)} failed: {', '.join(failed_tables)}"

            return True, result_msg, steps

        except Exception as e:
            error_msg = f"Failed to add tables: {str(e)}"
            self._log_error(error_msg)
            return False, error_msg, steps


    def _apply_pending_truncates(self) -> None:
        """
        Apply any TRUNCATE+resync operations that were deferred while a connector
        was paused.  Called by resume_connector() and resume_sink_connector().

        A deferred TRUNCATE truncates the target table directly and fires a
        re-snapshot that flows through source → Kafka → sink, so it must only run
        once BOTH the source and sink connectors are active.  If either is still
        paused the queue is left intact and drained on the next resume.
        """
        from client.models.replication import ReplicationConfig
        cfg = ReplicationConfig.objects.only(
            'pending_truncates', 'connector_state', 'sink_connector_state'
        ).get(pk=self.config.pk)
        pending = list(cfg.pending_truncates or [])
        if not pending:
            return

        if cfg.connector_state == 'PAUSED' or cfg.sink_connector_state == 'PAUSED':
            self._log_info(
                f"Deferred TRUNCATE(s) kept queued — pipeline still paused "
                f"(source={cfg.connector_state}, sink={cfg.sink_connector_state}): {pending}"
            )
            return

        self._log_info(f"Applying {len(pending)} deferred TRUNCATE(s) after resume: {pending}")

        from client.utils.ddl.base_processor import _apply_truncate_and_snapshot
        applied = []
        for table_name in pending:
            try:
                ok = _apply_truncate_and_snapshot(self.config, table_name)
                if ok:
                    applied.append(table_name)
                else:
                    self._log_warning(f"  ✗ Deferred TRUNCATE failed for '{table_name}' — will retry on next resume")
            except Exception as e:
                self._log_error(f"  ✗ Deferred TRUNCATE error for '{table_name}': {e}")

        remaining = [t for t in pending if t not in applied]
        ReplicationConfig.objects.filter(pk=self.config.pk).update(pending_truncates=remaining)
        self.config.pending_truncates = remaining

        if applied:
            self._log_info(f"  ✓ Deferred TRUNCATEs applied: {applied}")
