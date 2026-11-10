"""
Re-render source and sink connector configs from the current templates and push
them to Kafka Connect.

Template changes (connector_templates.py / sink_connector_templates.py) only
affect connectors created afterwards. Use this command to roll a template change
out to existing connectors, e.g. the sink's rewriteBatchedStatements /
foreign_key_checks=0 / flush retries and the MySQL source heartbeat.

Dry run by default: prints, per connector, the keys that would be added, changed
or removed. Nothing is sent to Kafka Connect without --apply.

Usage:
    python manage.py refresh_connector_configs                      # dry run, all active configs
    python manage.py refresh_connector_configs --config-id 12       # dry run, one config
    python manage.py refresh_connector_configs --sink-only --apply  # push sink configs
    python manage.py refresh_connector_configs --config-id 12 --apply

Notes:
  * A config PUT restarts the connector's tasks. Sources resume from their stored
    offsets; sinks resume from their committed consumer offsets. Run it in a quiet
    window.
  * Connectors with an active resync job are skipped (a restart mid-snapshot
    would interrupt it).
  * Model state (connector_state / sink_connector_state, e.g. a user-paused sink)
    is never changed by this command.
"""

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from client.models.replication import ReplicationConfig

_SECRET_HINTS = ('password', 'secret', 'token')


def _mask(key, value):
    if any(h in key.lower() for h in _SECRET_HINTS):
        return '***'
    value = str(value)
    return value if len(value) <= 120 else value[:117] + '...'


def _diff(current, new):
    current = {k: str(v) for k, v in (current or {}).items() if k != 'name'}
    new = {k: str(v) for k, v in (new or {}).items() if k != 'name'}
    added = {k: new[k] for k in new.keys() - current.keys()}
    removed = {k: current[k] for k in current.keys() - new.keys()}
    changed = {k: (current[k], new[k]) for k in new.keys() & current.keys() if current[k] != new[k]}
    return added, removed, changed


class Command(BaseCommand):
    help = "Re-render connector configs from the current templates and (with --apply) push them to Kafka Connect"

    def add_arguments(self, parser):
        parser.add_argument('--config-id', type=int, help='Only this ReplicationConfig')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--sink-only', action='store_true', help='Only sink connectors')
        group.add_argument('--source-only', action='store_true', help='Only source connectors')
        parser.add_argument('--apply', action='store_true', help='Push the new configs (default: dry run)')

    def handle(self, *args, **opts):
        from client.replication.orchestrator import ReplicationOrchestrator
        from client.replication.resync import has_active_job

        qs = ReplicationConfig.objects.select_related('client_database', 'client_database__client')
        if opts['config_id']:
            qs = qs.filter(pk=opts['config_id'])
            if not qs.exists():
                raise CommandError(f"ReplicationConfig {opts['config_id']} not found")
        else:
            qs = qs.filter(is_active=True)

        do_source = not opts['sink_only']
        do_sink = not opts['source_only']
        apply = opts['apply']

        if not apply:
            self.stdout.write(self.style.WARNING("DRY RUN — nothing will be sent to Kafka Connect (use --apply)"))

        totals = {'updated': 0, 'unchanged': 0, 'skipped': 0, 'failed': 0}

        for config in qs:
            label = f"config #{config.pk} ({config.connector_name or 'no connector'})"
            if has_active_job(config.pk):
                self.stdout.write(self.style.WARNING(f"\n{label}: SKIPPED — resync job in progress"))
                totals['skipped'] += 1
                continue

            orchestrator = ReplicationOrchestrator(config)
            self.stdout.write(self.style.MIGRATE_HEADING(f"\n{label}"))

            if do_source and config.connector_name:
                self._handle_connector(
                    orchestrator, config.connector_name, self._source_config(config), apply, totals,
                    before_apply=lambda c=config, o=orchestrator: self._ensure_heartbeat_topic(c, o),
                )
            if do_sink and config.sink_connector_name:
                new_sink = self._sink_config(config, orchestrator)
                if new_sink is None:
                    self.stdout.write("  sink: no target database — skipped")
                    totals['skipped'] += 1
                else:
                    self._handle_connector(orchestrator, config.sink_connector_name, new_sink, apply, totals)

        self.stdout.write(
            f"\nDone: {totals['updated']} {'updated' if apply else 'would change'}, "
            f"{totals['unchanged']} unchanged, {totals['skipped']} skipped, {totals['failed']} failed"
        )

    # ------------------------------------------------------------------

    def _source_config(self, config):
        from jovoclient.utils.debezium.connector_templates import get_connector_config_for_database

        db_config = config.client_database
        mappings = config.table_mappings.filter(is_enabled=True)
        if db_config.db_type == 'oracle':
            tables = [f"{m.source_schema}.{m.source_table}" if m.source_schema else m.source_table for m in mappings]
        else:
            tables = list(mappings.values_list('source_table', flat=True))
        if not tables:
            # Never render a source config without table.include.list (would capture everything).
            return None
        return get_connector_config_for_database(
            db_config=db_config, replication_config=config, tables_whitelist=tables,
        )

    def _sink_config(self, config, orchestrator):
        """Same rendering as SinkConnectorMixin._update_sink_connector, without its model save."""
        from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database

        target_db = config.client_database.client.get_target_database()
        if not target_db:
            return None
        return get_sink_connector_config_for_database(
            db_config=target_db,
            topics=None,
            kafka_bootstrap_servers=settings.DEBEZIUM_CONFIG['KAFKA_INTERNAL_SERVERS'],
            primary_key_fields=orchestrator._get_primary_key_fields(),
            delete_enabled=True,
            custom_config={'name': config.sink_connector_name},
            replication_config=config,
        )

    def _ensure_heartbeat_topic(self, config, orchestrator):
        topic = f"__debezium-heartbeat.{config.kafka_topic_prefix}"
        try:
            if not orchestrator.topic_manager.topic_exists(topic):
                orchestrator.topic_manager.create_topics_bulk([topic])
                self.stdout.write(f"    created heartbeat topic {topic}")
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"    could not verify heartbeat topic {topic}: {e}"))

    def _handle_connector(self, orchestrator, name, new_config, apply, totals, before_apply=None):
        if new_config is None:
            self.stdout.write(f"  {name}: no enabled tables — skipped")
            totals['skipped'] += 1
            return

        current = orchestrator.connector_manager.get_connector_config(name)
        if current is None:
            self.stdout.write(self.style.WARNING(f"  {name}: not found in Kafka Connect — skipped"))
            totals['skipped'] += 1
            return

        added, removed, changed = _diff(current, new_config)
        if not (added or removed or changed):
            self.stdout.write(f"  {name}: up to date")
            totals['unchanged'] += 1
            return

        self.stdout.write(f"  {name}:")
        for k in sorted(added):
            self.stdout.write(self.style.SUCCESS(f"    + {k} = {_mask(k, added[k])}"))
        for k in sorted(changed):
            old, new = changed[k]
            self.stdout.write(self.style.WARNING(f"    ~ {k}: {_mask(k, old)}  →  {_mask(k, new)}"))
        for k in sorted(removed):
            self.stdout.write(self.style.ERROR(f"    - {k} (was {_mask(k, removed[k])})"))

        if not apply:
            totals['updated'] += 1
            return

        if before_apply:
            before_apply()
        ok, err = orchestrator.connector_manager.update_connector_config(name, new_config)
        if ok:
            self.stdout.write(self.style.SUCCESS(f"    ✓ applied"))
            totals['updated'] += 1
        else:
            self.stdout.write(self.style.ERROR(f"    ✗ failed: {err}"))
            totals['failed'] += 1
