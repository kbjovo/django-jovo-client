"""
Rebuild a MySQL connector's Kafka schema-history topic from the live database.

Use this on a connector that is FAILED with:

    io.debezium.DebeziumException: Encountered change event for table <db>.<table>
    whose schema isn't known to this connector

That happens when a table was added to table.include.list but its CREATE TABLE was
never written to the schema-history topic — the case for every connector created
while schema.history.internal.store.only.captured.tables.ddl was "true".

The command runs the same recovery cycle add_tables now performs automatically:
temporarily set snapshot.mode=recovery, restart, let Debezium rebuild history from
the current database while keeping the stored binlog offset, then revert
snapshot.mode.  No connector delete, no re-snapshot of existing data.

Usage:
    python manage.py rebuild_schema_history --config-id 12
    python manage.py rebuild_schema_history --connector-name client_1_db_2_v_3
    python manage.py rebuild_schema_history --list
"""

from django.core.management.base import BaseCommand, CommandError

from client.models.replication import ReplicationConfig
from client.replication.orchestrator import ReplicationOrchestrator


class Command(BaseCommand):
    help = "Rebuild a MySQL connector's schema-history topic from the live database"

    def add_arguments(self, parser):
        parser.add_argument('--config-id', type=int, help='ReplicationConfig primary key')
        parser.add_argument('--connector-name', type=str, help='Debezium connector name')
        parser.add_argument('--list', action='store_true', help='List MySQL replication configs and exit')
        parser.add_argument(
            '--timeout', type=int, default=180,
            help='Seconds to wait for the connector to resume streaming (default: 180)',
        )

    def handle(self, *args, **options):
        if options['list']:
            self._list_configs()
            return

        config = self._resolve_config(options)

        db_type = config.client_database.db_type.lower()
        if db_type != 'mysql':
            raise CommandError(
                f"Config {config.pk} is {db_type}, not mysql — schema history recovery "
                f"only applies to MySQL connectors."
            )

        self.stdout.write(
            f"Rebuilding schema history for '{config.connector_name}' "
            f"(database: {config.client_database.database_name})..."
        )

        orchestrator = ReplicationOrchestrator(config)
        success, message = orchestrator._restart_source_for_new_tables(timeout=options['timeout'])

        if success:
            self.stdout.write(self.style.SUCCESS(f"✓ {message}"))
        else:
            # Not necessarily fatal — the rebuild may still be running. Point at the logs
            # rather than implying it failed outright.
            self.stdout.write(self.style.WARNING(f"⚠ {message}"))
            self.stdout.write(
                "Check connector status and the Connect logs before retrying. "
                "If it is still FAILED on the same unknown-table error, the history topic "
                "may need to be deleted so recovery can recreate it from scratch."
            )

    def _resolve_config(self, options):
        if options['config_id']:
            try:
                return ReplicationConfig.objects.get(pk=options['config_id'])
            except ReplicationConfig.DoesNotExist:
                raise CommandError(f"No ReplicationConfig with id {options['config_id']}")

        if options['connector_name']:
            config = ReplicationConfig.objects.filter(
                connector_name=options['connector_name']
            ).first()
            if not config:
                raise CommandError(f"No ReplicationConfig with connector_name '{options['connector_name']}'")
            return config

        raise CommandError("Pass --config-id or --connector-name (or --list to see them)")

    def _list_configs(self):
        configs = ReplicationConfig.objects.filter(
            client_database__db_type='mysql'
        ).select_related('client_database')

        if not configs:
            self.stdout.write("No MySQL replication configs found.")
            return

        for config in configs:
            self.stdout.write(
                f"  id={config.pk}  {config.connector_name}  "
                f"db={config.client_database.database_name}  status={config.status}"
            )
