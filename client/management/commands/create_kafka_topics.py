"""
Django management command to create Kafka topics for CDC
"""

from django.core.management.base import BaseCommand
from client.utils.kafka_topic_manager import KafkaTopicManager
from client.models import ReplicationConfig


class Command(BaseCommand):
    help = 'Create Kafka topics for CDC with proper configuration'

    def add_arguments(self, parser):
        parser.add_argument(
            '--config-id',
            type=int,
            help='ReplicationConfig ID to create topics for',
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Create topics for all replication configs',
        )
        parser.add_argument(
            '--topics',
            nargs='+',
            help='Manually specify topic names to create',
        )
        parser.add_argument(
            '--list',
            action='store_true',
            help='List existing topics',
        )
        parser.add_argument(
            '--summary',
            action='store_true',
            help='Show topic configuration summary',
        )
        parser.add_argument(
            '--prefix',
            type=str,
            help='Filter topics by prefix when listing',
        )

    def handle(self, *args, **options):
        manager = KafkaTopicManager()

        # List topics
        if options['list']:
            prefix = options.get('prefix')
            topics = manager.list_topics(prefix=prefix)

            self.stdout.write(self.style.SUCCESS(f"\n{'='*60}"))
            self.stdout.write(self.style.SUCCESS(f"Kafka Topics ({len(topics)} total)"))
            self.stdout.write(self.style.SUCCESS(f"{'='*60}\n"))

            for topic in sorted(topics):
                self.stdout.write(f"  • {topic}")

            self.stdout.write("")
            return

        # Show summary
        if options['summary']:
            summary = manager.get_summary()

            self.stdout.write(self.style.SUCCESS(f"\n{'='*60}"))
            self.stdout.write(self.style.SUCCESS("Kafka Topic Configuration Summary"))
            self.stdout.write(self.style.SUCCESS(f"{'='*60}\n"))

            self.stdout.write(f"Bootstrap Servers: {summary['bootstrap_servers']}")
            self.stdout.write(f"Total Topics: {summary['total_topics']}\n")

            self.stdout.write(self.style.WARNING("Default Topic Configuration:"))
            config = summary['default_config']
            self.stdout.write(f"  • Partitions: {config['partitions']}")
            self.stdout.write(f"  • Replication Factor: {config['replication_factor']}")
            self.stdout.write(f"  • Retention: {config['retention_days']:.1f} days ({config['retention_ms']} ms)")
            self.stdout.write(f"  • Retention Bytes: {config['retention_bytes']}")
            self.stdout.write(f"  • Cleanup Policy: {config['cleanup_policy']}")
            self.stdout.write(f"  • Compression: {config['compression_type']}")
            self.stdout.write(f"  • Min In-Sync Replicas: {config['min_insync_replicas']}")
            self.stdout.write("")
            return

        # Create topics manually
        if options['topics']:
            topic_names = options['topics']

            self.stdout.write(self.style.WARNING(f"\nCreating {len(topic_names)} topics...\n"))

            results = manager.create_topics_bulk(topic_names)

            # Show results
            successful = [t for t, success in results.items() if success]
            failed = [t for t, success in results.items() if not success]

            if successful:
                self.stdout.write(self.style.SUCCESS(f"\n✅ Successfully created {len(successful)} topics:"))
                for topic in successful:
                    self.stdout.write(f"  • {topic}")

            if failed:
                self.stdout.write(self.style.ERROR(f"\n❌ Failed to create {len(failed)} topics:"))
                for topic in failed:
                    self.stdout.write(f"  • {topic}")

            self.stdout.write("")
            return

        # Create topics for specific replication config
        if options['config_id']:
            try:
                config = ReplicationConfig.objects.get(id=options['config_id'])
            except ReplicationConfig.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"ReplicationConfig {options['config_id']} not found"))
                return

            self._create_topics_for_config(manager, config)
            return

        # Create topics for all replication configs
        if options['all']:
            configs = ReplicationConfig.objects.all()

            if not configs.exists():
                self.stdout.write(self.style.WARNING("No replication configs found"))
                return

            self.stdout.write(self.style.SUCCESS(f"\nCreating topics for {configs.count()} replication config(s)...\n"))

            for config in configs:
                self._create_topics_for_config(manager, config)
                self.stdout.write("")  # Blank line between configs

            return

        # No options provided
        self.stdout.write(self.style.WARNING("\nNo action specified. Use --help for options.\n"))
        self.stdout.write("Examples:")
        self.stdout.write("  • List topics: python manage.py create_kafka_topics --list")
        self.stdout.write("  • Show summary: python manage.py create_kafka_topics --summary")
        self.stdout.write("  • Create for config: python manage.py create_kafka_topics --config-id 5")
        self.stdout.write("  • Create all: python manage.py create_kafka_topics --all")
        self.stdout.write("  • Manual create: python manage.py create_kafka_topics --topics client_2.kbe.users client_2.kbe.orders")
        self.stdout.write("")

    def _create_topics_for_config(self, manager, config):
        """Helper to create topics for a ReplicationConfig"""
        self.stdout.write(self.style.WARNING(f"Processing: {config.connector_name}"))
        self.stdout.write(f"  Database: {config.source_db.connection_name}")
        self.stdout.write(f"  Server Name: {config.debezium_server_name}")

        # Get table names from table mappings
        table_mappings = config.table_mappings.filter(is_enabled=True)
        table_names = [tm.source_table for tm in table_mappings]

        if not table_names:
            self.stdout.write(self.style.WARNING("  No enabled tables found"))
            return

        self.stdout.write(f"  Tables: {len(table_names)}")

        # Create topics
        results = manager.create_cdc_topics_for_tables(
            server_name=config.debezium_server_name,
            database=config.source_db.database_name,
            table_names=table_names
        )

        # Show results
        successful = [t for t, success in results.items() if success]
        failed = [t for t, success in results.items() if not success]
        already_exist = len(table_names) - len(successful) - len(failed)

        if successful:
            self.stdout.write(self.style.SUCCESS(f"  ✅ Created: {len(successful)} topics"))
        if already_exist > 0:
            self.stdout.write(self.style.WARNING(f"  ⚠️  Already existed: {already_exist} topics"))
        if failed:
            self.stdout.write(self.style.ERROR(f"  ❌ Failed: {len(failed)} topics"))