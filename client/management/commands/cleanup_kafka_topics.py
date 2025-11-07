"""
Management command to clean up orphaned Kafka topics
"""

from django.core.management.base import BaseCommand
from client.models.client import Client
from client.models.database import ClientDatabase
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException


class Command(BaseCommand):
    help = 'Clean up orphaned Kafka topics that no longer have corresponding ClientDatabase entries'

    def add_arguments(self, parser):
        parser.add_argument(
            '--client-id',
            type=int,
            help='Clean topics for specific client only',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Skip confirmation prompt',
        )
        parser.add_argument(
            '--delete-topic',
            type=str,
            help='Delete a specific topic by name',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('\n' + '='*80))
        self.stdout.write(self.style.SUCCESS('  KAFKA TOPIC CLEANUP'))
        self.stdout.write(self.style.SUCCESS('='*80 + '\n'))

        # Initialize Kafka admin client
        admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
        
        # Get all topics
        try:
            all_topics = admin.list_topics(timeout=10).topics
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Failed to connect to Kafka: {str(e)}'))
            return

        # Filter client topics
        client_topics = sorted([t for t in all_topics if t.startswith('client_') and 'schema-changes' not in t])
        
        if not client_topics:
            self.stdout.write(self.style.WARNING('⚠️  No client topics found'))
            return

        self.stdout.write(f'Found {len(client_topics)} client topics\n')

        # If specific topic deletion requested
        if options['delete_topic']:
            topic_name = options['delete_topic']
            
            if topic_name not in all_topics:
                self.stdout.write(self.style.ERROR(f'❌ Topic "{topic_name}" not found'))
                return
            
            self.stdout.write(f'Deleting topic: {self.style.WARNING(topic_name)}')
            
            if not options['force']:
                confirm = input('Are you sure? (yes/no): ')
                if confirm.lower() != 'yes':
                    self.stdout.write('Cancelled.')
                    return
            
            self._delete_topics(admin, [topic_name])
            return

        # Build map of valid topics based on ClientDatabase entries
        valid_topics = set()
        
        # Get all clients (or specific client)
        if options['client_id']:
            clients = Client.objects.filter(id=options['client_id'])
            if not clients.exists():
                self.stdout.write(self.style.ERROR(f'❌ Client {options["client_id"]} not found'))
                return
        else:
            clients = Client.objects.all()

        self.stdout.write('Building map of valid topics...\n')
        
        for client in clients:
            client_id = client.id
            
            # Get all source databases for this client
            source_databases = ClientDatabase.objects.filter(client_id=client_id)
            
            if not source_databases.exists():
                self.stdout.write(f'Client {client.name} (ID:{client_id}): No source databases')
                continue
            
            self.stdout.write(f'Client {self.style.WARNING(client.name)} (ID:{client_id}):')
            
            for db in source_databases:
                # Expected topic pattern: client_{id}.{database_name}.*
                topic_prefix = f"client_{client_id}.{db.database_name}."
                
                # Find all topics matching this pattern
                matching_topics = [t for t in client_topics if t.startswith(topic_prefix)]
                
                if matching_topics:
                    self.stdout.write(f'  ✅ {db.database_name}: {len(matching_topics)} topics')
                    valid_topics.update(matching_topics)
                else:
                    self.stdout.write(f'  ⚠️  {db.database_name}: No topics yet (connector may be new)')
        
        self.stdout.write(f'\nValid topics: {len(valid_topics)}')
        
        # Find orphaned topics
        orphaned_topics = []
        
        self.stdout.write(self.style.HTTP_INFO('\n' + '━'*80))
        self.stdout.write(self.style.HTTP_INFO('ANALYZING TOPICS'))
        self.stdout.write(self.style.HTTP_INFO('━'*80 + '\n'))
        
        for topic in client_topics:
            # Parse topic: client_3.database_name.table_name
            parts = topic.split('.')
            
            if len(parts) < 3:
                self.stdout.write(f'⚠️  Malformed topic: {topic}')
                continue
            
            client_id_str = parts[0].replace('client_', '')
            try:
                topic_client_id = int(client_id_str)
            except ValueError:
                self.stdout.write(f'⚠️  Invalid client ID in topic: {topic}')
                continue
            
            source_db_name = parts[1]
            table_name = parts[2]
            
            # Check if this topic is valid
            if topic in valid_topics:
                self.stdout.write(f'✅ KEEP: {topic}')
            else:
                # Check why it's orphaned
                reason = self._get_orphan_reason(topic_client_id, source_db_name)
                self.stdout.write(f'❌ DELETE: {topic}')
                self.stdout.write(f'   Reason: {reason}')
                orphaned_topics.append(topic)
        
        # Summary
        self.stdout.write(self.style.HTTP_INFO('\n' + '━'*80))
        self.stdout.write(self.style.HTTP_INFO('SUMMARY'))
        self.stdout.write(self.style.HTTP_INFO('━'*80 + '\n'))
        
        self.stdout.write(f'Total topics: {len(client_topics)}')
        self.stdout.write(f'Valid topics: {self.style.SUCCESS(str(len(valid_topics)))}')
        self.stdout.write(f'Orphaned topics: {self.style.ERROR(str(len(orphaned_topics)))}')
        
        if not orphaned_topics:
            self.stdout.write(self.style.SUCCESS('\n✅ No orphaned topics found!'))
            return
        
        # Show orphaned topics grouped by source DB
        self.stdout.write(f'\nOrphaned topics by source database:')
        orphaned_by_db = {}
        for topic in orphaned_topics:
            parts = topic.split('.')
            if len(parts) >= 2:
                db_name = parts[1]
                if db_name not in orphaned_by_db:
                    orphaned_by_db[db_name] = []
                orphaned_by_db[db_name].append(topic)
        
        for db_name, topics in orphaned_by_db.items():
            self.stdout.write(f'\n  {db_name}: {len(topics)} topics')
            for topic in topics[:5]:  # Show first 5
                self.stdout.write(f'    - {topic}')
            if len(topics) > 5:
                self.stdout.write(f'    ... and {len(topics) - 5} more')
        
        # Dry run mode
        if options['dry_run']:
            self.stdout.write(self.style.WARNING('\n⚠️  DRY RUN MODE - No topics will be deleted'))
            self.stdout.write('\nTo actually delete, run without --dry-run flag')
            return
        
        # Confirmation
        if not options['force']:
            self.stdout.write(self.style.WARNING(f'\n⚠️  You are about to delete {len(orphaned_topics)} topics'))
            self.stdout.write(self.style.ERROR('This action CANNOT be undone!'))
            confirm = input('\nType "DELETE" to confirm: ')
            
            if confirm != 'DELETE':
                self.stdout.write('Cancelled.')
                return
        
        # Delete topics
        self.stdout.write(f'\nDeleting {len(orphaned_topics)} orphaned topics...')
        self._delete_topics(admin, orphaned_topics)
        
        self.stdout.write(self.style.SUCCESS('\n✅ Cleanup complete!'))
        self.stdout.write(self.style.SUCCESS('='*80 + '\n'))

    def _get_orphan_reason(self, client_id, source_db_name):
        """Determine why a topic is orphaned"""
        try:
            client = Client.objects.get(id=client_id)
        except Client.DoesNotExist:
            return f"Client ID {client_id} does not exist"
        
        # Check if database exists for this client
        db_exists = ClientDatabase.objects.filter(
            client_id=client_id,
            database_name=source_db_name
        ).exists()
        
        if not db_exists:
            return f"Database '{source_db_name}' no longer configured for client '{client.name}'"
        
        return "Unknown reason"

    def _delete_topics(self, admin, topics):
        """Delete topics from Kafka"""
        try:
            # Delete topics
            fs = admin.delete_topics(topics, operation_timeout=30)
            
            # Wait for deletion to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    self.stdout.write(self.style.SUCCESS(f'  ✅ Deleted: {topic}'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'  ❌ Failed to delete {topic}: {str(e)}'))
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error deleting topics: {str(e)}'))