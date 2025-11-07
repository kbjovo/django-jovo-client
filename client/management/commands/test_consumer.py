# """
# Management command to test Kafka consumer
# """

# from django.core.management.base import BaseCommand
# from client.models.client import Client
# from client.utils.kafka_consumer import DebeziumCDCConsumer
# from client.utils.database_utils import get_database_engine, create_client_database
# from sqlalchemy import create_engine


# class Command(BaseCommand):
#     help = 'Test Kafka CDC consumer'

#     def add_arguments(self, parser):
#         parser.add_argument(
#             '--client-id',
#             type=int,
#             required=True,
#             help='Client ID to consume data for',
#         )
#         parser.add_argument(
#             '--max-messages',
#             type=int,
#             default=100,
#             help='Maximum messages to consume (default: 100)',
#         )
#         parser.add_argument(
#             '--timeout',
#             type=float,
#             default=5.0,
#             help='Poll timeout in seconds (default: 5.0)',
#         )

#     def handle(self, *args, **options):
#         self.stdout.write(self.style.SUCCESS('\n' + '='*60))
#         self.stdout.write(self.style.SUCCESS('  KAFKA CONSUMER TEST'))
#         self.stdout.write(self.style.SUCCESS('='*60 + '\n'))

#         client_id = options['client_id']
#         max_messages = options['max_messages']
#         timeout = options['timeout']

#         # Get client
#         try:
#             client = Client.objects.get(id=client_id)
#         except Client.DoesNotExist:
#             self.stdout.write(self.style.ERROR(f'❌ Client with ID {client_id} not found'))
#             return

#         self.stdout.write(f'Client: {self.style.WARNING(client.name)}')
#         self.stdout.write(f'Client ID: {client_id}')
        
#         # Get target database from client.db_name
#         target_db_name = client.db_name
        
#         if not target_db_name:
#             self.stdout.write(self.style.ERROR('❌ Client has no db_name configured'))
#             self.stdout.write('Please set client.db_name field')
#             return
        
#         self.stdout.write(f'Target Database: {target_db_name}')
#         self.stdout.write(f'(From client.db_name field)\n')

#         # Verify database exists (should exist from Client signal)
#         self.stdout.write('Verifying target database exists...')
#         from django.db import connection
#         with connection.cursor() as cursor:
#             cursor.execute(
#                 "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s",
#                 [target_db_name]
#             )
#             if cursor.fetchone():
#                 self.stdout.write(self.style.SUCCESS(f'✅ Database {target_db_name} exists'))
#             else:
#                 self.stdout.write(self.style.WARNING(f'⚠️  Database {target_db_name} does not exist, creating...'))
#                 try:
#                     cursor.execute(f"CREATE DATABASE `{target_db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
#                     self.stdout.write(self.style.SUCCESS(f'✅ Created database {target_db_name}'))
#                 except Exception as e:
#                     self.stdout.write(self.style.ERROR(f'❌ Failed to create database: {str(e)}'))
#                     return

#         # Create SQLAlchemy engine for target database
#         from django.conf import settings
#         connection_string = (
#             f"mysql+pymysql://{settings.DATABASES['default']['USER']}:"
#             f"{settings.DATABASES['default']['PASSWORD']}@"
#             f"{settings.DATABASES['default']['HOST']}:"
#             f"{settings.DATABASES['default']['PORT']}/"
#             f"{target_db_name}"
#         )
        
#         self.stdout.write(f'\nConnecting to target database...')
#         try:
#             target_engine = create_engine(
#                 connection_string, 
#                 pool_pre_ping=True,
#                 echo=False
#             )
#             # Test connection with proper SQLAlchemy syntax
#             from sqlalchemy import text
#             with target_engine.connect() as conn:
#                 result = conn.execute(text("SELECT 1"))
#                 result.fetchone()
#             self.stdout.write(self.style.SUCCESS('✅ Connected to target database'))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f'❌ Failed to connect: {str(e)}'))
#             return

#         # Define topics to consume
#         # Topics follow pattern: client_{id}.{source_db}.{table}
#         # We need to consume ALL topics for this client across all their source databases
        
#         self.stdout.write('\nDiscovering source databases for client...')
#         from client.models.database import ClientDatabase
#         source_databases = ClientDatabase.objects.filter(client_id=client_id)
        
#         if not source_databases.exists():
#             self.stdout.write(self.style.WARNING(f'⚠️  No source databases configured for this client'))
#             self.stdout.write('Add ClientDatabase entries to start replication')
#             return
        
#         self.stdout.write(self.style.SUCCESS(f'✅ Found {source_databases.count()} source database(s):'))
#         for db in source_databases:
#             self.stdout.write(f'   - {db.connection_name} ({db.host}:{db.port}/{db.database_name})')
        
#         self.stdout.write('\nDiscovering Kafka topics...')
#         from confluent_kafka.admin import AdminClient
#         admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
        
#         all_topics = admin.list_topics(timeout=10).topics
        
#         # Find all topics for this client (from any source database)
#         # Pattern: client_{id}.{any_db}.{any_table}
#         client_topics = [
#             topic for topic in all_topics 
#             if topic.startswith(f'client_{client_id}.') and 'schema-changes' not in topic
#         ]
        
#         if not client_topics:
#             self.stdout.write(self.style.WARNING(f'⚠️  No Kafka topics found for client_{client_id}'))
#             self.stdout.write('\nPossible reasons:')
#             self.stdout.write('  1. Debezium connector not created yet')
#             self.stdout.write('  2. Source databases have no tables')
#             self.stdout.write('  3. Initial snapshot not completed')
#             self.stdout.write('\nCreate a connector with:')
#             self.stdout.write(f'  python manage.py test_debezium --create --db-id <SOURCE_DB_ID>')
#             return
        
#         self.stdout.write(self.style.SUCCESS(f'✅ Found {len(client_topics)} Kafka topic(s):'))
        
#         # Group topics by source database for better display
#         topics_by_db = {}
#         for topic in client_topics:
#             # Parse topic: client_3.kbe.users -> database: kbe, table: users
#             parts = topic.split('.')
#             if len(parts) >= 3:
#                 source_db = parts[1]
#                 table = parts[2]
#                 if source_db not in topics_by_db:
#                     topics_by_db[source_db] = []
#                 topics_by_db[source_db].append(table)
        
#         for db_name, tables in topics_by_db.items():
#             self.stdout.write(f'\n   Source DB: {db_name}')
#             for table in tables:
#                 self.stdout.write(f'     └─ {table}')
        
#         self.stdout.write(f'\n   All data will be replicated to: {self.style.WARNING(target_db_name)}')


#         # Create consumer
#         consumer_group_id = f"client_{client_id}_consumer"
        
#         self.stdout.write(f'\nInitializing consumer...')
#         self.stdout.write(f'Consumer Group: {consumer_group_id}')
#         self.stdout.write(f'Max Messages: {max_messages}')
#         self.stdout.write(f'Timeout: {timeout}s\n')
        
#         try:
#             consumer = DebeziumCDCConsumer(
#                 consumer_group_id=consumer_group_id,
#                 topics=client_topics,
#                 target_engine=target_engine,
#                 bootstrap_servers='localhost:9092',
#                 auto_offset_reset='earliest',  # Start from beginning
#             )
            
#             self.stdout.write(self.style.SUCCESS('✅ Consumer initialized'))
            
#             # Start consuming
#             self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
#             self.stdout.write(self.style.HTTP_INFO('Starting consumption...'))
#             self.stdout.write(self.style.HTTP_INFO('━'*60))
#             self.stdout.write('Press Ctrl+C to stop\n')
            
#             # Consume messages
#             consumer.consume(max_messages=max_messages, timeout=timeout)
            
#             # Get stats
#             stats = consumer.get_stats()
            
#             self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
#             self.stdout.write(self.style.HTTP_INFO('CONSUMPTION COMPLETE'))
#             self.stdout.write(self.style.HTTP_INFO('━'*60))
            
#             self.stdout.write(f'\nStatistics:')
#             self.stdout.write(f'  Messages Processed: {stats["messages_processed"]}')
#             self.stdout.write(f'  Inserts: {self.style.SUCCESS(str(stats["inserts"]))}')
#             self.stdout.write(f'  Updates: {self.style.WARNING(str(stats["updates"]))}')
#             self.stdout.write(f'  Deletes: {self.style.ERROR(str(stats["deletes"]))}')
#             self.stdout.write(f'  Errors: {stats["errors"]}')
            
#             if stats['last_message_time']:
#                 self.stdout.write(f'  Last Message: {stats["last_message_time"]}')
            
#             # Check target database
#             self.stdout.write(f'\nVerifying target database: {self.style.WARNING(target_db_name)}')
#             from sqlalchemy import inspect, text
#             inspector = inspect(target_engine)
#             tables = inspector.get_table_names()
            
#             if tables:
#                 self.stdout.write(self.style.SUCCESS(f'✅ Replicated {len(tables)} table(s):'))
#                 for table in tables:
#                     # Count rows
#                     with target_engine.connect() as conn:
#                         result = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`"))
#                         count = result.fetchone()[0]
#                     self.stdout.write(f'   - {table}: {count} rows')
#             else:
#                 self.stdout.write(self.style.WARNING('⚠️  No tables replicated yet'))
            
#         except KeyboardInterrupt:
#             self.stdout.write(self.style.WARNING('\n\n⚠️  Consumption stopped by user'))
#         except Exception as e:
#             self.stdout.write(self.style.ERROR(f'\n❌ Error: {str(e)}'))
#             import traceback
#             self.stdout.write(traceback.format_exc())
#         finally:
#             if 'target_engine' in locals():
#                 target_engine.dispose()
            
#         self.stdout.write(self.style.SUCCESS('\n' + '='*60))
#         self.stdout.write(self.style.SUCCESS('  TEST COMPLETE'))
#         self.stdout.write(self.style.SUCCESS('='*60 + '\n'))


"""
Dynamic Debezium CDC consumer for all tables across all client databases.
"""

import pymysql
import urllib.parse
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, inspect, text
from client.models.client import Client
from client.models.database import ClientDatabase
from client.utils.kafka_consumer import DebeziumCDCConsumer


class Command(BaseCommand):
    help = "Consume Debezium Kafka topics for all tables in all databases of a client"

    def add_arguments(self, parser):
        parser.add_argument("--client-id", type=int, required=True, help="Client ID")
        parser.add_argument("--max-messages", type=int, default=500, help="Max messages to consume per DB")
        parser.add_argument("--timeout", type=float, default=5.0, help="Polling timeout in seconds")

    def handle(self, *args, **options):
        client_id = options["client_id"]
        max_messages = options["max_messages"]
        timeout = options["timeout"]

        self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
        self.stdout.write(self.style.SUCCESS("   DYNAMIC KAFKA CONSUMER TEST"))
        self.stdout.write(self.style.SUCCESS("=" * 60 + "\n"))

        # Load client
        try:
            client = Client.objects.get(id=client_id)
        except Client.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"❌ Client ID {client_id} not found"))
            return

        self.stdout.write(f"Client: {client.name}")
        self.stdout.write(f"Client ID: {client.id}")

        # Target DB connection
        target_db = client.db_name or "client"
        self.stdout.write(f"Target Database: {target_db}")
        self.stdout.write("(From client.db_name field)\n")

        # Connect to target DB
        try:
            target_engine = create_engine(
                f"mysql+pymysql://root:root@localhost:3306/{target_db}",
                pool_recycle=600,
                pool_pre_ping=True,
            )
            with target_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.stdout.write(self.style.SUCCESS(f"✅ Connected to target DB '{target_db}'\n"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Could not connect to target DB: {e}"))
            return

        # Get all client databases
        source_dbs = ClientDatabase.objects.filter(client=client)
        if not source_dbs.exists():
            self.stdout.write(self.style.ERROR("❌ No source databases found for this client"))
            return

        self.stdout.write(self.style.SUCCESS(f"✅ Found {source_dbs.count()} source database(s):"))
        for db in source_dbs:
            self.stdout.write(f"   - {db.connection_name} ({db.host}:{db.port}/{db.database_name})")
        self.stdout.write("\n")

        # Process each source database
        for db in source_dbs:
            self.stdout.write(self.style.HTTP_INFO("━" * 60))
            self.stdout.write(self.style.HTTP_INFO(f"🧩 Starting replication for source DB: {db.database_name}"))
            self.stdout.write(self.style.HTTP_INFO("━" * 60))

            # Step 1: Direct pymysql connection check
            try:
                self.stdout.write(self.style.HTTP_INFO("\n🔍 Checking source DB connection details:"))
                self.stdout.write(f"   Host: {db.host}")
                self.stdout.write(f"   Port: {db.port}")
                self.stdout.write(f"   Database: {db.database_name}")
                self.stdout.write(f"   Username: {db.username}")
                self.stdout.write(f"   Password: {db.password}")

                pymysql.connect(
                    host=db.host,
                    user=db.username,
                    password=db.password,
                    database=db.database_name,
                    port=int(db.port),
                    connect_timeout=5,
                )
                self.stdout.write(self.style.SUCCESS(f"✅ Direct pymysql connection successful to {db.database_name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ pymysql connection failed for {db.database_name}: {e}"))
                continue

            # Step 2: SQLAlchemy connection & table discovery
            try:
                encoded_password = urllib.parse.quote_plus(db.password)
                sqlalchemy_url = f"mysql+pymysql://{db.username}:{encoded_password}@{db.host}:{db.port}/{db.database_name}"
                self.stdout.write(f"🔗 Using SQLAlchemy URL: {sqlalchemy_url}")

                source_engine = create_engine(
                    sqlalchemy_url,
                    pool_recycle=600,
                    pool_pre_ping=True,
                )

                inspector = inspect(source_engine)
                tables = inspector.get_table_names()

                if not tables:
                    self.stdout.write(self.style.WARNING("⚠️  No tables found in source DB"))
                    continue

                self.stdout.write(self.style.SUCCESS(f"✅ Found {len(tables)} tables:"))
                for t in tables:
                    self.stdout.write(f"   - {t}")
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Could not connect to source DB {db.database_name}: {e}"))
                continue

            # Step 3: Build topic list dynamically
            topics = [f"client_{client.id}.{db.database_name}.{table}" for table in tables]
            self.stdout.write(self.style.SUCCESS(f"\nKafka Topics ({len(topics)}):"))
            for t in topics:
                self.stdout.write(f"   └─ {t}")

            # Step 4: Start consumer for all topics of this DB
            try:
                consumer = DebeziumCDCConsumer(
                    consumer_group_id=f"client_{client_id}_consumer_{db.database_name}",
                    topics=topics,
                    target_engine=target_engine,
                    bootstrap_servers="localhost:9092",
                    auto_offset_reset="earliest",
                )

                self.stdout.write(self.style.SUCCESS("✅ Consumer initialized, starting to consume..."))
                consumer.consume(max_messages=max_messages, timeout=timeout)

                stats = consumer.get_stats()
                self.stdout.write(self.style.SUCCESS("✅ Replication complete"))
                self.stdout.write(f"   Messages processed: {stats['messages_processed']}")
                self.stdout.write(f"   Inserts: {stats['inserts']}")
                self.stdout.write(f"   Updates: {stats['updates']}")
                self.stdout.write(f"   Deletes: {stats['deletes']}")
                self.stdout.write(f"   Errors: {stats['errors']}")

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Error replicating {db.database_name}: {e}"))
                continue

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS("  DYNAMIC TEST COMPLETE"))
        self.stdout.write("=" * 60 + "\n")
