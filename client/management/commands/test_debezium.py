# """
# Management command to test Debezium connector operations
# """

# from django.core.management.base import BaseCommand
# from client.models.database import ClientDatabase
# from client.models.client import Client

# from client.utils import (
#     DebeziumConnectorManager,
#     get_connector_config_for_database,
#     generate_connector_name,
#     validate_connector_config,
#     get_snapshot_modes,
# )


# class Command(BaseCommand):
#     help = 'Test Debezium connector manager and operations'

#     def add_arguments(self, parser):
#         parser.add_argument(
#             '--db-id',
#             type=int,
#             help='ClientDatabase ID to create connector for',
#         )
#         parser.add_argument(
#             '--list',
#             action='store_true',
#             help='List all existing connectors',
#         )
#         parser.add_argument(
#             '--status',
#             type=str,
#             help='Get status of a specific connector by name',
#         )
#         parser.add_argument(
#             '--create',
#             action='store_true',
#             help='Create a connector for the specified database',
#         )
#         parser.add_argument(
#             '--delete',
#             type=str,
#             help='Delete a connector by name',
#         )
#         parser.add_argument(
#             '--tables',
#             type=str,
#             help='Comma-separated list of tables to replicate (optional)',
#         )

#     def handle(self, *args, **options):
#         self.stdout.write(self.style.SUCCESS('\n' + '='*60))
#         self.stdout.write(self.style.SUCCESS('  DEBEZIUM CONNECTOR TEST'))
#         self.stdout.write(self.style.SUCCESS('='*60 + '\n'))

#         # Initialize manager
#         manager = DebeziumConnectorManager()
        
#         # Test 1: Health Check
#         self.stdout.write(self.style.HTTP_INFO('━'*60))
#         self.stdout.write(self.style.HTTP_INFO('TEST 1: Kafka Connect Health Check'))
#         self.stdout.write(self.style.HTTP_INFO('━'*60))
        
#         is_healthy, error = manager.check_kafka_connect_health()
        
#         if is_healthy:
#             self.stdout.write(self.style.SUCCESS('✅ Kafka Connect is running and healthy'))
#         else:
#             self.stdout.write(self.style.ERROR(f'❌ Kafka Connect is not healthy: {error}'))
#             self.stdout.write(self.style.WARNING('\nMake sure Docker services are running:'))
#             self.stdout.write('   cd ~/django-jovo-client/debezium-setup')
#             self.stdout.write('   docker-compose up -d')
#             return

#         # Test 2: List Connectors
#         if options['list'] or not any([options.get('status'), options.get('create'), options.get('delete')]):
#             self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
#             self.stdout.write(self.style.HTTP_INFO('TEST 2: List All Connectors'))
#             self.stdout.write(self.style.HTTP_INFO('━'*60))
            
#             connectors = manager.list_connectors()
            
#             if connectors:
#                 self.stdout.write(self.style.SUCCESS(f'✅ Found {len(connectors)} connector(s)'))
#                 for i, connector in enumerate(connectors, 1):
#                     self.stdout.write(f'   {i}. {connector}')
                    
#                     # Get status for each
#                     exists, status = manager.get_connector_status(connector)
#                     if exists and status:
#                         state = status.get('connector', {}).get('state', 'UNKNOWN')
                        
#                         if state == 'RUNNING':
#                             state_display = self.style.SUCCESS(state)
#                         elif state == 'FAILED':
#                             state_display = self.style.ERROR(state)
#                         else:
#                             state_display = self.style.WARNING(state)
                        
#                         self.stdout.write(f'      Status: {state_display}')
#             else:
#                 self.stdout.write(self.style.WARNING('⚠️  No connectors found'))

#         # Test 3: Get Connector Status
#         if options['status']:
#             self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
#             self.stdout.write(self.style.HTTP_INFO(f'TEST 3: Get Status for "{options["status"]}"'))
#             self.stdout.write(self.style.HTTP_INFO('━'*60))
            
#             exists, status = manager.get_connector_status(options['status'])
            
#             if exists and status:
#                 self.stdout.write(self.style.SUCCESS(f'✅ Connector found'))
                
#                 # Connector state
#                 connector_state = status.get('connector', {})
#                 self.stdout.write(f'\n   Connector State: {connector_state.get("state", "UNKNOWN")}')
                
#                 if connector_state.get('worker_id'):
#                     self.stdout.write(f'   Worker ID: {connector_state["worker_id"]}')
                
#                 # Tasks
#                 tasks = status.get('tasks', [])
#                 self.stdout.write(f'\n   Tasks ({len(tasks)}):')
#                 for i, task in enumerate(tasks):
#                     task_id = task.get('id', i)
#                     task_state = task.get('state', 'UNKNOWN')
                    
#                     if task_state == 'RUNNING':
#                         state_display = self.style.SUCCESS(task_state)
#                     elif task_state == 'FAILED':
#                         state_display = self.style.ERROR(task_state)
#                     else:
#                         state_display = self.style.WARNING(task_state)
                    
#                     self.stdout.write(f'   - Task {task_id}: {state_display}')
                    
#                     if task.get('worker_id'):
#                         self.stdout.write(f'     Worker: {task["worker_id"]}')
                    
#                     if task.get('trace'):
#                         self.stdout.write(self.style.ERROR(f'     Error: {task["trace"][:200]}...'))
#             else:
#                 self.stdout.write(self.style.ERROR(f'❌ Connector "{options["status"]}" not found'))

#         # Test 4: Create Connector
#         if options['create']:
#             if not options['db_id']:
#                 self.stdout.write(self.style.ERROR('\n❌ --db-id is required to create a connector'))
#                 return
            
#             self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
#             self.stdout.write(self.style.HTTP_INFO('TEST 4: Create Connector'))
#             self.stdout.write(self.style.HTTP_INFO('━'*60))
            
#             try:
#                 db = ClientDatabase.objects.get(id=options['db_id'])
#                 client = db.client
#             except ClientDatabase.DoesNotExist:
#                 self.stdout.write(self.style.ERROR(f'❌ Database with ID {options["db_id"]} not found'))
#                 return
            
#             self.stdout.write(f'Client: {client.name}')
#             self.stdout.write(f'Database: {db.connection_name} ({db.db_type})')
#             self.stdout.write(f'Host: {db.host}:{db.port}')
#             self.stdout.write(f'DB Name: {db.database_name}\n')
            
#             # Parse tables if provided
#             tables_list = None
#             if options['tables']:
#                 tables_list = [t.strip() for t in options['tables'].split(',')]
#                 self.stdout.write(f'Tables to replicate: {", ".join(tables_list)}\n')
            
#             # Generate connector name
#             connector_name = generate_connector_name(client, db)
#             self.stdout.write(f'Connector Name: {self.style.WARNING(connector_name)}\n')
            
#             # Generate configuration
#             self.stdout.write('Generating connector configuration...')
#             config = get_connector_config_for_database(
#                 db_config=db,
#                 tables_whitelist=tables_list,
#             )
            
#             if not config:
#                 self.stdout.write(self.style.ERROR(f'❌ Failed to generate config for {db.db_type}'))
#                 return
            
#             self.stdout.write(self.style.SUCCESS('✅ Configuration generated'))
            
#             # Validate configuration
#             self.stdout.write('Validating configuration...')
#             is_valid, errors = validate_connector_config(config)
            
#             if not is_valid:
#                 self.stdout.write(self.style.ERROR('❌ Configuration validation failed:'))
#                 for error in errors:
#                     self.stdout.write(f'   - {error}')
#                 return
            
#             self.stdout.write(self.style.SUCCESS('✅ Configuration is valid'))
            
#             # Show key configuration
#             self.stdout.write('\nKey Configuration:')
#             key_fields = [
#                 'connector.class',
#                 'database.hostname',
#                 'database.port',
#                 'database.dbname',
#                 'snapshot.mode',
#                 'table.include.list',
#             ]
            
#             for field in key_fields:
#                 if field in config:
#                     value = config[field]
#                     if field == 'database.password':
#                         value = '***hidden***'
#                     self.stdout.write(f'   {field}: {value}')
            
#             # Ask for confirmation
#             self.stdout.write(self.style.WARNING('\n⚠️  Ready to create connector'))
#             confirm = input('Proceed? (yes/no): ')
            
#             if confirm.lower() != 'yes':
#                 self.stdout.write('Cancelled.')
#                 return
            
#             # Create connector
#             self.stdout.write('\nCreating connector...')
#             success, error = manager.create_connector(connector_name, config)
            
#             if success:
#                 self.stdout.write(self.style.SUCCESS(f'✅ Connector created successfully!'))
#                 self.stdout.write(f'\nConnector: {connector_name}')
#                 self.stdout.write('You can monitor it at: http://localhost:8080')
                
#                 # Wait and check status
#                 import time
#                 self.stdout.write('\nWaiting 3 seconds...')
#                 time.sleep(3)
                
#                 exists, status = manager.get_connector_status(connector_name)
#                 if exists and status:
#                     state = status.get('connector', {}).get('state', 'UNKNOWN')
#                     self.stdout.write(f'Current State: {state}')
                    
#                     if state == 'RUNNING':
#                         self.stdout.write(self.style.SUCCESS('✅ Connector is RUNNING!'))
#                     elif state == 'FAILED':
#                         self.stdout.write(self.style.ERROR('❌ Connector FAILED'))
#                         self.stdout.write('Check Kafka Connect logs:')
#                         self.stdout.write('   docker logs debezium-connect')
#             else:
#                 self.stdout.write(self.style.ERROR(f'❌ Failed to create connector: {error}'))

#         # Test 5: Delete Connector
#         if options['delete']:
#             self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
#             self.stdout.write(self.style.HTTP_INFO(f'TEST 5: Delete Connector "{options["delete"]}"'))
#             self.stdout.write(self.style.HTTP_INFO('━'*60))
            
#             # Confirm deletion
#             self.stdout.write(self.style.WARNING(f'⚠️  You are about to delete connector: {options["delete"]}'))
#             confirm = input('Are you sure? (yes/no): ')
            
#             if confirm.lower() != 'yes':
#                 self.stdout.write('Cancelled.')
#                 return
            
#             success, error = manager.delete_connector(options['delete'])
            
#             if success:
#                 self.stdout.write(self.style.SUCCESS(f'✅ Connector deleted successfully'))
#             else:
#                 self.stdout.write(self.style.ERROR(f'❌ Failed to delete connector: {error}'))

#         # Summary
#         self.stdout.write(self.style.SUCCESS('\n' + '='*60))
#         self.stdout.write(self.style.SUCCESS('  TEST COMPLETE'))
#         self.stdout.write(self.style.SUCCESS('='*60))
        
#         self.stdout.write('\nUseful commands:')
#         self.stdout.write('  List connectors:        python manage.py test_debezium --list')
#         self.stdout.write('  Get status:             python manage.py test_debezium --status CONNECTOR_NAME')
#         self.stdout.write('  Create connector:       python manage.py test_debezium --create --db-id 1')
#         self.stdout.write('  Create with tables:     python manage.py test_debezium --create --db-id 1 --tables "users,orders"')
#         self.stdout.write('  Delete connector:       python manage.py test_debezium --delete CONNECTOR_NAME')
#         self.stdout.write('\nKafka UI: http://localhost:8080')
#         self.stdout.write()



"""
Management command to test Debezium connector operations (Dynamic Version)
"""

from django.core.management.base import BaseCommand
from sqlalchemy import create_engine, inspect, text
from client.models.database import ClientDatabase
from client.models.client import Client

from client.utils import (
    DebeziumConnectorManager,
    get_connector_config_for_database,
    generate_connector_name,
    validate_connector_config,
)


class Command(BaseCommand):
    help = "Dynamically create and manage Debezium connectors for all client databases"

    def add_arguments(self, parser):
        parser.add_argument(
            "--db-id",
            type=int,
            help="ClientDatabase ID to create connector for",
        )
        parser.add_argument(
            "--list",
            action="store_true",
            help="List all existing connectors",
        )
        parser.add_argument(
            "--status",
            type=str,
            help="Get status of a specific connector by name",
        )
        parser.add_argument(
            "--create",
            action="store_true",
            help="Create a connector for the specified database",
        )
        parser.add_argument(
            "--delete",
            type=str,
            help="Delete a connector by name",
        )
        parser.add_argument(
            "--auto-tables",
            action="store_true",
            help="Automatically detect all tables from database (default behavior)",
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
        self.stdout.write(self.style.SUCCESS("  DYNAMIC DEBEZIUM CONNECTOR TEST"))
        self.stdout.write(self.style.SUCCESS("=" * 60 + "\n"))

        # Initialize Debezium connector manager
        manager = DebeziumConnectorManager()

        # ──────────────────────────────────────────────────────────────
        # 1️⃣  Kafka Connect Health Check
        # ──────────────────────────────────────────────────────────────
        self.stdout.write(self.style.HTTP_INFO("━" * 60))
        self.stdout.write(self.style.HTTP_INFO("TEST 1: Kafka Connect Health Check"))
        self.stdout.write(self.style.HTTP_INFO("━" * 60))

        is_healthy, error = manager.check_kafka_connect_health()
        if is_healthy:
            self.stdout.write(self.style.SUCCESS("✅ Kafka Connect is running and healthy"))
        else:
            self.stdout.write(self.style.ERROR(f"❌ Kafka Connect is not healthy: {error}"))
            self.stdout.write(self.style.WARNING("\nMake sure Docker services are running:"))
            self.stdout.write("   cd ~/django-jovo-client/debezium-setup")
            self.stdout.write("   docker-compose up -d")
            return

        # ──────────────────────────────────────────────────────────────
        # 2️⃣  List Existing Connectors
        # ──────────────────────────────────────────────────────────────
        if options["list"] or not any(
            [options.get("status"), options.get("create"), options.get("delete")]
        ):
            self.stdout.write(self.style.HTTP_INFO("\n" + "━" * 60))
            self.stdout.write(self.style.HTTP_INFO("TEST 2: List All Connectors"))
            self.stdout.write(self.style.HTTP_INFO("━" * 60))

            connectors = manager.list_connectors()
            if connectors:
                self.stdout.write(self.style.SUCCESS(f"✅ Found {len(connectors)} connector(s):"))
                for connector in connectors:
                    exists, status = manager.get_connector_status(connector)
                    if not exists:
                        continue
                    state = status.get("connector", {}).get("state", "UNKNOWN")
                    color = (
                        self.style.SUCCESS if state == "RUNNING"
                        else self.style.ERROR if state == "FAILED"
                        else self.style.WARNING
                    )
                    self.stdout.write(f"   - {connector}: {color(state)}")
            else:
                self.stdout.write(self.style.WARNING("⚠️  No connectors found"))

        # ──────────────────────────────────────────────────────────────
        # 3️⃣  Connector Status
        # ──────────────────────────────────────────────────────────────
        if options["status"]:
            connector_name = options["status"]
            self.stdout.write(self.style.HTTP_INFO("\n" + "━" * 60))
            self.stdout.write(self.style.HTTP_INFO(f"TEST 3: Connector Status - {connector_name}"))
            self.stdout.write(self.style.HTTP_INFO("━" * 60))

            exists, status = manager.get_connector_status(connector_name)
            if not exists:
                self.stdout.write(self.style.ERROR(f"❌ Connector '{connector_name}' not found"))
                return

            connector_state = status.get("connector", {})
            state = connector_state.get("state", "UNKNOWN")
            self.stdout.write(self.style.SUCCESS(f"Connector State: {state}"))
            self.stdout.write(f"Worker ID: {connector_state.get('worker_id', 'N/A')}")

            for task in status.get("tasks", []):
                t_state = task.get("state", "UNKNOWN")
                color = (
                    self.style.SUCCESS if t_state == "RUNNING"
                    else self.style.ERROR if t_state == "FAILED"
                    else self.style.WARNING
                )
                self.stdout.write(f"   - Task {task.get('id', '?')}: {color(t_state)}")
                if task.get("trace"):
                    self.stdout.write(self.style.ERROR(f"     Error: {task['trace'][:200]}..."))


        # ──────────────────────────────────────────────────────────────
        # 4️⃣  Create Connector (Dynamic Table Discovery)
        # ──────────────────────────────────────────────────────────────
        if options["create"]:
            if not options["db_id"]:
                self.stdout.write(self.style.ERROR("❌ --db-id is required to create a connector"))
                return

            try:
                db = ClientDatabase.objects.get(id=options["db_id"])
                client = db.client
            except ClientDatabase.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"❌ Database ID {options['db_id']} not found"))
                return

            self.stdout.write(f"Client: {client.name}")
            self.stdout.write(f"Database: {db.connection_name} ({db.host}:{db.port})")

            # Build SQLAlchemy connection for table discovery
            try:
                url = f"mysql+pymysql://{db.username}:{db.password}@{db.host}:{db.port}/{db.database_name}"
                engine = create_engine(url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                inspector = inspect(engine)
                all_tables = inspector.get_table_names()
                if not all_tables:
                    self.stdout.write(self.style.WARNING("⚠️  No tables found in source DB"))
                    return
                self.stdout.write(self.style.SUCCESS(f"✅ Found {len(all_tables)} table(s):"))
                for t in all_tables:
                    self.stdout.write(f"   - {t}")
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Could not inspect database tables: {e}"))
                return

            # Auto-table discovery
            tables_list = all_tables

            # Generate connector name
            connector_name = generate_connector_name(client, db)
            self.stdout.write(f"\nConnector Name: {self.style.WARNING(connector_name)}")

            # Generate configuration
            self.stdout.write("Generating connector configuration dynamically...")
            config = get_connector_config_for_database(db_config=db, tables_whitelist=tables_list)
            if not config:
                self.stdout.write(self.style.ERROR("❌ Failed to generate connector config"))
                return
            self.stdout.write(self.style.SUCCESS("✅ Configuration generated"))

            # Validate
            is_valid, errors = validate_connector_config(config)
            if not is_valid:
                self.stdout.write(self.style.ERROR("❌ Config validation failed:"))
                for err in errors:
                    self.stdout.write(f"   - {err}")
                return
            self.stdout.write(self.style.SUCCESS("✅ Configuration valid"))

            # Confirm and create
            self.stdout.write(self.style.WARNING("\n⚠️  Ready to create connector"))
            confirm = input("Proceed? (yes/no): ").strip().lower()
            if confirm != "yes":
                self.stdout.write("Cancelled.")
                return

            self.stdout.write("Creating connector...")
            success, error = manager.create_connector(connector_name, config)
            if success:
                self.stdout.write(self.style.SUCCESS("✅ Connector created successfully!"))
                self.stdout.write(f"Monitor at: http://localhost:8080\n")
            else:
                self.stdout.write(self.style.ERROR(f"❌ Failed to create connector: {error}"))
                return

            # Check final status
            import time
            time.sleep(3)
            exists, status = manager.get_connector_status(connector_name)
            if exists and status:
                state = status.get("connector", {}).get("state", "UNKNOWN")
                if state == "RUNNING":
                    self.stdout.write(self.style.SUCCESS("✅ Connector is RUNNING"))
                elif state == "FAILED":
                    self.stdout.write(self.style.ERROR("❌ Connector FAILED - check logs"))
                    self.stdout.write("   docker logs debezium-connect")

        # ──────────────────────────────────────────────────────────────
        # 5️⃣  Delete Connector
        # ──────────────────────────────────────────────────────────────
        if options["delete"]:
            connector_name = options["delete"]
            self.stdout.write(self.style.WARNING(f"⚠️  Deleting connector: {connector_name}"))
            confirm = input("Are you sure? (yes/no): ").strip().lower()
            if confirm != "yes":
                self.stdout.write("Cancelled.")
                return
            success, error = manager.delete_connector(connector_name)
            if success:
                self.stdout.write(self.style.SUCCESS("✅ Connector deleted successfully"))
            else:
                self.stdout.write(self.style.ERROR(f"❌ Failed to delete connector: {error}"))

        # ──────────────────────────────────────────────────────────────
        # ✅ Summary
        # ──────────────────────────────────────────────────────────────
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
        self.stdout.write(self.style.SUCCESS("  TEST COMPLETE"))
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write("\nKafka UI: http://localhost:8080\n")
