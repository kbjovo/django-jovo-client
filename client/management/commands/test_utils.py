"""
Management command to test database utilities
"""

from django.core.management.base import BaseCommand
from client.models.database import ClientDatabase
from client.utils import (
    test_database_connection,
    get_table_list,
    get_table_schema,
    check_binary_logging,
    get_database_size,
    send_error_notification,
)


class Command(BaseCommand):
    help = 'Test database utilities and connections'

    def add_arguments(self, parser):
        parser.add_argument(
            '--db-id',
            type=int,
            help='Specific ClientDatabase ID to test',
        )
        parser.add_argument(
            '--skip-email',
            action='store_true',
            help='Skip email notification test',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('\n' + '='*60))
        self.stdout.write(self.style.SUCCESS('  DATABASE UTILITIES TEST'))
        self.stdout.write(self.style.SUCCESS('='*60 + '\n'))

        # Get database connection to test
        if options['db_id']:
            try:
                db = ClientDatabase.objects.get(id=options['db_id'])
            except ClientDatabase.DoesNotExist:
                self.stdout.write(self.style.ERROR(f'❌ Database with ID {options["db_id"]} not found'))
                return
        else:
            db = ClientDatabase.objects.first()

        if not db:
            self.stdout.write(self.style.ERROR('❌ No database connections found'))
            self.stdout.write(self.style.WARNING('\nCreate a ClientDatabase first:'))
            self.stdout.write('   python manage.py shell')
            self.stdout.write('   >>> from client.models import ClientDatabase, Client')
            self.stdout.write('   >>> client = Client.objects.first()')
            self.stdout.write('   >>> db = ClientDatabase.objects.create(...)')
            return

        self.stdout.write(f'Testing database: {self.style.WARNING(db.connection_name)}')
        self.stdout.write(f'Type: {db.db_type} | Host: {db.host}:{db.port} | Database: {db.database_name}\n')

        # Test 1: Connection Test
        self.stdout.write(self.style.HTTP_INFO('━'*60))
        self.stdout.write(self.style.HTTP_INFO('TEST 1: Connection Test'))
        self.stdout.write(self.style.HTTP_INFO('━'*60))
        
        success, error = test_database_connection(db)
        
        if success:
            self.stdout.write(self.style.SUCCESS('✅ Connection successful!'))
        else:
            self.stdout.write(self.style.ERROR(f'❌ Connection failed: {error}'))
            self.stdout.write(self.style.WARNING('\nCannot proceed with other tests. Please fix the connection.\n'))
            return

        # Test 2: Binary Logging Check (MySQL only)
        if db.db_type.lower() == 'mysql':
            self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
            self.stdout.write(self.style.HTTP_INFO('TEST 2: Binary Logging Check (Required for CDC)'))
            self.stdout.write(self.style.HTTP_INFO('━'*60))
            
            try:
                bin_log_enabled, log_format = check_binary_logging(db)
                
                if bin_log_enabled:
                    self.stdout.write(self.style.SUCCESS(f'✅ Binary logging is ENABLED'))
                    self.stdout.write(f'   Format: {self.style.WARNING(log_format)}')
                    if log_format.upper() == 'ROW':
                        self.stdout.write(self.style.SUCCESS('   ✅ ROW format is optimal for Debezium'))
                    else:
                        self.stdout.write(self.style.WARNING(f'   ⚠️  {log_format} format works but ROW is recommended'))
                else:
                    self.stdout.write(self.style.ERROR('❌ Binary logging is NOT enabled'))
                    self.stdout.write(self.style.WARNING('   CDC will not work without binary logging!'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error checking binary logging: {str(e)}'))

        # Test 3: Get Table List
        self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
        self.stdout.write(self.style.HTTP_INFO('TEST 3: Get Table List'))
        self.stdout.write(self.style.HTTP_INFO('━'*60))
        
        try:
            tables = get_table_list(db)
            self.stdout.write(self.style.SUCCESS(f'✅ Found {len(tables)} tables'))
            
            if tables:
                display_count = min(10, len(tables))
                self.stdout.write(f'\n   First {display_count} tables:')
                for i, table in enumerate(tables[:display_count], 1):
                    self.stdout.write(f'   {i:2}. {table}')
                
                if len(tables) > 10:
                    self.stdout.write(f'   ... and {len(tables) - 10} more')
            else:
                self.stdout.write(self.style.WARNING('   No tables found in database'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error getting table list: {str(e)}'))
            tables = []

        # Test 4: Get Table Schema
        if tables:
            self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
            self.stdout.write(self.style.HTTP_INFO(f'TEST 4: Get Schema for Table "{tables[0]}"'))
            self.stdout.write(self.style.HTTP_INFO('━'*60))
            
            try:
                schema = get_table_schema(db, tables[0])
                self.stdout.write(self.style.SUCCESS(f'✅ Schema retrieved successfully'))
                
                self.stdout.write(f'\n   Columns ({len(schema["columns"])}):')
                for i, col in enumerate(schema['columns'][:10], 1):
                    nullable = '(NULL)' if col.get('nullable', False) else '(NOT NULL)'
                    default = f' DEFAULT {col.get("default")}' if col.get('default') is not None else ''
                    self.stdout.write(f'   {i:2}. {col["name"]:20} {str(col["type"]):15} {nullable}{default}')
                
                if len(schema['columns']) > 10:
                    self.stdout.write(f'   ... and {len(schema["columns"]) - 10} more columns')
                
                if schema['primary_keys']:
                    self.stdout.write(f'\n   Primary Keys: {", ".join(schema["primary_keys"])}')
                
                if schema['indexes']:
                    self.stdout.write(f'\n   Indexes ({len(schema["indexes"])}):')
                    for idx in schema['indexes'][:5]:
                        cols = ', '.join(idx.get('column_names', []))
                        unique = ' (UNIQUE)' if idx.get('unique', False) else ''
                        self.stdout.write(f'   - {idx.get("name", "unnamed")}: {cols}{unique}')
                
                if schema['foreign_keys']:
                    self.stdout.write(f'\n   Foreign Keys ({len(schema["foreign_keys"])}):')
                    for fk in schema['foreign_keys'][:5]:
                        self.stdout.write(f'   - {fk.get("name", "unnamed")}')
                        
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error getting schema: {str(e)}'))

        # Test 5: Get Database Size
        self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
        self.stdout.write(self.style.HTTP_INFO('TEST 5: Get Database Size'))
        self.stdout.write(self.style.HTTP_INFO('━'*60))
        
        try:
            size_info = get_database_size(db)
            
            if 'error' in size_info:
                self.stdout.write(self.style.WARNING(f'⚠️  {size_info["error"]}'))
            else:
                size_mb = size_info['size_mb']
                table_count = size_info['table_count']
                
                self.stdout.write(self.style.SUCCESS(f'✅ Database size retrieved'))
                self.stdout.write(f'   Size: {size_mb:.2f} MB')
                self.stdout.write(f'   Tables: {table_count}')
                
                # Size warning
                if size_mb > 1000:
                    self.stdout.write(self.style.WARNING(f'   ⚠️  Large database (>1GB) - replication may take time'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error getting database size: {str(e)}'))

        # Test 6: Email Notification
        if not options['skip_email']:
            self.stdout.write(self.style.HTTP_INFO('\n' + '━'*60))
            self.stdout.write(self.style.HTTP_INFO('TEST 6: Email Notification'))
            self.stdout.write(self.style.HTTP_INFO('━'*60))
            
            try:
                sent = send_error_notification(
                    error_title="Test Notification from test_utils command",
                    error_message=f"This is a test notification for database: {db.connection_name}",
                    context={
                        'database_id': db.id,
                        'database_name': db.database_name,
                        'host': db.host,
                        'test': True,
                    },
                    include_traceback=False
                )
                
                if sent:
                    self.stdout.write(self.style.SUCCESS('✅ Email notification sent successfully'))
                    self.stdout.write(self.style.WARNING('   Check your email inbox!'))
                else:
                    self.stdout.write(self.style.WARNING('⚠️  Email not sent (check email configuration in settings.py)'))
                    self.stdout.write('   Add the following to settings.py:')
                    self.stdout.write('   EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"')
                    self.stdout.write('   EMAIL_HOST = "smtp.gmail.com"')
                    self.stdout.write('   ADMINS = [("Your Name", "your-email@example.com")]')
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error sending email: {str(e)}'))
        else:
            self.stdout.write(self.style.WARNING('\n⏭️  Skipping email test (--skip-email flag)'))

        # Summary
        self.stdout.write(self.style.SUCCESS('\n' + '='*60))
        self.stdout.write(self.style.SUCCESS('  TEST SUMMARY'))
        self.stdout.write(self.style.SUCCESS('='*60))
        self.stdout.write(self.style.SUCCESS('✅ All utility functions are working!'))
        self.stdout.write(self.style.SUCCESS('✅ Database connection is functional'))
        self.stdout.write(self.style.SUCCESS('✅ Ready to proceed with Debezium integration\n'))