# from django.core.management.base import BaseCommand
# from client.models import ClientDatabase
# from client.utils import (
#     test_database_connection,
#     get_table_list,
#     get_table_schema,
#     check_binary_logging,
#     send_error_notification,
# )

# class Command(BaseCommand):
#     def handle(self, *args, **options):
#         # Get a test database connection
#         db = ClientDatabase.objects.first()
        
#         if not db:
#             self.stdout.write(self.style.ERROR('No database connections found'))
#             return
        
#         # Test connection
#         self.stdout.write(f"\n1. Testing connection to {db.connection_name}...")
#         success, error = test_database_connection(db)
        
#         if success:
#             self.stdout.write(self.style.SUCCESS('✅ Connection successful'))
#         else:
#             self.stdout.write(self.style.ERROR(f'❌ Connection failed: {error}'))
#             return
        
#         # Check binary logging (for MySQL)
#         self.stdout.write(f"\n2. Checking binary logging...")
#         bin_log_enabled, log_format = check_binary_logging(db)
        
#         if bin_log_enabled:
#             self.stdout.write(self.style.SUCCESS(f'✅ Binary logging enabled: {log_format}'))
#         else:
#             self.stdout.write(self.style.WARNING('⚠️ Binary logging not enabled'))
        
#         # Get tables
#         self.stdout.write(f"\n3. Getting table list...")
#         tables = get_table_list(db)
#         self.stdout.write(self.style.SUCCESS(f'✅ Found {len(tables)} tables'))
        
#         if tables:
#             self.stdout.write(f"   Tables: {', '.join(tables[:5])}...")
            
#             # Get schema for first table
#             self.stdout.write(f"\n4. Getting schema for {tables[0]}...")
#             schema = get_table_schema(db, tables[0])
#             self.stdout.write(self.style.SUCCESS(f'✅ Found {len(schema["columns"])} columns'))
            
#             for col in schema['columns'][:3]:
#                 self.stdout.write(f"   - {col['name']}: {col['type']}")
        
#         # Test notification
#         self.stdout.write(f"\n5. Testing error notification...")
#         sent = send_error_notification(
#             error_title="Test Error",
#             error_message="This is a test error notification",
#             context={'test': True},
#             include_traceback=False
#         )
        
#         if sent:
#             self.stdout.write(self.style.SUCCESS('✅ Notification sent'))
#         else:
#             self.stdout.write(self.style.WARNING('⚠️ Notification not sent (check email config)'))