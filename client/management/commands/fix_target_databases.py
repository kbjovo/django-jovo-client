"""
Management command to fix/create target databases for existing clients
Ensures ONLY ONE target database per client
"""

from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings
from client.models import Client, ClientDatabase


class Command(BaseCommand):
    help = 'Create/fix target database entries for existing clients (ensures only ONE target per client)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )

    def handle(self, *args, **options):
        dry_run = options.get('dry_run', False)
        
        if dry_run:
            self.stdout.write(self.style.WARNING("\n🔍 DRY RUN MODE - No changes will be made\n"))
        
        clients = Client.objects.all()
        
        self.stdout.write(f"Found {clients.count()} clients\n")
        
        db_config = settings.DATABASES.get('default', {})
        
        for client in clients:
            self.stdout.write(f"\n{'='*60}")
            self.stdout.write(f"Processing client: {client.name}")
            self.stdout.write(f"  Client DB name: {client.db_name}")
            
            # Get all databases for this client
            all_dbs = client.client_databases.all()
            target_dbs = all_dbs.filter(is_target=True)
            
            self.stdout.write(f"  Total databases: {all_dbs.count()}")
            self.stdout.write(f"  Target databases found: {target_dbs.count()}")
            
            # Show all current databases
            for db in all_dbs:
                db_type = "TARGET" if db.is_target else "SOURCE" if db.is_primary else "OTHER"
                self.stdout.write(f"    - {db.connection_name} ({db_type}) - DB: {db.database_name}")
            
            # STEP 1: Fix multiple targets issue
            if target_dbs.count() > 1:
                self.stdout.write(self.style.WARNING(
                    f"\n  ⚠️ Found {target_dbs.count()} target databases! Should only be 1."
                ))
                
                # Find the correct target (matching client.db_name)
                correct_target = target_dbs.filter(database_name=client.db_name).first()
                
                if correct_target:
                    self.stdout.write(
                        f"  ✅ Found correct target: {correct_target.connection_name} (DB: {correct_target.database_name})"
                    )
                    
                    # Remove target flag from others
                    for db in target_dbs.exclude(id=correct_target.id):
                        self.stdout.write(
                            f"  🔧 Removing target flag from: {db.connection_name} (DB: {db.database_name})"
                        )
                        if not dry_run:
                            db.is_target = False
                            db.save()
                else:
                    self.stdout.write(
                        self.style.WARNING(
                            f"  ⚠️ No target matches client.db_name '{client.db_name}'"
                        )
                    )
                    # Keep the first one, remove others
                    correct_target = target_dbs.first()
                    self.stdout.write(
                        f"  ✅ Keeping: {correct_target.connection_name} (DB: {correct_target.database_name})"
                    )
                    
                    for db in target_dbs.exclude(id=correct_target.id):
                        self.stdout.write(
                            f"  🔧 Removing target flag from: {db.connection_name}"
                        )
                        if not dry_run:
                            db.is_target = False
                            db.save()
            
            # STEP 2: Ensure database exists
            try:
                if not dry_run:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"CREATE DATABASE IF NOT EXISTS `{client.db_name}` "
                            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                        )
                self.stdout.write(
                    self.style.SUCCESS(f"\n  ✅ Database '{client.db_name}' ready")
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"  ❌ Failed to create database: {e}")
                )
                continue
            
            # STEP 3: Ensure correct target database entry exists
            target_db = client.client_databases.filter(is_target=True).first()
            
            if not target_db:
                # No target database - create one
                self.stdout.write("  📝 No target database found, creating...")
                
                if not dry_run:
                    target_db, created = ClientDatabase.objects.get_or_create(
                        client=client,
                        database_name=client.db_name,
                        defaults={
                            'connection_name': f"{client.name} - Target DB",
                            'db_type': 'mysql',
                            'host': db_config.get('HOST', 'localhost'),
                            'port': int(db_config.get('PORT', 3306)),
                            'username': db_config.get('USER', 'root'),
                            'password': db_config.get('PASSWORD', ''),
                            'is_primary': False,
                            'is_target': True,
                            'connection_status': 'success',
                        }
                    )
                    
                    if created:
                        self.stdout.write(
                            self.style.SUCCESS(f"  ✅ Created target database entry")
                        )
                    else:
                        # Entry exists but not marked as target
                        target_db.is_target = True
                        target_db.save()
                        self.stdout.write(
                            self.style.SUCCESS(f"  ✅ Updated existing entry as target")
                        )
                else:
                    self.stdout.write(
                        self.style.WARNING(f"  [DRY RUN] Would create target database entry")
                    )
            else:
                # Target exists - verify it matches client.db_name
                if target_db.database_name != client.db_name:
                    self.stdout.write(
                        self.style.WARNING(
                            f"  ⚠️ Target DB name mismatch!\n"
                            f"     Target DB: {target_db.database_name}\n"
                            f"     Client DB: {client.db_name}"
                        )
                    )
                    self.stdout.write(f"  🔧 Updating target database name...")
                    
                    if not dry_run:
                        target_db.database_name = client.db_name
                        target_db.save()
                        self.stdout.write(
                            self.style.SUCCESS(f"  ✅ Updated target database name")
                        )
                    else:
                        self.stdout.write(
                            self.style.WARNING(f"  [DRY RUN] Would update database name")
                        )
                else:
                    self.stdout.write(
                        self.style.SUCCESS(f"  ✅ Target database correctly configured")
                    )
            
            # STEP 4: Show final summary
            if not dry_run:
                all_dbs = client.client_databases.all()
                target_dbs = all_dbs.filter(is_target=True)
                source_dbs = all_dbs.filter(is_primary=True)
                
                self.stdout.write(f"\n  📊 Final Summary:")
                self.stdout.write(f"     Total databases: {all_dbs.count()}")
                self.stdout.write(f"     Target databases: {target_dbs.count()}")
                self.stdout.write(f"     Source databases: {source_dbs.count()}")
                
                if target_dbs.count() == 1:
                    self.stdout.write(
                        self.style.SUCCESS(f"     ✅ Exactly 1 target database (correct)")
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR(f"     ❌ {target_dbs.count()} target databases (should be 1!)")
                    )
                
                for db in all_dbs:
                    db_type = "TARGET" if db.is_target else "SOURCE" if db.is_primary else "OTHER"
                    self.stdout.write(f"     - {db.connection_name} ({db_type}) - DB: {db.database_name}")
        
        self.stdout.write(f"\n{'='*60}")
        if dry_run:
            self.stdout.write(self.style.WARNING("\n🔍 DRY RUN COMPLETE - No changes were made"))
            self.stdout.write("Run without --dry-run to apply changes\n")
        else:
            self.stdout.write(self.style.SUCCESS("\n✅ All done!"))