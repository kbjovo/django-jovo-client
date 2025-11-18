"""
Quick diagnostic script to check replication state
Run with: python manage.py shell < debug_replication.py
"""

from client.models import ReplicationConfig
from client.replication import ReplicationOrchestrator
from django.utils import timezone

# Get the config (adjust ID as needed)
config_id = 6  # CHANGE THIS TO YOUR CONFIG ID
config = ReplicationConfig.objects.get(id=config_id)

print("=" * 80)
print("REPLICATION DIAGNOSTIC REPORT")
print("=" * 80)
print()

# 1. Basic config info
print("1. CONFIGURATION INFO:")
print(f"   Config ID: {config.id}")
print(f"   Connector Name: {config.connector_name}")
print(f"   Status: {config.status}")
print(f"   Is Active: {config.is_active}")
print(f"   Connector State: {config.connector_state}")
print(f"   Consumer State: {config.consumer_state}")
print(f"   Consumer Task ID: {config.consumer_task_id}")
print()

# 2. Database info
print("2. DATABASE INFO:")
print(f"   Source DB: {config.client_database.host}:{config.client_database.port}/{config.client_database.database_name}")
client = config.client_database.client
target_db = client.get_target_database()
if target_db:
    print(f"   Target DB: {target_db.host}:{target_db.port}/{target_db.database_name}")
else:
    print(f"   Target DB: NOT CONFIGURED ❌")
print()

# 3. Table mappings
print("3. TABLE MAPPINGS:")
enabled_tables = config.table_mappings.filter(is_enabled=True)
print(f"   Total enabled tables: {enabled_tables.count()}")
for tm in enabled_tables:
    print(f"   - {tm.source_table} → {tm.target_table} (synced: {tm.total_rows_synced or 0} rows)")
print()

# 4. Heartbeat info
print("4. CONSUMER HEARTBEAT:")
if config.consumer_last_heartbeat:
    time_diff = timezone.now() - config.consumer_last_heartbeat
    seconds_ago = int(time_diff.total_seconds())
    print(f"   Last heartbeat: {config.consumer_last_heartbeat}")
    print(f"   Seconds ago: {seconds_ago}s")
    if seconds_ago < 120:
        print(f"   Status: ✓ HEALTHY (recent)")
    else:
        print(f"   Status: ❌ STALE (>2 minutes old)")
else:
    print(f"   Last heartbeat: None")
    print(f"   Status: ❌ NEVER UPDATED")
print()

# 5. Error messages
print("5. LAST ERROR:")
if config.last_error_message:
    print(f"   {config.last_error_message}")
else:
    print(f"   None")
print()

# 6. Get unified status
print("6. UNIFIED STATUS:")
orchestrator = ReplicationOrchestrator(config)
try:
    status = orchestrator.get_unified_status()
    print(f"   Overall Health: {status['overall'].upper()}")
    print(f"   Connector State: {status['connector']['state']}")
    print(f"   Consumer State: {status['consumer']['state']}")
    print(f"   Total Rows Synced: {status['statistics']['total_rows_synced']}")
except Exception as e:
    print(f"   ERROR getting status: {e}")
print()

# 7. Check actual data in target database
print("7. TARGET DATABASE CHECK:")
if target_db:
    from client.utils.database_utils import get_database_engine
    from sqlalchemy import text, inspect

    try:
        target_engine = get_database_engine(target_db)

        for tm in enabled_tables:
            try:
                with target_engine.connect() as conn:
                    # Get row count
                    if 'mysql' in target_engine.dialect.name:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM `{target_db.database_name}`.`{tm.target_table}`"))
                    else:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {tm.target_table}"))

                    row_count = result.scalar() or 0
                    print(f"   {tm.target_table}: {row_count} rows")
            except Exception as e:
                print(f"   {tm.target_table}: ERROR - {e}")

        target_engine.dispose()
    except Exception as e:
        print(f"   ERROR connecting to target DB: {e}")
else:
    print(f"   Target database not configured")
print()

# 8. Check source database
print("8. SOURCE DATABASE CHECK:")
from client.utils.database_utils import get_database_engine
from sqlalchemy import text

try:
    source_engine = get_database_engine(config.client_database)

    for tm in enabled_tables:
        try:
            with source_engine.connect() as conn:
                if 'mysql' in source_engine.dialect.name:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{config.client_database.database_name}`.`{tm.source_table}`"))
                else:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {tm.source_table}"))

                row_count = result.scalar() or 0
                print(f"   {tm.source_table}: {row_count} rows")
        except Exception as e:
            print(f"   {tm.source_table}: ERROR - {e}")

    source_engine.dispose()
except Exception as e:
    print(f"   ERROR connecting to source DB: {e}")
print()

print("=" * 80)
print("RECOMMENDATIONS:")
print("=" * 80)

# Provide recommendations based on findings
if not target_db:
    print("❌ No target database configured - fix this first!")
elif config.consumer_state != 'RUNNING':
    print("❌ Consumer not running - start replication")
elif config.connector_state != 'RUNNING':
    print("❌ Connector not running - check connector status")
else:
    print("✓ Configuration looks good")
    print("  Check logs: tail -f logs/celery.log | grep '[client_'")

print()