#!/usr/bin/env python
"""
Test CDC for all 3 tables
"""
import os
import sys
import django

sys.path.insert(0, '/root/projects/django/django-jovo-client')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'jovoclient.settings')
django.setup()

from client.models import ReplicationConfig
from client.utils.database_utils import get_database_engine
from sqlalchemy import text

# Get active config
config = ReplicationConfig.objects.filter(is_active=True).first()
if not config:
    print("ERROR: No active replication config")
    sys.exit(1)

print("=" * 80)
print("CDC TEST - Checking data in SOURCE vs TARGET")
print("=" * 80)

# Get engines
source_engine = get_database_engine(config.client_database)
target_db = config.client_database.client.get_target_database()
target_engine = get_database_engine(target_db)

source_db = config.client_database.database_name
target_db_name = target_db.database_name

# Check each table mapping
for mapping in config.table_mappings.filter(is_enabled=True):
    print(f"\n{mapping.source_table} -> {mapping.target_table}")
    print("-" * 80)

    # Get row count from source
    with source_engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM `{source_db}`.`{mapping.source_table}`"))
        source_count = result.scalar()

    # Get row count from target
    with target_engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM `{target_db_name}`.`{mapping.target_table}`"))
        target_count = result.scalar()

    print(f"  Source rows: {source_count}")
    print(f"  Target rows: {target_count}")

    if source_count == target_count:
        print(f"  ✓ Row counts match!")
    else:
        diff = source_count - target_count
        print(f"  ✗ MISMATCH: {diff} rows missing in target")

    # Show last few rows from both
    print(f"\n  Last 3 rows from SOURCE:")
    with source_engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM `{source_db}`.`{mapping.source_table}` ORDER BY id DESC LIMIT 3"))
        rows = result.fetchall()
        if rows:
            # Get column names
            cols = result.keys()
            for row in rows:
                print(f"    {dict(zip(cols, row))}")
        else:
            print(f"    (empty)")

    print(f"\n  Last 3 rows from TARGET:")
    with target_engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM `{target_db_name}`.`{mapping.target_table}` ORDER BY id DESC LIMIT 3"))
        rows = result.fetchall()
        if rows:
            # Get column names
            cols = result.keys()
            for row in rows:
                print(f"    {dict(zip(cols, row))}")
        else:
            print(f"    (empty)")

source_engine.dispose()
target_engine.dispose()

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)