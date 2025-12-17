# Run this in Django shell to debug Oracle connection
# docker compose exec django python manage.py shell

from client.models.database import ClientDatabase
from sqlalchemy import create_engine, text
import oracledb

# Get Oracle database config
oracle_db = ClientDatabase.objects.get(db_type='oracle')

print(f"🔍 Testing Oracle Connection")
print(f"   Host: {oracle_db.host}:{oracle_db.port}")
print(f"   Service: {oracle_db.database_name}")
print(f"   Username: {oracle_db.username}")
print(f"   Password: {'*' * len(oracle_db.get_decrypted_password())}")
print()

# Create connection
connection_url = oracle_db.get_connection_url()
print(f"📡 Connection URL: {connection_url.replace(oracle_db.get_decrypted_password(), '***')}")
print()

try:
    engine = create_engine(connection_url, echo=False)
    
    with engine.connect() as conn:
        print("✅ Connection successful!")
        print()
        
        # Test 1: Current user
        print("=" * 60)
        print("TEST 1: Current User")
        print("=" * 60)
        result = conn.execute(text("SELECT USER FROM DUAL"))
        current_user = result.scalar()
        print(f"Current user: {current_user}")
        print()
        
        # Test 2: User tables
        print("=" * 60)
        print("TEST 2: USER_TABLES")
        print("=" * 60)
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM user_tables"))
            count = result.scalar()
            print(f"Tables in user_tables: {count}")
            
            if count > 0:
                result = conn.execute(text("SELECT table_name FROM user_tables ORDER BY table_name"))
                tables = result.fetchall()
                print("Tables:")
                for table in tables:
                    print(f"  - {table[0]}")
            print()
        except Exception as e:
            print(f"❌ Error accessing user_tables: {e}")
            print()
        
        # Test 3: All tables (with UPPER username)
        print("=" * 60)
        print("TEST 3: ALL_TABLES (owner = JOVO)")
        print("=" * 60)
        try:
            result = conn.execute(
                text("SELECT COUNT(*) FROM all_tables WHERE owner = :owner"),
                {"owner": oracle_db.username.upper()}
            )
            count = result.scalar()
            print(f"Tables for owner '{oracle_db.username.upper()}': {count}")
            
            if count > 0:
                result = conn.execute(
                    text("SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name"),
                    {"owner": oracle_db.username.upper()}
                )
                tables = result.fetchall()
                print("Tables:")
                for table in tables:
                    print(f"  - {table[0]}")
            print()
        except Exception as e:
            print(f"❌ Error accessing all_tables: {e}")
            print()
        
        # Test 4: DBA_TABLES (requires DBA privileges)
        print("=" * 60)
        print("TEST 4: DBA_TABLES (owner = JOVO)")
        print("=" * 60)
        try:
            result = conn.execute(
                text("SELECT COUNT(*) FROM dba_tables WHERE owner = :owner"),
                {"owner": oracle_db.username.upper()}
            )
            count = result.scalar()
            print(f"Tables in dba_tables for owner 'JOVO': {count}")
            
            if count > 0:
                result = conn.execute(
                    text("SELECT table_name FROM dba_tables WHERE owner = :owner ORDER BY table_name"),
                    {"owner": oracle_db.username.upper()}
                )
                tables = result.fetchall()
                print("Tables:")
                for table in tables:
                    print(f"  - {table[0]}")
            print()
        except Exception as e:
            print(f"❌ No access to dba_tables (expected if not DBA): {e}")
            print()
        
        # Test 5: Direct table query
        print("=" * 60)
        print("TEST 5: Direct Query to JOVO.CUSTOMERS")
        print("=" * 60)
        try:
            result = conn.execute(text('SELECT COUNT(*) FROM "JOVO"."CUSTOMERS"'))
            count = result.scalar()
            print(f"✅ Can access JOVO.CUSTOMERS: {count} rows")
            print()
        except Exception as e:
            print(f"❌ Cannot access JOVO.CUSTOMERS: {e}")
            print()
        
        # Test 6: Alternative - Try tab (synonym for user_tables in some cases)
        print("=" * 60)
        print("TEST 6: TAB (User Objects)")
        print("=" * 60)
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM tab WHERE tabtype = 'TABLE'"))
            count = result.scalar()
            print(f"Tables in tab: {count}")
            
            if count > 0:
                result = conn.execute(text("SELECT tname FROM tab WHERE tabtype = 'TABLE' ORDER BY tname"))
                tables = result.fetchall()
                print("Tables:")
                for table in tables:
                    print(f"  - {table[0]}")
            print()
        except Exception as e:
            print(f"❌ Error accessing tab: {e}")
            print()
        
        # Test 7: Check user privileges
        print("=" * 60)
        print("TEST 7: User Privileges")
        print("=" * 60)
        try:
            result = conn.execute(text("""
                SELECT privilege 
                FROM user_sys_privs 
                WHERE privilege LIKE '%TABLE%' OR privilege LIKE '%SELECT%'
                ORDER BY privilege
            """))
            privileges = result.fetchall()
            print(f"System privileges: {len(privileges)}")
            for priv in privileges:
                print(f"  - {priv[0]}")
            print()
        except Exception as e:
            print(f"❌ Error checking privileges: {e}")
            print()
        
        # Test 8: Show all accessible schemas
        print("=" * 60)
        print("TEST 8: All Accessible Schemas")
        print("=" * 60)
        try:
            result = conn.execute(text("""
                SELECT DISTINCT owner, COUNT(*) as table_count
                FROM all_tables
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS')
                GROUP BY owner
                ORDER BY owner
            """))
            schemas = result.fetchall()
            print(f"Accessible schemas: {len(schemas)}")
            for schema, count in schemas:
                print(f"  - {schema}: {count} tables")
            print()
        except Exception as e:
            print(f"❌ Error listing schemas: {e}")
            print()
    
    engine.dispose()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    import traceback
    traceback.print_exc()