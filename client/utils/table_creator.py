"""
File: client/utils/table_creator.py
Create tables in target database based on replication configuration
"""

import logging
import re
from sqlalchemy import MetaData, Table, Column, inspect, text
from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time, DECIMAL
from sqlalchemy.dialects.mysql import BIGINT, TINYINT

from client.utils.database_utils import get_database_engine, get_table_schema

logger = logging.getLogger(__name__)


def create_target_tables(replication_config, specific_tables=None):
    """
    Auto-create tables in target database based on configuration
    
    Args:
        replication_config: ReplicationConfig instance
        specific_tables: List of source table names to create (if None, creates all enabled tables)
    
    CRITICAL: Must use DIFFERENT database than source!
    """
    logger.info(f"🚀 Starting table creation for ReplicationConfig ID: {replication_config.id}")
    
    # Get client and source database
    source_db = replication_config.client_database
    client = source_db.client
    
    logger.info(f"Client: {client.name}")
    logger.info(f"SOURCE Database: {source_db.connection_name} (ID: {source_db.id})")
    logger.info(f"   - Host: {source_db.host}:{source_db.port}")
    logger.info(f"   - Database Name: {source_db.database_name}")
    
    # CRITICAL: Find target database using the helper method
    target_db = client.get_target_database()
    
    if not target_db:
        error_msg = (
            f"❌ CRITICAL ERROR: No target database found for client '{client.name}'!\n"
            f"   Source DB: {source_db.connection_name} (DB: {source_db.database_name})\n\n"
            f"   SOLUTION:\n"
            f"   1. The target database should have been created automatically\n"
            f"   2. Check if client.db_name ('{client.db_name}') exists\n"
            f"   3. Verify a ClientDatabase entry exists with is_target=True\n"
            f"   4. Run: python manage.py fix_target_databases"
        )
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # CRITICAL VALIDATION: Target must be different from source
    if target_db.id == source_db.id:
        error_msg = (
            f"❌ CRITICAL ERROR: Target database is SAME as source database!\n"
            f"   Source: {source_db.connection_name} (ID: {source_db.id})\n"
            f"   Target: {target_db.connection_name} (ID: {target_db.id})"
        )
        logger.error(error_msg)
        raise Exception(error_msg)
    
    if target_db.database_name == source_db.database_name and target_db.host == source_db.host:
        error_msg = (
            f"❌ CRITICAL ERROR: Target database name is SAME as source!\n"
            f"   Source: {source_db.database_name} on {source_db.host}\n"
            f"   Target: {target_db.database_name} on {target_db.host}\n\n"
            f"   They must have DIFFERENT database names!"
        )
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info(f"✅ TARGET Database: {target_db.connection_name} (ID: {target_db.id})")
    logger.info(f"   - Host: {target_db.host}:{target_db.port}")
    logger.info(f"   - Database: {target_db.database_name}")
    logger.info(f"   ✓ Confirmed: Target ({target_db.database_name}) is DIFFERENT from source ({source_db.database_name})")
    
    # Check drop_before_sync setting
    drop_before_sync = replication_config.drop_before_sync
    if drop_before_sync:
        logger.warning(f"⚠️ DROP BEFORE SYNC is ENABLED - Tables will be dropped if they exist!")
    
    # Create database engines
    try:
        target_engine = get_database_engine(target_db)
        source_engine = get_database_engine(source_db)
    except Exception as e:
        logger.error(f"❌ Failed to create database engines: {e}")
        raise
    
    metadata = MetaData()
    created_count = 0
    dropped_count = 0
    skipped_count = 0
    
    try:
        # Get table mappings - filter by specific_tables if provided
        table_mappings = replication_config.table_mappings.filter(is_enabled=True)
        
        if specific_tables:
            table_mappings = table_mappings.filter(source_table__in=specific_tables)
            logger.info(f"📋 Processing {table_mappings.count()} specific table mappings: {specific_tables}")
        else:
            logger.info(f"📋 Processing ALL {table_mappings.count()} table mappings...")
        
        if table_mappings.count() == 0:
            logger.warning(f"⚠️ No table mappings found to process!")
            return
        
        # VERIFY we're connected to the right database
        with target_engine.connect() as conn:
            result = conn.execute(text("SELECT DATABASE()"))
            connected_db = result.scalar()
            logger.info(f"🔍 Verified target connection: Connected to database '{connected_db}'")
            
            if connected_db != target_db.database_name:
                logger.error(
                    f"❌ ERROR: Connected to wrong database!\n"
                    f"   Expected: {target_db.database_name}\n"
                    f"   Got: {connected_db}"
                )
                raise Exception(f"Connected to wrong database: {connected_db}")
        
        for table_mapping in table_mappings:
            source_table = table_mapping.source_table
            target_table = table_mapping.target_table
            
            logger.info(f"🔄 Processing table: {target_table} (from {source_table})")
            
            try:
                # Get source schema
                source_schema = get_table_schema(source_db, source_table)

                # Get ALL column mappings (not just enabled ones)
                # The target table needs ALL columns because Debezium sends all columns
                # unless column.include.list is explicitly set in the source connector.
                # The is_enabled flag controls what's in column.include.list, not the target schema.
                column_mappings = list(table_mapping.column_mappings.all())
                enabled_count = sum(1 for cm in column_mappings if cm.is_enabled)
                logger.info(f"   ✓ Found {len(column_mappings)} column mappings ({enabled_count} enabled)")
                
                if not column_mappings:
                    logger.warning(f"   ⚠️ No column mappings, skipping")
                    skipped_count += 1
                    continue
                
                # Check if table already exists in TARGET
                inspector = inspect(target_engine)
                existing_tables = inspector.get_table_names()
                table_exists = target_table in existing_tables
                
                if table_exists:
                    if drop_before_sync:
                        # DROP TABLE with CASCADE
                        logger.warning(f"   🗑️ Table {target_table} exists - DROPPING (drop_before_sync=True)...")
                        
                        with target_engine.begin() as conn:
                            # Double-check we're in the right database
                            result = conn.execute(text("SELECT DATABASE()"))
                            current_db = result.scalar()
                            
                            if current_db != target_db.database_name:
                                raise Exception(f"Wrong database! Expected {target_db.database_name}, got {current_db}")
                            
                            # Drop table with CASCADE to remove foreign key constraints
                            drop_sql = f"DROP TABLE IF EXISTS `{target_table}` CASCADE"
                            conn.execute(text(drop_sql))
                            logger.warning(f"   ✅ Table '{target_table}' dropped successfully")
                            
                            # Verify drop was successful
                            result = conn.execute(text(f"SHOW TABLES LIKE '{target_table}'"))
                            if result.fetchone():
                                logger.error(f"   ❌ Table drop verification failed - table still exists!")
                                skipped_count += 1
                                continue
                            else:
                                logger.info(f"   ☑️ VERIFIED: Table '{target_table}' no longer exists")
                                dropped_count += 1
                        
                        # Table is now dropped, continue to create it
                        table_exists = False
                    elif specific_tables:
                        # IMPORTANT: When creating specific tables (e.g., during edit),
                        # we want to CREATE them even if they exist (unless drop_before_sync=False)
                        # In this case, we skip to avoid accidental data loss
                        logger.info(f"   ℹ️ Table {target_table} already exists, skipping (drop_before_sync=False)")
                        logger.info(f"   💡 TIP: Enable 'Drop before sync' to recreate existing tables")
                        skipped_count += 1
                        continue
                    else:
                        # Table exists and we're doing full creation (not specific tables)
                        logger.info(f"   ℹ️ Table {target_table} already exists, skipping creation (drop_before_sync=False)")
                        skipped_count += 1
                        
                        # Verify it exists
                        with target_engine.connect() as conn:
                            result = conn.execute(text(f"SHOW TABLES LIKE '{target_table}'"))
                            if result.fetchone():
                                logger.info(f"   ☑️ VERIFIED: Table {target_table} exists in database")
                                
                                # Check column count
                                result = conn.execute(text(f"DESCRIBE `{target_table}`"))
                                col_count = len(result.fetchall())
                                logger.info(f"   ☑️ VERIFIED: Table has {col_count} columns")
                        continue
                
                # Create a mapping of source column names to their positions
                source_columns = source_schema.get('columns', [])
                column_order = {col.get('name'): idx for idx, col in enumerate(source_columns)}
                
                # Sort column mappings by source column order
                column_mappings_sorted = sorted(
                    column_mappings,
                    key=lambda cm: column_order.get(cm.source_column, float('inf'))
                )
                
                logger.info(f"   ✓ Sorted columns by source table order")
                
                # Build columns
                columns = []
                for col_mapping in column_mappings_sorted:
                    source_col = col_mapping.source_column
                    target_col = col_mapping.target_column
                    col_type_str = col_mapping.source_type or col_mapping.source_data_type or 'VARCHAR(255)'
                    
                    # Map type
                    col_type = map_type_to_sqlalchemy(col_type_str)
                    
                    # Check if PK
                    is_pk = source_col in source_schema.get('primary_keys', [])
                    
                    # Check nullable
                    source_col_info = next(
                        (c for c in source_columns if c.get('name') == source_col),
                        None
                    )
                    nullable = source_col_info.get('nullable', True) if source_col_info else True
                    
                    col = Column(
                        target_col,
                        col_type,
                        primary_key=is_pk,
                        nullable=nullable and not is_pk,
                    )
                    columns.append(col)
                
                if not columns:
                    logger.error(f"   ❌ No columns created")
                    skipped_count += 1
                    continue
                
                logger.info(f"   ✓ Built {len(columns)} columns in source order")
                
                # Create table
                table = Table(target_table, metadata, *columns, extend_existing=True)
                
                logger.info(f"   💾 Creating table in TARGET database '{target_db.database_name}'...")
                
                with target_engine.begin() as conn:
                    # Double-check we're in the right database
                    result = conn.execute(text("SELECT DATABASE()"))
                    current_db = result.scalar()
                    
                    if current_db != target_db.database_name:
                        raise Exception(f"Wrong database! Expected {target_db.database_name}, got {current_db}")
                    
                    # Create the table
                    table.create(conn, checkfirst=True)
                    
                    # Verify creation
                    result = conn.execute(text(f"SHOW TABLES LIKE '{target_table}'"))
                    if result.fetchone():
                        logger.info(f"   ✅ Successfully created table '{target_table}' in '{current_db}'")
                        created_count += 1
                    else:
                        logger.error(f"   ❌ Table creation failed - table not found after creation")
                        skipped_count += 1
                
            except Exception as e:
                logger.error(f"   ❌ Error processing table {target_table}: {e}", exc_info=True)
                skipped_count += 1
        
        # Summary
        logger.info(f"{'='*80}")
        logger.info(f"📊 TABLE CREATION SUMMARY:")
        if specific_tables:
            logger.info(f"   🎯 Mode: Specific tables only ({len(specific_tables)} requested)")
        else:
            logger.info(f"   🎯 Mode: All enabled tables")
        logger.info(f"   ✅ Successfully created: {created_count} tables")
        if dropped_count > 0:
            logger.info(f"   🗑️ Dropped & recreated: {dropped_count} tables")
        if skipped_count > 0:
            logger.info(f"   ⭐ Skipped (already exist): {skipped_count} tables")
        logger.info(f"   🎯 Target Database: {target_db.database_name} on {target_db.host}")
        logger.info(f"{'='*80}")
        
    finally:
        source_engine.dispose()
        target_engine.dispose()


def drop_target_tables(replication_config, table_names):
    """
    Drop specific tables from target database
    
    Args:
        replication_config: ReplicationConfig instance
        table_names: List of SOURCE table names to drop (will be mapped to target names)
    
    ONLY drops tables if drop_before_sync is enabled for safety!
    """
    if not table_names:
        logger.info("ℹ️ No tables to drop")
        return
    
    # Safety check: Only drop if drop_before_sync is enabled
    if not replication_config.drop_before_sync:
        logger.warning(f"⚠️ SKIP: drop_before_sync is disabled. Tables NOT dropped: {table_names}")
        logger.info(f"   💡 TIP: Enable 'Drop before sync' in config to allow table deletion")
        return
    
    logger.info(f"🗑️ Starting table deletion for {len(table_names)} tables...")
    
    # Get target database
    source_db = replication_config.client_database
    client = source_db.client
    target_db = client.get_target_database()
    
    if not target_db:
        error_msg = f"❌ No target database found for client '{client.name}'"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info(f"Target Database: {target_db.database_name} on {target_db.host}")
    
    # Get target table names from mappings
    table_mappings = replication_config.table_mappings.filter(source_table__in=table_names)
    target_table_names = [tm.target_table for tm in table_mappings]
    
    if not target_table_names:
        logger.warning(f"⚠️ No table mappings found for: {table_names}")
        return
    
    logger.info(f"Target tables to drop: {target_table_names}")
    
    # Create engine
    try:
        target_engine = get_database_engine(target_db)
    except Exception as e:
        logger.error(f"❌ Failed to create database engine: {e}")
        raise
    
    dropped_count = 0
    failed_count = 0
    
    try:
        # Verify connection
        with target_engine.connect() as conn:
            result = conn.execute(text("SELECT DATABASE()"))
            connected_db = result.scalar()
            logger.info(f"🔍 Connected to database: '{connected_db}'")
            
            if connected_db != target_db.database_name:
                raise Exception(f"Connected to wrong database: {connected_db}")
        
        # Drop each table
        for target_table in target_table_names:
            try:
                logger.info(f"   🗑️ Dropping table: {target_table}...")
                
                with target_engine.begin() as conn:
                    # Verify database
                    result = conn.execute(text("SELECT DATABASE()"))
                    current_db = result.scalar()
                    
                    if current_db != target_db.database_name:
                        raise Exception(f"Wrong database! Expected {target_db.database_name}, got {current_db}")
                    
                    # Drop table with CASCADE
                    drop_sql = f"DROP TABLE IF EXISTS `{target_table}` CASCADE"
                    conn.execute(text(drop_sql))
                    
                    # Verify drop
                    result = conn.execute(text(f"SHOW TABLES LIKE '{target_table}'"))
                    if result.fetchone():
                        logger.error(f"   ❌ Table {target_table} still exists after drop!")
                        failed_count += 1
                    else:
                        logger.info(f"   ✅ Successfully dropped table: {target_table}")
                        dropped_count += 1
                        
            except Exception as e:
                logger.error(f"   ❌ Error dropping table {target_table}: {e}", exc_info=True)
                failed_count += 1
        
        # Summary
        logger.info(f"{'='*80}")
        logger.info(f"📊 TABLE DELETION SUMMARY:")
        logger.info(f"   ✅ Successfully dropped: {dropped_count} tables")
        if failed_count > 0:
            logger.info(f"   ❌ Failed to drop: {failed_count} tables")
        logger.info(f"   🎯 Target Database: {target_db.database_name} on {target_db.host}")
        logger.info(f"{'='*80}")
        
    finally:
        target_engine.dispose()


def map_type_to_sqlalchemy(type_str: str):
    """Map database type string to SQLAlchemy type"""
    if not type_str:
        return String(255)

    type_lower = str(type_str).lower()

    if 'tinyint' in type_lower:
        return TINYINT
    elif 'bigint' in type_lower:
        return BIGINT
    elif 'int' in type_lower:
        return Integer
    elif 'varchar' in type_lower or 'char' in type_lower:
        match = re.search(r'\((\d+)\)', type_str)
        length = int(match.group(1)) if match else 255
        return String(length)
    elif 'text' in type_lower:
        return Text
    elif 'decimal' in type_lower or 'numeric' in type_lower:
        match = re.search(r'\((\d+),\s*(\d+)\)', type_str)
        if match:
            return DECIMAL(int(match.group(1)), int(match.group(2)))
        return DECIMAL(18, 4)
    elif 'float' in type_lower or 'double' in type_lower:
        return Float
    elif 'bool' in type_lower:
        return Boolean
    elif 'datetime' in type_lower or 'timestamp' in type_lower:
        return DateTime
    elif 'date' in type_lower:
        return Date
    elif 'time' in type_lower:
        return Time
    else:
        return String(255)


def manual_create_target_tables(replication_config_id: int, table_names: list = None) -> tuple:
    """
    Manually create target tables for a replication config.

    Use this function to pre-create tables BEFORE starting the connector,
    allowing you to set schema.evolution=none in the sink connector config.

    Args:
        replication_config_id: ID of the ReplicationConfig
        table_names: Optional list of source table names to create.
                    If None, creates all enabled tables.

    Returns:
        Tuple[bool, str, dict]: (success, message, details)

    Example usage:
        # From a view or management command:
        success, message, details = manual_create_target_tables(config_id)

        # Create specific tables only:
        success, message, details = manual_create_target_tables(config_id, ['users', 'orders'])
    """
    from client.models.replication import ReplicationConfig

    try:
        replication_config = ReplicationConfig.objects.get(pk=replication_config_id)
    except ReplicationConfig.DoesNotExist:
        return False, f"ReplicationConfig with ID {replication_config_id} not found", {}

    try:
        logger.info(f"Manual table creation requested for config ID: {replication_config_id}")
        logger.info(f"Connector: {replication_config.connector_name}")

        # Get table count before creation
        if table_names:
            table_count = len(table_names)
        else:
            table_count = replication_config.table_mappings.filter(is_enabled=True).count()

        # Call the existing create_target_tables function
        create_target_tables(replication_config, specific_tables=table_names)

        # Get target database info for response
        client = replication_config.client_database.client
        target_db = client.get_target_database()

        details = {
            'config_id': replication_config_id,
            'connector_name': replication_config.connector_name,
            'tables_processed': table_count,
            'target_database': target_db.database_name if target_db else None,
            'target_host': f"{target_db.host}:{target_db.port}" if target_db else None,
        }

        return True, f"Successfully processed {table_count} table(s) in target database", details

    except Exception as e:
        error_msg = f"Failed to create target tables: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg, {}


def add_foreign_keys_to_target(replication_config, specific_tables=None):
    """
    Add foreign key constraints to target tables after sink connector creates them.

    This should be called AFTER the sink connector has created the tables.
    Foreign keys are retrieved from the source database and applied to the target.

    Args:
        replication_config: ReplicationConfig instance
        specific_tables: List of source table names to process (if None, processes all enabled tables)

    Returns:
        Tuple[int, int, List[str]]: (created_count, skipped_count, errors)
    """
    logger.info(f"🔗 Adding foreign keys for ReplicationConfig ID: {replication_config.id}")

    source_db = replication_config.client_database
    client = source_db.client
    target_db = client.get_target_database()

    if not target_db:
        error_msg = f"❌ No target database found for client '{client.name}'"
        logger.error(error_msg)
        raise Exception(error_msg)

    logger.info(f"Source: {source_db.connection_name} ({source_db.database_name})")
    logger.info(f"Target: {target_db.connection_name} ({target_db.database_name})")

    source_engine = get_database_engine(source_db)
    target_engine = get_database_engine(target_db)

    created_count = 0
    skipped_count = 0
    errors = []

    try:
        table_mappings = replication_config.table_mappings.filter(is_enabled=True)
        if specific_tables:
            table_mappings = table_mappings.filter(source_table__in=specific_tables)

        logger.info(f"📋 Processing {table_mappings.count()} tables for foreign keys...")

        # Collect all foreign keys first
        all_foreign_keys = []
        table_name_map = {}  # source_table -> target_table mapping

        for table_mapping in table_mappings:
            source_table = table_mapping.source_table
            target_table = table_mapping.target_table
            table_name_map[source_table] = target_table

            try:
                source_schema = get_table_schema(source_db, source_table)
                foreign_keys = source_schema.get('foreign_keys', [])

                if foreign_keys:
                    logger.info(f"   Found {len(foreign_keys)} foreign keys in {source_table}")
                    for fk in foreign_keys:
                        all_foreign_keys.append({
                            'source_table': source_table,
                            'target_table': target_table,
                            'fk_info': fk
                        })
            except Exception as e:
                logger.warning(f"   ⚠️ Could not get schema for {source_table}: {e}")

        if not all_foreign_keys:
            logger.info("ℹ️ No foreign keys found in source tables")
            return 0, 0, []

        logger.info(f"🔗 Found {len(all_foreign_keys)} total foreign keys to create")

        # Create foreign keys on target
        target_db_type = target_db.db_type.lower()

        with target_engine.begin() as conn:
            for fk_data in all_foreign_keys:
                target_table = fk_data['target_table']
                fk_info = fk_data['fk_info']

                try:
                    # Get FK details
                    fk_name = fk_info.get('name', '')
                    constrained_columns = fk_info.get('constrained_columns', [])
                    referred_table = fk_info.get('referred_table', '')
                    referred_columns = fk_info.get('referred_columns', [])
                    referred_schema = fk_info.get('referred_schema', '')

                    if not constrained_columns or not referred_table or not referred_columns:
                        logger.warning(f"   ⚠️ Incomplete FK info for {target_table}, skipping")
                        skipped_count += 1
                        continue

                    # Map referred table to target table name
                    # The sink connector names tables as: {source_db}_{table_name}
                    # Check if referred table is in our mapping
                    if referred_table in table_name_map:
                        target_referred_table = table_name_map[referred_table]
                    else:
                        # Try to find it with the same naming pattern
                        target_referred_table = f"{source_db.database_name}_{referred_table}"

                    # Generate unique FK name
                    new_fk_name = f"fk_{target_table}_{constrained_columns[0]}"[:64]

                    # Build ALTER TABLE statement based on database type
                    if target_db_type == 'mysql':
                        cols = ', '.join(f"`{c}`" for c in constrained_columns)
                        ref_cols = ', '.join(f"`{c}`" for c in referred_columns)
                        alter_sql = f"""
                            ALTER TABLE `{target_table}`
                            ADD CONSTRAINT `{new_fk_name}`
                            FOREIGN KEY ({cols})
                            REFERENCES `{target_referred_table}` ({ref_cols})
                        """
                    elif target_db_type == 'postgresql':
                        cols = ', '.join(f'"{c}"' for c in constrained_columns)
                        ref_cols = ', '.join(f'"{c}"' for c in referred_columns)
                        alter_sql = f"""
                            ALTER TABLE "{target_table}"
                            ADD CONSTRAINT "{new_fk_name}"
                            FOREIGN KEY ({cols})
                            REFERENCES "{target_referred_table}" ({ref_cols})
                        """
                    else:
                        logger.warning(f"   ⚠️ Unsupported target DB type for FK: {target_db_type}")
                        skipped_count += 1
                        continue

                    # Check if FK already exists
                    if target_db_type == 'mysql':
                        check_sql = text(f"""
                            SELECT CONSTRAINT_NAME
                            FROM information_schema.TABLE_CONSTRAINTS
                            WHERE TABLE_SCHEMA = :db_name
                            AND TABLE_NAME = :table_name
                            AND CONSTRAINT_TYPE = 'FOREIGN KEY'
                            AND CONSTRAINT_NAME = :fk_name
                        """)
                        result = conn.execute(check_sql, {
                            'db_name': target_db.database_name,
                            'table_name': target_table,
                            'fk_name': new_fk_name
                        })
                    elif target_db_type == 'postgresql':
                        check_sql = text(f"""
                            SELECT constraint_name
                            FROM information_schema.table_constraints
                            WHERE table_name = :table_name
                            AND constraint_type = 'FOREIGN KEY'
                            AND constraint_name = :fk_name
                        """)
                        result = conn.execute(check_sql, {
                            'table_name': target_table,
                            'fk_name': new_fk_name
                        })

                    if result.fetchone():
                        logger.info(f"   ℹ️ FK {new_fk_name} already exists on {target_table}")
                        skipped_count += 1
                        continue

                    # Execute ALTER TABLE
                    conn.execute(text(alter_sql))
                    logger.info(f"   ✅ Created FK: {new_fk_name} on {target_table} -> {target_referred_table}")
                    created_count += 1

                except Exception as e:
                    error_msg = f"Failed to create FK on {target_table}: {e}"
                    logger.error(f"   ❌ {error_msg}")
                    errors.append(error_msg)

        # Summary
        logger.info(f"{'='*60}")
        logger.info(f"📊 FOREIGN KEY CREATION SUMMARY:")
        logger.info(f"   ✅ Created: {created_count} foreign keys")
        logger.info(f"   ⏭️ Skipped: {skipped_count} (already exist or incomplete)")
        if errors:
            logger.info(f"   ❌ Errors: {len(errors)}")
        logger.info(f"{'='*60}")

    finally:
        source_engine.dispose()
        target_engine.dispose()

    return created_count, skipped_count, errors


def add_foreign_keys_after_sink(replication_config_id: int, table_names: list = None) -> tuple:
    """
    Add foreign keys to target tables after sink connector has created them.

    Call this function AFTER the sink connector has successfully created the tables
    in the target database. It will:
    1. Read foreign key definitions from source tables
    2. Create matching foreign key constraints on target tables

    Args:
        replication_config_id: ID of the ReplicationConfig
        table_names: Optional list of source table names to process.
                    If None, processes all enabled tables.

    Returns:
        Tuple[bool, str, dict]: (success, message, details)

    Example usage:
        # After sink connector creates tables:
        success, message, details = add_foreign_keys_after_sink(config_id)

        # For specific tables only:
        success, message, details = add_foreign_keys_after_sink(config_id, ['orders', 'order_items'])
    """
    from client.models.replication import ReplicationConfig

    try:
        replication_config = ReplicationConfig.objects.get(pk=replication_config_id)
    except ReplicationConfig.DoesNotExist:
        return False, f"ReplicationConfig with ID {replication_config_id} not found", {}

    try:
        logger.info(f"Adding foreign keys for config ID: {replication_config_id}")

        created, skipped, errors = add_foreign_keys_to_target(replication_config, specific_tables=table_names)

        client = replication_config.client_database.client
        target_db = client.get_target_database()

        details = {
            'config_id': replication_config_id,
            'connector_name': replication_config.connector_name,
            'foreign_keys_created': created,
            'foreign_keys_skipped': skipped,
            'errors': errors,
            'target_database': target_db.database_name if target_db else None,
        }

        if errors:
            return False, f"Created {created} FK(s) with {len(errors)} error(s)", details

        return True, f"Successfully created {created} foreign key(s)", details

    except Exception as e:
        error_msg = f"Failed to add foreign keys: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg, {}


def drop_tables_for_mappings(target_db, table_mappings):
    """
    Drop specific target tables given table mappings.

    Supports: MySQL, PostgreSQL, MS SQL, Oracle
    Uses database-specific DROP syntax with proper error handling.

    Args:
        target_db: ClientDatabase instance for the target database
        table_mappings: QuerySet or list of TableMapping objects

    Returns:
        Tuple[bool, str]: (success, message)
    """
    if not table_mappings:
        return True, "No tables to drop"

    # Convert QuerySet to list if needed
    mappings_list = list(table_mappings)
    logger.info(f"Dropping {len(mappings_list)} target tables...")

    engine = get_database_engine(target_db)
    db_type = target_db.db_type.lower()

    dropped_tables = []
    failed_tables = []

    try:
        with engine.connect() as conn:
            for table_mapping in mappings_list:
                target_table = table_mapping.target_table
                target_schema = table_mapping.target_schema or None

                try:
                    # Build fully qualified table name
                    if target_schema:
                        full_table_name = f"{target_schema}.{target_table}"
                    else:
                        full_table_name = target_table

                    # Build DROP statement based on database type
                    if db_type == 'mysql':
                        drop_sql = f"DROP TABLE IF EXISTS `{target_table}`"

                    elif db_type == 'postgresql':
                        if target_schema:
                            drop_sql = f'DROP TABLE IF EXISTS "{target_schema}"."{target_table}" CASCADE'
                        else:
                            drop_sql = f'DROP TABLE IF EXISTS "{target_table}" CASCADE'

                    elif db_type == 'mssql':
                        if target_schema:
                            drop_sql = f"DROP TABLE IF EXISTS [{target_schema}].[{target_table}]"
                        else:
                            drop_sql = f"DROP TABLE IF EXISTS [{target_table}]"

                    elif db_type == 'oracle':
                        if target_schema:
                            drop_sql = f'DROP TABLE "{target_schema}"."{target_table}" CASCADE CONSTRAINTS'
                        else:
                            drop_sql = f'DROP TABLE "{target_table}" CASCADE CONSTRAINTS'

                    else:
                        drop_sql = f"DROP TABLE IF EXISTS {target_table}"

                    conn.execute(text(drop_sql))
                    conn.commit()

                    dropped_tables.append(full_table_name)
                    logger.info(f"  Dropped table: {full_table_name}")

                except Exception as e:
                    # Handle Oracle's "table does not exist" error gracefully
                    if db_type == 'oracle' and ('ORA-00942' in str(e) or 'does not exist' in str(e)):
                        logger.info(f"  Table already dropped: {full_table_name}")
                    else:
                        failed_tables.append(f"{full_table_name}: {str(e)}")
                        logger.warning(f"  Failed to drop {full_table_name}: {e}")

    finally:
        engine.dispose()

    # Build result message
    if dropped_tables:
        message = f"Dropped {len(dropped_tables)} table(s)"
    else:
        message = "No tables were dropped"

    if failed_tables:
        message += f" | {len(failed_tables)} failed"
        return False, message

    return True, message

