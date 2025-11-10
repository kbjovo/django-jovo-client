"""
File: client/utils/table_creator.py
Create tables in target database based on replication configuration
"""

import logging
import re
from typing import Optional
from sqlalchemy import MetaData, Table, Column, inspect, text
from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time, DECIMAL
from sqlalchemy.dialects.mysql import BIGINT, TINYINT

from client.utils.database_utils import get_database_engine, get_table_schema

logger = logging.getLogger(__name__)


def create_target_tables(replication_config):
    """
    Auto-create tables in target database based on configuration
    
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
    
    # Create database engines
    try:
        target_engine = get_database_engine(target_db)
        source_engine = get_database_engine(source_db)
    except Exception as e:
        logger.error(f"❌ Failed to create database engines: {e}")
        raise
    
    metadata = MetaData()
    created_count = 0
    skipped_count = 0
    
    try:
        # Get table mappings
        table_mappings = replication_config.table_mappings.filter(is_enabled=True)
        logger.info(f"📋 Processing {table_mappings.count()} table mappings...")
        
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
            
            logger.info(f"🔄 Creating table: {target_table} (from {source_table})")
            
            try:
                # Get source schema
                source_schema = get_table_schema(source_db, source_table)
                
                # Get column mappings
                column_mappings = table_mapping.column_mappings.filter(is_enabled=True)
                logger.info(f"   ✓ Found {column_mappings.count()} column mappings")
                
                if not column_mappings.exists():
                    logger.warning(f"   ⚠️ No column mappings, skipping")
                    skipped_count += 1
                    continue
                
                # Check if table already exists in TARGET
                inspector = inspect(target_engine)
                existing_tables = inspector.get_table_names()
                
                if target_table in existing_tables:
                    logger.info(f"   ℹ️ Table {target_table} already exists, skipping creation")
                    skipped_count += 1
                    
                    # Verify it exists
                    with target_engine.connect() as conn:
                        result = conn.execute(text(f"SHOW TABLES LIKE '{target_table}'"))
                        if result.fetchone():
                            logger.info(f"   ✔️ VERIFIED: Table {target_table} exists in database")
                            
                            # Check column count
                            result = conn.execute(text(f"DESCRIBE `{target_table}`"))
                            col_count = len(result.fetchall())
                            logger.info(f"   ✔️ VERIFIED: Table has {col_count} columns")
                    continue
                
                # Build columns
                columns = []
                for col_mapping in column_mappings:
                    source_col = col_mapping.source_column
                    target_col = col_mapping.target_column
                    col_type_str = col_mapping.source_type or col_mapping.source_data_type or 'VARCHAR(255)'
                    
                    # Map type
                    col_type = map_type_to_sqlalchemy(col_type_str)
                    
                    # Check if PK
                    is_pk = source_col in source_schema.get('primary_keys', [])
                    
                    # Check nullable
                    source_columns = source_schema.get('columns', [])
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
                
                logger.info(f"   ✓ Built {len(columns)} columns")
                
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
                logger.error(f"   ❌ Error creating table {target_table}: {e}")
                skipped_count += 1
        
        # Summary
        logger.info(f"{'='*80}")
        logger.info(f"📊 TABLE CREATION SUMMARY:")
        logger.info(f"   ✅ Successfully created: {created_count} tables")
        logger.info(f"   ⏭️ Skipped (already exist): {skipped_count} tables")
        logger.info(f"   🎯 Target Database: {target_db.database_name} on {target_db.host}")
        logger.info(f"{'='*80}")
        
    finally:
        source_engine.dispose()
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