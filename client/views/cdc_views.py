"""
CDC (Change Data Capture) workflow and replication management.

This module handles the complete CDC setup workflow:
1. Discover tables from source database
2. Configure tables (select columns, mappings)
3. Create Debezium connector
4. Monitor connector status
5. Manage replication lifecycle (start, stop, restart)

It also provides:
- AJAX endpoints for dynamic table schema fetching
- Configuration editing and updates
- Connector action handlers (pause, resume, delete)
"""

# import pprint

from client.utils.database_utils import get_table_list, get_table_schema
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Q
from sqlalchemy import text
import logging
import json
import time 

from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.kafka_topic_manager import KafkaTopicManager
from client.replication import ReplicationOrchestrator

from client.utils.connector_templates import (
    get_connector_config_for_database,
    generate_connector_name
)
from client.utils.database_utils import (
    get_table_list,
    get_table_schema,
    get_database_engine,
    check_binary_logging,
    check_oracle_logminer
)
from client.utils.offset_manager import delete_connector_offsets
from client.tasks import (
    create_debezium_connector,
    start_kafka_consumer,
    stop_kafka_consumer,
    delete_debezium_connector,
    restart_replication
)

logger = logging.getLogger(__name__)


"""
Complete helper methods for cdc_views.py
Add these functions at the top of your cdc_views.py file (after imports)
"""

import logging
from typing import List
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping

logger = logging.getLogger(__name__)

def normalize_sql_server_table_name(table_name: str, db_type: str) -> str:
    if db_type.lower() != 'mssql':
        return table_name
    
    if '.' in table_name:
        return table_name
    else:
        return f"dbo.{table_name}"


def generate_target_table_name(source_db_name: str, source_table_name: str, db_type: str) -> str:
    safe_db_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in source_db_name)
    
    if db_type.lower() == 'mssql':
        if '.' in source_table_name:
            table_only = source_table_name.split('.', 1)[1]
        else:
            table_only = source_table_name
        
        safe_table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_only)
    else:
        safe_table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in source_table_name)
    
    return f"{safe_db_name}_{safe_table_name}"


def get_kafka_topics_for_tables(db_config: ClientDatabase, replication_config: ReplicationConfig, table_mappings) -> List[str]:
    """
    Generate correct Kafka topic names based on database type
    
    CRITICAL: Different databases have different topic naming conventions:
    - MySQL:      {prefix}.{database}.{table}
    - PostgreSQL: {prefix}.{schema}.{table}
    - SQL Server: {prefix}.{database}.{schema}.{table}
    - Oracle:     {prefix}.{schema}.{table}
    
    Args:
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance
        table_mappings: QuerySet of TableMapping objects
    
    Returns:
        List[str]: List of Kafka topic names
    """
    client = db_config.client
    topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"
    db_type = db_config.db_type.lower()
    
    topics = []
    
    logger.info(f"🔍 Generating Kafka topics for {db_type.upper()} database")
    logger.info(f"   Topic prefix: {topic_prefix}")
    logger.info(f"   Database: {db_config.database_name}")
    
    for tm in table_mappings:
        source_table = tm.source_table
        
        if db_type == 'mssql':
            # ✅ SQL Server format: {prefix}.{database}.{schema}.{table}
            # SQL Server uses 3-part naming: database.schema.table
            
            if '.' in source_table:
                # Format: 'dbo.Customers' or 'schema.table'
                parts = source_table.split('.')
                if len(parts) == 2:
                    schema, table = parts
                else:
                    # Weird case with multiple dots - take last as table
                    schema = parts[-2]
                    table = parts[-1]
            else:
                # Format: 'Customers' - use default schema
                schema = 'dbo'
                table = source_table
            
            # SQL Server topic format includes database name
            topic_name = f"{topic_prefix}.{db_config.database_name}.{schema}.{table}"
            
            logger.info(f"   ✓ SQL Server topic: {source_table} → {topic_name}")
            
        elif db_type == 'postgresql':
            # PostgreSQL format: {prefix}.{schema}.{table}
            # PostgreSQL uses 2-part naming: schema.table
            
            schema = 'public'  # Default schema
            if '.' in source_table:
                schema, table = source_table.rsplit('.', 1)
            else:
                table = source_table
            
            topic_name = f"{topic_prefix}.{schema}.{table}"
            
            logger.info(f"   ✓ PostgreSQL topic: {source_table} → {topic_name}")
            
        elif db_type == 'mysql':
            # MySQL format: {prefix}.{database}.{table}
            # MySQL doesn't use schemas (or schema = database)
            
            table = source_table.split('.')[-1] if '.' in source_table else source_table
            topic_name = f"{topic_prefix}.{db_config.database_name}.{table}"
            
            logger.info(f"   ✓ MySQL topic: {source_table} → {topic_name}")
            
        elif db_type == 'oracle':
            # Oracle format: {prefix}.{schema}.{table}
            # Oracle uses schema.table naming
            
            if '.' in source_table:
                schema, table = source_table.rsplit('.', 1)
            else:
                schema = db_config.username.upper()  # Default to user schema
                table = source_table
            
            topic_name = f"{topic_prefix}.{schema}.{table}"
            
            logger.info(f"   ✓ Oracle topic: {source_table} → {topic_name}")
            
        else:
            # Generic fallback format
            topic_name = f"{topic_prefix}.{db_config.database_name}.{source_table}"
            
            logger.warning(f"   ⚠️ Unknown DB type '{db_type}', using generic format: {topic_name}")
        
        topics.append(topic_name)
    
    logger.info(f"✅ Generated {len(topics)} Kafka topic names")
    return topics


def generate_consumer_group_id(replication_config: ReplicationConfig) -> str:
    """
    Generate unique consumer group ID for Kafka consumer
    
    Format: cdc_consumer_{config_id}_{database_name}
    
    Each replication config gets its own consumer group to:
    - Maintain independent offsets
    - Allow parallel consumption
    - Enable per-config consumer management
    
    Args:
        replication_config: ReplicationConfig instance
    
    Returns:
        str: Consumer group ID (Kafka-safe)
    """
    db_name = replication_config.client_database.database_name
    
    # Remove special characters for Kafka compatibility
    # Kafka group IDs can contain: a-z, A-Z, 0-9, . (period), _ (underscore), - (hyphen)
    safe_db_name = ''.join(c if c.isalnum() or c in '._-' else '_' for c in db_name)
    
    # Limit length to avoid Kafka limitations (max 255 chars, but be conservative)
    if len(safe_db_name) > 50:
        safe_db_name = safe_db_name[:50]
    
    group_id = f"cdc_consumer_{replication_config.id}_{safe_db_name}"
    
    logger.info(f"✅ Generated consumer group ID: {group_id}")
    logger.info(f"   Original DB name: {db_name}")
    logger.info(f"   Config ID: {replication_config.id}")
    
    return group_id


def get_signal_topic_name(db_config: ClientDatabase, replication_config: ReplicationConfig) -> str:
    """
    Generate signal topic name for incremental snapshots
    
    Signal topics are used to send commands to Debezium connectors:
    - Trigger incremental snapshots
    - Pause/resume replication
    - Change configuration
    
    Args:
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance
    
    Returns:
        str: Signal topic name
    """
    client = db_config.client
    topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"
    
    signal_topic = f"{topic_prefix}.signals"
    
    logger.info(f"✅ Signal topic: {signal_topic}")
    return signal_topic


def format_table_for_connector(db_config: ClientDatabase, table_name: str, schema_name: str = None) -> str:
    """
    Format table name for Debezium connector configuration
    
    Different databases require different table name formats in table.include.list:
    - MySQL:      {database}.{table}
    - PostgreSQL: {schema}.{table}
    - SQL Server: {database}.{schema}.{table}
    - Oracle:     {schema}.{table}
    
    Args:
        db_config: ClientDatabase instance
        table_name: Source table name (may include schema)
        schema_name: Optional schema override
    
    Returns:
        str: Formatted table name for connector config
    """
    db_type = db_config.db_type.lower()
    
    if db_type == 'mssql':
        # SQL Server: database.schema.table
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or 'dbo'
            table = table_name
        
        formatted = f"{db_config.database_name}.{schema}.{table}"
        
    elif db_type == 'postgresql':
        # PostgreSQL: schema.table
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or 'public'
            table = table_name
        
        formatted = f"{schema}.{table}"
        
    elif db_type == 'mysql':
        # MySQL: database.table
        table = table_name.split('.')[-1] if '.' in table_name else table_name
        formatted = f"{db_config.database_name}.{table}"
        
    elif db_type == 'oracle':
        # Oracle: schema.table
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or db_config.username.upper()
            table = table_name
        
        formatted = f"{schema}.{table}"
        
    else:
        # Generic fallback
        formatted = f"{db_config.database_name}.{table_name}"
    
    logger.debug(f"Formatted table: {table_name} → {formatted} ({db_type})")
    return formatted


def get_table_list_for_connector(db_config: ClientDatabase, table_mappings) -> List[str]:
    """
    Get formatted table list for Debezium connector configuration
    
    Combines format_table_for_connector() with table mappings to generate
    the complete table.include.list for connector configuration
    
    Args:
        db_config: ClientDatabase instance
        table_mappings: QuerySet of TableMapping objects
    
    Returns:
        List[str]: Formatted table names for connector config
    """
    formatted_tables = []
    
    for tm in table_mappings:
        formatted = format_table_for_connector(db_config, tm.source_table)
        formatted_tables.append(formatted)
    
    logger.info(f"✅ Formatted {len(formatted_tables)} tables for connector config")
    for original, formatted in zip([tm.source_table for tm in table_mappings], formatted_tables):
        logger.info(f"   {original} → {formatted}")
    
    return formatted_tables


def validate_topic_subscription(db_config: ClientDatabase, replication_config: ReplicationConfig, 
                                table_mappings, kafka_topics: List[str]) -> bool:
    """
    Validate that generated topic names match expected Debezium output
    
    This helps catch configuration errors before starting the consumer
    
    Args:
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance
        table_mappings: QuerySet of TableMapping objects
        kafka_topics: List of topic names consumer will subscribe to
    
    Returns:
        bool: True if validation passes
    """
    logger.info("🔍 Validating topic subscription...")
    
    if not kafka_topics:
        logger.error("❌ No topics generated - validation failed")
        return False
    
    if len(kafka_topics) != table_mappings.count():
        logger.error(f"❌ Topic count mismatch: {len(kafka_topics)} topics vs {table_mappings.count()} tables")
        return False
    
    # Check topic naming pattern
    topic_prefix = replication_config.kafka_topic_prefix or f"client_{db_config.client.id}_db_{db_config.id}"
    
    for topic in kafka_topics:
        if not topic.startswith(topic_prefix):
            logger.error(f"❌ Topic doesn't match prefix: {topic} (expected prefix: {topic_prefix})")
            return False
    
    logger.info("✅ Topic validation passed")
    logger.info(f"   Expected prefix: {topic_prefix}")
    logger.info(f"   Topic count: {len(kafka_topics)}")
    
    return True


def cdc_discover_tables(request, database_pk):
    """
    Discover all tables from source database - FIXED for Oracle with better error handling
    Returns list of tables with metadata including totals
    Supports: MySQL, PostgreSQL, SQL Server, Oracle, SQLite
    
    ✅ MySQL: WORKING
    ✅ PostgreSQL: WORKING
    ✅ SQL Server: WORKING
    ✅ Oracle: FIXED (unquoted identifiers for case-insensitive matching)
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    if request.method == "POST":
        # User selected tables to replicate
        selected_tables = request.POST.getlist('selected_tables')
        
        if not selected_tables:
            messages.error(request, 'Please select at least one table')
            return redirect('cdc_discover_tables', database_pk=database_pk)
        
        # Store in session and redirect to configuration
        request.session['selected_tables'] = selected_tables
        request.session['database_pk'] = database_pk
        
        return redirect('cdc_configure_tables', database_pk=database_pk)
    
    # GET: Discover tables
    try:
        # ================================================================
        # STEP 1: Check prerequisites based on database type
        # ================================================================
        if db_config.db_type.lower() == 'mysql':
            # Check if binary logging is enabled (required for MySQL CDC)
            is_enabled, log_format = check_binary_logging(db_config)
            if not is_enabled:
                messages.warning(
                    request, 
                    'Binary logging is not enabled on this MySQL database. CDC requires binary logging to be enabled.'
                )

        elif db_config.db_type.lower() == 'oracle':
            logger.info(f"🔍 Oracle CDC Discovery Started")
            logger.info(f"   Username: {db_config.username}")
            logger.info(f"   Database: {db_config.database_name}")
            logger.info(f"   Host: {db_config.host}")
            
            # ✅ FIX: Make LogMiner check optional - don't fail if no permissions
            try:
                is_enabled, info = check_oracle_logminer(db_config)
                if not is_enabled:
                    warnings = []
                    if not info.get('supplemental_logging'):
                        warnings.append("Supplemental logging is not enabled. Run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;")
                    if not info.get('archive_log_mode'):
                        warnings.append("Archive log mode is not enabled. Required for CDC.")
                    if not info.get('logminer_available'):
                        warnings.append("LogMiner is not available. Check Oracle installation.")
                    
                    for warning in warnings:
                        messages.warning(request, warning)
            except Exception as e:
                # ✅ Don't fail - just show warning
                logger.warning(f"⚠️  Could not check LogMiner status (may lack permissions): {e}")
                messages.info(
                    request,
                    f"⚠️  Could not verify LogMiner configuration. "
                    f"This may be due to insufficient privileges on V$ views. "
                    f"CDC setup can continue, but ensure LogMiner is properly configured."
                )

                    
        elif db_config.db_type.lower() == 'mssql':
            # Check if SQL Server Agent is running (required for CDC)
            engine = get_database_engine(db_config)
            with engine.connect() as conn:
                try:
                    # Check if CDC is enabled at database level
                    cdc_check = text("""
                        SELECT is_cdc_enabled, name 
                        FROM sys.databases 
                        WHERE name = :db_name
                    """)
                    result = conn.execute(cdc_check, {"db_name": db_config.database_name})
                    row = result.fetchone()
                    
                    if row and not row[0]:
                        messages.warning(
                            request,
                            f'CDC is not enabled on database "{db_config.database_name}". '
                            f'Run: EXEC sys.sp_cdc_enable_db to enable CDC.'
                        )
                    
                    # Check if SQL Server Agent is running
                    agent_check = text("""
                        SELECT dss.[status], dss.[status_desc]
                        FROM sys.dm_server_services dss
                        WHERE dss.[servicename] LIKE N'SQL Server Agent (%';
                    """)
                    result = conn.execute(agent_check)
                    agent_row = result.fetchone()
                    
                    if agent_row and agent_row[0] != 4:  # 4 = Running
                        messages.warning(
                            request,
                            f'SQL Server Agent is not running (Status: {agent_row[1]}). '
                            f'CDC requires SQL Server Agent to be running.'
                        )
                except Exception as e:
                    logger.warning(f"Could not check SQL Server CDC prerequisites: {e}")
            
            engine.dispose()
        
        # ================================================================
        # STEP 2: Get list of tables - ENHANCED FOR ORACLE
        # ================================================================
        logger.info(f"📋 Fetching table list for {db_config.db_type}...")
        
        try:
            tables = get_table_list(db_config)
            logger.info(f"✅ Found {len(tables)} tables via get_table_list()")
            
            if tables:
                logger.info(f"   Sample tables: {tables[:5]}")
        except Exception as e:
            logger.error(f"❌ get_table_list() failed: {e}", exc_info=True)
            tables = []
        
        # ================================================================
        # ORACLE FALLBACK: Direct query if get_table_list() returns empty
        # ================================================================
        if not tables and db_config.db_type.lower() == 'oracle':
            logger.warning(f"⚠️  No tables found via get_table_list(), trying direct Oracle query...")
            
            try:
                engine = get_database_engine(db_config)
                with engine.connect() as conn:
                    # Try multiple approaches to find tables
                    
                    # Approach 1: user_tables (tables owned by current user)
                    logger.info(f"🔍 Trying user_tables...")
                    oracle_query = text("""
                        SELECT table_name 
                        FROM user_tables 
                        ORDER BY table_name
                    """)
                    
                    result = conn.execute(oracle_query)
                    oracle_tables = [row[0] for row in result.fetchall()]
                    
                    if oracle_tables:
                        logger.info(f"✅ Found {len(oracle_tables)} tables via user_tables")
                        logger.info(f"   Tables: {oracle_tables[:10]}")
                        tables = oracle_tables
                    else:
                        logger.warning(f"⚠️  No tables in user_tables")
                        
                        # Approach 2: all_tables filtered by username (case-insensitive)
                        logger.info(f"🔍 Trying all_tables with owner = '{db_config.username.upper()}'...")
                        all_tables_query = text("""
                            SELECT table_name, owner
                            FROM all_tables 
                            WHERE owner = UPPER(:username)
                            ORDER BY table_name
                        """)
                        
                        result = conn.execute(all_tables_query, {"username": db_config.username})
                        all_oracle_tables = result.fetchall()
                        
                        if all_oracle_tables:
                            logger.info(f"✅ Found {len(all_oracle_tables)} tables in all_tables")
                            logger.info(f"   Owner: {all_oracle_tables[0][1]}")
                            logger.info(f"   Tables: {[row[0] for row in all_oracle_tables[:10]]}")
                            tables = [row[0] for row in all_oracle_tables]
                        else:
                            logger.warning(f"⚠️  No tables found for owner = '{db_config.username.upper()}'")
                            
                            # Approach 3: Show all accessible tables (any owner)
                            logger.info(f"🔍 Trying all accessible tables...")
                            accessible_query = text("""
                                SELECT owner, table_name, COUNT(*) OVER() as total_count
                                FROM all_tables 
                                WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'XDB', 
                                                    'CTXSYS', 'MDSYS', 'OLAPSYS', 'WMSYS',
                                                    'APPQOSSYS', 'AUDSYS', 'DBSFWUSER', 'GSMADMIN_INTERNAL',
                                                    'GSMCATUSER', 'GSMROOTUSER', 'GSMUSER',
                                                    'DIP', 'DVF', 'DVSYS', 'GGSYS', 'ANONYMOUS',
                                                    'DGPDB_INT', 'CDC_USER')
                                ORDER BY owner, table_name
                            """)
                            
                            result = conn.execute(accessible_query)
                            accessible_tables = result.fetchall()
                            
                            if accessible_tables:
                                # Group by owner
                                from collections import defaultdict
                                tables_by_owner = defaultdict(list)
                                
                                for owner, table_name, total in accessible_tables:
                                    tables_by_owner[owner].append(table_name)
                                
                                logger.info(f"✅ Found {len(accessible_tables)} accessible tables from {len(tables_by_owner)} schemas:")
                                for owner, table_list in sorted(tables_by_owner.items()):
                                    logger.info(f"   {owner}: {len(table_list)} tables - {table_list[:5]}")
                                
                                # Use tables from the username's schema (if exists) or show all
                                username_upper = db_config.username.upper()
                                if username_upper in tables_by_owner:
                                    logger.info(f"📋 Using tables from {username_upper} schema")
                                    tables = tables_by_owner[username_upper]
                                else:
                                    # Show warning and use all accessible tables
                                    logger.warning(f"⚠️  No tables in {username_upper} schema, showing all accessible tables")
                                    messages.warning(
                                        request,
                                        f"No tables found in schema '{username_upper}'. "
                                        f"Showing all accessible tables. "
                                        f"Available schemas: {', '.join(sorted(tables_by_owner.keys()))}"
                                    )
                                    # Flatten all tables with schema prefix
                                    tables = [f"{owner}.{table}" for owner, table_list in tables_by_owner.items() for table in table_list]
                            else:
                                logger.error(f"❌ No tables found at all!")
                                messages.error(
                                    request,
                                    f'No tables found in Oracle database. '
                                    f'User "{db_config.username}" may not have SELECT privileges on any tables.'
                                )
                
                engine.dispose()
                
            except Exception as e:
                logger.error(f"❌ Oracle direct query failed: {e}", exc_info=True)
                messages.error(request, f'Failed to discover Oracle tables: {str(e)}')
        
        if not tables:
            logger.error(f"❌ No tables found in database!")
            messages.error(
                request,
                f'No tables found in {db_config.db_type} database. '
                f'Please check database connection and user permissions.'
            )
            return redirect('client_detail', pk=db_config.client.pk)
        
        logger.info(f"Found {len(tables)} tables in database")
        
        # ================================================================
        # STEP 3: Get table details (row count, size, columns)
        # ================================================================
        tables_with_info = []
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            for table_name in tables:
                logger.info(f"🔍 Processing table: {table_name}")
                row_count = 0
                columns = []
                primary_keys = []
                error_msg = None
                
                try:
                    # ============================================================
                    # 1. Get row count - database-specific queries
                    # ============================================================
                    count_success = False
                    
                    if db_config.db_type.lower() == 'mysql':
                        count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                        result = conn.execute(count_query)
                        row = result.fetchone()
                        row_count = row[0] if row else 0
                        count_success = True
                    
                    elif db_config.db_type.lower() == 'postgresql':
                        count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
                        result = conn.execute(count_query)
                        row = result.fetchone()
                        row_count = row[0] if row else 0
                        count_success = True
                    
                    elif db_config.db_type.lower() == 'mssql':
                        schema_name = 'dbo'
                        actual_table = table_name
                        if '.' in table_name:
                            schema_name, actual_table = table_name.split('.', 1)
                        
                        count_query = text(f"SELECT COUNT(*) as cnt FROM [{schema_name}].[{actual_table}]")
                        result = conn.execute(count_query)
                        row = result.fetchone()
                        row_count = row[0] if row else 0
                        count_success = True
                    
                    elif db_config.db_type.lower() == 'oracle':
                        # ✅ CRITICAL FIX: Oracle requires UNQUOTED identifiers
                        # Oracle stores table names in UPPERCASE in data dictionary
                        # Quoted identifiers are case-sensitive and won't match
                        
                        if '.' in table_name:
                            schema_name, actual_table = table_name.rsplit('.', 1)
                            
                            # Try unquoted query (case-insensitive, matches UPPERCASE)
                            try:
                                count_query = text(f'SELECT COUNT(*) as cnt FROM {schema_name}.{actual_table}')
                                result = conn.execute(count_query)
                                row = result.fetchone()
                                row_count = row[0] if row else 0
                                count_success = True
                                logger.info(f"   ✅ Row count: {row_count}")
                            except Exception as e1:
                                logger.warning(f"   ⚠️  Unquoted query failed: {e1}")
                                error_msg = f"Count query failed: {str(e1)}"
                        else:
                            # No schema prefix - use current user's schema
                            try:
                                # Try unquoted query (case-insensitive)
                                count_query = text(f'SELECT COUNT(*) as cnt FROM {table_name}')
                                result = conn.execute(count_query)
                                row = result.fetchone()
                                row_count = row[0] if row else 0
                                count_success = True
                                logger.info(f"   ✅ Row count: {row_count}")
                            except Exception as e1:
                                logger.warning(f"   ⚠️  Unquoted query failed: {e1}")
                                error_msg = f"Count query failed: {str(e1)}"
                    
                    else:
                        # Generic fallback
                        count_query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")
                        result = conn.execute(count_query)
                        row = result.fetchone()
                        row_count = row[0] if row else 0
                        count_success = True
                    
                    # ============================================================
                    # 2. Get table schema (columns, types, primary keys)
                    # ============================================================
                    try:
                        schema = get_table_schema(db_config, table_name)
                        columns = schema.get('columns', [])
                        primary_keys = schema.get('primary_keys', [])
                        
                        logger.info(f"   ✅ Schema: {len(columns)} columns, PKs: {primary_keys}")
                        
                        if not columns:
                            logger.warning(f"   ⚠️  No columns found for table {table_name}")
                            if not error_msg:
                                error_msg = "No columns found - table may not exist or user lacks permissions"
                    
                    except Exception as e:
                        logger.error(f"   ❌ get_table_schema() failed: {e}", exc_info=True)
                        if not error_msg:
                            error_msg = f"Schema query failed: {str(e)}"
                    
                    # ============================================================
                    # 3. Check if table has timestamp column (for incremental sync)
                    # ============================================================
                    has_timestamp = any(
                        'timestamp' in str(col.get('type', '')).lower() or 
                        'datetime' in str(col.get('type', '')).lower() or
                        'date' in str(col.get('type', '')).lower() or
                        str(col.get('name', '')).endswith('_at') or
                        str(col.get('name', '')).endswith('_date')
                        for col in columns
                    )
                    
                    # ============================================================
                    # 4. Generate consistent target table name
                    # ============================================================
                    source_db_name = db_config.database_name
                    
                    # Normalize table name based on database type
                    if db_config.db_type.lower() == 'oracle':
                        # Oracle: Keep schema.table format or add username as schema
                        if '.' not in table_name:
                            normalized_table = f"{db_config.username.upper()}.{table_name}"
                        else:
                            normalized_table = table_name
                    else:
                        normalized_table = normalize_sql_server_table_name(table_name, db_config.db_type)
                    
                    # Generate consistent target name
                    target_table_name = generate_target_table_name(
                        source_db_name, 
                        normalized_table, 
                        db_config.db_type
                    )
                    
                    # Display name (short form for UI)
                    display_table_name = normalized_table.split('.')[-1] if '.' in normalized_table else normalized_table
                    
                    # ============================================================
                    # 5. Add to results
                    # ============================================================
                    table_info = {
                        'name': normalized_table,
                        'display_name': display_table_name,
                        'target_name': target_table_name,
                        'row_count': row_count,
                        'column_count': len(columns),
                        'has_timestamp': has_timestamp,
                        'primary_keys': primary_keys,
                    }
                    
                    # Add error message if any issues occurred
                    if error_msg:
                        table_info['error'] = error_msg
                        logger.warning(f"   ⚠️  Table added with error: {error_msg}")
                    
                    tables_with_info.append(table_info)
                    
                    logger.info(f"   ✅ Added: {display_table_name} ({row_count} rows, {len(columns)} cols)")
                    
                except Exception as e:
                    # Catastrophic failure for this table
                    logger.error(f"   ❌ FAILED to process table {table_name}: {str(e)}", exc_info=True)
                    
                    # Still add table with error info
                    source_db_name = db_config.database_name
                    
                    if db_config.db_type.lower() == 'oracle':
                        if '.' not in table_name:
                            normalized_table = f"{db_config.username.upper()}.{table_name}"
                        else:
                            normalized_table = table_name
                    else:
                        normalized_table = normalize_sql_server_table_name(table_name, db_config.db_type)
                    
                    display_table_name = normalized_table.split('.')[-1] if '.' in normalized_table else normalized_table
                    target_table_name = generate_target_table_name(source_db_name, normalized_table, db_config.db_type)
                    
                    tables_with_info.append({
                        'name': normalized_table,
                        'display_name': display_table_name,
                        'target_name': target_table_name,
                        'row_count': 0,
                        'column_count': 0,
                        'has_timestamp': False,
                        'primary_keys': [],
                        'error': str(e)
                    })
        
        engine.dispose()
        
        # ================================================================
        # STEP 4: Calculate totals and statistics
        # ================================================================
        total_rows = sum(t['row_count'] for t in tables_with_info)
        total_cols = sum(t['column_count'] for t in tables_with_info)
        
        # Count tables with errors
        tables_with_errors = [t for t in tables_with_info if 'error' in t]
        
        if tables_with_errors:
            logger.warning(f"⚠️  {len(tables_with_errors)} table(s) had errors:")
            for t in tables_with_errors:
                logger.warning(f"   - {t['name']}: {t['error']}")
            
            messages.warning(
                request,
                f"{len(tables_with_errors)} table(s) could not be fully analyzed. "
                f"Check server logs for details. This may be due to permissions or invalid table metadata."
            )
        
        logger.info(
            f"Discovery complete: {len(tables_with_info)} tables, "
            f"{total_rows} total rows, {total_cols} total columns"
        )
        
        # ================================================================
        # STEP 5: Render template
        # ================================================================
        context = {
            'db_config': db_config,
            'client': db_config.client,
            'tables': tables_with_info,
            'total_tables': len(tables_with_info),
            'total_rows': total_rows,
            'total_columns': total_cols,
            'tables_with_errors': len(tables_with_errors),
        }
        
        return render(request, 'client/cdc/discover_tables.html', context)
        
    except Exception as e:
        logger.error(f'Failed to discover tables: {str(e)}', exc_info=True)
        messages.error(request, f'Failed to discover tables: {str(e)}')
        return redirect('client_detail', pk=db_config.client.pk)


@require_http_methods(["GET", "POST"])
def cdc_configure_tables(request, database_pk):
    """
    Configure selected tables with column selection and mapping
    FIXED: Proper Oracle table name handling
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    # Get selected tables from session
    selected_tables = request.session.get('selected_tables', [])
    if not selected_tables:
        messages.error(request, 'No tables selected')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    if request.method == "POST":
        try:
            # VALIDATE BEFORE CREATING
            for table_name in selected_tables:
                try:
                    schema = get_table_schema(db_config, table_name)
                    if not schema.get('columns'):
                        raise ValueError(f"Table {table_name} has no columns or doesn't exist")
                except Exception as e:
                    logger.error(f"Pre-validation failed for table {table_name}: {e}")
                    messages.error(request, f'Cannot configure table {table_name}: {str(e)}')
                    return redirect('cdc_configure_tables', database_pk=database_pk)

            with transaction.atomic():
                # Create ReplicationConfig
                replication_config = ReplicationConfig.objects.create(
                    client_database=db_config,
                    sync_type=request.POST.get('sync_type', 'realtime'),
                    sync_frequency=request.POST.get('sync_frequency', 'realtime'),
                    status='configured',
                    is_active=False,
                    auto_create_tables=request.POST.get('auto_create_tables') == 'on',
                    drop_before_sync=request.POST.get('drop_before_sync') == 'on',
                    created_by=request.user if request.user.is_authenticated else None
                )

                logger.info(f"Created ReplicationConfig: {replication_config.id}")
                
                # Create TableMappings for each table
                for table_name in selected_tables:
                    sync_type = request.POST.get(f'sync_type_{table_name}', '')
                    if not sync_type:
                        sync_type = request.POST.get('sync_type', 'realtime')
                    
                    incremental_col = request.POST.get(f'incremental_col_{table_name}', '')
                    conflict_resolution = request.POST.get(f'conflict_resolution_{table_name}', 'source_wins')

                    # ✅ FIX: Normalize table name based on database type
                    if db_config.db_type.lower() == 'oracle':
                        # Oracle: Keep schema.table format or add username as schema
                        if '.' not in table_name:
                            normalized_source = f"{db_config.username.upper()}.{table_name}"
                        else:
                            normalized_source = table_name
                    else:
                        normalized_source = normalize_sql_server_table_name(table_name, db_config.db_type)
                    
                    # Get target table name
                    target_table_name = request.POST.get(f'target_table_name_{table_name}', '')
                    if not target_table_name:
                        source_db_name = db_config.database_name
                        target_table_name = generate_target_table_name(
                            source_db_name,
                            normalized_source,
                            db_config.db_type
                        )

                    # Create TableMapping
                    table_mapping = TableMapping.objects.create(
                        replication_config=replication_config,
                        source_table=normalized_source,
                        target_table=target_table_name,
                        is_enabled=True,
                        sync_type=sync_type,
                        incremental_column=incremental_col if sync_type == 'incremental' else None,
                        incremental_column_type='timestamp' if incremental_col else '',
                        conflict_resolution=conflict_resolution,
                    )
                    
                    logger.info(f"Created TableMapping: {normalized_source} -> {target_table_name}")
                    
                    # Get selected columns
                    selected_columns = request.POST.getlist(f'selected_columns_{table_name}')
                    
                    if selected_columns:
                        for source_column in selected_columns:
                            target_column = request.POST.get(
                                f'column_mapping_{table_name}_{source_column}', 
                                source_column
                            )
                            
                            try:
                                schema = get_table_schema(db_config, table_name)
                                columns = schema.get('columns', [])
                                column_info = next(
                                    (col for col in columns if col.get('name') == source_column), 
                                    None
                                )
                                
                                source_type = str(column_info.get('type', 'VARCHAR')) if column_info else 'VARCHAR'
                                
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=source_column,
                                    target_column=target_column or source_column,
                                    source_type=source_type,
                                    target_type=source_type,
                                    is_enabled=True,
                                )
                                
                            except Exception as e:
                                logger.error(f"Error creating column mapping for {source_column}: {e}")
                    else:
                        # Include all columns by default
                        try:
                            schema = get_table_schema(db_config, table_name)
                            columns = schema.get('columns', [])
                            
                            for col in columns:
                                col_name = col.get('name')
                                col_type = str(col.get('type', 'VARCHAR'))
                                
                                target_column = request.POST.get(
                                    f'column_mapping_{table_name}_{col_name}', 
                                    col_name
                                )
                                
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=col_name,
                                    target_column=target_column or col_name,
                                    source_type=col_type,
                                    target_type=col_type,
                                    is_enabled=True,
                                )
                        except Exception as e:
                            logger.error(f"Error creating default column mappings: {e}")
                
                # Auto-create tables if enabled
                if replication_config.auto_create_tables:
                    try:
                        from client.utils.table_creator import create_target_tables
                        create_target_tables(replication_config)
                        messages.success(request, 'Target tables created successfully!')
                    except Exception as e:
                        logger.error(f"Failed to create target tables: {e}")
                        messages.warning(request, f'Configuration saved, but failed to create target tables: {str(e)}')
                
                messages.success(request, 'CDC configuration saved successfully!')
                
                request.session.pop('selected_tables', None)
                request.session.pop('database_pk', None)
                
                return redirect('cdc_create_connector', config_pk=replication_config.pk)
                
        except Exception as e:
            logger.error(f'Failed to save configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to save configuration: {str(e)}')
    
    # ============================================================================
    # GET: Show configuration form - FIXED FOR ORACLE
    # ============================================================================
    tables_with_columns = []
    
    for table_name in selected_tables:
        try:
            logger.info(f"🔍 Getting schema for Oracle table: {table_name}")
            
            # ✅ FIX: Oracle table names may need special handling
            # The table_name from session should already be in correct format from discovery
            schema = get_table_schema(db_config, table_name)
            columns = schema.get('columns', [])
            
            if not columns:
                logger.warning(f"⚠️ No columns found for table: {table_name}")
                logger.info(f"   Schema returned: {schema}")
                
                # Try alternative approaches for Oracle
                if db_config.db_type.lower() == 'oracle':
                    # Try with uppercase
                    table_upper = table_name.upper()
                    if table_upper != table_name:
                        logger.info(f"   Trying uppercase: {table_upper}")
                        schema = get_table_schema(db_config, table_upper)
                        columns = schema.get('columns', [])
                    
                    # Try with schema prefix
                    if not columns and '.' not in table_name:
                        table_with_schema = f"{db_config.username.upper()}.{table_name}"
                        logger.info(f"   Trying with schema: {table_with_schema}")
                        schema = get_table_schema(db_config, table_with_schema)
                        columns = schema.get('columns', [])
            
            if not columns:
                logger.error(f"❌ Could not get columns for {table_name} after all attempts")
                messages.warning(request, f'Could not load schema for table {table_name}')
                continue
            
            logger.info(f"✅ Found {len(columns)} columns for {table_name}")
            
            # Find incremental candidates
            incremental_candidates = [
                col for col in columns
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   'date' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time') or
                   (col.get('name', '') in ['id', 'created_at', 'updated_at'] and 
                    'int' in str(col.get('type', '')).lower())
            ]
            
            # ✅ Generate consistent target table name
            if db_config.db_type.lower() == 'oracle':
                if '.' not in table_name:
                    normalized_table = f"{db_config.username.upper()}.{table_name}"
                else:
                    normalized_table = table_name
            else:
                normalized_table = normalize_sql_server_table_name(table_name, db_config.db_type)
            
            source_db_name = db_config.database_name
            target_table_name = generate_target_table_name(source_db_name, normalized_table, db_config.db_type)

            tables_with_columns.append({
                'name': normalized_table,
                'target_name': target_table_name,
                'columns': columns,
                'primary_keys': schema.get('primary_keys', []),
                'incremental_candidates': incremental_candidates,
            })
            
            logger.debug(f"Added table {normalized_table}: {len(columns)} columns, {len(incremental_candidates)} incremental candidates")
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}", exc_info=True)
            messages.warning(request, f'Could not load schema for table {table_name}: {str(e)}')
    
    if not tables_with_columns:
        messages.error(request, 'Could not load schema for any selected tables. Please check database permissions.')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    context = {
        'db_config': db_config,
        'client': db_config.client,
        'tables': tables_with_columns,
    }
    
    return render(request, 'client/cdc/configure_tables.html', context)


@require_http_methods(["GET", "POST"])
def cdc_create_connector(request, config_pk):
    """
    Finalize replication configuration and optionally auto-start
    FIXED: Proper SQL Server topic naming and consumer subscription
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    db_config = replication_config.client_database
    client = db_config.client

    if request.method == "POST":
        try:
            # Generate connector name
            connector_name = generate_connector_name(client, db_config)

            # Get list of tables to replicate
            table_mappings = replication_config.table_mappings.filter(is_enabled=True)
            
            if not table_mappings.exists():
                raise Exception("No tables selected for replication")

            logger.info(f"📋 Creating connector for {table_mappings.count()} tables")

            # ✅ CRITICAL: Get formatted table names for connector config
            formatted_tables = get_table_list_for_connector(db_config, table_mappings)
            
            logger.info(f"✅ Formatted tables for connector config:")
            for fmt_table in formatted_tables:
                logger.info(f"   - {fmt_table}")

            # Verify Kafka Connect is healthy
            logger.info(f"🔍 Checking Kafka Connect health...")
            manager = DebeziumConnectorManager()
            is_healthy, health_error = manager.check_kafka_connect_health()
            if not is_healthy:
                raise Exception(f"Kafka Connect is not healthy: {health_error}")
            
            logger.info(f"✅ Kafka Connect is healthy")

            # Update replication config
            replication_config.connector_name = connector_name
            replication_config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}"
            replication_config.status = 'configured'
            replication_config.is_active = False
            replication_config.save()

            logger.info(f"✅ Configuration saved for connector: {connector_name}")
            logger.info(f"   Tables configured: {len(formatted_tables)}")
            logger.info(f"   Topic prefix: {replication_config.kafka_topic_prefix}")

            # Check if user wants to auto-start
            auto_start = request.POST.get('auto_start', 'false').lower() == 'true'
            
            if auto_start:
                logger.info(f"🚀 Auto-starting replication for {connector_name}")

                from client.replication import ReplicationOrchestrator
                orchestrator = ReplicationOrchestrator(replication_config)

                success, message = orchestrator.start_replication()

                if success:
                    # ✅ CRITICAL: Validate topic subscription after connector starts
                    kafka_topics = get_kafka_topics_for_tables(db_config, replication_config, table_mappings)
                    
                    logger.info(f"✅ Expected Kafka topics for consumer:")
                    for topic in kafka_topics:
                        logger.info(f"   - {topic}")
                    
                    # Validate topics
                    if validate_topic_subscription(db_config, replication_config, table_mappings, kafka_topics):
                        logger.info(f"✅ Topic validation passed")
                    else:
                        logger.warning(f"⚠️ Topic validation failed - consumer may not receive messages")
                    
                    messages.success(
                        request,
                        f'✅ Replication started successfully! '
                        f'{len(formatted_tables)} tables are now being replicated in real-time.'
                    )
                else:
                    messages.warning(request, f'⚠️ Configuration saved, but failed to start: {message}')
            else:
                # Just save configuration
                messages.success(
                    request,
                    f'✅ Replication configured successfully! '
                    f'{len(formatted_tables)} tables ready. '
                    f'Click "Start Replication" to begin.'
                )

            return redirect('cdc_monitor_connector', config_pk=replication_config.pk)

        except Exception as e:
            logger.error(f"❌ Failed to configure replication: {e}", exc_info=True)
            messages.error(request, f'Failed to configure replication: {str(e)}')

    # GET: Show connector creation confirmation
    table_mappings = replication_config.table_mappings.filter(is_enabled=True)
    
    # ✅ Generate preview of topics and formatted table names
    kafka_topics = get_kafka_topics_for_tables(db_config, replication_config, table_mappings)
    formatted_tables = get_table_list_for_connector(db_config, table_mappings)
    
    # Build table preview data
    table_preview = []
    for tm, topic, formatted in zip(table_mappings, kafka_topics, formatted_tables):
        table_preview.append({
            'source_table': tm.source_table,
            'target_table': tm.target_table,
            'formatted_name': formatted,
            'kafka_topic': topic,
            'enabled_columns': tm.column_mappings.filter(is_enabled=True).count(),
        })

    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'table_mappings': table_mappings,
        'table_preview': table_preview,
        'expected_topics': kafka_topics,
        'db_type': db_config.db_type.upper(),
    }

    return render(request, 'client/cdc/create_connector.html', context)


@require_http_methods(["GET"])
def cdc_monitor_connector(request, config_pk):
    """
    Real-time monitoring of connector status (NEW: Uses orchestrator)
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

    if not replication_config.connector_name:
        messages.error(request, 'No connector configured for this replication')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)

    try:
        # Use orchestrator for unified status
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(replication_config)
        unified_status = orchestrator.get_unified_status()

        # Check if connector actually exists in Kafka Connect
        connector_exists = unified_status['connector']['state'] not in ['NOT_FOUND', 'ERROR']

        # Determine what actions are available based on state
        can_start = (
            replication_config.status == 'configured' and
            not replication_config.is_active
        )

        can_stop = (
            replication_config.is_active and
            connector_exists
        )

        can_restart = (
            replication_config.is_active and
            connector_exists
        )

        context = {
            'replication_config': replication_config,
            'client': replication_config.client_database.client,
            'unified_status': unified_status,
            'connector_exists': connector_exists,
            'can_start': can_start,
            'can_stop': can_stop,
            'can_restart': can_restart,
            'enabled_table_mappings': replication_config.table_mappings.filter(is_enabled=True),
            # Legacy fields for backward compatibility with template
            'connector_status': {
                'connector': {'state': unified_status['connector']['state']},
                'tasks': unified_status['connector'].get('tasks', [])
            } if connector_exists else None,
            'tasks': unified_status['connector'].get('tasks', []),
            'exists': connector_exists,
        }

        return render(request, 'client/cdc/monitor_connector.html', context)

    except Exception as e:
        logger.error(f'Failed to get connector status: {e}', exc_info=True)
        messages.error(request, f'Failed to get connector status: {str(e)}')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)


@require_http_methods(["GET"])
def ajax_get_table_schema(request, database_pk, table_name):
    """
    AJAX endpoint to get table schema details
    """
    try:
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        schema = get_table_schema(db_config, table_name)
        
        return JsonResponse({
            'success': True,
            'schema': schema
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)



@require_http_methods(["POST"])
def cdc_connector_action(request, config_pk, action):
    """
    Handle connector actions: start, pause, resume, restart, delete
    """
    from client.replication import ReplicationOrchestrator
    
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    connector_name = replication_config.connector_name
    
    if not connector_name and action not in ['start', 'delete']:
        return JsonResponse({
            'success': False,
            'error': 'No connector configured'
        }, status=400)
        
    try:
        
        if action == 'start':
            # Use orchestrator to start replication
            logger.info(f"🚀 Starting replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)

            # Start replication with new simplified flow
            success, message = orchestrator.start_replication()

            if success:
                # Update client replication status
                client = replication_config.client_database.client
                client.replication_enabled = True
                client.replication_status = 'active'
                client.save()

                error = None
            else:
                error = message
                message = None
            
        elif action == 'pause':
            # Use orchestrator to stop replication
            logger.info(f"⏸️ Pausing replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.stop_replication()

            if not success:
                error = message
                message = None
            else:
                error = None
            
        elif action == 'resume':
            # Use orchestrator to resume replication
            logger.info(f"▶️ Resuming replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.start_replication()

            if not success:
                error = message
                message = None
            else:
                error = None
            
        elif action == 'restart':
            # Use orchestrator to restart replication
            logger.info(f"🔄 Restarting replication for config {config_pk} via orchestrator")

            orchestrator = ReplicationOrchestrator(replication_config)
            success, message = orchestrator.restart_replication()

            if not success:
                error = message
                message = None
            else:
                error = None
            
        elif action == 'delete':
            # Use orchestrator to delete replication
            logger.info(f"🗑️ Deleting replication for config {config_pk} via orchestrator")

            # Store client ID before deletion (for redirect)
            client_id = replication_config.client_database.client.id

            # Create orchestrator BEFORE deletion
            orchestrator = ReplicationOrchestrator(replication_config)

            # This will delete the config and all related records
            success, message = orchestrator.delete_replication(delete_topics=False)

            logger.info(f"Delete result: {success} - {message}")

            if not success:
                error = message
                message = None
            else:
                error = None
                # Note: replication_config no longer exists in DB after this point
                # Return redirect URL so frontend knows where to go
                from django.urls import reverse
                messages.success(request, message)
                return JsonResponse({
                    'success': True,
                    'message': message,
                    'redirect_url': reverse('replications_list')
                })

        elif action == 'force_resnapshot':
            # Force resnapshot by clearing offsets and recreating connector
            logger.info(f"🔄 Forcing resnapshot for config {config_pk}")

            from client.tasks import force_resnapshot as force_resnapshot_task

            # Run async task
            result = force_resnapshot_task.apply_async(args=[config_pk])

            success = True
            message = 'Resnapshot initiated. This will delete and recreate the connector with a fresh snapshot of all data.'
            error = None

        else:
            return JsonResponse({
                'success': False,
                'error': f'Unknown action: {action}'
            }, status=400)
        
        if success:
            # Only save if config still exists (not deleted)
            if action != 'delete':
                replication_config.save()
            
            messages.success(request, message)
            return JsonResponse({'success': True, 'message': message})
        else:
            return JsonResponse({
                'success': False,
                'error': error
            }, status=400)
            
    except Exception as e:
        logger.error(f"❌ Error performing action {action}: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["POST"])
def start_replication(request, config_id):
    """Start CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Check if already running
        if config.is_active and config.status == 'active':
            messages.warning(request, "Replication is already running!")
            return redirect('cdc_config_detail', config_id=config_id)

        # Use orchestrator for simplified flow (Option A)
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        # Get force_resync parameter from request (default: False)
        force_resync = request.POST.get('force_resync', 'false').lower() == 'true'

        logger.info(f"Starting replication for config {config_id} (force_resync={force_resync})")

        # Start replication with new simplified flow
        # This will:
        # 1. Validate prerequisites
        # 2. Perform initial SQL copy
        # 3. Start Debezium connector (CDC-only mode)
        # 4. Start consumer with fresh group ID
        success, message = orchestrator.start_replication(force_resync=force_resync)

        if success:
            messages.success(
                request,
                f"🚀 Replication started successfully! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to start replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to start replication: {e}", exc_info=True)
        messages.error(request, f"Failed to start replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def stop_replication(request, config_id):
    """Stop CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        if not config.is_active:
            messages.warning(request, "Replication is not running!")
            return redirect('cdc_config_detail', config_id=config_id)

        # Use orchestrator to stop replication
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"Stopping replication for config {config_id}")

        success, message = orchestrator.stop_replication()

        if success:
            messages.success(
                request,
                f"⏸️ Replication stopped! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to stop replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to stop replication: {e}", exc_info=True)
        messages.error(request, f"Failed to stop replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def restart_replication_view(request, config_id):
    """Restart CDC replication for a config (NEW: Uses orchestrator)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator to restart replication
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"Restarting replication for config {config_id}")

        success, message = orchestrator.restart_replication()

        if success:
            messages.success(
                request,
                f"🔄 Replication restarted! {message}"
            )
        else:
            messages.error(
                request,
                f"❌ Failed to restart replication: {message}"
            )

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to restart replication: {e}", exc_info=True)
        messages.error(request, f"Failed to restart replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["GET"])
def replication_status(request, config_id):
    """Get replication status (AJAX endpoint) - NEW: Uses orchestrator"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator for unified status
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)
        unified_status = orchestrator.get_unified_status()

        # Return comprehensive status
        return JsonResponse({
            'success': True,
            'status': unified_status
        })

    except Exception as e:
        logger.error(f"Failed to get replication status: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def cdc_delete_config(request, config_pk):
    """
    Delete replication configuration (for incomplete setups or when no longer needed)

    Now uses ReplicationOrchestrator for proper cleanup:
    - Stops consumer tasks
    - Pauses and deletes connector
    - Clears offsets
    - Optionally deletes Kafka topics
    - Deletes database configuration
    """
    try:
        replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)

        logger.info(f"Deleting replication config {config_pk} (connector: {replication_config.connector_name})")

        # Check if user wants to delete topics (from query parameter)
        delete_topics = request.GET.get('delete_topics', 'false').lower() == 'true'

        # Use orchestrator for proper cleanup
        orchestrator = ReplicationOrchestrator(replication_config)
        success, message = orchestrator.delete_replication(delete_topics=delete_topics)

        if success:
            logger.info(f"✅ Successfully deleted replication config {config_pk}")

            from django.urls import reverse
            return JsonResponse({
                'success': True,
                'message': message,
                'redirect_url': reverse('replications_list')
            })
        else:
            logger.error(f"❌ Failed to delete replication config: {message}")
            return JsonResponse({
                'success': False,
                'error': message
            }, status=500)

    except Exception as e:
        logger.error(f"❌ Failed to delete configuration: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def cdc_config_details(request, config_pk):
    """
    AJAX endpoint: Get detailed configuration for editing in modal
    """
    try:
        replication_config = get_object_or_404(
            ReplicationConfig.objects.select_related('client_database', 'client_database__client')
            .prefetch_related('table_mappings__column_mappings'),
            pk=config_pk
        )

        client = replication_config.client_database.client
        db_config = replication_config.client_database

        # Build tables array with columns
        tables_data = []
        for table_mapping in replication_config.table_mappings.all():
            columns_data = []
            for col_mapping in table_mapping.column_mappings.all():
                columns_data.append({
                    'source_column': col_mapping.source_column,
                    'target_column': col_mapping.target_column,
                    'data_type': col_mapping.source_type or col_mapping.source_data_type or '',
                    'is_enabled': col_mapping.is_enabled,
                })

            tables_data.append({
                'id': table_mapping.id,
                'source_table': table_mapping.source_table,
                'target_table': table_mapping.target_table,
                'is_enabled': table_mapping.is_enabled,
                'sync_type': table_mapping.sync_type or '',  # Empty string if inheriting
                'sync_frequency': table_mapping.sync_frequency or '',  # Empty string if inheriting
                'columns': columns_data,
            })

        # Get all available tables from source database
        try:
            all_tables = get_table_list(db_config)
            # Filter out tables already configured
            configured_table_names = {tm.source_table for tm in replication_config.table_mappings.all()}
            available_tables = [t for t in all_tables if t not in configured_table_names]
        except Exception as e:
            logger.warning(f"Could not fetch available tables: {e}")
            available_tables = []

        config_data = {
            'id': replication_config.id,
            'client_name': client.name,
            'database_name': db_config.connection_name,
            'database_id': db_config.id,
            'connector_name': replication_config.connector_name or '',
            'status': replication_config.status,
            'status_display': dict(ReplicationConfig.STATUS_CHOICES).get(replication_config.status, replication_config.status).capitalize(),
            'sync_type': replication_config.sync_type,
            'sync_frequency': replication_config.sync_frequency,
            'auto_create_tables': replication_config.auto_create_tables,
            'drop_before_sync': replication_config.drop_before_sync,
            'tables': tables_data,
        }

        return JsonResponse({
            'success': True,
            'config': config_data,
            'available_tables': available_tables,
        })

    except Exception as e:
        logger.error(f"Failed to fetch config details: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)



@require_http_methods(["POST"])
def ajax_get_table_schemas_batch(request, database_pk):
    """
    AJAX endpoint: Get schemas for multiple tables to add to replication
    """
    try:
        db_config = get_object_or_404(ClientDatabase, pk=database_pk)
        data = json.loads(request.body)
        table_names = data.get('tables', [])

        if not table_names:
            return JsonResponse({
                'success': False,
                'error': 'No tables specified'
            }, status=400)

        schemas = []
        for table_name in table_names:
            try:
                schema = get_table_schema(db_config, table_name)
                # Format columns for the modal
                formatted_columns = []
                for col in schema.get('columns', []):
                    formatted_columns.append({
                        'name': col.get('name'),
                        'type': str(col.get('type', '')),  # Convert SQLAlchemy type to string
                        'nullable': col.get('nullable', True),
                    })

                schemas.append({
                    'table_name': table_name,
                    'columns': formatted_columns
                })
            except Exception as e:
                logger.error(f"Error fetching schema for {table_name}: {e}")
                # Continue with other tables even if one fails

        return JsonResponse({
            'success': True,
            'schemas': schemas
        })

    except Exception as e:
        logger.error(f"Failed to fetch table schemas: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
def cdc_unified_status(request, config_pk):
    """
    Get comprehensive unified status for replication (NEW API).

    Returns detailed health information for both connector and consumer.
    Useful for debugging and monitoring.
    """
    try:
        config = ReplicationConfig.objects.get(pk=config_pk)

        # Use new ReplicationOrchestrator for unified status
        from client.replication import ReplicationOrchestrator
        orchestrator = ReplicationOrchestrator(config)

        status = orchestrator.get_unified_status()

        return JsonResponse({
            'success': True,
            'status': status
        })

    except ReplicationConfig.DoesNotExist:
        return JsonResponse({
            'success': False,
            'error': 'Replication configuration not found'
        }, status=404)

    except Exception as e:
        logger.error(f"Failed to get unified status: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def terminate_active_slot_connections(db_config, slot_name):
    """
    Terminate any active connections holding the replication slot.
    
    This is necessary when restarting a connector because PostgreSQL
    only allows one active connection per replication slot.
    
    Args:
        db_config: ClientDatabase instance
        slot_name: Name of the replication slot (e.g., 'debezium_1_2')
    
    Returns:
        tuple: (success, message)
    """
    try:
        logger.info(f"🔍 Checking for active connections on slot: {slot_name}")
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # First, check if slot exists and get active PID
            check_query = text("""
                SELECT active, active_pid 
                FROM pg_replication_slots 
                WHERE slot_name = :slot_name
            """)
            
            result = conn.execute(check_query, {"slot_name": slot_name})
            slot_info = result.fetchone()
            
            if not slot_info:
                logger.warning(f"⚠️  Replication slot '{slot_name}' not found")
                return True, f"Slot {slot_name} doesn't exist (will be created)"
            
            is_active, active_pid = slot_info
            
            if not is_active or active_pid is None:
                logger.info(f"✅ Slot '{slot_name}' is not active")
                return True, f"Slot {slot_name} is available"
            
            logger.warning(f"⚠️  Slot '{slot_name}' is ACTIVE for PID {active_pid}")
            logger.info(f"🔪 Terminating connection PID {active_pid}...")
            
            # Terminate the active backend holding the slot
            terminate_query = text("""
                SELECT pg_terminate_backend(:pid)
            """)
            
            terminate_result = conn.execute(terminate_query, {"pid": active_pid})
            terminated = terminate_result.scalar()
            
            conn.commit()
            
            if terminated:
                logger.info(f"✅ Successfully terminated PID {active_pid}")
                
                # Verify slot is now inactive
                verify_result = conn.execute(check_query, {"slot_name": slot_name})
                verify_info = verify_result.fetchone()
                
                if verify_info and not verify_info[0]:
                    logger.info(f"✅ Slot '{slot_name}' is now available")
                    return True, f"Terminated blocking connection (PID {active_pid})"
                else:
                    logger.warning(f"⚠️  Slot may still be active, waiting for cleanup...")
                    return True, f"Terminated PID {active_pid}, slot should release shortly"
            else:
                logger.warning(f"⚠️  Failed to terminate PID {active_pid}")
                return False, f"Could not terminate blocking connection"
        
        engine.dispose()
        
    except Exception as e:
        error_msg = f"Error managing replication slot: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg


def get_replication_slot_status(db_config, slot_name):
    """
    Get detailed status of a replication slot.
    
    Returns:
        dict: Slot status information or None if not found
    """
    try:
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    slot_name,
                    plugin,
                    slot_type,
                    database,
                    active,
                    active_pid,
                    restart_lsn,
                    confirmed_flush_lsn
                FROM pg_replication_slots
                WHERE slot_name = :slot_name
            """)
            
            result = conn.execute(query, {"slot_name": slot_name})
            row = result.fetchone()
            
            if not row:
                return None
            
            return {
                'slot_name': row[0],
                'plugin': row[1],
                'slot_type': row[2],
                'database': row[3],
                'active': row[4],
                'active_pid': row[5],
                'restart_lsn': str(row[6]) if row[6] else None,
                'confirmed_flush_lsn': str(row[7]) if row[7] else None,
            }
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to get slot status: {e}")
        return None



def wait_for_connector_running(manager, connector_name, timeout=90, interval=3):
    """
    Wait for connector to be in RUNNING state with improved stability checking.
    
    Args:
        manager: DebeziumConnectorManager instance
        connector_name: Name of the connector
        timeout: Maximum seconds to wait (default: 90)
        interval: Seconds between status checks (default: 3)
    
    Returns:
        bool: True if connector is running, False if timeout exceeded
    """
    end_time = time.time() + timeout
    last_status = None
    
    logger.info(f"⏳ Waiting for connector {connector_name} to be RUNNING (timeout: {timeout}s)...")
    
    while time.time() < end_time:
        try:
            result = manager.get_connector_status(connector_name)
            
            if isinstance(result, tuple):
                success, details = result
                if not success or not details:
                    logger.warning(f"⚠️  Failed to get connector status, retrying...")
                    time.sleep(interval)
                    continue
            else:
                details = result
                if not details:
                    logger.warning(f"⚠️  Empty connector status, retrying...")
                    time.sleep(interval)
                    continue
            
            connector_state = details.get('connector', {}).get('state', 'UNKNOWN')
            tasks = details.get('tasks', [])
            
            current_status = f"Connector: {connector_state}, Tasks: {len(tasks)}"
            if current_status != last_status:
                logger.info(f"📊 Status: {current_status}")
                if tasks:
                    task_states = [t.get('state', 'UNKNOWN') for t in tasks]
                    logger.info(f"   Task states: {task_states}")
                last_status = current_status
            
            if connector_state != 'RUNNING':
                logger.debug(f"   Connector state is {connector_state}, waiting...")
                time.sleep(interval)
                continue
            
            if not tasks:
                logger.debug(f"   Connector is RUNNING but no tasks assigned yet...")
                time.sleep(interval)
                continue
            
            task_states = [t.get('state', 'UNKNOWN') for t in tasks]
            acceptable_states = ['RUNNING', 'RESTARTING', 'UNASSIGNED']
            failed_states = ['FAILED', 'DESTROYED']
            
            failed_tasks = [s for s in task_states if s in failed_states]
            if failed_tasks:
                logger.error(f"❌ Connector has failed tasks: {failed_tasks}")
                for task in tasks:
                    if task.get('state') in failed_states:
                        logger.error(f"   Task {task.get('id')} error: {task.get('trace', 'No trace')[:200]}")
                return False
            
            tasks_ok = all(state in acceptable_states for state in task_states)
            
            if connector_state == 'RUNNING' and tasks_ok:
                running_count = sum(1 for s in task_states if s == 'RUNNING')
                if running_count == len(tasks):
                    logger.info(f"✅ All {len(tasks)} connector task(s) are RUNNING")
                else:
                    logger.info(f"✅ Connector is RUNNING with {running_count}/{len(tasks)} tasks RUNNING")
                    logger.info(f"   Other tasks: {[s for s in task_states if s != 'RUNNING']}")
                return True
            
            logger.debug(f"   Tasks not ready yet: {task_states}")
            time.sleep(interval)
            
        except Exception as e:
            logger.warning(f"⚠️  Error checking connector status: {e}")
            time.sleep(interval)
    
    logger.error(f"❌ Timeout after {timeout} seconds")
    return False



def manage_postgresql_publication(db_config, replication_config, table_names):
    """
    Drop and recreate PostgreSQL publication with new table list.
    
    This is necessary because PostgreSQL publications in 'filtered' mode
    cannot be altered - they must be dropped and recreated when the table
    list changes.
    
    Args:
        db_config: ClientDatabase instance
        replication_config: ReplicationConfig instance
        table_names: List of table names to include in publication
    
    Returns:
        tuple: (success, message)
    """
    if db_config.db_type.lower() != 'postgresql':
        return True, "Not PostgreSQL - skipping publication management"
    
    try:
        client_id = db_config.client.id
        db_id = db_config.id
        publication_name = f"debezium_pub_{client_id}_{db_id}"
        
        logger.info(f"📋 Managing PostgreSQL publication: {publication_name}")
        logger.info(f"   Tables to include: {table_names}")
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # Check if publication exists
            check_query = text(
                f"SELECT COUNT(*) FROM pg_publication WHERE pubname = '{publication_name}'"
            )
            result = conn.execute(check_query)
            exists = result.scalar() > 0
            
            if exists:
                logger.info(f"🗑️  Dropping existing publication: {publication_name}")
                drop_query = text(f"DROP PUBLICATION IF EXISTS {publication_name}")
                conn.execute(drop_query)
                conn.commit()
                logger.info(f"✅ Dropped publication successfully")
            else:
                logger.info(f"ℹ️  Publication doesn't exist yet, will create new one")
            
            # Create publication with new table list
            schema_name = 'public'
            tables_qualified = [f"{schema_name}.{table}" for table in table_names]
            
            logger.info(f"🔨 Creating publication with {len(tables_qualified)} tables...")
            
            create_query = text(
                f"CREATE PUBLICATION {publication_name} FOR TABLE {', '.join(tables_qualified)}"
            )
            conn.execute(create_query)
            conn.commit()
            
            logger.info(f"✅ Created publication: {publication_name}")
            logger.info(f"   Tables: {', '.join(tables_qualified)}")
            
        engine.dispose()
        
        return True, f"Publication {publication_name} recreated with {len(table_names)} table(s)"
        
    except Exception as e:
        error_msg = f"Failed to manage publication: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg

"""
Complete cdc_edit_config POST handler with all fixes
Replace the POST section in your cdc_edit_config function (starting around line 1700)
"""

@require_http_methods(["GET", "POST"])
def cdc_edit_config(request, config_pk):
    """
    Edit replication configuration - FIXED for SQL Server consistency
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    db_config = replication_config.client_database
    client = db_config.client

    if request.method == "POST":
        try:
            with transaction.atomic():
                # Update basic configuration
                replication_config.sync_type = request.POST.get('sync_type', replication_config.sync_type)
                replication_config.sync_frequency = request.POST.get('sync_frequency', replication_config.sync_frequency)
                replication_config.auto_create_tables = request.POST.get('auto_create_tables') == 'on'
                replication_config.drop_before_sync = request.POST.get('drop_before_sync') == 'on'
                replication_config.save()

                # Get selected tables
                selected_table_names = request.POST.getlist('selected_tables')
                logger.info(f"Selected tables: {selected_table_names}")

                # ✅ FIX: Build normalized mapping lookup
                existing_mappings = {}
                for tm in replication_config.table_mappings.all():
                    normalized = normalize_sql_server_table_name(tm.source_table, db_config.db_type)
                    existing_mappings[normalized] = tm

                newly_added_tables = []
                removed_table_names = []

                # Find removed tables
                for normalized_name in existing_mappings.keys():
                    if normalized_name not in selected_table_names:
                        removed_table_names.append(normalized_name)

                # Process newly added tables
                for table_name in selected_table_names:
                    normalized_table = normalize_sql_server_table_name(table_name, db_config.db_type)
                    
                    if normalized_table not in existing_mappings:
                        newly_added_tables.append(normalized_table)
                        
                        target_table_name = request.POST.get(f'target_table_new_{table_name}', '')
                        if not target_table_name:
                            target_table_name = generate_target_table_name(
                                db_config.database_name,
                                normalized_table,
                                db_config.db_type
                            )

                        sync_type = request.POST.get(f'sync_type_table_new_{table_name}', 'realtime')
                        incremental_col = request.POST.get(f'incremental_col_table_new_{table_name}', '')
                        conflict_resolution = request.POST.get(f'conflict_resolution_table_new_{table_name}', 'source_wins')

                        table_mapping = TableMapping.objects.create(
                            replication_config=replication_config,
                            source_table=normalized_table,
                            target_table=target_table_name,
                            is_enabled=True,
                            sync_type=sync_type,
                            incremental_column=incremental_col if incremental_col else None,
                            conflict_resolution=conflict_resolution
                        )

                        # Create column mappings
                        try:
                            schema = get_table_schema(db_config, table_name)
                            for column in schema.get('columns', []):
                                col_enabled = request.POST.get(f'enabled_column_new_{table_name}_{column["name"]}') == 'on'
                                target_col_name = request.POST.get(f'target_column_new_{table_name}_{column["name"]}', column['name'])
                                
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=column['name'],
                                    target_column=target_col_name,
                                    source_type=str(column.get('type', 'unknown')),
                                    target_type=str(column.get('type', 'unknown')),
                                    is_enabled=col_enabled
                                )
                        except Exception as e:
                            logger.error(f"Failed to create column mappings: {e}")

                # CREATE TARGET TABLES
                if newly_added_tables and replication_config.auto_create_tables:
                    try:
                        from client.utils.table_creator import create_target_tables
                        create_target_tables(replication_config, specific_tables=newly_added_tables)
                        messages.success(request, f'✅ Created {len(newly_added_tables)} target tables!')
                    except Exception as e:
                        logger.error(f"Failed to create target tables: {e}")
                        messages.warning(request, f'Configuration saved, but table creation failed: {str(e)}')

                # CREATE KAFKA TOPICS
                if newly_added_tables:
                    try:
                        from client.utils.kafka_topic_manager import KafkaTopicManager
                        topic_manager = KafkaTopicManager()
                        topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"

                        # ✅ FIX: Correct topic names based on DB type
                        if db_config.db_type.lower() == 'mssql':
                            # SQL Server keeps schema.table format for topics
                            topic_results = topic_manager.create_cdc_topics_for_tables(
                                server_name=topic_prefix,
                                database=db_config.database_name,
                                table_names=newly_added_tables
                            )
                        elif db_config.db_type.lower() == 'postgresql':
                            # PostgreSQL uses schema name
                            table_names_stripped = [t.split('.')[-1] for t in newly_added_tables]
                            topic_results = topic_manager.create_cdc_topics_for_tables(
                                server_name=topic_prefix,
                                database='public',
                                table_names=table_names_stripped
                            )
                        else:
                            # MySQL
                            topic_results = topic_manager.create_cdc_topics_for_tables(
                                server_name=topic_prefix,
                                database=db_config.database_name,
                                table_names=newly_added_tables
                            )
                    except Exception as e:
                        logger.error(f"Error creating Kafka topics: {e}")

                # Process removed tables
                for table_name in removed_table_names:
                    table_mapping = existing_mappings[table_name]
                    table_mapping.delete()

                # DROP REMOVED TABLES
                if removed_table_names:
                    try:
                        from client.utils.table_creator import drop_target_tables
                        drop_target_tables(replication_config, removed_table_names)
                    except Exception as e:
                        logger.error(f"Failed to drop tables: {e}")

                # Update existing table mappings
                for table_mapping in replication_config.table_mappings.all():
                    table_key = f'table_{table_mapping.id}'
                    new_target_name = request.POST.get(f'target_{table_key}')
                    if new_target_name:
                        table_mapping.target_table = new_target_name

                    table_sync_type = request.POST.get(f'sync_type_{table_key}')
                    if table_sync_type:
                        table_mapping.sync_type = table_sync_type

                    incremental_col = request.POST.get(f'incremental_col_{table_key}')
                    if incremental_col:
                        table_mapping.incremental_column = incremental_col

                    conflict_res = request.POST.get(f'conflict_resolution_{table_key}')
                    if conflict_res:
                        table_mapping.conflict_resolution = conflict_res

                    table_mapping.save()

                    # Update column mappings
                    for column_mapping in table_mapping.column_mappings.all():
                        column_key = f'column_{column_mapping.id}'
                        column_mapping.is_enabled = request.POST.get(f'enabled_{column_key}') == 'on'
                        new_target_col = request.POST.get(f'target_{column_key}')
                        if new_target_col:
                            column_mapping.target_column = new_target_col
                        column_mapping.save()

                restart_connector = request.POST.get('restart_connector') == 'on'
                messages.success(request, '✅ Configuration updated successfully!')

                # ========================================================================
                # CONNECTOR UPDATE
                # ========================================================================
                if replication_config.connector_name:
                    try:
                        manager = DebeziumConnectorManager()
                        all_enabled_tables = replication_config.table_mappings.filter(is_enabled=True)
                        tables_list = [tm.source_table for tm in all_enabled_tables]

                        if tables_list:
                            connector_result = manager.get_connector_status(replication_config.connector_name)
                            connector_exists = isinstance(connector_result, tuple) and connector_result[0]

                            if not connector_exists:
                                logger.warning(f"Connector {replication_config.connector_name} doesn't exist")
                                replication_config.connector_name = None
                                replication_config.status = 'configured'
                                replication_config.is_active = False
                                replication_config.save()
                            else:
                                # ================================================================
                                # ✅ FIX: Handle table changes based on database type
                                # ================================================================
                                if newly_added_tables or removed_table_names:
                                    logger.info(f"🔧 Updating connector (added: {len(newly_added_tables)}, removed: {len(removed_table_names)})")

                                    try:
                                        db_type = db_config.db_type.lower()
                                        
                                        # PostgreSQL-specific handling
                                        if db_type == 'postgresql':
                                            success, message = manage_postgresql_publication(
                                                db_config, 
                                                replication_config, 
                                                tables_list
                                            )
                                            
                                            if not success:
                                                raise Exception(f"Publication management failed: {message}")
                                            
                                            # Clear slot connections
                                            slot_name = f"debezium_{client.id}_{db_config.id}"
                                            term_success, term_msg = terminate_active_slot_connections(db_config, slot_name)
                                            if term_success:
                                                time.sleep(2)
                                            
                                            # Restart connector
                                            restart_success, restart_error = manager.restart_connector(
                                                replication_config.connector_name
                                            )
                                            
                                            if restart_success:
                                                time.sleep(5)
                                        
                                        # Update connector configuration
                                        current_config = manager.get_connector_config(replication_config.connector_name)

                                        if not current_config:
                                            raise Exception("Failed to retrieve connector configuration")

                                        # ✅ FIX: Format table list correctly based on DB type
                                        if db_type == 'postgresql':
                                            # PostgreSQL: schema.table (e.g., 'public.users')
                                            schema_name = 'public'
                                            tables_full = [f"{schema_name}.{t.split('.')[-1]}" for t in tables_list]
                                        elif db_type == 'mssql':
                                            # SQL Server: database.schema.table (e.g., 'AppDB.dbo.Customers')
                                            tables_full = [f"{db_config.database_name}.{t}" for t in tables_list]
                                        else:
                                            # MySQL: database.table (e.g., 'mydb.users')
                                            tables_full = [f"{db_config.database_name}.{t}" for t in tables_list]
                                        
                                        current_config['table.include.list'] = ','.join(tables_full)

                                        # Change snapshot mode for new tables
                                        if newly_added_tables and current_config.get('snapshot.mode') == 'initial':
                                            current_config['snapshot.mode'] = 'when_needed'

                                        # Update connector config
                                        update_success, update_error = manager.update_connector_config(
                                            replication_config.connector_name,
                                            current_config
                                        )

                                        if not update_success:
                                            raise Exception(f"Failed to update config: {update_error}")

                                        time.sleep(3)  # Wait for auto-restart

                                        # Wait for connector to stabilize
                                        wait_success = wait_for_connector_running(
                                            manager, 
                                            replication_config.connector_name,
                                            timeout=90,
                                            interval=3
                                        )
                                        
                                        if not wait_success:
                                            messages.warning(request, 'Configuration updated. Connector may still be starting.')
                                        
                                        # ================================================================
                                        # ✅ FIX: Handle snapshots based on database type
                                        # ================================================================
                                        if newly_added_tables:
                                            if db_type == 'mssql':
                                                # SQL Server: Kafka signals DON'T work for read-only DBs
                                                # Solution: Restart connector (already done above)
                                                logger.info(f"✅ SQL Server: {len(newly_added_tables)} table(s) added!")
                                                logger.info(f"   Snapshot will occur via connector restart")
                                                messages.success(
                                                    request,
                                                    f'✅ {len(newly_added_tables)} table(s) added! '
                                                    f'Snapshot in progress (connector restart).'
                                                )
                                            else:
                                                # MySQL/PostgreSQL: Use Kafka signals
                                                logger.info(f"📡 Sending incremental snapshot signal...")
                                                
                                                try:
                                                    from client.utils.kafka_signal_sender import KafkaSignalSender
                                                    topic_prefix = replication_config.kafka_topic_prefix or f"client_{client.id}_db_{db_config.id}"
                                                    
                                                    signal_sender = KafkaSignalSender()
                                                    
                                                    # Strip schema for signal payload
                                                    tables_for_signal = [t.split('.')[-1] for t in newly_added_tables]
                                                    
                                                    if db_type == 'postgresql':
                                                        success, message = signal_sender.send_incremental_snapshot_signal(
                                                            topic_prefix=topic_prefix,
                                                            database_name=db_config.database_name,
                                                            table_names=tables_for_signal,
                                                            schema_name='public',
                                                            db_type='postgresql'
                                                        )
                                                    else:  # MySQL
                                                        success, message = signal_sender.send_incremental_snapshot_signal(
                                                            topic_prefix=topic_prefix,
                                                            database_name=db_config.database_name,
                                                            table_names=tables_for_signal,
                                                            db_type='mysql'
                                                        )
                                                    
                                                    signal_sender.close()
                                                    
                                                    if success:
                                                        messages.success(
                                                            request,
                                                            f'✅ {len(newly_added_tables)} table(s) added! Incremental snapshot in progress.'
                                                        )
                                                except Exception as e:
                                                    logger.error(f"Snapshot signal failed: {e}")
                                                    messages.warning(request, f'Snapshot signal failed: {str(e)}')
                                        
                                        # Restart consumer for new topics
                                        if replication_config.is_active:
                                            try:
                                                from client.tasks import stop_kafka_consumer, start_kafka_consumer
                                                stop_result = stop_kafka_consumer(replication_config.id)
                                                if stop_result.get('success'):
                                                    time.sleep(2)
                                                    replication_config.refresh_from_db()
                                                    replication_config.is_active = True
                                                    replication_config.status = 'active'
                                                    replication_config.save()
                                                    start_kafka_consumer.apply_async(
                                                        args=[replication_config.id],
                                                        countdown=3
                                                    )
                                            except Exception as e:
                                                logger.error(f"Consumer restart failed: {e}")

                                    except Exception as e:
                                        logger.error(f"Error updating connector: {e}", exc_info=True)
                                        messages.error(request, f'Failed to update connector: {str(e)}')
                                        
                                elif restart_connector:
                                    # No table changes, just restart if requested
                                    if db_config.db_type.lower() == 'postgresql':
                                        slot_name = f"debezium_{client.id}_{db_config.id}"
                                        terminate_active_slot_connections(db_config, slot_name)
                                        time.sleep(2)
                                    
                                    success, error = manager.restart_connector(replication_config.connector_name)
                                    if success:
                                        messages.success(request, '✅ Connector restarted!')
                                    else:
                                        messages.warning(request, f'Restart failed: {error}')

                    except Exception as e:
                        logger.error(f"Error with connector: {e}", exc_info=True)
                        messages.warning(request, f'Configuration saved, connector update failed: {str(e)}')

                # Redirect
                if replication_config.connector_name:
                    return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
                else:
                    return redirect('main-dashboard')

        except Exception as e:
            logger.error(f'Failed to update configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to update configuration: {str(e)}')

    # ========================================================================
    # GET: Show edit form
    # ========================================================================
    try:
        all_table_names = get_table_list(db_config)
        
        # ✅ FIX: Normalize discovered table names
        if db_config.db_type.lower() == 'mssql':
            all_table_names = [
                normalize_sql_server_table_name(t, db_config.db_type) 
                for t in all_table_names
            ]
    except Exception as e:
        logger.error(f"Failed to discover tables: {e}")
        all_table_names = []

    # ✅ FIX: Build normalized mapping lookup
    existing_mappings = {}
    for tm in replication_config.table_mappings.all():
        normalized = normalize_sql_server_table_name(tm.source_table, db_config.db_type)
        existing_mappings[normalized] = tm

    tables_with_columns = []
    for table_name in all_table_names:
        existing_mapping = existing_mappings.get(table_name)

        try:
            schema = get_table_schema(db_config, table_name)
            columns_from_schema = schema.get('columns', [])

            incremental_candidates = [
                col for col in columns_from_schema
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time')
            ]

            try:
                engine = get_database_engine(db_config)
                with engine.connect() as conn:
                    if db_config.db_type.lower() == 'mysql':
                        count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                    elif db_config.db_type.lower() == 'postgresql':
                        count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
                    elif db_config.db_type.lower() == 'mssql':
                        # Handle schema.table format
                        if '.' in table_name:
                            schema_name, actual_table = table_name.split('.', 1)
                            count_query = text(f"SELECT COUNT(*) as cnt FROM [{schema_name}].[{actual_table}]")
                        else:
                            count_query = text(f"SELECT COUNT(*) as cnt FROM [dbo].[{table_name}]")
                    else:
                        count_query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")

                    result = conn.execute(count_query)
                    row = result.fetchone()
                    row_count = row[0] if row else 0
                engine.dispose()
            except:
                row_count = 0

            # ✅ FIX: Generate consistent default target name
            default_target_table_name = generate_target_table_name(
                db_config.database_name,
                table_name,
                db_config.db_type
            )

            table_data = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(columns_from_schema),
                'is_mapped': existing_mapping is not None,
                'mapping': existing_mapping,
                'columns': existing_mapping.column_mappings.all() if existing_mapping else [],
                'all_columns': columns_from_schema,
                'incremental_candidates': incremental_candidates,
                'primary_keys': schema.get('primary_keys', []),
                'default_target_name': default_target_table_name,
            }

            tables_with_columns.append(table_data)

        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")

    # Sort tables: mapped first, then by name
    tables_with_columns.sort(key=lambda t: (not t['is_mapped'], t['table_name']))

    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'tables_with_columns': tables_with_columns,
    }

    return render(request, 'client/cdc/edit_config.html', context)