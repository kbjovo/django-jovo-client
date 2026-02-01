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
from typing import List

from client.models.client import Client
from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
from client.replication import ReplicationOrchestrator

from client.utils.database_utils import (
    get_table_list,
    get_table_schema,
    get_database_engine,
    check_binary_logging,
    check_oracle_logminer
)

logger = logging.getLogger(__name__)



def get_oracle_cdb_name(pdb_name: str) -> str:
    """
    Convert PDB name to CDB root name
    
    Examples:
        XEPDB1 → XE
        ORCLPDB1 → ORCL
        FREEPDB1 → FREE
    """
    pdb_upper = pdb_name.upper()
    
    # Common patterns
    if pdb_upper.endswith('PDB1'):
        return pdb_upper[:-4]  # Remove 'PDB1'
    elif pdb_upper.endswith('PDB'):
        return pdb_upper[:-3]  # Remove 'PDB'
    
    # If no PDB suffix, return as-is (might already be CDB)
    return pdb_name


def is_oracle_pdb(database_name: str) -> bool:
    """
    Check if database name indicates a Pluggable Database
    """
    db_upper = database_name.upper()
    return 'PDB' in db_upper or db_upper.endswith('PDB1')

def normalize_sql_server_table_name(table_name: str, db_type: str) -> str:
    if db_type.lower() != 'mssql':
        return table_name

    if '.' in table_name:
        return table_name
    else:
        return f"dbo.{table_name}"


# =============================================================================
# CONSOLIDATED HELPER FUNCTIONS (eliminates duplications)
# =============================================================================

def get_default_schema(db_type: str, username: str = None) -> str:
    """
    Get default schema name for database type.

    Args:
        db_type: Database type (oracle, postgresql, mssql, mysql)
        username: Database username (required for Oracle)

    Returns:
        Default schema name or None for MySQL
    """
    db_type = db_type.lower()
    if db_type == 'oracle':
        return username.upper() if username else None
    elif db_type == 'postgresql':
        return 'public'
    elif db_type == 'mssql':
        return 'dbo'
    else:  # MySQL
        return None


def normalize_table_name_with_schema(table_name: str, db_type: str, username: str = None) -> str:
    """
    Normalize table name by adding default schema if missing.

    Replaces multiple scattered implementations.

    Args:
        table_name: Table name (may or may not include schema)
        db_type: Database type (oracle, postgresql, mssql, mysql)
        username: Database username (needed for Oracle default schema)

    Returns:
        Normalized table name with schema prefix
    """
    db_type = db_type.lower()

    # If already has schema, return as-is
    if '.' in table_name:
        return table_name

    # Add default schema
    default_schema = get_default_schema(db_type, username)

    if default_schema:
        return f"{default_schema}.{table_name}"
    else:
        # MySQL has no schema concept
        return table_name


def build_row_count_query(table_name: str, db_type: str) -> str:
    """
    Build database-specific row count query.

    Consolidates query building logic from _get_table_row_count().

    Args:
        table_name: Table name (may include schema)
        db_type: Database type

    Returns:
        SQL query string
    """
    db_type = db_type.lower()

    if db_type == 'mysql':
        return f"SELECT COUNT(*) as cnt FROM `{table_name}`"

    elif db_type == 'postgresql':
        # ✅ FIX: PostgreSQL needs double quotes and proper schema handling
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
            # Remove any existing quotes first
            schema = schema.strip('"')
            table = table.strip('"')
            return f'SELECT COUNT(*) as cnt FROM "{schema}"."{table}"'
        else:
            # No schema - assume public
            table = table_name.strip('"')
            return f'SELECT COUNT(*) as cnt FROM "public"."{table}"'

    elif db_type == 'mssql':
        if '.' in table_name:
            schema_name, actual_table = table_name.split('.', 1)
            return f"SELECT COUNT(*) as cnt FROM [{schema_name}].[{actual_table}]"
        else:
            return f"SELECT COUNT(*) as cnt FROM [dbo].[{table_name}]"

    elif db_type == 'oracle':
        if '.' in table_name:
            schema_part, table_part = table_name.rsplit('.', 1)
            return f'SELECT COUNT(*) as cnt FROM {schema_part}.{table_part}'
        else:
            return f'SELECT COUNT(*) as cnt FROM {table_name}'

    else:
        return f"SELECT COUNT(*) as cnt FROM {table_name}"


def discover_tables_generic(db_config, db_type: str):
    """
    Generic table discovery function that works for all database types.

    Consolidates:
    - _discover_postgresql_tables()
    - _discover_mssql_tables()
    - _discover_mysql_tables()
    - Partial logic from _discover_oracle_tables() (Oracle still needs custom handling)

    Args:
        db_config: ClientDatabase instance
        db_type: Database type (postgresql, mssql, mysql)

    Returns:
        List of table names with schemas
    """
    db_type = db_type.lower()
    logger.info(f"🔍 {db_type.upper()}: Discovering tables...")

    try:
        raw_tables = get_table_list(db_config)

        # Apply database-specific normalization
        if db_type == 'postgresql':
            # Ensure schema prefix (default: public)
            tables = []
            for table in raw_tables:
                if '.' not in table:
                    tables.append(f"public.{table}")
                else:
                    tables.append(table)

        elif db_type == 'mssql':
            # Normalize to include schema (default: dbo)
            tables = []
            for table in raw_tables:
                normalized = normalize_sql_server_table_name(table, 'mssql')
                tables.append(normalized)

        else:  # MySQL
            # No schema concept - use tables as-is
            tables = raw_tables

        logger.info(f"   ✅ Found {len(tables)} {db_type.upper()} tables")
        return tables

    except Exception as e:
        logger.error(f"   ❌ {db_type.upper()} discovery failed: {e}")
        return []


def execute_replication_action(request, config_id, action_name: str, orchestrator_method: str,
                               success_emoji: str = "✅", action_display: str = None):
    """
    Generic handler for replication control actions.

    Consolidates:
    - start_replication()
    - stop_replication()
    - restart_replication_view()

    Args:
        request: Django request object
        config_id: Replication config ID
        action_name: Internal action name (for logging)
        orchestrator_method: Method name to call on orchestrator
        success_emoji: Emoji for success message
        action_display: Display name for user messages (defaults to action_name)

    Returns:
        Redirect response
    """
    action_display = action_display or action_name

    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        orchestrator = ReplicationOrchestrator(config)

        logger.info(f"{action_name.capitalize()} replication for config {config_id}")

        # Get the orchestrator method dynamically
        method = getattr(orchestrator, orchestrator_method)

        # Handle start_replication which has force_resync parameter
        if orchestrator_method == 'start_replication':
            force_resync = request.POST.get('force_resync', 'false').lower() == 'true'
            success, message = method(force_resync=force_resync)
        else:
            success, message = method()

        if success:
            messages.success(request, f"{success_emoji} {action_display.capitalize()} successful! {message}")
        else:
            messages.error(request, f"❌ Failed to {action_display}: {message}")

        return redirect('cdc_config_detail', config_id=config_id)

    except Exception as e:
        logger.error(f"Failed to {action_name} replication: {e}", exc_info=True)
        messages.error(request, f"Failed to {action_display} replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


# =============================================================================
# END CONSOLIDATED HELPERS
# =============================================================================


def generate_target_table_name(source_db_name: str, source_table_name: str, db_type: str) -> str:
    """
    Generate safe target table name for destination database.
    
    Handles:
    - Oracle: SCHEMA.TABLE → sourcedb_schema_table
    - PostgreSQL: schema.table → sourcedb_schema_table  
    - SQL Server: schema.table → sourcedb_schema_table
    - MySQL: table → sourcedb_table
    
    Args:
        source_db_name: Source database name
        source_table_name: Source table name (may include schema)
        db_type: Database type (oracle, postgresql, mssql, mysql)
    
    Returns:
        Safe target table name
    """
    # Sanitize database name
    safe_db_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in source_db_name)
    
    # Extract table name (strip schema if present)
    if '.' in source_table_name:
        parts = source_table_name.split('.')
        if len(parts) == 2:
            # schema.table
            schema_part = parts[0]
            table_part = parts[1]
            
            # Sanitize both parts
            safe_schema = ''.join(c if c.isalnum() or c == '_' else '_' for c in schema_part)
            safe_table = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_part)
            
            # Include schema in target name for uniqueness
            return f"{safe_db_name}_{safe_schema}_{safe_table}"
        else:
            # Multiple dots (unusual) - use last part as table
            table_part = parts[-1]
            safe_table = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_part)
            return f"{safe_db_name}_{safe_table}"
    else:
        # No schema - simple table name
        safe_table = ''.join(c if c.isalnum() or c == '_' else '_' for c in source_table_name)
        return f"{safe_db_name}_{safe_table}"


def get_kafka_topics_for_tables(db_config, replication_config, table_mappings):
    client = db_config.client
    topic_prefix = replication_config.kafka_topic_prefix
    db_type = db_config.db_type.lower()
    
    topics = []
    
    logger.info(f"🔍 Generating Kafka topics for {db_type.upper()} database")
    logger.info(f"   Topic prefix: {topic_prefix}")
    logger.info(f"   Database: {db_config.database_name}")
    
    for tm in table_mappings:
        source_table = tm.source_table
        
        if db_type == 'mssql':
            # ✅ SQL Server: {prefix}.{database}.{schema}.{table}
            if '.' in source_table:
                parts = source_table.split('.')
                if len(parts) == 2:
                    schema, table = parts
                else:
                    schema = parts[-2]
                    table = parts[-1]
            else:
                schema = 'dbo'
                table = source_table
            
            topic_name = f"{topic_prefix}.{db_config.database_name}.{schema}.{table}"
            logger.info(f"   ✓ SQL Server: {source_table} → {topic_name}")
            
        elif db_type == 'postgresql':
            # ✅ PostgreSQL: {prefix}.{database}.{table}
            # Use database name instead of schema to match connector's RegexRouter transform
            if '.' in source_table:
                _, table = source_table.rsplit('.', 1)
            else:
                table = source_table

            topic_name = f"{topic_prefix}.{db_config.database_name}.{table}"
            logger.info(f"   ✓ PostgreSQL: {source_table} → {topic_name}")
            
        elif db_type == 'mysql':
            # ✅ MySQL: {prefix}.{database}.{table}
            table = source_table.split('.')[-1] if '.' in source_table else source_table
            topic_name = f"{topic_prefix}.{db_config.database_name}.{table}"
            logger.info(f"   ✓ MySQL: {source_table} → {topic_name}")
            
        elif db_type == 'oracle':
            # ✅ CRITICAL FIX: Oracle topics INCLUDE schema
            if '.' in source_table:
                schema, table = source_table.rsplit('.', 1)
                schema = schema.upper()
                table = table.upper()
            else:
                username = db_config.username.upper()
                schema = username[3:] if username.startswith('C##') else username
                table = source_table.upper()
            
            # ✅ FIX: KEEP schema in topic name
            topic_name = f"{topic_prefix}.{schema}.{table}"
            logger.info(f"   ✓ Oracle: {source_table} → {topic_name}")
            
        else:
            # Generic fallback
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
    topic_prefix = replication_config.kafka_topic_prefix

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
        # SQL Server: schema.table (database name is specified in database.names config)
        # CRITICAL: Do NOT include database name in table.include.list for SQL Server
        if '.' in table_name:
            schema, table = table_name.rsplit('.', 1)
        else:
            schema = schema_name or 'dbo'
            table = table_name

        formatted = f"{schema}.{table}"
        
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
    topic_prefix = replication_config.kafka_topic_prefix

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
    ✅ COMPLETE: Discover tables from source database (all DB types supported)
    
    Database-Specific Behavior:
    - Oracle: Returns fully qualified SCHEMA.TABLE names (handles multi-schema)
    - PostgreSQL: Returns schema.table for non-public schemas
    - SQL Server: Returns schema.table format
    - MySQL: Returns simple table names (no schema concept)
    
    Handles:
    1. Oracle: Common users (C##CDCUSER) and local schemas with duplicate table names
    2. MySQL/PostgreSQL/SQL Server: Standard table discovery
    3. Filters out system/internal tables automatically
    4. Validates table access and gets row counts
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    # ========================================================================
    # POST: User selected tables, proceed to configuration
    # ========================================================================
    if request.method == "POST":
        selected_tables = request.POST.getlist('selected_tables')
        
        if not selected_tables:
            messages.error(request, 'Please select at least one table')
            return redirect('cdc_discover_tables', database_pk=database_pk)
        
        # Store in session for next step
        request.session['selected_tables'] = selected_tables
        request.session['database_pk'] = database_pk
        
        logger.info(f"✅ User selected {len(selected_tables)} tables:")
        for table in selected_tables:
            logger.info(f"   • {table}")
        
        return redirect('cdc_configure_tables', database_pk=database_pk)
    
    # ========================================================================
    # GET: Discover tables from database
    # ========================================================================
    try:
        db_type = db_config.db_type.lower()
        logger.info("=" * 80)
        logger.info(f"🔍 CDC TABLE DISCOVERY STARTED")
        logger.info("=" * 80)
        logger.info(f"   Database Type: {db_type.upper()}")
        logger.info(f"   Database Name: {db_config.database_name}")
        logger.info(f"   Host: {db_config.host}:{db_config.port}")
        logger.info(f"   Username: {db_config.username}")
        
        # Check Oracle LogMiner prerequisites (informational only)
        if db_type == 'oracle':
            try:
                is_enabled, info = check_oracle_logminer(db_config)
                if not is_enabled:
                    warnings = []
                    if not info.get('supplemental_logging'):
                        warnings.append("⚠️ Supplemental logging not enabled. Run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;")
                    if not info.get('archive_log_mode'):
                        warnings.append("⚠️ Archive log mode not enabled.")
                    if not info.get('logminer_available'):
                        warnings.append("⚠️ LogMiner not available.")
                    
                    for warning in warnings:
                        messages.warning(request, warning)
            except Exception as e:
                logger.warning(f"⚠️ Could not check LogMiner status: {e}")
        
        # ====================================================================
        # DATABASE-SPECIFIC TABLE DISCOVERY
        # ====================================================================
        tables = []
        
        # --------------------------------------------------------------------
        # ORACLE: Advanced discovery with multi-schema support
        # --------------------------------------------------------------------
        if db_type == 'oracle':
            logger.info("=" * 80)
            logger.info("🔍 ORACLE TABLE DISCOVERY")
            logger.info("=" * 80)
            
            engine = get_database_engine(db_config)
            
            try:
                with engine.connect() as conn:
                    # Get current user
                    current_user_result = conn.execute(text("SELECT USER FROM DUAL"))
                    current_user = current_user_result.scalar()
                    
                    logger.info(f"✅ Connected as: {current_user}")
                    
                    # Determine if common user or local user
                    username_upper = current_user.upper()
                    is_common_user = username_upper.startswith('C##')
                    
                    if is_common_user:
                        # Common user: C##CDCUSER -> schema is CDCUSER
                        default_schema = username_upper[3:]
                        logger.info(f"🔑 Common user detected: {username_upper}")
                        logger.info(f"   Default schema: {default_schema}")
                    else:
                        # Local user: schema is username
                        default_schema = username_upper
                        logger.info(f"🔑 Local user detected: {username_upper}")
                    
                    # ============================================================
                    # METHOD 1: Tables owned by current user
                    # ============================================================
                    logger.info(f"\n📋 Method 1: Checking user_tables...")
                    
                    user_tables_query = text("""
                        SELECT table_name
                        FROM user_tables 
                        WHERE table_name NOT LIKE 'LOG_MINING%'
                          AND table_name NOT LIKE 'DEBEZIUM%'
                          AND table_name NOT LIKE 'SYS_%'
                          AND table_name NOT LIKE 'MLOG$%'
                          AND table_name NOT LIKE 'RUPD$%'
                          AND table_name NOT LIKE 'BIN$%'
                          AND table_name NOT LIKE 'ISEQ$$_%'
                          AND table_name NOT LIKE 'DR$%'
                          AND table_name NOT LIKE 'SCHEDULER_%'
                          AND table_name NOT LIKE 'LOGMNR_%'
                          AND table_name NOT LIKE 'DBMS_%'
                          AND table_name NOT LIKE 'AQ$%'
                          AND table_name NOT LIKE 'DEF$%'
                        ORDER BY table_name
                    """)
                    
                    result = conn.execute(user_tables_query)
                    user_owned = [row[0] for row in result.fetchall()]
                    
                    # ✅ CRITICAL: Store with SCHEMA.TABLE format
                    discovered_tables = {}
                    for table_name in user_owned:
                        qualified_name = f"{default_schema}.{table_name}"
                        discovered_tables[qualified_name] = {
                            'schema': default_schema,
                            'table': table_name,
                            'source': 'owned'
                        }
                    
                    logger.info(f"   ✅ Found {len(user_owned)} owned tables")
                    
                    # ============================================================
                    # METHOD 2: Tables accessible via grants (other schemas)
                    # ============================================================
                    logger.info(f"\n📋 Method 2: Checking all_tables (granted access)...")
                    
                    all_tables_query = text("""
                        SELECT DISTINCT owner, table_name
                        FROM all_tables
                        WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 
                                           'WMSYS', 'EXFSYS', 'CTXSYS', 'XDB', 'ANONYMOUS',
                                           'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS', 'DVSYS',
                                           'DVF', 'GSMADMIN_INTERNAL', 'OJVMSYS', 'OLAPSYS')
                          AND table_name NOT LIKE 'LOG_MINING%'
                          AND table_name NOT LIKE 'DEBEZIUM%'
                          AND table_name NOT LIKE 'SYS_%'
                          AND table_name NOT LIKE 'MLOG$%'
                          AND table_name NOT LIKE 'RUPD$%'
                          AND table_name NOT LIKE 'BIN$%'
                          AND table_name NOT LIKE 'ISEQ$$_%'
                          AND table_name NOT LIKE 'DR$%'
                          AND table_name NOT LIKE 'SCHEDULER_%'
                          AND table_name NOT LIKE 'LOGMNR_%'
                          AND table_name NOT LIKE 'DBMS_%'
                          AND table_name NOT LIKE 'AQ$%'
                          AND table_name NOT LIKE 'DEF$%'
                        ORDER BY owner, table_name
                    """)
                    
                    result = conn.execute(all_tables_query)
                    granted_tables = [(row[0], row[1]) for row in result.fetchall()]
                    
                    for owner, table_name in granted_tables:
                        # Strip C## prefix from schema names if present
                        schema_name = owner[3:] if owner.startswith('C##') else owner
                        
                        # ✅ CRITICAL: Always use SCHEMA.TABLE format
                        qualified_name = f"{schema_name}.{table_name}"
                        
                        if qualified_name not in discovered_tables:
                            discovered_tables[qualified_name] = {
                                'schema': schema_name,
                                'table': table_name,
                                'source': 'granted'
                            }
                    
                    logger.info(f"   ✅ Found {len(granted_tables)} accessible tables (all schemas)")
                    
                    # ============================================================
                    # METHOD 3: Tables accessible via synonyms
                    # ============================================================
                    logger.info(f"\n📋 Method 3: Checking all_synonyms...")
                    
                    synonyms_query = text("""
                        SELECT 
                            synonym_name,
                            table_owner,
                            table_name
                        FROM all_synonyms
                        WHERE owner = :current_user
                          AND table_owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP')
                          AND table_name NOT LIKE 'LOG_MINING%'
                          AND table_name NOT LIKE 'DEBEZIUM%'
                          AND table_name NOT LIKE 'SYS_%'
                          AND table_name NOT LIKE 'ISEQ$$_%'
                          AND table_name NOT LIKE 'DR$%'
                          AND table_name NOT LIKE 'SCHEDULER_%'
                          AND table_name NOT LIKE 'LOGMNR_%'
                          AND table_name NOT LIKE 'DBMS_%'
                        ORDER BY synonym_name
                    """)
                    
                    result = conn.execute(synonyms_query, {"current_user": username_upper})
                    synonyms = [(row[0], row[1], row[2]) for row in result.fetchall()]
                    
                    for synonym_name, table_owner, table_name in synonyms:
                        schema_name = table_owner[3:] if table_owner.startswith('C##') else table_owner
                        
                        # ✅ CRITICAL: Use actual table name, not synonym
                        qualified_name = f"{schema_name}.{table_name}"
                        
                        if qualified_name not in discovered_tables:
                            discovered_tables[qualified_name] = {
                                'schema': schema_name,
                                'table': table_name,
                                'source': 'synonym',
                                'synonym': synonym_name
                            }
                    
                    logger.info(f"   ✅ Found {len(synonyms)} accessible synonyms")
                    
                    # ============================================================
                    # Build final table list
                    # ============================================================
                    logger.info("\n" + "=" * 80)
                    logger.info(f"📊 ORACLE DISCOVERY SUMMARY")
                    logger.info("=" * 80)
                    logger.info(f"Total unique tables found: {len(discovered_tables)}")
                    
                    if discovered_tables:
                        # Group by schema for better logging
                        schemas = {}
                        for qualified_name, info in discovered_tables.items():
                            schema = info['schema']
                            if schema not in schemas:
                                schemas[schema] = []
                            schemas[schema].append(info['table'])
                        
                        logger.info(f"\nTables by schema:")
                        for schema, schema_tables in sorted(schemas.items()):
                            logger.info(f"   {schema}: {len(schema_tables)} tables")
                            for table in sorted(schema_tables)[:5]:  # Show first 5
                                logger.info(f"      • {table}")
                            if len(schema_tables) > 5:
                                logger.info(f"      ... and {len(schema_tables) - 5} more")
                        
                        # ✅ CRITICAL: Return fully qualified names
                        tables = list(discovered_tables.keys())
                        
                    else:
                        logger.error(f"❌ No accessible tables found!")
                        messages.error(
                            request,
                            f'No accessible tables found for user "{username_upper}". '
                            f'Please verify grants or create synonyms.'
                        )
                        return redirect('client_detail', pk=db_config.client.pk)
                    
                    logger.info("=" * 80)
                
                engine.dispose()
                
            except Exception as e:
                logger.error(f"❌ Oracle table discovery failed: {e}", exc_info=True)
                messages.error(request, f'Failed to discover Oracle tables: {str(e)}')
                return redirect('client_detail', pk=db_config.client.pk)
        
        # --------------------------------------------------------------------
        # POSTGRESQL: Schema-aware discovery
        # --------------------------------------------------------------------
        elif db_type == 'postgresql':
            logger.info("🔍 PostgreSQL table discovery...")
            
            try:
                # Get basic table list
                raw_tables = get_table_list(db_config)
                
                # ✅ Ensure schema prefix for non-public tables
                tables = []
                for table in raw_tables:
                    if '.' not in table:
                        # Add public schema if no schema specified
                        tables.append(f"public.{table}")
                    else:
                        tables.append(table)
                
                logger.info(f"✅ Found {len(tables)} PostgreSQL tables")
                
            except Exception as e:
                logger.error(f"❌ PostgreSQL discovery failed: {e}", exc_info=True)
                tables = []
        
        # --------------------------------------------------------------------
        # SQL SERVER: Schema-aware discovery
        # --------------------------------------------------------------------
        elif db_type == 'mssql':
            logger.info("🔍 SQL Server table discovery...")
            
            try:
                # Get basic table list
                raw_tables = get_table_list(db_config)
                
                # ✅ Ensure schema prefix (default: dbo)
                tables = []
                for table in raw_tables:
                    if '.' not in table:
                        # Add dbo schema if no schema specified
                        tables.append(f"dbo.{table}")
                    else:
                        tables.append(table)
                
                logger.info(f"✅ Found {len(tables)} SQL Server tables")
                
            except Exception as e:
                logger.error(f"❌ SQL Server discovery failed: {e}", exc_info=True)
                tables = []
        
        # --------------------------------------------------------------------
        # MYSQL: Simple table discovery (no schema concept)
        # --------------------------------------------------------------------
        elif db_type == 'mysql':
            logger.info("🔍 MySQL table discovery...")
            
            try:
                tables = get_table_list(db_config)
                logger.info(f"✅ Found {len(tables)} MySQL tables")
                
            except Exception as e:
                logger.error(f"❌ MySQL discovery failed: {e}", exc_info=True)
                tables = []
        
        # --------------------------------------------------------------------
        # UNKNOWN: Fallback to basic discovery
        # --------------------------------------------------------------------
        else:
            logger.warning(f"⚠️ Unknown database type: {db_type}")
            try:
                tables = get_table_list(db_config)
                logger.info(f"✅ Found {len(tables)} tables using generic method")
            except Exception as e:
                logger.error(f"❌ Generic discovery failed: {e}", exc_info=True)
                tables = []
        
        # ====================================================================
        # COMMON FILTERING: Remove system tables
        # ====================================================================
        if tables:
            EXCLUDED_PATTERNS = [
                'debezium_signal',  # Our own signal table
                'log_mining_flush',  # Oracle CDC table
                'sys_',
                'information_schema',
                'pg_',
                'sql_',
            ]
            
            filtered_tables = []
            for table in tables:
                table_lower = table.lower()
                if not any(pattern.lower() in table_lower for pattern in EXCLUDED_PATTERNS):
                    filtered_tables.append(table)
            
            excluded_count = len(tables) - len(filtered_tables)
            if excluded_count > 0:
                logger.info(f"✅ Filtered out {excluded_count} system/internal tables")
            
            tables = filtered_tables
        
        # ====================================================================
        # VALIDATE: We have tables
        # ====================================================================
        if not tables:
            logger.error(f"❌ No tables found after filtering!")
            messages.error(
                request,
                'No tables found. Please check database connection and permissions.'
            )
            return redirect('client_detail', pk=db_config.client.pk)
        
        logger.info(f"✅ Final table count: {len(tables)}")
        
        # ====================================================================
        # GET TABLE DETAILS (row counts, columns, PKs)
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("📊 GATHERING TABLE DETAILS")
        logger.info("=" * 80)
        
        tables_with_info = []
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            for table_name in tables:
                logger.debug(f"📊 Processing table: {table_name}")
                row_count = 0
                columns = []
                primary_keys = []
                error_msg = None
                
                try:
                    # --------------------------------------------------------
                    # Get row count
                    # --------------------------------------------------------
                    try:
                        if db_type == 'oracle':
                            # Oracle: Handle schema.table format
                            if '.' in table_name:
                                schema_part, table_part = table_name.rsplit('.', 1)
                                count_query = text(f'SELECT COUNT(*) as cnt FROM {schema_part}.{table_part}')
                            else:
                                count_query = text(f'SELECT COUNT(*) as cnt FROM {table_name}')
                        
                        elif db_type == 'mysql':
                            count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                        
                        elif db_type == 'postgresql':
                            count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
                        
                        elif db_type == 'mssql':
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
                        
                    except Exception as e:
                        logger.warning(f"   ⚠️ Count query failed: {e}")
                        row_count = 0
                    
                    # --------------------------------------------------------
                    # Get table schema (columns, types, PKs)
                    # --------------------------------------------------------
                    try:
                        schema = get_table_schema(db_config, table_name)
                        columns = schema.get('columns', [])
                        primary_keys = schema.get('primary_keys', [])
                        
                        logger.debug(f"   ✅ Schema: {len(columns)} columns, PKs: {primary_keys}")
                    except Exception as e:
                        logger.warning(f"   ⚠️ Schema query failed: {e}")
                        if not error_msg:
                            error_msg = f"Schema query failed: {str(e)[:100]}"
                    
                    # --------------------------------------------------------
                    # Check for timestamp column (for incremental sync)
                    # --------------------------------------------------------
                    has_timestamp = any(
                        'timestamp' in str(col.get('type', '')).lower() or 
                        'datetime' in str(col.get('type', '')).lower() or
                        'date' in str(col.get('type', '')).lower() or
                        str(col.get('name', '')).upper().endswith('_AT') or
                        str(col.get('name', '')).upper().endswith('_DATE') or
                        str(col.get('name', '')).upper().endswith('_TIME')
                        for col in columns
                    )
                    
                    # --------------------------------------------------------
                    # Generate target table name (for destination)
                    # --------------------------------------------------------
                    target_table_name = generate_target_table_name(
                        db_config.database_name,
                        table_name,
                        db_type
                    )
                    
                    # --------------------------------------------------------
                    # Display name (for UI - strip schema for cleaner look)
                    # --------------------------------------------------------
                    if '.' in table_name:
                        display_table_name = table_name  # Keep schema for Oracle/PostgreSQL
                    else:
                        display_table_name = table_name
                    
                    # --------------------------------------------------------
                    # Add to results
                    # --------------------------------------------------------
                    table_info = {
                        'name': table_name,  # ✅ CRITICAL: Fully qualified name
                        'display_name': display_table_name,
                        'target_name': target_table_name,
                        'row_count': row_count,
                        'column_count': len(columns),
                        'has_timestamp': has_timestamp,
                        'primary_keys': primary_keys,
                    }
                    
                    if error_msg:
                        table_info['error'] = error_msg
                    
                    # ✅ FIX: Extract schema for ALL tables with schema prefix (not just Oracle)
                    if '.' in table_name:
                        parts = table_name.rsplit('.', 1)
                        table_info['schema'] = parts[0]
                        table_info['table_only'] = parts[1]
                    else:
                        table_info['schema'] = None
                        table_info['table_only'] = table_name
                    
                    tables_with_info.append(table_info)
                    logger.debug(f"   ✅ Added: {display_table_name} ({row_count} rows, {len(columns)} cols)")
                    
                except Exception as e:
                    logger.error(f"   ❌ FAILED to process table {table_name}: {str(e)}")
                    # Still add table but with error flag
                    tables_with_info.append({
                        'name': table_name,
                        'display_name': table_name,
                        'target_name': table_name,
                        'row_count': 0,
                        'column_count': 0,
                        'has_timestamp': False,
                        'primary_keys': [],
                        'schema': None,
                        'table_only': table_name,
                        'error': f"Failed to analyze: {str(e)[:100]}"
                    })
        
        engine.dispose()
        
        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        total_rows = sum(t['row_count'] for t in tables_with_info)
        total_cols = sum(t['column_count'] for t in tables_with_info)
        tables_with_errors = [t for t in tables_with_info if 'error' in t]
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 DISCOVERY COMPLETE")
        logger.info("=" * 80)
        logger.info(f"   Total tables: {len(tables_with_info)}")
        logger.info(f"   Total rows: {total_rows:,}")
        logger.info(f"   Total columns: {total_cols}")
        logger.info(f"   Tables with errors: {len(tables_with_errors)}")
        logger.info("=" * 80)
        
        if tables_with_errors:
            logger.warning(f"⚠️ {len(tables_with_errors)} table(s) had errors")
            messages.warning(
                request,
                f"{len(tables_with_errors)} table(s) could not be fully analyzed. "
                f"These are marked with ⚠️ in the table list."
            )
        
        # ====================================================================
        # RENDER TEMPLATE
        # ====================================================================
        context = {
            'db_config': db_config,
            'client': db_config.client,
            'tables': tables_with_info,
            'total_tables': len(tables_with_info),
            'total_rows': total_rows,
            'total_columns': total_cols,
            'tables_with_errors': len(tables_with_errors),
            'db_type': db_type,  # ✅ Pass to template for Oracle-specific rendering
        }
        
        return render(request, 'client/cdc/discover_tables.html', context)
        
    except Exception as e:
        logger.error(f'❌ DISCOVERY FAILED: {str(e)}', exc_info=True)
        messages.error(request, f'Failed to discover tables: {str(e)}')
        return redirect('client_detail', pk=db_config.client.pk)


@require_http_methods(["GET", "POST"])
def cdc_configure_tables(request, database_pk):
    """
    ✅ COMPLETE FIXED VERSION - Configure selected tables with column selection and mapping
    
    CRITICAL FIXES FOR ORACLE:
    1. ✅ Preserves SCHEMA.TABLE format from discovery
    2. ✅ Correct schema name handling (no C## prefix)
    3. ✅ Proper UPPERCASE handling
    4. ✅ Case-insensitive column matching
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    # Get selected tables from session
    selected_tables = request.session.get('selected_tables', [])
    if not selected_tables:
        messages.error(request, 'No tables selected')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    # ============================================================================
    # POST: Save configuration
    # ============================================================================
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

                logger.info(f"✅ Created ReplicationConfig: {replication_config.id}")
                
                # ================================================================
                # Create TableMappings for each table
                # ================================================================
                for table_name in selected_tables:
                    sync_type = request.POST.get(f'sync_type_{table_name}', '')
                    if not sync_type:
                        sync_type = request.POST.get('sync_type', 'realtime')
                    
                    incremental_col = request.POST.get(f'incremental_col_{table_name}', '')
                    conflict_resolution = request.POST.get(f'conflict_resolution_{table_name}', 'source_wins')

                    # ============================================================
                    # ✅ FIX: Normalize table name based on database type
                    # ============================================================
                    db_type = db_config.db_type.lower()
                    
                    if db_type == 'oracle':
                        # ✅ Oracle: Keep schema.table format or add username as schema
                        if '.' in table_name:
                            # Already has schema prefix - ensure UPPERCASE
                            parts = table_name.split('.', 1)
                            normalized_source = f"{parts[0].upper()}.{parts[1].upper()}"
                            logger.info(f"Oracle: Table already qualified: {table_name} → {normalized_source}")
                        else:
                            # Table without schema - add username as schema
                            username = db_config.username.upper()
                            # Remove C## prefix if present
                            schema_name = username[3:] if username.startswith('C##') else username
                            normalized_source = f"{schema_name}.{table_name.upper()}"
                            logger.info(f"Oracle: Added schema prefix: {table_name} → {normalized_source}")
                    
                    elif db_type == 'mssql':
                        # SQL Server: Ensure schema.table format (default: dbo)
                        normalized_source = normalize_sql_server_table_name(table_name, db_type)
                        logger.info(f"SQL Server: Normalized to: {normalized_source}")
                    
                    elif db_type == 'postgresql':
                        # PostgreSQL: Ensure schema.table format (default: public)
                        if '.' not in table_name:
                            normalized_source = f"public.{table_name}"
                            logger.info(f"PostgreSQL: Added schema prefix: {table_name} → {normalized_source}")
                        else:
                            normalized_source = table_name
                            logger.info(f"PostgreSQL: Table already qualified: {normalized_source}")
                    
                    else:
                        # MySQL: No schema concept (database.table handled by connector)
                        normalized_source = table_name
                        logger.info(f"MySQL: Using table name as-is: {normalized_source}")
                    
                    # ============================================================
                    # Get target table name
                    # ============================================================
                    target_table_name = request.POST.get(f'target_table_name_{table_name}', '')
                    if not target_table_name:
                        source_db_name = db_config.database_name
                        target_table_name = generate_target_table_name(
                            source_db_name,
                            normalized_source,
                            db_type
                        )

                    logger.info(f"✅ Creating TableMapping:")
                    logger.info(f"   Source: {normalized_source}")
                    logger.info(f"   Target: {target_table_name}")

                    # ============================================================
                    # Create TableMapping
                    # ============================================================
                    table_mapping = TableMapping.objects.create(
                        replication_config=replication_config,
                        source_table=normalized_source,  # ✅ Now includes schema for Oracle/PostgreSQL/SQL Server
                        target_table=target_table_name,
                        is_enabled=True,
                        sync_type=sync_type,
                        incremental_column=incremental_col if sync_type == 'incremental' else None,
                        incremental_column_type='timestamp' if incremental_col else '',
                        conflict_resolution=conflict_resolution,
                    )
                    
                    logger.info(f"✅ Created TableMapping: {normalized_source} -> {target_table_name}")
                    
                    # ============================================================
                    # Get selected columns
                    # ============================================================
                    selected_columns = request.POST.getlist(f'selected_columns_{table_name}')
                    
                    if selected_columns:
                        # User selected specific columns
                        logger.info(f"Creating column mappings for selected columns: {len(selected_columns)}")
                        
                        for source_column in selected_columns:
                            target_column = request.POST.get(
                                f'column_mapping_{table_name}_{source_column}', 
                                source_column
                            )
                            
                            try:
                                schema = get_table_schema(db_config, table_name)
                                columns = schema.get('columns', [])
                                primary_keys = schema.get('primary_keys', [])

                                # ✅ CRITICAL FIX: Case-insensitive column lookup for Oracle
                                column_info = None
                                for col in columns:
                                    if col.get('name', '').upper() == source_column.upper():
                                        column_info = col
                                        break

                                if not column_info:
                                    logger.warning(f"Column {source_column} not found in schema, using VARCHAR")

                                source_type = str(column_info.get('type', 'VARCHAR')) if column_info else 'VARCHAR'

                                # Check if this column is a primary key (case-insensitive)
                                is_pk = any(pk.upper() == source_column.upper() for pk in primary_keys)

                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=source_column,
                                    target_column=target_column or source_column,
                                    source_type=source_type,
                                    target_type=source_type,
                                    is_enabled=True,
                                    is_primary_key=is_pk,
                                )

                                pk_indicator = " (PK)" if is_pk else ""
                                logger.debug(f"   ✅ Mapped column: {source_column} -> {target_column or source_column}{pk_indicator}")

                            except Exception as e:
                                logger.error(f"Error creating column mapping for {source_column}: {e}")
                    else:
                        # Include all columns by default
                        try:
                            schema = get_table_schema(db_config, table_name)
                            columns = schema.get('columns', [])
                            primary_keys = schema.get('primary_keys', [])

                            logger.info(f"Creating {len(columns)} column mappings for {table_name} (PKs: {primary_keys})")

                            for col in columns:
                                col_name = col.get('name')
                                col_type = str(col.get('type', 'VARCHAR'))

                                target_column = request.POST.get(
                                    f'column_mapping_{table_name}_{col_name}',
                                    col_name
                                )

                                # Check if this column is a primary key
                                is_pk = col_name in primary_keys

                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=col_name,
                                    target_column=target_column or col_name,
                                    source_type=col_type,
                                    target_type=col_type,
                                    is_enabled=True,
                                    is_primary_key=is_pk,
                                )

                            logger.info(f"   ✅ Created {len(columns)} default column mappings ({len(primary_keys)} PKs)")
                            
                        except Exception as e:
                            logger.error(f"Error creating default column mappings: {e}")
                
                # ================================================================
                # Auto-create tables - DISABLED (handled by sink connector)
                # ================================================================
                # ✅ FIX: Sink connector now automatically creates tables with correct naming
                # Tables will be created as: {database}_{table} (e.g., kbe_busyuk_items)
                # Manual table creation was causing DUPLICATE tables to be created
                # if replication_config.auto_create_tables:
                #     try:
                #         from client.utils.table_creator import create_target_tables
                #         create_target_tables(replication_config)
                #         messages.success(request, '✅ Target tables created successfully!')
                #     except Exception as e:
                #         logger.error(f"Failed to create target tables: {e}")
                #         messages.warning(request, f'Configuration saved, but failed to create target tables: {str(e)}')

                messages.success(request, '✅ CDC configuration saved successfully! Target tables will be auto-created by sink connector.')
                
                # Clean up session
                request.session.pop('selected_tables', None)
                request.session.pop('database_pk', None)
                
                return redirect('cdc_create_connector', config_pk=replication_config.pk)
                
        except Exception as e:
            logger.error(f'Failed to save configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to save configuration: {str(e)}')
    
    # ============================================================================
    # GET: Show configuration form
    # ============================================================================
    tables_with_columns = []
    
    for table_name in selected_tables:
        try:
            logger.info(f"🔍 Getting schema for table: {table_name}")
            
            # ================================================================
            # ✅ FIX: Oracle table names may need special handling
            # ================================================================
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
                    
                    # Try with schema prefix if not present
                    if not columns and '.' not in table_name:
                        username = db_config.username.upper()
                        schema_name = username[3:] if username.startswith('C##') else username
                        table_with_schema = f"{schema_name}.{table_name}"
                        logger.info(f"   Trying with schema: {table_with_schema}")
                        schema = get_table_schema(db_config, table_with_schema)
                        columns = schema.get('columns', [])
            
            if not columns:
                logger.error(f"❌ Could not get columns for {table_name} after all attempts")
                messages.warning(request, f'Could not load schema for table {table_name}')
                continue
            
            logger.info(f"✅ Found {len(columns)} columns for {table_name}")
            
            # ================================================================
            # Find incremental candidates
            # ================================================================
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
            
            # ================================================================
            # ✅ Generate consistent target table name based on DB type
            # ================================================================
            db_type = db_config.db_type.lower()
            
            if db_type == 'oracle':
                # Oracle: Ensure schema.table format (already done in discovery)
                if '.' in table_name:
                    parts = table_name.split('.', 1)
                    normalized_table = f"{parts[0].upper()}.{parts[1].upper()}"
                else:
                    username = db_config.username.upper()
                    schema_name = username[3:] if username.startswith('C##') else username
                    normalized_table = f"{schema_name}.{table_name.upper()}"
            elif db_type == 'mssql':
                # SQL Server: Ensure schema.table format (default: dbo)
                normalized_table = normalize_sql_server_table_name(table_name, db_type)
            elif db_type == 'postgresql':
                # PostgreSQL: Ensure schema.table format (default: public)
                if '.' not in table_name:
                    normalized_table = f"public.{table_name}"
                else:
                    normalized_table = table_name
            else:
                # MySQL: No schema normalization
                normalized_table = table_name
            
            source_db_name = db_config.database_name
            target_table_name = generate_target_table_name(source_db_name, normalized_table, db_type)

            tables_with_columns.append({
                'name': normalized_table,  # ✅ Now includes schema for Oracle/PostgreSQL/SQL Server
                'target_name': target_table_name,
                'columns': columns,
                'primary_keys': schema.get('primary_keys', []),
                'incremental_candidates': incremental_candidates,
            })
            
            logger.debug(
                f"Added table {normalized_table}: "
                f"{len(columns)} columns, {len(incremental_candidates)} incremental candidates"
            )
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}", exc_info=True)
            messages.warning(request, f'Could not load schema for table {table_name}: {str(e)}')
    
    if not tables_with_columns:
        messages.error(request, 'Could not load schema for any selected tables. Please check database permissions.')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    logger.info(f"✅ Loaded {len(tables_with_columns)} tables for configuration")
    
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

            # Update replication config - DO NOT set connector_name here
            # The orchestrator will generate and set the versioned connector name
            # Topic prefix includes version for JMX uniqueness
            replication_config.kafka_topic_prefix = f"client_{client.id}_db_{db_config.id}_v_{replication_config.connector_version}"
            replication_config.status = 'configured'
            replication_config.is_active = False
            replication_config.save()

            logger.info(f"✅ Configuration saved")
            logger.info(f"   Tables configured: {len(formatted_tables)}")
            logger.info(f"   Topic prefix: {replication_config.kafka_topic_prefix}")

            # Check if user wants to auto-start
            auto_start = request.POST.get('auto_start', 'false').lower() == 'true'

            if auto_start:
                logger.info(f"🚀 Auto-starting replication")

                from client.replication import ReplicationOrchestrator
                orchestrator = ReplicationOrchestrator(replication_config)

                success, message = orchestrator.start_replication()
                from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
                from django.conf import settings
                # Use internal Kafka servers for Docker environment
                kafka_bootstrap = settings.DEBEZIUM_CONFIG.get('KAFKA_INTERNAL_SERVERS', 'kafka-1:29092,kafka-2:29092,kafka-3:29092')
                kafka_topic_manager = KafkaTopicManager(bootstrap_servers=kafka_bootstrap)
                topics_list = kafka_topic_manager.list_topics()
                print(f'topics_list_by_kafka: {topics_list}')
                logger.info(f'topics_list_by_kafka: {topics_list}')
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

        # Check if connectors actually exist in Kafka Connect
        source_connector_exists = unified_status['source_connector']['state'] not in ['NOT_FOUND', 'ERROR']
        sink_connector_exists = unified_status['sink_connector']['state'] not in ['NOT_FOUND', 'ERROR', 'NOT_CONFIGURED']

        # Determine what actions are available based on state
        can_start = (
            replication_config.status == 'configured' and
            not replication_config.is_active
        )

        can_stop = (
            replication_config.is_active and
            (source_connector_exists or sink_connector_exists)
        )

        can_restart = (
            replication_config.is_active and
            (source_connector_exists or sink_connector_exists)
        )

        context = {
            'replication_config': replication_config,
            'client': replication_config.client_database.client,
            'unified_status': unified_status,
            'connector_exists': source_connector_exists,  # For backward compatibility
            'source_connector_exists': source_connector_exists,
            'sink_connector_exists': sink_connector_exists,
            'can_start': can_start,
            'can_stop': can_stop,
            'can_restart': can_restart,
            'enabled_table_mappings': replication_config.table_mappings.filter(is_enabled=True),
            # Legacy fields for backward compatibility with template
            'connector_status': {
                'connector': {'state': unified_status['source_connector']['state']},
                'tasks': unified_status['source_connector'].get('tasks', [])
            } if source_connector_exists else None,
            'tasks': unified_status['source_connector'].get('tasks', []),
            'exists': source_connector_exists,
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

            # Always delete topics when deleting connector (mandatory)
            delete_topics = True
            logger.info(f"Delete topics: {delete_topics} (always True - mandatory)")

            # This will delete the config and all related records
            success, message = orchestrator.delete_replication(delete_topics=delete_topics)

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
    """Start CDC replication for a config (uses consolidated action handler)"""
    # Check if already running
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        if config.is_active and config.status == 'active':
            messages.warning(request, "Replication is already running!")
            return redirect('cdc_config_detail', config_id=config_id)
    except Exception:
        pass

    # Use consolidated replication action handler
    return execute_replication_action(
        request=request,
        config_id=config_id,
        action_name='start',
        orchestrator_method='start_replication',
        success_emoji='🚀',
        action_display='Replication started'
    )


@require_http_methods(["POST"])
def stop_replication(request, config_id):
    """Stop CDC replication for a config (uses consolidated action handler)"""
    # Check if running
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        if not config.is_active:
            messages.warning(request, "Replication is not running!")
            return redirect('cdc_config_detail', config_id=config_id)
    except Exception:
        pass

    # Use consolidated replication action handler
    return execute_replication_action(
        request=request,
        config_id=config_id,
        action_name='stop',
        orchestrator_method='stop_replication',
        success_emoji='⏸️',
        action_display='Replication stopped'
    )


@require_http_methods(["POST"])
def restart_replication_view(request, config_id):
    """Restart CDC replication for a config (uses consolidated action handler)"""
    return execute_replication_action(
        request=request,
        config_id=config_id,
        action_name='restart',
        orchestrator_method='restart_replication',
        success_emoji='🔄',
        action_display='Replication restarted'
    )


@require_http_methods(["POST"])
def create_topics(request, config_id):
    """Create Kafka topics explicitly before starting replication"""
    return execute_replication_action(
        request=request,
        config_id=config_id,
        action_name='create_topics',
        orchestrator_method='create_topics',
        success_emoji='📝',
        action_display='Topics created'
    )


@require_http_methods(["GET"])
def list_topics(request, config_id):
    """List all Kafka topics for this replication (AJAX endpoint)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)

        # Use orchestrator to list topics
        from client.replication import ReplicationOrchestrator

        orchestrator = ReplicationOrchestrator(config)
        success, topics, message = orchestrator.list_topics()

        return JsonResponse({
            'success': success,
            'topics': topics,
            'message': message,
            'count': len(topics) if success else 0
        })

    except Exception as e:
        import traceback
        return JsonResponse({
            'success': False,
            'topics': [],
            'message': str(e),
            'count': 0,
            'traceback': traceback.format_exc()
        }, status=500)


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

        # Always delete topics when deleting connector (mandatory)
        delete_topics = True
        logger.info(f"Delete topics: {delete_topics} (always True - mandatory)")

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
        table_names: List of table names (can be qualified like "public.table" or unqualified like "table")
    
    Returns:
        tuple: (success, message)
    """
    if db_config.db_type.lower() != 'postgresql':
        return True, "Not PostgreSQL - skipping publication management"
    
    try:
        client_id = db_config.client.id
        db_id = db_config.id
        publication_name = f"debezium_pub_{client_id}_{db_id}"
        
        logger.info("=" * 80)
        logger.info(f"📋 MANAGING POSTGRESQL PUBLICATION")
        logger.info("=" * 80)
        logger.info(f"   Publication: {publication_name}")
        logger.info(f"   Input tables: {table_names}")
        logger.info(f"   Table count: {len(table_names)}")
        
        # ✅ CRITICAL: Validate we have tables
        if not table_names:
            error_msg = "❌ Cannot create publication with empty table list"
            logger.error(error_msg)
            logger.error("   This usually means no tables are enabled in the replication config")
            return False, error_msg
        
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            # Check if publication exists
            check_query = text(
                "SELECT COUNT(*) FROM pg_publication WHERE pubname = :pub_name"
            )
            result = conn.execute(check_query, {"pub_name": publication_name})
            exists = result.scalar() > 0
            
            if exists:
                logger.info(f"🗑️  Dropping existing publication: {publication_name}")
                drop_query = text(f"DROP PUBLICATION IF EXISTS {publication_name}")
                conn.execute(drop_query)
                conn.commit()
                logger.info(f"✅ Dropped publication successfully")
            else:
                logger.info(f"ℹ️  Publication doesn't exist yet, will create new one")
            
            # ✅ FIX: Handle both qualified and unqualified table names
            tables_qualified = []
            for table in table_names:
                table_stripped = table.strip()
                
                if not table_stripped:
                    logger.warning(f"⚠️  Skipping empty table name")
                    continue
                
                if '.' in table_stripped:
                    # Already qualified (e.g., "public.busy_items_greenera")
                    tables_qualified.append(table_stripped)
                    logger.debug(f"   ✓ Already qualified: {table_stripped}")
                else:
                    # Add default schema
                    qualified = f"public.{table_stripped}"
                    tables_qualified.append(qualified)
                    logger.debug(f"   ✓ Added schema: {table_stripped} → {qualified}")
            
            logger.info(f"")
            logger.info(f"📊 Publication Summary:")
            logger.info(f"   Input tables: {len(table_names)}")
            logger.info(f"   Qualified tables: {len(tables_qualified)}")
            logger.info(f"")
            logger.info(f"📋 Final table list:")
            for i, table in enumerate(tables_qualified, 1):
                logger.info(f"   {i}. {table}")
            
            # ✅ CRITICAL: Double-check we have tables before creating SQL
            if not tables_qualified:
                error_msg = "❌ No valid tables to add to publication after qualification"
                logger.error(error_msg)
                logger.error(f"   Original input: {table_names}")
                return False, error_msg
            
            # Create the publication
            tables_list = ', '.join(tables_qualified)
            create_sql = f"CREATE PUBLICATION {publication_name} FOR TABLE {tables_list}"
            
            logger.info(f"")
            logger.info(f"🔨 Creating publication...")
            logger.info(f"   SQL: {create_sql}")
            
            create_query = text(create_sql)
            conn.execute(create_query)
            conn.commit()
            
            logger.info(f"")
            logger.info(f"✅ Publication created successfully!")
            logger.info("=" * 80)
            
        engine.dispose()
        
        return True, f"Publication {publication_name} recreated with {len(tables_qualified)} table(s)"
        
    except Exception as e:
        error_msg = f"Failed to manage publication: {str(e)}"
        logger.error("=" * 80)
        logger.error(f"❌ PUBLICATION MANAGEMENT FAILED")
        logger.error("=" * 80)
        logger.error(f"   Error: {error_msg}")
        logger.error(f"   Publication: {publication_name}")
        logger.error(f"   Input tables: {table_names}")
        logger.error("=" * 80)
        logger.error(f"Full traceback:", exc_info=True)
        return False, error_msg
    
    

def create_sqlserver_signal_table(db_config):
    """
    Create the debezium_signal table in SQL Server if it doesn't exist.

    Returns:
        tuple: (success, message)
    """
    try:
        from client.utils.database_utils import get_database_engine
        from sqlalchemy import text

        engine = get_database_engine(db_config)

        with engine.connect() as conn:
            # Check if signal table exists
            check_sql = text("""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dbo'
                AND TABLE_NAME = 'debezium_signal'
            """)

            result = conn.execute(check_sql)
            row = result.fetchone()
            table_exists = row[0] > 0

            if table_exists:
                logger.info("✅ Signal table already exists: dbo.debezium_signal")
                engine.dispose()
                return True, "Signal table exists"

            # Create signal table
            logger.info("📋 Creating signal table: dbo.debezium_signal")

            create_sql = text("""
                CREATE TABLE dbo.debezium_signal (
                    id VARCHAR(42) PRIMARY KEY,
                    type VARCHAR(32) NOT NULL,
                    data VARCHAR(2048)
                )
            """)

            conn.execute(create_sql)
            conn.commit()

            logger.info("✅ Signal table created successfully!")

        engine.dispose()
        return True, "Signal table created"

    except Exception as e:
        error_msg = f"Failed to create signal table: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg



def handle_sqlserver_new_tables_via_recovery_snapshot(manager, replication_config, db_config, newly_added_tables):
    """
    Handle SQL Server new tables using snapshot.mode=recovery (PRODUCTION RECOMMENDED)

    This is Debezium's official approach for adding tables to an existing connector.
    It snapshots ONLY new tables that have no offset data.

    Process:
    1. Update connector config with snapshot.mode=recovery
    2. Restart connector
    3. Debezium snapshots ONLY tables without offsets (new tables)
    4. Revert to snapshot.mode=when_needed after completion

    REQUIREMENTS:
    1. New tables must be in table.include.list
    2. New tables must have CDC enabled
    3. New tables must have a primary key

    Returns:
        tuple: (success, message)
    """
    try:
        logger.info("=" * 80)
        logger.info("📋 SQL SERVER NEW TABLES - RECOVERY SNAPSHOT (PRODUCTION METHOD)")
        logger.info("=" * 80)
        logger.info(f"Tables to snapshot: {newly_added_tables}")

        connector_name = replication_config.connector_name

        # Step 1: Get current connector config
        logger.info("\n🔧 Step 1: Getting current connector configuration...")
        current_config = manager.get_connector_config(connector_name)

        if not current_config:
            return False, f"Failed to get connector config for {connector_name}"

        current_snapshot_mode = current_config.get('snapshot.mode', 'when_needed')
        logger.info(f"   Current snapshot.mode: {current_snapshot_mode}")

        # Step 2: Update to recovery mode
        logger.info("\n🔧 Step 2: Updating connector to recovery mode...")
        updated_config = current_config.copy()
        updated_config['snapshot.mode'] = 'recovery'

        logger.info("   Setting snapshot.mode=recovery")
        logger.info("   This will snapshot ONLY tables without offset data")

        success = manager.update_connector_config(connector_name, updated_config)

        if not success:
            return False, "Failed to update connector config to recovery mode"

        logger.info("✅ Connector updated to recovery mode")

        # Step 3: Restart connector to trigger recovery snapshot
        logger.info("\n🔧 Step 3: Restarting connector to trigger snapshot...")

        restart_success = manager.restart_connector(connector_name)

        if not restart_success:
            logger.warning("⚠️ Connector restart failed, but may still work")
        else:
            logger.info("✅ Connector restarted successfully")

        logger.info("\n⏳ Connector will now:")
        logger.info("   1. Resume from last CDC position for existing tables (Customers)")
        logger.info("   2. Take INITIAL SNAPSHOT of new tables (Products)")
        logger.info("   3. Merge both streams into Kafka topics")

        logger.info("\n📊 Expected behavior:")
        for table in newly_added_tables:
            logger.info(f"   • {table}: Full snapshot of all existing rows")

        logger.info("\n" + "=" * 80)
        logger.info("✅ RECOVERY SNAPSHOT INITIATED")
        logger.info("=" * 80)
        logger.info("\nIMPORTANT:")
        logger.info("   • Monitor connector logs for snapshot progress")
        logger.info("   • After snapshot completes, optionally revert to snapshot.mode=when_needed")
        logger.info("   • You can check progress via connector status API")
        logger.info("\n⚠️ KNOWN LIMITATION:")
        logger.info("   • Recovery snapshot may only capture SCHEMA, not DATA")
        logger.info("   • If data is missing, use incremental snapshot signal as fallback")
        logger.info("=" * 80)

        return True, f"Recovery snapshot initiated for {len(newly_added_tables)} table(s). Monitor logs for completion."

    except Exception as e:
        error_msg = f"Failed to initiate recovery snapshot: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg


def trigger_incremental_snapshot_for_mssql(replication_config, db_config, table_names):
    """
    Trigger incremental snapshot for MS SQL tables using Kafka signals.

    ✅ FIXED: Uses correct 2-part table format (schema.table) to match connector config

    This is a fallback method when recovery snapshot fails to snapshot data.
    Common scenario: Recovery mode only snapshots schema when connector has existing offsets.

    Args:
        replication_config: ReplicationConfig instance
        db_config: ClientDatabase instance
        table_names: List of table names (e.g., ['dbo.Products'])

    Returns:
        tuple: (success, message)
    """
    try:
        logger.info("=" * 80)
        logger.info("📡 MS SQL INCREMENTAL SNAPSHOT VIA DATABASE TABLE (RECOMMENDED METHOD)")
        logger.info("=" * 80)
        logger.info(f"Tables to snapshot: {table_names}")

        from client.utils.kafka_signal_sender import KafkaSignalSender

        client = db_config.client
        topic_prefix = replication_config.kafka_topic_prefix

        logger.info(f"Topic prefix: {topic_prefix}")
        logger.info(f"Database: {db_config.database_name}")
        logger.info(f"Signal table: {db_config.database_name}.dbo.debezium_signal")

        # Get source database engine for inserting signal
        source_engine = get_database_engine(db_config)

        # Initialize signal sender (uses settings by default)
        signal_sender = KafkaSignalSender()

        # ✅ CRITICAL FIX: Use DATABASE TABLE method for SQL Server (more reliable)
        # Database table signals are processed more consistently than Kafka topic signals for SQL Server
        success, message = signal_sender.send_incremental_snapshot_signal_via_db(
            engine=source_engine,
            database_name=db_config.database_name,
            table_names=table_names,  # Already in dbo.Products format
            schema_name='dbo',
            signal_table='dbo.debezium_signal'
        )

        signal_sender.close()

        if success:
            logger.info("=" * 80)
            logger.info("✅ INCREMENTAL SNAPSHOT SIGNAL SENT VIA DATABASE TABLE!")
            logger.info("=" * 80)
            logger.info("Monitor connector logs:")
            logger.info("  docker logs kafka-connect 2>&1 | grep -i 'snapshot'")
            logger.info("")
            logger.info("Check Kafka topic for data:")
            for table in table_names:
                topic = f"{topic_prefix}.{db_config.database_name}.{table}"
                logger.info(f"  Topic: {topic}")
            logger.info("")
            logger.info("Check signal table:")
            logger.info(f"  SELECT * FROM {db_config.database_name}.dbo.debezium_signal ORDER BY id DESC")
            logger.info("=" * 80)
            return True, message
        else:
            return False, message

    except Exception as e:
        error_msg = f"Failed to send incremental snapshot signal: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg


def revert_snapshot_mode_after_recovery(manager, connector_name, original_mode='when_needed'):
    """
    Revert snapshot mode back to original after recovery snapshot completes.

    Call this after confirming the recovery snapshot has completed.

    Args:
        manager: KafkaConnectManager instance
        connector_name: Name of the connector
        original_mode: Mode to revert to (default: when_needed)

    Returns:
        tuple: (success, message)
    """
    try:
        logger.info("=" * 80)
        logger.info("🔄 REVERTING SNAPSHOT MODE")
        logger.info("=" * 80)

        current_config = manager.get_connector_config(connector_name)

        if not current_config:
            return False, f"Failed to get connector config for {connector_name}"

        current_mode = current_config.get('snapshot.mode', 'unknown')
        logger.info(f"Current mode: {current_mode}")
        logger.info(f"Reverting to: {original_mode}")

        updated_config = current_config.copy()
        updated_config['snapshot.mode'] = original_mode

        success = manager.update_connector_config(connector_name, updated_config)

        if success:
            logger.info("✅ Snapshot mode reverted successfully")
            logger.info(f"   {current_mode} → {original_mode}")
            logger.info("=" * 80)
            return True, f"Snapshot mode reverted to {original_mode}"
        else:
            return False, "Failed to update connector config"

    except Exception as e:
        error_msg = f"Failed to revert snapshot mode: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, error_msg


def ensure_signal_table_in_config(current_config, db_config, db_type):
    """
    Ensure signal table is included in connector's table.include.list
    """
    if db_type.lower() != 'mssql':
        return current_config
    
    try:
        table_include_list = current_config.get('table.include.list', '')
        
        if not table_include_list:
            logger.warning("⚠️ No table.include.list found")
            return current_config
        
        tables = [t.strip() for t in table_include_list.split(',')]
        signal_table = 'dbo.debezium_signal'
        
        if signal_table not in tables:
            logger.info(f"➕ Adding signal table: {signal_table}")
            tables.append(signal_table)
            current_config['table.include.list'] = ','.join(tables)
            
            logger.info("✅ Updated table.include.list:")
            for table in tables:
                logger.info(f"   • {table}")
        else:
            logger.info(f"✅ Signal table already in config")
        
        return current_config
    
    except Exception as e:
        logger.error(f"❌ Failed to update config: {e}")
        return current_config


@require_http_methods(["GET", "POST"])
def cdc_edit_config(request, config_pk):
    """
    Edit CDC replication configuration
    
    Modular design with DB-specific handling:
    - Oracle: Full discovery (owned + granted + synonyms)
    - PostgreSQL: Publication management
    - SQL Server: Recovery snapshots
    - MySQL: Standard CDC
    """
    # ========================================================================
    # INITIALIZE
    # ========================================================================
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    db_config = replication_config.client_database
    client = db_config.client
    db_type = db_config.db_type.lower()
    
    logger.info(f"=" * 80)
    logger.info(f"CDC EDIT CONFIG: {replication_config.id}")
    logger.info(f"=" * 80)
    logger.info(f"Database: {db_config.connection_name} ({db_type.upper()})")
    logger.info(f"Method: {request.method}")

    # ========================================================================
    # POST REQUEST
    # ========================================================================
    if request.method == "POST":
        return _handle_post_request(request, replication_config, db_config, client, db_type)
    
    # ========================================================================
    # GET REQUEST
    # ========================================================================
    return _handle_get_request(request, replication_config, db_config, client, db_type)


# ============================================================================
# POST REQUEST HANDLER
# ============================================================================

def _handle_post_request(request, replication_config, db_config, client, db_type):
    """Handle POST request - Update configuration"""
    try:
        with transaction.atomic():
            # Step 1: Update basic configuration
            _update_basic_config(request, replication_config)
            
            # Step 2: Analyze table changes
            changes = _analyze_table_changes(request, replication_config, db_type)
            
            # Step 3: Process new tables
            if changes['newly_added']:
                _process_new_tables(request, replication_config, db_config, changes['newly_added'], db_type)
            
            # Step 4: Handle infrastructure (targets, topics)
            _handle_infrastructure(request, replication_config, db_config, client, changes, db_type)
            
            # Step 5: Update existing mappings
            _update_existing_mappings(request, replication_config)
            
            # Step 6: Update connector if exists
            if replication_config.connector_name:
                _update_connector(request, replication_config, db_config, client, changes, db_type)
            
            # Step 7: Success message and redirect
            messages.success(request, '✅ Configuration updated successfully!')
            
            if replication_config.connector_name:
                return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
            else:
                return redirect('main-dashboard')
                
    except Exception as e:
        logger.error(f'❌ Failed to update configuration: {e}', exc_info=True)
        messages.error(request, f'Failed to update configuration: {str(e)}')
        return redirect('cdc_edit_config', config_pk=replication_config.pk)


def _update_basic_config(request, replication_config):
    """Update basic replication settings"""
    logger.info(f"\n📝 Updating basic configuration...")
    
    replication_config.sync_type = request.POST.get('sync_type', replication_config.sync_type)
    replication_config.sync_frequency = request.POST.get('sync_frequency', replication_config.sync_frequency)
    replication_config.auto_create_tables = request.POST.get('auto_create_tables') == 'on'
    replication_config.drop_before_sync = request.POST.get('drop_before_sync') == 'on'
    replication_config.save()
    
    logger.info(f"   ✅ Sync Type: {replication_config.sync_type}")
    logger.info(f"   ✅ Sync Frequency: {replication_config.sync_frequency}")
    logger.info(f"   ✅ Auto-create: {replication_config.auto_create_tables}")


def _analyze_table_changes(request, replication_config, db_type):
    """Analyze which tables were added/removed"""
    logger.info(f"\n🔍 Analyzing table changes...")
    
    selected_tables = request.POST.getlist('selected_tables')
    logger.info(f"   Selected tables: {len(selected_tables)}")
    
    # Build existing mappings
    existing_mappings = {}
    for tm in replication_config.table_mappings.all():
        normalized = normalize_sql_server_table_name(tm.source_table, db_type)
        existing_mappings[normalized] = tm
    
    logger.info(f"   Existing mappings: {len(existing_mappings)}")
    
    # Find additions and removals
    newly_added = []
    removed = []
    
    for table_name in selected_tables:
        normalized = normalize_sql_server_table_name(table_name, db_type)
        if normalized not in existing_mappings:
            newly_added.append(normalized)
    
    for normalized_name in existing_mappings.keys():
        if normalized_name not in selected_tables:
            removed.append(normalized_name)
    
    logger.info(f"")
    logger.info(f"   ➕ Newly added: {len(newly_added)}")
    logger.info(f"   ➖ Removed: {len(removed)}")
    
    return {
        'newly_added': newly_added,
        'removed': removed,
        'existing_mappings': existing_mappings
    }


def _process_new_tables(request, replication_config, db_config, newly_added_tables, db_type):
    """Process newly added tables - create mappings"""
    logger.info(f"\n➕ Processing {len(newly_added_tables)} new table(s)...")
    
    for table_name in newly_added_tables:
        # Normalize table name
        normalized_source = _normalize_table_name_for_db(table_name, db_config, db_type)
        
        # Get target table name
        target_table_name = request.POST.get(f'target_table_new_{table_name}', '')
        if not target_table_name:
            target_table_name = generate_target_table_name(
                db_config.database_name,
                normalized_source,
                db_type
            )
        
        # Get table settings
        sync_type = request.POST.get(f'sync_type_table_new_{table_name}', 'realtime')
        incremental_col = request.POST.get(f'incremental_col_table_new_{table_name}', '')
        conflict_resolution = request.POST.get(f'conflict_resolution_table_new_{table_name}', 'source_wins')
        
        logger.info(f"")
        logger.info(f"   📋 Creating mapping: {normalized_source} → {target_table_name}")
        
        # Create TableMapping
        table_mapping = TableMapping.objects.create(
            replication_config=replication_config,
            source_table=normalized_source,
            target_table=target_table_name,
            is_enabled=True,
            sync_type=sync_type,
            incremental_column=incremental_col if incremental_col else None,
            conflict_resolution=conflict_resolution
        )
        
        # Create ColumnMappings
        _create_column_mappings(request, table_mapping, db_config, table_name)
    
    logger.info(f"   ✅ Processed {len(newly_added_tables)} new table(s)")


def _normalize_table_name_for_db(table_name, db_config, db_type):
    """Normalize table name based on database type"""
    if db_type == 'oracle':
        # Oracle: Ensure SCHEMA.TABLE (UPPERCASE)
        if '.' in table_name:
            parts = table_name.split('.', 1)
            return f"{parts[0].upper()}.{parts[1].upper()}"
        else:
            username = db_config.username.upper()
            schema_name = username[3:] if username.startswith('C##') else username
            return f"{schema_name}.{table_name.upper()}"
    
    elif db_type == 'mssql':
        # SQL Server: schema.table (default: dbo)
        return normalize_sql_server_table_name(table_name, db_type)
    
    elif db_type == 'postgresql':
        # PostgreSQL: schema.table (default: public)
        if '.' not in table_name:
            return f"public.{table_name}"
        return table_name
    
    else:
        # MySQL: no schema
        return table_name


def _create_column_mappings(request, table_mapping, db_config, table_name):
    """Create column mappings for a table"""
    try:
        schema = get_table_schema(db_config, table_name)
        columns = schema.get('columns', [])
        primary_keys = schema.get('primary_keys', [])

        logger.info(f"      Creating {len(columns)} column mappings (PKs: {primary_keys})...")

        for column in columns:
            col_name = column.get('name')
            col_type = str(column.get('type', 'VARCHAR'))

            col_enabled = request.POST.get(f'enabled_column_new_{table_name}_{col_name}') == 'on'
            target_col_name = request.POST.get(f'target_column_new_{table_name}_{col_name}', col_name)

            # Check if this column is a primary key
            is_pk = col_name in primary_keys

            ColumnMapping.objects.create(
                table_mapping=table_mapping,
                source_column=col_name,
                target_column=target_col_name,
                source_type=col_type,
                target_type=col_type,
                is_enabled=col_enabled,
                is_primary_key=is_pk,
            )

        logger.info(f"      ✅ Created {len(columns)} column mappings ({len(primary_keys)} PKs)")
        
    except Exception as e:
        logger.error(f"      ❌ Column mapping failed: {e}")
        raise


def _handle_infrastructure(request, replication_config, db_config, client, changes, db_type):
    """Handle target tables, Kafka topics, and removals"""

    # ✅ FIX: Target table creation disabled - sink connector handles it automatically
    # Manual creation was causing DUPLICATE tables (both manual + sink connector auto-create)
    # Sink connector now creates tables with format: {database}_{table}
    # if changes['newly_added'] and replication_config.auto_create_tables:
    #     _create_target_tables(request, replication_config, changes['newly_added'])

    # Create Kafka topics for new tables (manual creation for better control)
    if changes['newly_added']:
        _create_kafka_topics(request, replication_config, db_config, client, changes['newly_added'], db_type)

    # Remove deleted tables
    if changes['removed']:
        _remove_tables(request, replication_config, changes['removed'], changes['existing_mappings'])


def _create_target_tables(request, replication_config, newly_added_tables):
    """Create target tables in destination database"""
    try:
        logger.info(f"\n🔨 Creating target tables...")
        
        from client.utils.table_creator import create_target_tables
        create_target_tables(replication_config, specific_tables=newly_added_tables)
        
        logger.info(f"   ✅ Created {len(newly_added_tables)} target table(s)")
        messages.success(request, f'✅ Created {len(newly_added_tables)} target tables!')
        
    except Exception as e:
        logger.error(f"   ❌ Failed to create target tables: {e}", exc_info=True)
        messages.warning(request, f'Configuration saved, but table creation failed: {str(e)}')


def _create_kafka_topics(request, replication_config, db_config, client, newly_added_tables, db_type):
    """
    Create Kafka topics for newly added tables.

    Manual creation provides:
    - Predictable timing (topics exist before snapshot starts)
    - Immediate validation of topic names
    - Custom topic configuration (partitions, retention, etc.)
    """
    try:
        logger.info(f"\n📡 Creating Kafka topics for {len(newly_added_tables)} table(s)...")

        from jovoclient.utils.kafka.topic_manager import KafkaTopicManager
        topic_manager = KafkaTopicManager()
        topic_prefix = replication_config.kafka_topic_prefix

        # Database-specific topic creation
        if db_type == 'mssql':
            # SQL Server: {prefix}.{database}.{schema}.{table}
            topic_results = topic_manager.create_cdc_topics_for_tables(
                server_name=topic_prefix,
                database=db_config.database_name,
                table_names=newly_added_tables
            )

        elif db_type == 'postgresql':
            # PostgreSQL: {prefix}.{database}.{table}
            # Use database name instead of schema to match connector's RegexRouter transform
            table_names_stripped = [t.split('.')[-1] for t in newly_added_tables]
            topic_results = topic_manager.create_cdc_topics_for_tables(
                server_name=topic_prefix,
                database=db_config.database_name,
                table_names=table_names_stripped
            )

        elif db_type == 'oracle':
            # ✅ ORACLE FIX: Table names are already in SCHEMA.TABLE format
            # Create topics: client_1_db_5.CDC_USER.ORDERS
            # NOT: client_1_db_5.XEPDB1.CDC_USER.ORDERS
            oracle_topics = []
            for table in newly_added_tables:
                if '.' in table:
                    # Already has schema.table format (e.g., "CDC_USER.ORDERS")
                    topic_name = f"{topic_prefix}.{table}"
                else:
                    # Fallback: assume CDC_USER schema
                    topic_name = f"{topic_prefix}.CDC_USER.{table}"

                oracle_topics.append(topic_name)
                logger.info(f"   → {table} → {topic_name}")

            topic_results = topic_manager.create_topics_bulk(oracle_topics)

        else:  # MySQL
            # MySQL: {prefix}.{database}.{table}
            topic_results = topic_manager.create_cdc_topics_for_tables(
                server_name=topic_prefix,
                database=db_config.database_name,
                table_names=newly_added_tables
            )

        # Log results
        success_count = sum(1 for success in topic_results.values() if success)
        logger.info(f"   ✅ Created {success_count}/{len(newly_added_tables)} Kafka topic(s)")

        if success_count > 0:
            messages.success(request, f'✅ Created {success_count} Kafka topic(s) for new tables')

    except Exception as e:
        logger.error(f"   ❌ Error creating Kafka topics: {e}", exc_info=True)
        messages.warning(request, f'Topics creation failed: {str(e)}')


def _remove_tables(request, replication_config, removed_tables, existing_mappings):
    """Remove deleted tables and their mappings"""
    try:
        logger.info(f"\n🗑️ Removing {len(removed_tables)} table(s)...")
        
        for table_name in removed_tables:
            table_mapping = existing_mappings.get(table_name)
            if table_mapping:
                logger.info(f"   🗑️ Deleting: {table_mapping.source_table}")
                table_mapping.delete()
        
        logger.info(f"   ✅ Deleted {len(removed_tables)} table mapping(s)")
        
        # Drop target tables
        try:
            from client.utils.table_creator import drop_target_tables
            drop_target_tables(replication_config, removed_tables)
            logger.info(f"   ✅ Dropped target tables")
        except Exception as e:
            logger.warning(f"   ⚠️ Failed to drop tables: {e}")
            
    except Exception as e:
        logger.error(f"   ❌ Failed to remove tables: {e}", exc_info=True)
        messages.warning(request, f'Failed to remove some tables: {str(e)}')


def _update_existing_mappings(request, replication_config):
    """Update existing table and column mappings"""
    logger.info(f"\n🔄 Updating existing table mappings...")
    
    for table_mapping in replication_config.table_mappings.all():
        table_key = f'table_{table_mapping.id}'
        
        # Update target table name
        new_target_name = request.POST.get(f'target_{table_key}')
        if new_target_name and new_target_name != table_mapping.target_table:
            table_mapping.target_table = new_target_name
        
        # Update sync type
        table_sync_type = request.POST.get(f'sync_type_{table_key}')
        if table_sync_type:
            table_mapping.sync_type = table_sync_type
        
        # Update incremental column
        incremental_col = request.POST.get(f'incremental_col_{table_key}')
        if incremental_col:
            table_mapping.incremental_column = incremental_col
        
        # Update conflict resolution
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
    
    logger.info(f"   ✅ Updated existing mappings")


def _update_connector(request, replication_config, db_config, client, changes, db_type):
    """Update Debezium connector configuration"""
    logger.info(f"\n" + "=" * 80)
    logger.info(f"🔧 CONNECTOR UPDATE")
    logger.info(f"=" * 80)
    
    manager = DebeziumConnectorManager()
    connector_name = replication_config.connector_name
    
    # Check if connector exists
    connector_result = manager.get_connector_status(connector_name)
    connector_exists = isinstance(connector_result, tuple) and connector_result[0]
    
    if not connector_exists:
        logger.warning(f"❌ Connector {connector_name} doesn't exist")
        replication_config.connector_name = None
        replication_config.status = 'configured'
        replication_config.is_active = False
        replication_config.save()
        messages.warning(request, 'Connector no longer exists. Configuration saved.')
        return
    
    # Get all enabled tables
    all_enabled_tables = replication_config.table_mappings.filter(is_enabled=True)
    tables_list = [tm.source_table for tm in all_enabled_tables]
    
    if not tables_list:
        logger.warning(f"⚠️ No tables enabled")
        return
    
    logger.info(f"Enabled tables: {len(tables_list)}")
    
    # Handle table changes or restart
    restart_connector = request.POST.get('restart_connector') == 'on'
    
    if changes['newly_added'] or changes['removed']:
        _update_connector_for_table_changes(
            request, manager, replication_config, db_config, client, 
            tables_list, changes, db_type
        )
    elif restart_connector:
        _restart_connector_simple(request, manager, replication_config, db_config, client, db_type)


def _update_connector_for_table_changes(request, manager, replication_config, db_config, 
                                        client, tables_list, changes, db_type):
    """Update connector when tables are added/removed"""
    logger.info(f"\n🔄 Updating connector for table changes...")
    logger.info(f"   Added: {len(changes['newly_added'])}")
    logger.info(f"   Removed: {len(changes['removed'])}")
    
    try:
        # Database-specific pre-update handling
        if db_type == 'postgresql':
            _handle_postgresql_update(manager, replication_config, db_config, client, tables_list)
        
        # Update connector configuration
        _update_connector_config(manager, replication_config, db_config, tables_list, db_type)
        
        # Wait for connector to stabilize
        wait_success = wait_for_connector_running(manager, replication_config.connector_name, timeout=90)
        
        if not wait_success:
            messages.warning(request, 'Configuration updated. Connector may still be starting.')
        
        # Handle snapshots for new tables
        if changes['newly_added']:
            _handle_snapshots_for_new_tables(request, manager, replication_config, db_config, client, changes['newly_added'], db_type)
        
        # Restart consumer for new topics
        if replication_config.is_active:
            _restart_consumer(replication_config)
            
    except Exception as e:
        logger.error(f"❌ Connector update failed: {e}", exc_info=True)
        messages.error(request, f'Failed to update connector: {str(e)}')


def _handle_postgresql_update(manager, replication_config, db_config, client, tables_list):
    """PostgreSQL-specific: Update publication and clear slot"""
    logger.info(f"\n🐘 PostgreSQL: Updating publication...")
    
    success, message = manage_postgresql_publication(db_config, replication_config, tables_list)
    
    if not success:
        raise Exception(f"Publication management failed: {message}")
    
    logger.info(f"   ✅ {message}")
    
    # Clear slot connections
    slot_name = f"debezium_{client.id}_{db_config.id}"
    term_success, term_msg = terminate_active_slot_connections(db_config, slot_name)
    if term_success:
        logger.info(f"   ✅ {term_msg}")
        time.sleep(2)
    
    # Restart connector
    restart_success, restart_error = manager.restart_connector(replication_config.connector_name)
    
    if restart_success:
        logger.info(f"   ✅ Connector restarted")
        time.sleep(5)


def _update_connector_config(manager, replication_config, db_config, tables_list, db_type):
    """Update connector's table.include.list"""
    logger.info(f"\n📝 Updating connector configuration...")
    
    current_config = manager.get_connector_config(replication_config.connector_name)
    
    if not current_config:
        raise Exception("Failed to retrieve connector configuration")
    
    # Format table list based on DB type
    tables_full = _format_tables_for_connector(db_config, tables_list, db_type)
    
    logger.info(f"   Final table list: {len(tables_full)} tables")
    for table in tables_full[:5]:  # Show first 5
        logger.info(f"      • {table}")
    if len(tables_full) > 5:
        logger.info(f"      ... and {len(tables_full) - 5} more")
    
    # Update table.include.list
    current_config['table.include.list'] = ','.join(tables_full)
    
    # Update snapshot mode if needed
    if db_type != 'mssql' and current_config.get('snapshot.mode') == 'initial':
        current_config['snapshot.mode'] = 'when_needed'
        logger.info(f"   📸 Changed snapshot.mode to 'when_needed'")
    
    # Apply update
    update_success, update_error = manager.update_connector_config(
        replication_config.connector_name,
        current_config
    )
    
    if not update_success:
        raise Exception(f"Failed to update config: {update_error}")
    
    logger.info(f"   ✅ Connector configuration updated")
    time.sleep(3)  # Wait for auto-restart


def _format_tables_for_connector(db_config, tables_list, db_type):
    """
    Format table list for connector based on database type.

    Uses consolidated format_table_for_connector() to eliminate duplication.
    """
    # Format all tables using consolidated function
    formatted_tables = [format_table_for_connector(db_config, table) for table in tables_list]

    # Special handling: Add signal table for SQL Server
    if db_type.lower() == 'mssql':
        signal_table = 'dbo.debezium_signal'
        if signal_table not in formatted_tables:
            formatted_tables.append(signal_table)
            logger.info(f"   ✅ Added signal table: {signal_table}")

    return formatted_tables


def _handle_snapshots_for_new_tables(request, manager, replication_config, db_config, client, newly_added_tables, db_type):
    """Handle snapshots for newly added tables"""
    logger.info(f"\n📸 SNAPSHOT HANDLING")
    logger.info(f"=" * 80)
    logger.info(f"Database: {db_type.upper()}")
    logger.info(f"Tables: {len(newly_added_tables)}")
    
    if db_type == 'mssql':
        _handle_mssql_snapshots(request, manager, replication_config, db_config, newly_added_tables)
    else:
        _handle_kafka_signal_snapshots(request, replication_config, db_config, client, newly_added_tables, db_type)


def _handle_mssql_snapshots(request, manager, replication_config, db_config, newly_added_tables):
    """SQL Server: Recovery snapshot + incremental fallback"""
    logger.info(f"\n🔧 SQL Server: Recovery snapshot...")
    
    try:
        # Recovery snapshot
        success, message = handle_sqlserver_new_tables_via_recovery_snapshot(
            manager=manager,
            replication_config=replication_config,
            db_config=db_config,
            newly_added_tables=newly_added_tables
        )
        
        if success:
            messages.success(request, f'✅ {message}')
            logger.info(f"   ✅ Recovery snapshot initiated")
            
            # Fallback: Incremental snapshot
            logger.info(f"\n🔄 Triggering incremental snapshot fallback...")
            time.sleep(5)
            
            try:
                fallback_success, fallback_msg = trigger_incremental_snapshot_for_mssql(
                    replication_config=replication_config,
                    db_config=db_config,
                    table_names=newly_added_tables
                )
                
                if fallback_success:
                    messages.success(request, '✅ Incremental snapshot signal sent as fallback.')
                else:
                    messages.warning(request, f'⚠️ Fallback snapshot failed: {fallback_msg}')
            except Exception as e:
                logger.error(f"   ❌ Fallback failed: {e}")
        else:
            messages.error(request, f'❌ {message}')
    
    except Exception as e:
        logger.error(f"❌ Snapshot error: {e}", exc_info=True)
        messages.error(request, f'Failed to initiate snapshot: {str(e)}')


def _handle_kafka_signal_snapshots(request, replication_config, db_config, client, newly_added_tables, db_type):
    """MySQL/PostgreSQL/Oracle: Kafka signal-based snapshots"""
    logger.info(f"\n📡 {db_type.upper()}: Kafka signal snapshot...")

    try:
        from client.utils.kafka_signal_sender import KafkaSignalSender
        from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

        # Topic prefix includes version to match connector's signal.kafka.topic
        topic_prefix = replication_config.kafka_topic_prefix
        signal_sender = KafkaSignalSender()

        # Database-specific handling
        if db_type == 'oracle':
            # ✅ ORACLE CRITICAL FIX FOR AVRO SERIALIZATION:
            # When using Avro with Schema Registry, incremental snapshots REQUIRE the schema
            # to be registered BEFORE data can be sent. Unlike regular snapshots (which register
            # schemas automatically), incremental snapshots expect schemas to already exist.
            #
            # PROBLEM: when_needed mode doesn't work when offsets exist (skips snapshot)
            # SOLUTION: Use schema_only snapshot to register schemas, then incremental snapshot for data
            #
            # Workflow:
            # 1. Set snapshot.mode=schema_only → registers Avro schemas (no data)
            # 2. Restart connector → executes schema_only snapshot
            # 3. Revert snapshot.mode → back to original mode
            # 4. Send incremental snapshot signal → sends data using registered schemas

            import time

            logger.info("=" * 80)
            logger.info("🔧 ORACLE: Registering Avro schemas for new tables...")
            logger.info("=" * 80)

            connector_name = replication_config.connector_name
            manager = DebeziumConnectorManager()

            # Step 1: Get current connector config
            logger.info("\n📋 Step 1/7: Getting current connector configuration...")
            current_config = manager.get_connector_config(connector_name)

            if not current_config:
                signal_sender.close()
                messages.error(request, f'❌ Failed to get connector config for {connector_name}')
                return

            original_snapshot_mode = current_config.get('snapshot.mode', 'when_needed')
            logger.info(f"   Current snapshot.mode: {original_snapshot_mode}")

            # Step 2: Set to schema_only mode (registers Avro schemas without data)
            logger.info("\n📸 Step 2/7: Setting snapshot.mode=schema_only...")
            logger.info("   This registers Avro schemas in Schema Registry without snapshotting data")

            updated_config = current_config.copy()
            updated_config['snapshot.mode'] = 'schema_only'

            if not manager.update_connector_config(connector_name, updated_config):
                signal_sender.close()
                messages.error(request, '❌ Failed to update connector config')
                return

            logger.info("   ✅ Config updated to schema_only")

            # Step 3: Restart connector to trigger schema registration
            logger.info("\n🔄 Step 3/7: Restarting connector to register Avro schemas...")
            if not manager.restart_connector(connector_name):
                logger.warning("   ⚠️  Restart may have failed, but continuing...")
            else:
                logger.info("   ✅ Connector restarted")

            # Step 4: Wait for schema registration (schema_only is fast, ~10 seconds)
            logger.info("\n⏳ Step 4/7: Waiting for Avro schema registration (15 seconds)...")
            logger.info("   Schema Registry will now have entries for new tables")
            time.sleep(15)

            # Step 5: Revert to original snapshot mode
            logger.info(f"\n🔧 Step 5/7: Reverting to snapshot.mode={original_snapshot_mode}...")
            updated_config['snapshot.mode'] = original_snapshot_mode

            if not manager.update_connector_config(connector_name, updated_config):
                signal_sender.close()
                messages.warning(request, '⚠️ Failed to revert snapshot mode, but schemas are registered')
                logger.warning("   ⚠️  Config revert failed, but schemas ARE registered")
            else:
                logger.info(f"   ✅ Reverted to {original_snapshot_mode}")

            # Step 6: Restart connector with original config
            logger.info("\n🔄 Step 6/7: Restarting connector with original config...")
            manager.restart_connector(connector_name)
            logger.info("   Waiting 5 seconds for connector to stabilize...")
            time.sleep(5)

            # Step 7: Send incremental snapshot signal (now schemas exist!)
            logger.info("\n📡 Step 7/7: Sending incremental snapshot signal...")
            logger.info("   Now that Avro schemas are registered, data can be sent")

            # Build table identifiers for signal payload
            tables_for_signal = []
            for table in newly_added_tables:
                if '.' in table:
                    # Already has schema: CDC_USER.PAYMENTS
                    tables_for_signal.append(table)
                else:
                    # Just table name: PAYMENTS → add schema
                    username = db_config.username.upper()
                    schema = username[3:] if username.startswith('C##') else username
                    tables_for_signal.append(f"{schema}.{table}")

            logger.info(f"   Tables to snapshot: {tables_for_signal}")

            # Send the signal (PDB name is needed for 3-part identifier)
            pdb_name = db_config.database_name.upper()

            success, message = signal_sender.send_incremental_snapshot_signal(
                topic_prefix=topic_prefix,
                database_name=pdb_name,
                table_names=tables_for_signal,
                schema_name=None,
                db_type='oracle'
            )

            # Log summary
            logger.info("\n" + "=" * 80)
            if success:
                logger.info("✅ ORACLE AVRO SCHEMA FIX COMPLETE")
                logger.info("=" * 80)
                logger.info("\n📊 Summary:")
                logger.info("   1. ✅ Avro schemas registered in Schema Registry")
                logger.info("   2. ✅ Incremental snapshot signal sent via Kafka")
                logger.info("   3. ⏳ Data snapshot in progress")
                logger.info("\n📋 Tables being snapshotted:")
                for table in newly_added_tables:
                    logger.info(f"   • {table}")
                logger.info("\n💡 Data will arrive in Kafka topics within 30-60 seconds")
                logger.info("=" * 80)
            else:
                logger.error("❌ INCREMENTAL SNAPSHOT SIGNAL FAILED")
                logger.error(f"   Error: {message}")
                logger.error("=" * 80)

            # Close signal sender and show user message
            signal_sender.close()

            if success:
                messages.success(
                    request,
                    f'✅ {len(newly_added_tables)} table(s) added! Avro schemas registered. Snapshot in progress.'
                )
            else:
                messages.warning(request, f'⚠️ Schema registration succeeded but snapshot signal failed: {message}')

            # Return early - Oracle is fully handled
            return

        elif db_type == 'postgresql':
            # Strip schema for signal payload
            tables_for_signal = [t.split('.')[-1] for t in newly_added_tables]
            
            success, message = signal_sender.send_incremental_snapshot_signal(
                topic_prefix=topic_prefix,
                database_name=db_config.database_name,
                table_names=tables_for_signal,
                schema_name='public',
                db_type='postgresql'
            )
        
        else:  # MySQL
            # Strip database prefix if present
            tables_for_signal = [t.split('.')[-1] for t in newly_added_tables]
            
            success, message = signal_sender.send_incremental_snapshot_signal(
                topic_prefix=topic_prefix,
                database_name=db_config.database_name,
                table_names=tables_for_signal,
                db_type='mysql'
            )
        
        signal_sender.close()
        
        if success:
            messages.success(request, f'✅ {len(newly_added_tables)} table(s) added! Incremental snapshot in progress.')
            logger.info(f"   ✅ Snapshot signal sent successfully")
        else:
            messages.warning(request, f'Snapshot signal failed: {message}')
            logger.warning(f"   ⚠️ Signal failed: {message}")
    
    except Exception as e:
        logger.error(f"❌ Signal error: {e}", exc_info=True)
        messages.warning(request, f'Snapshot signal failed: {str(e)}')


def _restart_consumer(replication_config):
    """Restart Kafka consumer for new topics"""
    try:
        logger.info(f"\n🔄 Restarting consumer...")
        
        from client.tasks import stop_kafka_consumer, start_kafka_consumer
        
        stop_result = stop_kafka_consumer(replication_config.id)
        
        if stop_result.get('success'):
            logger.info(f"   ✅ Consumer stopped")
            time.sleep(2)
            
            replication_config.refresh_from_db()
            replication_config.is_active = True
            replication_config.status = 'active'
            replication_config.save()
            
            start_kafka_consumer.apply_async(args=[replication_config.id], countdown=3)
            logger.info(f"   ✅ Consumer restart scheduled")
    
    except Exception as e:
        logger.error(f"❌ Consumer restart failed: {e}")


def _restart_connector_simple(request, manager, replication_config, db_config, client, db_type):
    """Simple connector restart (no table changes)"""
    logger.info(f"\n🔄 Restarting connector (no table changes)...")
    
    if db_type == 'postgresql':
        slot_name = f"debezium_{client.id}_{db_config.id}"
        terminate_active_slot_connections(db_config, slot_name)
        time.sleep(2)
    
    success, error = manager.restart_connector(replication_config.connector_name)
    
    if success:
        logger.info(f"   ✅ Connector restarted")
        messages.success(request, '✅ Connector restarted!')
    else:
        logger.warning(f"   ❌ Restart failed: {error}")
        messages.warning(request, f'Restart failed: {error}')


# ============================================================================
# GET REQUEST HANDLER
# ============================================================================

def _handle_get_request(request, replication_config, db_config, client, db_type):
    """Handle GET request - Show edit form"""
    logger.info(f"\n" + "=" * 80)
    logger.info(f"GET REQUEST: Loading edit form")
    logger.info(f"=" * 80)
    
    # Discover all available tables (DB-specific)
    all_table_names = _discover_all_tables(db_config, db_type)
    
    # Build table data with columns and metadata
    tables_with_columns = _build_table_data(replication_config, db_config, all_table_names, db_type)
    
    # Render template
    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'tables_with_columns': tables_with_columns,
    }
    
    logger.info(f"\n✅ Edit form ready: {len(tables_with_columns)} tables")
    logger.info(f"=" * 80)
    
    return render(request, 'client/cdc/edit_config.html', context)


def _discover_all_tables(db_config, db_type):
    """
    Discover all available tables based on database type
    
    ✅ ORACLE: Shows ALL accessible tables (owned + granted + synonyms)
    ✅ POSTGRESQL: Shows all tables with schema
    ✅ SQL SERVER: Shows all tables with schema
    ✅ MYSQL: Shows all tables
    """
    logger.info(f"\n🔍 Discovering tables for {db_type.upper()}...")
    
    try:
        if db_type == 'oracle':
            return _discover_oracle_tables(db_config)
        elif db_type == 'postgresql':
            return _discover_postgresql_tables(db_config)
        elif db_type == 'mssql':
            return _discover_mssql_tables(db_config)
        else:  # MySQL
            return _discover_mysql_tables(db_config)
    
    except Exception as e:
        logger.error(f"❌ Table discovery failed: {e}", exc_info=True)
        return []


def _discover_oracle_tables(db_config):
    """
    Oracle: Comprehensive discovery (OWNED + GRANTED + SYNONYMS)
    
    Returns fully qualified SCHEMA.TABLE names
    """
    logger.info(f"=" * 80)
    logger.info(f"🔍 ORACLE: Comprehensive Table Discovery")
    logger.info(f"=" * 80)
    
    engine = get_database_engine(db_config)
    discovered_tables = {}
    
    try:
        with engine.connect() as conn:
            # Get current user
            current_user_result = conn.execute(text("SELECT USER FROM DUAL"))
            current_user = current_user_result.scalar()
            username_upper = current_user.upper()
            
            logger.info(f"Connected as: {username_upper}")
            
            # Determine schema
            is_common_user = username_upper.startswith('C##')
            if is_common_user:
                default_schema = username_upper[3:]  # Remove C## prefix
                logger.info(f"Common user detected: {username_upper} → schema: {default_schema}")
            else:
                default_schema = username_upper
                logger.info(f"Local user detected: {username_upper}")
            
            # ================================================================
            # METHOD 1: User-owned tables (user_tables)
            # ================================================================
            logger.info(f"\n📋 Method 1: Checking user_tables...")
            
            user_tables_query = text("""
                SELECT table_name
                FROM user_tables 
                WHERE table_name NOT LIKE 'LOG_MINING%'
                  AND table_name NOT LIKE 'DEBEZIUM%'
                  AND table_name NOT LIKE 'SYS_%'
                  AND table_name NOT LIKE 'MLOG$%'
                  AND table_name NOT LIKE 'RUPD$%'
                  AND table_name NOT LIKE 'BIN$%'
                  AND table_name NOT LIKE 'ISEQ$_%'
                  AND table_name NOT LIKE 'DR$%'
                  AND table_name NOT LIKE 'SCHEDULER_%'
                  AND table_name NOT LIKE 'LOGMNR_%'
                  AND table_name NOT LIKE 'DBMS_%'
                  AND table_name NOT LIKE 'AQ$%'
                  AND table_name NOT LIKE 'DEF$%'
                ORDER BY table_name
            """)
            
            result = conn.execute(user_tables_query)
            user_owned = [row[0] for row in result.fetchall()]
            
            for table_name in user_owned:
                qualified_name = f"{default_schema}.{table_name}"
                discovered_tables[qualified_name] = {
                    'schema': default_schema,
                    'table': table_name,
                    'source': 'owned'
                }
            
            logger.info(f"   ✅ Found {len(user_owned)} owned tables")
            
            # ================================================================
            # METHOD 2: Granted tables from other schemas (all_tables)
            # ================================================================
            logger.info(f"\n📋 Method 2: Checking all_tables (granted access)...")
            
            all_tables_query = text("""
                SELECT DISTINCT owner, table_name
                FROM all_tables
                WHERE owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 
                                   'WMSYS', 'EXFSYS', 'CTXSYS', 'XDB', 'ANONYMOUS',
                                   'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS', 'DVSYS',
                                   'DVF', 'GSMADMIN_INTERNAL', 'OJVMSYS', 'OLAPSYS')
                  AND table_name NOT LIKE 'LOG_MINING%'
                  AND table_name NOT LIKE 'DEBEZIUM%'
                  AND table_name NOT LIKE 'SYS_%'
                  AND table_name NOT LIKE 'MLOG$%'
                  AND table_name NOT LIKE 'RUPD$%'
                  AND table_name NOT LIKE 'BIN$%'
                  AND table_name NOT LIKE 'ISEQ$_%'
                  AND table_name NOT LIKE 'DR$%'
                  AND table_name NOT LIKE 'SCHEDULER_%'
                  AND table_name NOT LIKE 'LOGMNR_%'
                  AND table_name NOT LIKE 'DBMS_%'
                  AND table_name NOT LIKE 'AQ$%'
                  AND table_name NOT LIKE 'DEF$%'
                ORDER BY owner, table_name
            """)
            
            result = conn.execute(all_tables_query)
            granted_tables = [(row[0], row[1]) for row in result.fetchall()]
            
            for owner, table_name in granted_tables:
                # Strip C## prefix from schema names
                schema_name = owner[3:] if owner.startswith('C##') else owner
                qualified_name = f"{schema_name}.{table_name}"
                
                if qualified_name not in discovered_tables:
                    discovered_tables[qualified_name] = {
                        'schema': schema_name,
                        'table': table_name,
                        'source': 'granted'
                    }
            
            logger.info(f"   ✅ Found {len(granted_tables)} accessible tables (all schemas)")
            
            # ================================================================
            # METHOD 3: Tables accessible via synonyms (all_synonyms)
            # ================================================================
            logger.info(f"\n📋 Method 3: Checking all_synonyms...")
            
            synonyms_query = text("""
                SELECT 
                    synonym_name,
                    table_owner,
                    table_name
                FROM all_synonyms
                WHERE owner = :current_user
                  AND table_owner NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP')
                  AND table_name NOT LIKE 'LOG_MINING%'
                  AND table_name NOT LIKE 'DEBEZIUM%'
                  AND table_name NOT LIKE 'SYS_%'
                  AND table_name NOT LIKE 'ISEQ$_%'
                  AND table_name NOT LIKE 'DR$%'
                  AND table_name NOT LIKE 'SCHEDULER_%'
                  AND table_name NOT LIKE 'LOGMNR_%'
                  AND table_name NOT LIKE 'DBMS_%'
                ORDER BY synonym_name
            """)
            
            result = conn.execute(synonyms_query, {"current_user": username_upper})
            synonyms = [(row[0], row[1], row[2]) for row in result.fetchall()]
            
            for synonym_name, table_owner, table_name in synonyms:
                schema_name = table_owner[3:] if table_owner.startswith('C##') else table_owner
                
                # Use actual table name, not synonym
                qualified_name = f"{schema_name}.{table_name}"
                
                if qualified_name not in discovered_tables:
                    discovered_tables[qualified_name] = {
                        'schema': schema_name,
                        'table': table_name,
                        'source': 'synonym',
                        'synonym': synonym_name
                    }
            
            logger.info(f"   ✅ Found {len(synonyms)} accessible synonyms")
            
            # ================================================================
            # Build final list
            # ================================================================
            all_table_names = list(discovered_tables.keys())
            
            logger.info(f"\n" + "=" * 80)
            logger.info(f"📊 ORACLE DISCOVERY SUMMARY")
            logger.info(f"=" * 80)
            logger.info(f"Total unique tables: {len(all_table_names)}")
            
            # Group by schema for logging
            schemas = {}
            for qualified_name, info in discovered_tables.items():
                schema = info['schema']
                if schema not in schemas:
                    schemas[schema] = []
                schemas[schema].append(info['table'])
            
            logger.info(f"\nTables by schema:")
            for schema, tables in sorted(schemas.items()):
                logger.info(f"   {schema}: {len(tables)} tables")
                for table in sorted(tables)[:3]:
                    logger.info(f"      • {table}")
                if len(tables) > 3:
                    logger.info(f"      ... and {len(tables) - 3} more")
            
            logger.info(f"=" * 80)
            
            return all_table_names
    
    finally:
        engine.dispose()


def _discover_postgresql_tables(db_config):
    """PostgreSQL: Discover tables with schema (uses consolidated generic function)"""
    return discover_tables_generic(db_config, 'postgresql')


def _discover_mssql_tables(db_config):
    """SQL Server: Discover tables with schema (uses consolidated generic function)"""
    return discover_tables_generic(db_config, 'mssql')


def _discover_mysql_tables(db_config):
    """MySQL: Simple table discovery (uses consolidated generic function)"""
    return discover_tables_generic(db_config, 'mysql')


def _build_table_data(replication_config, db_config, all_table_names, db_type):
    """
    Build table data structure for edit form
    
    Enriches tables with:
    - Schema information
    - Row counts
    - Column details
    - Existing mappings
    - Incremental candidates
    """
    logger.info(f"\n📊 Building table data for edit form...")
    logger.info(f"   Total available tables: {len(all_table_names)}")
    
    tables_with_columns = []
    
    # Build existing mappings lookup
    existing_mappings = {}
    for tm in replication_config.table_mappings.all():
        normalized = normalize_sql_server_table_name(tm.source_table, db_type)
        existing_mappings[normalized] = tm
    
    logger.info(f"   Existing mappings: {len(existing_mappings)}")
    
    # Process each table
    for table_name in all_table_names:
        existing_mapping = existing_mappings.get(table_name)
        
        try:
            # Get table schema
            schema = get_table_schema(db_config, table_name)
            columns_from_schema = schema.get('columns', [])
            
            # Find incremental candidates
            incremental_candidates = [
                col for col in columns_from_schema
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time')
            ]
            
            # Get row count
            row_count = _get_table_row_count(db_config, table_name, db_type)
            
            # Generate default target name
            default_target_table_name = generate_target_table_name(
                db_config.database_name,
                table_name,
                db_type
            )
            
            # Build table data
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
            logger.error(f"   ❌ Error processing table {table_name}: {e}")
            continue
    
    # Sort: mapped tables first, then by name
    tables_with_columns.sort(key=lambda t: (not t['is_mapped'], t['table_name']))
    
    logger.info(f"   ✅ Built data for {len(tables_with_columns)} tables")
    logger.info(f"      Mapped: {sum(1 for t in tables_with_columns if t['is_mapped'])}")
    logger.info(f"      Available: {sum(1 for t in tables_with_columns if not t['is_mapped'])}")
    
    return tables_with_columns


def _get_table_row_count(db_config, table_name, db_type):
    """Get row count for a table (uses consolidated query builder)"""
    try:
        engine = get_database_engine(db_config)

        with engine.connect() as conn:
            # Use consolidated query builder
            query_str = build_row_count_query(table_name, db_type)
            count_query = text(query_str)

            result = conn.execute(count_query)
            row = result.fetchone()
            row_count = row[0] if row else 0

        engine.dispose()
        return row_count
    
    except:
        return 0