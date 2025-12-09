"""
Database utility functions for connection management and operations
Supports: MySQL, PostgreSQL, SQL Server, Oracle, SQLite
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import pymysql
import psycopg2
from django.conf import settings
from client.models.database import ClientDatabase

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails"""
    pass


class DatabaseOperationError(Exception):
    """Raised when database operation fails"""
    pass


def build_connection_string(db_config: ClientDatabase) -> str:
    """
    Build SQLAlchemy connection string from ClientDatabase instance

    Args:
        db_config: ClientDatabase model instance

    Returns:
        str: SQLAlchemy connection string

    Raises:
        ValueError: If database type is not supported
    """
    from urllib.parse import quote_plus

    db_type = db_config.db_type.lower()
    host = db_config.host
    port = db_config.port
    # URL-encode username and password to handle special characters (@, :, /, etc.)
    username = quote_plus(db_config.username)
    password = quote_plus(db_config.get_decrypted_password())
    database = db_config.database_name

    connection_strings = {
        'mysql': f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
        'postgresql': f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
        'mssql': f"mssql+pymssql://{username}:{password}@{host}:{port}/{database}",
        'oracle': f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}",
        'sqlite': f"sqlite:///{database}",
    }

    if db_type not in connection_strings:
        raise ValueError(f"Unsupported database type: {db_type}")

    return connection_strings[db_type]


def get_database_engine(db_config: ClientDatabase, pool_size: int = 5) -> Engine:
    """
    Create SQLAlchemy engine for database connection
    
    Args:
        db_config: ClientDatabase model instance
        pool_size: Connection pool size (default: 5)
        
    Returns:
        Engine: SQLAlchemy engine instance
        
    Raises:
        DatabaseConnectionError: If connection fails
    """
    try:
        connection_string = build_connection_string(db_config)
        
        engine = create_engine(
            connection_string,
            pool_size=pool_size,
            pool_pre_ping=True,  # Test connections before using
            pool_recycle=3600,   # Recycle connections after 1 hour
            echo=False
        )
        
        logger.info(f"Created database engine for {db_config.connection_name}")
        return engine
        
    except Exception as e:
        error_msg = f"Failed to create database engine for {db_config.connection_name}: {str(e)}"
        logger.error(error_msg)
        raise DatabaseConnectionError(error_msg) from e


def test_database_connection(db_config: ClientDatabase) -> Tuple[bool, Optional[str]]:
    """
    Test database connection
    
    Args:
        db_config: ClientDatabase model instance
        
    Returns:
        Tuple[bool, Optional[str]]: (success, error_message)
    """
    try:
        engine = get_database_engine(db_config, pool_size=1)
        
        with engine.connect() as conn:
            # Test query based on database type
            if db_config.db_type.lower() == 'mysql':
                result = conn.execute(text("SELECT 1"))
            elif db_config.db_type.lower() == 'postgresql':
                result = conn.execute(text("SELECT 1"))
            elif db_config.db_type.lower() == 'mssql':
                result = conn.execute(text("SELECT 1"))
            elif db_config.db_type.lower() == 'oracle':
                result = conn.execute(text("SELECT 1 FROM DUAL"))
            else:
                result = conn.execute(text("SELECT 1"))
            
            result.fetchone()
            
        engine.dispose()
        logger.info(f"Successfully tested connection: {db_config.connection_name}")
        return True, None
        
    except Exception as e:
        error_msg = f"Connection test failed: {str(e)}"
        logger.error(f"Connection test failed for {db_config.connection_name}: {error_msg}")
        return False, error_msg


@contextmanager
def get_database_connection(db_config: ClientDatabase):
    """
    Context manager for database connections
    
    Args:
        db_config: ClientDatabase model instance
        
    Yields:
        Connection object
        
    Example:
        with get_database_connection(db_config) as conn:
            result = conn.execute(text("SELECT * FROM users"))
    """
    engine = None
    connection = None
    
    try:
        engine = get_database_engine(db_config)
        connection = engine.connect()
        yield connection
        
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise DatabaseConnectionError(f"Failed to connect to database: {str(e)}") from e
        
    finally:
        if connection:
            connection.close()
        if engine:
            engine.dispose()


def execute_query(db_config: ClientDatabase, query: str, params: Optional[Dict] = None) -> List[Dict]:
    """
    Execute a SQL query and return results
    
    Args:
        db_config: ClientDatabase model instance
        query: SQL query string
        params: Query parameters (optional)
        
    Returns:
        List[Dict]: Query results as list of dictionaries
        
    Raises:
        DatabaseOperationError: If query execution fails
    """
    try:
        with get_database_connection(db_config) as conn:
            result = conn.execute(text(query), params or {})
            
            # Convert to list of dictionaries
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            
            logger.info(f"Executed query successfully: {query[:100]}...")
            return rows
            
    except Exception as e:
        error_msg = f"Query execution failed: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def get_table_list(db_config: ClientDatabase, schema: Optional[str] = None) -> List[str]:
    """
    Get list of tables in database
    
    Supports:
    - MySQL: All tables from specified database
    - PostgreSQL: Tables from 'public' schema (or specified schema)
    - SQL Server: Tables from all user schemas (dbo, etc.) as 'schema.table'
    - Oracle: All tables from user schema
    - SQLite: All tables
    
    Args:
        db_config: ClientDatabase model instance
        schema: Schema name (optional, overrides defaults)
        
    Returns:
        List[str]: List of table names (SQL Server format: 'schema.table')
        
    Raises:
        DatabaseOperationError: If operation fails
    """
    try:
        engine = get_database_engine(db_config)
        inspector = inspect(engine)
        
        db_type = db_config.db_type.lower()
        
        if db_type == 'mssql':
            # SQL Server: Get tables from all user schemas
            tables = []
            
            with engine.connect() as conn:
                # Get all user schemas (exclude system schemas)
                schema_query = text("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name NOT IN (
                        'sys', 'INFORMATION_SCHEMA', 'guest', 
                        'db_owner', 'db_accessadmin', 'db_securityadmin', 
                        'db_ddladmin', 'db_backupoperator', 'db_datareader', 
                        'db_datawriter', 'db_denydatareader', 'db_denydatawriter'
                    )
                    ORDER BY schema_name
                """)
                result = conn.execute(schema_query)
                schemas = [row[0] for row in result]
            
            logger.info(f"SQL Server: Found {len(schemas)} user schemas: {schemas}")
            
            # Get tables from each schema
            for schema_name in schemas:
                try:
                    schema_tables = inspector.get_table_names(schema=schema_name)
                    # Add schema prefix to table names
                    for table in schema_tables:
                        tables.append(f"{schema_name}.{table}")
                    logger.debug(f"Schema '{schema_name}': {len(schema_tables)} tables")
                except Exception as e:
                    logger.warning(f"Could not get tables from schema '{schema_name}': {e}")
                    continue
            
            logger.info(f"SQL Server: Total {len(tables)} tables discovered")
            
        elif db_type == 'postgresql':
            # PostgreSQL: Use specified schema or default to 'public'
            schema_name = schema or 'public'
            tables = inspector.get_table_names(schema=schema_name)
            logger.info(f"PostgreSQL: Found {len(tables)} tables in schema '{schema_name}'")
            
        elif db_type == 'mysql':
            # MySQL: Get all tables from database
            tables = inspector.get_table_names()
            logger.info(f"MySQL: Found {len(tables)} tables")
            
        elif db_type == 'oracle':
            # Oracle: Get tables from current user schema
            if schema:
                tables = inspector.get_table_names(schema=schema)
            else:
                tables = inspector.get_table_names()
            logger.info(f"Oracle: Found {len(tables)} tables")
            
        elif db_type == 'sqlite':
            # SQLite: Get all tables
            tables = inspector.get_table_names()
            logger.info(f"SQLite: Found {len(tables)} tables")
            
        else:
            # Default: Try to get tables
            tables = inspector.get_table_names()
            logger.info(f"{db_type}: Found {len(tables)} tables")
        
        engine.dispose()
        return sorted(tables)
        
    except Exception as e:
        error_msg = f"Failed to get table list: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def get_table_schema(db_config: ClientDatabase, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get table schema information (columns, types, constraints)
    
    Args:
        db_config: ClientDatabase model instance
        table_name: Name of the table (for SQL Server: 'schema.table' or just 'table')
        schema: Schema name (optional, overrides default)
        
    Returns:
        Dict containing:
            - table_name: Table name
            - columns: List of column definitions
            - primary_keys: List of primary key columns
            - indexes: List of indexes
            - foreign_keys: List of foreign key constraints
        
    Raises:
        DatabaseOperationError: If operation fails
    """
    try:
        engine = get_database_engine(db_config)
        inspector = inspect(engine)
        
        db_type = db_config.db_type.lower()
        
        # Parse schema and table name for SQL Server
        schema_name = schema
        actual_table_name = table_name
        
        if db_type == 'mssql':
            # SQL Server: Parse 'schema.table' format
            if '.' in table_name:
                schema_name, actual_table_name = table_name.split('.', 1)
            else:
                schema_name = schema_name or 'dbo'  # Default schema
            
            logger.debug(f"SQL Server: schema='{schema_name}', table='{actual_table_name}'")
            
        elif db_type == 'postgresql':
            # PostgreSQL: Use specified schema or default to 'public'
            schema_name = schema_name or 'public'
            logger.debug(f"PostgreSQL: schema='{schema_name}', table='{actual_table_name}'")
        
        # Get column information
        if schema_name:
            columns = inspector.get_columns(actual_table_name, schema=schema_name)
            pk_constraint = inspector.get_pk_constraint(actual_table_name, schema=schema_name)
            indexes = inspector.get_indexes(actual_table_name, schema=schema_name)
            foreign_keys = inspector.get_foreign_keys(actual_table_name, schema=schema_name)
        else:
            columns = inspector.get_columns(actual_table_name)
            pk_constraint = inspector.get_pk_constraint(actual_table_name)
            indexes = inspector.get_indexes(actual_table_name)
            foreign_keys = inspector.get_foreign_keys(actual_table_name)
        
        primary_keys = pk_constraint.get('constrained_columns', []) if pk_constraint else []
        
        engine.dispose()
        
        schema_info = {
            'table_name': table_name,
            'columns': columns,
            'primary_keys': primary_keys,
            'indexes': indexes,
            'foreign_keys': foreign_keys,
        }
        
        logger.info(f"Retrieved schema for table '{table_name}': {len(columns)} columns, {len(primary_keys)} PKs")
        return schema_info
        
    except Exception as e:
        error_msg = f"Failed to get table schema for {table_name}: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def create_client_database(client_name: str, client_id: int) -> bool:
    """
    Create a new MySQL database for a client
    
    Args:
        client_name: Name of the client
        client_id: Client ID
        
    Returns:
        bool: True if successful
        
    Raises:
        DatabaseOperationError: If database creation fails
    """
    try:
        db_name = f"client_{client_id}_db"
        
        # Connect to MySQL server (without database)
        connection_string = (
            f"mysql+pymysql://{settings.DATABASES['default']['USER']}:"
            f"{settings.DATABASES['default']['PASSWORD']}@"
            f"{settings.DATABASES['default']['HOST']}:"
            f"{settings.DATABASES['default']['PORT']}/"
        )
        
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Create database
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
            conn.commit()
            
            logger.info(f"Created database: {db_name} for client: {client_name}")
        
        engine.dispose()
        return True
        
    except Exception as e:
        error_msg = f"Failed to create database for client {client_name}: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def check_binary_logging(db_config: ClientDatabase) -> Tuple[bool, Optional[str]]:
    """
    Check if binary logging is enabled (required for MySQL CDC)
    Only applicable for MySQL
    
    Args:
        db_config: ClientDatabase model instance
        
    Returns:
        Tuple[bool, Optional[str]]: (is_enabled, log_format)
        
    Raises:
        DatabaseOperationError: If check fails
    """
    if db_config.db_type.lower() != 'mysql':
        return True, 'N/A'  # Only required for MySQL
    
    try:
        with get_database_connection(db_config) as conn:
            # Check if binary logging is enabled
            result = conn.execute(text("SHOW VARIABLES LIKE 'log_bin'"))
            log_bin = result.fetchone()
            
            if not log_bin or log_bin[1].lower() != 'on':
                return False, None
            
            # Check binary log format (ROW format is required for Debezium)
            result = conn.execute(text("SHOW VARIABLES LIKE 'binlog_format'"))
            binlog_format = result.fetchone()
            
            format_value = binlog_format[1] if binlog_format else 'UNKNOWN'
            
            logger.info(f"Binary logging check for {db_config.connection_name}: Enabled, Format: {format_value}")
            return True, format_value
            
    except Exception as e:
        error_msg = f"Failed to check binary logging: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def check_sql_server_cdc(db_config: ClientDatabase) -> Tuple[bool, Dict[str, Any]]:
    """
    Check if SQL Server CDC is enabled (required for SQL Server CDC)
    Only applicable for SQL Server
    
    Args:
        db_config: ClientDatabase model instance
        
    Returns:
        Tuple[bool, Dict]: (is_enabled, info_dict)
            info_dict contains:
                - database_cdc_enabled: bool
                - agent_running: bool
                - agent_status: str
        
    Raises:
        DatabaseOperationError: If check fails
    """
    if db_config.db_type.lower() != 'mssql':
        return True, {'message': 'Not applicable'}
    
    try:
        info = {
            'database_cdc_enabled': False,
            'agent_running': False,
            'agent_status': 'Unknown'
        }
        
        with get_database_connection(db_config) as conn:
            # Check if CDC is enabled at database level
            cdc_check = text("""
                SELECT is_cdc_enabled 
                FROM sys.databases 
                WHERE name = :db_name
            """)
            result = conn.execute(cdc_check, {"db_name": db_config.database_name})
            row = result.fetchone()
            
            if row:
                info['database_cdc_enabled'] = bool(row[0])
            
            # Check if SQL Server Agent is running
            agent_check = text("""
                SELECT dss.[status], dss.[status_desc]
                FROM sys.dm_server_services dss
                WHERE dss.[servicename] LIKE N'SQL Server Agent (%';
            """)
            result = conn.execute(agent_check)
            agent_row = result.fetchone()
            
            if agent_row:
                info['agent_running'] = (agent_row[0] == 4)  # 4 = Running
                info['agent_status'] = agent_row[1]
            
            is_enabled = info['database_cdc_enabled'] and info['agent_running']
            
            logger.info(
                f"SQL Server CDC check for {db_config.connection_name}: "
                f"DB CDC={info['database_cdc_enabled']}, "
                f"Agent={info['agent_status']}"
            )
            
            return is_enabled, info
            
    except Exception as e:
        error_msg = f"Failed to check SQL Server CDC: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def get_database_size(db_config: ClientDatabase) -> Dict[str, Any]:
    """
    Get database size information
    
    Args:
        db_config: ClientDatabase model instance
        
    Returns:
        Dict containing size information (in MB)
    """
    try:
        db_type = db_config.db_type.lower()
        
        if db_type == 'mysql':
            query = """
                SELECT 
                    SUM(data_length + index_length) / 1024 / 1024 AS size_mb,
                    COUNT(*) AS table_count
                FROM information_schema.TABLES 
                WHERE table_schema = :database_name
            """
        elif db_type == 'postgresql':
            query = """
                SELECT 
                    pg_database_size(:database_name) / 1024 / 1024 AS size_mb,
                    COUNT(*) AS table_count
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """
        elif db_type == 'mssql':
            query = """
                SELECT 
                    SUM(size * 8.0 / 1024) AS size_mb,
                    (SELECT COUNT(*) FROM sys.tables WHERE is_ms_shipped = 0) AS table_count
                FROM sys.master_files
                WHERE database_id = DB_ID(:database_name)
            """
        else:
            return {'size_mb': 0, 'table_count': 0, 'error': 'Unsupported database type'}
        
        result = execute_query(db_config, query, {'database_name': db_config.database_name})
        
        if result:
            return {
                'size_mb': float(result[0].get('size_mb', 0) or 0),
                'table_count': int(result[0].get('table_count', 0) or 0)
            }
        
        return {'size_mb': 0, 'table_count': 0}
        
    except Exception as e:
        logger.error(f"Failed to get database size: {str(e)}")
        return {'size_mb': 0, 'table_count': 0, 'error': str(e)}


def get_row_count(db_config: ClientDatabase, table_name: str, schema: Optional[str] = None) -> int:
    """
    Get row count for a specific table
    
    Args:
        db_config: ClientDatabase model instance
        table_name: Name of the table (for SQL Server: 'schema.table' or just 'table')
        schema: Schema name (optional)
        
    Returns:
        int: Number of rows in the table
        
    Raises:
        DatabaseOperationError: If operation fails
    """
    try:
        db_type = db_config.db_type.lower()
        
        # Parse schema and table name for SQL Server
        schema_name = schema
        actual_table_name = table_name
        
        if db_type == 'mssql':
            if '.' in table_name:
                schema_name, actual_table_name = table_name.split('.', 1)
            else:
                schema_name = schema_name or 'dbo'
            
            query = f"SELECT COUNT(*) as cnt FROM [{schema_name}].[{actual_table_name}]"
            
        elif db_type == 'postgresql':
            schema_name = schema_name or 'public'
            query = f'SELECT COUNT(*) as cnt FROM "{schema_name}"."{actual_table_name}"'
            
        elif db_type == 'mysql':
            query = f"SELECT COUNT(*) as cnt FROM `{actual_table_name}`"
            
        else:
            query = f"SELECT COUNT(*) as cnt FROM {actual_table_name}"
        
        with get_database_connection(db_config) as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
            count = row[0] if row else 0
            
        logger.debug(f"Row count for '{table_name}': {count}")
        return count
        
    except Exception as e:
        error_msg = f"Failed to get row count for {table_name}: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def table_exists(db_config: ClientDatabase, table_name: str, schema: Optional[str] = None) -> bool:
    """
    Check if a table exists in the database
    
    Args:
        db_config: ClientDatabase model instance
        table_name: Name of the table (for SQL Server: 'schema.table' or just 'table')
        schema: Schema name (optional)
        
    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        tables = get_table_list(db_config, schema=schema)
        
        # For SQL Server, handle both 'schema.table' and 'table' formats
        if db_config.db_type.lower() == 'mssql':
            if '.' not in table_name:
                schema_name = schema or 'dbo'
                table_name = f"{schema_name}.{table_name}"
        
        return table_name in tables
        
    except Exception as e:
        logger.error(f"Failed to check if table exists: {str(e)}")
        return False