"""
Database utility functions for connection management and operations
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
    db_type = db_config.db_type.lower()
    host = db_config.host
    port = db_config.port
    username = db_config.username
    password = db_config.get_decrypted_password()
    database = db_config.database_name
    
    connection_strings = {
        'mysql': f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
        'postgresql': f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
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
    
    Args:
        db_config: ClientDatabase model instance
        schema: Schema name (optional, for PostgreSQL/Oracle)
        
    Returns:
        List[str]: List of table names
        
    Raises:
        DatabaseOperationError: If operation fails
    """
    try:
        engine = get_database_engine(db_config)
        inspector = inspect(engine)
        
        if schema:
            tables = inspector.get_table_names(schema=schema)
        else:
            tables = inspector.get_table_names()
        
        engine.dispose()
        logger.info(f"Retrieved {len(tables)} tables from {db_config.connection_name}")
        return tables
        
    except Exception as e:
        error_msg = f"Failed to get table list: {str(e)}"
        logger.error(error_msg)
        raise DatabaseOperationError(error_msg) from e


def get_table_schema(db_config: ClientDatabase, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get table schema information (columns, types, constraints)
    
    Args:
        db_config: ClientDatabase model instance
        table_name: Name of the table
        schema: Schema name (optional)
        
    Returns:
        Dict containing:
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
        
        # Get column information
        columns = inspector.get_columns(table_name, schema=schema)
        
        # Get primary keys
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
        primary_keys = pk_constraint.get('constrained_columns', [])
        
        # Get indexes
        indexes = inspector.get_indexes(table_name, schema=schema)
        
        # Get foreign keys
        foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)
        
        engine.dispose()
        
        schema_info = {
            'table_name': table_name,
            'columns': columns,
            'primary_keys': primary_keys,
            'indexes': indexes,
            'foreign_keys': foreign_keys,
        }
        
        logger.info(f"Retrieved schema for table {table_name}")
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
    Check if binary logging is enabled (required for Debezium CDC)
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
            
            # Check binary log format
            result = conn.execute(text("SHOW VARIABLES LIKE 'binlog_format'"))
            binlog_format = result.fetchone()
            
            format_value = binlog_format[1] if binlog_format else 'UNKNOWN'
            
            logger.info(f"Binary logging check for {db_config.connection_name}: Enabled, Format: {format_value}")
            return True, format_value
            
    except Exception as e:
        error_msg = f"Failed to check binary logging: {str(e)}"
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