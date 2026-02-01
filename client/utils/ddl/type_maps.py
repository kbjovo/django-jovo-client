"""
Type mappings for source databases to SQLAlchemy types.

Provides comprehensive type mappings for:
- MySQL source types
- SQL Server (MSSQL) source types
- PostgreSQL source types
"""

from typing import Dict, Type, Any, Optional
from sqlalchemy import (
    Integer, BigInteger, SmallInteger, Float, Numeric,
    String, Text, Boolean, DateTime, Date, Time,
    LargeBinary, JSON
)


# MySQL source types -> SQLAlchemy types
MYSQL_TYPE_MAP: Dict[str, Type] = {
    # Integer types
    'TINYINT': SmallInteger,
    'SMALLINT': SmallInteger,
    'MEDIUMINT': Integer,
    'INT': Integer,
    'INTEGER': Integer,
    'BIGINT': BigInteger,

    # Floating point types
    'FLOAT': Float,
    'DOUBLE': Float,
    'REAL': Float,

    # Fixed-point types
    'DECIMAL': Numeric,
    'NUMERIC': Numeric,
    'DEC': Numeric,

    # String types
    'VARCHAR': String,
    'CHAR': String,
    'TINYTEXT': Text,
    'TEXT': Text,
    'MEDIUMTEXT': Text,
    'LONGTEXT': Text,

    # Binary types
    'BINARY': LargeBinary,
    'VARBINARY': LargeBinary,
    'TINYBLOB': LargeBinary,
    'BLOB': LargeBinary,
    'MEDIUMBLOB': LargeBinary,
    'LONGBLOB': LargeBinary,

    # Date/Time types
    'DATE': Date,
    'DATETIME': DateTime,
    'TIMESTAMP': DateTime,
    'TIME': Time,
    'YEAR': Integer,

    # Other types
    'JSON': JSON,
    'ENUM': String,
    'SET': Text,
    'BIT': Integer,
    'BOOL': Boolean,
    'BOOLEAN': Boolean,
}


# SQL Server source types -> SQLAlchemy types
MSSQL_TYPE_MAP: Dict[str, Type] = {
    # Integer types
    'int': Integer,
    'bigint': BigInteger,
    'smallint': SmallInteger,
    'tinyint': SmallInteger,
    'bit': Boolean,

    # Fixed-point types
    'decimal': Numeric,
    'numeric': Numeric,
    'money': Numeric,
    'smallmoney': Numeric,

    # Floating point types
    'float': Float,
    'real': Float,

    # Date/Time types
    'datetime': DateTime,
    'datetime2': DateTime,
    'smalldatetime': DateTime,
    'date': Date,
    'time': Time,
    'datetimeoffset': DateTime,

    # String types
    'char': String,
    'varchar': String,
    'text': Text,
    'nchar': String,
    'nvarchar': String,
    'ntext': Text,

    # Binary types
    'binary': LargeBinary,
    'varbinary': LargeBinary,
    'image': LargeBinary,

    # Other types
    'uniqueidentifier': String,
    'xml': Text,
    'sql_variant': Text,
}


# PostgreSQL source types -> SQLAlchemy types
POSTGRESQL_TYPE_MAP: Dict[str, Type] = {
    # Integer types
    'integer': Integer,
    'int': Integer,
    'int4': Integer,
    'bigint': BigInteger,
    'int8': BigInteger,
    'smallint': SmallInteger,
    'int2': SmallInteger,
    'serial': Integer,
    'bigserial': BigInteger,
    'smallserial': SmallInteger,

    # Fixed-point types
    'numeric': Numeric,
    'decimal': Numeric,

    # Floating point types
    'real': Float,
    'float4': Float,
    'double precision': Float,
    'float8': Float,

    # Boolean
    'boolean': Boolean,
    'bool': Boolean,

    # String types
    'character varying': String,
    'varchar': String,
    'character': String,
    'char': String,
    'text': Text,
    'name': String,

    # Binary types
    'bytea': LargeBinary,

    # Date/Time types
    'timestamp': DateTime,
    'timestamp without time zone': DateTime,
    'timestamp with time zone': DateTime,
    'timestamptz': DateTime,
    'date': Date,
    'time': Time,
    'time without time zone': Time,
    'time with time zone': Time,
    'timetz': Time,
    'interval': String,

    # JSON types
    'json': JSON,
    'jsonb': JSON,

    # Other types
    'uuid': String,
    'inet': String,
    'cidr': String,
    'macaddr': String,
    'point': String,
    'line': String,
    'lseg': String,
    'box': String,
    'path': String,
    'polygon': String,
    'circle': String,
    'money': Numeric,
    'bit': Integer,
    'bit varying': String,
    'tsvector': Text,
    'tsquery': Text,
    'xml': Text,
    'oid': Integer,
}


def get_type_map(source_db_type: str) -> Dict[str, Type]:
    """
    Get type map for source database type.

    Args:
        source_db_type: Database type ('mysql', 'mssql', 'postgresql')

    Returns:
        Dictionary mapping source type names to SQLAlchemy types
    """
    type_maps = {
        'mysql': MYSQL_TYPE_MAP,
        'mssql': MSSQL_TYPE_MAP,
        'sqlserver': MSSQL_TYPE_MAP,
        'postgresql': POSTGRESQL_TYPE_MAP,
        'postgres': POSTGRESQL_TYPE_MAP,
    }
    return type_maps.get(source_db_type.lower(), MYSQL_TYPE_MAP)


def map_type(
    source_type: str,
    source_db_type: str,
    length: Optional[int] = None,
    scale: Optional[int] = None
) -> Any:
    """
    Map source database type to SQLAlchemy type with parameters.

    Args:
        source_type: Source database column type (e.g., 'VARCHAR(255)')
        source_db_type: Source database type ('mysql', 'mssql', 'postgresql')
        length: Column length/precision (overrides parsed value)
        scale: Column scale for numeric types

    Returns:
        SQLAlchemy type instance

    Examples:
        >>> map_type('VARCHAR', 'mysql', length=255)
        String(255)
        >>> map_type('DECIMAL(10,2)', 'mysql')
        Numeric(10, 2)
        >>> map_type('int', 'mssql')
        Integer()
    """
    import re

    type_map = get_type_map(source_db_type)

    # Normalize type name - extract base type and parameters
    source_upper = source_type.upper().strip()

    # Handle types with parameters like VARCHAR(255) or DECIMAL(10,2)
    match = re.match(r'(\w+(?:\s+\w+)?)\s*(?:\(([^)]+)\))?', source_upper)
    if match:
        base_type = match.group(1).strip()
        params = match.group(2)

        # Parse parameters if not provided
        if params and not length:
            param_parts = [p.strip() for p in params.split(',')]
            if len(param_parts) >= 1 and param_parts[0].isdigit():
                length = int(param_parts[0])
            if len(param_parts) >= 2 and param_parts[1].isdigit():
                scale = int(param_parts[1])
    else:
        base_type = source_upper

    # For case-sensitive lookups (MSSQL uses lowercase)
    lookup_type = base_type if source_db_type.lower() in ('mssql', 'sqlserver') else base_type.upper()

    # Also try lowercase for MSSQL
    sqlalchemy_type_class = type_map.get(lookup_type)
    if sqlalchemy_type_class is None:
        sqlalchemy_type_class = type_map.get(lookup_type.lower(), String)

    # Build type with parameters
    if sqlalchemy_type_class in (String,):
        if length and length > 0:
            return sqlalchemy_type_class(length)
        return Text()
    elif sqlalchemy_type_class == LargeBinary:
        if length and length > 0:
            return sqlalchemy_type_class(length)
        return sqlalchemy_type_class()
    elif sqlalchemy_type_class == Numeric:
        if length and scale:
            return Numeric(precision=length, scale=scale)
        elif length:
            return Numeric(precision=length)
        return Numeric()
    else:
        return sqlalchemy_type_class()


def get_mysql_to_postgres_type(mysql_type: str, length: Optional[int] = None, scale: Optional[int] = None) -> str:
    """
    Convert MySQL type string to PostgreSQL type string.

    Used by PostgreSQL target adapter for direct type conversion.

    Args:
        mysql_type: MySQL column type
        length: Column length/precision
        scale: Column scale

    Returns:
        PostgreSQL type string
    """
    type_upper = mysql_type.upper().split('(')[0].strip()

    mysql_to_pg = {
        'TINYINT': 'SMALLINT',
        'MEDIUMINT': 'INTEGER',
        'INT': 'INTEGER',
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'FLOAT': 'REAL',
        'DOUBLE': 'DOUBLE PRECISION',
        'DECIMAL': 'NUMERIC',
        'NUMERIC': 'NUMERIC',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
        'TINYTEXT': 'TEXT',
        'MEDIUMTEXT': 'TEXT',
        'LONGTEXT': 'TEXT',
        'BLOB': 'BYTEA',
        'TINYBLOB': 'BYTEA',
        'MEDIUMBLOB': 'BYTEA',
        'LONGBLOB': 'BYTEA',
        'BINARY': 'BYTEA',
        'VARBINARY': 'BYTEA',
        'BIT': 'BIT',
        'BOOL': 'BOOLEAN',
        'BOOLEAN': 'BOOLEAN',
        'JSON': 'JSONB',
        'ENUM': 'VARCHAR',
        'SET': 'TEXT',
        'YEAR': 'SMALLINT',
    }

    pg_type = mysql_to_pg.get(type_upper, type_upper)

    # Add length parameters where applicable
    if pg_type in ('VARCHAR', 'CHAR', 'CHARACTER VARYING') and length:
        return f"{pg_type}({length})"
    elif pg_type == 'NUMERIC' and length:
        if scale:
            return f"NUMERIC({length},{scale})"
        return f"NUMERIC({length})"

    return pg_type


def get_postgres_to_mysql_type(pg_type: str, length: Optional[int] = None, scale: Optional[int] = None) -> str:
    """
    Convert PostgreSQL type string to MySQL type string.

    Used by MySQL target adapter for direct type conversion.

    Args:
        pg_type: PostgreSQL column type
        length: Column length/precision
        scale: Column scale

    Returns:
        MySQL type string
    """
    type_lower = pg_type.lower().split('(')[0].strip()

    pg_to_mysql = {
        'smallint': 'SMALLINT',
        'int2': 'SMALLINT',
        'integer': 'INT',
        'int': 'INT',
        'int4': 'INT',
        'bigint': 'BIGINT',
        'int8': 'BIGINT',
        'serial': 'INT AUTO_INCREMENT',
        'bigserial': 'BIGINT AUTO_INCREMENT',
        'real': 'FLOAT',
        'float4': 'FLOAT',
        'double precision': 'DOUBLE',
        'float8': 'DOUBLE',
        'numeric': 'DECIMAL',
        'decimal': 'DECIMAL',
        'boolean': 'TINYINT(1)',
        'bool': 'TINYINT(1)',
        'timestamp': 'DATETIME',
        'timestamp without time zone': 'DATETIME',
        'timestamp with time zone': 'DATETIME',
        'timestamptz': 'DATETIME',
        'bytea': 'LONGBLOB',
        'json': 'JSON',
        'jsonb': 'JSON',
        'uuid': 'VARCHAR(36)',
        'text': 'LONGTEXT',
        'character varying': 'VARCHAR',
        'varchar': 'VARCHAR',
        'character': 'CHAR',
        'char': 'CHAR',
        'interval': 'VARCHAR(50)',
        'inet': 'VARCHAR(45)',
        'cidr': 'VARCHAR(50)',
        'macaddr': 'VARCHAR(17)',
        'money': 'DECIMAL(19,4)',
    }

    mysql_type = pg_to_mysql.get(type_lower, 'VARCHAR')

    # Add length parameters where applicable
    if mysql_type in ('VARCHAR', 'CHAR') and length:
        return f"{mysql_type}({length})"
    elif mysql_type == 'DECIMAL' and length:
        if scale:
            return f"DECIMAL({length},{scale})"
        return f"DECIMAL({length})"
    elif mysql_type == 'VARCHAR' and not length:
        return 'VARCHAR(255)'

    return mysql_type


def get_mssql_to_postgres_type(mssql_type: str, length: Optional[int] = None, scale: Optional[int] = None) -> str:
    """
    Convert SQL Server type string to PostgreSQL type string.

    Args:
        mssql_type: SQL Server column type
        length: Column length/precision
        scale: Column scale

    Returns:
        PostgreSQL type string
    """
    type_lower = mssql_type.lower().split('(')[0].strip()

    mssql_to_pg = {
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'bit': 'BOOLEAN',
        'decimal': 'NUMERIC',
        'numeric': 'NUMERIC',
        'money': 'NUMERIC(19,4)',
        'smallmoney': 'NUMERIC(10,4)',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
        'date': 'DATE',
        'time': 'TIME',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'nchar': 'CHAR',
        'nvarchar': 'VARCHAR',
        'ntext': 'TEXT',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',
        'uniqueidentifier': 'UUID',
        'xml': 'XML',
    }

    pg_type = mssql_to_pg.get(type_lower, 'TEXT')

    # Handle MAX length
    if length == -1:  # SQL Server uses -1 for MAX
        if pg_type in ('VARCHAR', 'CHAR'):
            return 'TEXT'
        elif pg_type == 'BYTEA':
            return 'BYTEA'

    # Add length parameters where applicable
    if pg_type in ('VARCHAR', 'CHAR') and length and length > 0:
        return f"{pg_type}({length})"
    elif pg_type == 'NUMERIC' and length:
        if scale:
            return f"NUMERIC({length},{scale})"
        return f"NUMERIC({length})"

    return pg_type


def get_mssql_to_mysql_type(mssql_type: str, length: Optional[int] = None, scale: Optional[int] = None) -> str:
    """
    Convert SQL Server type string to MySQL type string.

    Args:
        mssql_type: SQL Server column type
        length: Column length/precision
        scale: Column scale

    Returns:
        MySQL type string
    """
    type_lower = mssql_type.lower().split('(')[0].strip()

    mssql_to_mysql = {
        'int': 'INT',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'TINYINT',
        'bit': 'TINYINT(1)',
        'decimal': 'DECIMAL',
        'numeric': 'DECIMAL',
        'money': 'DECIMAL(19,4)',
        'smallmoney': 'DECIMAL(10,4)',
        'float': 'DOUBLE',
        'real': 'FLOAT',
        'datetime': 'DATETIME',
        'datetime2': 'DATETIME(6)',
        'smalldatetime': 'DATETIME',
        'datetimeoffset': 'DATETIME',
        'date': 'DATE',
        'time': 'TIME',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'LONGTEXT',
        'nchar': 'CHAR',
        'nvarchar': 'VARCHAR',
        'ntext': 'LONGTEXT',
        'binary': 'BINARY',
        'varbinary': 'VARBINARY',
        'image': 'LONGBLOB',
        'uniqueidentifier': 'VARCHAR(36)',
        'xml': 'LONGTEXT',
    }

    mysql_type = mssql_to_mysql.get(type_lower, 'VARCHAR(255)')

    # Handle MAX length
    if length == -1:
        if mysql_type in ('VARCHAR', 'CHAR'):
            return 'LONGTEXT'
        elif mysql_type in ('VARBINARY', 'BINARY'):
            return 'LONGBLOB'

    # Add length parameters where applicable
    if mysql_type in ('VARCHAR', 'CHAR', 'BINARY', 'VARBINARY') and length and length > 0:
        return f"{mysql_type}({length})"
    elif mysql_type == 'DECIMAL' and length:
        if scale:
            return f"DECIMAL({length},{scale})"
        return f"DECIMAL({length})"

    return mysql_type