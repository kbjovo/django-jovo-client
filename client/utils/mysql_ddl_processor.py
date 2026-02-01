# mysql_ddl_processor_sqlalchemy.py

import logging
import re
from typing import Dict, List, Optional, Any
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import (
    MetaData, Table, Column, Index, PrimaryKeyConstraint,
    Integer, String, Text, Boolean, DateTime, Date, Time,
    Numeric, Float, LargeBinary, JSON,
    text, inspect, create_engine
)
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable, DropTable, AddConstraint, DropConstraint
from sqlalchemy.dialects import postgresql
from django.utils import timezone

logger = logging.getLogger(__name__)


class SQLAlchemyDDLProcessor:
    """
    Process MySQL DDL changes using SQLAlchemy for better type safety and portability.
    
    Uses hybrid approach:
    - SQLAlchemy for standard operations (CREATE, ADD COLUMN, etc.)
    - Raw SQL for complex operations (MODIFY with USING clause)
    """

    # Type mapping: MySQL → SQLAlchemy types
    TYPE_MAP = {
        'TINYINT': Integer,
        'SMALLINT': Integer,
        'MEDIUMINT': Integer,
        'INT': Integer,
        'BIGINT': Integer,
        'FLOAT': Float,
        'DOUBLE': Float,
        'DECIMAL': Numeric,
        'VARCHAR': String,
        'CHAR': String,
        'TEXT': Text,
        'MEDIUMTEXT': Text,
        'LONGTEXT': Text,
        'TINYTEXT': Text,
        'BLOB': LargeBinary,
        'MEDIUMBLOB': LargeBinary,
        'LONGBLOB': LargeBinary,
        'DATE': Date,
        'DATETIME': DateTime,
        'TIMESTAMP': DateTime,
        'TIME': Time,
        'YEAR': Integer,
        'JSON': JSON,
        'ENUM': String,  # Fallback to VARCHAR
        'SET': Text,
    }

    def __init__(
        self,
        bootstrap_servers: str,
        schema_topic: str,
        target_engine: Engine,
        target_schema: str = 'public',
        consumer_group: str = None,
        auto_execute_destructive: bool = False
    ):
        self.bootstrap_servers = bootstrap_servers
        self.schema_topic = schema_topic
        self.target_engine = target_engine
        self.target_schema = target_schema
        self.auto_execute_destructive = auto_execute_destructive
        
        # SQLAlchemy metadata for schema management
        self.metadata = MetaData(schema=target_schema)
        
        # Inspector for schema reflection
        self.inspector = inspect(target_engine)
        
        # Schema cache
        self.schema_cache = {}
        
        # Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': consumer_group or f'ddl-sqlalchemy-{schema_topic}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        
        self.consumer.subscribe([schema_topic])
        logger.info(f"✅ SQLAlchemy DDL Processor initialized")

    def _handle_create_table(self, table_info: Dict, ddl: str):
        """Handle CREATE TABLE using SQLAlchemy."""
        table_id = table_info.get('id', '').replace('"', '')
        table_name = self._extract_table_name(table_id)
        
        logger.info(f"📝 Creating table: {table_name}")
        
        try:
            # Build Table object
            table = self._build_sqlalchemy_table(table_name, table_info)
            
            # Generate CREATE TABLE statement
            create_stmt = CreateTable(table)
            
            # Compile to SQL for logging
            compiled = create_stmt.compile(dialect=postgresql.dialect())
            logger.info(f"SQLAlchemy generated SQL:\n{compiled}")
            
            # Execute
            table.create(self.target_engine, checkfirst=True)
            
            logger.info(f"✅ Table created: {self.target_schema}.{table_name}")
            self._log_audit('CREATE', table_name, str(compiled), 'success')
            
            # Cache schema
            self._cache_table_schema(table_name, table_info)
            
        except Exception as e:
            logger.error(f"❌ CREATE failed: {e}")
            self._log_audit('CREATE', table_name, '', 'failed', str(e))
            raise

    def _build_sqlalchemy_table(self, table_name: str, table_info: Dict) -> Table:
        """
        Build SQLAlchemy Table object from Debezium table info.
        
        This is the key advantage of SQLAlchemy - type-safe table definitions.
        """
        columns_list = []
        pk_columns = table_info.get('primaryKeyColumnNames', [])
        
        for col in table_info.get('columns', []):
            sqlalchemy_col = self._build_sqlalchemy_column(col, pk_columns)
            columns_list.append(sqlalchemy_col)
        
        # Create table
        table = Table(
            table_name,
            self.metadata,
            *columns_list,
            schema=self.target_schema
        )
        
        return table

    def _build_sqlalchemy_column(self, col_info: Dict, pk_columns: List[str]) -> Column:
        """
        Build SQLAlchemy Column object from Debezium column info.
        
        Args:
            col_info: Column metadata from Debezium
            pk_columns: List of primary key column names
            
        Returns:
            SQLAlchemy Column object
        """
        name = col_info['name']
        type_name = col_info.get('typeName', '').upper()
        length = col_info.get('length')
        scale = col_info.get('scale')
        optional = col_info.get('optional', True)
        auto_incremented = col_info.get('autoIncremented', False)
        
        # Get SQLAlchemy type
        sqlalchemy_type_class = self.TYPE_MAP.get(type_name, String)
        
        # Build type with parameters
        if sqlalchemy_type_class in (String, LargeBinary):
            if length:
                col_type = sqlalchemy_type_class(length)
            else:
                col_type = Text()  # Unbounded
        elif sqlalchemy_type_class == Numeric:
            if length and scale:
                col_type = Numeric(precision=length, scale=scale)
            elif length:
                col_type = Numeric(precision=length)
            else:
                col_type = Numeric()
        else:
            col_type = sqlalchemy_type_class()
        
        # Build column
        is_primary = name in pk_columns
        
        column = Column(
            name,
            col_type,
            primary_key=is_primary,
            nullable=optional and not is_primary,
            autoincrement=auto_incremented
        )
        
        return column

    def _handle_alter_add_column(self, table_name: str, column_info: Dict):
        """
        Handle ADD COLUMN using SQLAlchemy.
        
        This is cleaner and safer than raw SQL.
        """
        from sqlalchemy.schema import AddColumn
        
        full_table = f"{self.target_schema}.{table_name}"
        
        # Build Column object
        col = self._build_sqlalchemy_column(column_info, [])
        
        # Create ADD COLUMN operation
        add_col_stmt = AddColumn(col)
        
        # Compile to SQL
        compiled = add_col_stmt.compile(dialect=postgresql.dialect())
        logger.info(f"   SQLAlchemy: {compiled}")
        
        # Execute using Alembic-style operation
        with self.target_engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {full_table} ADD COLUMN {col.compile(dialect=postgresql.dialect())}"))
        
        logger.info(f"   ✅ Column added: {col.name}")

    def _handle_alter_drop_column(self, table_name: str, column_name: str):
        """Handle DROP COLUMN using SQLAlchemy."""
        from sqlalchemy.schema import DropColumn
        
        full_table = f"{self.target_schema}.{table_name}"
        
        # Simple and safe
        with self.target_engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {full_table} DROP COLUMN {column_name}"))
        
        logger.info(f"   ✅ Column dropped: {column_name}")

    def _handle_alter_modify_column_type(self, table_name: str, operation: Dict):
        """
        Handle MODIFY COLUMN TYPE.
        
        This uses RAW SQL because SQLAlchemy doesn't support USING clause well.
        """
        full_table = f"{self.target_schema}.{table_name}"
        col_name = operation['column_name']
        col_info = operation['column']
        
        # Build new type using SQLAlchemy
        col = self._build_sqlalchemy_column(col_info, [])
        new_type = col.type.compile(dialect=postgresql.dialect())
        
        # Check if USING clause needed
        old_type = operation['old_type']
        new_type_name = operation['new_type']
        
        if self._needs_using_clause(old_type, new_type_name):
            # Use raw SQL with USING clause
            sql = f"ALTER TABLE {full_table} ALTER COLUMN {col_name} TYPE {new_type} USING {col_name}::{new_type}"
        else:
            sql = f"ALTER TABLE {full_table} ALTER COLUMN {col_name} TYPE {new_type}"
        
        logger.info(f"   Executing: {sql}")
        
        with self.target_engine.begin() as conn:
            conn.execute(text(sql))
        
        logger.info(f"   ✅ Column type modified: {col_name}")

    def _handle_alter_rename_column(self, table_name: str, old_name: str, new_name: str):
        """Handle RENAME COLUMN using SQLAlchemy."""
        from sqlalchemy import DDL
        
        full_table = f"{self.target_schema}.{table_name}"
        
        # SQLAlchemy supports this cleanly
        sql = f"ALTER TABLE {full_table} RENAME COLUMN {old_name} TO {new_name}"
        
        with self.target_engine.begin() as conn:
            conn.execute(text(sql))
        
        logger.info(f"   ✅ Column renamed: {old_name} → {new_name}")

    def _handle_alter_add_index(self, table_name: str, operation: Dict):
        """
        Handle ADD INDEX using SQLAlchemy.
        
        Much cleaner than raw SQL!
        """
        index_name = operation['index_name']
        columns_str = operation['columns']
        unique = operation.get('unique', False)
        
        # Parse column names
        columns = [c.strip() for c in columns_str.split(',')]
        
        # Create Index object
        index = Index(
            index_name,
            *columns,
            unique=unique,
            _table=f"{self.target_schema}.{table_name}"
        )
        
        # Create index
        index.create(self.target_engine)
        
        logger.info(f"   ✅ Index created: {index_name}")

    def _handle_alter_add_primary_key(self, table_name: str, columns: List[str]):
        """Handle ADD PRIMARY KEY."""
        full_table = f"{self.target_schema}.{table_name}"
        
        sql = f"ALTER TABLE {full_table} ADD PRIMARY KEY ({', '.join(columns)})"
        
        with self.target_engine.begin() as conn:
            conn.execute(text(sql))
        
        logger.info(f"   ✅ Primary key added: {columns}")

    def _reflect_table_schema(self, table_name: str) -> Dict:
        """
        Reflect current table schema from database using SQLAlchemy Inspector.
        
        This is more reliable than caching!
        """
        try:
            columns = self.inspector.get_columns(table_name, schema=self.target_schema)
            pk = self.inspector.get_pk_constraint(table_name, schema=self.target_schema)
            indexes = self.inspector.get_indexes(table_name, schema=self.target_schema)
            
            return {
                'columns': columns,
                'primaryKey': pk,
                'indexes': indexes
            }
        except Exception as e:
            logger.warning(f"Could not reflect table {table_name}: {e}")
            return {}

    # ... (rest of the methods similar to raw SQL version)
    
    def _needs_using_clause(self, old_type: str, new_type: str) -> bool:
        """Check if type conversion needs USING clause."""
        incompatible = [
            ('VARCHAR', 'INT'),
            ('TEXT', 'INT'),
            ('VARCHAR', 'NUMERIC'),
            ('TEXT', 'NUMERIC'),
            ('INT', 'VARCHAR'),
            ('NUMERIC', 'VARCHAR'),
        ]
        
        old = old_type.upper()
        new = new_type.upper()
        
        return (old, new) in incompatible or (new, old) in incompatible

    def _extract_table_name(self, table_id: str) -> str:
        """Extract table name from table ID."""
        if '.' in table_id:
            _, table_name = table_id.rsplit('.', 1)
            return table_name
        return table_id

    def _cache_table_schema(self, table_name: str, table_info: Dict):
        """Cache table schema."""
        self.schema_cache[table_name] = table_info

    def _log_audit(self, operation: str, table_name: str, ddl_sql: str, status: str, error_message: str = ''):
        """Log DDL audit."""
        try:
            audit_sql = text("""
                INSERT INTO public.ddl_audit_log 
                (operation, table_name, ddl_sql, status, error_message, created_at)
                VALUES (:op, :table, :sql, :status, :error, :created)
            """)
            
            with self.target_engine.begin() as conn:
                conn.execute(audit_sql, {
                    'op': operation,
                    'table': table_name,
                    'sql': ddl_sql[:5000],
                    'status': status,
                    'error': error_message[:1000] if error_message else '',
                    'created': timezone.now()
                })
        except Exception as e:
            logger.error(f"Failed to log audit: {e}")

    def close(self):
        """Close consumer."""
        self.consumer.close()
        


