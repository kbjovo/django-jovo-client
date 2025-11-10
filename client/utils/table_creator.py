# """
# File: client/utils/table_creator.py
# Action: CREATE THIS NEW FILE

# Table Creator Utility
# Auto-creates tables in target database based on replication configuration
# """

# import logging
# import re
# from typing import Optional
# from sqlalchemy import MetaData, Table, Column, inspect
# from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time, DECIMAL
# from sqlalchemy.dialects.mysql import BIGINT, TINYINT

# from client.utils.database_utils import get_database_engine, get_table_schema

# logger = logging.getLogger(__name__)


# def create_target_tables(replication_config):
#     """
#     Auto-create tables in target database based on configuration
    
#     Args:
#         replication_config: ReplicationConfig instance
#     """
#     # Get target database
#     client = replication_config.client_database.client
#     target_db = client.client_databases.filter(is_target=True).first()
    
#     if not target_db:
#         raise Exception("No target database configured for client")
    
#     target_engine = get_database_engine(target_db)
#     metadata = MetaData()
#     source_engine = get_database_engine(replication_config.client_database)
    
#     try:
#         table_mappings = replication_config.table_mappings.filter(is_enabled=True)
        
#         for table_mapping in table_mappings:
#             source_table = table_mapping.source_table
#             target_table = table_mapping.target_table
            
#             logger.info(f"Creating target table: {target_table} from {source_table}")
            
#             # Get source table schema
#             source_schema = get_table_schema(replication_config.client_database, source_table)
            
#             # Get column mappings
#             column_mappings = table_mapping.column_mappings.filter(is_enabled=True)
            
#             if not column_mappings.exists():
#                 logger.warning(f"No column mappings for {source_table}, skipping")
#                 continue
            
#             # Build SQLAlchemy columns
#             columns = []
#             for col_mapping in column_mappings:
#                 source_col_name = col_mapping.source_column
#                 target_col_name = col_mapping.target_column
#                 col_type_str = col_mapping.source_type
                
#                 # Map type
#                 col_type = map_type_to_sqlalchemy(col_type_str)
                
#                 # Check if PK
#                 is_pk = source_col_name in source_schema.get('primary_keys', [])
                
#                 # Check nullable
#                 source_columns = source_schema.get('columns', [])
#                 source_col_info = next(
#                     (c for c in source_columns if c.get('name') == source_col_name), 
#                     None
#                 )
#                 nullable = source_col_info.get('nullable', True) if source_col_info else True
                
#                 col = Column(
#                     target_col_name,
#                     col_type,
#                     primary_key=is_pk,
#                     nullable=nullable and not is_pk,
#                 )
#                 columns.append(col)
            
#             # Create table
#             table = Table(target_table, metadata, *columns, extend_existing=True)
            
#             with target_engine.connect() as conn:
#                 table.create(conn, checkfirst=True)
#                 conn.commit()
            
#             logger.info(f"Successfully created table: {target_table}")
            
#     except Exception as e:
#         logger.error(f"Error creating target tables: {e}", exc_info=True)
#         raise
#     finally:
#         source_engine.dispose()
#         target_engine.dispose()


# def map_type_to_sqlalchemy(type_str: str):
#     """Map database type string to SQLAlchemy type"""
#     type_lower = str(type_str).lower()
    
#     if 'tinyint' in type_lower:
#         return TINYINT
#     elif 'bigint' in type_lower:
#         return BIGINT
#     elif 'int' in type_lower:
#         return Integer
#     elif 'varchar' in type_lower or 'char' in type_lower:
#         match = re.search(r'\((\d+)\)', type_str)
#         length = int(match.group(1)) if match else 255
#         return String(length)
#     elif 'text' in type_lower:
#         return Text
#     elif 'decimal' in type_lower or 'numeric' in type_lower:
#         return DECIMAL(18, 4)
#     elif 'float' in type_lower or 'double' in type_lower:
#         return Float
#     elif 'bool' in type_lower:
#         return Boolean
#     elif 'datetime' in type_lower or 'timestamp' in type_lower:
#         return DateTime
#     elif 'date' in type_lower:
#         return Date
#     elif 'time' in type_lower:
#         return Time
#     else:
#         return String(255)





"""
File: client/utils/table_creator.py
Action: CREATE THIS NEW FILE

Table Creator Utility
Auto-creates tables in target database based on replication configuration.
"""

import logging
import re
from typing import Optional
from sqlalchemy import MetaData, Table, Column, inspect
from sqlalchemy import Integer, String, Text, Float, Boolean, DateTime, Date, Time, DECIMAL
from sqlalchemy.dialects.mysql import BIGINT, TINYINT
from sqlalchemy.schema import CreateTable

from client.utils.database_utils import get_database_engine, get_table_schema

logger = logging.getLogger(__name__)


def create_target_tables(replication_config):
    """
    Auto-create tables in target database based on replication configuration.

    Args:
        replication_config: ReplicationConfig instance
    """
    # ──────────────────────────────
    # 1️⃣ Get target DB connection
    # ──────────────────────────────
    client = replication_config.client_database.client
    target_db = client.client_databases.filter(is_target=True).first()

    if not target_db:
        raise Exception("No target database configured for client")

    source_engine = get_database_engine(replication_config.client_database)
    target_engine = get_database_engine(target_db)
    metadata = MetaData()

    try:
        # ──────────────────────────────
        # 2️⃣ Get table mappings
        # ──────────────────────────────
        table_mappings = replication_config.table_mappings.filter(is_enabled=True)

        for table_mapping in table_mappings:
            source_table = table_mapping.source_table
            target_table = table_mapping.target_table

            logger.info(f"🔧 Creating target table '{target_table}' from source '{source_table}'")

            # ──────────────────────────────
            # 3️⃣ Get source schema
            # ──────────────────────────────
            source_schema = get_table_schema(replication_config.client_database, source_table)
            if not source_schema:
                logger.warning(f"⚠️ Could not retrieve schema for table '{source_table}' — skipping")
                continue

            # ──────────────────────────────
            # 4️⃣ Column mappings
            # ──────────────────────────────
            column_mappings = table_mapping.column_mappings.filter(is_enabled=True)
            if not column_mappings.exists():
                logger.warning(f"⚠️ No column mappings for '{source_table}', skipping")
                continue

            columns = []
            for col_mapping in column_mappings:
                source_col_name = col_mapping.source_column
                target_col_name = col_mapping.target_column
                col_type_str = col_mapping.source_type or ""

                # Map type
                col_type = map_type_to_sqlalchemy(col_type_str)

                # Primary key check
                is_pk = source_col_name in source_schema.get("primary_keys", [])

                # Nullable check
                source_columns = source_schema.get("columns", [])
                source_col_info = next(
                    (c for c in source_columns if c.get("name") == source_col_name),
                    None
                )
                nullable = source_col_info.get("nullable", True) if source_col_info else True

                # Build SQLAlchemy column
                col = Column(
                    target_col_name,
                    col_type,
                    primary_key=is_pk,
                    nullable=nullable and not is_pk,
                )
                columns.append(col)

            # ──────────────────────────────
            # 5️⃣ Create table
            # ──────────────────────────────
            table = Table(target_table, metadata, *columns, extend_existing=True)

            # Log generated SQL before execution
            create_sql = str(CreateTable(table).compile(target_engine))
            logger.info(f"🧩 Generated SQL for '{target_table}':\n{create_sql}")

            with target_engine.connect() as conn:
                table.create(conn, checkfirst=True)
                conn.commit()

            logger.info(f"✅ Successfully created table '{target_table}'")

    except Exception as e:
        logger.exception("❌ Error while creating target tables", exc_info=True)
        raise
    finally:
        source_engine.dispose()
        target_engine.dispose()


# ──────────────────────────────
# Helper: Map MySQL types to SQLAlchemy
# ──────────────────────────────
def map_type_to_sqlalchemy(type_str: str):
    """Map a MySQL type string to an SQLAlchemy type instance."""
    type_lower = str(type_str).lower()

    if "tinyint" in type_lower:
        return TINYINT()
    elif "bigint" in type_lower:
        return BIGINT()
    elif "int" in type_lower:
        return Integer()
    elif "varchar" in type_lower or "char" in type_lower:
        match = re.search(r"\((\d+)\)", type_str)
        length = int(match.group(1)) if match else 255
        return String(length)
    elif "text" in type_lower:
        return Text()
    elif "decimal" in type_lower or "numeric" in type_lower:
        return DECIMAL(18, 4)
    elif "float" in type_lower or "double" in type_lower:
        return Float()
    elif "bool" in type_lower:
        return Boolean()
    elif "datetime" in type_lower or "timestamp" in type_lower:
        return DateTime()
    elif "date" in type_lower:
        return Date()
    elif "time" in type_lower:
        return Time()
    else:
        logger.warning(f"⚠️ Unrecognized column type '{type_str}', defaulting to String(255)")
        return String(255)
