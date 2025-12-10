"""
CDC Discovery Views.

Handles table discovery from source databases.
"""

import logging
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from sqlalchemy import text

from client.models.database import ClientDatabase
from client.utils.database_utils import (
    get_table_list,
    get_table_schema,
    get_database_engine,
    check_binary_logging
)

logger = logging.getLogger(__name__)


def cdc_discover_tables(request, database_pk):
    """
    Discover all tables from source database
    Returns list of tables with metadata including totals
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
        # Check if binary logging is enabled (for MySQL)
        if db_config.db_type.lower() == 'mysql':
            is_enabled, log_format = check_binary_logging(db_config)
            if not is_enabled:
                messages.warning(
                    request,
                    'Binary logging is not enabled on this MySQL database. CDC requires binary logging to be enabled.'
                )

        # Get list of tables
        tables = get_table_list(db_config)
        logger.info(f"Found {len(tables)} tables in database")

        # Get table details (row count, size estimates)
        tables_with_info = []
        engine = get_database_engine(db_config)

        with engine.connect() as conn:
            for table_name in tables:
                try:
                    # Get row count - use proper text() wrapper
                    if db_config.db_type.lower() == 'mysql':
                        count_query = text(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
                    elif db_config.db_type.lower() == 'postgresql':
                        count_query = text(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
                    else:
                        count_query = text(f"SELECT COUNT(*) as cnt FROM {table_name}")

                    result = conn.execute(count_query)
                    row = result.fetchone()
                    row_count = row[0] if row else 0

                    # Get table schema
                    schema = get_table_schema(db_config, table_name)
                    columns = schema.get('columns', [])

                    # Check if table has timestamp column
                    has_timestamp = any(
                        'timestamp' in str(col.get('type', '')).lower() or
                        'datetime' in str(col.get('type', '')).lower() or
                        str(col.get('name', '')).endswith('_at')
                        for col in columns
                    )

                    # Generate prefixed target table name
                    source_db_name = db_config.database_name
                    target_table_name = f"{source_db_name}_{table_name}"

                    tables_with_info.append({
                        'name': table_name,
                        'target_name': target_table_name,
                        'row_count': row_count,
                        'column_count': len(columns),
                        'has_timestamp': has_timestamp,
                        'primary_keys': schema.get('primary_keys', []),
                    })

                    logger.debug(f"Table {table_name}: {row_count} rows, {len(columns)} columns")

                except Exception as e:
                    logger.error(f"Error getting info for table {table_name}: {str(e)}", exc_info=True)
                    source_db_name = db_config.database_name
                    target_table_name = f"{source_db_name}_{table_name}"
                    tables_with_info.append({
                        'name': table_name,
                        'target_name': target_table_name,
                        'row_count': 0,
                        'column_count': 0,
                        'has_timestamp': False,
                        'primary_keys': [],
                        'error': str(e)
                    })

        engine.dispose()

        # Calculate totals
        total_rows = sum(t['row_count'] for t in tables_with_info)
        total_cols = sum(t['column_count'] for t in tables_with_info)
        logger.info(f"Discovery complete: {len(tables_with_info)} tables, {total_rows} total rows, {total_cols} total columns")

        context = {
            'db_config': db_config,
            'client': db_config.client,
            'tables': tables_with_info,
            'total_tables': len(tables_with_info),
            'total_rows': total_rows,
            'total_columns': total_cols,
        }

        return render(request, 'client/cdc/discover_tables.html', context)

    except Exception as e:
        logger.error(f'Failed to discover tables: {str(e)}', exc_info=True)
        messages.error(request, f'Failed to discover tables: {str(e)}')
        return redirect('client_detail', pk=db_config.client.pk)
