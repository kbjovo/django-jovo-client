from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponse
from client.models.client import Client
from client.models.database import ClientDatabase
from .forms import ClientForm, ClientDatabaseForm
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.debezium_manager import DebeziumConnectorManager
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView, View
from django.urls import reverse_lazy, reverse
from django.http import JsonResponse
from django.db import transaction
from django.utils.decorators import method_decorator
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db.models import Q
import logging
from jovoclient.utils.table_utils import build_paginated_table
from sqlalchemy import text
from client.utils.connector_templates import (
    get_connector_config_for_database,
    generate_connector_name
)
from client.utils.database_utils import (
    get_table_list, 
    get_table_schema,
    get_database_engine,
    check_binary_logging
)


from client.tasks import (
    create_debezium_connector,
    start_kafka_consumer,
    stop_kafka_consumer,
    delete_debezium_connector,
    restart_replication
)

logger = logging.getLogger(__name__)

class ClientCreateView(CreateView):
    model = Client
    form_class = ClientForm
    template_name = 'client/client_form.html'
    success_url = reverse_lazy('main-dashboard')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['is_update'] = False
        return context

    def form_valid(self, form):
        self.object = form.save()
        messages.success(self.request, 'Client created successfully!')
        response = super().form_valid(form)
        # Trigger toast notification for HTMX requests
        if self.request.headers.get('HX-Request'):
            from django.http import HttpResponse
            response = HttpResponse()
            response['HX-Redirect'] = self.success_url
            response['HX-Trigger'] = 'clientCreated'
        return response



class ClientUpdateView(UpdateView):
    model = Client
    form_class = ClientForm
    template_name = 'client/client_form.html'
    success_url = reverse_lazy('main-dashboard')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['is_update'] = True
        context['client'] = self.object
        return context
    
    def form_valid(self, form):
        old_status = self.object.status
        new_status = form.cleaned_data.get('status')
        
        self.object = form.save()
        
        if old_status != new_status:
            if new_status == "active" and old_status == "inactive":
                self.object.deleted_at = None
                self.object.save(update_fields=['deleted_at'])

        # Check if this is an HTMX request from Basic Details tab
        if self.request.headers.get('HX-Request'):
            # Check if target is basic-details-content (inline edit)
            if 'basic-details-content' in self.request.headers.get('HX-Target', ''):
                # Return only the view partial
                return render(
                    self.request,
                    'client/partials/basic_details_view.html',
                    {'client': self.object}
                )
            else:
                # Full page edit - return detail page
                context = {'client': self.object}
                response = render(self.request, 'client/client_detail.html', context)
                response['HX-Trigger'] = 'clientUpdated'
                return response

        return redirect('client_detail', pk=self.object.pk)
    
    def form_invalid(self, form):
        if self.request.headers.get('HX-Request'):
            response = render(
                self.request,
                'client/client_form.html',
                {'form': form, 'is_update': True, 'client': self.object}
            )
            response['HX-Trigger'] = 'formError'
            return response
        return super().form_invalid(form)



class ClientDetailView(DetailView):
    model = Client
    template_name = 'client/client_detail.html'
    context_object_name = 'client'


@require_http_methods(["POST"])
def client_soft_delete(request, pk):
    """
    Soft deletes a client.
    """
    client = get_object_or_404(Client, pk=pk)
    client.soft_delete()

    messages.success(request, 'Client deleted successfully!')

    # For HTMX requests, trigger toast notification
    if request.headers.get('HX-Request'):
        response = redirect("main-dashboard")
        response['HX-Trigger'] = 'clientDeleted'
        return response

    return redirect("main-dashboard")


@require_http_methods(["GET"])
def check_client_unique_field(request):
    """
    Check if a field value is unique.
    Used for real-time validation.
    """
    field = request.GET.get('field')
    value = request.GET.get('value', '').strip()
    client_id = request.GET.get('client_id')  # For edit mode
    
    if not field or not value:
        return JsonResponse({'is_unique': True})
    
    # Build query
    query = {field: value}
    queryset = Client.objects.filter(**query)
    
    # Exclude current client when editing
    if client_id:
        queryset = queryset.exclude(pk=client_id)
    
    is_unique = not queryset.exists()
    
    return JsonResponse({
        'is_unique': is_unique,
        'message': f'This {field.replace("_", " ")} is already taken.' if not is_unique else ''
    })



def dashboard(request):
    """
    Main dashboard view with automatic client table and replications data.
    """
    clients = Client.objects.filter(status__in=["active", "inactive"]).order_by('-id')

    table_config = {
        'exclude': ['country', 'updated_at', 'deleted_at'],
        'searchable': ['name', 'email', 'phone', 'company_name', 'db_name'],
        'detail_url_name': 'client_detail',
        'per_page': 10,
        'selectable': True,
        'empty_message': 'No clients found',
        'column_overrides': {
            'name': {
                'type': 'link',
                'clickable': True,
            },
            'status': {
                'type': 'badge',
                'badge_colors': {
                    'active': 'green',
                    'inactive': 'yellow',
                    'deleted': 'red',
                }
            },
        }
    }

    table_data = build_paginated_table(
        queryset=clients,
        request=request,
        config=table_config
    )

    # Get replication data for Replications tab - only show configs with connector_name
    all_replications = ReplicationConfig.objects.select_related(
        'client_database', 'client_database__client'
    ).prefetch_related('table_mappings').exclude(
        connector_name__isnull=True
    ).exclude(
        connector_name=''
    ).order_by('-created_at')

    # Calculate statistics
    total_replications = all_replications.count()
    active_count = all_replications.filter(status='active').count()
    total_tables = sum(config.table_mappings.filter(is_enabled=True).count() for config in all_replications)
    has_errors = all_replications.filter(status='error').exists()

    # Build replication rows for table display
    replication_rows = []
    for config in all_replications:
        replication_rows.append({
            'id': config.id,
            'client_name': config.client_database.client.name,
            'client_id': config.client_database.client.id,
            'database_name': config.client_database.connection_name,
            'database_type': config.client_database.get_db_type_display(),
            'status': config.status,
            'status_display': config.get_status_display(),
            'connector_name': config.connector_name,
            'tables_count': config.table_mappings.filter(is_enabled=True).count(),
            'last_sync': config.last_sync_at,
            'sync_type': config.get_sync_type_display(),
        })

    # Add replication data to context
    table_data['replication_rows'] = replication_rows
    table_data['total_replications'] = total_replications
    table_data['active_count'] = active_count
    table_data['total_tables'] = total_tables
    table_data['has_errors'] = has_errors
    table_data['all_clients'] = clients

    return render(request, "dashboard.html", table_data)


class ClientDatabaseCreateView(CreateView):
    model = ClientDatabase
    form_class = ClientDatabaseForm
    template_name = 'client/partials/database_form.html'
    
    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        self.client = get_object_or_404(Client, pk=self.kwargs['client_pk'])
        kwargs['client'] = self.client
        return kwargs
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['client'] = self.client
        context['is_update'] = False
        return context
    
    def post(self, request, *args, **kwargs):
        self.object = None
        self.client = get_object_or_404(Client, pk=self.kwargs['client_pk'])
        form = self.get_form()
        
        # Check if this is a test-only request
        if request.POST.get('test_only') == 'true':
            if form.is_valid():
                # Create temporary instance without saving
                temp_instance = form.save(commit=False)
                temp_instance.client = self.client
                
                try:
                    # Test connection
                    status = temp_instance.check_connection_status(save=False)
                    
                    if status == 'success':
                        return JsonResponse({
                            'success': True,
                            'message': '✓ Connection test successful! The database is reachable and credentials are valid.'
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'message': '✗ Connection test failed. Unable to connect to the database. Please verify the host, port, username, and password are correct.'
                        })
                except Exception as e:
                    # Return user-friendly error message (traceback suppressed)
                    error_message = str(e)
                    return JsonResponse({
                        'success': False,
                        'message': f'✗ Connection test failed: {error_message}'
                    })
            else:
                # Form validation failed
                errors = []
                for field, error_list in form.errors.items():
                    for error in error_list:
                        errors.append(f"{field}: {error}")
                
                return JsonResponse({
                    'success': False,
                    'message': f'✗ Please fill in all required fields correctly. {", ".join(errors)}'
                })
        
        # Normal save operation
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)
    
    def form_valid(self, form):
        form.instance.client = self.client
        self.object = form.save()
        
        # Test connection and save status
        try:
            self.object.check_connection_status(save=True)
        except Exception as e:
            print(f"Error checking connection status after save: {e}")
            # Continue anyway - connection status will be marked as failed
        
        if self.request.headers.get('HX-Request') or self.request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            # Return the databases list view
            databases = ClientDatabase.objects.filter(client=self.client)
            return render(
                self.request,
                'client/partials/databases_list.html',
                {'client': self.client, 'databases': databases}
            )
        
        return redirect('client_detail', pk=self.client.pk)


class ClientDatabaseUpdateView(UpdateView):
    model = ClientDatabase
    form_class = ClientDatabaseForm
    template_name = 'client/partials/database_form.html'
    
    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['client'] = self.object.client
        return kwargs
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['client'] = self.object.client
        context['is_update'] = True
        return context
    
    def post(self, request, *args, **kwargs):
        self.object = self.get_object()
        form = self.get_form()
        
        # Check if this is a test-only request
        if request.POST.get('test_only') == 'true':
            if form.is_valid():
                # Create temporary instance without saving
                temp_instance = form.save(commit=False)

                # If password is empty in update mode, use the existing password for testing
                if not form.cleaned_data.get('password'):
                    temp_instance.password = self.object.password

                try:
                    # Test connection
                    status = temp_instance.check_connection_status(save=False)
                    
                    if status == 'success':
                        return JsonResponse({
                            'success': True,
                            'message': '✓ Connection test successful! The database is reachable and credentials are valid.'
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'message': '✗ Connection test failed. Unable to connect to the database. Please verify the host, port, username, and password are correct.'
                        })
                except Exception as e:
                    # Return user-friendly error message (traceback suppressed)
                    error_message = str(e)
                    return JsonResponse({
                        'success': False,
                        'message': f'✗ Connection test failed: {error_message}'
                    })
            else:
                # Form validation failed
                errors = []
                for field, error_list in form.errors.items():
                    for error in error_list:
                        errors.append(f"{field}: {error}")
                
                return JsonResponse({
                    'success': False,
                    'message': f'✗ Please fill in all required fields correctly. {", ".join(errors)}'
                })
        
        # Normal save operation
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)
    
    def form_valid(self, form):
        # Handle password update: if empty, keep the existing password
        if not form.cleaned_data.get('password'):
            # Password field is empty, preserve the existing encrypted password
            old_password = self.object.password
            self.object = form.save(commit=False)
            self.object.password = old_password
            self.object.save()
        else:
            # New password provided, save normally (will be encrypted by model's save method)
            self.object = form.save()

        # Test connection and save status
        try:
            self.object.check_connection_status(save=True)
        except Exception:
            pass  # Continue anyway - connection status will be marked as failed

        if self.request.headers.get('HX-Request') or self.request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            # Return the databases list view
            databases = ClientDatabase.objects.filter(client=self.object.client)
            return render(
                self.request,
                'client/partials/databases_list.html',
                {'client': self.object.client, 'databases': databases}
            )

        return redirect('client_detail', pk=self.object.client.pk)


class ClientDatabaseDeleteView(View):
    def post(self, request, pk):
        database = get_object_or_404(ClientDatabase, pk=pk)
        client = database.client
        database.delete()
        
        if request.headers.get('HX-Request'):
            databases = ClientDatabase.objects.filter(client=client)
            return render(
                request,
                'client/partials/databases_list.html',
                {'client': client, 'databases': databases}
            )
        
        return redirect('client_detail', pk=client.pk)
    


# ============================================
# 1. CDC Dashboard - Show all configurations
# ============================================
# REMOVED: CDC Dashboard - Now using direct navigation to monitor/setup
# @require_http_methods(["GET"])
# def cdc_dashboard(request, client_pk):
#     """
#     Main CDC dashboard showing all replication configurations for a client
#     """
#     client = get_object_or_404(Client, pk=client_pk)
#     databases = ClientDatabase.objects.filter(client=client)
#
#     # Get all replication configs with statistics
#     replication_configs = ReplicationConfig.objects.filter(
#         client_database__client=client
#     ).select_related('client_database')
#
#     context = {
#         'client': client,
#         'databases': databases,
#         'replication_configs': replication_configs,
#     }
#
#     return render(request, 'client/cdc/dashboard.html', context)



@require_http_methods(["GET", "POST"])
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

# ============================================
# 3. Configure Tables - Step 2
# ============================================
"""
File: client/views/cdc_views.py
Action: REPLACE your cdc_configure_tables function with this

Find this function in your file and REPLACE it entirely
"""

@require_http_methods(["GET", "POST"])
def cdc_configure_tables(request, database_pk):
    """
    Configure selected tables with column selection and mapping
    - Select which columns to replicate
    - Map column names (source -> target)
    - Map table names (source -> target)
    - Auto-create tables in target database
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    # Get selected tables from session
    selected_tables = request.session.get('selected_tables', [])
    if not selected_tables:
        messages.error(request, 'No tables selected')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    if request.method == "POST":
        try:
            with transaction.atomic():
                # Create ReplicationConfig
                replication_config = ReplicationConfig.objects.create(
                    client_database=db_config,
                    sync_type=request.POST.get('sync_type', 'realtime'),
                    sync_frequency=request.POST.get('sync_frequency', 'realtime'),
                    status='configured',
                    is_active=False,
                    auto_create_tables=request.POST.get('auto_create_tables') == 'on',
                    created_by=request.user if request.user.is_authenticated else None
                )
                
                logger.info(f"Created ReplicationConfig: {replication_config.id}")
                
                # Create TableMappings for each table
                for table_name in selected_tables:
                    # Get table-specific settings
                    sync_type = request.POST.get(f'sync_type_{table_name}', '')
                    if not sync_type:  # Use global if not specified
                        sync_type = request.POST.get('sync_type', 'realtime')
                    
                    incremental_col = request.POST.get(f'incremental_col_{table_name}', '')
                    conflict_resolution = request.POST.get(f'conflict_resolution_{table_name}', 'source_wins')

                    # Get target table name (mapped name) - default to prefixed name
                    target_table_name = request.POST.get(f'target_table_name_{table_name}', '')
                    if not target_table_name:
                        # Auto-prefix with source database name to avoid collisions
                        source_db_name = db_config.database_name
                        target_table_name = f"{source_db_name}_{table_name}"
                    
                    # Create TableMapping
                    table_mapping = TableMapping.objects.create(
                        replication_config=replication_config,
                        source_table=table_name,
                        target_table=target_table_name,
                        is_enabled=True,
                        sync_type=sync_type,
                        incremental_column=incremental_col if sync_type == 'incremental' else None,
                        incremental_column_type='timestamp' if incremental_col else '',
                        conflict_resolution=conflict_resolution,
                    )
                    
                    logger.info(f"Created TableMapping: {table_name} -> {target_table_name}")
                    
                    # Get selected columns for this table
                    selected_columns = request.POST.getlist(f'selected_columns_{table_name}')
                    
                    if selected_columns:
                        # Create ColumnMapping for each selected column
                        for source_column in selected_columns:
                            # Get mapped target column name
                            target_column = request.POST.get(
                                f'column_mapping_{table_name}_{source_column}', 
                                source_column
                            )
                            
                            # Get column type from schema
                            try:
                                schema = get_table_schema(db_config, table_name)
                                columns = schema.get('columns', [])
                                column_info = next(
                                    (col for col in columns if col.get('name') == source_column), 
                                    None
                                )
                                
                                source_type = str(column_info.get('type', 'VARCHAR')) if column_info else 'VARCHAR'
                                
                                # Create ColumnMapping
                                ColumnMapping.objects.create(
                                    table_mapping=table_mapping,
                                    source_column=source_column,
                                    target_column=target_column or source_column,
                                    source_type=source_type,
                                    target_type=source_type,  # Same type by default
                                    is_enabled=True,
                                )
                                
                                logger.debug(f"Created ColumnMapping: {source_column} -> {target_column}")
                                
                            except Exception as e:
                                logger.error(f"Error creating column mapping for {source_column}: {e}")
                    else:
                        # If no columns selected, include all columns by default
                        logger.warning(f"No columns selected for {table_name}, including all columns")
                        try:
                            schema = get_table_schema(db_config, table_name)
                            columns = schema.get('columns', [])
                            
                            for col in columns:
                                col_name = col.get('name')
                                col_type = str(col.get('type', 'VARCHAR'))
                                
                                # Get mapped name if provided
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
                
                # Auto-create tables in target database if option is checked
                if replication_config.auto_create_tables:
                    try:
                        from client.utils.table_creator import create_target_tables
                        create_target_tables(replication_config)
                        messages.success(request, 'Target tables created successfully!')
                    except Exception as e:
                        logger.error(f"Failed to create target tables: {e}")
                        messages.warning(request, f'Configuration saved, but failed to create target tables: {str(e)}')
                
                messages.success(request, 'CDC configuration saved successfully!')
                
                # Clear session
                request.session.pop('selected_tables', None)
                request.session.pop('database_pk', None)
                
                return redirect('cdc_create_connector', config_pk=replication_config.pk)
                
        except Exception as e:
            logger.error(f'Failed to save configuration: {e}', exc_info=True)
            messages.error(request, f'Failed to save configuration: {str(e)}')
    
    # GET: Show configuration form
    tables_with_columns = []
    for table_name in selected_tables:
        try:
            schema = get_table_schema(db_config, table_name)
            columns = schema.get('columns', [])
            
            # Find potential incremental columns
            incremental_candidates = [
                col for col in columns
                if 'timestamp' in str(col.get('type', '')).lower() or
                   'datetime' in str(col.get('type', '')).lower() or
                   col.get('name', '').endswith('_at') or
                   col.get('name', '').endswith('_time') or
                   (col.get('name', '') in ['id', 'created_at', 'updated_at'] and 
                    'int' in str(col.get('type', '')).lower())
            ]
            
            # Generate prefixed target table name
            source_db_name = db_config.database_name
            target_table_name = f"{source_db_name}_{table_name}"

            tables_with_columns.append({
                'name': table_name,
                'target_name': target_table_name,
                'columns': columns,
                'primary_keys': schema.get('primary_keys', []),
                'incremental_candidates': incremental_candidates,
            })
            
            logger.debug(f"Table {table_name}: {len(columns)} columns, {len(incremental_candidates)} incremental candidates")
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}", exc_info=True)
            messages.warning(request, f'Could not load schema for table {table_name}')
    
    context = {
        'db_config': db_config,
        'client': db_config.client,
        'tables': tables_with_columns,
    }
    
    return render(request, 'client/cdc/configure_tables.html', context)


# ============================================
# 4. Create Debezium Connector - Step 3
# ============================================
@require_http_methods(["GET", "POST"])
def cdc_create_connector(request, config_pk):
    """
    Create Debezium connector for the replication configuration
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
            tables_list = [tm.source_table for tm in table_mappings]
            
            # Generate connector configuration
            connector_config = get_connector_config_for_database(
                db_config=db_config,
                replication_config=replication_config,
                tables_whitelist=tables_list,
            )
            
            if not connector_config:
                raise Exception("Failed to generate connector configuration")
            
            # Create connector via Debezium Manager
            manager = DebeziumConnectorManager()
            
            # Check Kafka Connect health
            is_healthy, health_error = manager.check_kafka_connect_health()
            if not is_healthy:
                raise Exception(f"Kafka Connect is not healthy: {health_error}")
            
            # Create connector
            success, error = manager.create_connector(
                connector_name=connector_name,
                config=connector_config,
                notify_on_error=True
            )
            
            if not success:
                raise Exception(f"Failed to create connector: {error}")
            
            # Update replication config
            replication_config.connector_name = connector_name
            replication_config.kafka_topic_prefix = f"client_{client.id}"
            replication_config.status = 'active'
            replication_config.is_active = True
            replication_config.save()
            
            # Update client replication status
            client.replication_enabled = True
            client.replication_status = 'active'
            client.save()
            
            messages.success(
                request, 
                f'Debezium connector "{connector_name}" created successfully! CDC is now active.'
            )
            
            return redirect('cdc_monitor_connector', config_pk=replication_config.pk)
            
        except Exception as e:
            messages.error(request, f'Failed to create connector: {str(e)}')
    
    # GET: Show connector creation confirmation
    table_mappings = replication_config.table_mappings.filter(is_enabled=True)
    
    context = {
        'replication_config': replication_config,
        'db_config': db_config,
        'client': client,
        'table_mappings': table_mappings,
    }
    
    return render(request, 'client/cdc/create_connector.html', context)


# ============================================
# 5. Monitor Connector Status
# ============================================
@require_http_methods(["GET"])
def cdc_monitor_connector(request, config_pk):
    """
    Real-time monitoring of connector status
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    
    if not replication_config.connector_name:
        messages.error(request, 'No connector configured for this replication')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)
    
    try:
        manager = DebeziumConnectorManager()
        
        # Get connector status
        exists, status_data = manager.get_connector_status(replication_config.connector_name)
        
        if not exists:
            messages.warning(request, 'Connector not found in Kafka Connect')
            status_data = None
        
        # Get connector tasks
        tasks = manager.get_connector_tasks(replication_config.connector_name) if exists else []
        
        context = {
            'replication_config': replication_config,
            'client': replication_config.client_database.client,
            'connector_status': status_data,
            'tasks': tasks,
            'exists': exists,
        }
        
        return render(request, 'client/cdc/monitor_connector.html', context)
        
    except Exception as e:
        messages.error(request, f'Failed to get connector status: {str(e)}')
        return redirect('client_detail', pk=replication_config.client_database.client.pk)


# ============================================
# 6. AJAX: Get Table Schema
# ============================================
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


# ============================================
# 7. Connector Actions (Pause/Resume/Delete)
# ============================================
@require_http_methods(["POST"])
def cdc_connector_action(request, config_pk, action):
    """
    Handle connector actions: pause, resume, restart, delete
    """
    replication_config = get_object_or_404(ReplicationConfig, pk=config_pk)
    connector_name = replication_config.connector_name
    
    if not connector_name:
        return JsonResponse({
            'success': False,
            'error': 'No connector configured'
        }, status=400)
    
    try:
        manager = DebeziumConnectorManager()
        
        if action == 'pause':
            success, error = manager.pause_connector(connector_name)
            replication_config.status = 'paused' if success else replication_config.status
            message = 'Connector paused successfully'
            
        elif action == 'resume':
            success, error = manager.resume_connector(connector_name)
            replication_config.status = 'active' if success else replication_config.status
            message = 'Connector resumed successfully'
            
        elif action == 'restart':
            success, error = manager.restart_connector(connector_name)
            message = 'Connector restarted successfully'
            
        elif action == 'delete':
            success, error = manager.delete_connector(connector_name, notify=True)
            if success:
                replication_config.status = 'disabled'
                replication_config.connector_name = None
            message = 'Connector deleted successfully'
            
        else:
            return JsonResponse({
                'success': False,
                'error': f'Unknown action: {action}'
            }, status=400)
        
        if success:
            replication_config.save()
            messages.success(request, message)
            return JsonResponse({'success': True, 'message': message})
        else:
            return JsonResponse({
                'success': False,
                'error': error
            }, status=400)
            
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
    


@require_http_methods(["POST"])
def start_replication(request, config_id):
    """Start CDC replication for a config"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        # Check if already running
        if config.is_active and config.status == 'active':
            messages.warning(request, "Replication is already running!")
            return redirect('cdc_config_detail', config_id=config_id)
        
        # Create connector and start consumer
        task = create_debezium_connector.apply_async(args=[config_id])
        
        messages.success(
            request,
            f"🚀 Replication started! Task ID: {task.id}"
        )
        
        return redirect('cdc_config_detail', config_id=config_id)
        
    except Exception as e:
        messages.error(request, f"Failed to start replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def stop_replication(request, config_id):
    """Stop CDC replication for a config"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        if not config.is_active:
            messages.warning(request, "Replication is not running!")
            return redirect('cdc_config_detail', config_id=config_id)
        
        # Stop consumer
        task = stop_kafka_consumer.apply_async(args=[config_id])
        
        messages.success(
            request,
            f"⏸️ Replication stopped! Task ID: {task.id}"
        )
        
        return redirect('cdc_config_detail', config_id=config_id)
        
    except Exception as e:
        messages.error(request, f"Failed to stop replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["POST"])
def restart_replication_view(request, config_id):
    """Restart CDC replication for a config"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        # Restart replication
        task = restart_replication.apply_async(args=[config_id])
        
        messages.success(
            request,
            f"🔄 Replication restarted! Task ID: {task.id}"
        )
        
        return redirect('cdc_config_detail', config_id=config_id)
        
    except Exception as e:
        messages.error(request, f"Failed to restart replication: {str(e)}")
        return redirect('cdc_config_detail', config_id=config_id)


@require_http_methods(["GET"])
def replication_status(request, config_id):
    """Get replication status (AJAX endpoint)"""
    try:
        config = get_object_or_404(ReplicationConfig, id=config_id)
        
        data = {
            'is_active': config.is_active,
            'status': config.status,
            'connector_name': config.connector_name,
            'last_sync_at': config.last_sync_at.isoformat() if config.last_sync_at else None,
            'table_count': config.table_mappings.filter(is_enabled=True).count(),
        }
        
        # Get connector status if exists
        if config.connector_name:
            from client.utils.debezium_manager import DebeziumConnectorManager
            manager = DebeziumConnectorManager()
            
            exists, status_data = manager.get_connector_status(config.connector_name)
            
            if exists and status_data:
                data['connector_state'] = status_data.get('connector', {}).get('state', 'UNKNOWN')
                data['tasks'] = status_data.get('tasks', [])
        
        return JsonResponse(data)
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)




