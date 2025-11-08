from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpResponse
from client.models.client import Client
from client.models.database import ClientDatabase
from .forms import ClientForm, ClientDatabaseForm
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView, View
from django.urls import reverse_lazy, reverse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.decorators.http import require_http_methods
from django.core.paginator import Paginator
from django.contrib import messages
from django.db.models import Q
import logging
from jovoclient.utils.table_utils import build_paginated_table
from sqlalchemy import text
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
    Main dashboard view with automatic client table.
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
    



"""
Add these views to your client/views.py
These handle CDC configuration workflow
"""

from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db import transaction

from client.models.database import ClientDatabase
from client.models.replication import ReplicationConfig, TableMapping, ColumnMapping
from client.utils.database_utils import (
    get_table_list, 
    get_table_schema,
    get_database_engine,
    check_binary_logging
)
from client.utils.debezium_manager import DebeziumConnectorManager
from client.utils.connector_templates import (
    get_connector_config_for_database,
    generate_connector_name
)


# ============================================
# 1. CDC Dashboard - Show all configurations
# ============================================
@require_http_methods(["GET"])
def cdc_dashboard(request, client_pk):
    """
    Main CDC dashboard showing all replication configurations for a client
    """
    client = get_object_or_404(Client, pk=client_pk)
    databases = ClientDatabase.objects.filter(client=client)
    
    # Get all replication configs with statistics
    replication_configs = ReplicationConfig.objects.filter(
        client_database__client=client
    ).select_related('client_database')
    
    context = {
        'client': client,
        'databases': databases,
        'replication_configs': replication_configs,
    }
    
    return render(request, 'client/cdc/dashboard.html', context)


# ============================================
# 2. Discover Tables - Step 1
# ============================================
@require_http_methods(["GET", "POST"])
def cdc_discover_tables(request, database_pk):
    """
    Discover all tables from source database
    Returns list of tables with metadata
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
        
        # Get table details (row count, size estimates)
        tables_with_info = []
        engine = get_database_engine(db_config)
        
        with engine.connect() as conn:
            for table_name in tables:
                try:
                    # Get row count
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    row_count = result.scalar() or 0
                    
                    # Check if table has timestamp column
                    schema = get_table_schema(db_config, table_name)
                    columns = schema.get('columns', [])
                    has_timestamp = any(
                        'timestamp' in col.get('type', '').lower() or 
                        'datetime' in col.get('type', '').lower() or
                        col.get('name', '').endswith('_at')
                        for col in columns
                    )
                    
                    tables_with_info.append({
                        'name': table_name,
                        'row_count': row_count,
                        'column_count': len(columns),
                        'has_timestamp': has_timestamp,
                        'primary_keys': schema.get('primary_keys', []),
                    })
                except Exception as e:
                    logger.error(f"Error getting info for table {table_name}: {e}")
                    tables_with_info.append({
                        'name': table_name,
                        'row_count': 0,
                        'column_count': 0,
                        'has_timestamp': False,
                        'primary_keys': [],
                    })
        
        engine.dispose()
        
        context = {
            'db_config': db_config,
            'client': db_config.client,
            'tables': tables_with_info,
        }
        
        return render(request, 'client/cdc/discover_tables.html', context)
        
    except Exception as e:
        messages.error(request, f'Failed to discover tables: {str(e)}')
        return redirect('client_detail', pk=db_config.client.pk)


# ============================================
# 3. Configure Tables - Step 2
# ============================================
@require_http_methods(["GET", "POST"])
def cdc_configure_tables(request, database_pk):
    """
    Configure selected tables: 
    - Choose sync type (full/incremental/realtime)
    - Select incremental column
    - Configure column mappings
    """
    db_config = get_object_or_404(ClientDatabase, pk=database_pk)
    
    # Get selected tables from session
    selected_tables = request.session.get('selected_tables', [])
    if not selected_tables:
        messages.error(request, 'No tables selected')
        return redirect('cdc_discover_tables', database_pk=database_pk)
    
    if request.method == "POST":
        # Save configuration and create replication config
        try:
            with transaction.atomic():
                # Create ReplicationConfig
                replication_config = ReplicationConfig.objects.create(
                    client_database=db_config,
                    sync_type=request.POST.get('sync_type', 'realtime'),
                    sync_frequency=request.POST.get('sync_frequency', 'realtime'),
                    status='active',
                    is_active=True,
                    auto_create_tables=True,
                    created_by=request.user if request.user.is_authenticated else None
                )
                
                # Create TableMappings
                for table_name in selected_tables:
                    sync_type = request.POST.get(f'sync_type_{table_name}', '')
                    incremental_col = request.POST.get(f'incremental_col_{table_name}', '')
                    
                    TableMapping.objects.create(
                        replication_config=replication_config,
                        source_table=table_name,
                        target_table=table_name,  # Same name in target
                        is_enabled=True,
                        sync_type=sync_type,
                        incremental_column=incremental_col if sync_type == 'incremental' else None,
                        incremental_column_type='timestamp' if incremental_col else '',
                    )
                
                messages.success(request, 'CDC configuration saved successfully!')
                
                # Clear session
                request.session.pop('selected_tables', None)
                request.session.pop('database_pk', None)
                
                return redirect('cdc_create_connector', config_pk=replication_config.pk)
                
        except Exception as e:
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
                if 'timestamp' in col.get('type', '').lower() or
                   'datetime' in col.get('type', '').lower() or
                   col.get('name', '').endswith('_at') or
                   (col.get('name', '') == 'id' and 'int' in col.get('type', '').lower())
            ]
            
            tables_with_columns.append({
                'name': table_name,
                'columns': columns,
                'primary_keys': schema.get('primary_keys', []),
                'incremental_candidates': incremental_candidates,
            })
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
    
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
        return redirect('cdc_dashboard', client_pk=replication_config.client_database.client.pk)
    
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
        return redirect('cdc_dashboard', client_pk=replication_config.client_database.client.pk)


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