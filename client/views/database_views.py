"""
ClientDatabase CRUD operations and connection testing.

This module handles:
- Creating database connections for clients
- Updating database connection settings
- Testing database connectivity
- Deleting database connections
"""

from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.generic import CreateView, UpdateView, View
from django.views.decorators.http import require_http_methods
import logging

from client.models.client import Client
from client.models.database import ClientDatabase
from client.forms import ClientDatabaseForm, SinkConnectorForm
from client.replication.orchestrator import ReplicationOrchestrator

logger = logging.getLogger(__name__)


class ClientDatabaseCreateView(CreateView):
    """
    Create a new database connection for a client.

    Supports:
    - Connection testing before saving (via test_only POST parameter)
    - Automatic status checking after creation
    - HTMX partial rendering for seamless UX
    """
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
        form.instance.is_target = False  # Source connections are never targets
        self.object = form.save()

        # Test connection and save status
        try:
            self.object.check_connection_status(save=True)
        except Exception as e:
            logger.warning(f"Error checking connection status after save: {e}")
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
    """
    Update an existing database connection.

    Features:
    - Password field is optional (preserves existing if left empty)
    - Connection testing before updating
    - Automatic status checking after update
    """
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

        # Check if this is a test-only request
        if request.POST.get('test_only') == 'true':
            # Test using the existing database object
            try:
                status = self.object.check_connection_status(save=False)
                if status == 'success':
                    return JsonResponse({
                        'success': True,
                        'message': '✓ Connection test successful!'
                    })
                else:
                    error_message = getattr(self.object, 'last_error', 'Connection failed')
                    return JsonResponse({
                        'success': False,
                        'message': f'✗ Connection test failed: {error_message}'
                    })
            except Exception as e:
                logger.error(f"Error testing connection: {e}")
                return JsonResponse({
                    'success': False,
                    'message': f'✗ Connection test error: {str(e)}'
                })

        # For normal updates, get and validate the form
        form = self.get_form()

        # Check if this is a test-only request with form data (from form submission)
        if request.POST.get('test_only') == 'true' and len(request.POST) > 2:
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
    """
    Delete a database connection and clean up all associated CDC resources.

    Cleanup includes:
    - Deleting Debezium source connectors from Kafka Connect
    - Updating/removing shared sink connectors
    - Marking ConnectorHistory as deleted
    - Optionally deleting Kafka topics (if requested via form checkbox)
    - Removing the ClientDatabase row (CASCADE deletes ReplicationConfig/TableMapping/ColumnMapping)
    """

    def post(self, request, pk):
        database = get_object_or_404(ClientDatabase, pk=pk)
        client = database.client
        delete_topics = request.POST.get('delete_topics') == '1'
        drop_tables = request.POST.get('drop_tables') == '1'

        # Clean up all replication configs BEFORE cascade delete.
        # Django's CASCADE uses bulk SQL and does NOT call ReplicationConfig.delete(),
        # so we must explicitly clean up connectors in Kafka Connect via the orchestrator.
        for config in database.replication_configs.all():
            try:
                orchestrator = ReplicationOrchestrator(config)
                orchestrator.delete_replication(delete_topics=delete_topics, drop_tables=drop_tables)
            except Exception as e:
                logger.warning(f"Failed to clean up replication config {config.id}: {e}")

        # Delete the database (CASCADE cleans up any remaining DB rows)
        database.delete()

        if request.headers.get('HX-Request'):
            databases = ClientDatabase.objects.filter(client=client)
            return render(
                request,
                'client/partials/databases_list.html',
                {'client': client, 'databases': databases}
            )

        return redirect('client_detail', pk=client.pk)


# ==========================================
# Sink Connector (Target Database) Views
# ==========================================

def _render_databases_list(request, client):
    """Helper to render the databases list partial."""
    databases = ClientDatabase.objects.filter(client=client)
    return render(request, 'client/partials/databases_list.html', {
        'client': client, 'databases': databases
    })


def _test_sink_connection(form, client, existing_password=None):
    """Helper to test sink connector connection from form data."""
    temp_instance = form.save(commit=False)
    temp_instance.client = client
    temp_instance.is_target = True

    if existing_password and not form.cleaned_data.get('password'):
        temp_instance.password = existing_password

    try:
        status = temp_instance.check_connection_status(save=False)
        if status == 'success':
            return JsonResponse({
                'success': True,
                'message': 'Connection test successful! The database is reachable and credentials are valid.'
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Connection test failed. Please verify host, port, username, and password.'
            })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'message': f'Connection test failed: {str(e)}'
        })


class SinkConnectorCreateView(CreateView):
    """Create the sink connector (target database) for a client."""
    model = ClientDatabase
    form_class = SinkConnectorForm
    template_name = 'client/partials/sink_connector_form.html'

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

        # Check if client already has a target database
        if ClientDatabase.objects.filter(client=self.client, is_target=True).exists():
            return JsonResponse({
                'success': False,
                'message': 'This client already has a sink connector configured.'
            }, status=400)

        form = self.get_form()

        if request.POST.get('test_only') == 'true':
            if form.is_valid():
                return _test_sink_connection(form, self.client)
            else:
                errors = [f"{f}: {e}" for f, el in form.errors.items() for e in el]
                return JsonResponse({
                    'success': False,
                    'message': f'Please fix form errors: {", ".join(errors)}'
                })

        if form.is_valid():
            return self.form_valid(form)
        return self.form_invalid(form)

    def form_valid(self, form):
        instance = form.save(commit=False)
        instance.client = self.client
        instance.is_target = True
        instance.is_primary = False
        instance.save()

        try:
            instance.check_connection_status(save=True)
        except Exception as e:
            logger.warning(f"Error checking sink connection after save: {e}")

        if self.request.headers.get('HX-Request'):
            return _render_databases_list(self.request, self.client)

        return redirect('client_detail', pk=self.client.pk)


class SinkConnectorUpdateView(UpdateView):
    """
    Update the sink connector (target database).

    Handles 3 scenarios:
    1. Credentials only (host/port/user/pass): Update record + PATCH Kafka Connect
    2. DB name change: Update record + delete old sink + redeploy
    3. DB type change: Update record + delete old sink + redeploy
    """
    model = ClientDatabase
    form_class = SinkConnectorForm
    template_name = 'client/partials/sink_connector_form.html'

    def get_queryset(self):
        return ClientDatabase.objects.filter(is_target=True)

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

        if request.POST.get('test_only') == 'true':
            form = self.get_form()
            if form.is_valid():
                return _test_sink_connection(
                    form, self.object.client,
                    existing_password=self.object.password
                )
            else:
                errors = [f"{f}: {e}" for f, el in form.errors.items() for e in el]
                return JsonResponse({
                    'success': False,
                    'message': f'Please fix form errors: {", ".join(errors)}'
                })

        form = self.get_form()
        if form.is_valid():
            return self.form_valid(form)
        return self.form_invalid(form)

    def form_valid(self, form):
        old_db_type = self.object.db_type
        old_database_name = self.object.database_name
        old_host = self.object.host
        old_port = self.object.port

        # Handle password: if empty, keep existing
        if not form.cleaned_data.get('password'):
            old_password = self.object.password
            self.object = form.save(commit=False)
            self.object.password = old_password
            self.object.save()
        else:
            self.object = form.save()

        # Test connection
        try:
            self.object.check_connection_status(save=True)
        except Exception:
            pass

        # Determine what changed and handle Kafka Connect accordingly
        db_type_changed = old_db_type != self.object.db_type
        db_name_changed = old_database_name != self.object.database_name
        host_changed = old_host != self.object.host
        port_changed = old_port != self.object.port

        is_destructive = db_type_changed or db_name_changed

        try:
            self._handle_kafka_connect_update(is_destructive, host_changed or port_changed)
        except Exception as e:
            logger.warning(f"Error updating sink in Kafka Connect: {e}")

        if self.request.headers.get('HX-Request'):
            return _render_databases_list(self.request, self.object.client)

        return redirect('client_detail', pk=self.object.client.pk)

    def _handle_kafka_connect_update(self, is_destructive, credentials_changed):
        """Update the deployed Kafka Connect sink connector if it exists."""
        from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager
        from jovoclient.utils.debezium.sink_connector_templates import get_sink_connector_config_for_database

        sink_name = self.object.get_sink_connector_name()
        manager = DebeziumConnectorManager()

        exists, _ = manager.get_connector_status(sink_name)
        if not exists:
            return  # Sink not deployed yet, nothing to do

        if is_destructive:
            # Delete old sink and redeploy with new config
            logger.info(f"Destructive sink change detected — recreating sink connector: {sink_name}")
            try:
                manager.delete_connector(sink_name)
            except Exception as e:
                logger.warning(f"Error deleting old sink connector: {e}")

            sink_config = get_sink_connector_config_for_database(
                db_config=self.object,
                delete_enabled=True,
                custom_config={'name': sink_name}
            )
            if sink_config:
                manager.create_connector(sink_name, sink_config)
        elif credentials_changed:
            # Just update the config (PATCH)
            logger.info(f"Credentials-only sink change — updating config: {sink_name}")
            sink_config = get_sink_connector_config_for_database(
                db_config=self.object,
                delete_enabled=True,
                custom_config={'name': sink_name}
            )
            if sink_config:
                manager.update_connector_config(sink_name, sink_config)


class SinkConnectorDeleteView(View):
    """Delete the sink connector (target database) and clean up Kafka Connect."""

    def post(self, request, pk):
        database = get_object_or_404(ClientDatabase, pk=pk, is_target=True)
        client = database.client

        # Check if there are active source connectors — warn but allow
        from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

        sink_name = database.get_sink_connector_name()
        manager = DebeziumConnectorManager()

        # Delete the deployed sink connector from Kafka Connect
        try:
            exists, _ = manager.get_connector_status(sink_name)
            if exists:
                manager.delete_connector(sink_name)
                logger.info(f"Deleted sink connector from Kafka Connect: {sink_name}")
        except Exception as e:
            logger.warning(f"Error deleting sink connector from Kafka Connect: {e}")

        database.delete()

        if request.headers.get('HX-Request'):
            return _render_databases_list(request, client)

        return redirect('client_detail', pk=client.pk)