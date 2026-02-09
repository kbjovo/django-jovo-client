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
from client.forms import ClientDatabaseForm
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