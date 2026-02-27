"""
Client CRUD operations and related views.

This module handles:
- Creating new clients
- Updating client information
- Viewing client details
- Soft deleting clients
- Validating unique fields
"""

from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views.generic import CreateView, UpdateView, DetailView
from django.urls import reverse_lazy
from django.views.decorators.http import require_http_methods
from django.contrib import messages
import logging

from client.models.client import Client
from client.forms import ClientForm, ClientDatabaseForm, SinkConnectorForm

logger = logging.getLogger(__name__)


class ClientCreateView(CreateView):
    """
    Create a new client.

    On successful creation:
    - Auto-creates a dedicated database for the client
    - Creates a ClientDatabase entry for the target database
    - Redirects to the main dashboard
    """
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
    """
    Update an existing client's information.

    Supports both full-page edits and inline HTMX edits.
    Handles status changes (active/inactive) appropriately.
    """
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

        # Handle status change from inactive to active
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
    """
    Display detailed information about a client.

    Shows:
    - Basic client information
    - Associated datasource connections
    - Connectors across all databases
    - Status and timestamps
    """
    model = Client
    template_name = 'client/client_detail.html'
    context_object_name = 'client'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Prefetch related replication_configs to avoid N+1 queries
        # and ensure the relationship data is available in the template
        from client.models.database import ClientDatabase
        from client.models.replication import ReplicationConfig
        from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

        databases = ClientDatabase.objects.filter(
            client=self.object
        ).prefetch_related('replication_configs')
        context['databases'] = databases
        context['form'] = ClientDatabaseForm(client=self.object)
        context['sink_form'] = SinkConnectorForm(client=self.object)

        # Get all connectors for this client
        all_connectors = ReplicationConfig.objects.filter(
            client_database__client=self.object,
            status__in=['configured', 'active', 'paused', 'error']
        ).select_related('client_database').prefetch_related('table_mappings').order_by('client_database__connection_name', 'connector_version')

        # Get Debezium status for each connector
        connector_manager = DebeziumConnectorManager()
        total_tables = 0
        running_count = 0

        for connector in all_connectors:
            try:
                exists, status_data = connector_manager.get_connector_status(connector.connector_name)
                if exists and status_data:
                    connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                    tasks = status_data.get('tasks', [])
                    has_failed_task = any(t.get('state') == 'FAILED' for t in tasks)
                    connector.debezium_status = {'state': connector_state, 'raw': status_data}
                    if connector_state in ('RUNNING', 'PAUSED') and not has_failed_task:
                        running_count += 1
                else:
                    connector.debezium_status = {'state': 'NOT_FOUND'}
            except Exception:
                connector.debezium_status = {'state': 'UNKNOWN'}

            # Get table count
            connector.table_count = connector.table_mappings.filter(is_enabled=True).count()
            total_tables += connector.table_count

        context['all_connectors'] = all_connectors

        # Calculate connector stats
        total_count = all_connectors.count()
        if total_count == 0:
            health_status = 'none'
        elif running_count == total_count:
            health_status = 'healthy'
        elif running_count > 0:
            health_status = 'partial'
        else:
            health_status = 'degraded'

        context['connector_stats'] = {
            'total': total_count,
            'running': running_count,
            'total_tables': total_tables,
            'health_status': health_status,
        }

        return context


@require_http_methods(["POST"])
def client_soft_delete(request, pk):
    """
    Soft delete a client.

    Cleans up all Kafka Connect source connectors and the shared sink connector
    before marking the client as deleted and removing all ClientDatabase rows.
    """
    from client.models.database import ClientDatabase
    from client.models.replication import ReplicationConfig
    from client.replication.orchestrator import ReplicationOrchestrator
    from jovoclient.utils.debezium.connector_manager import DebeziumConnectorManager

    client = get_object_or_404(Client, pk=pk)

    # Block deletion if any source connectors still exist across all databases.
    remaining = ReplicationConfig.objects.filter(
        client_database__client=client
    ).count()
    if remaining:
        messages.error(
            request,
            f"Cannot delete client: {remaining} source connector"
            f"{'s' if remaining != 1 else ''} still exist. "
            "Delete all source connectors first."
        )
        return redirect('client_detail', pk=client.pk)

    # Clean up source connectors for every source database.
    # Must be done before the bulk CASCADE delete that soft_delete() triggers,
    # because Django's CASCADE skips custom delete() methods.
    for database in client.client_databases.filter(is_target=False):
        for config in database.replication_configs.all():
            try:
                orchestrator = ReplicationOrchestrator(config)
                orchestrator.delete_replication(delete_topics=False, drop_tables=False)
            except Exception as e:
                logger.warning(f"Failed to clean up connector {config.id} for client {pk}: {e}")

    # Clean up the shared sink connector from Kafka Connect.
    sink_db = client.client_databases.filter(is_target=True).first()
    if sink_db:
        try:
            manager = DebeziumConnectorManager()
            sink_name = sink_db.get_sink_connector_name()
            exists, _ = manager.get_connector_status(sink_name)
            if exists:
                manager.delete_connector(sink_name)
        except Exception as e:
            logger.warning(f"Failed to delete sink connector for client {pk}: {e}")

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
    Check if a field value is unique across clients.

    Used for real-time validation in forms (email, phone, db_name).

    Query Parameters:
        field: Field name to check (email, phone, db_name)
        value: Value to check for uniqueness
        client_id: (Optional) Client ID to exclude when editing

    Returns:
        JSON response with is_unique flag and optional error message
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