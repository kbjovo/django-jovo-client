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
from client.forms import ClientForm

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
        databases = ClientDatabase.objects.filter(
            client=self.object
        ).prefetch_related('replication_configs')
        context['databases'] = databases
        return context


@require_http_methods(["POST"])
def client_soft_delete(request, pk):
    """
    Soft delete a client.

    This marks the client as deleted, drops its dedicated database,
    and removes all associated ClientDatabase entries.

    Args:
        request: HTTP request
        pk: Primary key of the client to delete

    Returns:
        Redirect to main dashboard
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