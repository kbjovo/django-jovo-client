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

from jovoclient.utils.table_utils import build_paginated_table


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