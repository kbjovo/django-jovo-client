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


def _check_mysql_replication(db_instance) -> dict:
    """
    Replication readiness check for a MySQL source.
    Returns dict with: log_bin, gtid_mode, has_replication_slave, error,
                       privileges [{name, granted, note}], all_granted
    """
    result = {
        'log_bin': False, 'gtid_mode': False, 'has_replication_slave': False,
        'error': '', 'privileges': [], 'all_granted': False,
    }
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_instance.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            row = conn.execute(text("SHOW VARIABLES LIKE 'log_bin'")).fetchone()
            result['log_bin'] = bool(row and row[1].upper() == 'ON')

            row = conn.execute(text("SHOW VARIABLES LIKE 'gtid_mode'")).fetchone()
            result['gtid_mode'] = bool(row and row[1].upper() == 'ON')

            grants = conn.execute(text("SHOW GRANTS FOR CURRENT_USER()")).fetchall()
            grants_text = ' '.join(str(g[0]).upper() for g in grants)
            has_all = 'ALL PRIVILEGES' in grants_text
            result['has_replication_slave'] = has_all or 'REPLICATION SLAVE' in grants_text
            has_replication_client = has_all or 'REPLICATION CLIENT' in grants_text or 'SUPER' in grants_text
            has_select = has_all or 'SELECT' in grants_text

        engine.dispose()

        result['privileges'] = [
            {'name': 'Binary logging (log_bin)', 'granted': result['log_bin'],
             'note': 'Required for CDC to capture row changes'},
            {'name': 'GTID mode', 'granted': result['gtid_mode'],
             'note': 'Required for reliable replication position tracking'},
            {'name': 'REPLICATION SLAVE', 'granted': result['has_replication_slave'],
             'note': 'Required to stream binary log events'},
            {'name': 'REPLICATION CLIENT', 'granted': has_replication_client,
             'note': 'Required to read current binlog position'},
            {'name': 'SELECT', 'granted': has_select,
             'note': 'Required for initial snapshot of table data'},
        ]
        result['all_granted'] = all(p['granted'] for p in result['privileges'])
    except Exception as e:
        result['error'] = str(e)
    return result


def _check_postgresql_replication(db_instance) -> dict:
    """
    Replication readiness check for a PostgreSQL source.
    Returns dict with: has_replication, error,
                       privileges [{name, granted, note}], all_granted
    """
    result = {
        'has_replication': False, 'error': '',
        'privileges': [], 'all_granted': False,
    }
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_instance.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user"
            )).fetchone()
            has_replication = bool(row[0]) if row else False
            is_super = bool(row[1]) if row else False
            result['has_replication'] = has_replication

            row = conn.execute(text(
                "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
            )).fetchone()
            can_create_pub = is_super or (bool(row[0]) if row else False)

        engine.dispose()

        result['privileges'] = [
            {'name': 'REPLICATION role', 'granted': has_replication or is_super,
             'note': 'Required for logical replication (CDC)'},
            {'name': 'CREATE on database', 'granted': can_create_pub,
             'note': 'Required to create publications (PostgreSQL 15+)'},
        ]
        result['all_granted'] = all(p['granted'] for p in result['privileges'])
    except Exception as e:
        result['error'] = str(e)
    return result


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
                    status = temp_instance.check_connection_status(save=False)
                    if status == 'success':
                        replication = None
                        db_type = (temp_instance.db_type or '').lower()
                        if db_type == 'mysql':
                            replication = _check_mysql_replication(temp_instance)
                        elif db_type in ('postgresql', 'postgres'):
                            replication = _check_postgresql_replication(temp_instance)
                        return JsonResponse({
                            'success': True,
                            'message': '✓ Connection test successful! The database is reachable and credentials are valid.',
                            'replication': replication,
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'message': '✗ Connection test failed. Unable to connect to the database. Please verify the host, port, username, and password are correct.'
                        })
                except Exception as e:
                    error_message = str(e)
                    return JsonResponse({
                        'success': False,
                        'message': f'✗ Connection test failed: {error_message}'
                    })
            else:
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

        if request.POST.get('test_only') == 'true':
            # Build a temp instance from the saved object and apply any POST overrides.
            # No form validation — we just want to test the connection.
            import copy
            temp = copy.copy(self.object)
            for field in ('host', 'database_name', 'username', 'db_type', 'oracle_connection_mode'):
                val = request.POST.get(field, '').strip()
                if val:
                    setattr(temp, field, val)
            try:
                temp.port = int(request.POST.get('port') or self.object.port)
            except (ValueError, TypeError):
                pass
            # Use submitted password only if the user typed a new one; otherwise keep encrypted stored value
            submitted_password = request.POST.get('password', '').strip()
            if submitted_password:
                temp.password = submitted_password  # decrypt_password() has plaintext fallback
            try:
                status = temp.check_connection_status(save=False)
                if status == 'success':
                    replication = None
                    db_type = (temp.db_type or '').lower()
                    if db_type == 'mysql':
                        replication = _check_mysql_replication(temp)
                    elif db_type in ('postgresql', 'postgres'):
                        replication = _check_postgresql_replication(temp)
                    return JsonResponse({
                        'success': True,
                        'message': '✓ Connection test successful! The database is reachable and credentials are valid.',
                        'replication': replication,
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'message': '✗ Connection test failed. Please verify host, port, username, and password.'
                    })
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'message': f'✗ Connection test failed: {str(e)}'
                })

        form = self.get_form()

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
    - Removing the ClientDatabase row (CASCADE deletes ReplicationConfig/TableMapping)
    """

    def post(self, request, pk):
        database = get_object_or_404(ClientDatabase, pk=pk)
        client = database.client

        # Block deletion if any source connectors still exist.
        remaining = database.replication_configs.count()
        if remaining:
            error_msg = (
                f"Cannot delete: {remaining} source connector"
                f"{'s' if remaining != 1 else ''} still exist. "
                "Delete all source connectors first."
            )
            if request.headers.get('HX-Request'):
                return JsonResponse({'error': error_msg}, status=400)
            return redirect('client_detail', pk=client.pk)

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


def _check_sink_privileges(db_instance) -> dict:
    """
    Check write privileges required for the sink connector user.

    MySQL  — checks: CREATE TABLE, ALTER TABLE, INSERT, UPDATE, DELETE
    PostgreSQL — checks: CREATE on schema 'public', INSERT, UPDATE, DELETE

    Returns dict with:
        privileges  list[{name, granted, note}]  — one entry per required privilege
        all_granted bool
        error       str   — non-empty if the check itself failed
    """
    result = {'privileges': [], 'all_granted': False, 'error': ''}
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_instance.get_connection_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            db_type = (db_instance.db_type or '').lower()

            if db_type == 'mysql':
                grants = conn.execute(text("SHOW GRANTS FOR CURRENT_USER()")).fetchall()
                grants_text = ' '.join(str(g[0]).upper() for g in grants)
                has_all = 'ALL PRIVILEGES' in grants_text

                checks = [
                    ('CREATE TABLE',  'CREATE',  'Required to auto-create target tables'),
                    ('ALTER TABLE',   'ALTER',   'Required for schema evolution (DDL sync)'),
                    ('INSERT',        'INSERT',  'Required to write replicated rows'),
                    ('UPDATE',        'UPDATE',  'Required to apply row updates'),
                    ('DELETE',        'DELETE',  'Required to apply row deletes'),
                ]
                for label, keyword, note in checks:
                    granted = has_all or keyword in grants_text
                    result['privileges'].append({'name': label, 'granted': granted, 'note': note})

            elif db_type == 'postgresql':
                is_super = bool(conn.execute(text(
                    "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
                )).scalar())

                has_schema_create = is_super or bool(conn.execute(text(
                    "SELECT has_schema_privilege(current_user, 'public', 'CREATE')"
                )).scalar())

                priv_rows = conn.execute(text("""
                    SELECT UPPER(privilege_type)
                    FROM information_schema.role_table_grants
                    WHERE grantee = current_user
                      AND privilege_type IN ('INSERT','UPDATE','DELETE')
                    GROUP BY privilege_type
                """)).fetchall()
                granted_table_privs = {r[0] for r in priv_rows}

                checks = [
                    ('CREATE on schema', has_schema_create,         'Required to auto-create target tables'),
                    ('INSERT',          is_super or 'INSERT' in granted_table_privs, 'Required to write replicated rows'),
                    ('UPDATE',          is_super or 'UPDATE' in granted_table_privs, 'Required to apply row updates'),
                    ('DELETE',          is_super or 'DELETE' in granted_table_privs, 'Required to apply row deletes'),
                ]
                for label, granted, note in checks:
                    result['privileges'].append({'name': label, 'granted': granted, 'note': note})

        engine.dispose()
        result['all_granted'] = all(p['granted'] for p in result['privileges'])
    except Exception as e:
        result['error'] = str(e)
    return result


def _test_sink_connection(instance):
    """Helper to test sink connector connection from a pre-built (unsaved) instance."""
    temp_instance = instance

    try:
        status = temp_instance.check_connection_status(save=False)
        if status == 'success':
            privs = _check_sink_privileges(temp_instance)
            return JsonResponse({
                'success': True,
                'message': 'Connection test successful! The database is reachable and credentials are valid.',
                'privileges': privs['privileges'],
                'all_privileges_granted': privs['all_granted'],
                'privilege_check_error': privs['error'],
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Connection test failed. Please verify host, port, username, and password.',
                'privileges': [],
                'all_privileges_granted': False,
                'privilege_check_error': '',
            })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'message': f'Connection test failed: {str(e)}',
            'privileges': [],
            'all_privileges_granted': False,
            'privilege_check_error': '',
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
                temp = form.save(commit=False)
                temp.client = self.client
                temp.is_target = True
                return _test_sink_connection(temp)
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
            import copy
            temp = copy.copy(self.object)
            for field in ('host', 'database_name', 'username', 'db_type'):
                val = request.POST.get(field, '').strip()
                if val:
                    setattr(temp, field, val)
            try:
                temp.port = int(request.POST.get('port') or self.object.port)
            except (ValueError, TypeError):
                pass
            submitted_password = request.POST.get('password', '').strip()
            if submitted_password:
                temp.password = submitted_password
            return _test_sink_connection(temp)

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