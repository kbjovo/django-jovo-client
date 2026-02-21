from django.urls import path
from .views import (
    # Dashboard views
    dashboard,
    clients_list,
    monitoring_dashboard,

    # Client views
    ClientCreateView,
    ClientDetailView,
    ClientUpdateView,
    client_soft_delete,
    check_client_unique_field,

    # Database views
    ClientDatabaseCreateView,
    ClientDatabaseUpdateView,
    ClientDatabaseDeleteView,

    # Sink connector views
    SinkConnectorCreateView,
    SinkConnectorUpdateView,
    SinkConnectorDeleteView,
)

# Import multi-connector management views
from .views.connector_views import (
    ajax_get_table_schema,
    connectors_list,
    client_connectors_list,
    connector_list,
    connector_add,
    connector_create_debezium,
    connector_edit_tables,
    connector_delete,
    connector_recreate,
    connector_monitor,
    connector_status_api,
    connector_pause,
    connector_resume,
    connector_restart,
    connector_restart_failed_tasks,
    connector_restart_all_tasks,
    connector_sync_schedule,
    sink_restart,
    sink_restart_failed_tasks,
    sink_restart_all_tasks,
    connector_table_rows_api,
    connector_live_metrics_api,
    connector_fk_preview_api,
    connector_fk_apply_api,
)


urlpatterns = [
    # ==========================================
    # Dashboard & Main Pages
    # ==========================================
    path('', dashboard, name='main-dashboard'),
    path('clients/', clients_list, name='clients_list'),
    path('connectors/', connectors_list, name='connectors_list'),  # Global connectors view (replaces replications)
    path('monitoring/', monitoring_dashboard, name='monitoring_dashboard'),

    # ==========================================
    # Client Management
    # ==========================================
    path('clients/add/', ClientCreateView.as_view(), name='client_add'),
    path('clients/<int:pk>/', ClientDetailView.as_view(), name='client_detail'),
    path('clients/<int:pk>/update/', ClientUpdateView.as_view(), name='client_update'),
    path('clients/<int:pk>/delete/', client_soft_delete, name='client_soft_delete'),

    # Client-level connectors view (all connectors across all databases)
    path('clients/<int:client_pk>/connectors/', client_connectors_list, name='client_connectors_list'),

    path('api/check-client-unique/', check_client_unique_field, name='check_client_unique_field'),


    # ==========================================
    # Database Connection Management
    # ==========================================
    path('clients/<int:client_pk>/databases/add/',
         ClientDatabaseCreateView.as_view(),
         name='client_database_add'),

    path('clients/databases/<int:pk>/update/',
         ClientDatabaseUpdateView.as_view(),
         name='client_database_update'),

    path('clients/databases/<int:pk>/delete/',
         ClientDatabaseDeleteView.as_view(),
         name='client_database_delete'),

    # ==========================================
    # Sink Connector (Target Database) Management
    # ==========================================
    path('clients/<int:client_pk>/sink/add/',
         SinkConnectorCreateView.as_view(),
         name='sink_connector_add'),

    path('clients/sink/<int:pk>/update/',
         SinkConnectorUpdateView.as_view(),
         name='sink_connector_update'),

    path('clients/sink/<int:pk>/delete/',
         SinkConnectorDeleteView.as_view(),
         name='sink_connector_delete'),


    # ==========================================
    # Multi-Source Connector Management (NEW)
    # ==========================================
    # List all connectors for a database
    path('database/<int:database_pk>/connectors/',
         connector_list,
         name='connector_list'),

    # Add new source connector
    path('database/<int:database_pk>/connectors/add/',
         connector_add,
         name='connector_add'),

    # AJAX: Get table schema for accordion UI
    path('ajax/table-schema/<int:database_pk>/<str:table_name>/',
         ajax_get_table_schema,
         name='ajax_get_table_schema'),

    # Create Debezium connectors (source + sink)
    path('connector/<int:config_pk>/create-debezium/',
         connector_create_debezium,
         name='connector_create_debezium'),

    # Edit tables in connector (add/remove with signals)
    path('connector/<int:config_pk>/edit-tables/',
         connector_edit_tables,
         name='connector_edit_tables'),

    # Delete connector with validations
    path('connector/<int:config_pk>/delete/',
         connector_delete,
         name='connector_delete'),

    # Recreate source connector (same version, same topics)
    path('connector/<int:config_pk>/recreate/',
         connector_recreate,
         name='connector_recreate'),

    # Monitor connector (real-time status + snapshot progress)
    path('connector/<int:config_pk>/monitor/',
         connector_monitor,
         name='connector_monitor'),

    # AJAX: Connector status JSON (polled by monitor page)
    path('connector/<int:config_pk>/status/',
         connector_status_api,
         name='connector_status_api'),

    # Connector action endpoints (AJAX)
    path('connector/<int:config_pk>/pause/',
         connector_pause,
         name='connector_pause'),

    path('connector/<int:config_pk>/resume/',
         connector_resume,
         name='connector_resume'),

    path('connector/<int:config_pk>/restart/',
         connector_restart,
         name='connector_restart'),

    path('connector/<int:config_pk>/restart-failed-tasks/',
         connector_restart_failed_tasks,
         name='connector_restart_failed_tasks'),

    path('connector/<int:config_pk>/restart-all-tasks/',
         connector_restart_all_tasks,
         name='connector_restart_all_tasks'),

    path('connector/<int:config_pk>/sync-schedule/',
         connector_sync_schedule,
         name='connector_sync_schedule'),

    # Sink connector action endpoints (AJAX)
    path('connector/<int:config_pk>/sink/restart/',
         sink_restart,
         name='sink_restart'),

    path('connector/<int:config_pk>/sink/restart-failed-tasks/',
         sink_restart_failed_tasks,
         name='sink_restart_failed_tasks'),

    path('connector/<int:config_pk>/sink/restart-all-tasks/',
         sink_restart_all_tasks,
         name='sink_restart_all_tasks'),

    # AJAX: Table row counts (source + target)
    path('connector/<int:config_pk>/table-rows/',
         connector_table_rows_api,
         name='connector_table_rows_api'),

    # AJAX: Live metrics (streaming lag, DLQ count, config diff)
    path('connector/<int:config_pk>/live-metrics/',
         connector_live_metrics_api,
         name='connector_live_metrics_api'),

    # AJAX: FK constraint preview (read-only analysis)
    path('connector/<int:config_pk>/fk-preview/',
         connector_fk_preview_api,
         name='connector_fk_preview_api'),

    # AJAX: FK constraint apply (POST to create constraints)
    path('connector/<int:config_pk>/fk-apply/',
         connector_fk_apply_api,
         name='connector_fk_apply_api'),

]




