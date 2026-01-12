from django.urls import path
from .views import (
    # Dashboard views
    dashboard,
    clients_list,
    replications_list,
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

    # CDC views
    cdc_discover_tables,
    cdc_configure_tables,
    cdc_create_connector,
    cdc_monitor_connector,
    cdc_connector_action,
    cdc_edit_config,
    cdc_delete_config,
    cdc_config_details,
    ajax_get_table_schema,
    ajax_get_table_schemas_batch,
    start_replication,
    stop_replication,
    restart_replication_view,
    replication_status,
    create_topics,
    list_topics,
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
    connector_delete_global,
)


urlpatterns = [
    # ==========================================
    # Dashboard & Main Pages
    # ==========================================
    path('', dashboard, name='main-dashboard'),
    path('clients/', clients_list, name='clients_list'),
    path('connectors/', connectors_list, name='connectors_list'),  # Global connectors view (replaces replications)
    path('connectors/<int:config_pk>/delete-global/', connector_delete_global, name='connector_delete_global'),  # Delete connector from global list
    path('replications/', replications_list, name='replications_list'),  # Keep for now, will remove later
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
    # CDC Configuration Workflow
    # ==========================================

    # Step 1: Discover Tables
    path('database/<int:database_pk>/cdc/discover-tables/',
         cdc_discover_tables,
         name='cdc_discover_tables'),

    # Step 2: Configure Tables
    path('database/<int:database_pk>/cdc/configure-tables/',
         cdc_configure_tables,
         name='cdc_configure_tables'),

    # Step 3: Create Connector
    path('cdc/config/<int:config_pk>/create-connector/',
         cdc_create_connector,
         name='cdc_create_connector'),

    # Step 4: Monitor Connector
    path('cdc/config/<int:config_pk>/monitor/',
         cdc_monitor_connector,
         name='cdc_monitor_connector'),

    # ==========================================
    # CDC Replication Control
    # ==========================================
    path('cdc/config/<int:config_id>/start/', start_replication, name='start_replication'),
    path('cdc/config/<int:config_id>/stop/', stop_replication, name='stop_replication'),
    path('cdc/config/<int:config_id>/restart/', restart_replication_view, name='restart_replication'),
    path('cdc/config/<int:config_id>/status/', replication_status, name='replication_status'),

    # Topic Management
    path('cdc/config/<int:config_id>/topics/create/', create_topics, name='create_topics'),
    path('cdc/config/<int:config_id>/topics/list/', list_topics, name='list_topics'),

    # Connector Actions
     path('cdc/config/<int:config_pk>/action/<str:action>/',
          cdc_connector_action,
          name='cdc_connector_action'),

    # ==========================================
    # CDC Configuration Management
    # ==========================================
    path('cdc/config/<int:config_pk>/edit/', cdc_edit_config, name='cdc_edit_config'),
    path('cdc/config/<int:config_pk>/delete/', cdc_delete_config, name='cdc_delete_config'),

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

    # ==========================================
    # AJAX Endpoints
    # ==========================================
    path('database/<int:database_pk>/table/<str:table_name>/schema/',
         ajax_get_table_schema,
         name='ajax_get_table_schema'),

    path('cdc/config/<int:config_pk>/details/', cdc_config_details, name='cdc_config_details'),
    path('database/<int:database_pk>/table-schemas/', ajax_get_table_schemas_batch, name='ajax_get_table_schemas_batch'),
]




