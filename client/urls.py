from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='main-dashboard'),

    path('clients/add/', views.ClientCreateView.as_view(), name='client_add'),
    path('clients/<int:pk>/', views.ClientDetailView.as_view(), name='client_detail'),

    path('clients/<int:pk>/update/', views.ClientUpdateView.as_view(), name='client_update'),
    path('clients/<int:pk>/delete/', views.client_soft_delete, name='client_soft_delete'),
    
    path('api/check-client-unique/', views.check_client_unique_field, name='check_client_unique_field'),
    
    
    
    # Database connection URLs
    path('clients/<int:client_pk>/databases/add/', 
         views.ClientDatabaseCreateView.as_view(), 
         name='client_database_add'),
    
    path('clients/databases/<int:pk>/update/', 
         views.ClientDatabaseUpdateView.as_view(), 
         name='client_database_update'),
    
    path('clients/databases/<int:pk>/delete/', 
         views.ClientDatabaseDeleteView.as_view(), 
         name='client_database_delete'),
    



    # ... your existing URLs ...
    
    # ==========================================
    # CDC Configuration URLs
    # ==========================================

    # Main CDC Dashboard (REMOVED - Direct to monitor/setup instead)
    # path(
    #     'clients/<int:client_pk>/cdc/dashboard/',
    #     views.cdc_dashboard,
    #     name='cdc_dashboard'
    # ),

    # Step 1: Discover Tables
    path(
        'database/<int:database_pk>/cdc/discover-tables/', 
        views.cdc_discover_tables, 
        name='cdc_discover_tables'
    ),
    
    # Step 2: Configure Tables
    path(
        'database/<int:database_pk>/cdc/configure-tables/', 
        views.cdc_configure_tables, 
        name='cdc_configure_tables'
    ),
    
    # Step 3: Create Connector
    path(
        'cdc/config/<int:config_pk>/create-connector/', 
        views.cdc_create_connector, 
        name='cdc_create_connector'
    ),
    
    # Step 4: Monitor Connector
    path(
        'cdc/config/<int:config_pk>/monitor/', 
        views.cdc_monitor_connector, 
        name='cdc_monitor_connector'
    ),
    
    # Connector Actions
    path(
        'cdc/config/<int:config_pk>/action/<str:action>/', 
        views.cdc_connector_action, 
        name='cdc_connector_action'
    ),
    
    # AJAX Endpoints
    path(
        'database/<int:database_pk>/table/<str:table_name>/schema/', 
        views.ajax_get_table_schema, 
        name='ajax_get_table_schema'
    ),


    path('cdc/config/<int:config_id>/start/', views.start_replication, name='start_replication'),
    path('cdc/config/<int:config_id>/stop/', views.stop_replication, name='stop_replication'),
    path('cdc/config/<int:config_id>/restart/', views.restart_replication_view, name='restart_replication'),
    path('cdc/config/<int:config_id>/status/', views.replication_status, name='replication_status'),

    # Edit & Delete Configuration
    path('cdc/config/<int:config_pk>/edit/', views.cdc_edit_config, name='cdc_edit_config'),
    path('cdc/config/<int:config_pk>/delete/', views.cdc_delete_config, name='cdc_delete_config'),

    # AJAX Endpoints for Modal Editing
    path('cdc/config/<int:config_pk>/details/', views.cdc_config_details, name='cdc_config_details'),
    path('cdc/config/<int:config_pk>/update/', views.cdc_config_update, name='cdc_config_update'),
    path('database/<int:database_pk>/table-schemas/', views.ajax_get_table_schemas_batch, name='ajax_get_table_schemas_batch'),

]




