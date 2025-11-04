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
    
]



