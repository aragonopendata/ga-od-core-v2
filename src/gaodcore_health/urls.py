"""
URL configuration for gaodcore_health app.
"""

from django.urls import path
from . import views

app_name = 'gaodcore_health'

urlpatterns = [
    # Dashboard
    path('dashboard/', views.health_dashboard, name='dashboard'),

    # Connector API endpoints
    path('api/status/', views.HealthStatusView.as_view(), name='api_status'),
    path('api/summary/', views.HealthSummaryView.as_view(), name='api_summary'),
    path('api/check/', views.HealthCheckView.as_view(), name='api_check'),
    path('api/history/', views.HealthHistoryView.as_view(), name='api_history'),
    path('api/connector/<int:connector_id>/detail/', views.ConnectorHealthDetailView.as_view(), name='api_connector_detail'),

    # Resource API endpoints
    path('api/resource/status/', views.ResourceHealthStatusView.as_view(), name='api_resource_status'),
    path('api/resource/summary/', views.ResourceHealthSummaryView.as_view(), name='api_resource_summary'),
    path('api/resource/check/', views.ResourceHealthCheckView.as_view(), name='api_resource_check'),
    path('api/resource/history/', views.ResourceHealthHistoryView.as_view(), name='api_resource_history'),
    path('api/resource/<int:resource_id>/detail/', views.ResourceHealthDetailView.as_view(), name='api_resource_detail'),
]
