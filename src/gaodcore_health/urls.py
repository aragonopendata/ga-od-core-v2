"""
URL configuration for gaodcore_health app.
"""

from django.urls import path
from . import views

app_name = 'gaodcore_health'

urlpatterns = [
    # Dashboard
    path('dashboard/', views.health_dashboard, name='dashboard'),

    # API endpoints
    path('api/status/', views.HealthStatusView.as_view(), name='api_status'),
    path('api/summary/', views.HealthSummaryView.as_view(), name='api_summary'),
    path('api/check/', views.HealthCheckView.as_view(), name='api_check'),
    path('api/history/', views.HealthHistoryView.as_view(), name='api_history'),
    path('api/connector/<int:connector_id>/detail/', views.ConnectorHealthDetailView.as_view(), name='api_connector_detail'),
]
