"""
URL configuration for gaodcore_health app.
"""

from django.urls import path
from . import views

app_name = "gaodcore_health"

urlpatterns = [
    # Main health views
    path("", views.health_index, name="health_index"),
    path("connectors/", views.ConnectorHealthListView.as_view(), name="connector_list"),
    path("resources/", views.ResourceHealthListView.as_view(), name="resource_list"),
    path(
        "connectors/<int:connector_id>/",
        views.ConnectorHealthDetailView.as_view(),
        name="connector_detail",
    ),
    path(
        "resources/<int:resource_id>/",
        views.ResourceHealthDetailView.as_view(),
        name="resource_detail",
    ),
    path(
        "connectors/<int:connector_id>/resources/",
        views.ConnectorResourceListView.as_view(),
        name="connector_resources",
    ),
    # Legacy dashboard (keep for backwards compatibility)
    path("dashboard/", views.health_dashboard, name="dashboard"),
    # Connector API endpoints
    path("api/status/", views.HealthStatusView.as_view(), name="api_status"),
    path("api/summary/", views.HealthSummaryView.as_view(), name="api_summary"),
    path("api/check/", views.HealthCheckView.as_view(), name="api_check"),
    path("api/history/", views.HealthHistoryView.as_view(), name="api_history"),
    path(
        "api/connector/<int:connector_id>/detail/",
        views.ConnectorHealthDetailAPIView.as_view(),
        name="api_connector_detail",
    ),
    # Resource API endpoints
    path(
        "api/resource/status/",
        views.ResourceHealthStatusView.as_view(),
        name="api_resource_status",
    ),
    path(
        "api/resource/summary/",
        views.ResourceHealthSummaryView.as_view(),
        name="api_resource_summary",
    ),
    path(
        "api/resource/check/",
        views.ResourceHealthCheckView.as_view(),
        name="api_resource_check",
    ),
    path(
        "api/resource/history/",
        views.ResourceHealthHistoryView.as_view(),
        name="api_resource_history",
    ),
    path(
        "api/resource/<int:resource_id>/detail/",
        views.ResourceHealthDetailAPIView.as_view(),
        name="api_resource_detail",
    ),
]
