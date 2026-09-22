from django.urls import path

from gaodcore_manager.web_views import (
    ConnectorConfigCheckView,
    ConnectorConfigCreateView,
    ConnectorConfigDeleteView,
    ConnectorConfigDetailView,
    ConnectorConfigListView,
    ConnectorConfigUpdateView,
    ResourceConfigCheckView,
    ResourceConfigCreateView,
    ResourceConfigDeleteView,
    ResourceConfigDetailView,
    ResourceConfigListView,
    ResourceConfigUpdateView,
)

app_name = "manager_web"

urlpatterns = [
    path("", ResourceConfigListView.as_view(), name="resource-list"),
    path("resources/new/", ResourceConfigCreateView.as_view(), name="resource-create"),
    path("resources/<int:pk>/", ResourceConfigDetailView.as_view(), name="resource-detail"),
    path("resources/<int:pk>/edit/", ResourceConfigUpdateView.as_view(), name="resource-update"),
    path("resources/<int:pk>/delete/", ResourceConfigDeleteView.as_view(), name="resource-delete"),
    path("resources/<int:pk>/check/", ResourceConfigCheckView.as_view(), name="resource-check"),
    path("connectors/", ConnectorConfigListView.as_view(), name="connector-list"),
    path("connectors/new/", ConnectorConfigCreateView.as_view(), name="connector-create"),
    path("connectors/<int:pk>/", ConnectorConfigDetailView.as_view(), name="connector-detail"),
    path("connectors/<int:pk>/edit/", ConnectorConfigUpdateView.as_view(), name="connector-update"),
    path("connectors/<int:pk>/delete/", ConnectorConfigDeleteView.as_view(), name="connector-delete"),
    path("connectors/<int:pk>/check/", ConnectorConfigCheckView.as_view(), name="connector-check"),
]
