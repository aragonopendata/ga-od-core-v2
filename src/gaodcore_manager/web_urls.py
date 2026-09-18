from django.urls import path

from gaodcore_manager.web_views import (
    ConnectorConfigDetailView,
    ConnectorConfigListView,
    ResourceConfigDetailView,
    ResourceConfigListView,
)

app_name = "manager_web"

urlpatterns = [
    path("", ResourceConfigListView.as_view(), name="resource-list"),
    path("resources/<int:pk>/", ResourceConfigDetailView.as_view(), name="resource-detail"),
    path("connectors/", ConnectorConfigListView.as_view(), name="connector-list"),
    path("connectors/<int:pk>/", ConnectorConfigDetailView.as_view(), name="connector-detail"),
]
