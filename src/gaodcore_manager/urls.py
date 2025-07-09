from django.urls import path
from rest_framework import routers
from rest_framework.urlpatterns import format_suffix_patterns

from gaodcore_manager.views import (
    ConnectorConfigView, ResourceConfigView, ValidatorView,
    ConnectorListView, ConnectorDetailView, ResourceListView, ResourceDetailView
)

router = routers.SimpleRouter()
router.register(r'connector-config', ConnectorConfigView)
router.register(r'resource-config', ResourceConfigView)

app_name = 'manager'

urlpatterns = format_suffix_patterns([
    *router.urls,
    path('validator', ValidatorView.as_view()),
],
                                     allowed=['json', 'xml', 'csv', 'yaml', 'xlsx'])

# Template-based views for web interface
urlpatterns += [
    path('views/connectors/', ConnectorListView.as_view(), name='connector-list'),
    path('views/connectors/<int:pk>/', ConnectorDetailView.as_view(), name='connector-detail'),
    path('views/resources/', ResourceListView.as_view(), name='resource-list'),
    path('views/resources/<int:pk>/', ResourceDetailView.as_view(), name='resource-detail'),
]
