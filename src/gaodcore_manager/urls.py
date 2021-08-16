from django.urls import path
from rest_framework import routers
from rest_framework.urlpatterns import format_suffix_patterns

from gaodcore_manager.views import ConnectorConfigView, ResourceConfigView, ValidatorView

router = routers.SimpleRouter()
router.register(r'connector-config', ConnectorConfigView)
router.register(r'resource-config', ResourceConfigView)

urlpatterns = format_suffix_patterns(
    [
        *router.urls,
        path('validator', ValidatorView.as_view()),
    ],
    allowed=['json', 'xml', 'csv', 'yaml', 'xlsx']
)
