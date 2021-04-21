from rest_framework import routers
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns

from gaodcore.views import ConnectorConfigView, ResourceConfigView, DownloadView, ValidatorView, ShowColumnsView, \
    ResourcesView

router = routers.SimpleRouter()
router.register(r'connector-config', ConnectorConfigView)
router.register(r'resource-config', ResourceConfigView)


urlpatterns = format_suffix_patterns([
    *router.urls,
    path('views', ResourcesView.as_view()),
    path('validator', ValidatorView.as_view()),
    path('download', DownloadView.as_view()),
    path('preview', DownloadView.as_view()),
    path('show_columns', ShowColumnsView.as_view()),
], allowed=['json', 'xml', 'csv', 'yaml', 'xlsx'])
