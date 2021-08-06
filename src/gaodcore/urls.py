from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns

from gaodcore.views import DownloadView, ShowColumnsView, \
    ResourcesView

urlpatterns = [
    path('views', ResourcesView.as_view()),
    path('download', DownloadView.as_view()),
    path('preview', DownloadView.as_view()),
    path('show_columns', ShowColumnsView.as_view()),
]
