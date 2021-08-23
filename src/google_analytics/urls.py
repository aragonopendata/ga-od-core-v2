"""GAODCore transports urlpatterns."""

from django.urls import path
# from rest_framework.urlpatterns import format_suffix_patterns

from google_analytics.views import GoogleAnalyticsView


urlpatterns = [
    path('analytics', GoogleAnalyticsView.as_view()),
]
