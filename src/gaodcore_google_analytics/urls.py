# Add URLs
from django.urls import path, re_path, include
from gaodcore_google_analytics.views import GetAnalyticsData

urlpatterns = [
    path('analytics/', GetAnalyticsData.as_view())
]