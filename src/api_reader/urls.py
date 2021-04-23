from rest_framework import routers
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns

from api_reader.views.views_transporte_aragon import ListVehicleView, ListDriverView, LivePositionLatestView, VehicleJourneyHistoryLatestView, DistanceTravelledView

urlpatterns = format_suffix_patterns([
    path('trasportes/aragon/vehicles', ListVehicleView.as_view()),
    path('trasportes/aragon/drivers',  ListDriverView.as_view()),
    path('trasportes/aragon/live-position-latest',  LivePositionLatestView.as_view()),
    path('trasportes/aragon/vehicle-journey-history-latest',  VehicleJourneyHistoryLatestView.as_view()),
    path('trasportes/aragon/distance-travelled',  DistanceTravelledView.as_view()),

], allowed=['json', 'xml', 'csv', 'yaml', 'xlsx'])