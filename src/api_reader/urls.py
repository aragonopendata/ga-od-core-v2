from rest_framework import routers
from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns

from api_reader.views.views_transporte_aragon import ListVehicleView, ListDriverView, LivePositionLatestView, VehicleJourneyHistoryLatestView, DistanceTravelledView
from api_reader.views.views_transporte_zaragoza import LineasView, LineasStopsView, RoutesView, StopsRouteView, ArrivalTimeView, NoticesView, OriginsView, DestinationsView, LinesOriDesView, TimesRouteView, ExpOriDesView, StopsOriDesView, ArrivalOriDesView, SAEView

urlpatterns = format_suffix_patterns([
    path('transportes/aragon/vehicles', ListVehicleView.as_view()),
    path('transportes/aragon/drivers',  ListDriverView.as_view()),
    path('transportes/aragon/live-position-latest',  LivePositionLatestView.as_view()),
    path('transportes/aragon/vehicle-journey-history-latest',  VehicleJourneyHistoryLatestView.as_view()),
    path('transportes/aragon/distance-travelled',  DistanceTravelledView.as_view()),

    path('transportes/zaragoza/lines', LineasView.as_view()),
    path('transportes/zaragoza/stops', LineasStopsView.as_view()),
    path('transportes/zaragoza/routes', RoutesView.as_view()),
    path('transportes/zaragoza/stops_route', StopsRouteView.as_view()),
    path('transportes/zaragoza/arrival_time', ArrivalTimeView.as_view()),
    path('transportes/zaragoza/notices', NoticesView.as_view()),
    path('transportes/zaragoza/origins', OriginsView.as_view()),
    path('transportes/zaragoza/destinations', DestinationsView.as_view()),
    path('transportes/zaragoza/lines_ori_des', LinesOriDesView.as_view()),
    path('transportes/zaragoza/times_route', TimesRouteView.as_view()),
    path('transportes/zaragoza/exp_ori_des', ExpOriDesView.as_view()),
    path('transportes/zaragoza/stops_ori_des', StopsOriDesView.as_view()),
    path('transportes/zaragoza/arrival_ori_des', ArrivalOriDesView.as_view()),
    path('transportes/zaragoza/sae', SAEView.as_view())


], allowed=['json', 'xml', 'csv', 'yaml', 'xlsx'])