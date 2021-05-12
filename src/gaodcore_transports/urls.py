from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns

from gaodcore_transports.views.aragon import ListVehicleView, ListDriverView, LivePositionLatestView, \
    VehicleJourneyHistoryLatestView, DistanceTravelledView
from gaodcore_transports.views.zaragoza import LineView, LineStopsView, RoutesView, StopsRouteView, \
    ArrivalTimeView, NoticesView, OriginsView, DestinationsView, LinesOriDesView, TimesRouteView, ExpOriDesView, \
    StopsOriDesView, ArrivalOriDesView, SAEView

urlpatterns = format_suffix_patterns([
    path('aragon/vehicles', ListVehicleView.as_view()),
    path('aragon/drivers', ListDriverView.as_view()),
    path('aragon/live-position-latest', LivePositionLatestView.as_view()),
    path('aragon/vehicle-journey-history-latest', VehicleJourneyHistoryLatestView.as_view()),
    path('aragon/distance-travelled', DistanceTravelledView.as_view()),
    path('zaragoza/lines', LineView.as_view()),
    path('zaragoza/stops', LineStopsView.as_view()),
    path('zaragoza/routes', RoutesView.as_view()),
    path('zaragoza/stops_route', StopsRouteView.as_view()),
    path('zaragoza/arrival_time', ArrivalTimeView.as_view()),
    path('zaragoza/notices', NoticesView.as_view()),
    path('zaragoza/origins', OriginsView.as_view()),
    path('zaragoza/destinations', DestinationsView.as_view()),
    path('zaragoza/lines_ori_des', LinesOriDesView.as_view()),
    path('zaragoza/times_route', TimesRouteView.as_view()),
    path('zaragoza/exp_ori_des', ExpOriDesView.as_view()),
    path('zaragoza/stops_ori_des', StopsOriDesView.as_view()),
    path('zaragoza/arrival_ori_des', ArrivalOriDesView.as_view()),
    path('zaragoza/sae', SAEView.as_view())
],
                                     allowed=['json', 'xml', 'csv', 'yaml', 'xlsx'])
