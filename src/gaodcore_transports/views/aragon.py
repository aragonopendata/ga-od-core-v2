from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List

import aiohttp
import requests
import requests.auth
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from drf_yasg.utils import swagger_auto_schema
from rest_framework.request import Request
from rest_framework.response import Response

from gaodcore_project.settings import CONFIG
from utils import download, download_bulk, get_return_list
from views import APIViewMixin


class APIViewGetDataMixin(APIViewMixin, metaclass=ABCMeta):
    _DATA_FIELD = 'items'
    _OPTIONS = {}

    def _get_options(self):
        return self._OPTIONS

    @property
    @abstractmethod
    def _ENDPOINT(self) -> str:
        pass

    def _get_default_endpoint_data(self) -> List[Dict[str, Any]]:
        url = CONFIG.projects.transport.aragon.get_url(
            self._ENDPOINT, customer_id=CONFIG.projects.transport.aragon.customer_id)
        return self._get_endpoint_data(url)

    def _get_endpoint_data(self, url: str) -> List[Dict[str, Any]]:
        data = download(url,
                        auth=requests.auth.HTTPBasicAuth(CONFIG.projects.transport.aragon.user,
                                                         CONFIG.projects.transport.aragon.password))
        return data[self._DATA_FIELD]

    def _get_data(self) -> Iterable[Dict[str, Any]]:
        return self._get_default_endpoint_data()

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _request: Request, **_kwargs):
        return Response(get_return_list(self._get_data()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ListVehicleView(APIViewGetDataMixin):
    """Returns the registration, group and current odometer reading for a vehicle. The odometer reading is in kilometres."""
    _ENDPOINT = 'vehicles'
    _FIELD_TAGS = 'tags'
    _FIELD_STATUS = 'status'
    _FIELD_STATUS_SOLD = 'SOLD'

    def _get_data(self) -> Iterable[Dict[str, Any]]:
        data = super()._get_data()
        return self._process(data)

    def _process(self, data: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        for row in data:
            if self._FIELD_TAGS in row:
                del row[self._FIELD_TAGS]
            if row[self._FIELD_STATUS] != self._FIELD_STATUS_SOLD:
                yield row


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ListDriverView(APIViewGetDataMixin):
    """Returns details for all drivers and the group they have been assigned to."""
    _ENDPOINT = 'drivers'


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LivePositionLatestView(APIViewGetDataMixin):
    """Returns the live position and status for a resource (driver or vehicle). The live position for a vehicle on a private journey is not returned."""
    _ENDPOINT = 'live_position_latest'


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class VehicleJourneyHistoryLatestView(APIViewGetDataMixin):
    """Returns the latest tracking events that have been processed by Connect for a vehicle since the specified date. No details are returned for a private journey."""
    _ENDPOINT = 'vehicles'
    _TIMEDELTA = {"weeks": 2}
    _VEHICLE_JOURNEY_HISTORY_LATEST = 'vehicle_journey_history_latest'

    def _get_data(self):
        urls = {
            CONFIG.projects.transport.aragon.get_url(
                self._VEHICLE_JOURNEY_HISTORY_LATEST,
                customer_id=CONFIG.projects.transport.aragon.customer_id,
                from_date_time=(datetime.now() - timedelta(**self._TIMEDELTA)).isoformat(),
                vehicle_id=row['id'],
            )
            for row in self._get_default_endpoint_data()
        }

        data = download_bulk(urls,
                             auth=aiohttp.BasicAuth(CONFIG.projects.transport.aragon.user,
                                                    CONFIG.projects.transport.aragon.password))
        return (journey for journey_history in data for journey in journey_history['items'])


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class DistanceTravelledView(APIViewGetDataMixin):
    """Returns the distance traveled (in kilometers) by a vehicle for the last day made."""
    _ENDPOINT = 'vehicles'
    _DISTANCE_TRAVELED = 'distance_travelled'

    def _get_data(self):
        url = CONFIG.projects.transport.aragon.get_url(
            self._DISTANCE_TRAVELED,
            customer_id=CONFIG.projects.transport.aragon.customer_id,
            vehicle_ids=','.join({row['id']
                                  for row in self._get_default_endpoint_data()}),
            start_date=datetime.min.isoformat(),
            end_date=datetime.now().isoformat())
        data = download(url,
                        auth=requests.auth.HTTPBasicAuth(CONFIG.projects.transport.aragon.user,
                                                         CONFIG.projects.transport.aragon.password))
        return data
