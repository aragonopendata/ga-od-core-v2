"""GAODCore Zaragoza transports views."""

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List
from datetime import datetime

import aiohttp
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.request import Request
from rest_framework.response import Response

from gaodcore_project.settings import CONFIG
from utils import get_return_list, download, download_async, gather_limited
from views import APIViewMixin


@dataclass
class DownloadProcessorConfig:
    """Dataclass that contain all data to download a resource. Also include extra data that will be constant in all
    rows."""
    url: str
    root_name: str
    extra_data: Dict[str, Any]


async def _download_processor_item(session: aiohttp.ClientSession, url: str, root_name: str,
                                   extra_data) -> List[Dict[str, Any]]:
    """Download data a url and include extra data in all rows."""
    data = (await download_async(session, url)).get(root_name)

    if not data:
        return []
    if isinstance(data, dict):
        data = data.values()

    return [{**item, **extra_data} for item in data]


async def _download_processor(configs: List[DownloadProcessorConfig]) -> List[Dict[str, Any]]:
    """Download all resources."""
    async with aiohttp.ClientSession() as session:
        full_data = await gather_limited(CONFIG.projects.transport.zaragoza.max_concurrency, [
            _download_processor_item(session, config.url, config.root_name, extra_data=config.extra_data)
            for config in configs
        ])

    return [item for partial_data in full_data for item in partial_data]


def _get_origins() -> List[Dict[str, Any]]:
    """Returns the list of available origins."""
    url = CONFIG.projects.transport.zaragoza.get_url('origins')
    download(url)
    return download(url)['origins']


def _get_origins_destinations() -> List[Dict[str, Any]]:
    """Returns the list of available destinations for each origin."""
    origins_ids = [origin['id'] for origin in _get_origins()]
    config = [
        DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('destinations', id=origin_id),
                                root_name='destinations',
                                extra_data={'origin_id': origin_id}) for origin_id in origins_ids
    ]
    return asyncio.run(_download_processor(config))


def _get_routes():
    """Returns the list of available stops for each line."""
    configs = [
        DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('routes', id=line['id']),
                                root_name='routes',
                                extra_data={'line_id': line['id']}) for line in get_lines()
    ]

    data = asyncio.run(_download_processor(configs))
    return data


def _get_stops():
    """Returns the list of available stops."""
    return download(CONFIG.projects.transport.zaragoza.get_url('stops'))['stops']


def get_lines():
    """Returns the list of available lines."""
    return download(CONFIG.projects.transport.zaragoza.get_url('lines'))['lines']


class ZaragozaTransportMixin(APIViewMixin):
    """Mixin with field names."""
    _LINE_ID_FIELD = 'line_id'
    _ROUTE_FIELD = 'route'
    _IS_RETURN_FIELD = 'isreturn'
    _STOP_ID_FIELD = 'stop_id'
    _ID_FIELD = 'id'
    _ORIGIN_ID_FIELD = 'origin_id'
    _DESTINATION_ID_FIELD = 'destination_id'


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LineView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the list of available bus lines, for the current date."""
    @staticmethod
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Returns the list of available bus lines, for the current date."""
        return Response(get_lines())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LineStopsView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the list of available stop lines, for the current date."""
    @staticmethod
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        return Response(_get_stops())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class RoutesView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the routes that a line performs, for the current date."""
    @staticmethod
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        return Response(get_return_list(_get_routes()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class StopsRouteView(ZaragozaTransportMixin):  # pylint: disable=too-few-public-methods
    """Sequence of stops for each line, for the current date."""

    _ENDPOINT = 'stops_route'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'stops_route',
                line_id=route[self._LINE_ID_FIELD],
                route_id=route[self._ROUTE_FIELD],
                isreturn=route[self._IS_RETURN_FIELD]),
                                    root_name=self._ENDPOINT,
                                    extra_data={
                                        self._LINE_ID_FIELD: route[self._LINE_ID_FIELD],
                                        self._ROUTE_FIELD: route[self._ROUTE_FIELD],
                                        self._IS_RETURN_FIELD: route[self._IS_RETURN_FIELD]
                                    }) for route in _get_routes()
        ]
        data = asyncio.run(_download_processor(configs=configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ArrivalTimeView(ZaragozaTransportMixin):  # pylint: disable=too-few-public-methods
    """Returns the passing hours of each stop, for the current date."""

    _ENDPOINT = 'arrival_time'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(self._ENDPOINT,
                                                                                   stop_id=stop[self._STOP_ID_FIELD]),
                                    root_name=self._ENDPOINT,
                                    extra_data={self._STOP_ID_FIELD: stop[self._STOP_ID_FIELD]})
            for stop in _get_stops()
        ]
        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class NoticesView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the different warnings that may be produced or generated by the system, for the current date."""

    _ENDPOINT = 'notices'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        data = download(CONFIG.projects.transport.zaragoza.get_url(self._ENDPOINT))[self._ENDPOINT]
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class OriginsView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the list of municipalities of origin for the current date."""
    @staticmethod
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        return Response(get_return_list(_get_origins()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class DestinationsView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the list of destination municipalities, depending on the origin, for the current date."""
    @staticmethod
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        return Response(_get_origins_destinations())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LinesOriDesView(ZaragozaTransportMixin):  # pylint: disable=too-few-public-methods
    """Returns the lines that pass through the origin and destination for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'lines_ori_des',
                origin=origin_destination[self._ORIGIN_ID_FIELD],
                destination=origin_destination[self._ID_FIELD],
            ),
                                    root_name='lines_ori_des',
                                    extra_data={
                                        self._ORIGIN_ID_FIELD: origin_destination[self._ORIGIN_ID_FIELD],
                                        self._DESTINATION_ID_FIELD: origin_destination[self._ID_FIELD]
                                    }) for origin_destination in _get_origins_destinations()
        ]

        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


class TimesRouteView(ZaragozaTransportMixin):  # pylint: disable=too-few-public-methods
    """Returns the complete route if it is a theoretical route, or from where the bus is located, showing the times it
    will cost in each of them, for the current date."""
    _DEFAULT_BUS = 0
    _DEFAULT_DEPARTURE_TIME = datetime.now().hour * 3600
    _DIRECTIONS = [0, 1]
    _ENDPOINT = 'times_route'
    _TIME_SEC_PARAM = 'time_sec'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @swagger_auto_schema(manual_parameters=[
        openapi.Parameter('time_sec', openapi.IN_QUERY, description="Time in seconds", type=openapi.TYPE_NUMBER)
    ],
                         tags=['transports'])
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        time_sec = self.request.query_params.get(self._TIME_SEC_PARAM, self._DEFAULT_DEPARTURE_TIME)
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(self._ENDPOINT,
                                                                                   id_linea=line[self._ID_FIELD],
                                                                                   bus=self._DEFAULT_BUS,
                                                                                   departure_time=time_sec,
                                                                                   direction=direction),
                                    root_name=self._ENDPOINT,
                                    extra_data={
                                        self._LINE_ID_FIELD: line[self._ID_FIELD],
                                        'direction': direction,
                                        'bus': self._DEFAULT_BUS,
                                        'departure_time': time_sec
                                    }) for line in get_lines() for direction in self._DIRECTIONS
        ]

        return Response(get_return_list(asyncio.run(_download_processor(configs))))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ExpOriDesView(ZaragozaTransportMixin):  # pylint: disable=too-few-public-methods
    """It shows all the shipments that go between an origin and a destination, the order of introduction of the origin
    and the destination determines the direction, for the current date."""

    _ENDPOINT = 'exp_ori_des'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """It shows all the shipments that go between an origin and a destination, the order of introduction of the origin
    and the destination determines the direction, for the current date."""
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                self._ENDPOINT,
                origin=origins_destination[self._ORIGIN_ID_FIELD],
                destination=origins_destination[self._ID_FIELD]),
                                    root_name=self._ENDPOINT,
                                    extra_data={
                                        self._ORIGIN_ID_FIELD: origins_destination[self._ORIGIN_ID_FIELD],
                                        self._DESTINATION_ID_FIELD: origins_destination[self._ID_FIELD]
                                    }) for origins_destination in _get_origins_destinations()
        ]
        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class StopsOriDesView(ZaragozaTransportMixin):  # pylint: disable=too-few-public-methods
    """Returns the stops that are made for the expeditions that are made between an origin and a destination, for the
    current date."""

    _ENDPOINT = 'stops_ori_des'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                self._ENDPOINT,
                origin=origin_destination[self._ORIGIN_ID_FIELD],
                destination=origin_destination[self._ID_FIELD]),
                                    root_name=self._ENDPOINT,
                                    extra_data={
                                        self._ORIGIN_ID_FIELD: origin_destination[self._ORIGIN_ID_FIELD],
                                        self._DESTINATION_ID_FIELD: origin_destination[self._ID_FIELD]
                                    }) for origin_destination in _get_origins_destinations()
        ]

        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ArrivalOriDesView(APIViewMixin):  # pylint: disable=too-few-public-methods
    """Returns the time of arrival at the origin and destination, for each of the expeditions that pass through the two
     locations, for the current date."""
    _ORIGIN_ID_FIELD = 'origin_id'
    _DESTINATION_ID_FIELD = 'destination_id'
    _ID_FIELD = 'id'
    _ENDPOINT = 'arrival_ori_des'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        origins_destinations = _get_origins_destinations()
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                self._ENDPOINT,
                origin=origin_destination[self._ORIGIN_ID_FIELD],
                destination=origin_destination[self._ID_FIELD]),
                                    root_name=self._ENDPOINT,
                                    extra_data={
                                        self._ORIGIN_ID_FIELD: origin_destination[self._ORIGIN_ID_FIELD],
                                        self._DESTINATION_ID_FIELD: origin_destination[self._ID_FIELD]
                                    }) for origin_destination in origins_destinations
        ]
        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class SAEView(APIViewMixin):  # pylint: disable=too-few-public-methods
    _ENDPOINT = 'sae'
    """Returns the geoposicions of the buses at the time of the query."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        """Implementation of get of APIViewMixin."""
        url = CONFIG.projects.transport.zaragoza.get_url(self._ENDPOINT)
        return Response(get_return_list(download(url)[self._ENDPOINT]))
