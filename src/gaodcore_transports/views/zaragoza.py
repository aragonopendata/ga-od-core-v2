import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List
from datetime import datetime

import aiohttp
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from drf_yasg.utils import swagger_auto_schema
from rest_framework.request import Request
from rest_framework.response import Response


from gaodcore_project.settings import CONFIG
from utils import get_return_list, download, download_async, gather_limited
from views import APIViewMixin


@dataclass
class DownloadProcessorConfig:
    url: str
    root_name: str
    extra_data: Dict[str, Any]


async def _download_processor_item(session: aiohttp.ClientSession, url: str, root_name: str,
                                   extra_data) -> List[Dict[str, Any]]:
    data = (await download_async(session, url)).get(root_name)

    if not data:
        return []
    if isinstance(data, dict):
        data = data.values()

    return [{**item, **extra_data} for item in data]


async def _download_processor(configs: List[DownloadProcessorConfig]) -> List[Dict[str, Any]]:
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


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LineView(APIViewMixin):
    """Returns the list of available bus lines, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        return Response(get_lines())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LineStopsView(APIViewMixin):
    """Returns the list of available stop lines, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        return Response(_get_stops())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class RoutesView(APIViewMixin):
    """Returns the routes that a line performs, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        return Response(get_return_list(_get_routes()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class StopsRouteView(APIViewMixin):
    """Sequence of stops for each line, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('stops_route',
                                                                                   line_id=route['line_id'],
                                                                                   route_id=route['route'],
                                                                                   isreturn=route['isreturn']),
                                    root_name='stops_route',
                                    extra_data={
                                        'line_id': route['line_id'],
                                        'route': route['route'],
                                        'isreturn': route['isreturn']
                                    }) for route in _get_routes()
        ]
        data = asyncio.run(_download_processor(configs=configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ArrivalTimeView(APIViewMixin):
    """Returns the passing hours of each stop, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('arrival_time',
                                                                                   stop_id=stop['stop_id']),
                                    root_name='arrival_time',
                                    extra_data={'stop_id': stop['stop_id']}) for stop in _get_stops()
        ]
        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class NoticesView(APIViewMixin):
    """Reurns the different warnings that may be produced or generated by the system, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        data = download(CONFIG.projects.transport.zaragoza.get_url('notices'))['notices']
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class OriginsView(APIViewMixin):
    """Returns the list of municipalities of origin for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        return Response(get_return_list(_get_origins()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class DestinationsView(APIViewMixin):
    """Returns the list of destination municipalities, depending on the origin, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        return Response(_get_origins_destinations())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LinesOriDesView(APIViewMixin):
    """Returns the lines that pass through the origin and destination for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'lines_ori_des',
                origin=origin_destination['origin_id'],
                destination=origin_destination['id'],
            ),
                                    root_name='lines_ori_des',
                                    extra_data={
                                        'origin_id': origin_destination['origin_id'],
                                        'destination_id': origin_destination['id']
                                    }) for origin_destination in _get_origins_destinations()
        ]

        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get',
                  decorator=swagger_auto_schema(manual_parameters=[
                      openapi.Parameter('time_sec',
                                        openapi.IN_QUERY,
                                        description="Time in seconds",
                                        type=openapi.TYPE_NUMBER)
                  ],
                                                tags=['transports']))
class TimesRouteView(APIViewMixin):
    _DEFAULT_BUS = 0
    _DEFAULT_DEPARTURE_TIME = datetime.now().hour * 3600
    # TODO: introducir el tiempo de salida (12:00) - Habría que hacerlo con la consulta
    #  ExpOriDesView
    _DIRECTIONS = [0, 1]
    _ROOT_NAME = 'times_route'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        time_sec = self.request.query_params.get('time_sec', self._DEFAULT_DEPARTURE_TIME)
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('times_route',
                                                                                   id_linea=line['id'],
                                                                                   bus=self._DEFAULT_BUS,
                                                                                   departure_time=time_sec,
                                                                                   direction=direction),
                                    root_name=self._ROOT_NAME,
                                    extra_data={
                                        'line_id': line['id'],
                                        'direction': direction,
                                        'bus': self._DEFAULT_BUS,
                                        'departure_time': time_sec
                                    }) for line in get_lines() for direction in self._DIRECTIONS
        ]

        return Response(get_return_list(asyncio.run(_download_processor(configs))))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ExpOriDesView(APIViewMixin):
    """It shows all the shipments that go between an origin and a destination, the order of introduction of the origin and the destination determines the direction, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'exp_ori_des', origin=origins_destination['origin_id'], destination=origins_destination['id']),
                                    root_name='exp_ori_des',
                                    extra_data={
                                        'origin_id': origins_destination['origin_id'],
                                        'destination_id': origins_destination['id']
                                    }) for origins_destination in _get_origins_destinations()
        ]
        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class StopsOriDesView(APIViewMixin):
    """Returns the stops that are made for the expeditions that are made between an origin and a destination, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'stops_ori_des', origin=origin_destination['origin_id'], destination=origin_destination['id']),
                                    root_name='stops_ori_des',
                                    extra_data={
                                        'origin_id': origin_destination['origin_id'],
                                        'destination_id': origin_destination['id']
                                    }) for origin_destination in _get_origins_destinations()
        ]

        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ArrivalOriDesView(APIViewMixin):
    """Returns the time of arrival at the origin and destination, for each of the expeditions that pass through the two locations, for the current date."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        origins_destinations = _get_origins_destinations()
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'arrival_ori_des', origin=origin_destination['origin_id'], destination=origin_destination['id']),
                                    root_name='arrival_ori_des',
                                    extra_data={
                                        'origin_id': origin_destination['origin_id'],
                                        'destination_id': origin_destination['id']
                                    }) for origin_destination in origins_destinations
        ]
        data = asyncio.run(_download_processor(configs))
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class SAEView(APIViewMixin):
    """Returns the geoposition of the buses at the time of the query."""
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    @staticmethod
    def get(_: Request, **_kwargs):
        url = CONFIG.projects.transport.zaragoza.get_url('sae')
        return Response(get_return_list(download(url)['sae']))
