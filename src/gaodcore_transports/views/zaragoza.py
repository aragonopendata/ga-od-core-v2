import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List

import aiohttp
import numpy as np
import pandas as pd
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
    if type(data) == dict:
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
    url = CONFIG.projects.transport.zaragoza.get_url('origins')
    download(url)
    return download(url)['origins']


def _get_origins_destinations() -> List[Dict[str, Any]]:
    origins_ids = [origin['id'] for origin in _get_origins()]
    config = [
        DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('destinations', id=origin_id),
                                root_name='destinations',
                                extra_data={'origin_id': origin_id}) for origin_id in origins_ids
    ]
    return asyncio.run(_download_processor(config))


def _get_routes():
    configs = [
        DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url('routes', id=line['id']),
                                root_name='routes',
                                extra_data={'line_id': line['id']}) for line in get_lines()
    ]

    data = asyncio.run(_download_processor(configs))
    return data


def _get_stops():
    return download(CONFIG.projects.transport.zaragoza.get_url('stops'))['stops']


def get_lines():
    return download(CONFIG.projects.transport.zaragoza.get_url('lines'))['lines']


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LineView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        return Response(get_lines())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LineStopsView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        return Response(_get_stops())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class RoutesView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        return Response(get_return_list(_get_routes()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class StopsRouteView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'stops_route', line_id=route['line_id'], route_id=route['route'], isreturn=route['isreturn']),
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
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
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
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        data = download(CONFIG.projects.transport.zaragoza.get_url('notices'))['notices']
        return Response(get_return_list(data))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class OriginsView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        return Response(get_return_list(_get_origins()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class DestinationsView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        return Response(_get_origins_destinations())


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class LinesOriDesView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
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


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class TimesRouteView(APIViewMixin):
    _DEFAULT_BUS = 0
    _DEFAULT_DEPARTURE_TIME = 43200
    # TODO: introducir el tiempo de salida (12:00) - Habría que hacerlo con la consulta
    #  ExpOriDesView
    _DIRECTIONS = [0, 1]
    _ROOT_NAME = 'times_route'

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        configs = [
            DownloadProcessorConfig(url=CONFIG.projects.transport.zaragoza.get_url(
                'times_route',
                id_linea=line['id'],
                bus=self._DEFAULT_BUS,
                departure_time=self._DEFAULT_DEPARTURE_TIME,
                direction=direction),
                                    root_name=self._ROOT_NAME,
                                    extra_data={
                                        'line_id': line['id'],
                                        'direction': direction,
                                        'bus': self._DEFAULT_BUS,
                                        'departure_time': self._DEFAULT_DEPARTURE_TIME
                                    }) for line in get_lines() for direction in self._DIRECTIONS
        ]

        return Response(get_return_list(asyncio.run(_download_processor(configs))))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['transports']))
class ExpOriDesView(APIViewMixin):
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
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
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
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
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
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
    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _: Request, **_kwargs):
        url = CONFIG.projects.transport.zaragoza.get_url('sae')
        return Response(get_return_list(download(url)['sae']))
