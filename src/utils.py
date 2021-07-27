"""Generic module that contains functions that can be used by all Django apps."""

import asyncio
import json
from json import JSONDecodeError
from typing import Iterable, List, Dict, Any, Optional, Union, Coroutine

import aiohttp
import aiohttp.client_exceptions
import requests
import requests.auth
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.utils.serializer_helpers import ReturnList

from exceptions import BadGateway
from serializers import DictSerializer


def get_return_list(data: Iterable[dict]) -> ReturnList:
    """From a iterable of dicts convert to Django ReturnList. ReturnList is a object that must be send to render
    by Django."""
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    parsed_data = json.loads(json.dumps(list(data), cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list


def download_check(response: Union[aiohttp.client_reqrep.ClientResponse, requests.Response]) -> bool:
    """Check if response of aiohttp or response of request is correct."""
    if not response.ok:
        raise BadGateway()
    return True


def download(url: str,
             auth: Optional[requests.auth.HTTPBasicAuth] = None) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Download a resource without asyncio."""
    response = requests.get(url, auth=auth)
    download_check(response)
    return response.json()


def download_bulk(urls: Iterable[str], auth: Optional[aiohttp.BasicAuth] = None) -> List[Dict[str, Any]]:
    """Download a bulk of resources with asyncio."""
    return asyncio.run(download_async_bulk(urls=urls, auth=auth))


async def download_async_bulk(urls: Iterable[str], auth: Optional[aiohttp.BasicAuth] = None) -> List[Dict[str, Any]]:
    """Download a bulk of resources with asyncio."""
    async with aiohttp.ClientSession() as session:
        data = await asyncio.gather(*[download_async(session, url, auth) for url in urls])
    return data


async def download_async(session: aiohttp.ClientSession,
                         url: str,
                         auth: Optional[aiohttp.BasicAuth] = None) -> Dict[str, Any]:
    """Download a resource with asyncio."""
    try:
        response = await session.get(url, auth=auth)
    except aiohttp.client_exceptions.ServerDisconnectedError as err:
        raise BadGateway() from err

    download_check(response)
    try:
        data = await response.json()
        return data
    except JSONDecodeError:
        # Unexpectedly 200
        return {}


async def gather_limited(concurrency_limit: int, tasks: Iterable[Coroutine]):
    """Limit concurrency with a limit."""
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


def flatten_dict(data_dict: dict) -> dict:
    """Flatten a dict. All keys of list or dict of will be moved to the root dict."""
    out = {}

    def flatten(obj_to_flatten, name=''):
        if isinstance(obj_to_flatten, dict):
            for aux in obj_to_flatten:
                flatten(obj_to_flatten[aux], name + aux + '_')
        elif isinstance(obj_to_flatten, list):
            i = 0
            for aux in obj_to_flatten:
                flatten(aux, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = obj_to_flatten

    flatten(data_dict)
    return out
