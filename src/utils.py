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
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    parsed_data = json.loads(json.dumps(list(data), cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list


def download_check(response: Union[aiohttp.client_reqrep.ClientResponse, requests.Response]) -> bool:
    if not response.ok:
        raise BadGateway()
    return True


def download(url: str,
             auth: Optional[requests.auth.HTTPBasicAuth] = None) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    response = requests.get(url, auth=auth)
    download_check(response)
    return response.json()


def download_bulk(urls: Iterable[str], auth: Optional[aiohttp.BasicAuth] = None) -> List[Dict[str, Any]]:
    return asyncio.run(download_async_bulk(urls=urls, auth=auth))


async def download_async_bulk(urls: Iterable[str], auth: Optional[aiohttp.BasicAuth] = None) -> List[Dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        data = await asyncio.gather(*[download_async(session, url, auth) for url in urls])
    return data


async def download_async(session: aiohttp.ClientSession(), url: str,
                         auth: Optional[aiohttp.BasicAuth] = None) -> Dict[str, Any]:
    try:
        response = await session.get(url, auth=auth)
    except aiohttp.client_exceptions.ServerDisconnectedError:
        raise BadGateway()

    download_check(response)
    try:
        data = await response.json()
        return data
    except JSONDecodeError:
        # Unexpectedly 200
        return {}


async def gather_limited(concurrency_limit: int, tasks: Iterable[Coroutine]):
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def sem_task(task):
        async with semaphore:
            return await task
    return await asyncio.gather(*(sem_task(task) for task in tasks))


def flatten_object(data_dict: dict) -> dict:
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(data_dict)
    return out
