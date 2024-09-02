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
from rest_framework.exceptions import ValidationError
from rest_framework.utils.serializer_helpers import ReturnList

from connectors import TooManyRowsErrorExcel
from exceptions import BadGateway
from serializers import DictSerializer

from geoalchemy2 import functions, Geometry, elements
from geoalchemy2.shape import to_shape 
from sqlalchemy import func
from shapely_geojson import dumps, Feature

from django.core.serializers.base import DeserializationError
from django.core.serializers.python import (
    Deserializer as PythonDeserializer, Serializer as PythonSerializer,
)
from django.utils.duration import duration_iso_string
from django.utils.functional import Promise
from django.utils.timezone import is_aware

import datetime
import decimal
import uuid
import pandas as pd
import logging



def serializerJsonEncoder(o):
       
            # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime.datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith('+00:00'):
                r = r[:-6] + 'Z'
            return r
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, datetime.time):
            if is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, datetime.timedelta):
            return duration_iso_string(o)
        elif isinstance(o, (decimal.Decimal, uuid.UUID, Promise)):
            return str(o)
        # See 'Geometry Format' transform geoJson format
        elif isinstance(o, elements.WKBElement):
            shply_geom = str(to_shape(o))
            return shply_geom
        else:
            return o



def get_return_list(data: Iterable[dict]) -> ReturnList:
    """From a iterable of dicts convert to Django ReturnList. ReturnList is a object that must be send to render
    by Django."""
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    # a new presonal serializers DjangoJSONEncoder, similar django.core.serializers.json include GeoJson serializar))

    
    parsed_data = json.loads(json.dumps(list(data), default=serializerJsonEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list

def modify_header(return_list, columns_name):

    if len(columns_name) > 0 and len(columns_name) == len(list(return_list[0].keys())):
        df=pd.DataFrame(return_list)
        columns_modification_dict = dict(zip(list(return_list[0].keys()), columns_name))
        df=df.rename(index=str, columns=columns_modification_dict)
        return df.to_dict('records')
    elif len(columns_name) > 0:
        raise ValidationError("El número de columnas tiene que ser igual al numero de fields o al número total de columnas por defecto", 400) from TooManyRowsErrorExcel

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
