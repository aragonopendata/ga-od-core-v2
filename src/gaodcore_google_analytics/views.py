from gaodcore_project.settings import CONFIG
from abc import ABCMeta, abstractmethod
from django.shortcuts import render
from django.utils.decorators import method_decorator
from dataclasses import dataclass
from rest_framework.request import Request
from rest_framework.response import Response
from utils import download, download_bulk, get_return_list, flatten_object
from typing import Any, Dict, Iterable, List
from views import APIViewMixin
from django.views.decorators.cache import cache_page

#### MIGUEL #####
import sys
sys.path.insert(0, CONFIG.projects.google_analytics.app_path)
import json
import sys
import os
os.environ["NLS_LANG"] = "SPANISH_SPAIN.AL32UTF8"
import cgi
from rest_framework.request import Request
from rest_framework.response import Response

import logging
import logging.handlers

#### MIGUEL #####
from drf_yasg.utils import swagger_auto_schema

class APIViewGetDataMixin(APIViewMixin, metaclass=ABCMeta):
    _DATA_FIELD = 'items'
    _OPTIONS = {}

    def _get_options(self):
        return self._OPTIONS

    @property
    @abstractmethod
    def _ENDPOINT(self) -> str:
        pass

    @property
    @abstractmethod
    def _FLATTEN(self) -> str:
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
        data = self._get_default_endpoint_data()
        if self._FLATTEN:
            data = (flatten_object(row) for row in data)
        return data

    @method_decorator(cache_page(CONFIG.common_config.cache_ttl))
    def get(self, _request: Request, **_kwargs):
        return Response(get_return_list(self._get_data()))


@method_decorator(name='get', decorator=swagger_auto_schema(tags=['analytics']))
class GetAnalyticsData(APIViewGetDataMixin):
    _ENDPOINT = 'vehicles'
    _FIELD_TAGS = 'tags'
    _FIELD_STATUS = 'status'
    _FIELD_STATUS_SOLD = 'SOLD'
    _FLATTEN = False

    SERVICE_ACCOUNT_EMAIL = CONFIG.projects.google_analytics.service_account
    KEY_FILE = CONFIG.projects.google_analytics.key_file


