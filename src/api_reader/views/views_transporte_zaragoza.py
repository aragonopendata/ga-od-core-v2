
from typing import List

from django.shortcuts import render

from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.views import APIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.serializer_helpers import ReturnList

from api_reader.serializers import DictSerializer

import pandas as pd
import numpy as np
import requests
import json
import yaml

def get_return_list(data: List[dict]) -> ReturnList:
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    parsed_data = json.loads(json.dumps(data, cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list

def get_config(campo):
    with open(r'../api_reader/config.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
        conf = yaml.load(file, Loader=yaml.FullLoader)
        return conf[campo]

# Create your views here.
class ListVehicleView(APIView):
    """
    List all snippets, or create a new snippet.
    """

    def get(self, request: Request, format=None):
        # usuario = 'GobiernoDeAragon_api'
        # contr = '9u-92piXbl'
        # customerId = 39249
        
        url='https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/vehicle'
        r = requests.get(url.format(customerId=get_config('customerId')), 
                                    auth=(get_config('usuario'), get_config('contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')

        # return Response(get_return_list(data))
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class ListDriverView(APIView):
    """
    List all snippets, or create a new snippet.
    """

    def get(self, request: Request, format=None):
        # usuario = 'GobiernoDeAragon_api'
        # contr = '9u-92piXbl'
        # customerId = 39249
        
        url='https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/driver'
        r = requests.get(url.format(customerId=get_config('customerId')), 
                                    auth=(get_config('usuario'), get_config('contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')

        # return Response(get_return_list(data))
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class LivePositionLatestView(APIView):
    """
    List all snippets, or create a new snippet.
    """

    def get(self, request: Request, format=None):
        # usuario = 'GobiernoDeAragon_api'
        # contr = '9u-92piXbl'
        # customerId = 39249
        
        url='https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/tracking/live/latest'
        r = requests.get(url.format(customerId=get_config('customerId')), 
                                    auth=(get_config('usuario'), get_config('contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')

        # return Response(get_return_list(data))
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser
