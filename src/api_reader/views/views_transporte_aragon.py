
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
from datetime import datetime, timedelta

def get_return_list(data: List[dict]) -> ReturnList:
    return_list = ReturnList(serializer=DictSerializer)

    # FIXME: this convert dates to string, in some renders like xlsx produce a bad representation.
    #  This is required to fixit and find a better solution. :(
    parsed_data = json.loads(json.dumps(data, cls=DjangoJSONEncoder))
    for item in parsed_data:
        return_list.append(item)

    return return_list

def get_config(campo):
    with open(r'./api_reader/config.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
        conf = yaml.load(file, Loader=yaml.FullLoader)
        return conf[campo]

def get_vehicles():
    url='https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/vehicle'
    r = requests.get(url.format(customerId=get_config('customerId')), 
                                    auth=(get_config('usuario'), get_config('contr')))
    return r

# Create your views here.
class ListVehicleView(APIView):
    """
    List all snippets, or create a new snippet.
    """

    def get(self, request: Request, format=None):    
        data = pd.json_normalize(get_vehicles().json()['items']).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class ListDriverView(APIView):
    """
    List all snippets, or create a new snippet.
    """

    def get(self, request: Request, format=None):
        url='https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/driver'
        r = requests.get(url.format(customerId=get_config('customerId')), 
                                    auth=(get_config('usuario'), get_config('contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')

        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class LivePositionLatestView(APIView):
    """
    List all snippets, or create a new snippet.
    """


    def get(self, request: Request, format=None):     
        url='https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/tracking/live/latest'
        r = requests.get(url.format(customerId=get_config('customerId')), 
                                    auth=(get_config('usuario'), get_config('contr')))
        data = pd.json_normalize(r.json()['items']).replace({np.nan:None}).to_dict('records')

        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class VehicleJourneyHistoryLatestView(APIView):
    """
    List all snippets, or create a new snippet.
    """
    #TODO: Tarda alrededor de 1.5 min en generar los resultados

    def get(self, request: Request, format=None):
        fromDateTime = (datetime.now()- timedelta(days=13)).strftime('%Y-%m-%dT00:00:00')

        df = pd.json_normalize(get_vehicles().json()['items'])
        list_vehicleIds = list(df['id'])

        df = []
        for vehicleIds in list_vehicleIds:
            url = 'https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/tracking/history/vehicle/latest'
            url += '?fromDateTime={fromDateTime}&vehicleId={vehicleIds}'.format(fromDateTime=fromDateTime,vehicleIds=vehicleIds)
            r = requests.get(url.format(customerId=get_config('customerId')), 
                            auth=(get_config('usuario'), get_config('contr')))
            tmp = pd.json_normalize(r.json()['items'])
            tmp['vehicleIds'] = vehicleIds
            df.append(tmp)
        
        data = pd.concat(df).reset_index(drop=True).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser


class DistanceTravelledView(APIView):
    """
    List all snippets, or create a new snippet.
    """
    #TODO: Tarda alrededor de 1.5 min en generar los resultados

    def get(self, request: Request, format=None):

        startDate = '2015-06-01T22:00:00'
        endDate = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        df = pd.json_normalize(get_vehicles().json()['items'])
        list_vehicleIds = list(df['id'])

        df = []
        for vehicleIds in list_vehicleIds:
            url = 'https://api.masternautconnect.com/connect-webservices/services/public/v1/customer/{customerId}/tracking/journey/summary'
            url += '?startDate={startDate}&endDate={endDate}&vehicleIds={vehicleIds}'.format(startDate=startDate,endDate=endDate,vehicleIds=vehicleIds)
            r = requests.get(url.format(customerId=get_config('customerId')), 
                            auth=(get_config('usuario'), get_config('contr')))
            tmp = pd.json_normalize(r.json())
            tmp['vehicleIds'] = vehicleIds
            df.append(tmp)
        
        data = pd.concat(df).reset_index(drop=True).replace({np.nan:None}).to_dict('records')
        return Response(get_return_list(data))

    def get_serializer(self, *args, **kwargs):
        ser = DictSerializer(*args, **kwargs, data=self.response.data)
        return ser